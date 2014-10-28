// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/consensus/log-test-base.h"

#include <vector>

#include "kudu/common/iterator.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/server/logical_clock.h"
#include "kudu/server/metadata.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet_metadata.h"

namespace kudu {

namespace log {

extern const char* kTestTable;
extern const char* kTestTablet;

} // namespace log

namespace tablet {

using std::vector;
using std::string;

using consensus::ConsensusBootstrapInfo;
using consensus::ConsensusMetadata;
using consensus::kMinimumTerm;
using consensus::kUninitializedQuorumSeqNo;
using log::Log;
using log::LogTestBase;
using log::OpIdAnchorRegistry;
using log::ReadableLogSegment;
using server::Clock;
using server::LogicalClock;

class BootstrapTest : public LogTestBase {
 protected:

  void SetUp() OVERRIDE {
    LogTestBase::SetUp();
    master_block_.set_table_id(log::kTestTable);
    master_block_.set_tablet_id(log::kTestTablet);
    master_block_.set_block_a(fs_manager_->GenerateBlockId().ToString());
    master_block_.set_block_b(fs_manager_->GenerateBlockId().ToString());
  }

  Status LoadTestTabletMetadata(int mrs_id, int delta_id, scoped_refptr<TabletMetadata>* meta) {
    RETURN_NOT_OK(TabletMetadata::LoadOrCreate(fs_manager_.get(),
                                               master_block_,
                                               log::kTestTable,
                                               // We need a schema with ids for
                                               // TabletMetadata::LoadOrCreate()
                                               SchemaBuilder(schema_).Build(),
                                               "",
                                               "",
                                               REMOTE_BOOTSTRAP_DONE,
                                               meta));
    (*meta)->SetLastDurableMrsIdForTests(mrs_id);
    if ((*meta)->GetRowSetForTests(0) != NULL) {
      (*meta)->GetRowSetForTests(0)->SetLastDurableRedoDmsIdForTests(delta_id);
    }
    return (*meta)->Flush();
  }

  Status PersistTestTabletMetadataState(TabletBootstrapStatePB state) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK(LoadTestTabletMetadata(-1, -1, &meta));
    meta->set_remote_bootstrap_state(state);
    RETURN_NOT_OK(meta->Flush());
    return Status::OK();
  }

  Status RunBootstrapOnTestTablet(const scoped_refptr<TabletMetadata>& meta,
                                  shared_ptr<Tablet>* tablet,
                                  ConsensusBootstrapInfo* boot_info) {
    gscoped_ptr<Log> new_log;
    scoped_refptr<OpIdAnchorRegistry> new_anchor_registry;
    gscoped_ptr<TabletStatusListener> listener(new TabletStatusListener(meta));

    // Now attempt to recover the log
    RETURN_NOT_OK(BootstrapTablet(
        meta,
        scoped_refptr<Clock>(LogicalClock::CreateStartingAt(Timestamp::kInitialTimestamp)),
        NULL,
        listener.get(),
        tablet,
        &new_log,
        &new_anchor_registry,
        boot_info));
    log_.reset(new_log.release());

    return Status::OK();
  }

  Status BootstrapTestTablet(int mrs_id,
                             int delta_id,
                             shared_ptr<Tablet>* tablet,
                             ConsensusBootstrapInfo* boot_info) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK_PREPEND(LoadTestTabletMetadata(mrs_id, delta_id, &meta),
                          "Unable to load test tablet metadata");

    metadata::QuorumPB quorum;
    quorum.set_local(true);
    quorum.set_seqno(kUninitializedQuorumSeqNo);
    quorum.add_peers()->set_permanent_uuid(meta->fs_manager()->uuid());
    gscoped_ptr<ConsensusMetadata> cmeta;
    RETURN_NOT_OK_PREPEND(ConsensusMetadata::Create(meta->fs_manager(), meta->oid(), quorum,
                                                    kMinimumTerm, &cmeta),
                          "Unable to create consensus metadata");

    RETURN_NOT_OK_PREPEND(RunBootstrapOnTestTablet(meta, tablet, boot_info),
                          "Unable to bootstrap test tablet");
    return Status::OK();
  }

  void IterateTabletRows(const Tablet* tablet,
                         vector<string>* results) {
    gscoped_ptr<RowwiseIterator> iter;
    // TODO: there seems to be something funny with timestamps in this test.
    // Unless we explicitly scan at a snapshot including all timestamps, we don't
    // see the bootstrapped operation. This is likely due to KUDU-138 -- perhaps
    // we aren't properly setting up the clock after bootstrap.
    MvccSnapshot snap = MvccSnapshot::CreateSnapshotIncludingAllTransactions();
    ASSERT_STATUS_OK(tablet->NewRowIterator(schema_, snap, &iter));
    ASSERT_STATUS_OK(iter->Init(NULL));
    ASSERT_STATUS_OK(IterateToStringList(iter.get(), results));
    BOOST_FOREACH(const string& result, *results) {
      VLOG(1) << result;
    }
  }

  TabletMasterBlockPB master_block_;
};

// Tests a normal bootstrap scenario
TEST_F(BootstrapTest, TestBootstrap) {
  BuildLog();

  AppendReplicateBatch(current_id_);
  ASSERT_STATUS_OK(RollLog());

  AppendCommit(current_id_);

  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  ASSERT_STATUS_OK(BootstrapTestTablet(-1, -1, &tablet, &boot_info));

  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());
}

// Tests attempting a local bootstrap of a tablet that was in the middle of a
// remote bootstrap before "crashing".
TEST_F(BootstrapTest, TestIncompleteRemoteBootstrap) {
  BuildLog();

  ASSERT_OK(PersistTestTabletMetadataState(REMOTE_BOOTSTRAP_COPYING));
  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  Status s = BootstrapTestTablet(-1, -1, &tablet, &boot_info);
  ASSERT_TRUE(s.IsCorruption()) << "Expected corruption: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "TabletMetadata bootstrap state is REMOTE_BOOTSTRAP_COPYING");
  LOG(INFO) << "State is still REMOTE_BOOTSTRAP_COPYING, as expected: " << s.ToString();
}

// Tests the KUDU-141 scenario: bootstrap when there is
// an orphaned commit after a log roll.
// The test simulates the following scenario:
//
// 1) 'Replicate A' is written to Segment_1, which is anchored
// on MemRowSet_1.
// 2) Segment_1 is rolled, 'Commit A' is written to Segment_2.
// 3) MemRowSet_1 is flushed, releasing all anchors.
// 4) Segment_1 is garbage collected.
// 5) We crash, requiring a recovery of Segment_2 which now contains
// the orphan 'Commit A'.
TEST_F(BootstrapTest, TestOrphanCommit) {
  BuildLog();

  // Step 1) Write a REPLICATE to the log, and roll it.
  AppendReplicateBatch(current_id_);
  ASSERT_STATUS_OK(RollLog());

  // Step 2) Write the corresponding COMMIT in the second segment.
  AppendCommit(current_id_);

  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;

  // Step 3) Apply the operations in the log to the tablet and flush
  // the tablet to disk.
  ASSERT_STATUS_OK(BootstrapTestTablet(-1, -1, &tablet, &boot_info));
  ASSERT_STATUS_OK(tablet->Flush());

  // Create a new log segment.
  ASSERT_STATUS_OK(RollLog());

  // Step 4) Create an orphanned commit by first adding a commit to
  // the newly rolled logfile, and then by removing the previous
  // commits.
  AppendCommit(current_id_);
  log::SegmentSequence segments;
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));
  fs_manager_->env()->DeleteFile(segments[0]->path());

  // Note: when GLOG_v=1, the test logs should include 'Ignoring
  // orphan commit: op_type: WRITE_OP...' line.
  ASSERT_STATUS_OK(BootstrapTestTablet(2, 1, &tablet, &boot_info));

  // Confirm that the legitimate data (from Step 3) is still there.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());
  ASSERT_EQ("(uint32 key=0, uint32 int_val=0, string string_val=this is a test insert)",
            results[0]);
  ASSERT_EQ(2, tablet->metadata()->last_durable_mrs_id());
}

// Tests this scenario:
// Orphan COMMIT with id <= current mrs id, followed by a REPLICATE
// message with mrs_id > current mrs_id, and a COMMIT message for that
// REPLICATE message.
//
// This should result in the orphan COMMIT being ignored, but the last
// REPLICATE/COMMIT messages ending up in the tablet.
TEST_F(BootstrapTest, TestNonOrphansAfterOrphanCommit) {
  BuildLog();

  AppendReplicateBatch(current_id_);
  ASSERT_STATUS_OK(RollLog());

  AppendCommit(current_id_);

  log::SegmentSequence segments;
  ASSERT_OK(log_->GetLogReader()->GetSegmentsSnapshot(&segments));
  fs_manager_->env()->DeleteFile(segments[0]->path());

  current_id_ += 2;

  AppendReplicateBatch(current_id_);
  AppendCommit(current_id_, 2, 1, 0);

  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  ASSERT_STATUS_OK(BootstrapTestTablet(1, 0, &tablet, &boot_info));

  // Confirm that the legitimate data is there.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());

  // 'key=2' means the REPLICATE message was inserted when current_id_ was 2, meaning
  // that only the non-orphan commit went in.
  ASSERT_EQ("(uint32 key=2, uint32 int_val=0, string string_val=this is a test insert)",
            results[0]);
}

// Test for where the server crashes in between REPLICATE and COMMIT.
// Bootstrap should not replay the operation, but should return it in
// the ConsensusBootstrapInfo
TEST_F(BootstrapTest, TestOrphanedReplicate) {
  BuildLog();

  // Append a REPLICATE with no commit
  int replicate_index = current_id_++;
  AppendReplicateBatch(replicate_index);

  // Bootstrap the tablet. It shouldn't replay anything.
  ConsensusBootstrapInfo boot_info;
  shared_ptr<Tablet> tablet;
  ASSERT_STATUS_OK(BootstrapTestTablet(0, 0, &tablet, &boot_info));

  // Table should be empty because we didn't replay the REPLICATE
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(0, results.size());

  // The consensus bootstrap info should include the orphaned REPLICATE.
  ASSERT_EQ(1, boot_info.orphaned_replicates.size());
  ASSERT_STR_CONTAINS(boot_info.orphaned_replicates[0]->ShortDebugString(),
                      "this is a test mutate");

  // And it should also include the latest opids.
  EXPECT_EQ("term: 0 index: 0", boot_info.last_id.ShortDebugString());
}

// Bootstrap should fail if no ConsensusMetadata file exists.
TEST_F(BootstrapTest, TestMissingConsensusMetadata) {
  BuildLog();

  scoped_refptr<TabletMetadata> meta;
  ASSERT_OK(LoadTestTabletMetadata(-1, -1, &meta));

  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  Status s = RunBootstrapOnTestTablet(meta, &tablet, &boot_info);

  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "Unable to load Consensus metadata");
}

} // namespace tablet
} // namespace kudu
