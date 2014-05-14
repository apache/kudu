// Copyright (c) 2014, Cloudera, inc.

#include "consensus/log-test-base.h"

#include <vector>

#include "common/iterator.h"
#include "consensus/log_util.h"
#include "tablet/tablet_bootstrap.h"
#include "tablet/tablet-test-util.h"
#include "server/logical_clock.h"

namespace kudu {

namespace log {

extern const char* kTestTable;
extern const char* kTestTablet;

} // namespace log

namespace tablet {

using std::vector;
using std::string;

using log::Log;
using log::LogTestBase;
using log::OpIdAnchorRegistry;
using log::ReadableLogSegmentMap;
using log::TabletMasterBlockPB;
using server::Clock;
using server::LogicalClock;

class BootstrapTest : public LogTestBase {
 protected:

  void SetUp() {
    LogTestBase::SetUp();
    master_block_.set_table_id(log::kTestTable);
    master_block_.set_tablet_id(log::kTestTablet);
    master_block_.set_block_a(fs_manager_->GenerateName());
    master_block_.set_block_b(fs_manager_->GenerateName());
  }

  Status BootstrapTestTablet(int mrs_id,
                             int delta_id,
                             shared_ptr<Tablet>* tablet) {
    metadata::QuorumPB quorum;
    quorum.set_seqno(0);

    gscoped_ptr<metadata::TabletMetadata> meta;
    scoped_refptr<OpIdAnchorRegistry> new_anchor_registry;

    RETURN_NOT_OK(metadata::TabletMetadata::LoadOrCreate(fs_manager_.get(),
                                                         master_block_,
                                                         log::kTestTable,
                                                         // We need a schema with ids for
                                                         // TabletMetadata::LoadOrCreate()
                                                         SchemaBuilder(schema_).Build(),
                                                         quorum,
                                                         "",
                                                         "",
                                                         &meta));
    meta->SetLastDurableMrsIdForTests(mrs_id);
    if (meta->GetRowSetForTests(0) != NULL) {
      meta->GetRowSetForTests(0)->SetLastDurableRedoDmsIdForTests(delta_id);
    }
    meta->Flush();

    gscoped_ptr<Log> new_log;
    gscoped_ptr<TabletStatusListener> listener(new TabletStatusListener(*meta));

    // Now attempt to recover the log
    RETURN_NOT_OK(BootstrapTablet(
        meta.Pass(),
        scoped_refptr<Clock>(LogicalClock::CreateStartingAt(Timestamp::kInitialTimestamp)),
        NULL,
        listener.get(),
        tablet,
        &new_log,
        &new_anchor_registry));
    log_.reset(new_log.release());

    return Status::OK();
  }

  TabletMasterBlockPB master_block_;
};

// Tests a normal bootstrap scenario
TEST_F(BootstrapTest, TestBootstrap) {
  BuildLog();

  AppendReplicateBatch(current_id_);
  ASSERT_STATUS_OK(RollLog());

  AppendCommit(current_id_ + 1, current_id_);

  shared_ptr<Tablet> tablet;
  ASSERT_STATUS_OK(BootstrapTestTablet(-1, -1, &tablet));

  gscoped_ptr<RowwiseIterator> iter;
  ASSERT_STATUS_OK(tablet->NewRowIterator(schema_, &iter));
  vector<string> results;
  ASSERT_STATUS_OK(iter->Init(NULL));
  ASSERT_STATUS_OK(IterateToStringList(iter.get(), &results));
  ASSERT_EQ(1, results.size());
}

// Tests the KUDU-141 scenario
TEST_F(BootstrapTest, TestOrphanCommit) {
  BuildLog();

  // Write something to the log log.
  AppendReplicateBatch(current_id_);
  ASSERT_STATUS_OK(RollLog());

  AppendCommit(current_id_ + 1, current_id_);

  shared_ptr<Tablet> tablet;
  ASSERT_STATUS_OK(BootstrapTestTablet(-1, -1, &tablet));

  ASSERT_STATUS_OK(tablet->Flush());

  ASSERT_STATUS_OK(RollLog());

  // Create an orphanned commit by first adding a commit a newly
  // rolled logfile, and then by removing the previous commits.
  AppendCommit(current_id_ + 1, current_id_);
  ReadableLogSegmentMap segments;
  log_->GetReadableLogSegments(&segments);
  fs_manager_->env()->DeleteFile(segments.begin()->second->path());

  // Note: when GLOG_v=1, the test logs should include 'Ignoring
  // orphan commit: op_type: WRITE_OP...' line.
  ASSERT_STATUS_OK(BootstrapTestTablet(2, 1, &tablet));

  // Confirm that the legitimate data is there.
  gscoped_ptr<RowwiseIterator> iter;
  ASSERT_STATUS_OK(tablet->NewRowIterator(
      schema_, MvccSnapshot::CreateSnapshotIncludingAllTransactions(), &iter));
  vector<string> results;
  ASSERT_STATUS_OK(iter->Init(NULL));
  ASSERT_STATUS_OK(IterateToStringList(iter.get(), &results));
  BOOST_FOREACH(string& result, results) {
    VLOG(1) << result;
  }
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

  AppendCommit(current_id_ + 1, current_id_);

  ReadableLogSegmentMap segments;
  log_->GetReadableLogSegments(&segments);
  fs_manager_->env()->DeleteFile(segments.begin()->second->path());

  current_id_ += 2;

  AppendReplicateBatch(current_id_);
  AppendCommit(current_id_ + 1, current_id_, 2, 1, 0);

  shared_ptr<Tablet> tablet;
  ASSERT_STATUS_OK(BootstrapTestTablet(1, 0, &tablet));

  // Confirm that the legitimate data is there.
  gscoped_ptr<RowwiseIterator> iter;
  ASSERT_STATUS_OK(tablet->NewRowIterator(
      schema_, MvccSnapshot::CreateSnapshotIncludingAllTransactions(), &iter));
  vector<string> results;
  ASSERT_STATUS_OK(iter->Init(NULL));
  ASSERT_STATUS_OK(IterateToStringList(iter.get(), &results));
  BOOST_FOREACH(string& result, results) {
    VLOG(1) << result;
  }
  ASSERT_EQ(1, results.size());

  // 'key=2' means the REPLICATE message was inserted when current_id_ was 2, meaning
  // that only the non-orphan commit went in.
  ASSERT_EQ("(uint32 key=2, uint32 int_val=0, string string_val=this is a test insert)",
            results[0]);
}

} // namespace tablet
} // namespace kudu
