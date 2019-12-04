// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/tablet/tablet_bootstrap.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/logical_clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/log-test-base.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/logging_test_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

class MemTracker;

namespace tablet {

using clock::Clock;
using clock::LogicalClock;
using consensus::ConsensusBootstrapInfo;
using consensus::ConsensusMetadata;
using consensus::ConsensusMetadataManager;
using consensus::MakeOpId;
using consensus::OpId;
using consensus::ReplicateMsg;
using consensus::ReplicateRefPtr;
using consensus::kMinimumTerm;
using consensus::make_scoped_refptr_replicate;
using log::Log;
using log::LogAnchorRegistry;
using log::LogTestBase;
using pb_util::SecureShortDebugString;
using tserver::WriteRequestPB;

class BootstrapTest : public LogTestBase {
 protected:

  void SetUp() OVERRIDE {
    LogTestBase::SetUp();
    cmeta_manager_.reset(new ConsensusMetadataManager(fs_manager_.get()));
  }

  Status LoadTestTabletMetadata(int mrs_id, int delta_id, scoped_refptr<TabletMetadata>* meta) {
    Schema schema = SchemaBuilder(schema_).Build();
    std::pair<PartitionSchema, Partition> partition = CreateDefaultPartition(schema);

    RETURN_NOT_OK(TabletMetadata::LoadOrCreate(fs_manager_.get(),
                                               log::kTestTablet,
                                               log::kTestTable,
                                               log::kTestTableId,
                                               schema,
                                               partition.first,
                                               partition.second,
                                               TABLET_DATA_READY,
                                               /*tombstone_last_logged_opid=*/ boost::none,
                                               /*extra_config=*/ boost::none,
                                               /*dimension_label=*/ boost::none,
                                               meta));
    (*meta)->SetLastDurableMrsIdForTests(mrs_id);
    if ((*meta)->GetRowSetForTests(0) != nullptr) {
      (*meta)->GetRowSetForTests(0)->SetLastDurableRedoDmsIdForTests(delta_id);
    }
    return (*meta)->Flush();
  }

  Status CreateConsensusMetadata(const scoped_refptr<TabletMetadata>& meta) {
    consensus::RaftConfigPB config;
    config.set_opid_index(consensus::kInvalidOpIdIndex);
    consensus::RaftPeerPB* peer = config.add_peers();
    peer->set_permanent_uuid(meta->fs_manager()->uuid());
    peer->set_member_type(consensus::RaftPeerPB::VOTER);

    RETURN_NOT_OK_PREPEND(cmeta_manager_->Create(meta->tablet_id(), config, kMinimumTerm),
                          "Unable to create consensus metadata");

    return Status::OK();
  }

  Status PersistTestTabletMetadataState(TabletDataState state) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK(LoadTestTabletMetadata(-1, -1, &meta));
    meta->set_tablet_data_state(state);
    RETURN_NOT_OK(meta->Flush());
    return Status::OK();
  }

  Status RunBootstrapOnTestTablet(const scoped_refptr<TabletMetadata>& meta,
                                  shared_ptr<Tablet>* tablet,
                                  ConsensusBootstrapInfo* boot_info) {

    scoped_refptr<ConsensusMetadata> cmeta;
    RETURN_NOT_OK(cmeta_manager_->Load(meta->tablet_id(), &cmeta));

    scoped_refptr<LogAnchorRegistry> log_anchor_registry(new LogAnchorRegistry());
    // Now attempt to recover the log
    RETURN_NOT_OK(BootstrapTablet(
        meta,
        cmeta->CommittedConfig(),
        scoped_refptr<Clock>(LogicalClock::CreateStartingAt(Timestamp::kInitialTimestamp)),
        shared_ptr<MemTracker>(),
        scoped_refptr<rpc::ResultTracker>(),
        nullptr,
        nullptr, // no status listener
        tablet,
        &log_,
        log_anchor_registry,
        boot_info));

    return Status::OK();
  }

  Status BootstrapTestTablet(int mrs_id,
                             int delta_id,
                             shared_ptr<Tablet>* tablet,
                             ConsensusBootstrapInfo* boot_info) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK_PREPEND(LoadTestTabletMetadata(mrs_id, delta_id, &meta),
                          "Unable to load test tablet metadata");

    RETURN_NOT_OK(CreateConsensusMetadata(meta));

    RETURN_NOT_OK_PREPEND(RunBootstrapOnTestTablet(meta, tablet, boot_info),
                          "Unable to bootstrap test tablet");
    return Status::OK();
  }

  void IterateTabletRows(const Tablet* tablet,
                         vector<string>* results) {
    unique_ptr<RowwiseIterator> iter;
    RowIteratorOptions opts;
    opts.projection = &schema_;
    ASSERT_OK(tablet->NewRowIterator(std::move(opts), &iter));
    ASSERT_OK(iter->Init(nullptr));
    ASSERT_OK(IterateToStringList(iter.get(), results));
    for (const string& result : *results) {
      VLOG(1) << result;
    }
  }

  scoped_refptr<ConsensusMetadataManager> cmeta_manager_;
};

// Tests a normal bootstrap scenario
TEST_F(BootstrapTest, TestBootstrap) {
  ASSERT_OK(BuildLog());

  ASSERT_OK(AppendReplicateBatch(MakeOpId(1, current_index_)));
  ASSERT_OK(RollLog());

  ASSERT_OK(AppendCommit(MakeOpId(1, current_index_)));

  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  StringVectorSink capture_logs;
  {
    // Capture the log messages during bootstrap.
    ScopedRegisterSink reg(&capture_logs);
    ASSERT_OK(BootstrapTestTablet(-1, -1, &tablet, &boot_info));
  }

  // Make sure we don't see anything in the logs that would make a user scared.
  for (const string& s : capture_logs.logged_msgs()) {
    ASSERT_STR_NOT_MATCHES(s, "[cC]orrupt");
  }

  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());
}

// Test that we don't overflow opids. Regression test for KUDU-1933.
TEST_F(BootstrapTest, TestBootstrapHighOpIdIndex) {
  // Start appending with a log index 3 under the int32 max value.
  // Append 6 log entries, which will roll us right through the int32 max.
  const int64_t first_log_index = std::numeric_limits<int32_t>::max() - 3;
  const int kNumEntries = 6;
  ASSERT_OK(BuildLog());
  current_index_ = first_log_index;
  for (int i = 0; i < kNumEntries; i++) {
    AppendReplicateBatchAndCommitEntryPairsToLog(1);
  }

  // Kick off tablet bootstrap and ensure everything worked.
  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  ASSERT_OK(BootstrapTestTablet(-1, -1, &tablet, &boot_info));
  OpId last_opid;
  last_opid.set_term(1);
  last_opid.set_index(current_index_ - 1);
  ASSERT_OPID_EQ(last_opid, boot_info.last_id);
  ASSERT_OPID_EQ(last_opid, boot_info.last_committed_id);
}

// Tests attempting a local bootstrap of a tablet that was in the middle of a
// tablet copy before "crashing".
TEST_F(BootstrapTest, TestIncompleteTabletCopy) {
  ASSERT_OK(BuildLog());

  ASSERT_OK(PersistTestTabletMetadataState(TABLET_DATA_COPYING));
  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  fs_manager_->dd_manager()->DeleteDataDirGroup(log::kTestTablet);
  Status s = BootstrapTestTablet(-1, -1, &tablet, &boot_info);
  ASSERT_TRUE(s.IsCorruption()) << "Expected corruption: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "TabletMetadata bootstrap state is TABLET_DATA_COPYING");
  LOG(INFO) << "State is still TABLET_DATA_COPYING, as expected: " << s.ToString();
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
  ASSERT_OK(BuildLog());

  OpId opid = MakeOpId(1, current_index_);

  // Step 1) Write a REPLICATE to the log, and roll it.
  ASSERT_OK(AppendReplicateBatch(opid));
  ASSERT_OK(RollLog());

  // Step 2) Write the corresponding COMMIT in the second segment.
  ASSERT_OK(AppendCommit(opid));

  scoped_refptr<TabletMetadata> meta;
  ASSERT_OK(LoadTestTabletMetadata(/*mrs_id=*/ -1, /*delta_id=*/ -1, &meta));
  ASSERT_OK(CreateConsensusMetadata(meta));

  {
    shared_ptr<Tablet> tablet;
    ConsensusBootstrapInfo boot_info;

    // Step 3) Apply the operations in the log to the tablet and flush
    // the tablet to disk.
    ASSERT_OK(RunBootstrapOnTestTablet(meta, &tablet, &boot_info));
    ASSERT_OK(tablet->Flush());

    // Create a new log segment.
    ASSERT_OK(RollLog());

    // Step 4) Create an orphaned commit by first adding a commit to
    // the newly rolled logfile, and then by removing the previous
    // commits.
    ASSERT_OK(AppendCommit(opid));
    log::SegmentSequence segments;
    log_->reader()->GetSegmentsSnapshot(&segments);
    fs_manager_->env()->DeleteFile(segments[0]->path());

    // Untrack the tablet in the data dir manager so upon the next call to
    // BootstrapTestTablet, the tablet metadata's data dir group can be loaded.
    fs_manager_->dd_manager()->DeleteDataDirGroup(tablet->tablet_id());
  }
  {
    shared_ptr<Tablet> tablet;
    ConsensusBootstrapInfo boot_info;

    // Note: when GLOG_v=1, the test logs should include 'Ignoring
    // orphan commit: op_type: WRITE_OP...' line.
    ASSERT_OK(LoadTestTabletMetadata(/*mrs_id=*/ 2, /*delta_id=*/ 1, &meta));
    ASSERT_OK(RunBootstrapOnTestTablet(meta, &tablet, &boot_info));

    // Confirm that the legitimate data (from Step 3) is still there.
    vector<string> results;
    IterateTabletRows(tablet.get(), &results);
    ASSERT_EQ(1, results.size());
    ASSERT_EQ(R"((int32 key=1, int32 int_val=0, string string_val="this is a test insert"))",
              results[0]);
    ASSERT_EQ(2, tablet->metadata()->last_durable_mrs_id());
  }
}

// Regression test for KUDU-1477: we should successfully start up
// even if a pending commit contains only failed operations.
TEST_F(BootstrapTest, TestPendingFailedCommit) {
  ASSERT_OK(BuildLog());

  OpId opid_1 = MakeOpId(1, current_index_++);
  OpId opid_2 = MakeOpId(1, current_index_++);

  // Step 2) Write the corresponding COMMIT in the second segment,
  // with a status indicating that the writes had 'NotFound' results.
  ASSERT_OK(AppendReplicateBatch(opid_1));
  ASSERT_OK(AppendReplicateBatch(opid_2));
  ASSERT_OK(AppendCommitWithNotFoundOpResults(opid_2));

  {
    shared_ptr<Tablet> tablet;
    ConsensusBootstrapInfo boot_info;

    // Step 3) Apply the operations in the log to the tablet and flush
    // the tablet to disk.
    ASSERT_OK(BootstrapTestTablet(-1, -1, &tablet, &boot_info));
  }
}

// Tests this scenario:
// Orphan COMMIT with id <= current mrs id, followed by a REPLICATE
// message with mrs_id > current mrs_id, and a COMMIT message for that
// REPLICATE message.
//
// This should result in the orphan COMMIT being ignored, but the last
// REPLICATE/COMMIT messages ending up in the tablet.
TEST_F(BootstrapTest, TestNonOrphansAfterOrphanCommit) {
  ASSERT_OK(BuildLog());

  OpId opid = MakeOpId(1, current_index_);

  ASSERT_OK(AppendReplicateBatch(opid));
  ASSERT_OK(RollLog());

  ASSERT_OK(AppendCommit(opid));

  log::SegmentSequence segments;
  log_->reader()->GetSegmentsSnapshot(&segments);
  fs_manager_->env()->DeleteFile(segments[0]->path());

  current_index_ += 2;

  opid = MakeOpId(1, current_index_);

  ASSERT_OK(AppendReplicateBatch(opid));
  ASSERT_OK(AppendCommit(opid, 2, 1, 0));

  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  ASSERT_OK(BootstrapTestTablet(1, 0, &tablet, &boot_info));

  // Confirm that the legitimate data is there.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());

  // 'key=3' means the REPLICATE message was inserted when current_id_ was 3, meaning
  // that only the non-orphan commit went in.
  ASSERT_EQ(R"((int32 key=3, int32 int_val=0, string string_val="this is a test insert"))",
            results[0]);
}

// Test for where the server crashes in between REPLICATE and COMMIT.
// Bootstrap should not replay the operation, but should return it in
// the ConsensusBootstrapInfo
TEST_F(BootstrapTest, TestOrphanedReplicate) {
  ASSERT_OK(BuildLog());

  // Append a REPLICATE with no commit
  int replicate_index = current_index_++;

  OpId opid = MakeOpId(1, replicate_index);

  ASSERT_OK(AppendReplicateBatch(opid));

  // Bootstrap the tablet. It shouldn't replay anything.
  ConsensusBootstrapInfo boot_info;
  shared_ptr<Tablet> tablet;
  ASSERT_OK(BootstrapTestTablet(0, 0, &tablet, &boot_info));

  // Table should be empty because we didn't replay the REPLICATE
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(0, results.size());

  // The consensus bootstrap info should include the orphaned REPLICATE.
  ASSERT_EQ(1, boot_info.orphaned_replicates.size());
  ASSERT_STR_CONTAINS(SecureShortDebugString(*boot_info.orphaned_replicates[0]),
                      "this is a test mutate");

  // And it should also include the latest opids.
  EXPECT_EQ("term: 1 index: 1", SecureShortDebugString(boot_info.last_id));
}

// Bootstrap should fail if no ConsensusMetadata file exists.
TEST_F(BootstrapTest, TestMissingConsensusMetadata) {
  ASSERT_OK(BuildLog());

  scoped_refptr<TabletMetadata> meta;
  ASSERT_OK(LoadTestTabletMetadata(-1, -1, &meta));

  shared_ptr<Tablet> tablet;
  ConsensusBootstrapInfo boot_info;
  Status s = RunBootstrapOnTestTablet(meta, &tablet, &boot_info);

  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "Unable to load consensus metadata");
}

TEST_F(BootstrapTest, TestOperationOverwriting) {
  ASSERT_OK(BuildLog());

  OpId opid = MakeOpId(1, 1);

  // Append a replicate in term 1
  ASSERT_OK(AppendReplicateBatch(opid));

  // Append a commit for op 1.1
  ASSERT_OK(AppendCommit(opid));

  // Now append replicates for 4.2 and 4.3
  ASSERT_OK(AppendReplicateBatch(MakeOpId(4, 2)));
  ASSERT_OK(AppendReplicateBatch(MakeOpId(4, 3)));

  ASSERT_OK(RollLog());
  // And overwrite with 3.2
  ASSERT_OK(AppendReplicateBatch(MakeOpId(3, 2)));

  // When bootstrapping we should apply ops 1.1 and get 3.2 as pending.
  ConsensusBootstrapInfo boot_info;
  shared_ptr<Tablet> tablet;
  ASSERT_OK(BootstrapTestTablet(-1, -1, &tablet, &boot_info));

  ASSERT_EQ(boot_info.orphaned_replicates.size(), 1);
  ASSERT_OPID_EQ(boot_info.orphaned_replicates[0]->id(), MakeOpId(3, 2));

  // Confirm that the legitimate data is there.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());

  ASSERT_EQ(R"((int32 key=1, int32 int_val=0, string string_val="this is a test insert"))",
            results[0]);
}

// Tests that when we have out-of-order commits that touch the same rows, operations are
// still applied and in the correct order.
TEST_F(BootstrapTest, TestOutOfOrderCommits) {
  ASSERT_OK(BuildLog());

  consensus::ReplicateRefPtr replicate = consensus::make_scoped_refptr_replicate(
      new consensus::ReplicateMsg());
  replicate->get()->set_op_type(consensus::WRITE_OP);
  tserver::WriteRequestPB* batch_request = replicate->get()->mutable_write_request();
  ASSERT_OK(SchemaToPB(schema_, batch_request->mutable_schema()));
  batch_request->set_tablet_id(log::kTestTablet);

  // This appends Insert(1) with op 10.10
  OpId insert_opid = MakeOpId(10, 10);
  replicate->get()->mutable_id()->CopyFrom(insert_opid);
  replicate->get()->set_timestamp(clock_->Now().ToUint64());
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 10, 1,
                 "this is a test insert", batch_request->mutable_row_operations());
  ASSERT_OK(AppendReplicateBatch(replicate));

  // This appends Mutate(1) with op 10.11
  OpId mutate_opid = MakeOpId(10, 11);
  batch_request->mutable_row_operations()->Clear();
  replicate->get()->mutable_id()->CopyFrom(mutate_opid);
  replicate->get()->set_timestamp(clock_->Now().ToUint64());
  AddTestRowToPB(RowOperationsPB::UPDATE, schema_,
                 10, 2, "this is a test mutate",
                 batch_request->mutable_row_operations());
  ASSERT_OK(AppendReplicateBatch(replicate));

  // Now commit the mutate before the insert (in the log).
  gscoped_ptr<consensus::CommitMsg> mutate_commit(new consensus::CommitMsg);
  mutate_commit->set_op_type(consensus::WRITE_OP);
  mutate_commit->mutable_commited_op_id()->CopyFrom(mutate_opid);
  TxResultPB* result = mutate_commit->mutable_result();
  OperationResultPB* mutate = result->add_ops();
  MemStoreTargetPB* target = mutate->add_mutated_stores();
  target->set_mrs_id(1);

  ASSERT_OK(AppendCommit(std::move(mutate_commit)));

  gscoped_ptr<consensus::CommitMsg> insert_commit(new consensus::CommitMsg);
  insert_commit->set_op_type(consensus::WRITE_OP);
  insert_commit->mutable_commited_op_id()->CopyFrom(insert_opid);
  result = insert_commit->mutable_result();
  OperationResultPB* insert = result->add_ops();
  target = insert->add_mutated_stores();
  target->set_mrs_id(1);

  ASSERT_OK(AppendCommit(std::move(insert_commit)));

  ConsensusBootstrapInfo boot_info;
  shared_ptr<Tablet> tablet;
  ASSERT_OK(BootstrapTestTablet(-1, -1, &tablet, &boot_info));

  // Confirm that both operations were applied.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());

  ASSERT_EQ(R"((int32 key=10, int32 int_val=2, string string_val="this is a test mutate"))",
            results[0]);
}

// Tests that when we have two consecutive replicates but the commit message for the
// first one is missing, both appear as pending in ConsensusInfo.
TEST_F(BootstrapTest, TestMissingCommitMessage) {
  ASSERT_OK(BuildLog());

  consensus::ReplicateRefPtr replicate = consensus::make_scoped_refptr_replicate(
      new consensus::ReplicateMsg());
  replicate->get()->set_op_type(consensus::WRITE_OP);
  tserver::WriteRequestPB* batch_request = replicate->get()->mutable_write_request();
  ASSERT_OK(SchemaToPB(schema_, batch_request->mutable_schema()));
  batch_request->set_tablet_id(log::kTestTablet);

  // This appends Insert(1) with op 10.10
  OpId insert_opid = MakeOpId(10, 10);
  replicate->get()->mutable_id()->CopyFrom(insert_opid);
  replicate->get()->set_timestamp(clock_->Now().ToUint64());
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 10, 1,
                 "this is a test insert", batch_request->mutable_row_operations());
  ASSERT_OK(AppendReplicateBatch(replicate));

  // This appends Mutate(1) with op 10.11
  OpId mutate_opid = MakeOpId(10, 11);
  batch_request->mutable_row_operations()->Clear();
  replicate->get()->mutable_id()->CopyFrom(mutate_opid);
  replicate->get()->set_timestamp(clock_->Now().ToUint64());
  AddTestRowToPB(RowOperationsPB::UPDATE, schema_,
                 10, 2, "this is a test mutate",
                 batch_request->mutable_row_operations());
  ASSERT_OK(AppendReplicateBatch(replicate));

  // Now commit the mutate before the insert (in the log).
  gscoped_ptr<consensus::CommitMsg> mutate_commit(new consensus::CommitMsg);
  mutate_commit->set_op_type(consensus::WRITE_OP);
  mutate_commit->mutable_commited_op_id()->CopyFrom(mutate_opid);
  TxResultPB* result = mutate_commit->mutable_result();
  OperationResultPB* mutate = result->add_ops();
  MemStoreTargetPB* target = mutate->add_mutated_stores();
  target->set_mrs_id(1);

  ASSERT_OK(AppendCommit(std::move(mutate_commit)));

  ConsensusBootstrapInfo boot_info;
  shared_ptr<Tablet> tablet;
  ASSERT_OK(BootstrapTestTablet(-1, -1, &tablet, &boot_info));
  ASSERT_EQ(boot_info.orphaned_replicates.size(), 2);
  ASSERT_OPID_EQ(boot_info.last_committed_id, mutate_opid);

  // Confirm that no operation was applied.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(0, results.size());
}

// Test that we do not crash when a consensus-only operation has a timestamp
// that is higher than a timestamp assigned to a write operation that follows
// it in the log.
TEST_F(BootstrapTest, TestConsensusOnlyOperationOutOfOrderTimestamp) {
  ASSERT_OK(BuildLog());

  // Append NO_OP.
  ReplicateRefPtr noop_replicate = make_scoped_refptr_replicate(new ReplicateMsg());
  noop_replicate->get()->set_op_type(consensus::NO_OP);
  *noop_replicate->get()->mutable_id() = MakeOpId(1, 1);
  noop_replicate->get()->set_timestamp(2);
  noop_replicate->get()->mutable_noop_request()->set_timestamp_in_opid_order(false);

  ASSERT_OK(AppendReplicateBatch(noop_replicate));

  // Append WRITE_OP with higher OpId and lower timestamp.
  ReplicateRefPtr write_replicate = make_scoped_refptr_replicate(new ReplicateMsg());
  write_replicate->get()->set_op_type(consensus::WRITE_OP);
  WriteRequestPB* batch_request = write_replicate->get()->mutable_write_request();
  ASSERT_OK(SchemaToPB(schema_, batch_request->mutable_schema()));
  batch_request->set_tablet_id(log::kTestTablet);
  *write_replicate->get()->mutable_id() = MakeOpId(1, 2);
  write_replicate->get()->set_timestamp(1);
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 1, 1, "foo",
                 batch_request->mutable_row_operations());

  ASSERT_OK(AppendReplicateBatch(write_replicate));

  // Now commit in OpId order.
  // NO_OP...
  gscoped_ptr<consensus::CommitMsg> mutate_commit(new consensus::CommitMsg);
  mutate_commit->set_op_type(consensus::NO_OP);
  *mutate_commit->mutable_commited_op_id() = noop_replicate->get()->id();

  ASSERT_OK(AppendCommit(std::move(mutate_commit)));

  // ...and WRITE_OP...
  mutate_commit = gscoped_ptr<consensus::CommitMsg>(new consensus::CommitMsg);
  mutate_commit->set_op_type(consensus::WRITE_OP);
  *mutate_commit->mutable_commited_op_id() = write_replicate->get()->id();
  TxResultPB* result = mutate_commit->mutable_result();
  OperationResultPB* mutate = result->add_ops();
  MemStoreTargetPB* target = mutate->add_mutated_stores();
  target->set_mrs_id(1);

  ASSERT_OK(AppendCommit(std::move(mutate_commit)));

  ConsensusBootstrapInfo boot_info;
  shared_ptr<Tablet> tablet;
  ASSERT_OK(BootstrapTestTablet(-1, -1, &tablet, &boot_info));
  ASSERT_EQ(boot_info.orphaned_replicates.size(), 0);
  ASSERT_OPID_EQ(boot_info.last_committed_id, write_replicate->get()->id());

  // Confirm that the insert op was applied.
  vector<string> results;
  IterateTabletRows(tablet.get(), &results);
  ASSERT_EQ(1, results.size());
}

// Regression test for KUDU-2509. There was a use-after-free bug that sometimes
// lead to SIGSEGV while replaying the WAL. This scenario would crash or
// at least UB sanitizer would report a warning if such condition exists.
TEST_F(BootstrapTest, TestKudu2509) {
  ASSERT_OK(BuildLog());

  consensus::ReplicateRefPtr replicate = consensus::make_scoped_refptr_replicate(
      new consensus::ReplicateMsg());
  replicate->get()->set_op_type(consensus::WRITE_OP);
  tserver::WriteRequestPB* batch_request = replicate->get()->mutable_write_request();
  ASSERT_OK(SchemaToPB(schema_, batch_request->mutable_schema()));
  batch_request->set_tablet_id(log::kTestTablet);

  // This appends Insert(1) with op 10.10
  const OpId insert_opid = MakeOpId(10, 10);
  replicate->get()->mutable_id()->CopyFrom(insert_opid);
  replicate->get()->set_timestamp(clock_->Now().ToUint64());
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 10, 1,
                 "this is a test insert", batch_request->mutable_row_operations());
  ASSERT_OK(AppendReplicateBatch(replicate));

  // This appends Mutate(1) with op 10.11. The operation would try to update
  // a row having an extra column. This should fail since there hasn't been
  // corresponding DDL operation committed yet.
  const OpId mutate_opid = MakeOpId(10, 11);
  batch_request->mutable_row_operations()->Clear();
  replicate->get()->mutable_id()->CopyFrom(mutate_opid);
  replicate->get()->set_timestamp(clock_->Now().ToUint64());
  {
    // Modify the existing schema to add an extra row.
    SchemaBuilder builder(schema_);
    ASSERT_OK(builder.AddNullableColumn("string_val_extra", STRING));
    const auto schema = builder.BuildWithoutIds();
    ASSERT_OK(SchemaToPB(schema, batch_request->mutable_schema()));

    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("key", 100));
    ASSERT_OK(row.SetInt32("int_val", 200));
    ASSERT_OK(row.SetStringCopy("string_val", "300"));
    ASSERT_OK(row.SetStringCopy("string_val_extra", "100500"));
    RowOperationsPBEncoder enc(batch_request->mutable_row_operations());
    enc.Add(RowOperationsPB::UPDATE, row);
  }
  ASSERT_OK(AppendReplicateBatch(replicate));

  // Now commit the mutate before the insert (in the log).
  gscoped_ptr<consensus::CommitMsg> mutate_commit(new consensus::CommitMsg);
  mutate_commit->set_op_type(consensus::WRITE_OP);
  mutate_commit->mutable_commited_op_id()->CopyFrom(mutate_opid);
  mutate_commit->mutable_result()->add_ops()->add_mutated_stores()->set_mrs_id(1);
  ASSERT_OK(AppendCommit(std::move(mutate_commit)));

  gscoped_ptr<consensus::CommitMsg> insert_commit(new consensus::CommitMsg);
  insert_commit->set_op_type(consensus::WRITE_OP);
  insert_commit->mutable_commited_op_id()->CopyFrom(insert_opid);
  insert_commit->mutable_result()->add_ops()->add_mutated_stores()->set_mrs_id(1);
  ASSERT_OK(AppendCommit(std::move(insert_commit)));

  ConsensusBootstrapInfo boot_info;
  shared_ptr<Tablet> tablet;
  const auto s = BootstrapTestTablet(-1, -1, &tablet, &boot_info);
  const auto& status_msg = s.ToString();
  ASSERT_TRUE(s.IsInvalidArgument()) << status_msg;
  ASSERT_STR_CONTAINS(status_msg,
      "Unable to bootstrap test tablet: Failed log replay.");
  ASSERT_STR_CONTAINS(status_msg,
      "column string_val_extra STRING NULLABLE not present in tablet");
}

} // namespace tablet
} // namespace kudu
