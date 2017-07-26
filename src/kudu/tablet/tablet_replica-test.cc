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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/logical_clock.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/rpc/messenger.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/tablet_replica_mm_ops.h"
#include "kudu/tablet/transactions/transaction.h"
#include "kudu/tablet/transactions/transaction_driver.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/threadpool.h"

METRIC_DECLARE_entity(tablet);

DECLARE_int32(flush_threshold_mb);

namespace kudu {
namespace tablet {

using consensus::CommitMsg;
using consensus::ConsensusBootstrapInfo;
using consensus::ConsensusMetadata;
using consensus::ConsensusMetadataManager;
using consensus::OpId;
using consensus::RaftPeerPB;
using log::Log;
using log::LogOptions;
using rpc::Messenger;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;

static Schema GetTestSchema() {
  return Schema({ ColumnSchema("key", INT32) }, 1);
}

class TabletReplicaTest : public KuduTabletTest {
 public:
  TabletReplicaTest()
    : KuduTabletTest(GetTestSchema()),
      insert_counter_(0),
      delete_counter_(0) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTabletTest::SetUp();

    ASSERT_OK(ThreadPoolBuilder("prepare").Build(&prepare_pool_));
    ASSERT_OK(ThreadPoolBuilder("apply").Build(&apply_pool_));
    ASSERT_OK(ThreadPoolBuilder("raft").Build(&raft_pool_));

    rpc::MessengerBuilder builder(CURRENT_TEST_NAME());
    ASSERT_OK(builder.Build(&messenger_));

    metric_entity_ = METRIC_ENTITY_tablet.Instantiate(&metric_registry_, "test-tablet");

    RaftPeerPB config_peer;
    config_peer.set_permanent_uuid(tablet()->metadata()->fs_manager()->uuid());
    config_peer.mutable_last_known_addr()->set_host("0.0.0.0");
    config_peer.mutable_last_known_addr()->set_port(0);
    config_peer.set_member_type(RaftPeerPB::VOTER);

    scoped_refptr<ConsensusMetadataManager> cmeta_manager(
        new ConsensusMetadataManager(tablet()->metadata()->fs_manager()));

    // "Bootstrap" and start the TabletReplica.
    tablet_replica_.reset(
      new TabletReplica(tablet()->shared_metadata(),
                        cmeta_manager,
                        config_peer,
                        apply_pool_.get(),
                        Bind(&TabletReplicaTest::TabletReplicaStateChangedCallback,
                             Unretained(this),
                             tablet()->tablet_id())));

    // Make TabletReplica use the same LogAnchorRegistry as the Tablet created by the harness.
    // TODO(mpercy): Refactor TabletHarness to allow taking a
    // LogAnchorRegistry, while also providing TabletMetadata for consumption
    // by TabletReplica before Tablet is instantiated.
    tablet_replica_->log_anchor_registry_ = tablet()->log_anchor_registry_;

    RaftConfigPB config;
    config.add_peers()->CopyFrom(config_peer);
    config.set_opid_index(consensus::kInvalidOpIdIndex);

    scoped_refptr<ConsensusMetadata> cmeta;
    ASSERT_OK(cmeta_manager->Create(tablet()->tablet_id(), config, consensus::kMinimumTerm,
                                    &cmeta));
  }

  Status StartReplica(const ConsensusBootstrapInfo& info) {
    scoped_refptr<Log> log;
    RETURN_NOT_OK(Log::Open(LogOptions(), fs_manager(), tablet()->tablet_id(),
                            *tablet()->schema(), tablet()->metadata()->schema_version(),
                            metric_entity_.get(), &log));
    tablet_replica_->SetBootstrapping();
    return tablet_replica_->Start(info,
                                  tablet(),
                                  clock(),
                                  messenger_,
                                  scoped_refptr<rpc::ResultTracker>(),
                                  log,
                                  raft_pool_.get(),
                                  prepare_pool_.get());
  }

  Status StartReplicaAndWaitUntilLeader(const ConsensusBootstrapInfo& info) {
    RETURN_NOT_OK(StartReplica(info));
    const MonoDelta kTimeout = MonoDelta::FromSeconds(10);
    return tablet_replica_->consensus()->WaitUntilLeaderForTests(kTimeout);
  }

  void TabletReplicaStateChangedCallback(const string& tablet_id, const string& reason) {
    LOG(INFO) << "Tablet replica state changed for tablet " << tablet_id << ". Reason: " << reason;
  }

  virtual void TearDown() OVERRIDE {
    tablet_replica_->Shutdown();
    prepare_pool_->Shutdown();
    apply_pool_->Shutdown();
    KuduTabletTest::TearDown();
  }

 protected:
  // Generate monotonic sequence of key column integers.
  Status GenerateSequentialInsertRequest(WriteRequestPB* write_req) {
    Schema schema(GetTestSchema());
    write_req->set_tablet_id(tablet()->tablet_id());
    CHECK_OK(SchemaToPB(schema, write_req->mutable_schema()));

    KuduPartialRow row(&schema);
    CHECK_OK(row.SetInt32("key", insert_counter_++));

    RowOperationsPBEncoder enc(write_req->mutable_row_operations());
    enc.Add(RowOperationsPB::INSERT, row);
    return Status::OK();
  }

  // Generate monotonic sequence of deletions, starting with 0.
  // Will assert if you try to delete more rows than you inserted.
  Status GenerateSequentialDeleteRequest(WriteRequestPB* write_req) {
    CHECK_LT(delete_counter_, insert_counter_);
    Schema schema(GetTestSchema());
    write_req->set_tablet_id(tablet()->tablet_id());
    CHECK_OK(SchemaToPB(schema, write_req->mutable_schema()));

    KuduPartialRow row(&schema);
    CHECK_OK(row.SetInt32("key", delete_counter_++));

    RowOperationsPBEncoder enc(write_req->mutable_row_operations());
    enc.Add(RowOperationsPB::DELETE, row);
    return Status::OK();
  }

  Status ExecuteWriteAndRollLog(TabletReplica* tablet_replica, const WriteRequestPB& req) {
    gscoped_ptr<WriteResponsePB> resp(new WriteResponsePB());
    unique_ptr<WriteTransactionState> tx_state(new WriteTransactionState(tablet_replica,
                                                                         &req,
                                                                         nullptr, // No RequestIdPB
                                                                         resp.get()));

    CountDownLatch rpc_latch(1);
    tx_state->set_completion_callback(gscoped_ptr<TransactionCompletionCallback>(
        new LatchTransactionCompletionCallback<WriteResponsePB>(&rpc_latch, resp.get())));

    CHECK_OK(tablet_replica->SubmitWrite(std::move(tx_state)));
    rpc_latch.Wait();
    CHECK(!resp->has_error())
        << "\nReq:\n" << SecureDebugString(req) << "Resp:\n" << SecureDebugString(*resp);

    // Roll the log after each write.
    // Usually the append thread does the roll and no additional sync is required. However in
    // this test the thread that is appending is not the same thread that is rolling the log
    // so we must make sure the Log's queue is flushed before we roll or we might have a race
    // between the appender thread and the thread executing the test.
    CHECK_OK(tablet_replica->log_->WaitUntilAllFlushed());
    CHECK_OK(tablet_replica->log_->AllocateSegmentAndRollOver());
    return Status::OK();
  }

  // Execute insert requests and roll log after each one.
  Status ExecuteInsertsAndRollLogs(int num_inserts) {
    for (int i = 0; i < num_inserts; i++) {
      gscoped_ptr<WriteRequestPB> req(new WriteRequestPB());
      RETURN_NOT_OK(GenerateSequentialInsertRequest(req.get()));
      RETURN_NOT_OK(ExecuteWriteAndRollLog(tablet_replica_.get(), *req));
    }

    return Status::OK();
  }

  // Execute delete requests and roll log after each one.
  Status ExecuteDeletesAndRollLogs(int num_deletes) {
    for (int i = 0; i < num_deletes; i++) {
      gscoped_ptr<WriteRequestPB> req(new WriteRequestPB());
      CHECK_OK(GenerateSequentialDeleteRequest(req.get()));
      CHECK_OK(ExecuteWriteAndRollLog(tablet_replica_.get(), *req));
    }

    return Status::OK();
  }

  void AssertNoLogAnchors() {
    // Make sure that there are no registered anchors in the registry
    CHECK_EQ(0, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  }

  // Assert that the Log GC() anchor is earlier than the latest OpId in the Log.
  void AssertLogAnchorEarlierThanLogLatest() {
    log::RetentionIndexes retention = tablet_replica_->GetRetentionIndexes();
    OpId last_log_opid;
    tablet_replica_->log_->GetLatestEntryOpId(&last_log_opid);
    CHECK_LT(retention.for_durability, last_log_opid.index())
      << "Expected valid log anchor, got earliest opid: " << retention.for_durability
      << " (expected any value earlier than last log id: " << SecureShortDebugString(last_log_opid)
      << ")";
  }

  // We disable automatic log GC. Don't leak those changes.
  google::FlagSaver flag_saver_;

  int32_t insert_counter_;
  int32_t delete_counter_;
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  shared_ptr<Messenger> messenger_;
  gscoped_ptr<ThreadPool> prepare_pool_;
  gscoped_ptr<ThreadPool> apply_pool_;
  gscoped_ptr<ThreadPool> raft_pool_;

  // Must be destroyed before thread pools.
  scoped_refptr<TabletReplica> tablet_replica_;
};

// A Transaction that waits on the apply_continue latch inside of Apply().
class DelayedApplyTransaction : public WriteTransaction {
 public:
  DelayedApplyTransaction(CountDownLatch* apply_started,
                          CountDownLatch* apply_continue,
                          unique_ptr<WriteTransactionState> state)
      : WriteTransaction(std::move(state), consensus::LEADER),
        apply_started_(DCHECK_NOTNULL(apply_started)),
        apply_continue_(DCHECK_NOTNULL(apply_continue)) {
  }

  virtual Status Apply(gscoped_ptr<CommitMsg>* commit_msg) OVERRIDE {
    apply_started_->CountDown();
    LOG(INFO) << "Delaying apply...";
    apply_continue_->Wait();
    LOG(INFO) << "Apply proceeding";
    return WriteTransaction::Apply(commit_msg);
  }

 private:
  CountDownLatch* apply_started_;
  CountDownLatch* apply_continue_;
  DISALLOW_COPY_AND_ASSIGN(DelayedApplyTransaction);
};

// Ensure that Log::GC() doesn't delete logs when the MRS has an anchor.
TEST_F(TabletReplicaTest, TestMRSAnchorPreventsLogGC) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  Log* log = tablet_replica_->log_.get();
  int32_t num_gced;

  AssertNoLogAnchors();

  log::SegmentSequence segments;
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));

  ASSERT_EQ(1, segments.size());
  ASSERT_OK(ExecuteInsertsAndRollLogs(3));
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(4, segments.size());

  AssertLogAnchorEarlierThanLogLatest();
  ASSERT_GT(tablet_replica_->log_anchor_registry()->GetAnchorCountForTests(), 0);

  // Ensure nothing gets deleted.
  log::RetentionIndexes retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(0, num_gced) << "earliest needed: " << retention.for_durability;

  // Flush MRS as needed to ensure that we don't have OpId anchors in the MRS.
  tablet_replica_->tablet()->Flush();
  AssertNoLogAnchors();

  // The first two segments should be deleted.
  // The last is anchored due to the commit in the last segment being the last
  // OpId in the log.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(2, num_gced) << "earliest needed: " << retention.for_durability;
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(2, segments.size());
}

// Ensure that Log::GC() doesn't delete logs when the DMS has an anchor.
TEST_F(TabletReplicaTest, TestDMSAnchorPreventsLogGC) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  Log* log = tablet_replica_->log_.get();
  int32_t num_gced;

  AssertNoLogAnchors();

  log::SegmentSequence segments;
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));

  ASSERT_EQ(1, segments.size());
  ASSERT_OK(ExecuteInsertsAndRollLogs(2));
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(3, segments.size());

  // Flush MRS & GC log so the next mutation goes into a DMS.
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  log::RetentionIndexes retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  // We will only GC 1, and have 1 left because the earliest needed OpId falls
  // back to the latest OpId written to the Log if no anchors are set.
  ASSERT_EQ(1, num_gced);
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(2, segments.size());
  AssertNoLogAnchors();

  OpId id;
  log->GetLatestEntryOpId(&id);
  LOG(INFO) << "Before: " << SecureShortDebugString(id);


  // We currently have no anchors and the last operation in the log is 0.3
  // Before the below was ExecuteDeletesAndRollLogs(1) but that was breaking
  // what I think is a wrong assertion.
  // I.e. since 0.4 is the last operation that we know is in memory 0.4 is the
  // last anchor we expect _and_ it's the last op in the log.
  // Only if we apply two operations is the last anchored operation and the
  // last operation in the log different.

  // Execute a mutation.
  ASSERT_OK(ExecuteDeletesAndRollLogs(2));
  AssertLogAnchorEarlierThanLogLatest();
  ASSERT_GT(tablet_replica_->log_anchor_registry()->GetAnchorCountForTests(), 0);
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(4, segments.size());

  // Execute another couple inserts, but Flush it so it doesn't anchor.
  ASSERT_OK(ExecuteInsertsAndRollLogs(2));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(6, segments.size());

  // Ensure the delta and last insert remain in the logs, anchored by the delta.
  // Note that this will allow GC of the 2nd insert done above.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(1, num_gced);
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(5, segments.size());

  // Flush DMS to release the anchor.
  tablet_replica_->tablet()->FlushBiggestDMS();

  // Verify no anchors after Flush().
  AssertNoLogAnchors();

  // We should only hang onto one segment due to no anchors.
  // The last log OpId is the commit in the last segment, so it only anchors
  // that segment, not the previous, because it's not the first OpId in the
  // segment.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(3, num_gced);
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(2, segments.size());
}

// Ensure that Log::GC() doesn't compact logs with OpIds of active transactions.
TEST_F(TabletReplicaTest, TestActiveTransactionPreventsLogGC) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  Log* log = tablet_replica_->log_.get();
  int32_t num_gced;

  AssertNoLogAnchors();

  log::SegmentSequence segments;
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));

  ASSERT_EQ(1, segments.size());
  ASSERT_OK(ExecuteInsertsAndRollLogs(4));
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(5, segments.size());

  // Flush MRS as needed to ensure that we don't have OpId anchors in the MRS.
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  tablet_replica_->tablet()->Flush();

  // Verify no anchors after Flush().
  AssertNoLogAnchors();

  // Now create a long-lived Transaction that hangs during Apply().
  // Allow other transactions to go through. Logs should be populated, but the
  // long-lived Transaction should prevent the log from being deleted since it
  // is in-flight.
  CountDownLatch rpc_latch(1);
  CountDownLatch apply_started(1);
  CountDownLatch apply_continue(1);
  gscoped_ptr<WriteRequestPB> req(new WriteRequestPB());
  gscoped_ptr<WriteResponsePB> resp(new WriteResponsePB());
  {
    // Long-running mutation.
    ASSERT_OK(GenerateSequentialDeleteRequest(req.get()));
    unique_ptr<WriteTransactionState> tx_state(new WriteTransactionState(tablet_replica_.get(),
                                                                         req.get(),
                                                                         nullptr, // No RequestIdPB
                                                                         resp.get()));

    tx_state->set_completion_callback(gscoped_ptr<TransactionCompletionCallback>(
        new LatchTransactionCompletionCallback<WriteResponsePB>(&rpc_latch, resp.get())));

    gscoped_ptr<DelayedApplyTransaction> transaction(
        new DelayedApplyTransaction(&apply_started,
                                    &apply_continue,
                                    std::move(tx_state)));

    scoped_refptr<TransactionDriver> driver;
    ASSERT_OK(tablet_replica_->NewLeaderTransactionDriver(transaction.PassAs<Transaction>(),
                                                       &driver));

    ASSERT_OK(driver->ExecuteAsync());
    apply_started.Wait();
    ASSERT_TRUE(driver->GetOpId().IsInitialized())
      << "By the time a transaction is applied, it should have an Opid";
    // The apply will hang until we CountDown() the continue latch.
    // Now, roll the log. Below, we execute a few more insertions with rolling.
    ASSERT_OK(log->AllocateSegmentAndRollOver());
  }

  ASSERT_EQ(1, tablet_replica_->txn_tracker_.GetNumPendingForTests());
  // The log anchor is currently equal to the latest OpId written to the Log
  // because we are delaying the Commit message with the CountDownLatch.

  // GC the first four segments created by the inserts.
  log::RetentionIndexes retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(4, num_gced);
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(2, segments.size());

  // We use mutations here, since an MRS Flush() quiesces the tablet, and we
  // want to ensure the only thing "anchoring" is the TransactionTracker.
  ASSERT_OK(ExecuteDeletesAndRollLogs(3));
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(5, segments.size());
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  tablet_replica_->tablet()->FlushBiggestDMS();
  ASSERT_EQ(0, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_EQ(1, tablet_replica_->txn_tracker_.GetNumPendingForTests());

  AssertLogAnchorEarlierThanLogLatest();

  // Try to GC(), nothing should be deleted due to the in-flight transaction.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(0, num_gced);
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(5, segments.size());

  // Now we release the transaction and wait for everything to complete.
  // We fully quiesce and flush, which should release all anchors.
  ASSERT_EQ(1, tablet_replica_->txn_tracker_.GetNumPendingForTests());
  apply_continue.CountDown();
  rpc_latch.Wait();
  tablet_replica_->txn_tracker_.WaitForAllToFinish();
  ASSERT_EQ(0, tablet_replica_->txn_tracker_.GetNumPendingForTests());
  tablet_replica_->tablet()->FlushBiggestDMS();
  AssertNoLogAnchors();

  // All should be deleted except the two last segments.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(3, num_gced);
  ASSERT_OK(log->reader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(2, segments.size());
}

TEST_F(TabletReplicaTest, TestGCEmptyLog) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplica(info));
  // We don't wait on consensus on purpose.
  ASSERT_OK(tablet_replica_->RunLogGC());
}

TEST_F(TabletReplicaTest, TestFlushOpsPerfImprovements) {
  FLAGS_flush_threshold_mb = 64;

  MaintenanceOpStats stats;

  // Just on the threshold and not enough time has passed for a time-based flush.
  stats.set_ram_anchored(64 * 1024 * 1024);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 1);
  ASSERT_EQ(0.0, stats.perf_improvement());
  stats.Clear();

  // Just on the threshold and enough time has passed, we'll have a low improvement.
  stats.set_ram_anchored(64 * 1024 * 1024);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 3 * 60 * 1000);
  ASSERT_GT(stats.perf_improvement(), 0.01);
  stats.Clear();

  // Over the threshold, we expect improvement equal to the excess MB.
  stats.set_ram_anchored(128 * 1024 * 1024);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 1);
  ASSERT_NEAR(stats.perf_improvement(), 64, 0.01);
  stats.Clear();

  // Below the threshold but have been there a long time, closing in to 1.0.
  stats.set_ram_anchored(30 * 1024 * 1024);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 60 * 50 * 1000);
  ASSERT_LT(0.7, stats.perf_improvement());
  ASSERT_GT(1.0, stats.perf_improvement());
  stats.Clear();
}

} // namespace tablet
} // namespace kudu
