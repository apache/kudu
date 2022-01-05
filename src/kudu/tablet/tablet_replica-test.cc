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

#include "kudu/tablet/tablet_replica.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/ops/alter_schema_op.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/ops/op_driver.h"
#include "kudu/tablet/ops/op_tracker.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica-test-base.h"
#include "kudu/tablet/tablet_replica_mm_ops.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(enable_maintenance_manager);
DECLARE_int32(flush_threshold_mb);
DECLARE_int32(tablet_history_max_age_sec);

METRIC_DECLARE_entity(tablet);

METRIC_DECLARE_gauge_uint64(live_row_count);

using kudu::consensus::CommitMsg;
using kudu::consensus::ConsensusBootstrapInfo;
using kudu::consensus::OpId;
using kudu::consensus::RECEIVED_OPID;
using kudu::consensus::RaftConsensus;
using kudu::log::Log;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::tserver::AlterSchemaRequestPB;
using kudu::tserver::AlterSchemaResponsePB;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;
using std::shared_ptr;
using std::string;
using std::unique_ptr;

namespace kudu {

namespace tablet {

static Schema GetTestSchema() {
  return Schema({ ColumnSchema("key", INT32) }, 1);
}

class TabletReplicaTest : public TabletReplicaTestBase {
 public:
  TabletReplicaTest()
      : TabletReplicaTestBase(GetTestSchema()),
        insert_counter_(0),
        delete_counter_(0) {
  }

 protected:
  // Generate monotonic sequence of key column integers.
  Status GenerateSequentialInsertRequest(const Schema& schema,
                                         WriteRequestPB* write_req) {
    write_req->set_tablet_id(tablet()->tablet_id());
    RETURN_NOT_OK(SchemaToPB(schema, write_req->mutable_schema()));

    KuduPartialRow row(&schema);
    for (int i = 0; i < schema.num_columns(); i++) {
      RETURN_NOT_OK(row.SetInt32(i, insert_counter_++));
    }

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

  Status UpdateSchema(const SchemaPB& schema, int schema_version) {
    AlterSchemaRequestPB alter;
    alter.set_dest_uuid(tablet()->metadata()->fs_manager()->uuid());
    alter.set_tablet_id(tablet()->tablet_id());
    alter.set_schema_version(schema_version);
    *alter.mutable_schema() = schema;
    return ExecuteAlter(tablet_replica_.get(), alter);
  }

  Status ExecuteAlter(TabletReplica* replica, const AlterSchemaRequestPB& req) {
    unique_ptr<AlterSchemaResponsePB> resp(new AlterSchemaResponsePB());
    unique_ptr<AlterSchemaOpState> op_state(
        new AlterSchemaOpState(replica, &req, resp.get()));
    CountDownLatch rpc_latch(1);
    op_state->set_completion_callback(unique_ptr<OpCompletionCallback>(
          new LatchOpCompletionCallback<AlterSchemaResponsePB>(&rpc_latch, resp.get())));
    RETURN_NOT_OK(replica->SubmitAlterSchema(std::move(op_state)));
    rpc_latch.Wait();
    CHECK(!resp->has_error())
        << "\nReq:\n" << SecureDebugString(req) << "Resp:\n" << SecureDebugString(*resp);
    return Status::OK();
  }

  static Status RollLog(TabletReplica* replica) {
    RETURN_NOT_OK(replica->log_->WaitUntilAllFlushed());
    return replica->log_->AllocateSegmentAndRollOverForTests();
  }

  Status ExecuteWriteAndRollLog(TabletReplica* tablet_replica, const WriteRequestPB& req) {
    RETURN_NOT_OK(ExecuteWrite(tablet_replica, req));

    // Roll the log after each write.
    // Usually the append thread does the roll and no additional sync is required. However in
    // this test the thread that is appending is not the same thread that is rolling the log
    // so we must make sure the Log's queue is flushed before we roll or we might have a race
    // between the appender thread and the thread executing the test.
    CHECK_OK(RollLog(tablet_replica));
    return Status::OK();
  }

  // Execute insert requests and roll log after each one.
  Status ExecuteInsertsAndRollLogs(int num_inserts) {
    for (int i = 0; i < num_inserts; i++) {
      WriteRequestPB req;
      RETURN_NOT_OK(GenerateSequentialInsertRequest(GetTestSchema(), &req));
      RETURN_NOT_OK(ExecuteWriteAndRollLog(tablet_replica_.get(), req));
    }
    return Status::OK();
  }

  // Execute delete requests and roll log after each one.
  Status ExecuteDeletesAndRollLogs(int num_deletes) {
    for (int i = 0; i < num_deletes; i++) {
      WriteRequestPB req;
      RETURN_NOT_OK(GenerateSequentialDeleteRequest(&req));
      RETURN_NOT_OK(ExecuteWriteAndRollLog(tablet_replica_.get(), req));
    }
    return Status::OK();
  }

  // Assert that there are no log anchors held on the tablet replica.
  //
  // NOTE: when an op finishes and notifies the completion callback, it still is
  // registered with the op tracker for a very short time before being
  // destructed. So, this should always be called with an ASSERT_EVENTUALLY wrapper.
  void AssertNoLogAnchors() {
    // Make sure that there are no registered anchors in the registry
    ASSERT_EQ(0, tablet_replica()->log_anchor_registry()->GetAnchorCountForTests());
  }

  // Assert that the Log GC() anchor is earlier than the latest OpId in the Log.
  void AssertLogAnchorEarlierThanLogLatest() {
    log::RetentionIndexes retention = tablet_replica_->GetRetentionIndexes();
    boost::optional<OpId> last_log_opid = tablet_replica_->consensus()->GetLastOpId(RECEIVED_OPID);
    ASSERT_NE(boost::none, last_log_opid);
    ASSERT_LT(retention.for_durability, last_log_opid->index())
      << "Expected valid log anchor, got earliest opid: " << retention.for_durability
      << " (expected any value earlier than last log id: " << SecureShortDebugString(*last_log_opid)
      << ")";
  }

  // We disable automatic log GC. Don't leak those changes.
  google::FlagSaver flag_saver_;

  int32_t insert_counter_;
  int32_t delete_counter_;
};

// A Op that waits on the apply_continue latch inside of Apply().
class DelayedApplyOp : public WriteOp {
 public:
  DelayedApplyOp(CountDownLatch* apply_started,
                 CountDownLatch* apply_continue,
                 unique_ptr<WriteOpState> state)
      : WriteOp(std::move(state), consensus::LEADER),
        apply_started_(DCHECK_NOTNULL(apply_started)),
        apply_continue_(DCHECK_NOTNULL(apply_continue)) {
  }

  virtual Status Apply(CommitMsg** commit_msg) override {
    apply_started_->CountDown();
    LOG(INFO) << "Delaying apply...";
    apply_continue_->Wait();
    LOG(INFO) << "Apply proceeding";
    return WriteOp::Apply(commit_msg);
  }

 private:
  CountDownLatch* apply_started_;
  CountDownLatch* apply_continue_;
  DISALLOW_COPY_AND_ASSIGN(DelayedApplyOp);
};

// Ensure that Log::GC() doesn't delete logs when the MRS has an anchor.
TEST_F(TabletReplicaTest, TestMRSAnchorPreventsLogGC) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  Log* log = tablet_replica_->log_.get();
  int32_t num_gced;

  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  log::SegmentSequence segments;
  log->reader()->GetSegmentsSnapshot(&segments);

  ASSERT_EQ(1, segments.size());
  ASSERT_OK(ExecuteInsertsAndRollLogs(3));
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(4, segments.size());

  NO_FATALS(AssertLogAnchorEarlierThanLogLatest());
  ASSERT_GT(tablet_replica_->log_anchor_registry()->GetAnchorCountForTests(), 0);

  // Ensure nothing gets deleted.
  log::RetentionIndexes retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(0, num_gced) << "earliest needed: " << retention.for_durability;

  // Flush MRS as needed to ensure that we don't have OpId anchors in the MRS.
  tablet_replica_->tablet()->Flush();
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  // The first two segments should be deleted.
  // The last is anchored due to the commit in the last segment being the last
  // OpId in the log.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(2, num_gced) << "earliest needed: " << retention.for_durability;
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(2, segments.size());
}

// Ensure that Log::GC() doesn't delete logs when the DMS has an anchor.
TEST_F(TabletReplicaTest, TestDMSAnchorPreventsLogGC) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  Log* log = tablet_replica_->log_.get();
  shared_ptr<RaftConsensus> consensus = tablet_replica_->shared_consensus();
  int32_t num_gced;

  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  log::SegmentSequence segments;
  log->reader()->GetSegmentsSnapshot(&segments);

  ASSERT_EQ(1, segments.size());
  ASSERT_OK(ExecuteInsertsAndRollLogs(2));
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(3, segments.size());

  // Flush MRS & GC log so the next mutation goes into a DMS.
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });
  log::RetentionIndexes retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  // We will only GC 1, and have 1 left because the earliest needed OpId falls
  // back to the latest OpId written to the Log if no anchors are set.
  ASSERT_EQ(1, num_gced);
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(2, segments.size());

  boost::optional<OpId> id = consensus->GetLastOpId(consensus::RECEIVED_OPID);
  ASSERT_NE(boost::none, id);
  LOG(INFO) << "Before: " << *id;

  // We currently have no anchors and the last operation in the log is 0.3
  // Before the below was ExecuteDeletesAndRollLogs(1) but that was breaking
  // what I think is a wrong assertion.
  // I.e. since 0.4 is the last operation that we know is in memory 0.4 is the
  // last anchor we expect _and_ it's the last op in the log.
  // Only if we apply two operations is the last anchored operation and the
  // last operation in the log different.

  // Execute a mutation.
  ASSERT_OK(ExecuteDeletesAndRollLogs(2));
  NO_FATALS(AssertLogAnchorEarlierThanLogLatest());
  ASSERT_GT(tablet_replica_->log_anchor_registry()->GetAnchorCountForTests(), 0);
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(4, segments.size());

  // Execute another couple inserts, but Flush it so it doesn't anchor.
  ASSERT_OK(ExecuteInsertsAndRollLogs(2));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(6, segments.size());

  // Ensure the delta and last insert remain in the logs, anchored by the delta.
  // Note that this will allow GC of the 2nd insert done above.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(1, num_gced);
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(5, segments.size());

  // Flush DMS to release the anchor.
  tablet_replica_->tablet()->FlushBiggestDMS();

  // Verify no anchors after Flush().
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  // We should only hang onto one segment due to no anchors.
  // The last log OpId is the commit in the last segment, so it only anchors
  // that segment, not the previous, because it's not the first OpId in the
  // segment.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(3, num_gced);
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(2, segments.size());
}

// Ensure that Log::GC() doesn't compact logs with OpIds of active ops.
TEST_F(TabletReplicaTest, TestActiveOpPreventsLogGC) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  Log* log = tablet_replica_->log_.get();
  int32_t num_gced;

  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  log::SegmentSequence segments;
  log->reader()->GetSegmentsSnapshot(&segments);

  ASSERT_EQ(1, segments.size());
  ASSERT_OK(ExecuteInsertsAndRollLogs(4));
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(5, segments.size());

  // Flush MRS as needed to ensure that we don't have OpId anchors in the MRS.
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  tablet_replica_->tablet()->Flush();

  // Verify no anchors after Flush().
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  // Now create a long-lived op that hangs during Apply().
  // Allow other ops to go through. Logs should be populated, but the
  // long-lived op should prevent the log from being deleted since it
  // is in-flight.
  CountDownLatch rpc_latch(1);
  CountDownLatch apply_started(1);
  CountDownLatch apply_continue(1);
  unique_ptr<WriteRequestPB> req(new WriteRequestPB());
  unique_ptr<WriteResponsePB> resp(new WriteResponsePB());
  {
    // Long-running mutation.
    ASSERT_OK(GenerateSequentialDeleteRequest(req.get()));
    unique_ptr<WriteOpState> op_state(new WriteOpState(tablet_replica_.get(),
                                                       req.get(),
                                                       nullptr, // No RequestIdPB
                                                       resp.get()));

    op_state->set_completion_callback(unique_ptr<OpCompletionCallback>(
        new LatchOpCompletionCallback<WriteResponsePB>(&rpc_latch, resp.get())));

    unique_ptr<DelayedApplyOp> op(
        new DelayedApplyOp(&apply_started,
                           &apply_continue,
                           std::move(op_state)));

    scoped_refptr<OpDriver> driver;
    ASSERT_OK(tablet_replica_->NewLeaderOpDriver(std::move(op),
                                                 &driver));
    driver->ExecuteAsync();
    apply_started.Wait();
    ASSERT_TRUE(driver->GetOpId().IsInitialized())
      << "By the time an op is applied, it should have an Opid";
    // The apply will hang until we CountDown() the continue latch.
    // Now, roll the log. Below, we execute a few more insertions with rolling.
    ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  }

  ASSERT_EQ(1, tablet_replica_->op_tracker_.GetNumPendingForTests());
  // The log anchor is currently equal to the latest OpId written to the Log
  // because we are delaying the Commit message with the CountDownLatch.

  // GC the first four segments created by the inserts.
  log::RetentionIndexes retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(4, num_gced);
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(2, segments.size());

  // We use mutations here, since an MRS Flush() quiesces the tablet, and we
  // want to ensure the only thing "anchoring" is the OpTracker.
  ASSERT_OK(ExecuteDeletesAndRollLogs(3));
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(5, segments.size());
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  tablet_replica_->tablet()->FlushBiggestDMS();

  ASSERT_EVENTUALLY([&]{
      AssertNoLogAnchors();
      ASSERT_EQ(1, tablet_replica_->op_tracker_.GetNumPendingForTests());
    });

  NO_FATALS(AssertLogAnchorEarlierThanLogLatest());

  // Try to GC(), nothing should be deleted due to the in-flight op.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(0, num_gced);
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(5, segments.size());

  // Now we release the op and wait for everything to complete.
  // We fully quiesce and flush, which should release all anchors.
  ASSERT_EQ(1, tablet_replica_->op_tracker_.GetNumPendingForTests());
  apply_continue.CountDown();
  rpc_latch.Wait();
  tablet_replica_->op_tracker_.WaitForAllToFinish();
  ASSERT_EQ(0, tablet_replica_->op_tracker_.GetNumPendingForTests());
  tablet_replica_->tablet()->FlushBiggestDMS();
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  // All should be deleted except the two last segments.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(3, num_gced);
  log->reader()->GetSegmentsSnapshot(&segments);
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

  // Just on the threshold and not enough time has passed for a time-based flush,
  // we'll expect improvement equal to '1'.
  stats.set_ram_anchored(64 * 1024 * 1024);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 1);
  ASSERT_EQ(1.0, stats.perf_improvement());
  stats.Clear();

  // Below the threshold and enough time has passed, we'll have a low improvement.
  stats.set_ram_anchored(2 * 1024 * 1024);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 3 * 60 * 1000);
  ASSERT_LT(0.01, stats.perf_improvement());
  ASSERT_GT(0.1, stats.perf_improvement());
  stats.Clear();

  // Over the threshold, we expect improvement equal to the excess MB.
  stats.set_ram_anchored(128 * 1024 * 1024);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 1);
  ASSERT_NEAR(stats.perf_improvement(), 64, 0.01);
  stats.Clear();

  // Below the threshold but have been there a long time, closing in to 1.0.
  stats.set_ram_anchored(1);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 60 * 50 * 1000);
  ASSERT_LT(0.7, stats.perf_improvement());
  ASSERT_GT(1.0, stats.perf_improvement());
  stats.Clear();

  // Approaching threshold, enough time has passed but haven't been there a long time,
  // closing in to 1.0.
  stats.set_ram_anchored(63 * 1024 * 1024);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 3 * 60 * 1000);
  ASSERT_LT(0.9, stats.perf_improvement());
  ASSERT_GT(1.0, stats.perf_improvement());
  stats.Clear();
}

// Test that the schema of a tablet will be rolled forward upon replaying an
// alter schema request.
TEST_F(TabletReplicaTest, TestRollLogSegmentSchemaOnAlter) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  SchemaPB orig_schema_pb;
  ASSERT_OK(SchemaToPB(SchemaBuilder(*tablet()->metadata()->schema()).Build(), &orig_schema_pb));
  const int orig_schema_version = tablet()->metadata()->schema_version();

  // Add a new column.
  SchemaBuilder builder(*tablet()->metadata()->schema());
  ASSERT_OK(builder.AddColumn("new_col", INT32));
  Schema new_client_schema = builder.BuildWithoutIds();
  SchemaPB new_schema;
  ASSERT_OK(SchemaToPB(builder.Build(), &new_schema));
  ASSERT_OK(UpdateSchema(new_schema, orig_schema_version + 1));

  const auto write = [&] {
    unique_ptr<WriteRequestPB> req(new WriteRequestPB());
    ASSERT_OK(GenerateSequentialInsertRequest(new_client_schema, req.get()));
    ASSERT_OK(ExecuteWrite(tablet_replica_.get(), *req));
  };
  // Upon restarting, our log segment header schema should have "new_col".
  NO_FATALS(write());
  ASSERT_OK(RestartReplica());

  // Get rid of the alter in the WALs.
  NO_FATALS(write());
  ASSERT_OK(RollLog(tablet_replica_.get()));
  NO_FATALS(write());
  tablet_replica_->tablet()->Flush();
  ASSERT_OK(tablet_replica_->RunLogGC());

  // Now write some more and restart. If our segment header schema previously
  // didn't have "new_col", bootstrapping would fail, complaining about a
  // mismatch between the segment header schema and the write request schema.
  NO_FATALS(write());
  ASSERT_OK(RestartReplica());
}

// Regression test for KUDU-2690, wherein a alter schema request that failed
// (e.g. because of an invalid schema) would roll forward the log segment
// header schema, causing a failure or crash upon bootstrapping.
TEST_F(TabletReplicaTest, Kudu2690Test) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  SchemaPB orig_schema_pb;
  ASSERT_OK(SchemaToPB(SchemaBuilder(*tablet()->metadata()->schema()).Build(), &orig_schema_pb));
  const int orig_schema_version = tablet()->metadata()->schema_version();

  // First things first, add a new column.
  SchemaBuilder builder(*tablet()->metadata()->schema());
  ASSERT_OK(builder.AddColumn("new_col", INT32));
  Schema new_client_schema = builder.BuildWithoutIds();
  SchemaPB new_schema;
  ASSERT_OK(SchemaToPB(builder.Build(), &new_schema));
  ASSERT_OK(UpdateSchema(new_schema, orig_schema_version + 1));

  // Try to update the schema to an older version. Before the fix for
  // KUDU-2690, this would revert the schema in the next log segment header
  // upon rolling the log below.
  ASSERT_OK(UpdateSchema(orig_schema_pb, orig_schema_version));

  // Roll onto a new segment so we can begin filling a new segment. This allows
  // us to GC the first segment.
  ASSERT_OK(RollLog(tablet_replica_.get()));
  {
    unique_ptr<WriteRequestPB> req(new WriteRequestPB());
    ASSERT_OK(GenerateSequentialInsertRequest(new_client_schema, req.get()));
    ASSERT_OK(ExecuteWrite(tablet_replica_.get(), *req));
  }
  ASSERT_OK(tablet_replica_->RunLogGC());

  // Before KUDU-2960 was fixed, bootstrapping would fail, complaining that the
  // write requests contained a column that was not in the log segment header's
  // schema.
  ASSERT_OK(RestartReplica());
}

TEST_F(TabletReplicaTest, TestLiveRowCountMetric) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  // We don't care what the function is, since the metric is already instantiated.
  auto live_row_count = METRIC_live_row_count.InstantiateFunctionGauge(
      tablet_replica_->tablet()->GetMetricEntity(), [](){ return 0; });
  ASSERT_EQ(0, live_row_count->value());

  // Insert some rows.
  Random rand(SeedRandom());
  const int kNumInsert = rand.Next() % 100 + 1;
  ASSERT_OK(ExecuteInsertsAndRollLogs(kNumInsert));
  ASSERT_EQ(kNumInsert, live_row_count->value());

  // Delete some rows.
  const int kNumDelete = rand.Next() % kNumInsert;
  ASSERT_OK(ExecuteDeletesAndRollLogs(kNumDelete));
  ASSERT_EQ(kNumInsert - kNumDelete, live_row_count->value());
}

TEST_F(TabletReplicaTest, TestRestartAfterGCDeletedRowsets) {
  FLAGS_enable_maintenance_manager = false;
  FLAGS_tablet_history_max_age_sec = 1;
  const int kNumRows = 10;
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  auto* tablet = tablet_replica_->tablet();
  // Metrics are already registered so pass a dummy lambda.
  auto live_row_count = METRIC_live_row_count.InstantiateFunctionGauge(
      tablet->GetMetricEntity(), [] () { return 0; });

  // Insert some rows and flush so we get a DRS, and then delete them so we
  // have an ancient, fully deleted DRS.
  ASSERT_OK(ExecuteInsertsAndRollLogs(kNumRows));
  ASSERT_OK(tablet->Flush());
  ASSERT_OK(ExecuteDeletesAndRollLogs(kNumRows));
  ASSERT_EQ(1, tablet->num_rowsets());
  ASSERT_EQ(0, live_row_count->value());
  SleepFor(MonoDelta::FromSeconds(FLAGS_tablet_history_max_age_sec));

  // Insert some fresh rows so we can validate that we don't GC everything.
  ASSERT_OK(ExecuteInsertsAndRollLogs(kNumRows));
  ASSERT_OK(tablet->Flush());
  ASSERT_EQ(2, tablet->num_rowsets());
  ASSERT_EQ(kNumRows, live_row_count->value());

  // Now GC what we can. The first rowset should be gone.
  ASSERT_OK(tablet->DeleteAncientDeletedRowsets());
  ASSERT_EQ(1, tablet->num_rowsets());
  ASSERT_EQ(kNumRows, live_row_count->value());
  ASSERT_OK(ExecuteDeletesAndRollLogs(kNumRows));
  ASSERT_EQ(0, live_row_count->value());

  // Restart and ensure we can rebuild our DMS okay.
  ASSERT_OK(RestartReplica());
  tablet = tablet_replica_->tablet();
  ASSERT_EQ(1, tablet->num_rowsets());
  live_row_count = METRIC_live_row_count.InstantiateFunctionGauge(
      tablet->GetMetricEntity(), [] () { return 0; });
  ASSERT_EQ(0, live_row_count->value());

  // Now do that again but with deltafiles.
  ASSERT_OK(tablet->FlushBiggestDMS());
  ASSERT_OK(RestartReplica());
  tablet = tablet_replica_->tablet();
  ASSERT_EQ(1, tablet->num_rowsets());

  // Wait for our deleted rowset to become ancient. Since we just started up,
  // we shouldn't have read any delta stats, so running the GC won't pick up
  // our deleted DRS.
  SleepFor(MonoDelta::FromSeconds(FLAGS_tablet_history_max_age_sec));
  ASSERT_OK(tablet->DeleteAncientDeletedRowsets());
  ASSERT_EQ(1, tablet->num_rowsets());
}

} // namespace tablet
} // namespace kudu
