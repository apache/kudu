// Copyright (c) 2014 Cloudera, Inc.

#include <boost/assign/list_of.hpp>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/partial_row.h"
#include "common/timestamp.h"
#include "common/wire_protocol.h"
#include "common/wire_protocol-test-util.h"
#include "consensus/log.h"
#include "consensus/log_util.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "rpc/messenger.h"
#include "server/clock.h"
#include "server/logical_clock.h"
#include "tablet/transactions/transaction.h"
#include "tablet/transactions/write_transaction.h"
#include "tablet/tablet_peer.h"
#include "tablet/tablet-test-util.h"
#include "tserver/tserver.pb.h"
#include "util/metrics.h"
#include "util/test_util.h"
#include "util/test_macros.h"

DECLARE_bool(log_gc_enable);

namespace kudu {
namespace tablet {

using consensus::CommitMsg;
using consensus::Consensus;
using consensus::OpId;
using consensus::WRITE_OP;
using log::Log;
using log::LogOptions;
using log::MinimumOpId;
using log::OpIdAnchorRegistry;
using log::OpIdEquals;
using metadata::QuorumPeerPB;
using rpc::Messenger;
using server::Clock;
using server::LogicalClock;
using std::tr1::shared_ptr;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;

static Schema GetTestSchema() {
  return Schema(boost::assign::list_of(ColumnSchema("key", UINT32)), 1);
}

class TabletPeerTest : public KuduTabletTest {
 public:
  TabletPeerTest()
    : KuduTabletTest(GetTestSchema()),
      insert_counter_(0),
      delete_counter_(0) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTabletTest::SetUp();

    rpc::MessengerBuilder builder(CURRENT_TEST_NAME());
    ASSERT_STATUS_OK(builder.Build(&messenger_));

    // "Bootstrap" and start the TabletPeer.
    tablet_peer_.reset(new TabletPeer(*tablet_->metadata()));
    QuorumPeerPB quorum_peer;
    quorum_peer.set_permanent_uuid("test1");
    quorum_peer.set_role(QuorumPeerPB::LEADER);
    metric_ctx_.reset(new MetricContext(&metric_registry_, CURRENT_TEST_NAME()));

    gscoped_ptr<Log> log;
    ASSERT_STATUS_OK(Log::Open(LogOptions(), fs_manager_.get(), tablet_->tablet_id(),
                                metric_ctx_.get(), &log));
    ASSERT_STATUS_OK(tablet_peer_->Init(tablet_, clock_, messenger_, quorum_peer,
                                        log.Pass(), opid_anchor_registry_.get(), true));

    // Disable Log GC. We will call it manually.
    // This flag is restored by the FlagSaver member at destruction time.
    FLAGS_log_gc_enable = false;

    QuorumPB quorum;
    quorum.set_local(true);
    quorum.set_seqno(0);
    ASSERT_STATUS_OK(tablet_peer_->Start(quorum));
  }

  virtual void TearDown() OVERRIDE {
    tablet_peer_->Shutdown();
    KuduTabletTest::TearDown();
  }

 protected:
  // Generate monotonic sequence of key column integers.
  Status GenerateSequentialInsertRequest(WriteRequestPB* write_req) {
    Schema schema(GetTestSchema());
    write_req->set_tablet_id(tablet_->tablet_id());
    CHECK_OK(SchemaToPB(schema, write_req->mutable_schema()));

    PartialRow row(&schema);
    CHECK_OK(row.SetUInt32("key", insert_counter_++));

    RowOperationsPBEncoder enc(write_req->mutable_row_operations());
    enc.Add(RowOperationsPB::INSERT, row);
    return Status::OK();
  }

  // Generate monotonic sequence of deletions, starting with 0.
  // Will assert if you try to delete more rows than you inserted.
  Status GenerateSequentialDeleteRequest(WriteRequestPB* write_req) {
    CHECK_LT(delete_counter_, insert_counter_);
    Schema schema(GetTestSchema());
    write_req->set_tablet_id(tablet_->tablet_id());
    CHECK_OK(SchemaToPB(schema, write_req->mutable_schema()));

    PartialRow row(&schema);
    CHECK_OK(row.SetUInt32("key", delete_counter_++));

    RowOperationsPBEncoder enc(write_req->mutable_row_operations());
    enc.Add(RowOperationsPB::DELETE, row);
    return Status::OK();
  }

  Status ExecuteWriteAndRollLog(TabletPeer* tablet_peer, const WriteRequestPB& req) {
    WriteResponsePB resp;
    WriteTransactionState* tx_state =
        new WriteTransactionState(tablet_peer, &req, &resp);

    CountDownLatch rpc_latch(1);
    tx_state->set_completion_callback(gscoped_ptr<TransactionCompletionCallback>(
        new LatchTransactionCompletionCallback<WriteResponsePB>(&rpc_latch, &resp)).Pass());

    CHECK_OK(tablet_peer->SubmitWrite(tx_state));
    rpc_latch.Wait();
    CHECK(StatusFromPB(resp.error().status()).ok())
        << "\nReq:\n" << req.DebugString() << "Resp:\n" << resp.DebugString();

    // Roll the log after each write.
    CHECK_OK(tablet_peer->log_->RollOverForTests());
    return Status::OK();
  }

  // Execute insert requests and roll log after each one.
  Status ExecuteInsertsAndRollLogs(int num_inserts) {
    for (int i = 0; i < num_inserts; i++) {
      WriteRequestPB req;
      RETURN_NOT_OK(GenerateSequentialInsertRequest(&req));
      RETURN_NOT_OK(ExecuteWriteAndRollLog(tablet_peer_.get(), req));
    }

    return Status::OK();
  }

  // Execute delete requests and roll log after each one.
  Status ExecuteDeletesAndRollLogs(int num_deletes) {
    for (int i = 0; i < num_deletes; i++) {
      WriteRequestPB req;
      CHECK_OK(GenerateSequentialDeleteRequest(&req));
      CHECK_OK(ExecuteWriteAndRollLog(tablet_peer_.get(), req));
    }

    return Status::OK();
  }

  void AssertNoLogAnchors() {
    CHECK_EQ(0, tablet_peer_->opid_anchor_registry_->GetAnchorCountForTests());
    OpId earliest_opid;
    tablet_peer_->GetEarliestNeededOpId(&earliest_opid);
    OpId last_log_opid;
    CHECK_OK(tablet_peer_->log_->GetLastEntryOpId(&last_log_opid));
    CHECK(OpIdEquals(earliest_opid, last_log_opid))
      << "Found unexpected anchor: " << earliest_opid.ShortDebugString();
  }

  // Assert that the Log GC() anchor is earlier than the latest OpId in the Log.
  void AssertLogAnchorEarlierThanLogLatest() {
    OpId earliest_opid;
    tablet_peer_->GetEarliestNeededOpId(&earliest_opid);
    OpId last_log_opid;
    CHECK_OK(tablet_peer_->log_->GetLastEntryOpId(&last_log_opid));
    CHECK(!OpIdEquals(earliest_opid, last_log_opid))
      << "Expected valid log anchor, got last log opid: " << earliest_opid.ShortDebugString();
  }

  // We disable automatic log GC. Don't leak those changes.
  google::FlagSaver flag_saver_;

  uint32_t insert_counter_;
  uint32_t delete_counter_;
  MetricRegistry metric_registry_;
  gscoped_ptr<MetricContext> metric_ctx_;
  shared_ptr<Messenger> messenger_;
  shared_ptr<TabletPeer> tablet_peer_;
};

// A Transaction that waits on the apply_continue latch inside of Apply().
class DelayedApplyTransaction : public LeaderWriteTransaction {
 public:
  DelayedApplyTransaction(CountDownLatch* apply_started,
                          CountDownLatch* apply_continue,
                          TransactionTracker* txn_tracker,
                          WriteTransactionState* tx_state,
                          Consensus* consensus,
                          TaskExecutor* prepare_executor,
                          TaskExecutor* apply_executor,
                          simple_spinlock& prepare_replicate_lock)
    : LeaderWriteTransaction(txn_tracker,
                             tx_state,
                             consensus,
                             prepare_executor,
                             apply_executor,
                             &prepare_replicate_lock),
      apply_started_(DCHECK_NOTNULL(apply_started)),
      apply_continue_(DCHECK_NOTNULL(apply_continue)) {
  }

  virtual Status Apply() OVERRIDE {
    apply_started_->CountDown();
    apply_continue_->Wait();
    return LeaderWriteTransaction::Apply();
  }

 private:
  CountDownLatch* apply_started_;
  CountDownLatch* apply_continue_;
  DISALLOW_COPY_AND_ASSIGN(DelayedApplyTransaction);
};

// Ensure that Log::GC() doesn't delete logs when the MRS has an anchor.
TEST_F(TabletPeerTest, TestMRSAnchorPreventsLogGC) {
  Log* log = tablet_peer_->log_.get();
  OpId min_op_id;
  int32_t num_gced;

  AssertNoLogAnchors();

  ASSERT_EQ(0, log->PreviousSegmentsForTests().size());
  ASSERT_STATUS_OK(ExecuteInsertsAndRollLogs(3));
  ASSERT_EQ(3, log->PreviousSegmentsForTests().size());

  AssertLogAnchorEarlierThanLogLatest();
  CHECK_GT(tablet_peer_->opid_anchor_registry_->GetAnchorCountForTests(), 0);

  // Ensure nothing gets deleted.
  tablet_peer_->GetEarliestNeededOpId(&min_op_id);
  ASSERT_STATUS_OK(log->GC(min_op_id, &num_gced));
  ASSERT_EQ(0, num_gced);

  // Flush MRS as needed to ensure that we don't have OpId anchors in the MRS.
  tablet_peer_->tablet()->Flush();
  AssertNoLogAnchors();

  // The first two segments should be deleted.
  // The last is anchored due to the commit in the last segment being the last
  // OpId in the log.
  tablet_peer_->GetEarliestNeededOpId(&min_op_id);
  ASSERT_STATUS_OK(log->GC(min_op_id, &num_gced));
  ASSERT_EQ(2, num_gced);
  ASSERT_EQ(1, log->PreviousSegmentsForTests().size());
}

// Ensure that Log::GC() doesn't delete logs when the DMS has an anchor.
TEST_F(TabletPeerTest, TestDMSAnchorPreventsLogGC) {
  Log* log = tablet_peer_->log_.get();
  OpId min_op_id;
  int32_t num_gced;

  AssertNoLogAnchors();

  ASSERT_EQ(0, log->PreviousSegmentsForTests().size());
  ASSERT_STATUS_OK(ExecuteInsertsAndRollLogs(2));
  ASSERT_EQ(2, log->PreviousSegmentsForTests().size());

  // Flush MRS & GC log so the next mutation goes into a DMS.
  ASSERT_STATUS_OK(tablet_peer_->tablet()->Flush());
  tablet_peer_->GetEarliestNeededOpId(&min_op_id);
  ASSERT_STATUS_OK(log->GC(min_op_id, &num_gced));
  // We will only GC 1, and have 1 left because the earliest needed OpId falls
  // back to the latest OpId written to the Log if no anchors are set.
  ASSERT_EQ(1, num_gced);
  ASSERT_EQ(1, log->PreviousSegmentsForTests().size());
  AssertNoLogAnchors();

  // Execute a mutation.
  ASSERT_STATUS_OK(ExecuteDeletesAndRollLogs(1));
  AssertLogAnchorEarlierThanLogLatest();
  CHECK_GT(tablet_peer_->opid_anchor_registry_->GetAnchorCountForTests(), 0);
  ASSERT_EQ(2, log->PreviousSegmentsForTests().size());

  // Execute another couple inserts, but Flush it so it doesn't anchor.
  ASSERT_STATUS_OK(ExecuteInsertsAndRollLogs(2));
  ASSERT_STATUS_OK(tablet_peer_->tablet()->Flush());
  ASSERT_EQ(4, log->PreviousSegmentsForTests().size());

  // Ensure the delta and last insert remain in the logs, anchored by the delta.
  // Note that this will allow GC of the 2nd insert done above.
  tablet_peer_->GetEarliestNeededOpId(&min_op_id);
  ASSERT_STATUS_OK(log->GC(min_op_id, &num_gced));
  ASSERT_EQ(1, num_gced);
  ASSERT_EQ(3, log->PreviousSegmentsForTests().size());

  // Flush DMS to release the anchor.
  tablet_peer_->tablet()->FlushBiggestDMS();

  // Verify no anchors after Flush().
  AssertNoLogAnchors();

  // We should only hang onto one segment due to no anchors.
  // The last log OpId is the commit in the last segment, so it only anchors
  // that segment, not the previous, because it's not the first OpId in the
  // segment.
  tablet_peer_->GetEarliestNeededOpId(&min_op_id);
  ASSERT_STATUS_OK(log->GC(min_op_id, &num_gced));
  ASSERT_EQ(2, num_gced);
  ASSERT_EQ(1, log->PreviousSegmentsForTests().size());
}

// Ensure that Log::GC() doesn't compact logs with OpIds of active transactions.
TEST_F(TabletPeerTest, TestActiveTransactionPreventsLogGC) {
  Log* log = tablet_peer_->log_.get();
  OpId min_op_id;
  int32_t num_gced;

  AssertNoLogAnchors();

  ASSERT_EQ(0, log->PreviousSegmentsForTests().size());
  ASSERT_STATUS_OK(ExecuteInsertsAndRollLogs(4));
  ASSERT_EQ(4, log->PreviousSegmentsForTests().size());

  // Flush MRS as needed to ensure that we don't have OpId anchors in the MRS.
  ASSERT_EQ(1, tablet_peer_->opid_anchor_registry_->GetAnchorCountForTests());
  tablet_peer_->tablet()->Flush();

  // Verify no anchors after Flush().
  AssertNoLogAnchors();

  // Now create a long-lived Transaction that hangs during Apply().
  // Allow other transactions to go through. Logs should be populated, but the
  // long-lived Transaction should prevent the log from being deleted since it
  // is in-flight.
  CountDownLatch rpc_latch(1);
  CountDownLatch apply_started(1);
  CountDownLatch apply_continue(1);
  {
    WriteRequestPB req;
    // Long-running mutation.
    ASSERT_STATUS_OK(GenerateSequentialDeleteRequest(&req));
    WriteResponsePB resp;
    WriteTransactionState* tx_state =
      new WriteTransactionState(tablet_peer_.get(), &req, &resp);

    tx_state->set_completion_callback(gscoped_ptr<TransactionCompletionCallback>(
          new LatchTransactionCompletionCallback<WriteResponsePB>(&rpc_latch, &resp)).Pass());

    DelayedApplyTransaction* transaction = new DelayedApplyTransaction(
        &apply_started,
        &apply_continue,
        &tablet_peer_->txn_tracker_,
        tx_state,
        tablet_peer_->consensus(),
        tablet_peer_->prepare_executor_.get(),
        tablet_peer_->apply_executor_.get(),
        tablet_peer_->prepare_replicate_lock_);
    ASSERT_STATUS_OK(transaction->Execute());
    apply_started.Wait();
    // The apply will hang until we CountDown() the continue latch.
    // Now, roll the log. Below, we execute a few more insertions with rolling.
    ASSERT_STATUS_OK(log->RollOverForTests());
  }

  ASSERT_EQ(1, tablet_peer_->txn_tracker_.GetNumPendingForTests());
  // The log anchor is currently equal to the latest OpId written to the Log
  // because we are delaying the Commit message with the CountDownLatch.

  // GC the first three segments created by the inserts.
  tablet_peer_->GetEarliestNeededOpId(&min_op_id);
  ASSERT_STATUS_OK(log->GC(min_op_id, &num_gced));
  ASSERT_EQ(4, num_gced);
  ASSERT_EQ(1, log->PreviousSegmentsForTests().size());

  // We use mutations here, since an MRS Flush() quiesces the tablet, and we
  // want to ensure the only thing "anchoring" is the TransactionTracker.
  ASSERT_STATUS_OK(ExecuteDeletesAndRollLogs(3));
  ASSERT_EQ(4, log->PreviousSegmentsForTests().size());
  ASSERT_EQ(1, tablet_peer_->opid_anchor_registry_->GetAnchorCountForTests());
  tablet_peer_->tablet()->FlushBiggestDMS();
  ASSERT_EQ(0, tablet_peer_->opid_anchor_registry_->GetAnchorCountForTests());
  ASSERT_EQ(1, tablet_peer_->txn_tracker_.GetNumPendingForTests());

  AssertLogAnchorEarlierThanLogLatest();

  // Try to GC(), nothing should be deleted due to the in-flight transaction.
  tablet_peer_->GetEarliestNeededOpId(&min_op_id);
  ASSERT_STATUS_OK(log->GC(min_op_id, &num_gced));
  ASSERT_EQ(0, num_gced);
  ASSERT_EQ(4, log->PreviousSegmentsForTests().size());

  // Now we release the transaction and wait for everything to complete.
  // We fully quiesce and flush, which should release all anchors.
  ASSERT_EQ(1, tablet_peer_->txn_tracker_.GetNumPendingForTests());
  apply_continue.CountDown();
  rpc_latch.Wait();
  tablet_peer_->txn_tracker_.WaitForAllToFinish();
  ASSERT_EQ(0, tablet_peer_->txn_tracker_.GetNumPendingForTests());
  tablet_peer_->tablet()->FlushBiggestDMS();
  AssertNoLogAnchors();

  // All should be deleted except the last one, which is never GCed.
  tablet_peer_->GetEarliestNeededOpId(&min_op_id);
  ASSERT_STATUS_OK(log->GC(min_op_id, &num_gced));
  ASSERT_EQ(3, num_gced);
  ASSERT_EQ(1, log->PreviousSegmentsForTests().size());
}

} // namespace tablet
} // namespace kudu
