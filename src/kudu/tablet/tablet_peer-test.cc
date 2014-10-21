// Copyright (c) 2014 Cloudera, Inc.

#include <boost/assign/list_of.hpp>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/partial_row.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/rpc/messenger.h"
#include "kudu/server/clock.h"
#include "kudu/server/logical_clock.h"
#include "kudu/tablet/transactions/transaction.h"
#include "kudu/tablet/transactions/transaction_driver.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_util.h"
#include "kudu/util/test_macros.h"

DECLARE_bool(enable_log_gc);

namespace kudu {
namespace tablet {

using consensus::ConsensusBootstrapInfo;
using consensus::CommitMsg;
using consensus::Consensus;
using consensus::ConsensusMetadata;
using consensus::MinimumOpId;
using consensus::OpId;
using consensus::OpIdEquals;
using consensus::WRITE_OP;
using log::Log;
using log::LogOptions;
using log::OpIdAnchorRegistry;
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

    ASSERT_STATUS_OK(ThreadPoolBuilder("ldr-apply").Build(&leader_apply_pool_));
    ASSERT_STATUS_OK(ThreadPoolBuilder("repl-apply").Build(&replica_apply_pool_));

    rpc::MessengerBuilder builder(CURRENT_TEST_NAME());
    ASSERT_STATUS_OK(builder.Build(&messenger_));


    metric_ctx_.reset(new MetricContext(&metric_registry_, CURRENT_TEST_NAME()));

    // "Bootstrap" and start the TabletPeer.
    tablet_peer_.reset(
      new TabletPeer(make_scoped_refptr(tablet()->metadata()),
                     leader_apply_pool_.get(),
                     replica_apply_pool_.get(),
                     boost::bind(&TabletPeerTest::TabletPeerStateChangedCallback, this, _1)));

    QuorumPeerPB quorum_peer;
    quorum_peer.set_permanent_uuid(tablet()->metadata()->fs_manager()->uuid());
    quorum_peer.set_role(QuorumPeerPB::CANDIDATE);
    QuorumPB quorum;
    quorum.set_local(true);
    quorum.set_seqno(consensus::kUninitializedQuorumSeqNo);
    quorum.add_peers()->CopyFrom(quorum_peer);

    gscoped_ptr<ConsensusMetadata> cmeta;
    ASSERT_OK(ConsensusMetadata::Create(tablet()->metadata()->fs_manager(),
                                        tablet()->tablet_id(), quorum,
                                        consensus::kMinimumTerm, &cmeta));

    gscoped_ptr<Log> log;
    ASSERT_STATUS_OK(Log::Open(LogOptions(), fs_manager(), tablet()->tablet_id(),
                               *tablet()->schema(),
                               metric_ctx_.get(), &log));

    ASSERT_STATUS_OK(tablet_peer_->Init(tablet(), clock(), messenger_, log.Pass(), *metric_ctx_));

    // Disable Log GC. We will call it manually.
    // This flag is restored by the FlagSaver member at destruction time.
    FLAGS_enable_log_gc = false;
  }

  Status StartPeer(const ConsensusBootstrapInfo& info) {
    RETURN_NOT_OK(tablet_peer_->Start(info));

    RETURN_NOT_OK(tablet_peer_->WaitUntilRunning(MonoDelta::FromSeconds(10)));

    // As we execute a change config txn on tablet peer start we need to
    // also wait for the transaction to be cleaned up so that we don't
    // have any explicit or implicit anchors when tests start.
    tablet_peer_->txn_tracker_.WaitForAllToFinish();

    return Status::OK();
  }

  void TabletPeerStateChangedCallback(TabletPeer* tablet_peer) {
    LOG(INFO) << "Tablet peer state changed.";
  }

  virtual void TearDown() OVERRIDE {
    tablet_peer_->Shutdown();
    leader_apply_pool_->Shutdown();
    replica_apply_pool_->Shutdown();
    KuduTabletTest::TearDown();
  }

 protected:
  // Generate monotonic sequence of key column integers.
  Status GenerateSequentialInsertRequest(WriteRequestPB* write_req) {
    Schema schema(GetTestSchema());
    write_req->set_tablet_id(tablet()->tablet_id());
    CHECK_OK(SchemaToPB(schema, write_req->mutable_schema()));

    KuduPartialRow row(&schema);
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
    write_req->set_tablet_id(tablet()->tablet_id());
    CHECK_OK(SchemaToPB(schema, write_req->mutable_schema()));

    KuduPartialRow row(&schema);
    CHECK_OK(row.SetUInt32("key", delete_counter_++));

    RowOperationsPBEncoder enc(write_req->mutable_row_operations());
    enc.Add(RowOperationsPB::DELETE, row);
    return Status::OK();
  }

  Status ExecuteWriteAndRollLog(TabletPeer* tablet_peer, const WriteRequestPB& req) {
    gscoped_ptr<WriteResponsePB> resp(new WriteResponsePB());
    WriteTransactionState* tx_state =
        new WriteTransactionState(tablet_peer, &req, resp.get());

    CountDownLatch rpc_latch(1);
    tx_state->set_completion_callback(gscoped_ptr<TransactionCompletionCallback>(
        new LatchTransactionCompletionCallback<WriteResponsePB>(&rpc_latch, resp.get())).Pass());

    CHECK_OK(tablet_peer->SubmitWrite(tx_state));
    rpc_latch.Wait();
    CHECK(StatusFromPB(resp->error().status()).ok())
        << "\nReq:\n" << req.DebugString() << "Resp:\n" << resp->DebugString();

    // Roll the log after each write.
    CHECK_OK(tablet_peer->log_->AllocateSegmentAndRollOver());
    return Status::OK();
  }

  // Execute insert requests and roll log after each one.
  Status ExecuteInsertsAndRollLogs(int num_inserts) {
    for (int i = 0; i < num_inserts; i++) {
      gscoped_ptr<WriteRequestPB> req(new WriteRequestPB());
      RETURN_NOT_OK(GenerateSequentialInsertRequest(req.get()));
      RETURN_NOT_OK(ExecuteWriteAndRollLog(tablet_peer_.get(), *req));
    }

    return Status::OK();
  }

  // Execute delete requests and roll log after each one.
  Status ExecuteDeletesAndRollLogs(int num_deletes) {
    for (int i = 0; i < num_deletes; i++) {
      gscoped_ptr<WriteRequestPB> req(new WriteRequestPB());
      CHECK_OK(GenerateSequentialDeleteRequest(req.get()));
      CHECK_OK(ExecuteWriteAndRollLog(tablet_peer_.get(), *req));
    }

    return Status::OK();
  }

  void AssertNoLogAnchors() {
    // Make sure that there are no registered anchors in the registry
    CHECK_EQ(0, tablet()->opid_anchor_registry()->GetAnchorCountForTests());
    OpId earliest_opid;
    // And that there are no in-flight transactions (which are implicit
    // anchors) by comparing the TabletPeer's earliest needed OpId and the last
    // entry in the log; if they match there is nothing in flight.
    tablet_peer_->GetEarliestNeededOpId(&earliest_opid);
    OpId last_log_opid;
    CHECK_OK(tablet_peer_->log_->GetLastEntryOpId(&last_log_opid));
    CHECK(OpIdEquals(earliest_opid, last_log_opid))
      << "Found unexpected anchor: " << earliest_opid.ShortDebugString()
      << " Last log entry: " << last_log_opid.ShortDebugString();
  }

  // Assert that the Log GC() anchor is earlier than the latest OpId in the Log.
  void AssertLogAnchorEarlierThanLogLatest() {
    OpId earliest_opid;
    tablet_peer_->GetEarliestNeededOpId(&earliest_opid);
    OpId last_log_opid;
    CHECK_OK(tablet_peer_->log_->GetLastEntryOpId(&last_log_opid));
    CHECK(OpIdCompare(earliest_opid, last_log_opid) < 0)
      << "Expected valid log anchor, got earliest opid: " << earliest_opid.ShortDebugString()
      << " (expected any value earlier than last log id: " << last_log_opid.ShortDebugString()
      << ")";
  }

  // We disable automatic log GC. Don't leak those changes.
  google::FlagSaver flag_saver_;

  uint32_t insert_counter_;
  uint32_t delete_counter_;
  MetricRegistry metric_registry_;
  gscoped_ptr<MetricContext> metric_ctx_;
  shared_ptr<Messenger> messenger_;
  scoped_refptr<TabletPeer> tablet_peer_;
  gscoped_ptr<ThreadPool> leader_apply_pool_;
  gscoped_ptr<ThreadPool> replica_apply_pool_;
};

// A Transaction that waits on the apply_continue latch inside of Apply().
class DelayedApplyTransaction : public WriteTransaction {
 public:
  DelayedApplyTransaction(CountDownLatch* apply_started,
                          CountDownLatch* apply_continue,
                          WriteTransactionState* state)
      : WriteTransaction(state, consensus::LEADER),
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
TEST_F(TabletPeerTest, TestMRSAnchorPreventsLogGC) {
  ConsensusBootstrapInfo info;
  ASSERT_STATUS_OK(StartPeer(info));

  Log* log = tablet_peer_->log_.get();
  OpId min_op_id;
  int32_t num_gced;

  AssertNoLogAnchors();

  log::SegmentSequence segments;
  ASSERT_STATUS_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));

  ASSERT_EQ(1, segments.size());
  ASSERT_STATUS_OK(ExecuteInsertsAndRollLogs(3));
  ASSERT_STATUS_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(4, segments.size());

  AssertLogAnchorEarlierThanLogLatest();
  CHECK_GT(tablet()->opid_anchor_registry()->GetAnchorCountForTests(), 0);

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
  ASSERT_STATUS_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(2, segments.size());
}

// Ensure that Log::GC() doesn't delete logs when the DMS has an anchor.
TEST_F(TabletPeerTest, TestDMSAnchorPreventsLogGC) {
  ConsensusBootstrapInfo info;
  ASSERT_STATUS_OK(StartPeer(info));

  Log* log = tablet_peer_->log_.get();
  OpId min_op_id;
  int32_t num_gced;

  AssertNoLogAnchors();

  log::SegmentSequence segments;
  ASSERT_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));

  ASSERT_EQ(1, segments.size());
  ASSERT_STATUS_OK(ExecuteInsertsAndRollLogs(2));
  ASSERT_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(3, segments.size());

  // Flush MRS & GC log so the next mutation goes into a DMS.
  ASSERT_STATUS_OK(tablet_peer_->tablet()->Flush());
  tablet_peer_->GetEarliestNeededOpId(&min_op_id);
  ASSERT_STATUS_OK(log->GC(min_op_id, &num_gced));
  // We will only GC 1, and have 1 left because the earliest needed OpId falls
  // back to the latest OpId written to the Log if no anchors are set.
  ASSERT_EQ(1, num_gced);
  ASSERT_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(2, segments.size());
  AssertNoLogAnchors();

  OpId id;
  log->GetLastEntryOpId(&id);
  LOG(INFO) << "Before: " << id.ShortDebugString();


  // We currently have no anchors and the last operation in the log is 0.3
  // Before the below was ExecuteDeletesAndRollLogs(1) but that was breaking
  // what I think is a wrong assertion.
  // I.e. since 0.4 is the last operation that we know is in memory 0.4 is the
  // last anchor we expect _and_ it's the last op in the log.
  // Only if we apply two operations is the last anchored operation and the
  // last operation in the log different.

  // Execute a mutation.
  ASSERT_STATUS_OK(ExecuteDeletesAndRollLogs(2));
  AssertLogAnchorEarlierThanLogLatest();
  CHECK_GT(tablet()->opid_anchor_registry()->GetAnchorCountForTests(), 0);
  ASSERT_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(4, segments.size());

  // Execute another couple inserts, but Flush it so it doesn't anchor.
  ASSERT_STATUS_OK(ExecuteInsertsAndRollLogs(2));
  ASSERT_STATUS_OK(tablet_peer_->tablet()->Flush());
  ASSERT_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(6, segments.size());

  // Ensure the delta and last insert remain in the logs, anchored by the delta.
  // Note that this will allow GC of the 2nd insert done above.
  tablet_peer_->GetEarliestNeededOpId(&min_op_id);
  ASSERT_STATUS_OK(log->GC(min_op_id, &num_gced));
  ASSERT_EQ(1, num_gced);
  ASSERT_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(5, segments.size());

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
  ASSERT_EQ(3, num_gced);
  ASSERT_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(2, segments.size());
}

// Ensure that Log::GC() doesn't compact logs with OpIds of active transactions.
TEST_F(TabletPeerTest, TestActiveTransactionPreventsLogGC) {
  ConsensusBootstrapInfo info;
  ASSERT_STATUS_OK(StartPeer(info));

  Log* log = tablet_peer_->log_.get();
  OpId min_op_id;
  int32_t num_gced;

  AssertNoLogAnchors();

  log::SegmentSequence segments;
  ASSERT_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));

  ASSERT_EQ(1, segments.size());
  ASSERT_STATUS_OK(ExecuteInsertsAndRollLogs(4));
  ASSERT_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(5, segments.size());

  // Flush MRS as needed to ensure that we don't have OpId anchors in the MRS.
  ASSERT_EQ(1, tablet()->opid_anchor_registry()->GetAnchorCountForTests());
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
  gscoped_ptr<WriteResponsePB> resp(new WriteResponsePB());
  {
    gscoped_ptr<WriteRequestPB> req(new WriteRequestPB());
    // Long-running mutation.
    ASSERT_STATUS_OK(GenerateSequentialDeleteRequest(req.get()));
    WriteTransactionState* tx_state =
      new WriteTransactionState(tablet_peer_.get(), req.get(), resp.get());

    tx_state->set_completion_callback(gscoped_ptr<TransactionCompletionCallback>(
          new LatchTransactionCompletionCallback<WriteResponsePB>(&rpc_latch, resp.get())).Pass());

    DelayedApplyTransaction* transaction = new DelayedApplyTransaction(&apply_started,
                                                                       &apply_continue,
                                                                       tx_state);

    scoped_refptr<TransactionDriver> driver;
    tablet_peer_->NewLeaderTransactionDriver(transaction, &driver);

    ASSERT_STATUS_OK(driver->ExecuteAsync());
    apply_started.Wait();
    ASSERT_TRUE(driver->GetOpId().IsInitialized())
      << "By the time a transaction is applied, it should have an Opid";
    // The apply will hang until we CountDown() the continue latch.
    // Now, roll the log. Below, we execute a few more insertions with rolling.
    ASSERT_STATUS_OK(log->AllocateSegmentAndRollOver());
  }

  ASSERT_EQ(1, tablet_peer_->txn_tracker_.GetNumPendingForTests());
  // The log anchor is currently equal to the latest OpId written to the Log
  // because we are delaying the Commit message with the CountDownLatch.

  // GC the first four segments created by the inserts.
  tablet_peer_->GetEarliestNeededOpId(&min_op_id);
  ASSERT_STATUS_OK(log->GC(min_op_id, &num_gced));
  ASSERT_EQ(4, num_gced);
  ASSERT_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(2, segments.size());

  // We use mutations here, since an MRS Flush() quiesces the tablet, and we
  // want to ensure the only thing "anchoring" is the TransactionTracker.
  ASSERT_STATUS_OK(ExecuteDeletesAndRollLogs(3));
  ASSERT_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(5, segments.size());
  ASSERT_EQ(1, tablet()->opid_anchor_registry()->GetAnchorCountForTests());
  tablet_peer_->tablet()->FlushBiggestDMS();
  ASSERT_EQ(0, tablet()->opid_anchor_registry()->GetAnchorCountForTests());
  ASSERT_EQ(1, tablet_peer_->txn_tracker_.GetNumPendingForTests());

  AssertLogAnchorEarlierThanLogLatest();

  // Try to GC(), nothing should be deleted due to the in-flight transaction.
  tablet_peer_->GetEarliestNeededOpId(&min_op_id);
  ASSERT_STATUS_OK(log->GC(min_op_id, &num_gced));
  ASSERT_EQ(0, num_gced);
  ASSERT_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(5, segments.size());

  // Now we release the transaction and wait for everything to complete.
  // We fully quiesce and flush, which should release all anchors.
  ASSERT_EQ(1, tablet_peer_->txn_tracker_.GetNumPendingForTests());
  apply_continue.CountDown();
  rpc_latch.Wait();
  tablet_peer_->txn_tracker_.WaitForAllToFinish();
  ASSERT_EQ(0, tablet_peer_->txn_tracker_.GetNumPendingForTests());
  tablet_peer_->tablet()->FlushBiggestDMS();
  AssertNoLogAnchors();

  // All should be deleted except the two last segments.
  tablet_peer_->GetEarliestNeededOpId(&min_op_id);
  ASSERT_STATUS_OK(log->GC(min_op_id, &num_gced));
  ASSERT_EQ(3, num_gced);
  ASSERT_OK(log->GetLogReader()->GetSegmentsSnapshot(&segments));
  ASSERT_EQ(2, segments.size());
}

} // namespace tablet
} // namespace kudu
