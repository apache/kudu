// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <tr1/memory>

#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/opid_anchor_registry.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(enable_data_block_fsync);
DECLARE_int32(consensus_max_batch_size_bytes);
DECLARE_int32(consensus_entry_cache_size_soft_limit_mb);
DECLARE_int32(consensus_entry_cache_size_hard_limit_mb);
DECLARE_int32(global_consensus_entry_cache_size_soft_limit_mb);
DECLARE_int32(global_consensus_entry_cache_size_hard_limit_mb);

using std::tr1::shared_ptr;

namespace kudu {
namespace consensus {

static const char* kPeerUuid = "a";
static const char* kTestTablet = "test-tablet";

class ConsensusQueueTest : public KuduTest {
 public:
  ConsensusQueueTest()
      : schema_(GetSimpleTestSchema()),
        metric_context_(&metric_registry_, "queue-test"),
        registry_(new log::OpIdAnchorRegistry) {
    FLAGS_enable_data_block_fsync = false; // Keep unit tests fast.
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    fs_manager_.reset(new FsManager(env_.get(), test_dir_));
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());
    CHECK_OK(log::Log::Open(log::LogOptions(),
                            fs_manager_.get(),
                            kTestTablet,
                            schema_,
                            NULL,
                            &log_));

    consensus_.reset(new TestRaftConsensusQueueIface(log_.get()));
    queue_.reset(new PeerMessageQueue(metric_context_));
  }


  Status AppendReplicateMsg(int term, int index, int payload_size) {
    return queue_->AppendOperation(CreateDummyReplicate(term, index, payload_size));
  }

  // Updates the peer's watermark in the queue so that it matches
  // the operation we want, since the queue always assumes that
  // when a peer gets tracked it's always tracked starting at the
  // last operation in the queue
  void UpdatePeerWatermarkToOp(ConsensusRequestPB* request,
                               ConsensusResponsePB* response,
                               const OpId& last_received,
                               bool* more_pending) {

    ASSERT_STATUS_OK(queue_->TrackPeer(kPeerUuid));
    response->set_responder_uuid(kPeerUuid);

    // Ask for a request. The queue assumes the peer is up-to-date so
    // this should contain no operations.
    queue_->RequestForPeer(kPeerUuid, request);
    ASSERT_EQ(request->ops_size(), 0);
    response->set_responder_term(request->caller_term());

    // Refuse saying that the log matching property check failed and
    // that our last operation is actually 'last_received'.
    RefuseWithLogPropertyMismatch(response, last_received);
    queue_->ResponseFromPeer(*response, more_pending);
    request->Clear();
    response->mutable_status()->Clear();
  }

  void RefuseWithLogPropertyMismatch(ConsensusResponsePB* response,
                                     const OpId& last_received) {
    ConsensusStatusPB* status = response->mutable_status();
    status->mutable_last_received()->CopyFrom(last_received);
    ConsensusErrorPB* error = status->mutable_error();
    error->set_code(ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH);
    StatusToPB(Status::IllegalState("LMP failed."), error->mutable_status());
  }

 protected:
  gscoped_ptr<TestRaftConsensusQueueIface> consensus_;
  const Schema schema_;
  gscoped_ptr<FsManager> fs_manager_;
  MetricRegistry metric_registry_;
  MetricContext metric_context_;
  gscoped_ptr<PeerMessageQueue> queue_;
  gscoped_ptr<log::Log> log_;
  scoped_refptr<log::OpIdAnchorRegistry> registry_;
};

// Tests that the queue is able to track a peer when it starts tracking a peer
// after the initial message in the queue. In particular this creates a queue
// with several messages and then starts to track a peer whose watermark
// falls in the middle of the current messages in the queue.
TEST_F(ConsensusQueueTest, TestStartTrackingAfterStart) {
  queue_->Init(consensus_.get(), MinimumOpId(), MinimumOpId().term(), 1);
  AppendReplicateMessagesToQueue(queue_.get(), 1, 100);

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  bool more_pending = false;

  // Peer already has some messages, last one being 7.50
  OpId last_received;
  last_received.set_term(7);
  last_received.set_index(50);

  UpdatePeerWatermarkToOp(&request, &response, last_received, &more_pending);
  ASSERT_TRUE(more_pending);

  // Getting a new request should get all operations after 7.50
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(request.ops_size(), 50);
  response.mutable_status()->mutable_last_received()->CopyFrom(request.ops(49).id());
  queue_->ResponseFromPeer(response, &more_pending);
  ASSERT_FALSE(more_pending) << "Queue still had requests pending";

  // if we ask for a new request, it should come back empty
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(0, request.ops_size());

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), NULL);
 }

// Tests that the peers gets the messages pages, with the size of a page
// being 'consensus_max_batch_size_bytes'
TEST_F(ConsensusQueueTest, TestGetPagedMessages) {
  queue_->Init(consensus_.get(), MinimumOpId(), MinimumOpId().term(), 1);

  // helper to estimate request size so that we can set the max batch size appropriately
  ConsensusRequestPB page_size_estimator;
  page_size_estimator.set_caller_term(14);
  OpId* committed_index = page_size_estimator.mutable_committed_index();
  committed_index->set_index(0);
  committed_index->set_term(0);
  OpId* preceding_id = page_size_estimator.mutable_preceding_id();
  preceding_id->set_index(0);
  preceding_id->set_term(0);

  // We're going to add 100 messages to the queue so we make each page fetch 9 of those,
  // for a total of 12 pages. The last page should have a single op.
  const int kOpsPerRequest = 9;
  for (int i = 0; i < kOpsPerRequest; i++) {
    page_size_estimator.mutable_ops()->AddAllocated(CreateDummyReplicate(0, 0, 0).release());
  }

  // Save the current flag state.
  google::FlagSaver saver;
  FLAGS_consensus_max_batch_size_bytes = page_size_estimator.ByteSize();

  AppendReplicateMessagesToQueue(queue_.get(), 1, 100);

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  bool more_pending = false;

  UpdatePeerWatermarkToOp(&request, &response, MinimumOpId(), &more_pending);
  ASSERT_TRUE(more_pending);

  for (int i = 0; i < 11; i++) {
    VLOG(1) << "Making request " << i;
    queue_->RequestForPeer(kPeerUuid, &request);
    ASSERT_EQ(kOpsPerRequest, request.ops_size());
    response.mutable_status()->mutable_last_received()->CopyFrom(
        request.ops(request.ops_size() -1).id());
    queue_->ResponseFromPeer(response, &more_pending);
    ASSERT_TRUE(more_pending);
  }
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(1, request.ops_size());
  response.mutable_status()->mutable_last_received()->CopyFrom(
      request.ops(request.ops_size() -1).id());
  queue_->ResponseFromPeer(response, &more_pending);
  ASSERT_FALSE(more_pending);

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), NULL);
}

TEST_F(ConsensusQueueTest, TestPeersDontAckBeyondWatermarks) {
  queue_->Init(consensus_.get(), MinimumOpId(), MinimumOpId().term(), 1);
  AppendReplicateMessagesToQueue(queue_.get(), 1, 100);

  // Start to track the peer after the queue has some messages in it
  // at a point that is halfway through the current messages in the queue.
  OpId first_msg;
  first_msg.set_term(7);
  first_msg.set_index(50);

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  bool more_pending = false;

  UpdatePeerWatermarkToOp(&request, &response, first_msg, &more_pending);
  ASSERT_TRUE(more_pending);

  // ask for a request, with normal flags this should get half the queue.
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(50, request.ops_size());

  response.mutable_status()->mutable_last_received()->CopyFrom(request.ops(49).id());

  AppendReplicateMessagesToQueue(queue_.get(), 101, 100);
  response.set_responder_term(28);

  queue_->ResponseFromPeer(response, &more_pending);
  ASSERT_TRUE(more_pending) << "Queue didn't have anymore requests pending";

  OpId expected;
  expected.set_term(14);
  expected.set_index(100);
  ASSERT_OPID_EQ(queue_->GetCommittedIndexForTests(), expected);

  // if we ask for a new request, it should come back with the rest of the messages
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(100, request.ops_size());

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), NULL);
}

TEST_F(ConsensusQueueTest, TestQueueAdvancesCommittedIndex) {
  queue_->Init(consensus_.get(), MinimumOpId(), MinimumOpId().term(), 2);
  // Track 3 different peers;
  queue_->TrackPeer("peer-1");
  queue_->TrackPeer("peer-2");
  queue_->TrackPeer("peer-3");

  // Append 10 messages to the queue with a majority of 2 for a total of 3 peers.
  // This should add messages 0.1 -> 0.7, 1.8 -> 1.10 to the queue.
  AppendReplicateMessagesToQueue(queue_.get(), 1, 10);

  // Since no operation was ack'd the committed_index should be
  // MinimumOpId()
  ASSERT_OPID_EQ(queue_->GetCommittedIndexForTests(), MinimumOpId());

  // NOTE: We don't need to get operations from the queue. The queue
  // only cares about what the peer reported as received, not what was sent.
  ConsensusResponsePB response;
  response.set_responder_term(1);
  ConsensusStatusPB* status = response.mutable_status();
  OpId* last_received = status->mutable_last_received();

  bool more_pending;

  // Ack the first five operations for peer-1
  response.set_responder_uuid("peer-1");

  last_received->set_term(0);
  last_received->set_index(5);

  queue_->ResponseFromPeer(response, &more_pending);
  ASSERT_TRUE(more_pending);

  // Committed index should be the same
  ASSERT_OPID_EQ(queue_->GetCommittedIndexForTests(), MinimumOpId());

  // Ack the first five operations for peer-2
  response.set_responder_uuid("peer-2");

  queue_->ResponseFromPeer(response, &more_pending);
  ASSERT_TRUE(more_pending);


  // Committed index should now have advanced to 0.5
  OpId expected_committed_index;
  expected_committed_index.set_term(0);
  expected_committed_index.set_index(5);

  ASSERT_OPID_EQ(queue_->GetCommittedIndexForTests(), expected_committed_index);

  // Ack all operations for peer-3
  response.set_responder_uuid("peer-3");
  last_received->set_term(1);
  last_received->set_index(10);

  queue_->ResponseFromPeer(response, &more_pending);
  ASSERT_FALSE(more_pending);

  // Committed index should be the same
  ASSERT_OPID_EQ(queue_->GetCommittedIndexForTests(), expected_committed_index);

  // Ack the remaining operations for peer-1
  response.set_responder_uuid("peer-1");
  queue_->ResponseFromPeer(response, &more_pending);
  ASSERT_FALSE(more_pending);

  // Committed index should be the tail of the queue
  expected_committed_index.set_term(1);
  expected_committed_index.set_index(10);
  ASSERT_OPID_EQ(queue_->GetCommittedIndexForTests(), expected_committed_index);
}

// In this test we append a sequence of operations to a log
// and then start tracking a peer whose first required operation
// is before the first operation in the queue.
TEST_F(ConsensusQueueTest, TestQueueLoadsOperationsForPeer) {
  for (int i = 1; i <= 100; i++) {
    log::LogEntryPB log_entry;
    log_entry.set_type(log::REPLICATE);
    ReplicateMsg* repl = log_entry.mutable_replicate();
    repl->mutable_id()->set_term(1);
    repl->mutable_id()->set_index(i);
    repl->set_op_type(NO_OP);
    ASSERT_OK(log_->Append(&log_entry));
    // Roll the log every 10 ops
    if (i % 10 == 0) {
      ASSERT_OK(log_->AllocateSegmentAndRollOver());
    }
  }
  ASSERT_OK(log_->WaitUntilAllFlushed());

  // Now reset the queue so that we can pass a new committed index,
  // the last operation in the log.
  queue_.reset(new PeerMessageQueue(metric_context_));
  OpId committed_index;
  committed_index.set_term(1);
  committed_index.set_index(100);
  queue_->Init(consensus_.get(), committed_index, 1, 1);

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  bool more_pending = false;

  // The peer will actually be behind the first operation in the queue
  // in this case about 50 operations before.
  OpId peers_last_op;
  peers_last_op.set_term(1);
  peers_last_op.set_index(50);

  // Now we start tracking the peer, this negotiation round should let
  // the queue know how far along the peer is.
  ASSERT_NO_FATAL_FAILURE(UpdatePeerWatermarkToOp(&request,
                                                  &response,
                                                  peers_last_op,
                                                  &more_pending));

  // The queue should reply that there are more messages for the peer.
  ASSERT_TRUE(more_pending);

  // When we get another request for the peer the queue should enqueue
  // async loading of the missing operations.
  queue_->RequestForPeer(kPeerUuid, &request);
  // Since this just enqueued the op loading, the request should have
  // no additional ops.
  ASSERT_EQ(request.ops_size(), 0);

  // The request's preceding_id should now match the peers last id.
  ASSERT_OPID_EQ(request.preceding_id(), peers_last_op);

  // When we now reply the same response as before (nothing changed
  // from the peer's perspective) we should get that 'more_pending' is
  // false. This is important or we would have a constant back and
  // forth between the queue and the peer until the operations we
  // want are loaded.
  response.mutable_status()->mutable_last_received()->CopyFrom(peers_last_op);
  queue_->ResponseFromPeer(response, &more_pending);
  ASSERT_FALSE(more_pending);

  // Spin a few times to give the queue loader a change to load the requests
  // while at the same time adding more messages to the queue.
  int expected_count = 50;
  int current_index = 101;
  while (request.ops_size() == 0) {
    AppendReplicateMsg(1, current_index, 0);
    current_index++;
    expected_count++;
    queue_->RequestForPeer(kPeerUuid, &request);
    if (request.ops_size() == 0) {
      response.mutable_status()->mutable_last_received()->CopyFrom(peers_last_op);
      queue_->ResponseFromPeer(response, &more_pending);
    } else {
      response.mutable_status()->mutable_last_received()->CopyFrom(
          request.ops(request.ops_size() - 1).id());
      queue_->ResponseFromPeer(response, &more_pending);
    }
    usleep(10);
  }

  ASSERT_EQ(request.ops_size(), expected_count);

  // The messages still belong to the queue so we have to release them.
  request.mutable_ops()->ExtractSubrange(0, request.ops().size(), NULL);
}

}  // namespace consensus
}  // namespace kudu
