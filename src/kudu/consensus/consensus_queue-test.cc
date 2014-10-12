// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <tr1/memory>

#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/log_util.h"
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

class ConsensusQueueTest : public KuduTest {
 public:
  ConsensusQueueTest()
      : consensus_(new MockRaftConsensusQueueIface),
        metric_context_(&metric_registry_, "queue-test"),
        queue_(new PeerMessageQueue(consensus_.get(), metric_context_)) {
    FLAGS_enable_data_block_fsync = false; // Keep unit tests fast.
    queue_->Init(MinimumOpId(), MinimumOpId().term());
  }

  Status AppendReplicateMsg(int term, int index,
                            const std::string& payload,
                            scoped_refptr<OperationStatusTracker>* ost) {
    gscoped_ptr<ReplicateMsg> msg(new ReplicateMsg);
    OpId* id = msg->mutable_id();
    id->set_term(term);
    id->set_index(index);

    msg->set_op_type(NO_OP);
    msg->mutable_noop_request()->set_payload_for_tests(payload);
    ost->reset(new TestOpStatusTracker(msg.Pass(), 1, 1));
    return queue_->AppendOperation(*ost);
  }

 protected:
  gscoped_ptr<MockRaftConsensusQueueIface> consensus_;
  MetricRegistry metric_registry_;
  MetricContext metric_context_;
  gscoped_ptr<PeerMessageQueue> queue_;
};

// This tests that the peer gets all the messages in the buffer
TEST_F(ConsensusQueueTest, TestGetAllMessages) {
  AppendReplicateMessagesToQueue(queue_.get(), 1, 100);

  ASSERT_STATUS_OK(queue_->TrackPeer(kPeerUuid, MinimumOpId()));

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  response.set_responder_term(14);

  bool more_pending = false;

  // ask for a request. with normal flags this should get the whole queue.
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(request.ops_size(), 100);
  response.mutable_status()->mutable_last_received()->CopyFrom(request.ops(99).id());
  queue_->ResponseFromPeer(response, &more_pending);
  ASSERT_FALSE(more_pending) << "Queue still had requests pending";

  // if we ask for a new request, it should come back empty
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(0, request.ops_size());

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), NULL);
 }

// Tests that the queue is able to track a peer when it starts tracking a peer
// after the initial message in the queue. In particular this creates a queue
// with several messages and then starts to track a peer whose watermark
// falls in the middle of the current messages in the queue.
TEST_F(ConsensusQueueTest, TestStartTrackingAfterStart) {
  AppendReplicateMessagesToQueue(queue_.get(), 1, 100);

  // Start to track the peer after the queue has some messages in it
  // at a point that is halfway through the current messages in the queue.
  OpId first_msg;
  first_msg.set_term(7);
  first_msg.set_index(50);
  ASSERT_STATUS_OK(queue_->TrackPeer(kPeerUuid, first_msg));

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  response.set_responder_term(14);
  bool more_pending = false;

  // ask for a request, with normal flags this should get half the queue.
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
  for (int i = 0; i < 9; i++) {
    ReplicateMsg* msg = page_size_estimator.add_ops();
    OpId* id = msg->mutable_id();
    id->set_index(0);
    id->set_term(0);
    msg->set_op_type(NO_OP);
    msg->mutable_noop_request()->set_payload_for_tests("");
  }

  // Save the current flag state.
  google::FlagSaver saver;
  FLAGS_consensus_max_batch_size_bytes = page_size_estimator.ByteSize();

  AppendReplicateMessagesToQueue(queue_.get(), 1, 100);

  queue_->TrackPeer(kPeerUuid, MinimumOpId());
  bool more_pending = false;

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  response.set_responder_term(14);
  for (int i = 0; i < 11; i++) {
    queue_->RequestForPeer(kPeerUuid, &request);
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

// Ensure that the queue always sends at least one message to a peer,
// even if that message is larger than the batch size. This ensures
// that we don't get "stuck" in the case that a large message enters
// the queue.
TEST_F(ConsensusQueueTest, TestAlwaysYieldsAtLeastOneMessage) {
  // generate a 2MB dummy payload
  string test_payload(2 * 1024 * 1024, '0');

  // Set a small batch size -- smaller than the message we're appending.
  google::FlagSaver saver;
  FLAGS_consensus_max_batch_size_bytes = 10000;

  // Append the large op to the queue
  gscoped_ptr<ReplicateMsg> msg;
  scoped_refptr<OperationStatusTracker> status;
  {
    msg.reset(new ReplicateMsg);
    OpId* id = msg->mutable_id();
    id->set_term(0);
    id->set_index(1);
    msg->set_op_type(NO_OP);
    msg->mutable_noop_request()->set_payload_for_tests(test_payload);
    status.reset(new TestOpStatusTracker(msg.Pass(), 1, 1));
  }
  ASSERT_STATUS_OK(queue_->AppendOperation(status));

  // Ensure that a request contains the message.
  ConsensusRequestPB request;
  queue_->TrackPeer(kPeerUuid, MinimumOpId());
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(1, request.ops_size());

  // extract the op from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), NULL);
}

TEST_F(ConsensusQueueTest, TestPeersDontAckBeyondWatermarks) {
  vector<scoped_refptr<OperationStatusTracker> > statuses;
  AppendReplicateMessagesToQueue(queue_.get(), 1, 100, 1, 1, "", &statuses);

  // Start to track the peer after the queue has some messages in it
  // at a point that is halfway through the current messages in the queue.
  OpId first_msg;
  first_msg.set_term(7);
  first_msg.set_index(50);

  ASSERT_STATUS_OK(queue_->TrackPeer(kPeerUuid, first_msg));

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  response.set_responder_term(14);

  bool more_pending = false;

  // ask for a request, with normal flags this should get half the queue.
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(50, request.ops_size());

  response.mutable_status()->mutable_last_received()->CopyFrom(request.ops(49).id());

  AppendReplicateMessagesToQueue(queue_.get(), 101, 100, 1, 1, "", &statuses);
  response.set_responder_term(28);

  queue_->ResponseFromPeer(response, &more_pending);
  ASSERT_TRUE(more_pending) << "Queue didn't have anymore requests pending";


  // Make sure the peer only ack'd between message 50 and 100
  for (int i = 0; i < statuses.size(); i++) {
    if (i < 50 || i > 99) {
      ASSERT_FALSE(statuses[i]->IsDone()) << "Operation: " << i << " should not be done.";
      continue;
    }
    ASSERT_TRUE(statuses[i]->IsDone())  << "Operation: " << i << " should be done.";
  }
  // if we ask for a new request, it should come back with the rest of the messages
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(100, request.ops_size());

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), NULL);
}

// Tests that the buffer gets trimmed when messages are not being sent by any peer.
TEST_F(ConsensusQueueTest, TestBufferTrimsWhenMessagesAreNotNeeded) {
  FLAGS_consensus_entry_cache_size_soft_limit_mb = 1;
  queue_.reset(new PeerMessageQueue(consensus_.get(), metric_context_));
  queue_->Init(MinimumOpId(), MinimumOpId().term());

  // generate a 128Kb dummy payload
  string test_payload(128 * 1024, '0');

  // this amount of messages will overflow the buffer
  AppendReplicateMessagesToQueue(queue_.get(), 1, 15, 0, 0, test_payload);

  // test that even though we've overflown the buffer it still keeps within bounds
  ASSERT_LE(queue_->GetQueuedOperationsSizeBytesForTests(), 1 * 1024 * 1024);
}

TEST_F(ConsensusQueueTest, TestGetOperationStatusTracker) {
  vector<scoped_refptr<OperationStatusTracker> > statuses;
  AppendReplicateMessagesToQueue(queue_.get(), 1, 100, 1, 1, "", &statuses);

  // Try and acccess some random operation in the queue. In this case
  // the 25th operation in the queue.
  OpId op;
  op.set_term(3);
  op.set_index(25);
  scoped_refptr<OperationStatusTracker> status;
  queue_->GetOperationStatus(op, &status);
  ASSERT_EQ(statuses[24]->ToString(), status->ToString());
}

TEST_F(ConsensusQueueTest, TestQueueRefusesRequestWhenFilled) {
  vector<scoped_refptr<OperationStatusTracker> > statuses;
  FLAGS_consensus_entry_cache_size_soft_limit_mb = 0;
  FLAGS_consensus_entry_cache_size_hard_limit_mb = 1;

  queue_.reset(new PeerMessageQueue(consensus_.get(), metric_context_));
  queue_->Init(MinimumOpId(), MinimumOpId().term());

  // generate a 128Kb dummy payload
  string test_payload(128 * 1024, '0');

  // append 8 messages to the queue, these should be allowed
  AppendReplicateMessagesToQueue(queue_.get(), 1, 7, 1, 1, test_payload, &statuses);

  scoped_refptr<OperationStatusTracker> status;

  // should fail with service unavailable
  Status s = AppendReplicateMsg(1, 8, test_payload, &status);

  ASSERT_TRUE(s.IsServiceUnavailable());

  // now ack the first and second ops
  statuses[0]->AckPeer("");
  statuses[1]->AckPeer("");
  // .. and try again
  ASSERT_STATUS_OK(AppendReplicateMsg(1, 8, test_payload, &status));
}

TEST_F(ConsensusQueueTest, TestQueueAdvancesCommittedIndex) {
  // Track 3 different peers;
  queue_->TrackPeer("peer-1", MinimumOpId());
  queue_->TrackPeer("peer-2", MinimumOpId());
  queue_->TrackPeer("peer-3", MinimumOpId());

  // Append 10 messages to the queue with a majority of 2 for a total of 3 peers.
  // This should add messages 0.1 -> 0.7, 1.8 -> 1.10 to the queue.
  vector<scoped_refptr<OperationStatusTracker> > statuses;
  AppendReplicateMessagesToQueue(queue_.get(), 1, 10, 2, 3);

  // Since no operation was ack'd the committed_index should be
  // MinimumOpId()
  ASSERT_TRUE(OpIdEquals(MinimumOpId(), queue_->GetCommittedIndexForTests()));

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
  ASSERT_TRUE(OpIdEquals(MinimumOpId(), queue_->GetCommittedIndexForTests()));

  // Ack the first five operations for peer-2
  response.set_responder_uuid("peer-2");

  queue_->ResponseFromPeer(response, &more_pending);
  ASSERT_TRUE(more_pending);

  // Committed index should now have advanced to 0.5
  OpId expected_committed_index;
  expected_committed_index.set_term(0);
  expected_committed_index.set_index(5);

  ASSERT_TRUE(OpIdEquals(expected_committed_index, queue_->GetCommittedIndexForTests()));

  // Ack all operations for peer-3
  response.set_responder_uuid("peer-3");
  last_received->set_term(1);
  last_received->set_index(10);

  queue_->ResponseFromPeer(response, &more_pending);
  ASSERT_FALSE(more_pending);

  // Committed index should be the same
  ASSERT_TRUE(OpIdEquals(expected_committed_index, queue_->GetCommittedIndexForTests()));

  // Ack the remaining operations for peer-1
  response.set_responder_uuid("peer-1");
  queue_->ResponseFromPeer(response, &more_pending);
  ASSERT_FALSE(more_pending);

  // Committed index should be the tail of the queue
  expected_committed_index.set_term(1);
  expected_committed_index.set_index(10);
  ASSERT_TRUE(OpIdEquals(expected_committed_index, queue_->GetCommittedIndexForTests()));
}

TEST_F(ConsensusQueueTest, TestQueueHardAndSoftLimit) {
  FLAGS_consensus_entry_cache_size_soft_limit_mb = 1;
  FLAGS_consensus_entry_cache_size_hard_limit_mb = 2;

  queue_.reset(new PeerMessageQueue(consensus_.get(), metric_context_));
  queue_->Init(MinimumOpId(), MinimumOpId().term());

  const int kPayloadSize = 768 * 1024;

  scoped_refptr<OperationStatusTracker> status_1;
  string payload(kPayloadSize, '0');

  // Soft limit should not be violated.
  ASSERT_STATUS_OK(AppendReplicateMsg(1, 1, payload, &status_1));

  int size_with_one_msg = queue_->GetQueuedOperationsSizeBytesForTests();
  ASSERT_LT(size_with_one_msg, 1 * 1024 * 1024);

  // Violating a soft limit, but not a hard limit should still allow
  // the operation to be added.
  scoped_refptr<OperationStatusTracker> status_2;
  ASSERT_STATUS_OK(AppendReplicateMsg(1, 2, payload, &status_2));

  // Since the first operation is not yet done, we can't trim.
  int size_with_two_msgs = queue_->GetQueuedOperationsSizeBytesForTests();
  ASSERT_GE(size_with_two_msgs, 2 * 768 * 1024);
  ASSERT_LT(size_with_two_msgs, 2 * 1024 * 1024);

  // Ensure that we can trim one message from the queue.
  status_1->AckPeer("");
  scoped_refptr<OperationStatusTracker> status_3;

  // Verify that we have trimmed by appending a message that would
  // otherwise be rejected, since the queue max size limit is 2MB.
  ASSERT_STATUS_OK(AppendReplicateMsg(1, 3, payload, &status_3));

  // The queue should be trimmed down to two messages.
  ASSERT_EQ(size_with_two_msgs, queue_->GetQueuedOperationsSizeBytesForTests());


  // Ack 'status_2', 'status_3'.
  status_2->AckPeer("");
  status_3->AckPeer("");
  scoped_refptr<OperationStatusTracker> status_4;
  ASSERT_STATUS_OK(AppendReplicateMsg(1, 4, payload, &status_4));

  // Verify that the queue is trimmed down to just one message.
  ASSERT_EQ(size_with_one_msg, queue_->GetQueuedOperationsSizeBytesForTests());

  status_4->AckPeer("");
  // Add a small message such that soft limit is not violated.
  string small_payload(128 * 1024, '0');
  scoped_refptr<OperationStatusTracker> status_5;
  ASSERT_STATUS_OK(AppendReplicateMsg(1, 5, small_payload, &status_5));

  // Verify that the queue is not trimmed.
  ASSERT_GT(queue_->GetQueuedOperationsSizeBytesForTests(), size_with_one_msg);
}

TEST_F(ConsensusQueueTest, TestGlobalHardLimit) {
 FLAGS_consensus_entry_cache_size_soft_limit_mb = 1;
 FLAGS_global_consensus_entry_cache_size_soft_limit_mb = 4;

 FLAGS_consensus_entry_cache_size_hard_limit_mb = 2;
 FLAGS_global_consensus_entry_cache_size_hard_limit_mb = 5;

 const string kParentTrackerId = "TestGlobalHardLimit";

 shared_ptr<MemTracker> parent_tracker = MemTracker::CreateTracker(
     FLAGS_consensus_entry_cache_size_soft_limit_mb * 1024 * 1024,
     kParentTrackerId,
     NULL);

 ASSERT_TRUE(parent_tracker.get() != NULL);


 // Exceed the global hard limit.
 parent_tracker->Consume(6 * 1024 * 1024);

 queue_.reset(new PeerMessageQueue(consensus_.get(),
                                   metric_context_,
                                   kParentTrackerId));
 queue_->Init(MinimumOpId(), MinimumOpId().term());

 const int kPayloadSize = 768 * 1024;

 scoped_refptr<OperationStatusTracker> status;

 string payload(kPayloadSize, '0');

 // Should fail with service unavailable.
 Status s = AppendReplicateMsg(1, 1, payload, &status);

 ASSERT_TRUE(s.IsServiceUnavailable());

 // Now release the memory.

 parent_tracker->Release(2 * 1024 * 1024);

 ASSERT_STATUS_OK(AppendReplicateMsg(1, 1, payload, &status));
}

TEST_F(ConsensusQueueTest, TestTrimWhenGlobalSoftLimitExceeded) {
  FLAGS_consensus_entry_cache_size_soft_limit_mb = 1;
  FLAGS_global_consensus_entry_cache_size_soft_limit_mb = 4;

  FLAGS_consensus_entry_cache_size_hard_limit_mb = 2;
  FLAGS_global_consensus_entry_cache_size_hard_limit_mb = 5;

  const string kParentTrackerId = "TestGlobalSoftLimit";

  shared_ptr<MemTracker> parent_tracker = MemTracker::CreateTracker(
     FLAGS_consensus_entry_cache_size_soft_limit_mb * 1024 * 1024,
     kParentTrackerId,
     NULL);

 ASSERT_TRUE(parent_tracker.get() != NULL);


 // Exceed the global soft limit.
 parent_tracker->Consume(4 * 1024 * 1024);
 parent_tracker->Consume(1024);

 queue_.reset(new PeerMessageQueue(consensus_.get(),
                                   metric_context_,
                                   kParentTrackerId));
 queue_->Init(MinimumOpId(), MinimumOpId().term());

 const int kPayloadSize = 768 * 1024;

 scoped_refptr<OperationStatusTracker> status;

 string payload(kPayloadSize, '0');

 ASSERT_STATUS_OK(AppendReplicateMsg(1, 1, payload, &status));

 int size_with_one_msg = queue_->GetQueuedOperationsSizeBytesForTests();

 status->AckPeer("");

 // If this goes through, that means the queue has been trimmed, otherwise
 // the hard limit would be violated and service unavailable status would
 // be returned.
 ASSERT_STATUS_OK(AppendReplicateMsg(1, 2, payload, &status));

 // Verify that there is only one message in the queue.
 ASSERT_EQ(size_with_one_msg, queue_->GetQueuedOperationsSizeBytesForTests());
}

}  // namespace consensus
}  // namespace kudu
