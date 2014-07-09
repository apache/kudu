// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include "consensus/consensus_queue.h"
#include "consensus/consensus-test-util.h"
#include "consensus/log_util.h"
#include "util/metrics.h"
#include "util/test_macros.h"
#include "util/test_util.h"

DECLARE_int32(consensus_max_batch_size_bytes);
DECLARE_int32(consensus_entry_cache_size_soft_limit_mb);
DECLARE_int32(consensus_entry_cache_size_hard_limit_mb);

namespace kudu {
namespace consensus {

static const char* kPeerUuid = "a";

class ConsensusQueueTest : public KuduTest {
 public:
  ConsensusQueueTest()
      : metric_context_(&metric_registry_, "queue-test"),
        queue_(new PeerMessageQueue(metric_context_)) {}

  Status AppendReplicateOp(gscoped_ptr<OperationPB> op,
                           int term, int index,
                           const std::string& payload,
                           scoped_refptr<OperationStatusTracker>* ost) {
    op.reset(new OperationPB);
    OpId* id = op->mutable_id();
    id->set_term(term);
    id->set_index(index);

    ReplicateMsg* msg = op->mutable_replicate();
    msg->set_op_type(NO_OP);
    msg->mutable_noop_request()->set_payload_for_tests(payload);
    ost->reset(new TestOpStatusTracker(op.Pass(), 1, 1));
    return queue_->AppendOperation(*ost);
  }

 protected:
  MetricRegistry metric_registry_;
  MetricContext metric_context_;
  gscoped_ptr<PeerMessageQueue> queue_;
};

// This tests that the peer gets all the messages in the buffer
TEST_F(ConsensusQueueTest, TestGetAllMessages) {
  AppendReplicateMessagesToQueue(queue_.get(), 1, 100);

  ASSERT_STATUS_OK(queue_->TrackPeer(kPeerUuid, log::MinimumOpId()));

  ConsensusRequestPB request;
  ConsensusStatusPB status;
  bool more_pending = false;

  // ask for a request. with normal flags this should get the whole queue.
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(request.ops_size(), 100);
  status.mutable_safe_commit_watermark()->CopyFrom(request.ops(99).id());
  status.mutable_replicated_watermark()->CopyFrom(request.ops(99).id());
  status.mutable_received_watermark()->CopyFrom(request.ops(99).id());
  queue_->ResponseFromPeer(kPeerUuid, status, &more_pending);
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
  first_msg.set_index(1);
  ASSERT_STATUS_OK(queue_->TrackPeer(kPeerUuid, first_msg));

  ConsensusRequestPB request;
  ConsensusStatusPB status;
  bool more_pending = false;

  // ask for a request, with normal flags this should get half the queue.
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(request.ops_size(), 50);
  status.mutable_safe_commit_watermark()->CopyFrom(request.ops(49).id());
  status.mutable_replicated_watermark()->CopyFrom(request.ops(49).id());
  status.mutable_received_watermark()->CopyFrom(request.ops(49).id());
  queue_->ResponseFromPeer(kPeerUuid, status, &more_pending);
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
  OperationPB* op = page_size_estimator.add_ops();
  OpId* id = op->mutable_id();
  id->set_index(0);
  id->set_term(0);
  ReplicateMsg* msg = op->mutable_replicate();
  msg->set_op_type(NO_OP);
  msg->mutable_noop_request()->set_payload_for_tests("");

  // Save the current flag state.
  google::FlagSaver saver;
  // we' going to add 100 messages to the queue so we make each page
  // fetch 9 of those, for a total of 12 pages. The last page should have a single op
  FLAGS_consensus_max_batch_size_bytes = 9 * page_size_estimator.ByteSize();

  AppendReplicateMessagesToQueue(queue_.get(), 1, 100);

  queue_->TrackPeer(kPeerUuid, log::MinimumOpId());
  bool more_pending = false;

  ConsensusRequestPB request;
  ConsensusStatusPB status;
  for (int i = 0; i < 11; i++) {
    queue_->RequestForPeer(kPeerUuid, &request);
    ASSERT_EQ(9, request.ops_size());
    status.mutable_safe_commit_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
    status.mutable_replicated_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
    status.mutable_received_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
    queue_->ResponseFromPeer(kPeerUuid, status, &more_pending);
    ASSERT_TRUE(more_pending);
  }
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(1, request.ops_size());
  status.mutable_safe_commit_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
  status.mutable_replicated_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
  status.mutable_received_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
  queue_->ResponseFromPeer(kPeerUuid, status, &more_pending);
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
  gscoped_ptr<OperationPB> op;
  scoped_refptr<OperationStatusTracker> status;
  {
    op.reset(new OperationPB);
    OpId* id = op->mutable_id();
    id->set_term(0);
    id->set_index(1);
    ReplicateMsg* msg = op->mutable_replicate();
    msg->set_op_type(NO_OP);
    msg->mutable_noop_request()->set_payload_for_tests(test_payload);
    status.reset(new TestOpStatusTracker(op.Pass(), 1, 1));
  }
  ASSERT_STATUS_OK(queue_->AppendOperation(status));

  // Ensure that a request contains the message.
  ConsensusRequestPB request;
  queue_->TrackPeer(kPeerUuid, log::MinimumOpId());
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
  first_msg.set_index(1);
  ASSERT_STATUS_OK(queue_->TrackPeer(kPeerUuid, first_msg));

  ConsensusRequestPB request;
  ConsensusStatusPB status;
  bool more_pending = false;

  // ask for a request, with normal flags this should get half the queue.
  queue_->RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(50, request.ops_size());
  status.mutable_safe_commit_watermark()->CopyFrom(request.ops(49).id());
  status.mutable_replicated_watermark()->CopyFrom(request.ops(49).id());
  status.mutable_received_watermark()->CopyFrom(request.ops(49).id());

  AppendReplicateMessagesToQueue(queue_.get(), 101, 100, 1, 1, "", &statuses);
  queue_->ResponseFromPeer(kPeerUuid, status, &more_pending);
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
  queue_.reset(new PeerMessageQueue(metric_context_));

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

  // Try and acccess some random operation in the queue. In this case operation
  // 3.4 corresponds to the 25th operation in the queue.
  OpId op;
  op.set_term(3);
  op.set_index(4);
  scoped_refptr<OperationStatusTracker> status;
  queue_->GetOperationStatus(op, &status);
  ASSERT_EQ(statuses[24]->ToString(), status->ToString());
}

TEST_F(ConsensusQueueTest, TestQueueRefusesRequestWhenFilled) {
  vector<scoped_refptr<OperationStatusTracker> > statuses;
  FLAGS_consensus_entry_cache_size_soft_limit_mb = 0;
  FLAGS_consensus_entry_cache_size_hard_limit_mb = 1;

  queue_.reset(new PeerMessageQueue(metric_context_));

  // generate a 128Kb dummy payload
  string test_payload(128 * 1024, '0');

  // append 8 messages to the queue, these should be allowed
  AppendReplicateMessagesToQueue(queue_.get(), 1, 7, 1, 1, test_payload, &statuses);

  gscoped_ptr<OperationPB> op;
  scoped_refptr<OperationStatusTracker> status;

  // should fail with service unavailable
  Status s = AppendReplicateOp(op.Pass(), 10, 1, test_payload, &status);

  LOG(INFO) << queue_->ToString();
  ASSERT_TRUE(s.IsServiceUnavailable());

  // but should still accept commits
  {
    op.reset(new OperationPB);
    OpId* id = op->mutable_id();
    id->set_term(10);
    id->set_index(1);
    CommitMsg* msg = op->mutable_commit();
    msg->set_op_type(NO_OP);
    msg->mutable_noop_response()->set_payload_for_tests(test_payload);
    status.reset(new TestOpStatusTracker(op.Pass(), 1, 1));
  }

  ASSERT_STATUS_OK(queue_->AppendOperation(status));

  // now ack the first and second ops
  statuses[0]->AckPeer("");
  statuses[1]->AckPeer("");

  // .. and try again
  ASSERT_STATUS_OK(AppendReplicateOp(op.Pass(), 10, 2, test_payload, &status));
}

TEST_F(ConsensusQueueTest, TestQueueHardAndSoftLimit) {
  FLAGS_consensus_entry_cache_size_soft_limit_mb = 1;
  FLAGS_consensus_entry_cache_size_hard_limit_mb = 2;

  queue_.reset(new PeerMessageQueue(metric_context_));

  const int kPayloadSize = 768 * 1024;

  gscoped_ptr<OperationPB> op;
  scoped_refptr<OperationStatusTracker> status_1;
  string payload(kPayloadSize, '0');

  // Soft limit should not be violated.
  ASSERT_STATUS_OK(AppendReplicateOp(op.Pass(), 1, 1, payload, &status_1));

  int size_with_one_msg = queue_->GetQueuedOperationsSizeBytesForTests();
  ASSERT_LT(size_with_one_msg, 1 * 1024 * 1024);

  // Violating a soft limit, but not a hard limit should still allow
  // the operation to be added.
  scoped_refptr<OperationStatusTracker> status_2;
  ASSERT_STATUS_OK(AppendReplicateOp(op.Pass(), 1, 2, payload, &status_2));

  // Since the first operation is not yet done, we can't trim.
  int size_with_two_msgs = queue_->GetQueuedOperationsSizeBytesForTests();
  ASSERT_GE(size_with_two_msgs, 2 * 768 * 1024);
  ASSERT_LT(size_with_two_msgs, 2 * 1024 * 1024);

  // Ensure that we can trim one message from the queue.
  status_1->AckPeer("");
  scoped_refptr<OperationStatusTracker> status_3;

  // Verify that we have trimmed by appending a message that would
  // otherwise be rejected, since the queue max size limit is 2MB.
  ASSERT_STATUS_OK(AppendReplicateOp(op.Pass(), 1, 3, payload, &status_3));

  // The queue should be trimmed down to two messages.
  ASSERT_EQ(size_with_two_msgs, queue_->GetQueuedOperationsSizeBytesForTests());


  // Ack 'status_2', 'status_3'.
  status_2->AckPeer("");
  status_3->AckPeer("");
  scoped_refptr<OperationStatusTracker> status_4;
  ASSERT_STATUS_OK(AppendReplicateOp(op.Pass(), 1, 4, payload, &status_4));

  // Verify that the queue is trimmed down to just one message.
  ASSERT_EQ(size_with_one_msg, queue_->GetQueuedOperationsSizeBytesForTests());

  status_4->AckPeer("");
  // Add a small message such that soft limit is not violated.
  string small_payload(128 * 1024, '0');
  scoped_refptr<OperationStatusTracker> status_5;
  ASSERT_STATUS_OK(AppendReplicateOp(op.Pass(), 1, 5, small_payload, &status_5));

  // Verify that the queue is not trimmed.
  ASSERT_GT(queue_->GetQueuedOperationsSizeBytesForTests(), size_with_one_msg);
}


}  // namespace consensus
}  // namespace kudu

