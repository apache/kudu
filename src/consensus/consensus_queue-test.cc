// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include "consensus/consensus_queue.h"
#include "consensus/consensus-test-util.h"
#include "consensus/log_util.h"
#include "util/test_macros.h"

DECLARE_int32(consensus_max_batch_size_bytes);
DECLARE_int32(consensus_entry_cache_size_soft_limit_mb);

namespace kudu {
namespace consensus {

static const char* kPeerUuid = "a";

// This tests that the peer gets all the messages in the buffer
TEST(TestConsensusRequestQueue, TestGetAllMessages) {
  PeerMessageQueue queue;
  AppendReplicateMessagesToQueue(&queue, 1, 100);

  ASSERT_STATUS_OK(queue.TrackPeer(kPeerUuid, log::MinimumOpId()));

  ConsensusRequestPB request;
  ConsensusStatusPB status;
  bool more_pending = false;

  // ask for a request. with normal flags this should get the whole queue.
  queue.RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(request.ops_size(), 100);
  status.mutable_safe_commit_watermark()->CopyFrom(request.ops(99).id());
  status.mutable_replicated_watermark()->CopyFrom(request.ops(99).id());
  status.mutable_received_watermark()->CopyFrom(request.ops(99).id());
  queue.ResponseFromPeer(kPeerUuid, status, &more_pending);
  ASSERT_FALSE(more_pending) << "Queue still had requests pending";

  // if we ask for a new request, it should come back empty
  queue.RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(0, request.ops_size());

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), NULL);
 }

// Tests that the queue is able to track a peer when it starts tracking a peer
// after the initial message in the queue. In particular this creates a queue
// with several messages and then starts to track a peer whose watermark
// falls in the middle of the current messages in the queue.
TEST(TestConsensusRequestQueue, TestStartTrackingAfterStart) {
  PeerMessageQueue queue;
  AppendReplicateMessagesToQueue(&queue, 1, 100);

  // Start to track the peer after the queue has some messages in it
  // at a point that is halfway through the current messages in the queue.
  OpId first_msg;
  first_msg.set_term(7);
  first_msg.set_index(1);
  ASSERT_STATUS_OK(queue.TrackPeer(kPeerUuid, first_msg));

  ConsensusRequestPB request;
  ConsensusStatusPB status;
  bool more_pending = false;

  // ask for a request, with normal flags this should get half the queue.
  queue.RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(request.ops_size(), 50);
  status.mutable_safe_commit_watermark()->CopyFrom(request.ops(49).id());
  status.mutable_replicated_watermark()->CopyFrom(request.ops(49).id());
  status.mutable_received_watermark()->CopyFrom(request.ops(49).id());
  queue.ResponseFromPeer(kPeerUuid, status, &more_pending);
  ASSERT_FALSE(more_pending) << "Queue still had requests pending";

  // if we ask for a new request, it should come back empty
  queue.RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(0, request.ops_size());

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), NULL);
 }

// Tests that the peers gets the messages pages, with the size of a page
// being 'consensus_max_batch_size_bytes'
TEST(TestConsensusRequestQueue, TestGetPagedMessages) {

  // helper to estimate request size so that we can set the max batch size appropriately
  ConsensusRequestPB page_size_estimator;
  OperationPB* op = page_size_estimator.add_ops();
  OpId* id = op->mutable_id();
  id->set_index(0);
  id->set_term(0);
  ReplicateMsg* msg = op->mutable_replicate();
  msg->set_op_type(NO_OP);

  // Save the current flag state.
  google::FlagSaver saver;
  // we' going to add 100 messages to the queue so we make each page
  // fetch 9 of those, for a total of 12 pages. The last page should have a single op
  FLAGS_consensus_max_batch_size_bytes = 9 * page_size_estimator.ByteSize();

  PeerMessageQueue queue;
  AppendReplicateMessagesToQueue(&queue, 1, 100);

  queue.TrackPeer(kPeerUuid, log::MinimumOpId());
  bool more_pending = false;

  ConsensusRequestPB request;
  ConsensusStatusPB status;
  for (int i = 0; i < 11; i++) {
    queue.RequestForPeer(kPeerUuid, &request);
    ASSERT_EQ(9, request.ops_size());
    status.mutable_safe_commit_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
    status.mutable_replicated_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
    status.mutable_received_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
    queue.ResponseFromPeer(kPeerUuid, status, &more_pending);
    ASSERT_TRUE(more_pending);
  }
  queue.RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(1, request.ops_size());
  status.mutable_safe_commit_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
  status.mutable_replicated_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
  status.mutable_received_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
  queue.ResponseFromPeer(kPeerUuid, status, &more_pending);
  ASSERT_FALSE(more_pending);

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), NULL);

}

TEST(TestConsensusRequestQueue, TestPeersDontAckBeyondWatermarks) {
  vector<scoped_refptr<OperationStatusTracker> > statuses;
  PeerMessageQueue queue;
  AppendReplicateMessagesToQueue(&queue, 1, 100, 1, 1, &statuses);

  // Start to track the peer after the queue has some messages in it
  // at a point that is halfway through the current messages in the queue.
  OpId first_msg;
  first_msg.set_term(7);
  first_msg.set_index(1);
  ASSERT_STATUS_OK(queue.TrackPeer(kPeerUuid, first_msg));

  ConsensusRequestPB request;
  ConsensusStatusPB status;
  bool more_pending = false;

  // ask for a request, with normal flags this should get half the queue.
  queue.RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(50, request.ops_size());
  status.mutable_safe_commit_watermark()->CopyFrom(request.ops(49).id());
  status.mutable_replicated_watermark()->CopyFrom(request.ops(49).id());
  status.mutable_received_watermark()->CopyFrom(request.ops(49).id());

  AppendReplicateMessagesToQueue(&queue, 101, 100, 1, 1, &statuses);
  queue.ResponseFromPeer(kPeerUuid, status, &more_pending);
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
  queue.RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(100, request.ops_size());

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), NULL);
}

// Tests that the buffer gets trimmed when messages are not being sent by any peer.
TEST(TestConsensusRequestQueue, TestBufferTrimsWhenMessagesAreNotNeeded) {

  FLAGS_consensus_entry_cache_size_soft_limit_mb = 1;

  PeerMessageQueue queue;
  // this amount of messages will overflow the buffer by about 2Kb
  AppendReplicateMessagesToQueue(&queue, 1, 200000, 0, 0);

  // test that even though we've overflown the buffer it still keeps within bounds
  ASSERT_LE(queue.GetQueuedOperationsSizeBytesForTests(), 1 * 1024 * 1024);
}

}  // namespace consensus
}  // namespace kudu

