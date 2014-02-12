// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include "consensus/consensus_queue.h"
#include "util/countdown_latch.h"
#include "util/test_macros.h"

DECLARE_int32(consensus_max_batch_size_bytes);
DECLARE_int32(consensus_entry_cache_size_soft_limit_mb);

namespace kudu {
namespace consensus {

class TestOperationStatus : public OperationStatus {
 public:
  explicit TestOperationStatus(int n_majority)
      : n_majority_(n_majority),
        latch_(n_majority) {
  }
  void AckPeer(const string& uuid) {
    latch_.CountDown();
  }
  bool IsDone() {
    return latch_.count() >= n_majority_;
  }
  void Wait() {
    latch_.Wait();
  }

 private:
  int n_majority_;
  CountDownLatch latch_;

};

static const char* kPeerUuid = "a";

void AppendReplicateMessagesToQueue(PeerMessageQueue* queue, int count, int n_majority = 1) {
  for (int i = 0; i < count; i++) {
    gscoped_ptr<OperationPB> op(new OperationPB);
    OpId* id = op->mutable_id();
    id->set_term(i / 7);
    id->set_index(i % 7);
    scoped_refptr<OperationStatus> status(new TestOperationStatus(n_majority));
    queue->AppendOperation(op.Pass(), status);
  }
}

// This tests that the peer gets all the messages in the buffer
TEST(TestConsensusRequestQueue, TestGetAllMessages) {
  PeerMessageQueue queue;
  AppendReplicateMessagesToQueue(&queue, 100);

  ASSERT_STATUS_OK(queue.TrackPeer(kPeerUuid));

  ConsensusRequestPB request;
  ConsensusStatusPB status;
  bool more_pending = false;

  // ask for a request. with normal flags this should get the whole queue.
  queue.RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(request.ops_size(), 100);
  status.mutable_replicated_watermark()->CopyFrom(request.ops(99).id());
  queue.ResponseFromPeer(kPeerUuid, status, &more_pending);
  ASSERT_FALSE(more_pending) << "Queue still had requests pending";

  // if we ask for a new request, it should come back empty
  queue.RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(0, request.ops_size());

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), NULL);
 }

// Tests that the queue is able to track a peer after the beginning
TEST(TestConsensusRequestQueue, TestStartTrackingAfterStart) {
  PeerMessageQueue queue;
  AppendReplicateMessagesToQueue(&queue, 100);

  OpId first_msg;
  first_msg.set_term(7);
  first_msg.set_index(0);
  ASSERT_STATUS_OK(queue.TrackPeer(kPeerUuid, first_msg));

  ConsensusRequestPB request;
  ConsensusStatusPB status;
  bool more_pending = false;

  // ask for a request. with normal flags this should get the whole queue.
  queue.RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(request.ops_size(), 50);
  status.mutable_replicated_watermark()->CopyFrom(request.ops(49).id());
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

  // we' going to add 100 messages to the queue so we make each page
  // fetch 9 of those, for a total of 12 pages. The last page should have a single op
  FLAGS_consensus_max_batch_size_bytes = 9 * page_size_estimator.ByteSize();

  PeerMessageQueue queue;
  AppendReplicateMessagesToQueue(&queue, 100);

  queue.TrackPeer(kPeerUuid);
  bool more_pending = false;

  ConsensusRequestPB request;
  ConsensusStatusPB status;
  for (int i = 0; i < 11; i++) {
    queue.RequestForPeer(kPeerUuid, &request);
    ASSERT_EQ(9, request.ops_size());
    status.mutable_replicated_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
    queue.ResponseFromPeer(kPeerUuid, status, &more_pending);
    ASSERT_TRUE(more_pending);
  }
  queue.RequestForPeer(kPeerUuid, &request);
  ASSERT_EQ(1, request.ops_size());
  status.mutable_replicated_watermark()->CopyFrom(request.ops(request.ops_size() -1).id());
  queue.ResponseFromPeer(kPeerUuid, status, &more_pending);
  ASSERT_FALSE(more_pending);

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), NULL);
}

// Tests that the buffer gets trimmed when messages are not being sent by any peer.
TEST(TestConsensusRequestQueue, TestBufferTrimsWhenMessagesAreNotNeeded) {

  FLAGS_consensus_entry_cache_size_soft_limit_mb = 1;

  PeerMessageQueue queue;
  // this amount of messages will overflow the buffer by about 2Kb
  AppendReplicateMessagesToQueue(&queue, 200000);
  // test that even though we've overflown the buffer it still keeps within bounds
  ASSERT_LE(queue.GetQueuedOperationsSizeBytesForTests(), 1 * 1024 * 1024);
}

}  // namespace consensus
}  // namespace kudu

