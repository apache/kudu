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

#include "kudu/consensus/consensus_queue.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <gtest/gtest_prod.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log-test-base.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/async_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/threadpool.h"

DECLARE_int32(consensus_max_batch_size_bytes);
DECLARE_int32(follower_unavailable_considered_failed_sec);

using kudu::consensus::HealthReportPB;
using std::atomic;
using std::deque;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace consensus {

static const char* kLeaderUuid = "peer-0";
static const char* kPeerUuid = "peer-1";
static const char* kTestTablet = "test-tablet";

class ConsensusQueueTest : public KuduTest {
 public:
  ConsensusQueueTest()
      : schema_(GetSimpleTestSchema()),
        metric_entity_(METRIC_ENTITY_tablet.Instantiate(&metric_registry_, "queue-test")),
        registry_(new log::LogAnchorRegistry),
        quiescing_(false) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    fs_manager_.reset(new FsManager(env_, FsManagerOpts(GetTestPath("fs_root"))));
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());
    CHECK_OK(log::Log::Open(log::LogOptions(),
                            fs_manager_.get(),
                            kTestTablet,
                            schema_,
                            /*schema_version*/0,
                            /*metric_entity*/nullptr,
                            &log_));
    clock_.reset(new clock::HybridClock());
    ASSERT_OK(clock_->Init());

    ASSERT_OK(ThreadPoolBuilder("raft").Build(&raft_pool_));
    time_manager_.reset(new TimeManager(clock_.get(), Timestamp::kMin));
    CloseAndReopenQueue(MinimumOpId(), MinimumOpId());
  }

  void CloseAndReopenQueue(const OpId& replicated_opid, const OpId& committed_opid) {
    queue_.reset(new PeerMessageQueue(
        metric_entity_,
        log_.get(),
        time_manager_.get(),
        FakeRaftPeerPB(kLeaderUuid),
        kTestTablet,
        raft_pool_->NewToken(ThreadPool::ExecutionMode::SERIAL),
        &quiescing_,
        replicated_opid,
        committed_opid));
  }

  virtual void TearDown() OVERRIDE {
    log_->WaitUntilAllFlushed();
    queue_->Close();
  }

  Status AppendReplicateMsg(int term, int index, int payload_size) {
    return queue_->AppendOperation(
        make_scoped_refptr_replicate(CreateDummyReplicate(term,
                                                          index,
                                                          clock_->Now(),
                                                          payload_size).release()));
  }

  RaftPeerPB MakePeer(const std::string& peer_uuid,
                      RaftPeerPB::MemberType member_type) {
    RaftPeerPB peer_pb;
    *peer_pb.mutable_permanent_uuid() = peer_uuid;
    peer_pb.set_member_type(member_type);
    return peer_pb;
  }

  // Updates the peer's watermark in the queue so that it matches
  // the operation we want, since the queue always assumes that
  // when a peer gets tracked it's always tracked starting at the
  // last operation in the queue
  void UpdatePeerWatermarkToOp(ConsensusRequestPB* request,
                               ConsensusResponsePB* response,
                               const OpId& last_received,
                               const OpId& last_received_current_leader,
                               int last_committed_idx,
                               bool* send_more_immediately) {

    queue_->TrackPeer(MakePeer(kPeerUuid, RaftPeerPB::VOTER));
    response->set_responder_uuid(kPeerUuid);

    // Ask for a request. The queue assumes the peer is up-to-date so
    // this should contain no operations.
    vector<ReplicateRefPtr> refs;
    bool needs_tablet_copy;
    ASSERT_OK(queue_->RequestForPeer(kPeerUuid, request, &refs, &needs_tablet_copy));
    ASSERT_FALSE(needs_tablet_copy);
    ASSERT_EQ(request->ops_size(), 0);

    // Refuse saying that the log matching property check failed and
    // that our last operation is actually 'last_received'.
    RefuseWithLogPropertyMismatch(response, last_received, last_received_current_leader);
    response->mutable_status()->set_last_committed_idx(last_committed_idx);
    *send_more_immediately = queue_->ResponseFromPeer(response->responder_uuid(), *response);
    request->Clear();
    response->mutable_status()->Clear();
  }

  // Like the above but uses the last received index as the commtited index.
  void UpdatePeerWatermarkToOp(ConsensusRequestPB* request,
                               ConsensusResponsePB* response,
                               const OpId& last_received,
                               const OpId& last_received_current_leader,
                               bool* send_more_immediately) {
    return UpdatePeerWatermarkToOp(request, response, last_received,
                                   last_received_current_leader,
                                   last_received.index(), send_more_immediately);
  }

  void RefuseWithLogPropertyMismatch(ConsensusResponsePB* response,
                                     const OpId& last_received,
                                     const OpId& last_received_current_leader) {
    ConsensusStatusPB* status = response->mutable_status();
    status->mutable_last_received()->CopyFrom(last_received);
    status->mutable_last_received_current_leader()->CopyFrom(last_received_current_leader);
    ConsensusErrorPB* error = status->mutable_error();
    error->set_code(ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH);
    StatusToPB(Status::IllegalState("LMP failed."), error->mutable_status());
  }

  void WaitForLocalPeerToAckIndex(int index) {
    while (true) {
      const auto leader = queue_->GetTrackedPeerForTests(kLeaderUuid);
      if (leader.last_received.index() >= index) {
        break;
      }
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
  }

  // Sets the last received op on the response, as well as the last committed index.
  void SetLastReceivedAndLastCommitted(ConsensusResponsePB* response,
                                       const OpId& last_received,
                                       const OpId& last_received_current_leader,
                                       int last_committed_idx) {
    *response->mutable_status()->mutable_last_received() = last_received;
    *response->mutable_status()->mutable_last_received_current_leader() =
        last_received_current_leader;
    response->mutable_status()->set_last_committed_idx(last_committed_idx);
  }

  // Like the above but uses the same last_received for current term.
  void SetLastReceivedAndLastCommitted(ConsensusResponsePB* response,
                                       const OpId& last_received,
                                       int last_committed_idx) {
    SetLastReceivedAndLastCommitted(response, last_received, last_received, last_committed_idx);
  }

  // Like the above but just sets the last committed index to have the same index
  // as the last received op.
  void SetLastReceivedAndLastCommitted(ConsensusResponsePB* response,
                                       const OpId& last_received) {
    SetLastReceivedAndLastCommitted(response, last_received, last_received.index());
  }

 protected:
  const Schema schema_;
  gscoped_ptr<FsManager> fs_manager_;
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  scoped_refptr<log::Log> log_;
  unique_ptr<ThreadPool> raft_pool_;
  unique_ptr<TimeManager> time_manager_;
  gscoped_ptr<PeerMessageQueue> queue_;
  scoped_refptr<log::LogAnchorRegistry> registry_;
  unique_ptr<clock::Clock> clock_;
  atomic<bool> quiescing_;
};

// Observer of a PeerMessageQueue that tracks the notifications sent to
// observers.
class SimpleObserver : public PeerMessageQueueObserver {
 public:
  SimpleObserver() = default;

  void NotifyPeerToStartElection(const string& peer_uuid) override {
    peers_to_start_election_.emplace_back(peer_uuid);
  }

  // Other notifications aren't implemented. Just no-op.
  void NotifyCommitIndex(int64_t /*commit_index*/) override {}
  void NotifyTermChange(int64_t /*term*/) override {}
  void NotifyFailedFollower(const string& /*peer_uuid*/, int64_t /*term*/,
                            const string& /*reason*/) override {}
  void NotifyPeerToPromote(const string& /*peer_uuid*/) override {}
  void NotifyPeerHealthChange() override {}

 private:
  FRIEND_TEST(ConsensusQueueTest, TestTransferLeadershipWhenAppropriate);

  // The following track the notifications sent in chronological order.
  deque<string> peers_to_start_election_;
};

// Test that the leader consensus queue will only attempt to trigger elections
// when appropriate.
TEST_F(ConsensusQueueTest, TestTransferLeadershipWhenAppropriate) {
  SimpleObserver observer;
  queue_->RegisterObserver(&observer);
  RaftConfigPB config = BuildRaftConfigPBForTests(/*num_voters*/2);
  queue_->SetLeaderMode(kMinimumOpIdIndex, kMinimumTerm, config);
  RaftPeerPB follower = MakePeer(kPeerUuid, RaftPeerPB::VOTER);
  queue_->TrackPeer(follower);

  AppendReplicateMessagesToQueue(queue_.get(), clock_.get(), 1, 10);
  WaitForLocalPeerToAckIndex(10);

  ConsensusResponsePB peer_response;
  peer_response.set_responder_term(1);
  peer_response.set_responder_uuid(kPeerUuid);
  SetLastReceivedAndLastCommitted(&peer_response, MakeOpId(1, 9), MinimumOpId().index());

  int elections_so_far = 0;
  // Simulates receiving the peer's response and checks that, upon receiving
  // it, the PeerMessageQueue either did or didn't notify that the peer should
  // start an election.
  auto verify_elections = [&] (bool election_happened) {
    ASSERT_TRUE(queue_->ResponseFromPeer(kPeerUuid, peer_response));
    // Notifications are communicated via the Raft threadpool, so wait for any
    // such notifying tasks to finish.
    raft_pool_->Wait();
    if (election_happened) {
      elections_so_far++;
    }
    ASSERT_EQ(elections_so_far, observer.peers_to_start_election_.size());
  };
  // We haven't begun watching for a successor yet and our conditions aren't
  // met for this peer to become a leader.
  NO_FATALS(verify_elections(/*election_happened*/false));

  // Even after waiting for a successor, this peer isn't ready yet.
  queue_->BeginWatchForSuccessor(boost::none);
  NO_FATALS(verify_elections(/*election_happened*/false));

  // Once the peer says it's gotten the last-appended op, we should be good to
  // transfer leadership to it.
  SetLastReceivedAndLastCommitted(&peer_response, MakeOpId(1, 10), MinimumOpId().index());
  NO_FATALS(verify_elections(/*election_happened*/true));

  // After we've triggered our election, we shouldn't trigger another.
  NO_FATALS(verify_elections(/*election_happened*/false));

  // And if we try to step down but specify a different peer, we also won't try
  // electing the peer in-hand.
  queue_->BeginWatchForSuccessor(boost::make_optional<string>("different-peer"));
  NO_FATALS(verify_elections(/*election_happened*/false));

  // Even if we begin quiescing, because we're looking for a specific
  // successor, we shouldn't see an election.
  quiescing_ = true;
  NO_FATALS(verify_elections(/*election_happened*/false));

  // If we stop watching for that successor and we're quiescing, we'll trigger
  // elections.
  queue_->EndWatchForSuccessor();
  for (int i = 0; i < 3; i++) {
    NO_FATALS(verify_elections(/*election_happened*/true));
  }

  // If the peer weren't a voter, we would also not trigger elections.
  config.mutable_peers(1)->set_member_type(RaftPeerPB::NON_VOTER);
  queue_->SetLeaderMode(10, 1, config);
  NO_FATALS(verify_elections(/*election_happened*/false));
}

// Tests that the queue is able to track a peer when it starts tracking a peer
// after the initial message in the queue. In particular this creates a queue
// with several messages and then starts to track a peer whose watermark
// falls in the middle of the current messages in the queue.
TEST_F(ConsensusQueueTest, TestStartTrackingAfterStart) {
  queue_->SetLeaderMode(kMinimumOpIdIndex, kMinimumTerm, BuildRaftConfigPBForTests(2));
  AppendReplicateMessagesToQueue(queue_.get(), clock_.get(), 1, 100);

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  bool send_more_immediately = false;

  // Peer already has some messages, last one being 7.50
  OpId last_received = MakeOpId(7, 50);
  OpId last_received_current_leader = MinimumOpId();

  UpdatePeerWatermarkToOp(&request, &response, last_received,
                          last_received_current_leader, &send_more_immediately);
  ASSERT_TRUE(send_more_immediately);

  // Getting a new request should get all operations after 7.50
  vector<ReplicateRefPtr> refs;
  bool needs_tablet_copy;
  ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
  ASSERT_FALSE(needs_tablet_copy);
  ASSERT_EQ(50, request.ops_size());

  SetLastReceivedAndLastCommitted(&response, request.ops(49).id());
  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_FALSE(send_more_immediately) << "Queue still had requests pending";

  // if we ask for a new request, it should come back empty
  ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
  ASSERT_FALSE(needs_tablet_copy);
  ASSERT_EQ(0, request.ops_size());

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), nullptr);
}

// Tests that the peers gets the messages pages, with the size of a page
// being 'consensus_max_batch_size_bytes'
TEST_F(ConsensusQueueTest, TestGetPagedMessages) {
  queue_->SetLeaderMode(kMinimumOpIdIndex, kMinimumTerm, BuildRaftConfigPBForTests(2));

  // helper to estimate request size so that we can set the max batch size appropriately
  // Note: This estimator must be precise, as it is used to set the max batch size.
  // In order for the estimate to be correct, all members of the request protobuf must be set.
  // If not all fields are set, this will set the batch size to be too small to hold the expected
  // number of ops.
  ConsensusRequestPB page_size_estimator;
  page_size_estimator.set_caller_term(14);
  page_size_estimator.set_committed_index(0);
  page_size_estimator.set_all_replicated_index(0);
  page_size_estimator.set_last_idx_appended_to_leader(0);
  page_size_estimator.mutable_preceding_id()->CopyFrom(MinimumOpId());

  // We're going to add 100 messages to the queue so we make each page fetch 9 of those,
  // for a total of 12 pages. The last page should have a single op.
  const int kOpsPerRequest = 9;
  for (int i = 0; i < kOpsPerRequest; i++) {
    page_size_estimator.mutable_ops()->AddAllocated(
        CreateDummyReplicate(0, 0, clock_->Now(), 0).release());
  }

  // Save the current flag state.
  google::FlagSaver saver;
  FLAGS_consensus_max_batch_size_bytes = page_size_estimator.ByteSize();

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  bool send_more_immediately = false;

  UpdatePeerWatermarkToOp(&request, &response, MinimumOpId(), MinimumOpId(),
                          &send_more_immediately);
  ASSERT_TRUE(send_more_immediately);

  // Append the messages after the queue is tracked. Otherwise the ops might
  // get evicted from the cache immediately and the requests below would
  // result in async log reads instead of cache hits.
  AppendReplicateMessagesToQueue(queue_.get(), clock_.get(), 1, 100);

  OpId last;
  for (int i = 0; i < 11; i++) {
    VLOG(1) << "Making request " << i;
    vector<ReplicateRefPtr> refs;
    bool needs_tablet_copy;
    ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
    ASSERT_FALSE(needs_tablet_copy);
    ASSERT_EQ(kOpsPerRequest, request.ops_size());
    last = request.ops(request.ops_size() -1).id();
    SetLastReceivedAndLastCommitted(&response, last);
    VLOG(1) << "Faking received up through " << last;
    send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
    ASSERT_TRUE(send_more_immediately);
  }
  vector<ReplicateRefPtr> refs;
  bool needs_tablet_copy;
  ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
  ASSERT_FALSE(needs_tablet_copy);
  ASSERT_EQ(1, request.ops_size());
  last = request.ops(request.ops_size() -1).id();
  SetLastReceivedAndLastCommitted(&response, last);
  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_FALSE(send_more_immediately);

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), nullptr);
}

TEST_F(ConsensusQueueTest, TestPeersDontAckBeyondWatermarks) {
  queue_->SetLeaderMode(kMinimumOpIdIndex, kMinimumTerm, BuildRaftConfigPBForTests(3));
  AppendReplicateMessagesToQueue(queue_.get(), clock_.get(), 1, 100);

  // Wait for the local peer to append all messages
  WaitForLocalPeerToAckIndex(100);

  ASSERT_EQ(queue_->GetMajorityReplicatedIndexForTests(), 0);
  // Since we're tracking a single peer still this should have moved the all
  // replicated watermark to the last op appended to the local log.
  ASSERT_EQ(queue_->GetAllReplicatedIndex(), 100);

  // Start to track the peer after the queue has some messages in it
  // at a point that is halfway through the current messages in the queue.
  OpId first_msg = MakeOpId(7, 50);

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  bool send_more_immediately = false;

  UpdatePeerWatermarkToOp(&request, &response, first_msg, MinimumOpId(), &send_more_immediately);
  ASSERT_TRUE(send_more_immediately);

  // Tracking a peer a new peer should have moved the all replicated watermark back.
  ASSERT_EQ(queue_->GetAllReplicatedIndex(), 0);
  ASSERT_EQ(queue_->GetMajorityReplicatedIndexForTests(), 0);

  vector<ReplicateRefPtr> refs;
  bool needs_tablet_copy;
  ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
  ASSERT_FALSE(needs_tablet_copy);
  ASSERT_EQ(50, request.ops_size());

  AppendReplicateMessagesToQueue(queue_.get(), clock_.get(), 101, 100);

  SetLastReceivedAndLastCommitted(&response, request.ops(49).id());
  response.set_responder_term(28);

  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_TRUE(send_more_immediately) << "Queue didn't have anymore requests pending";

  ASSERT_EQ(queue_->GetMajorityReplicatedIndexForTests(), 100);
  ASSERT_EQ(queue_->GetAllReplicatedIndex(), 100);

  // if we ask for a new request, it should come back with the rest of the messages
  ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
  ASSERT_FALSE(needs_tablet_copy);
  ASSERT_EQ(100, request.ops_size());

  OpId expected = request.ops(99).id();

  SetLastReceivedAndLastCommitted(&response, expected);
  response.set_responder_term(expected.term());
  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_FALSE(send_more_immediately) << "Queue didn't have anymore requests pending";

  WaitForLocalPeerToAckIndex(expected.index());

  ASSERT_EQ(queue_->GetMajorityReplicatedIndexForTests(), expected.index());
  ASSERT_EQ(queue_->GetAllReplicatedIndex(), expected.index());

  // extract the ops from the request to avoid double free
  request.mutable_ops()->ExtractSubrange(0, request.ops_size(), nullptr);
}

TEST_F(ConsensusQueueTest, TestQueueAdvancesCommittedIndex) {
  queue_->SetLeaderMode(kMinimumOpIdIndex, kMinimumTerm, BuildRaftConfigPBForTests(5));
  // Track 4 additional peers (in addition to the local peer)
  queue_->TrackPeer(MakePeer("peer-1", RaftPeerPB::VOTER));
  queue_->TrackPeer(MakePeer("peer-2", RaftPeerPB::VOTER));
  queue_->TrackPeer(MakePeer("peer-3", RaftPeerPB::VOTER));
  queue_->TrackPeer(MakePeer("peer-4", RaftPeerPB::VOTER));

  // Append 10 messages to the queue.
  // This should add messages 0.1 -> 0.7, 1.8 -> 1.10 to the queue.
  AppendReplicateMessagesToQueue(queue_.get(), clock_.get(), 1, 10);
  WaitForLocalPeerToAckIndex(10);

  // Since only the local log has ACKed at this point,
  // the committed_index should be MinimumOpId().
  ASSERT_EQ(queue_->GetCommittedIndex(), 0);

  // NOTE: We don't need to get operations from the queue. The queue
  // only cares about what the peer reported as received, not what was sent.
  ConsensusResponsePB response;
  response.set_responder_term(1);

  bool send_more_immediately;
  OpId last_sent = MakeOpId(0, 5);

  // Ack the first five operations for peer-1.
  response.set_responder_uuid("peer-1");
  SetLastReceivedAndLastCommitted(&response, last_sent, MinimumOpId().index());

  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_TRUE(send_more_immediately);

  // Committed index should be the same
  ASSERT_EQ(queue_->GetCommittedIndex(), 0);

  // Ack the first five operations for peer-2.
  response.set_responder_uuid("peer-2");
  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_TRUE(send_more_immediately);

  // A majority has now replicated up to 0.5: local, 'peer-1', and 'peer-2'.
  ASSERT_EQ(queue_->GetMajorityReplicatedIndexForTests(), 5);
  // However, this leader has appended operations in term 1, so we can't
  // advance the committed index yet.
  ASSERT_EQ(queue_->GetCommittedIndex(), 0);
  // Moreover, 'peer-3' and 'peer-4' have not acked yet, so the "all-replicated"
  // index also cannot advance.
  ASSERT_EQ(queue_->GetAllReplicatedIndex(), 0);

  // Ack all operations for peer-3.
  response.set_responder_uuid("peer-3");
  last_sent = MakeOpId(1, 10);
  SetLastReceivedAndLastCommitted(&response, last_sent, MinimumOpId().index());
  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);

  // peer-3 now has all operations, and the commit index hasn't advanced.
  EXPECT_FALSE(send_more_immediately);

  // Watermarks should remain the same as above: we still have not majority-replicated
  // anything in the current term, so committed index cannot advance.
  ASSERT_EQ(queue_->GetMajorityReplicatedIndexForTests(), 5);
  ASSERT_EQ(queue_->GetCommittedIndex(), 0);
  ASSERT_EQ(queue_->GetAllReplicatedIndex(), 0);

  // Ack the remaining operations for peer-4.
  response.set_responder_uuid("peer-4");
  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
  EXPECT_TRUE(send_more_immediately);

  // Now that a majority of peers have replicated an operation in the queue's
  // term the committed index should advance.
  ASSERT_EQ(queue_->GetMajorityReplicatedIndexForTests(), 10);
  ASSERT_EQ(queue_->GetCommittedIndex(), 10);
  ASSERT_EQ(queue_->GetAllReplicatedIndex(), 5);
}

// Ensure that the acks for a non-voter don't count toward the majority.
TEST_F(ConsensusQueueTest, TestNonVoterAcksDontCountTowardMajority) {
  const auto kOtherVoterPeer = "peer-1";
  const auto kNonVoterPeer = "non-voter-peer-0";

  // 1. Add a non-voter to the config where there are 2 voters.
  queue_->SetLeaderMode(kMinimumOpIdIndex, kMinimumTerm,
                        BuildRaftConfigPBForTests(/*num_voters=*/ 2,
                                                  /*num_non_voters=*/ 1));
  // Track 2 additional peers (in addition to the local peer)
  queue_->TrackPeer(MakePeer(kOtherVoterPeer, RaftPeerPB::VOTER));
  queue_->TrackPeer(MakePeer(kNonVoterPeer, RaftPeerPB::NON_VOTER));

  // 2. Add some writes. Only the local leader immediately acks them, which is
  // not enough to commit in a 2-voter + 1 non-voter config.
  //
  // Append 10 messages to the queue.
  // This should add messages 0.1 -> 0.7, 1.8 -> 1.10 to the queue.
  const int kNumMessages = 10;
  AppendReplicateMessagesToQueue(queue_.get(), clock_.get(),
                                 /*first=*/ 1, /*count=*/ kNumMessages);
  WaitForLocalPeerToAckIndex(kNumMessages);

  // Since only the local log has acked at this point, the committed_index
  // should be 0.
  const int64_t kNoneCommittedIndex = 0;
  ASSERT_EQ(kNoneCommittedIndex, queue_->GetCommittedIndex());

  // 3. Ack the operations from the NON_VOTER peer. The writes will not have
  // been committed yet, because the 2nd VOTER has not yet acked them.
  ConsensusResponsePB response;
  response.set_responder_uuid(kNonVoterPeer);
  const int64_t kCurrentTerm = 1;
  response.set_responder_term(kCurrentTerm);
  SetLastReceivedAndLastCommitted(&response,
                                  /*last_received=*/ MakeOpId(kCurrentTerm, kNumMessages),
                                  /*last_committed_idx=*/ kNoneCommittedIndex);

  bool send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_FALSE(send_more_immediately);

  // Committed index should be the same.
  ASSERT_EQ(kNoneCommittedIndex, queue_->GetCommittedIndex());

  // 4. Send an identical ack from the 2nd VOTER peer. This should cause the
  // operation to be committed.
  response.set_responder_uuid(kOtherVoterPeer);
  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_TRUE(send_more_immediately); // The committed index has increased.

  // The committed index should include the full set of ops now.
  ASSERT_EQ(kNumMessages, queue_->GetCommittedIndex());

  SetLastReceivedAndLastCommitted(&response,
                                  /*last_received=*/ MakeOpId(kCurrentTerm, kNumMessages),
                                  /*last_committed_idx=*/ kNumMessages);

  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_FALSE(send_more_immediately);
}

// In this test we append a sequence of operations to a log
// and then start tracking a peer whose first required operation
// is before the first operation in the queue.
TEST_F(ConsensusQueueTest, TestQueueLoadsOperationsForPeer) {

  OpId opid = MakeOpId(1, 1);

  const int kOpsToAppend = 100;
  for (int i = 1; i <= kOpsToAppend; i++) {
    ASSERT_OK(log::AppendNoOpToLogSync(clock_.get(), log_.get(), &opid));
    // Roll the log every 10 ops
    if (i % 10 == 0) {
      ASSERT_OK(log_->AllocateSegmentAndRollOverForTests());
    }
  }
  ASSERT_OK(log_->WaitUntilAllFlushed());

  ASSERT_OPID_EQ(MakeOpId(1, kOpsToAppend + 1), opid);
  OpId last_logged_opid = MakeOpId(opid.term(), opid.index() - 1);

  // Now reset the queue so that we can pass a new committed index,
  // the last operation in the log.
  CloseAndReopenQueue(last_logged_opid, last_logged_opid);

  queue_->SetLeaderMode(last_logged_opid.index(),
                        last_logged_opid.term(),
                        BuildRaftConfigPBForTests(3));

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  bool send_more_immediately = false;

  // The peer will actually be behind the first operation in the queue.
  // In this case about 50 operations before.
  OpId peers_last_op;
  peers_last_op.set_term(1);
  peers_last_op.set_index(50);

  // Now we start tracking the peer, this negotiation round should let
  // the queue know how far along the peer is.
  NO_FATALS(UpdatePeerWatermarkToOp(&request,
                                    &response,
                                    peers_last_op,
                                    MinimumOpId(),
                                    &send_more_immediately));

  // The queue should reply that there are more messages for the peer.
  ASSERT_TRUE(send_more_immediately);

  // When we get another request for the peer the queue should load
  // the missing operations.
  vector<ReplicateRefPtr> refs;
  bool needs_tablet_copy;
  ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
  ASSERT_FALSE(needs_tablet_copy);
  ASSERT_EQ(request.ops_size(), 50);

  // The messages still belong to the queue so we have to release them.
  request.mutable_ops()->ExtractSubrange(0, request.ops().size(), nullptr);
}

// This tests that the queue is able to handle operation overwriting, i.e. when a
// newly tracked peer reports the last received operations as some operation that
// doesn't exist in the leader's log. In particular it tests the case where a
// new leader starts at term 2 with only a part of the operations of the previous
// leader having been committed.
TEST_F(ConsensusQueueTest, TestQueueHandlesOperationOverwriting) {

  OpId opid = MakeOpId(1, 1);
  // Append 10 messages in term 1 to the log.
  for (int i = 1; i <= 10; i++) {
    ASSERT_OK(log::AppendNoOpToLogSync(clock_.get(), log_.get(), &opid));
    // Roll the log every 3 ops
    if (i % 3 == 0) {
      ASSERT_OK(log_->AllocateSegmentAndRollOverForTests());
    }
  }

  opid = MakeOpId(2, 11);
  // Now append 10 more messages in term 2.
  for (int i = 11; i <= 20; i++) {
    ASSERT_OK(log::AppendNoOpToLogSync(clock_.get(), log_.get(), &opid));
    // Roll the log every 3 ops
    if (i % 3 == 0) {
      ASSERT_OK(log_->AllocateSegmentAndRollOverForTests());
    }
  }

  OpId last_in_log = MakeOpId(opid.term(), opid.index() - 1);
  int64_t committed_index = 15;

  // Now reset the queue so that we can pass a new committed index (15).
  CloseAndReopenQueue(last_in_log, MakeOpId(2, committed_index));

  queue_->SetLeaderMode(committed_index,
                        last_in_log.term(),
                        BuildRaftConfigPBForTests(3));

  // Now get a request for a simulated old leader, which contains more operations
  // in term 1 than the new leader has.
  // The queue should realize that the old leader's last received doesn't exist
  // and send it operations starting at the old leader's committed index.
  ConsensusRequestPB request;
  ConsensusResponsePB response;
  vector<ReplicateRefPtr> refs;
  response.set_responder_uuid(kPeerUuid);
  bool send_more_immediately = false;

  queue_->TrackPeer(MakePeer(kPeerUuid, RaftPeerPB::VOTER));

  // Ask for a request. The queue assumes the peer is up-to-date so
  // this should contain no operations.
  bool needs_tablet_copy;
  ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
  ASSERT_FALSE(needs_tablet_copy);
  ASSERT_EQ(request.ops_size(), 0);
  ASSERT_OPID_EQ(request.preceding_id(), MakeOpId(2, 20));
  ASSERT_EQ(request.committed_index(), committed_index);

  // The old leader was still in term 1 but it increased its term with our request.
  response.set_responder_term(2);

  // We emulate that the old leader had 25 total operations in Term 1 (15 more than we knew about)
  // which were never committed, and that its last known committed index was 5.
  ConsensusStatusPB* status = response.mutable_status();
  status->mutable_last_received()->CopyFrom(MakeOpId(1, 25));
  status->mutable_last_received_current_leader()->CopyFrom(MinimumOpId());
  status->set_last_committed_idx(5);
  ConsensusErrorPB* error = status->mutable_error();
  error->set_code(ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH);
  StatusToPB(Status::IllegalState("LMP failed."), error->mutable_status());

  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
  request.Clear();

  // The queue should reply that there are more operations pending.
  ASSERT_TRUE(send_more_immediately);

  // We're waiting for a two nodes. The all committed watermark should be
  // 0.0 since we haven't had a successful exchange with the 'remote' peer.
  ASSERT_EQ(queue_->GetAllReplicatedIndex(), 0);

  // Test even when a correct peer responds (meaning we actually get to execute
  // watermark advancement) we sill have the same all-replicated watermark.
  ReplicateMsg* replicate = CreateDummyReplicate(2, 21, clock_->Now(), 0).release();
  ASSERT_OK(queue_->AppendOperation(make_scoped_refptr(new RefCountedReplicate(replicate))));
  WaitForLocalPeerToAckIndex(21);

  ASSERT_EQ(queue_->GetAllReplicatedIndex(), 0);

  // Generate another request for the remote peer, which should include
  // all of the ops since the peer's last-known committed index.
  ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
  ASSERT_FALSE(needs_tablet_copy);
  ASSERT_OPID_EQ(MakeOpId(1, 5), request.preceding_id());
  ASSERT_EQ(16, request.ops_size());

  // Now when we respond the watermarks should advance.
  response.mutable_status()->clear_error();
  SetLastReceivedAndLastCommitted(&response, MakeOpId(2, 21), 5);
  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_TRUE(send_more_immediately);

  // Now the watermark should have advanced.
  ASSERT_EQ(queue_->GetAllReplicatedIndex(), 21);

  // The messages still belong to the queue so we have to release them.
  request.mutable_ops()->ExtractSubrange(0, request.ops().size(), nullptr);
}

// Test for a bug where we wouldn't move any watermark back, when overwriting
// operations, which would cause a check failure on the write immediately
// following the overwriting write.
TEST_F(ConsensusQueueTest, TestQueueMovesWatermarksBackward) {
  queue_->SetNonLeaderMode(BuildRaftConfigPBForTests(3));
  // Append a bunch of messages and update as if they were also appeneded to the leader.
  queue_->UpdateLastIndexAppendedToLeader(10);
  AppendReplicateMessagesToQueue(queue_.get(), clock_.get(), 1, 10);
  log_->WaitUntilAllFlushed();

  // Now rewrite some of the operations and wait for the log to append.
  Synchronizer synch;
  CHECK_OK(queue_->AppendOperations(
      { make_scoped_refptr(new RefCountedReplicate(
          CreateDummyReplicate(2, 5, clock_->Now(), 0).release())) },
      synch.AsStatusCallback()));

  // Wait for the operation to be in the log.
  ASSERT_OK(synch.Wait());

  // Having appended index 5, the follower is still 5 ops behind the leader.
  ASSERT_EQ(5, queue_->metrics_.num_ops_behind_leader->value());

  // Without the fix the following append would trigger a check failure
  // in log cache.
  synch.Reset();
  CHECK_OK(queue_->AppendOperations(
      { make_scoped_refptr(new RefCountedReplicate(
          CreateDummyReplicate(2, 6, clock_->Now(), 0).release())) },
      synch.AsStatusCallback()));

  // Wait for the operation to be in the log.
  ASSERT_OK(synch.Wait());

  // Having appended index 6, the follower is still 4 ops behind the leader.
  ASSERT_EQ(4, queue_->metrics_.num_ops_behind_leader->value());

  // The replication watermark on a follower should not advance by virtue of appending
  // entries to the log.
  ASSERT_EQ(0, queue_->GetAllReplicatedIndex());
}

// Tests that we're advancing the watermarks properly and only when the peer
// has a prefix of our log. This also tests for a specific bug that we had. Here's
// the scenario:
// Peer would report:
//   - last received 75.49
//   - last committed 72.31
//
// Queue has messages:
// 72.31-72.45
// 73.46-73.51
// 76.52-76.53
//
// The queue has more messages than the peer, but the peer has messages
// that the queue doesn't and which will be overwritten.
//
// In the first round of negotiation the peer would report LMP mismatch.
// In the second round the queue would try to send it messages starting at 75.49
// but since that message didn't exist in the queue's log it would instead send
// messages starting at 72.31. However, because the batches were big it was only
// able to send a few messages (e.g. up to 72.40).
//
// Since in this last exchange everything went ok (the peer still doesn't know
// that messages will be overwritten later), the queue would mark the exchange
// as successful and the peer's last received would be taken into account when
// calculating watermarks, which was incorrect.
TEST_F(ConsensusQueueTest, TestOnlyAdvancesWatermarkWhenPeerHasAPrefixOfOurLog) {
  FLAGS_consensus_max_batch_size_bytes = 1024 * 10;

  const int kInitialCommittedIndex = 30;
  CloseAndReopenQueue(MakeOpId(72, 30), MakeOpId(82, 30));
  queue_->SetLeaderMode(kInitialCommittedIndex, 76, BuildRaftConfigPBForTests(3));

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  vector<ReplicateRefPtr> refs;

  bool send_more_immediately;
  // We expect the majority replicated watermark to start at the committed index.
  int64_t expected_majority_replicated = kInitialCommittedIndex;
  // We expect the all replicated watermark to be reset when we track a new peer.
  int64_t expected_all_replicated = 0;

  ASSERT_EQ(queue_->GetMajorityReplicatedIndexForTests(), expected_majority_replicated);
  ASSERT_EQ(queue_->GetAllReplicatedIndex(), expected_all_replicated);

  UpdatePeerWatermarkToOp(&request, &response, MakeOpId(75, 49), MinimumOpId(), 31,
                          &send_more_immediately);
  ASSERT_TRUE(send_more_immediately);

  for (int i = 31; i <= 53; i++) {
    if (i <= 45) {
      AppendReplicateMsg(72, i, 1024);
      continue;
    }
    if (i <= 51) {
      AppendReplicateMsg(73, i, 1024);
      continue;
    }
    AppendReplicateMsg(76, i, 1024);
  }

  WaitForLocalPeerToAckIndex(53);

  // When we get operations for this peer we should get them starting immediately after
  // the committed index, for a total of 9 operations.
  bool needs_tablet_copy;
  ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
  ASSERT_FALSE(needs_tablet_copy);
  ASSERT_EQ(request.ops_size(), 9);
  ASSERT_OPID_EQ(request.ops(0).id(), MakeOpId(72, 32));
  const OpId* last_op = &request.ops(request.ops_size() - 1).id();

  // When the peer acks that it received an operation that is not in our current
  // term, it gets ignored in terms of watermark advancement.
  SetLastReceivedAndLastCommitted(&response, MakeOpId(75, 49), *last_op, 31);
  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);
  ASSERT_TRUE(send_more_immediately);

  // We've sent (and received and ack) up to 72.40 from the remote peer
  expected_majority_replicated = expected_all_replicated = 40;

  ASSERT_EQ(queue_->GetMajorityReplicatedIndexForTests(), expected_majority_replicated);
  ASSERT_EQ(queue_->GetAllReplicatedIndex(), expected_all_replicated);

  // Another request for this peer should get another page of messages. Still not
  // on the queue's term (and thus without advancing watermarks).
  request.mutable_ops()->ExtractSubrange(0, request.ops().size(), nullptr);
  ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
  ASSERT_FALSE(needs_tablet_copy);
  ASSERT_EQ(request.ops_size(), 9);
  ASSERT_OPID_EQ(request.ops(0).id(), MakeOpId(72, 41));
  last_op = &request.ops(request.ops_size() - 1).id();

  SetLastReceivedAndLastCommitted(&response, MakeOpId(75, 49), *last_op, 31);
  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);

  // We've now sent (and received an ack) up to 73.39
  expected_majority_replicated = expected_all_replicated = 49;

  ASSERT_EQ(queue_->GetMajorityReplicatedIndexForTests(), expected_majority_replicated);
  ASSERT_EQ(queue_->GetAllReplicatedIndex(), expected_all_replicated);

  // The last page of request should overwrite the peer's operations and the
  // response should finally advance the watermarks.
  request.mutable_ops()->ExtractSubrange(0, request.ops().size(), nullptr);
  ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
  ASSERT_FALSE(needs_tablet_copy);
  ASSERT_EQ(request.ops_size(), 4);
  ASSERT_OPID_EQ(request.ops(0).id(), MakeOpId(73, 50));

  // We're done, both watermarks should be at the end.
  expected_majority_replicated = expected_all_replicated = 53;

  SetLastReceivedAndLastCommitted(&response, MakeOpId(76, 53), 31);
  send_more_immediately = queue_->ResponseFromPeer(response.responder_uuid(), response);

  ASSERT_EQ(queue_->GetMajorityReplicatedIndexForTests(), expected_majority_replicated);
  ASSERT_EQ(queue_->GetAllReplicatedIndex(), expected_all_replicated);

  request.mutable_ops()->ExtractSubrange(0, request.ops().size(), nullptr);
}

// Test that Tablet Copy is triggered when a "tablet not found" error occurs.
TEST_F(ConsensusQueueTest, TestTriggerTabletCopyIfTabletNotFound) {
  queue_->SetLeaderMode(kMinimumOpIdIndex, kMinimumTerm, BuildRaftConfigPBForTests(3));
  AppendReplicateMessagesToQueue(queue_.get(), clock_.get(), 1, 100);

  ConsensusRequestPB request;
  ConsensusResponsePB response;
  response.set_responder_uuid(kPeerUuid);
  queue_->TrackPeer(MakePeer(kPeerUuid, RaftPeerPB::VOTER));

  // Create request for new peer.
  vector<ReplicateRefPtr> refs;
  bool needs_tablet_copy;
  ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
  ASSERT_FALSE(needs_tablet_copy);

  // Peer responds with tablet not found.
  queue_->UpdatePeerStatus(kPeerUuid, PeerStatus::TABLET_NOT_FOUND,
                           Status::NotFound("No such tablet"));

  // On the next request, we should find out that the queue wants us to initiate Tablet Copy.
  request.Clear();
  ASSERT_OK(queue_->RequestForPeer(kPeerUuid, &request, &refs, &needs_tablet_copy));
  ASSERT_TRUE(needs_tablet_copy);

  StartTabletCopyRequestPB tc_req;
  ASSERT_OK(queue_->GetTabletCopyRequestForPeer(kPeerUuid, &tc_req));

  ASSERT_TRUE(tc_req.IsInitialized()) << pb_util::SecureShortDebugString(tc_req);
  ASSERT_EQ(kTestTablet, tc_req.tablet_id());
  ASSERT_EQ(kLeaderUuid, tc_req.copy_peer_uuid());
  ASSERT_EQ(pb_util::SecureShortDebugString(FakeRaftPeerPB(kLeaderUuid).last_known_addr()),
            pb_util::SecureShortDebugString(tc_req.copy_peer_addr()));
}

TEST_F(ConsensusQueueTest, TestFollowerCommittedIndexAndMetrics) {
  queue_->SetNonLeaderMode(BuildRaftConfigPBForTests(3));

  // Emulate a follower sending a request to replicate 10 messages.
  queue_->UpdateLastIndexAppendedToLeader(10);
  AppendReplicateMessagesToQueue(queue_.get(), clock_.get(), 1, 10);
  WaitForLocalPeerToAckIndex(10);

  // The committed_index should be MinimumOpId() since UpdateFollowerWatermarks
  // has not been called.
  ASSERT_EQ(0, queue_->GetCommittedIndex());

  // Update the committed index. In real life, this would be done by the consensus
  // implementation when it receives an updated committed index from the leader.
  queue_->UpdateFollowerWatermarks(/*committed_index=*/ 10,
                                   /*all_replicated_index=*/ 10);
  ASSERT_EQ(10, queue_->GetCommittedIndex());

  // Check the metrics have the right values based on the updated committed index.
  ASSERT_EQ(0, queue_->metrics_.num_majority_done_ops->value());
  ASSERT_EQ(0, queue_->metrics_.num_in_progress_ops->value());
  ASSERT_EQ(0, queue_->metrics_.num_ops_behind_leader->value());

  // Emulate the leader appending up to index 15. The num_ops_behind_leader should jump to 5.
  queue_->UpdateLastIndexAppendedToLeader(15);
  ASSERT_EQ(5, queue_->metrics_.num_ops_behind_leader->value());
}

// Unit test for the PeerMessageQueue::PeerHealthStatus() method.
TEST(ConsensusQueueUnitTest, PeerHealthStatus) {
  static constexpr PeerStatus kPeerStatusesForUnknown[] = {
     PeerStatus::NEW,
     PeerStatus::REMOTE_ERROR,
     PeerStatus::RPC_LAYER_ERROR,
     PeerStatus::TABLET_NOT_FOUND,
     PeerStatus::INVALID_TERM,
     PeerStatus::CANNOT_PREPARE,
     PeerStatus::LMP_MISMATCH,
  };

  RaftPeerPB peer_pb;
  PeerMessageQueue::TrackedPeer peer(peer_pb);
  EXPECT_EQ(HealthReportPB::UNKNOWN, PeerMessageQueue::PeerHealthStatus(peer));
  for (auto status : kPeerStatusesForUnknown) {
    peer.last_exchange_status = status;
    EXPECT_EQ(HealthReportPB::UNKNOWN, PeerMessageQueue::PeerHealthStatus(peer));
  }

  peer.last_exchange_status = PeerStatus::TABLET_FAILED;
  EXPECT_EQ(HealthReportPB::FAILED_UNRECOVERABLE, PeerMessageQueue::PeerHealthStatus(peer));

  peer.last_exchange_status = PeerStatus::OK;
  EXPECT_EQ(HealthReportPB::HEALTHY, PeerMessageQueue::PeerHealthStatus(peer));

  peer.wal_catchup_possible = false;
  EXPECT_EQ(HealthReportPB::FAILED_UNRECOVERABLE, PeerMessageQueue::PeerHealthStatus(peer));

  peer.wal_catchup_possible = true;
  EXPECT_EQ(HealthReportPB::HEALTHY, PeerMessageQueue::PeerHealthStatus(peer));

  peer.last_communication_time -=
      MonoDelta::FromSeconds(FLAGS_follower_unavailable_considered_failed_sec + 1);
  EXPECT_EQ(HealthReportPB::FAILED, PeerMessageQueue::PeerHealthStatus(peer));
  for (auto status : kPeerStatusesForUnknown) {
    peer.last_exchange_status = status;
    EXPECT_EQ(HealthReportPB::FAILED, PeerMessageQueue::PeerHealthStatus(peer));
  }

  peer.last_exchange_status = PeerStatus::TABLET_FAILED;
  EXPECT_EQ(HealthReportPB::FAILED_UNRECOVERABLE, PeerMessageQueue::PeerHealthStatus(peer));

  peer.last_exchange_status = PeerStatus::OK;
  EXPECT_EQ(HealthReportPB::FAILED, PeerMessageQueue::PeerHealthStatus(peer));

  peer.wal_catchup_possible = false;
  EXPECT_EQ(HealthReportPB::FAILED_UNRECOVERABLE, PeerMessageQueue::PeerHealthStatus(peer));

  for (auto status : kPeerStatusesForUnknown) {
    peer.last_exchange_status = status;
    EXPECT_EQ(HealthReportPB::FAILED_UNRECOVERABLE, PeerMessageQueue::PeerHealthStatus(peer));
  }

  peer.last_exchange_status = PeerStatus::OK;
  EXPECT_EQ(HealthReportPB::FAILED_UNRECOVERABLE, PeerMessageQueue::PeerHealthStatus(peer));

  peer.last_exchange_status = PeerStatus::TABLET_FAILED;
  EXPECT_EQ(HealthReportPB::FAILED_UNRECOVERABLE, PeerMessageQueue::PeerHealthStatus(peer));
}

}  // namespace consensus
}  // namespace kudu
