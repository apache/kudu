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

#include "kudu/consensus/consensus_peers.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/messenger.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/threadpool.h"

METRIC_DECLARE_entity(tablet);
METRIC_DECLARE_entity(server);

using kudu::log::Log;
using kudu::log::LogOptions;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using std::shared_ptr;
using std::string;
using std::unique_ptr;

namespace kudu {
namespace consensus {

const char* kTabletId = "test-peers-tablet";
const char* kLeaderUuid = "peer-0";
const char* kFollowerUuid = "peer-1";

template class OneTimeUsePeerProxyFactory<DelayablePeerProxy<NoOpTestPeerProxy>>;
template class OneTimeUsePeerProxyFactory<MockedPeerProxy>;

class ConsensusPeersTest : public KuduTest {
 public:
  ConsensusPeersTest()
      : metric_entity_server_(METRIC_ENTITY_server.Instantiate(
            &metric_registry_, "consensus-peer-test::server")),
        metric_entity_tablet_(METRIC_ENTITY_tablet.Instantiate(
            &metric_registry_, "consensus-peer-test::tablet")),
        schema_(GetSimpleTestSchema()) {
    CHECK_OK(ThreadPoolBuilder("test-raft-pool").Build(&raft_pool_));
    raft_pool_token_ = raft_pool_->NewToken(ThreadPool::ExecutionMode::CONCURRENT);
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    fs_manager_.reset(new FsManager(env_, FsManagerOpts(GetTestPath("fs_root"))));
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());
    SchemaPtr schema_ptr = std::make_shared<Schema>(schema_);
    ASSERT_OK(Log::Open(options_,
                        fs_manager_.get(),
                        /*file_cache*/nullptr,
                        kTabletId,
                        schema_ptr,
                        0, // schema_version
                        /*metric_entity*/nullptr,
                        &log_));
    clock_.reset(new clock::HybridClock(metric_entity_server_));
    ASSERT_OK(clock_->Init());

    time_manager_.reset(new TimeManager(clock_.get(), Timestamp::kMin));

    message_queue_.reset(new PeerMessageQueue(
        metric_entity_tablet_,
        log_.get(),
        time_manager_.get(),
        FakeRaftPeerPB(kLeaderUuid),
        kTabletId,
        raft_pool_->NewToken(ThreadPool::ExecutionMode::SERIAL),
        /*server_quiescing*/nullptr,
        MinimumOpId(),
        MinimumOpId()));

    MessengerBuilder bld("test");
    ASSERT_OK(bld.Build(&messenger_));
  }

  virtual void TearDown() OVERRIDE {
    if (log_) {
      ASSERT_OK(log_->WaitUntilAllFlushed());
    }
    if (messenger_) {
      messenger_->Shutdown();
    }
    if (raft_pool_) {
      // Make sure to drain any tasks from the pool we're using for our delayable
      // proxy before destructing the queue.
      raft_pool_->Wait();
    }
  }

  DelayablePeerProxy<NoOpTestPeerProxy>* NewRemotePeer(
      const string& peer_name,
      shared_ptr<Peer>* peer) {
    RaftPeerPB peer_pb;
    peer_pb.set_permanent_uuid(peer_name);
    peer_pb.set_member_type(RaftPeerPB::VOTER);
    std::unique_ptr<DelayablePeerProxy<NoOpTestPeerProxy>> proxy(
        new DelayablePeerProxy<NoOpTestPeerProxy>(
            raft_pool_.get(), new NoOpTestPeerProxy(raft_pool_.get(), peer_pb)));
    auto* proxy_ptr = proxy.get();
    OneTimeUsePeerProxyFactory<DelayablePeerProxy<NoOpTestPeerProxy>> factory(
        messenger_, proxy.release());
    Peer::NewRemotePeer(std::move(peer_pb),
                        kTabletId,
                        kLeaderUuid,
                        message_queue_.get(),
                        raft_pool_token_.get(),
                        &factory,
                        peer);
    return proxy_ptr;
  }

  void CheckLastRemoteEntry(DelayablePeerProxy<NoOpTestPeerProxy>* proxy, int term, int index) {
    OpId id;
    id.CopyFrom(proxy->proxy()->last_received());
    ASSERT_EQ(id.term(), term);
    ASSERT_EQ(id.index(), index);
  }

  // Registers a callback triggered when the op with the provided term and index
  // is committed in the test consensus impl.
  // This must be called _before_ the operation is committed.
  void WaitForCommitIndex(int index) {
    ASSERT_EVENTUALLY([&]() {
        ASSERT_GE(message_queue_->GetCommittedIndex(), index);
      });
  }

 protected:
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_server_;
  scoped_refptr<MetricEntity> metric_entity_tablet_;
  unique_ptr<FsManager> fs_manager_;
  scoped_refptr<Log> log_;
  unique_ptr<ThreadPool> raft_pool_;
  unique_ptr<TimeManager> time_manager_;
  unique_ptr<PeerMessageQueue> message_queue_;
  const Schema schema_;
  LogOptions options_;
  unique_ptr<ThreadPoolToken> raft_pool_token_;
  unique_ptr<clock::Clock> clock_;
  shared_ptr<Messenger> messenger_;

};


// Tests that a remote peer is correctly built and tracked
// by the message queue.
// After the operations are considered done the proxy (which
// simulates the other endpoint) should reflect the replicated
// messages.
TEST_F(ConsensusPeersTest, TestRemotePeer) {
  // We use a majority size of 2 since we make one fake remote peer
  // in addition to our real local log.
  message_queue_->SetLeaderMode(kMinimumOpIdIndex,
                                kMinimumTerm,
                                BuildRaftConfigPBForTests(3));

  shared_ptr<Peer> remote_peer;
  DelayablePeerProxy<NoOpTestPeerProxy>* proxy =
      NewRemotePeer(kFollowerUuid, &remote_peer);

  // Append a bunch of messages to the queue
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_.get(), 1, 20);

  // signal the peer there are requests pending.
  remote_peer->SignalRequest();
  // now wait on the status of the last operation
  // this will complete once the peer has logged all
  // requests.
  WaitForCommitIndex(20);
  // verify that the replicated watermark corresponds to the last replicated
  // message.
  CheckLastRemoteEntry(proxy, 2, 20);
}

TEST_F(ConsensusPeersTest, TestRemotePeers) {
  message_queue_->SetLeaderMode(kMinimumOpIdIndex,
                                kMinimumTerm,
                                BuildRaftConfigPBForTests(3));

  // Create a set of remote peers
  shared_ptr<Peer> remote_peer1;
  DelayablePeerProxy<NoOpTestPeerProxy>* remote_peer1_proxy =
      NewRemotePeer("peer-1", &remote_peer1);

  shared_ptr<Peer> remote_peer2;
  DelayablePeerProxy<NoOpTestPeerProxy>* remote_peer2_proxy =
      NewRemotePeer("peer-2", &remote_peer2);

  // Delay the response from the second remote peer.
  remote_peer2_proxy->DelayResponse();

  // Append one message to the queue.
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_.get(), 1, 1);

  OpId first = MakeOpId(0, 1);

  remote_peer1->SignalRequest();
  remote_peer2->SignalRequest();

  // Now wait for the message to be replicated, this should succeed since
  // majority = 2 and only one peer was delayed. The majority is made up
  // of remote-peer1 and the local log.
  WaitForCommitIndex(first.index());

  ASSERT_OPID_EQ(first, message_queue_->GetLastOpIdInLog());
  CheckLastRemoteEntry(remote_peer1_proxy, first.term(), first.index());

  remote_peer2_proxy->Respond(TestPeerProxy::kUpdate);
  // Wait until all peers have replicated the message, otherwise
  // when we add the next one remote_peer2 might find the next message
  // in the queue and will replicate it, which is not what we want.
  while (message_queue_->GetAllReplicatedIndex() != first.index()) {
    SleepFor(MonoDelta::FromMilliseconds(1));
  }

  // Now append another message to the queue
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_.get(), 2, 1);

  // We should not see it committed, even after 10ms,
  // since only the local peer replicates the message.
  SleepFor(MonoDelta::FromMilliseconds(10));
  ASSERT_LT(message_queue_->GetCommittedIndex(), 2);

  // Signal one of the two remote peers.
  remote_peer1->SignalRequest();
  // We should now be able to wait for it to replicate, since two peers (a majority)
  // have replicated the message.
  WaitForCommitIndex(2);
}

// Regression test for KUDU-699: even if a peer isn't making progress,
// and thus always has data pending, we should be able to close the peer.
TEST_F(ConsensusPeersTest, TestCloseWhenRemotePeerDoesntMakeProgress) {
  message_queue_->SetLeaderMode(kMinimumOpIdIndex,
                                kMinimumTerm,
                                BuildRaftConfigPBForTests(3));

  auto mock_proxy = new MockedPeerProxy(raft_pool_.get());
  OneTimeUsePeerProxyFactory<MockedPeerProxy> factory(messenger_, mock_proxy);
  shared_ptr<Peer> peer;
  Peer::NewRemotePeer(FakeRaftPeerPB(kFollowerUuid),
                      kTabletId,
                      kLeaderUuid,
                      message_queue_.get(),
                      raft_pool_token_.get(),
                      &factory,
                      &peer);

  // Make the peer respond without making any progress -- it always returns
  // that it has only replicated op 0.0. When we see the response, we always
  // decide that more data is pending, and we want to send another request.
  ConsensusResponsePB peer_resp;
  peer_resp.set_responder_uuid(kFollowerUuid);
  peer_resp.set_responder_term(0);
  peer_resp.mutable_status()->mutable_last_received()->CopyFrom(
      MakeOpId(0, 0));
  peer_resp.mutable_status()->mutable_last_received_current_leader()->CopyFrom(
      MakeOpId(0, 0));
  peer_resp.mutable_status()->set_last_committed_idx(0);

  mock_proxy->set_update_response(peer_resp);

  // Add an op to the queue and start sending requests to the peer.
  AppendReplicateMessagesToQueue(message_queue_.get(), clock_.get(), 1, 1);
  peer->SignalRequest(true);

  // We should be able to close the peer even though it has more data pending.
  peer->Close();
}

TEST_F(ConsensusPeersTest, TestDontSendOneRpcPerWriteWhenPeerIsDown) {
  message_queue_->SetLeaderMode(kMinimumOpIdIndex,
                                kMinimumTerm,
                                BuildRaftConfigPBForTests(3));

  auto* mock_proxy = new MockedPeerProxy(raft_pool_.get());
  OneTimeUsePeerProxyFactory<MockedPeerProxy> factory(messenger_, mock_proxy);
  shared_ptr<Peer> peer;
  Peer::NewRemotePeer(FakeRaftPeerPB(kFollowerUuid),
                      kTabletId,
                      kLeaderUuid,
                      message_queue_.get(),
                      raft_pool_token_.get(),
                      &factory,
                      &peer);

  // Initial response has to be successful -- otherwise we'll consider the peer
  // "new" and only send heartbeat RPCs.
  ConsensusResponsePB initial_resp;
  initial_resp.set_responder_uuid(kFollowerUuid);
  initial_resp.set_responder_term(0);
  initial_resp.mutable_status()->mutable_last_received()->CopyFrom(
      MakeOpId(1, 1));
  initial_resp.mutable_status()->mutable_last_received_current_leader()->CopyFrom(
      MakeOpId(1, 1));
  // We have to set the last_committed_index to 1 to avoid a tight loop
  // where the peer manager keeps trying to update the peer's committed
  // index.
  initial_resp.mutable_status()->set_last_committed_idx(1);
  mock_proxy->set_update_response(initial_resp);

  AppendReplicateMessagesToQueue(message_queue_.get(), clock_.get(), 1, 1);
  peer->SignalRequest(true);

  // Now wait for the message to be replicated, this should succeed since
  // the local (leader) peer always acks and the follower also acked this time.
  WaitForCommitIndex(1);

  // Set up the peer to respond with an error.
  ConsensusResponsePB error_resp;
  error_resp.mutable_error()->set_code(tserver::TabletServerErrorPB::UNKNOWN_ERROR);
  StatusToPB(Status::NotFound("fake error"), error_resp.mutable_error()->mutable_status());
  mock_proxy->set_update_response(error_resp);

  // Add a bunch of messages to the queue.
  for (int i = 2; i <= 100; i++) {
    AppendReplicateMessagesToQueue(message_queue_.get(), clock_.get(), i, 1);
    peer->SignalRequest(false);
    SleepFor(MonoDelta::FromMilliseconds(2));
  }

  // Check that we didn't attempt to send one UpdateConsensus call per
  // Write. 100 writes might have taken a second or two, though, so it's
  // OK to have called UpdateConsensus() a few times due to regularly
  // scheduled heartbeats.
  ASSERT_LT(mock_proxy->update_count(), 5);
}

}  // namespace consensus
}  // namespace kudu

