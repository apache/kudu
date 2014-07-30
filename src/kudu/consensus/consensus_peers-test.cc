// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>

#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_anchor_registry.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace consensus {

using log::Log;
using log::LogOptions;
using log::OpIdAnchorRegistry;
using metadata::QuorumPeerPB;

const char* kTabletId = "test-peers-tablet";
const char* kLeaderUuid = "test-peers-leader";

class ConsensusPeersTest : public KuduTest {
 public:
  ConsensusPeersTest()
    :  metric_context_(&metric_registry_, "peer-test"),
       message_queue_(metric_context_) {}

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    fs_manager_.reset(new FsManager(env_.get(), test_dir_));
  }

  void NewLocalPeer(Log* log, const string& peer_name, gscoped_ptr<Peer>* peer) {
    QuorumPeerPB peer_pb;
    peer_pb.set_permanent_uuid(peer_name);
    ASSERT_STATUS_OK(Peer::NewLocalPeer(peer_pb,
                                        kTabletId,
                                        kLeaderUuid,
                                        &message_queue_,
                                        log,
                                        log::MinimumOpId(),
                                        peer));
  }

  NoOpTestPeerProxy* NewRemotePeer(const string& peer_name,
                               gscoped_ptr<Peer>* peer) {
    QuorumPeerPB peer_pb;
    peer_pb.set_permanent_uuid(peer_name);
    gscoped_ptr<PeerProxy> proxy;
    CHECK_OK(peer_proxy_factory_.NewProxy(peer_pb, &proxy));
    NoOpTestPeerProxy* proxy_ptr = down_cast<NoOpTestPeerProxy*, PeerProxy>(proxy.get());
    CHECK_OK(Peer::NewRemotePeer(peer_pb,
                                 kTabletId,
                                 kLeaderUuid,
                                 &message_queue_,
                                 proxy.Pass(),
                                 peer));
    return proxy_ptr;
  }

  void CheckLastLogEntry(Log* log, int term, int index) {
    OpId id;
    log->GetLastEntryOpId(&id);
    ASSERT_EQ(id.term(), term);
    ASSERT_EQ(id.index(), index);
  }

  void CheckLastRemoteEntry(NoOpTestPeerProxy* proxy, int term, int index) {
    OpId id;
    id.CopyFrom(proxy->last_status().replicated_watermark());
    ASSERT_EQ(id.term(), term);
    ASSERT_EQ(id.index(), index);
  }

  TestOpStatusTracker* test_status(OperationStatusTracker* status) {
    return down_cast<TestOpStatusTracker*, OperationStatusTracker>(status);
  }

 protected:
  MetricRegistry metric_registry_;
  MetricContext metric_context_;
  PeerMessageQueue message_queue_;
  gscoped_ptr<FsManager> fs_manager_;
  LogOptions options_;
  vector<scoped_refptr<OperationStatusTracker> > statuses_;
  NoOpTestPeerProxyFactory peer_proxy_factory_;
};

// Tests that a local peer is correctly built and tracked
// by the message queue.
// After the operations are considered done the log should
// reflect the replicated messages.
TEST_F(ConsensusPeersTest, TestLocalPeer) {
  gscoped_ptr<Peer> local_peer;
  gscoped_ptr<Log> log;
  CHECK_OK(Log::Open(options_,
                     fs_manager_.get(),
                     kTabletId,
                     NULL,
                     &log));
  NewLocalPeer(log.get(), "local-peer", &local_peer);
  // Test that the local peer handles status-only requests.
  local_peer->SignalRequest(true);

  // Append a bunch of messages to the queue
  AppendReplicateMessagesToQueue(&message_queue_, 1, 20, 1, 1, "", &statuses_);
  // signal the peer there are requests pending.
  local_peer->SignalRequest();
  // now wait on the status of the last operation
  // this will complete once the peer has logged all
  // requests.
  statuses_[19]->Wait();
  // verify that the requests are in fact logged.
  CheckLastLogEntry(log.get(), 2, 6);
}

// Tests that a remote peer is correctly built and tracked
// by the message queue.
// After the operations are considered done the proxy (which
// simulates the other endpoint) should reflect the replicated
// messages.
TEST_F(ConsensusPeersTest, TestRemotePeer) {
  gscoped_ptr<Peer> remote_peer;
  NoOpTestPeerProxy* proxy = NewRemotePeer("remote-peer", &remote_peer);

  // Append a bunch of messages to the queue
  AppendReplicateMessagesToQueue(&message_queue_, 1, 20, 1, 1, "", &statuses_);
  // signal the peer there are requests pending.
  remote_peer->SignalRequest();
  // now wait on the status of the last operation
  // this will complete once the peer has logged all
  // requests.
  statuses_[19]->Wait();
  // verify that the replicated watermark corresponds to the last replicated
  // message.
  CheckLastRemoteEntry(proxy, 2, 6);
}

TEST_F(ConsensusPeersTest, TestLocalAndRemotePeers) {
  gscoped_ptr<Peer> local_peer;
  gscoped_ptr<Log> log;
  CHECK_OK(Log::Open(options_,
                     fs_manager_.get(),
                     kTabletId,
                     NULL,
                     &log));
  // Create a set of peers
  NewLocalPeer(log.get(), "local-peer", &local_peer);

  gscoped_ptr<Peer> remote_peer1;
  NoOpTestPeerProxy* remote_peer1_proxy = NewRemotePeer("remote-peer1", &remote_peer1);

  gscoped_ptr<Peer> remote_peer2;
  NoOpTestPeerProxy* remote_peer2_proxy = NewRemotePeer("remote-peer2", &remote_peer2);

  // Delay the response from the second remote peer.
  remote_peer2_proxy->DelayResponse();

  // Append one message to the queue with majority = 2.
  AppendReplicateMessagesToQueue(&message_queue_, 1, 1, 2, 3, "", &statuses_);

  local_peer->SignalRequest();
  remote_peer1->SignalRequest();
  remote_peer2->SignalRequest();

  // Now wait for the message to be replicated, this should succeed since
  // majority = 2 and only one peer was delayed.
  statuses_[0]->Wait();
  CheckLastLogEntry(log.get(), 0, 1);
  CheckLastRemoteEntry(remote_peer1_proxy, 0, 1);
  ASSERT_EQ(2, test_status(statuses_[0].get())->replicated_count());

  ASSERT_STATUS_OK(remote_peer2_proxy->Respond());
  // Wait until all peers have replicated the message, otherwise
  // when we add the next one remote_peer2 might find the next message
  // in the queue and will replicate it, which is not what we want.
  test_status(statuses_[0].get())->WaitAllReplicated();

  // Now append another message to the queue
  AppendReplicateMessagesToQueue(&message_queue_, 2, 1, 2, 3, "", &statuses_);

  // Signal a single peer
  remote_peer1->SignalRequest();
  // IsDone should be false since only a single peer replicated the message
  ASSERT_FALSE(statuses_[1]->IsDone());
  // Signal another peer
  remote_peer2->SignalRequest();
  // We should now be able to wait on the status until the two peers (a majority)
  // have replicated the message.
  statuses_[1]->Wait();
  ASSERT_TRUE(statuses_[1]->IsDone());
}

}  // namespace consensus
}  // namespace kudu

