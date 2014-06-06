// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_CONSENSUS_PEERS_H_
#define KUDU_CONSENSUS_CONSENSUS_PEERS_H_

#include <string>
#include <tr1/memory>

#include "rpc/response_callback.h"
#include "server/metadata.pb.h"
#include "util/locks.h"
#include "util/status.h"
#include "util/countdown_latch.h"
#include "util/resettable_heartbeater.h"

namespace kudu {
class HostPort;

namespace log {
class Log;
}

namespace rpc {
class Messenger;
class RpcController;
}

namespace tserver {
class TabletServerServiceProxy;
}

namespace consensus {
class ConsensusRequestPB;
class ConsensusResponsePB;
class ConsensusStatusPB;
class OpId;
class PeerProxy;
class PeerImpl;
class PeerMessageQueue;

// A peer in consensus (local or remote).
//
// Leaders use peers to update the local Log and remote replicas.
//
// Peers are owned by the consensus implementation and do not keep
// state aside from whether there are requests pending or if requests
// are being processed.
//
// There are two external actions that trigger a state change:
//
// SignalRequest(): Called by the consensus implementation, notifies
// that the queue contains messages to be processed.
//
// ProcessResponse() Called a response from a peer is received.
//
// The following state diagrams describe what happens when a state
// changing method is called.
//
//                        +
//                        |
//       SignalRequest()  |
//                        |
//                        |
//                        v
//              +------------------+
//       +------+    processing ?  +-----+
//       |      +------------------+     |
//       |                               |
//       | Yes                           | No
//       |                               |
//       v                               v
//     return                      ProcessNextRequest()
//                                 processing = true
//                                 - get reqs. from queue
//                                 - update peer async
//                                 return
//
//                         +
//                         |
//      ProcessResponse()  |
//      processing = false |
//                         v
//               +------------------+
//        +------+   more pending?  +-----+
//        |      +------------------+     |
//        |                               |
//        | Yes                           | No
//        |                               |
//        v                               v
//  SignalRequest()                    return
//
class Peer {
 public:
  // Initializes a peer and get its status.
  Status Init();

  // Signals that this peer has a new request to replicate/store.
  // 'force_if_queue_empty' indicates whether the peer should force
  // send the request even if the queue is empty. This is used for
  // status-only requests.
  Status SignalRequest(bool force_if_queue_empty = false);

  // Signals that a response was received from the peer.
  void ProcessResponse(const ConsensusStatusPB& status);

  // Signals there was an error sending the request to the peer.
  void ProcessResponseError(const Status& status);

  const metadata::QuorumPeerPB& peer_pb() const { return peer_pb_; }

  // Returns the PeerProxy if this is a remote peer or NULL if it
  // isn't. Used for tests to fiddle with the proxy and emulate remote
  // behavior.
  PeerProxy* GetPeerProxyForTests();

  void Close();

  ~Peer();

  // Creates a new local peer and makes the queue track it.
  static Status NewLocalPeer(const metadata::QuorumPeerPB& peer_pb,
                             const std::string& tablet_id,
                             const std::string& leader_uuid,
                             PeerMessageQueue* queue,
                             log::Log* log,
                             const OpId& initial_op,
                             gscoped_ptr<Peer>* peer);

  // Creates a new remote peer and makes the queue track it.
  static Status NewRemotePeer(const metadata::QuorumPeerPB& peer_pb,
                              const std::string& tablet_id,
                              const std::string& leader_uuid,
                              PeerMessageQueue* queue,
                              gscoped_ptr<PeerProxy> proxy,
                              gscoped_ptr<Peer>* peer);

 protected:
  // ctor for the local peer
  Peer(const metadata::QuorumPeerPB& peer,
       const std::string& tablet_id,
       const std::string& leader_uuid,
       PeerMessageQueue* queue,
       log::Log* log,
       const OpId& initial_op);

  // ctor for a remote peer
  Peer(const metadata::QuorumPeerPB& peer,
       const std::string& tablet_id,
       const std::string& leader_uuid,
       PeerMessageQueue* queue,
       gscoped_ptr<PeerProxy> proxy);

  metadata::QuorumPeerPB peer_pb_;
  gscoped_ptr<PeerImpl> peer_impl_;
  PeerMessageQueue* queue_;
  bool processing_;
  uint64_t failed_attempts_;

  CountDownLatch outstanding_req_latch_;

  // lock that protects Peer state changes
  mutable simple_spinlock peer_lock_;

  // Heartbeater for remote peer implementations.
  // This will send status only requests to the remote peers
  // whenever we go more than 'FLAGS_leader_heartbeat_interval_ms'
  // without sending actual data.
  gscoped_ptr<ResettableHeartbeater> heartbeater_;

  enum State {
    kPeerCreated,
    kPeerIntitialized,
    kPeerClosed
  };

  State state_;
};

// A proxy to another peer. Usually a thin wrapper around an rpc proxy but can
// be replaced for tests.
class PeerProxy {
 public:

  // Sends a request, asynchronously, to a remote peer.
  virtual Status UpdateAsync(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response,
                             rpc::RpcController* controller,
                             const rpc::ResponseCallback& callback) = 0;

  virtual ~PeerProxy() {}
};

// A peer proxy factory. Usually just obtains peers through the rpc implementation
// but can be replaced for tests.
class PeerProxyFactory {
 public:

  virtual Status NewProxy(const metadata::QuorumPeerPB& peer_pb,
                          gscoped_ptr<PeerProxy>* proxy) = 0;

  virtual ~PeerProxyFactory() {}
};

// PeerProxy implementation that does RPC calls
class RpcPeerProxy : public PeerProxy {
 public:
  RpcPeerProxy(gscoped_ptr<HostPort> hostport,
               gscoped_ptr<tserver::TabletServerServiceProxy> ts_proxy);

  virtual Status UpdateAsync(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response,
                             rpc::RpcController* controller,
                             const rpc::ResponseCallback& callback) OVERRIDE;

  virtual ~RpcPeerProxy();
 public:
  gscoped_ptr<HostPort> hostport_;
  gscoped_ptr<tserver::TabletServerServiceProxy> ts_proxy_;
};

// PeerProxyFactory implementation that generates RPCPeerProxies
class RpcPeerProxyFactory : public PeerProxyFactory {
 public:
  explicit RpcPeerProxyFactory(const std::tr1::shared_ptr<rpc::Messenger>& messenger);

  virtual Status NewProxy(const metadata::QuorumPeerPB& peer_pb,
                          gscoped_ptr<PeerProxy>* proxy) OVERRIDE;

  virtual ~RpcPeerProxyFactory();
 private:
  std::tr1::shared_ptr<rpc::Messenger> messenger_;
};

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_CONSENSUS_PEERS_H_ */
