// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_CONSENSUS_PEERS_H_
#define KUDU_CONSENSUS_CONSENSUS_PEERS_H_

#include <string>

#include "rpc/response_callback.h"
#include "server/metadata.pb.h"
#include "util/locks.h"
#include "util/status.h"

namespace kudu {

namespace log {
class Log;
}

namespace rpc {
class RpcController;
}

namespace consensus {
class ConsensusRequestPB;
class ConsensusResponsePB;
class ConsensusStatusPB;
class PeerProxy;
class PeerImpl;
class PeerMessageQueue;

// A peer in consensus (local or remote).
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
//               +----------------+
//          +---+|  Req. Pending? +---+
//          |    +----------------+   |
//          |                         |
//          | Yes                     | No
//          |                         |
//          |                         |
//          v                         v
//                           +------------------+
//       return       +------+ Processing Req.? +-----+
//                    |      +------------------+     |
//                    |                               |
//                    | Yes                           | No
//                    |                               |
//                    v                               v
//                pending = true                 ProcessNextRequest()
//                return                         processing = true
//                                               - get reqs. from queue
//                                               - update peer async
//                                               return
//
//                         +
//                         |
//      ProcessResponse()  |
//                         |
//                         v
//               +------------------+
//        +------+  Req. Pending()? +-----+
//        |      +------------------+     |
//        |                               |
//        | Yes                           | No
//        |                               |
//        v                               v
//  ProcessNextRequest()           processing = false
//                                 return
class Peer {
 public:
  // Initializes a peer and get its status.
  Status Init();

  // Signals that this peer has a new request to replicate/store.
  void SignalRequest();

  // Signals that a response was received from the peer.
  void ProcessResponse(const ConsensusStatusPB& status);

  const metadata::QuorumPeerPB& peer_pb() const { return peer_pb_; }

  ~Peer();

  // Creates a new local peer and makes the queue track it.
  static Status NewLocalPeer(const metadata::QuorumPeerPB& peer_pb,
                             const std::string& tablet_id,
                             PeerMessageQueue* queue,
                             log::Log* log,
                             gscoped_ptr<Peer>* peer);

  // Creates a new remote peer and makes the queue track it.
  static Status NewRemotePeer(const metadata::QuorumPeerPB& peer_pb,
                              const std::string& tablet_id,
                              PeerMessageQueue* queue,
                              gscoped_ptr<PeerProxy> proxy,
                              gscoped_ptr<Peer>* peer);

 protected:
  Peer(const metadata::QuorumPeerPB& peer,
       const std::string& tablet_id,
       PeerMessageQueue* queue,
       log::Log* log);

  Peer(const metadata::QuorumPeerPB& peer,
       const std::string& tablet_id,
       PeerMessageQueue* queue,
       gscoped_ptr<PeerProxy> proxy);

  metadata::QuorumPeerPB peer_pb_;
  gscoped_ptr<PeerImpl> peer_impl_;
  PeerMessageQueue* queue_;
  bool req_pending_;
  bool processing_;

  // lock that protects Peer state changes
  mutable simple_spinlock peer_lock_;
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

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_CONSENSUS_PEERS_H_ */
