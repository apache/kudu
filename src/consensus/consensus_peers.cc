// Copyright (c) 2013, Cloudera, inc.

#include <gflags/gflags.h>
#include <string>
#include <utility>
#include <vector>

#include "consensus/consensus_peers.h"

#include "common/wire_protocol.h"
#include "consensus/consensus_queue.h"
#include "consensus/log.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "tserver/tserver_service.proxy.h"
#include "util/logging.h"
#include "util/net/net_util.h"

DEFINE_int32(consensus_rpc_timeout_ms, 1000,
             "Timeout used for all consensus internal RPC comms.");

DECLARE_int32(leader_heartbeat_interval_ms);

namespace kudu {
namespace consensus {

using log::Log;
using log::LogEntryBatch;
using metadata::QuorumPeerPB;
using std::tr1::shared_ptr;
using std::tr1::unordered_map;
using rpc::Messenger;
using rpc::RpcController;
using strings::Substitute;
using tserver::TabletServerServiceProxy;

class PeerImpl {
 public:
  PeerImpl(Peer* peer,
           const string& tablet_id,
           const string& leader_uuid)
      : peer_(peer),
        tablet_id_(tablet_id),
        leader_uuid_(leader_uuid) {
  }

  // Initializes the Peer implementation and sets 'initial_id' to the last
  // id received by the replica.
  virtual Status Init(OpId* initial_id) = 0;

  // Sends the next request, asynchronously. The peer lock must be acquired
  // prior to calling this method.
  // Returns false if there were no pending messages, and thus no request was
  // sent.
  virtual bool ProcessNextRequest() = 0;

  virtual void RequestFinishedCallback() = 0;

  virtual ConsensusRequestPB* request() {
    return &request_;
  }

  // On destruction release the operations as the peers don't
  // own them.
  virtual ~PeerImpl() {
    request_.mutable_ops()->ExtractSubrange(0, request_.ops_size(), NULL);
  }

 protected:
  friend class Peer;

  Peer* peer_;
  const string tablet_id_;
  const string leader_uuid_;
  ConsensusRequestPB request_;
  ConsensusResponsePB response_;
};

// The local peer
class LocalPeer : public PeerImpl {
 public:
  LocalPeer(Peer* peer,
            const string& tablet_id,
            const string& leader_uuid,
            log::Log* log,
            const OpId& initial_op)
      : PeerImpl(peer, tablet_id, leader_uuid),
        log_(log) {
    last_replicated_.CopyFrom(initial_op);
    safe_commit_.CopyFrom(initial_op);
    last_received_.CopyFrom(initial_op);
  }

  Status Init(OpId* initial_id) {
    initial_id->CopyFrom(last_received_);
    request_.set_tablet_id(tablet_id_);
    request_.set_sender_uuid(leader_uuid_);
    return Status::OK();
  }

  bool ProcessNextRequest() {
    if (PREDICT_FALSE(request_.ops_size() == 0)) {
      return false;
    }
    response_.Clear();

    LogEntryBatch* reserved_entry_batch;
    vector<const OperationPB*> ops;
    const OpId* last_replicated = NULL;
    const OpId* last_committed = NULL;
    const OpId* last_received = NULL;
    for (int i = 0; i < request_.ops_size(); i++) {
      OperationPB* op = request_.mutable_ops(i);
      ops.push_back(op);
      last_received = &op->id();
      if (op->has_replicate()) {
        last_replicated = &op->id();
        continue;
      }
      if (op->has_commit()) {
        last_committed = &op->id();
        continue;
      }
    }

    if (last_replicated != NULL) last_replicated_.CopyFrom(*last_replicated);
    if (last_committed != NULL) safe_commit_.CopyFrom(*last_committed);

    last_received_.CopyFrom(*last_received);

    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG(2) << "Local peer appending to log: " << request_.ShortDebugString();
    }

    CHECK_OK(log_->Reserve(ops, &reserved_entry_batch));
    CHECK_OK(log_->AsyncAppend(reserved_entry_batch,
                               boost::bind(&LocalPeer::LogAppendCallback, this, _1)));
    return true;
  }

  void RequestFinishedCallback() {
    if (PREDICT_TRUE(status_.ok())) {
      if (PREDICT_FALSE(VLOG_IS_ON(2))) {
        VLOG(2) << "Local peer logged: " << request_.ShortDebugString();
      }
      ConsensusStatusPB* status = response_.mutable_status();
      status->mutable_replicated_watermark()->CopyFrom(last_replicated_);
      status->mutable_safe_commit_watermark()->CopyFrom(safe_commit_);
      status->mutable_received_watermark()->CopyFrom(last_received_);

      request_.mutable_ops()->ExtractSubrange(0, request_.ops_size(), NULL);
      peer_->ProcessResponse(response_.status());
    } else {
      LOG(FATAL) << "Error while storing in the local log. Status: "
          << status_.ToString();
    }
  }

 private:
  void LogAppendCallback(const Status& status) {
    status_ = status;
    RequestFinishedCallback();
  }

  log::Log* log_;
  shared_ptr<FutureCallback> log_append_callback_;
  Status status_;
  OpId last_replicated_;
  OpId safe_commit_;
  OpId last_received_;
};

// A remote peer.
class RemotePeer : public PeerImpl {
 public:
  RemotePeer(Peer* peer,
             const string& tablet_id,
             const string& leader_uuid,
             gscoped_ptr<PeerProxy> proxy)
      : PeerImpl(peer, tablet_id, leader_uuid),
        proxy_(proxy.Pass()),
        callback_(boost::bind(&RemotePeer::RequestFinishedCallback, this)),
        state_(kStateWaiting) {
  }

  Status Init(OpId* initial_id) {
    // TODO ask the remote peer for the initial id when we have catch up.
    initial_id->CopyFrom(log::MinimumOpId());
    request_.set_tablet_id(tablet_id_);
    request_.set_sender_uuid(leader_uuid_);
    return Status::OK();
  }

  bool ProcessNextRequest() {
    DCHECK_EQ(state_, kStateWaiting);
    response_.Clear();
    controller_.Reset();
    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG(2) << "Remote peer: " << peer_->peer_pb().permanent_uuid() <<" sending: "
          << request_.ShortDebugString();
    }

    state_ = kStateSending;
    // TODO handle errors
    CHECK_OK(proxy_->UpdateAsync(&request_, &response_, &controller_, callback_));
    return true;
  }

  virtual void RequestFinishedCallback() {
    DCHECK_EQ(state_, kStateSending);
    state_ = kStateWaiting;
    if (PREDICT_FALSE(!controller_.status().ok() || response_.has_error())) {
      if (VLOG_IS_ON(1)) {
        VLOG(1) << "Error connecting to peer: " << peer_->peer_pb().ShortDebugString()
            << ". Response: " << response_.ShortDebugString()
            << " Controller Status: " << controller_.status().ToString();
      }
      Status error;
      if (!controller_.status().ok()) {
        error = controller_.status();
      } else {
        error = StatusFromPB(response_.error().status());
      }
      peer_->ProcessResponseError(error);
    } else {
      if (PREDICT_FALSE(VLOG_IS_ON(2))) {
        VLOG(2) << "Remote peer: " << peer_->peer_pb().permanent_uuid()
            << " received from remote endpoint: " << response_.ShortDebugString();
      }
      peer_->ProcessResponse(response_.status());
    }
  }

 private:
  gscoped_ptr<PeerProxy> proxy_;
  rpc::RpcController controller_;
  rpc::ResponseCallback callback_;

  enum State {
    kStateSending,
    kStateWaiting
  };

  State state_;
};

Status Peer::NewLocalPeer(const QuorumPeerPB& peer_pb,
                          const string& tablet_id,
                          const string& leader_uuid,
                          PeerMessageQueue* queue,
                          Log* log,
                          const OpId& initial_op,
                          gscoped_ptr<Peer>* peer) {
  gscoped_ptr<Peer> new_peer(new Peer(peer_pb,
                                      tablet_id,
                                      leader_uuid,
                                      queue,
                                      log,
                                      initial_op));
  RETURN_NOT_OK(new_peer->Init());
  peer->reset(new_peer.release());
  return Status::OK();
}

Status Peer::NewRemotePeer(const metadata::QuorumPeerPB& peer_pb,
                           const string& tablet_id,
                           const string& leader_uuid,
                           PeerMessageQueue* queue,
                           gscoped_ptr<PeerProxy> proxy,
                           gscoped_ptr<Peer>* peer) {

  gscoped_ptr<Peer> new_peer(new Peer(peer_pb,
                                      tablet_id,
                                      leader_uuid,
                                      queue,
                                      proxy.Pass()));
  RETURN_NOT_OK(new_peer->Init());
  peer->reset(new_peer.release());
  return Status::OK();
}

Peer::Peer(const metadata::QuorumPeerPB& peer_pb,
           const string& tablet_id,
           const string& leader_uuid,
           PeerMessageQueue* queue,
           Log* log,
           const OpId& initial_op)
    : peer_pb_(peer_pb),
      peer_impl_(new LocalPeer(this, tablet_id, leader_uuid, log, initial_op)),
      queue_(queue),
      processing_(false),
      failed_attempts_(0),
      outstanding_req_latch_(0),
      heartbeater_(NULL),
      state_(kPeerCreated) {
}

Peer::Peer(const metadata::QuorumPeerPB& peer_pb,
           const string& tablet_id,
           const string& leader_uuid,
           PeerMessageQueue* queue,
           gscoped_ptr<PeerProxy> proxy)
    : peer_pb_(peer_pb),
      peer_impl_(new RemotePeer(this, tablet_id, leader_uuid, proxy.Pass())),
      queue_(queue),
      processing_(false),
      failed_attempts_(0),
      outstanding_req_latch_(0),
      heartbeater_(
          new ResettableHeartbeater(peer_pb.permanent_uuid(),
                                    MonoDelta::FromMilliseconds(
                                        FLAGS_leader_heartbeat_interval_ms),
                                    boost::bind(&Peer::SignalRequest, this, true))),
      state_(kPeerCreated) {
}

Status Peer::Init() {
  boost::lock_guard<simple_spinlock> lock(peer_lock_);
  OpId initial_id;
  RETURN_NOT_OK(peer_impl_->Init(&initial_id));
  RETURN_NOT_OK(queue_->TrackPeer(peer_pb_.permanent_uuid(), initial_id));
  if (heartbeater_) {
    RETURN_NOT_OK(heartbeater_->Start());
  }
  state_ = kPeerIntitialized;
  return Status::OK();
}

Status Peer::SignalRequest(bool send_status_only_if_queue_empty) {
  boost::lock_guard<simple_spinlock> lock(peer_lock_);;
  if (PREDICT_FALSE(state_ == kPeerClosed)) {
    return Status::IllegalState("Peer was closed.");
  }
  // If the peer is currently sending return Status::OK().
  // If there are new requests in the queue we'll get them on ProcessResponse().
  if (processing_) {
    return Status::OK();
  }
  // the peer has no pending request nor is sending: send the request
  queue_->RequestForPeer(peer_pb_.permanent_uuid(), peer_impl_->request());

  // If the queue is empty, check if we were told to send a status-only
  // message, if not just return.
  if (PREDICT_FALSE(peer_impl_->request()->ops_size() == 0 && !send_status_only_if_queue_empty)) {
    return Status::OK();
  }

  // If we're actually sending ops there's no need to heartbeat for a while,
  // reset the heartbeater
  if (PREDICT_FALSE(peer_impl_->request()->ops_size() == 0) && heartbeater_ != NULL) {
    heartbeater_->Reset();
  }

  processing_ = peer_impl_->ProcessNextRequest();
  if (processing_) {
    DCHECK_EQ(outstanding_req_latch_.count(), 0);
    outstanding_req_latch_.Reset(1);
  } else {
    outstanding_req_latch_.CountDown();
  }
  return Status::OK();
}

void Peer::ProcessResponse(const ConsensusStatusPB& status) {
  DCHECK(status.IsInitialized());
  boost::lock_guard<simple_spinlock> lock(peer_lock_);
  bool more_pending;
  queue_->ResponseFromPeer(peer_pb_.permanent_uuid(), status, &more_pending);
  outstanding_req_latch_.CountDown();
  if (more_pending && state_ != kPeerClosed) {
    queue_->RequestForPeer(peer_pb_.permanent_uuid(), peer_impl_->request());
    processing_ = peer_impl_->ProcessNextRequest();
    if (processing_) {
      outstanding_req_latch_.Reset(1);
    }
  } else {
    processing_ = false;
  }
  failed_attempts_ = 0;
}

void Peer::ProcessResponseError(const Status& status) {
  boost::lock_guard<simple_spinlock> lock(peer_lock_);
  // TODO handle the error.
  failed_attempts_++;
  LOG(WARNING) << "Couldn't send request to peer: " << peer_pb_.permanent_uuid()
      << " Status: " << status.ToString() << ". Retrying in the next heartbeat period."
      << " Already tried " << failed_attempts_ << " times.";
  outstanding_req_latch_.CountDown();
  processing_ = false;
}

void Peer::Close() {
  if (heartbeater_) {
    WARN_NOT_OK(heartbeater_->Stop(), "Could not stop heartbeater");
  }
  {
    boost::lock_guard<simple_spinlock> lock(peer_lock_);
    // If the peer is already closed return.
    if (state_ == kPeerClosed) return;

    DCHECK_EQ(state_, kPeerIntitialized);
    state_ = kPeerClosed;
  }

  LOG(INFO) << "Closing peer: " << peer_pb_.permanent_uuid()
      << " Waiting for outstanding requests to complete " << outstanding_req_latch_.count();
  outstanding_req_latch_.Wait();

  {
    boost::lock_guard<simple_spinlock> lock(peer_lock_);
    queue_->UntrackPeer(peer_pb_.permanent_uuid());
  }

}

Peer::~Peer() {
  Close();
}

RpcPeerProxy::RpcPeerProxy(gscoped_ptr<HostPort> hostport,
                           gscoped_ptr<TabletServerServiceProxy> ts_proxy)
    : hostport_(hostport.Pass()),
      ts_proxy_(ts_proxy.Pass()) {
}

Status RpcPeerProxy::UpdateAsync(const ConsensusRequestPB* request,
                                 ConsensusResponsePB* response,
                                 rpc::RpcController* controller,
                                 const rpc::ResponseCallback& callback) {
  controller->set_timeout(MonoDelta::FromMilliseconds(FLAGS_consensus_rpc_timeout_ms));
  ts_proxy_->UpdateConsensusAsync(*request, response, controller, callback);
  return Status::OK();
}

RpcPeerProxy::~RpcPeerProxy() {}

RpcPeerProxyFactory::RpcPeerProxyFactory(const shared_ptr<Messenger>& messenger)
    : messenger_(messenger) {
}

Status RpcPeerProxyFactory::NewProxy(const QuorumPeerPB& peer_pb,
                                     gscoped_ptr<PeerProxy>* proxy) {

  gscoped_ptr<HostPort> hostport(new HostPort);
  RETURN_NOT_OK(HostPortFromPB(peer_pb.last_known_addr(), hostport.get()))

  vector<Sockaddr> addrs;
  RETURN_NOT_OK(hostport->ResolveAddresses(&addrs));
  if (addrs.size() > 1) {
    LOG(WARNING)<< "Peer address '" << hostport->ToString() << "' "
    << "resolves to " << addrs.size() << " different addresses. Using "
    << addrs[0].ToString();
  }
  gscoped_ptr<TabletServerServiceProxy> new_proxy(
      new TabletServerServiceProxy(messenger_, addrs[0]));
  proxy->reset(new RpcPeerProxy(hostport.Pass(), new_proxy.Pass()));
  return Status::OK();
}

RpcPeerProxyFactory::~RpcPeerProxyFactory() {}

}  // namespace consensus
}  // namespace kudu

