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

DEFINE_int32(consensus_rpc_timeout_ms, 15000,
             "Timeout used for all consensus internal RPC comms.");

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
           const string& tablet_id)
      : peer_(peer),
        tablet_id_(tablet_id) {
  }

  virtual Status Init() = 0;

  // Sends the next request, asynchronously. The peer lock must be acquired
  // prior to calling this method.
  virtual void ProcessNextRequest() = 0;

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
  Peer* peer_;
  const string tablet_id_;
  ConsensusRequestPB request_;
  ConsensusResponsePB response_;
};

// The local peer
class LocalPeer : public PeerImpl {
 public:
  LocalPeer(Peer* peer,
            const string& tablet_id,
            log::Log* log)
      : PeerImpl(peer, tablet_id),
        log_(log),
        log_append_callback_(
            new BoundFunctionCallback(
                boost::bind(&LocalPeer::RequestFinishedCallback, this),
                boost::bind(&LocalPeer::LogAppendFailedCallback, this, _1))) {
  }

  Status Init() {
    request_.set_tablet_id(tablet_id_);
    return Status::OK();
  }

  void ProcessNextRequest() {
    response_.Clear();

    LogEntryBatch* reserved_entry_batch;
    vector<OperationPB*> ops;
    BOOST_FOREACH(const OperationPB& op, request_.ops()) {
      ops.push_back(const_cast<OperationPB*>(&op));
    }

    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG(2) << "Local peer appending to log: " << request_.ShortDebugString();
    }

    log_->Reserve(ops, &reserved_entry_batch);
    log_->AsyncAppend(reserved_entry_batch, log_append_callback_);
  }

  void RequestFinishedCallback() {
    if (PREDICT_TRUE(status_.ok())) {
      if (PREDICT_FALSE(VLOG_IS_ON(2))) {
        VLOG(2) << "Local peer logged: " << request_.ShortDebugString();
      }
      response_.mutable_status()->mutable_replicated_watermark()->CopyFrom(
          request_.ops(request_.ops_size() - 1).id());
      request_.mutable_ops()->ExtractSubrange(0, request_.ops_size(), NULL);
      peer_->ProcessResponse(response_.status());
    } else {
      LOG(FATAL) << "Error while storing in the local log. Status: "
          << status_.ToString();
    }
  }

  void LogAppendFailedCallback(Status status) {
    status_ = status;
    RequestFinishedCallback();
  }

 private:
  log::Log* log_;
  shared_ptr<FutureCallback> log_append_callback_;
  Status status_;
};

// A remote peer.
class RemotePeer : public PeerImpl {
 public:
  RemotePeer(Peer* peer,
             const string& tablet_id,
             gscoped_ptr<PeerProxy> proxy)
      : PeerImpl(peer, tablet_id),
        proxy_(proxy.Pass()),
        callback_(boost::bind(&RemotePeer::RequestFinishedCallback, this)) {
  }

  Status Init() {
    request_.set_tablet_id(tablet_id_);
    return Status::OK();
  }

  void ProcessNextRequest() {
    response_.Clear();
    controller_.Reset();

    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG(2) << "Remote peer sending: " << request_.ShortDebugString();
    }

    proxy_->UpdateAsync(&request_, &response_, &controller_, callback_);
  }

  virtual void RequestFinishedCallback() {
    if (!controller_.status().ok() || response_.has_error()) {
      if (VLOG_IS_ON(1)) {
        VLOG(1) << "Error connecting to peer: " << peer_->peer_pb().ShortDebugString()
            << ". Response: " << response_.ShortDebugString()
            << " Status: " << controller_.status().ToString()
            << ". sleeping for 20 ms";
      }
      // HACK: We need proper backoff but for now just sleep a fixed amount.
      usleep(20000);
      ProcessNextRequest();
    } else {
      if (PREDICT_FALSE(VLOG_IS_ON(2))) {
        VLOG(2) << "Remote peer received: " << response_.ShortDebugString();
      }
      peer_->ProcessResponse(response_.status());
    }
  }

 private:
  gscoped_ptr<PeerProxy> proxy_;
  rpc::RpcController controller_;
  rpc::ResponseCallback callback_;
};

Status Peer::NewLocalPeer(const QuorumPeerPB& peer_pb,
                          const string& tablet_id,
                          PeerMessageQueue* queue,
                          Log* log,
                          gscoped_ptr<Peer>* peer) {
  gscoped_ptr<Peer> new_peer(new Peer(peer_pb,
                                      tablet_id,
                                      queue,
                                      log));
  RETURN_NOT_OK(new_peer->Init());
  peer->reset(new_peer.release());
  return Status::OK();
}

Status Peer::NewRemotePeer(const metadata::QuorumPeerPB& peer_pb,
                           const string& tablet_id,
                           PeerMessageQueue* queue,
                           gscoped_ptr<PeerProxy> proxy,
                           gscoped_ptr<Peer>* peer) {

  gscoped_ptr<Peer> new_peer(new Peer(peer_pb,
                                      tablet_id,
                                      queue,
                                      proxy.Pass()));
  RETURN_NOT_OK(new_peer->Init());
  peer->reset(new_peer.release());
  return Status::OK();
}

Peer::Peer(const metadata::QuorumPeerPB& peer_pb,
           const string& tablet_id,
           PeerMessageQueue* queue,
           Log* log)
    : peer_pb_(peer_pb),
      peer_impl_(new LocalPeer(this, tablet_id, log)),
      queue_(queue),
      req_pending_(false),
      processing_(false) {
}

Peer::Peer(const metadata::QuorumPeerPB& peer_pb,
           const string& tablet_id,
           PeerMessageQueue* queue,
           gscoped_ptr<PeerProxy> proxy)
    : peer_pb_(peer_pb),
      peer_impl_(new RemotePeer(this, tablet_id, proxy.Pass())),
      queue_(queue),
      req_pending_(false),
      processing_(false) {
}

Status Peer::Init() {
  RETURN_NOT_OK(peer_impl_->Init());
  RETURN_NOT_OK(queue_->TrackPeer(peer_pb_.permanent_uuid()));
  return Status::OK();
}

void Peer::SignalRequest() {
  boost::lock_guard<simple_spinlock> lock(peer_lock_);
  // the peer is already set to send another request.
  if (req_pending_) {
    return;
  }
  // if the peer is currently sending, set the request as pending.
  if (processing_) {
    req_pending_ = true;
    return;
  }
  // the peer has no pending request nor is sending: send the request
  queue_->RequestForPeer(peer_pb_.permanent_uuid(), peer_impl_->request());
  peer_impl_->ProcessNextRequest();
  processing_ = true;
}

void Peer::ProcessResponse(const ConsensusStatusPB& status) {
  boost::lock_guard<simple_spinlock> lock(peer_lock_);
  queue_->ResponseFromPeer(peer_pb_.permanent_uuid(), status, &req_pending_);
  if (req_pending_) {
    queue_->RequestForPeer(peer_pb_.permanent_uuid(), peer_impl_->request());
    peer_impl_->ProcessNextRequest();
    processing_ = true;
  } else {
    processing_ = false;
  }
}

Peer::~Peer() {
  queue_->UntrackPeer(peer_pb_.permanent_uuid());
}

}  // namespace consensus
}  // namespace kudu

