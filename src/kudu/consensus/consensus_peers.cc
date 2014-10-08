// Copyright (c) 2013, Cloudera, inc.

#include <algorithm>
#include <gflags/gflags.h>
#include <string>
#include <utility>
#include <vector>

#include "kudu/consensus/consensus_peers.h"

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/log.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/net_util.h"

DEFINE_int32(consensus_rpc_timeout_ms, 1000,
             "Timeout used for all consensus internal RPC comms.");

DEFINE_int32(quorum_get_node_instance_timeout_ms, 30000,
             "Timeout for retrieving node instance data over RPC.");

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

class PeerImpl {
 public:
  // Initializes the Peer implementation and sets 'initial_id' to the last
  // id received by the replica.
  virtual Status Init(OpId* initial_id) = 0;


  // Return a string identifying what type of peer this is (eg "remote" or "local").
  virtual string PeerTypeString() const = 0;

  // Sends the next request, asynchronously. When the request is complete (eg the
  // remote peer responds, or the local logging finishes), 'callback' is called.
  // If 'callback' gets an OK status, then 'response' will contain the peer's
  // response.
  //
  // The request pointer must remain valid and unmodified until the callback is called.
  virtual void SendRequest(const ConsensusRequestPB* request,
                           ConsensusResponsePB* response,
                           const StatusCallback& callback) = 0;

  virtual ~PeerImpl() {}
};

// The local peer
class LocalPeer : public PeerImpl {
 public:
  LocalPeer(log::Log* log,
            const string& local_uuid)
    : log_(log),
      local_uuid_(local_uuid) {
  }

  Status Init(OpId* initial_id) OVERRIDE {
    Status s = log_->GetLastEntryOpId(initial_id);
    if (s.IsNotFound()) {
      *initial_id = MinimumOpId();
      s = Status::OK();
    }
    last_replicated_ = *initial_id;
    return s;
  }

  virtual void SendRequest(const ConsensusRequestPB* request,
                           ConsensusResponsePB* response,
                           const StatusCallback& callback) OVERRIDE {
    if (PREDICT_FALSE(request->ops_size() == 0)) {
      SetupResponse(response);
      callback.Run(Status::OK());
      return;
    }

    vector<const ReplicateMsg*> ops;
    ops.reserve(request->ops_size());

    BOOST_FOREACH(const ReplicateMsg& op, request->ops()) {
      ops.push_back(&op);
    }

    if (PREDICT_FALSE(VLOG_IS_ON(2))) {
      VLOG(2) << "Local peer appending to log: " << request->ShortDebugString();
    }

    CHECK_OK(log_->AsyncAppendReplicates(
               &ops[0], ops.size(),
               Bind(&LocalPeer::LogAppendCallback,
                    Unretained(this),
                    Unretained(request),
                    Unretained(response),
                    callback)));
  }

  virtual std::string PeerTypeString() const OVERRIDE {
    return "local";
  }

 private:
  void SetupResponse(ConsensusResponsePB* response) {
    response->Clear();
    response->set_responder_term(last_replicated_.term());
    response->set_responder_uuid(local_uuid_);
    ConsensusStatusPB* status = response->mutable_status();
    status->mutable_last_received()->CopyFrom(last_replicated_);
  }

  // Callback from Log::Append().
  // This sets up a fake ConsensusResponsePB to look the same as
  // a remote peer.
  void LogAppendCallback(const ConsensusRequestPB* request,
                         ConsensusResponsePB* response,
                         const StatusCallback& peer_callback,
                         const Status& log_status) {
    if (!log_status.ok()) {
      LOG(FATAL) << "Error while storing in the local log. Status: "
          << log_status.ToString();
    }
    VLOG(2) << "Local peer logged: " << request->ShortDebugString();

    // Update the replicated op ID
    const ReplicateMsg& last_msg = request->ops(request->ops_size() - 1);
    CHECK(last_msg.has_id());
    last_replicated_ = last_msg.id();

    SetupResponse(response);

    peer_callback.Run(log_status);
  }

  log::Log* log_;

  // The UUID of the local server.
  const std::string local_uuid_;

  // The last opid which has been durably written to the local
  // peer.
  OpId last_replicated_;
};

// A remote peer.
class RemotePeer : public PeerImpl {
 public:
  explicit RemotePeer(gscoped_ptr<PeerProxy> proxy)
    : proxy_(proxy.Pass()) {
  }

  Status Init(OpId* initial_id) OVERRIDE {
    // TODO ask the remote peer for the initial id when we have catch up.
    initial_id->CopyFrom(MinimumOpId());

    return Status::OK();
  }

  virtual void SendRequest(const ConsensusRequestPB* request,
                           ConsensusResponsePB* response,
                           const StatusCallback& callback) OVERRIDE {
    controller_.Reset();

    // TODO handle errors
    CHECK_OK(proxy_->UpdateAsync(
               request, response, &controller_,
               boost::bind(&RemotePeer::RequestCallback, this, callback)));
  }

  virtual std::string PeerTypeString() const OVERRIDE {
    return "remote";
  }

 private:
  void RequestCallback(const StatusCallback& peer_callback) {
    peer_callback.Run(controller_.status());
  }


  gscoped_ptr<PeerProxy> proxy_;
  rpc::RpcController controller_;
};

Status Peer::NewLocalPeer(const QuorumPeerPB& peer_pb,
                          const string& tablet_id,
                          const string& leader_uuid,
                          PeerMessageQueue* queue,
                          Log* log,
                          gscoped_ptr<Peer>* peer) {
  gscoped_ptr<PeerImpl> impl(new LocalPeer(log, leader_uuid));
  gscoped_ptr<Peer> new_peer(new Peer(peer_pb,
                                      tablet_id,
                                      leader_uuid,
                                      queue,
                                      impl.Pass()));
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

  gscoped_ptr<PeerImpl> impl(new RemotePeer(proxy.Pass()));
  gscoped_ptr<Peer> new_peer(new Peer(peer_pb,
                                      tablet_id,
                                      leader_uuid,
                                      queue,
                                      impl.Pass()));
  RETURN_NOT_OK(new_peer->Init());
  peer->reset(new_peer.release());
  return Status::OK();
}

Peer::Peer(const metadata::QuorumPeerPB& peer_pb,
           const string& tablet_id,
           const string& leader_uuid,
           PeerMessageQueue* queue,
           gscoped_ptr<PeerImpl> impl)
    : tablet_id_(tablet_id),
      leader_uuid_(leader_uuid),
      peer_pb_(peer_pb),
      peer_impl_(impl.Pass()),
      queue_(queue),
      failed_attempts_(0),
      sem_(1),
      heartbeater_(new ResettableHeartbeater(
                     peer_pb.permanent_uuid(),
                     MonoDelta::FromMilliseconds(
                       FLAGS_leader_heartbeat_interval_ms),
                     boost::bind(&Peer::SignalRequest, this, true))),
      state_(kPeerCreated) {

}

void Peer::SetTermForTest(int term) {
  response_.set_responder_term(term);
}

Status Peer::Init() {
  boost::lock_guard<Semaphore> lock(sem_);
  OpId initial_id;
  RETURN_NOT_OK(peer_impl_->Init(&initial_id));
  RETURN_NOT_OK(queue_->TrackPeer(peer_pb_.permanent_uuid(), initial_id));
  if (heartbeater_) {
    RETURN_NOT_OK(heartbeater_->Start());
  }
  state_ = kPeerIdle;
  return Status::OK();
}

Status Peer::SignalRequest(bool even_if_queue_empty) {
  // If the peer is currently sending, return Status::OK().
  // If there are new requests in the queue we'll get them on ProcessResponse().
  if (!sem_.TryAcquire()) {
    return Status::OK();
  }
  if (PREDICT_FALSE(state_ == kPeerClosed)) {
    sem_.Release();
    return Status::IllegalState("Peer was closed.");
  }

  DCHECK_EQ(state_, kPeerIdle);

  SendNextRequest(even_if_queue_empty);
  return Status::OK();
}

void Peer::SendNextRequest(bool even_if_queue_empty) {
  // the peer has no pending request nor is sending: send the request
  queue_->RequestForPeer(peer_pb_.permanent_uuid(), &request_);
  request_.set_tablet_id(tablet_id_);
  request_.set_caller_uuid(leader_uuid_);

  bool req_has_ops = request_.ops_size() > 0;
  // If the queue is empty, check if we were told to send a status-only
  // message, if not just return.
  if (PREDICT_FALSE(!req_has_ops && !even_if_queue_empty)) {
    sem_.Release();
    return;
  }

  // If we're actually sending ops there's no need to heartbeat for a while,
  // reset the heartbeater
  if (req_has_ops) {
    heartbeater_->Reset();
  }

  state_ = kPeerWaitingForResponse;

  VLOG(2) << "Sending to peer " << peer_pb().permanent_uuid() << ": "
          << request_.ShortDebugString();
  peer_impl_->SendRequest(&request_, &response_,
                          Bind(&Peer::ProcessResponse, Unretained(this)));
}

void Peer::ProcessResponse(const Status& status) {
  DCHECK_EQ(0, sem_.GetValue())
    << "Got a response when nothing was pending";
  DCHECK_EQ(state_, kPeerWaitingForResponse);

  if (!status.ok()) {
    ProcessResponseError(status);
    return;
  }

  if (response_.has_error()) {
    ProcessResponseError(StatusFromPB(response_.error().status()));
    return;
  }
  failed_attempts_ = 0;

  DCHECK(response_.status().IsInitialized());
  VLOG(2) << "Response from "
          << peer_impl_->PeerTypeString() << " peer " << peer_pb().permanent_uuid() << ": "
          << response_.ShortDebugString();


  bool more_pending;
  queue_->ResponseFromPeer(response_, &more_pending);
  if (more_pending && state_ != kPeerClosed) {
    SendNextRequest(true);
  } else {
    state_ = kPeerIdle;
    sem_.Release();
  }
}

namespace {

std::string OpsRangeString(const ConsensusRequestPB& req) {
  std::string ret;
  ret.reserve(100);
  ret.push_back('[');
  if (req.ops_size() > 0) {
    const OpId& first_op = req.ops(0).id();
    const OpId& last_op = req.ops(req.ops_size() - 1).id();
    strings::SubstituteAndAppend(&ret, "$0.$1-$2.$3",
                                 first_op.term(), first_op.index(),
                                 last_op.term(), last_op.index());
  }
  ret.push_back(']');
  return ret;
}

} // anonymous namespace

void Peer::ProcessResponseError(const Status& status) {
  failed_attempts_++;
  LOG(WARNING) << "Couldn't send request to peer " << peer_pb_.permanent_uuid()
      << " for tablet " << tablet_id_
      << " ops " << OpsRangeString(request_)
      << " Status: " << status.ToString() << ". Retrying in the next heartbeat period."
      << " Already tried " << failed_attempts_ << " times.";
  state_ = kPeerIdle;
  sem_.Release();
}

void Peer::Close() {
  if (heartbeater_) {
    WARN_NOT_OK(heartbeater_->Stop(), "Could not stop heartbeater");
  }

  // Acquire the semaphore to wait for any concurrent request to finish.
  boost::lock_guard<Semaphore> l(sem_);
  // If the peer is already closed return.
  if (state_ == kPeerClosed) return;
  DCHECK_EQ(state_, kPeerIdle);
  state_ = kPeerClosed;

  LOG(INFO) << "Closing peer: " << peer_pb_.permanent_uuid();
  queue_->UntrackPeer(peer_pb_.permanent_uuid());
}

Peer::~Peer() {
  // We don't own the ops (the queue does).
  request_.mutable_ops()->ExtractSubrange(0, request_.ops_size(), NULL);

  Close();
}

RpcPeerProxy::RpcPeerProxy(gscoped_ptr<HostPort> hostport,
                           gscoped_ptr<ConsensusServiceProxy> consensus_proxy)
    : hostport_(hostport.Pass()),
      consensus_proxy_(consensus_proxy.Pass()) {
}

Status RpcPeerProxy::UpdateAsync(const ConsensusRequestPB* request,
                                 ConsensusResponsePB* response,
                                 rpc::RpcController* controller,
                                 const rpc::ResponseCallback& callback) {
  controller->set_timeout(MonoDelta::FromMilliseconds(FLAGS_consensus_rpc_timeout_ms));
  consensus_proxy_->UpdateConsensusAsync(*request, response, controller, callback);
  return Status::OK();
}

RpcPeerProxy::~RpcPeerProxy() {}

namespace {

Status CreateConsensusServiceProxyForHost(const shared_ptr<Messenger>& messenger,
                                          const HostPort& hostport,
                                          gscoped_ptr<ConsensusServiceProxy>* new_proxy) {
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(hostport.ResolveAddresses(&addrs));
  if (addrs.size() > 1) {
    LOG(WARNING)<< "Peer address '" << hostport.ToString() << "' "
    << "resolves to " << addrs.size() << " different addresses. Using "
    << addrs[0].ToString();
  }
  new_proxy->reset(new ConsensusServiceProxy(messenger, addrs[0]));
  return Status::OK();
}

} // anonymous namespace

RpcPeerProxyFactory::RpcPeerProxyFactory(const shared_ptr<Messenger>& messenger)
    : messenger_(messenger) {
}

Status RpcPeerProxyFactory::NewProxy(const QuorumPeerPB& peer_pb,
                                     gscoped_ptr<PeerProxy>* proxy) {
  gscoped_ptr<HostPort> hostport(new HostPort);
  RETURN_NOT_OK(HostPortFromPB(peer_pb.last_known_addr(), hostport.get()));
  gscoped_ptr<ConsensusServiceProxy> new_proxy;
  RETURN_NOT_OK(CreateConsensusServiceProxyForHost(messenger_, *hostport, &new_proxy));
  proxy->reset(new RpcPeerProxy(hostport.Pass(), new_proxy.Pass()));
  return Status::OK();
}

RpcPeerProxyFactory::~RpcPeerProxyFactory() {}

Status SetPermanentUuidForRemotePeer(const shared_ptr<Messenger>& messenger,
                                     QuorumPeerPB* remote_peer) {
  DCHECK(!remote_peer->has_permanent_uuid());
  HostPort hostport;
  RETURN_NOT_OK(HostPortFromPB(remote_peer->last_known_addr(), &hostport));
  gscoped_ptr<ConsensusServiceProxy> proxy;
  RETURN_NOT_OK(CreateConsensusServiceProxyForHost(messenger, hostport, &proxy));
  GetNodeInstanceRequestPB req;
  GetNodeInstanceResponsePB resp;
  rpc::RpcController controller;

  // TODO generalize this exponential backoff algorithm, as we do the
  // same thing in catalog_manager.cc
  // (AsyncTabletRequestTask::RpcCallBack).
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(MonoDelta::FromMilliseconds(FLAGS_quorum_get_node_instance_timeout_ms));
  int attempt = 1;
  while (true) {
    VLOG(2) << "Getting uuid from remote peer. Request: " << req.ShortDebugString();

    controller.Reset();
    Status s = proxy->GetNodeInstance(req, &resp, &controller);
    if (s.ok()) {
      if (controller.status().ok()) {
        break;
      }
      s = controller.status();
    }

    LOG(WARNING) << "Error getting permanent uuid from quorum peer " << hostport.ToString() << ": "
                 << s.ToString();
    MonoTime now = MonoTime::Now(MonoTime::FINE);
    if (now.ComesBefore(deadline)) {
      int64_t remaining_ms = deadline.GetDeltaSince(now).ToMilliseconds();
      int64_t base_delay_ms = 1 << (attempt + 3); // 1st retry delayed 2^4 ms, 2nd 2^5, etc..
      int64_t jitter_ms = rand() % 50; // Add up to 50ms of additional random delay.
      int64_t delay_ms = std::min<int64_t>(base_delay_ms + jitter_ms, remaining_ms);
      VLOG(1) << "Sleeping " << delay_ms << " ms. before retrying to get uuid from remote peer...";
      usleep(delay_ms * 1000);
      LOG(INFO) << "Retrying to get permanent uuid for remote peer: "
          << remote_peer->ShortDebugString() << " attempt: " << attempt++;
    } else {
      s = Status::TimedOut(Substitute("Getting permanent uuid from $0 timed out after $1 ms.",
                                      hostport.ToString(),
                                      FLAGS_quorum_get_node_instance_timeout_ms),
                           s.ToString());
      return s;
    }
  }
  remote_peer->set_permanent_uuid(resp.node_instance().permanent_uuid());
  return Status::OK();
}

}  // namespace consensus
}  // namespace kudu
