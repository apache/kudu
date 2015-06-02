// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <algorithm>
#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <string>
#include <utility>
#include <vector>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/log.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/threadpool.h"

DEFINE_int32(consensus_rpc_timeout_ms, 1000,
             "Timeout used for all consensus internal RPC comms.");

DEFINE_int32(raft_get_node_instance_timeout_ms, 30000,
             "Timeout for retrieving node instance data over RPC.");

DECLARE_int32(leader_heartbeat_interval_ms);

DEFINE_double(fault_crash_on_leader_request_fraction, 0.0,
              "Fraction of the time when the leader will crash just before sending an "
              "UpdateConsensus RPC. (For testing only!)");
TAG_FLAG(fault_crash_on_leader_request_fraction, unsafe);

namespace kudu {
namespace consensus {

using log::Log;
using log::LogEntryBatch;
using std::tr1::shared_ptr;
using rpc::Messenger;
using rpc::RpcController;
using strings::Substitute;

Status Peer::NewRemotePeer(const RaftPeerPB& peer_pb,
                           const string& tablet_id,
                           const string& leader_uuid,
                           PeerMessageQueue* queue,
                           ThreadPool* thread_pool,
                           gscoped_ptr<PeerProxy> proxy,
                           gscoped_ptr<Peer>* peer) {

  gscoped_ptr<Peer> new_peer(new Peer(peer_pb,
                                      tablet_id,
                                      leader_uuid,
                                      proxy.Pass(),
                                      queue,
                                      thread_pool));
  RETURN_NOT_OK(new_peer->Init());
  peer->reset(new_peer.release());
  return Status::OK();
}

Peer::Peer(const RaftPeerPB& peer_pb,
           const string& tablet_id,
           const string& leader_uuid,
           gscoped_ptr<PeerProxy> proxy,
           PeerMessageQueue* queue,
           ThreadPool* thread_pool)
    : tablet_id_(tablet_id),
      leader_uuid_(leader_uuid),
      peer_pb_(peer_pb),
      proxy_(proxy.Pass()),
      queue_(queue),
      failed_attempts_(0),
      sem_(1),
      heartbeater_(peer_pb.permanent_uuid(),
                   MonoDelta::FromMilliseconds(FLAGS_leader_heartbeat_interval_ms),
                   boost::bind(&Peer::SignalRequest, this, true)),
      thread_pool_(thread_pool),
      state_(kPeerCreated) {
}

void Peer::SetTermForTest(int term) {
  response_.set_responder_term(term);
}

Status Peer::Init() {
  boost::lock_guard<Semaphore> lock(sem_);
  queue_->TrackPeer(peer_pb_.permanent_uuid());
  RETURN_NOT_OK(heartbeater_.Start());
  state_ = kPeerStarted;
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

  // For the first request sent by the peer, we send it even if the queue is empty,
  // which it will always appear to be for the first request, since this is the
  // negotiation round.
  if (PREDICT_FALSE(state_ == kPeerStarted)) {
    even_if_queue_empty = true;
    state_ = kPeerIdle;
  }

  DCHECK_EQ(state_, kPeerIdle);

  RETURN_NOT_OK(thread_pool_->SubmitClosure(
                  Bind(&Peer::SendNextRequest, Unretained(this), even_if_queue_empty)));
  return Status::OK();
}

void Peer::SendNextRequest(bool even_if_queue_empty) {
  // the peer has no pending request nor is sending: send the request
  Status s = queue_->RequestForPeer(peer_pb_.permanent_uuid(), &request_,
                                    &replicate_msg_refs_);
  if (PREDICT_FALSE(!s.ok())) {
    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Could not obtain request from queue for peer: "
        << peer_pb_.permanent_uuid() << ". Status: " << s.ToString();
    sem_.Release();
    return;
  }
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
    heartbeater_.Reset();
  }

  MAYBE_FAULT(FLAGS_fault_crash_on_leader_request_fraction);

  state_ = kPeerWaitingForResponse;

  VLOG_WITH_PREFIX_UNLOCKED(2) << "Sending to peer " << peer_pb().permanent_uuid() << ": "
      << request_.ShortDebugString();
  controller_.Reset();

  proxy_->UpdateAsync(&request_, &response_, &controller_,
                      boost::bind(&Peer::ProcessResponse, this));
}

void Peer::ProcessResponse() {
  // Note: This method runs on the reactor thread.

  DCHECK_EQ(0, sem_.GetValue())
    << "Got a response when nothing was pending";
  DCHECK_EQ(state_, kPeerWaitingForResponse);

  if (!controller_.status().ok()) {
    ProcessResponseError(controller_.status());
    return;
  }

  if (response_.has_error()) {
    ProcessResponseError(StatusFromPB(response_.error().status()));
    return;
  }

  // The queue's handling of the peer response may generate IO (reads against
  // the WAL) and SendNextRequest() may do the same thing. So we run the rest
  // of the response handling logic on our thread pool and not on the reactor
  // thread.
  Status s = thread_pool_->SubmitClosure(Bind(&Peer::DoProcessResponse, Unretained(this)));
  if (PREDICT_FALSE(!s.ok())) {
    LOG_WITH_PREFIX_UNLOCKED(WARNING) << "Unable to process peer response: " << s.ToString()
        << ": " << response_.ShortDebugString();
    state_ = kPeerIdle;
    sem_.Release();
  }
}

void Peer::DoProcessResponse() {
  failed_attempts_ = 0;

  DCHECK(response_.status().IsInitialized()) << "Error: Uninitialized: "
      << response_.InitializationErrorString() << ". Response: " << response_.ShortDebugString();
  VLOG_WITH_PREFIX_UNLOCKED(2) << "Response from peer " << peer_pb().permanent_uuid() << ": "
      << response_.ShortDebugString();

  bool more_pending;
  queue_->ResponseFromPeer(response_, &more_pending);
  state_ = kPeerIdle;
  if (more_pending && state_ != kPeerClosed) {
    SendNextRequest(true);
  } else {
    sem_.Release();
  }
}

void Peer::ProcessResponseError(const Status& status) {
  failed_attempts_++;
  LOG_WITH_PREFIX_UNLOCKED(WARNING) << "Couldn't send request to peer " << peer_pb_.permanent_uuid()
      << " for tablet " << tablet_id_
      << " Status: " << status.ToString() << ". Retrying in the next heartbeat period."
      << " Already tried " << failed_attempts_ << " times.";
  state_ = kPeerIdle;
  sem_.Release();
}

string Peer::LogPrefixUnlocked() const {
  return Substitute("T $0 P $1: ", tablet_id_, leader_uuid_);
}

void Peer::Close() {
  WARN_NOT_OK(heartbeater_.Stop(), "Could not stop heartbeater");

  // Acquire the semaphore to wait for any concurrent request to finish.
  boost::lock_guard<Semaphore> l(sem_);

  // If the peer is already closed return.
  if (state_ == kPeerClosed) return;
  DCHECK(state_ == kPeerIdle || state_ == kPeerStarted) << "Unexpected state: " << state_;
  state_ = kPeerClosed;

  LOG_WITH_PREFIX_UNLOCKED(INFO) << "Closing peer: " << peer_pb_.permanent_uuid();
  queue_->UntrackPeer(peer_pb_.permanent_uuid());
  // We don't own the ops (the queue does).
  request_.mutable_ops()->ExtractSubrange(0, request_.ops_size(), NULL);
}

Peer::~Peer() {
  Close();
}


RpcPeerProxy::RpcPeerProxy(gscoped_ptr<HostPort> hostport,
                           gscoped_ptr<ConsensusServiceProxy> consensus_proxy)
    : hostport_(hostport.Pass()),
      consensus_proxy_(consensus_proxy.Pass()) {
}

void RpcPeerProxy::UpdateAsync(const ConsensusRequestPB* request,
                               ConsensusResponsePB* response,
                               rpc::RpcController* controller,
                               const rpc::ResponseCallback& callback) {
  controller->set_timeout(MonoDelta::FromMilliseconds(FLAGS_consensus_rpc_timeout_ms));
  consensus_proxy_->UpdateConsensusAsync(*request, response, controller, callback);
}

void RpcPeerProxy::RequestConsensusVoteAsync(const VoteRequestPB* request,
                                             VoteResponsePB* response,
                                             rpc::RpcController* controller,
                                             const rpc::ResponseCallback& callback) {
  consensus_proxy_->RequestConsensusVoteAsync(*request, response, controller, callback);
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

Status RpcPeerProxyFactory::NewProxy(const RaftPeerPB& peer_pb,
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
                                     RaftPeerPB* remote_peer) {
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
  deadline.AddDelta(MonoDelta::FromMilliseconds(FLAGS_raft_get_node_instance_timeout_ms));
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

    LOG(WARNING) << "Error getting permanent uuid from config peer " << hostport.ToString() << ": "
                 << s.ToString();
    MonoTime now = MonoTime::Now(MonoTime::FINE);
    if (now.ComesBefore(deadline)) {
      int64_t remaining_ms = deadline.GetDeltaSince(now).ToMilliseconds();
      int64_t base_delay_ms = 1 << (attempt + 3); // 1st retry delayed 2^4 ms, 2nd 2^5, etc..
      int64_t jitter_ms = rand() % 50; // Add up to 50ms of additional random delay.
      int64_t delay_ms = std::min<int64_t>(base_delay_ms + jitter_ms, remaining_ms);
      VLOG(1) << "Sleeping " << delay_ms << " ms. before retrying to get uuid from remote peer...";
      SleepFor(MonoDelta::FromMilliseconds(delay_ms));
      LOG(INFO) << "Retrying to get permanent uuid for remote peer: "
          << remote_peer->ShortDebugString() << " attempt: " << attempt++;
    } else {
      s = Status::TimedOut(Substitute("Getting permanent uuid from $0 timed out after $1 ms.",
                                      hostport.ToString(),
                                      FLAGS_raft_get_node_instance_timeout_ms),
                           s.ToString());
      return s;
    }
  }
  remote_peer->set_permanent_uuid(resp.node_instance().permanent_uuid());
  return Status::OK();
}

}  // namespace consensus
}  // namespace kudu
