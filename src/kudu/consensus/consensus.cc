// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/consensus/consensus.h"

#include <boost/foreach.hpp>
#include <set>

#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/task_executor.h"

namespace kudu {
namespace consensus {

using std::tr1::shared_ptr;
using strings::Substitute;

ConsensusBootstrapInfo::ConsensusBootstrapInfo()
  : last_id(MinimumOpId()),
    last_committed_id(MinimumOpId()) {
}

ConsensusBootstrapInfo::~ConsensusBootstrapInfo() {
  STLDeleteElements(&orphaned_replicates);
}

ConsensusRound::ConsensusRound(Consensus* consensus,
                               gscoped_ptr<ReplicateMsg> replicate_msg,
                               ConsensusCommitContinuation* commit_continuation,
                               const std::tr1::shared_ptr<FutureCallback>& commit_callback)
    : consensus_(consensus),
      replicate_msg_(replicate_msg.Pass()),
      continuation_(commit_continuation),
      commit_callback_(commit_callback) {
}

ConsensusRound::ConsensusRound(Consensus* consensus,
                               gscoped_ptr<ReplicateMsg> replicate_msg)
    : consensus_(consensus),
      replicate_msg_(replicate_msg.Pass()),
      continuation_(NULL) {
  DCHECK_NOTNULL(replicate_msg_.get());
}

Status ConsensusRound::Commit(gscoped_ptr<CommitMsg> commit) {
  commit->mutable_commited_op_id()->CopyFrom(replicate_msg_->id());
  return consensus_->Commit(commit.Pass(), commit_callback_->AsStatusCallback());
}

Status Consensus::VerifyQuorum(const metadata::QuorumPB& quorum) {
  std::set<string> uuids;
  bool found_leader = false;
  if (quorum.peers_size() == 0) {
    return Status::IllegalState(
        Substitute("Quorum must have at least one peer. Quorum: $0",
                   quorum.ShortDebugString()));
  }

  if (!quorum.has_local()) {
    return Status::IllegalState(
        Substitute("Quorum must specify whether it is local. Quorum: ",
                   quorum.ShortDebugString()));
  }

  if (!quorum.has_seqno()) {
    return Status::IllegalState(
        Substitute("Quorum must have a sequence number. Quorum: ",
                   quorum.ShortDebugString()));
  }

  // Local quorums must have only one peer and it may or may not
  // have an address.
  if (quorum.local()) {
    if (quorum.peers_size() != 1) {
      return Status::IllegalState(
          Substitute("Local quorums must have 1 and only one peer. Quorum: ",
                     quorum.ShortDebugString()));
    }
    if (!quorum.peers(0).has_permanent_uuid() ||
        quorum.peers(0).permanent_uuid() == "") {
      return Status::IllegalState(
          Substitute("Local peer must have an UUID. Quorum: ",
                     quorum.ShortDebugString()));
    }
    return Status::OK();
  }

  BOOST_FOREACH(const metadata::QuorumPeerPB& peer, quorum.peers()) {
    if (!peer.has_permanent_uuid() || peer.permanent_uuid() == "") {
      return Status::IllegalState(Substitute("One peer didn't have an uuid or had the empty"
          " string. Quorum: $0", quorum.ShortDebugString()));
    }
    if (uuids.count(peer.permanent_uuid()) == 1) {
      return Status::IllegalState(
          Substitute("Found two peers with uuid: $0. Quorum: $1",
                     peer.permanent_uuid(), quorum.ShortDebugString()));
    }
    uuids.insert(peer.permanent_uuid());

    if (!peer.has_last_known_addr()) {
      return Status::IllegalState(
          Substitute("Peer: $0 has no address. Quorum: $1",
                     peer.permanent_uuid(), quorum.ShortDebugString()));
    }
    if (!peer.has_role()) {
      return Status::IllegalState(
          Substitute("Peer: $0 has no role. Quorum: $1", peer.permanent_uuid(),
                     quorum.ShortDebugString()));
    }
    if (peer.role() == metadata::QuorumPeerPB::LEADER
        || peer.role() == metadata::QuorumPeerPB::CANDIDATE) {
      if (!found_leader) {
        found_leader = true;
        continue;
      }
      return Status::IllegalState(
          Substitute("Found two peers with LEADER/CANDIDATE role. Quorum: $0",
                     quorum.ShortDebugString()));
    }
    if (peer.role() == metadata::QuorumPeerPB::LEARNER) {
      return Status::IllegalState(
          Substitute(
              "Peer: $0 has LEARNER role but this isn't supported yet. Quorum: $1",
              peer.permanent_uuid(), quorum.ShortDebugString()));
    }
  }
  return Status::OK();
}

ConsensusRound* Consensus::NewRound(gscoped_ptr<ReplicateMsg> replicate_msg,
                                    ConsensusCommitContinuation* commit_continuation,
                                    const std::tr1::shared_ptr<FutureCallback>& commit_callback) {
  return new ConsensusRound(this, replicate_msg.Pass(), commit_continuation, commit_callback);
}

void Consensus::SetFaultHooks(const std::tr1::shared_ptr<ConsensusFaultHooks>& hooks) {
  fault_hooks_ = hooks;
}

const std::tr1::shared_ptr<Consensus::ConsensusFaultHooks>& Consensus::GetFaultHooks() const {
  return fault_hooks_;
}

Status Consensus::ExecuteHook(HookPoint point) {
  if (PREDICT_FALSE(fault_hooks_.get() != NULL)) {
    switch (point) {
      case Consensus::PRE_START: return fault_hooks_->PreStart();
      case Consensus::POST_START: return fault_hooks_->PostStart();
      case Consensus::PRE_CONFIG_CHANGE: return fault_hooks_->PreConfigChange();
      case Consensus::POST_CONFIG_CHANGE: return fault_hooks_->PostConfigChange();
      case Consensus::PRE_REPLICATE: return fault_hooks_->PreReplicate();
      case Consensus::POST_REPLICATE: return fault_hooks_->PostReplicate();
      case Consensus::PRE_COMMIT: return fault_hooks_->PreCommit();
      case Consensus::POST_COMMIT: return fault_hooks_->PostCommit();
      case Consensus::PRE_UPDATE: return fault_hooks_->PreUpdate();
      case Consensus::POST_UPDATE: return fault_hooks_->PostUpdate();
      case Consensus::PRE_SHUTDOWN: return fault_hooks_->PreShutdown();
      case Consensus::POST_SHUTDOWN: return fault_hooks_->PostShutdown();
      default: LOG(FATAL) << "Unknown fault hook.";
    }
  }
  return Status::OK();
}

} // namespace consensus
} // namespace kudu
