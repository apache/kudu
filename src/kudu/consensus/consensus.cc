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
                               ConsensusCommitContinuation* commit_continuation)
    : consensus_(consensus),
      replicate_msg_(new RefCountedReplicate(replicate_msg.release())),
      continuation_(commit_continuation) {
}

ConsensusRound::ConsensusRound(Consensus* consensus,
                               const ReplicateRefPtr& replicate_msg)
    : consensus_(consensus),
      replicate_msg_(replicate_msg),
      continuation_(NULL) {
  DCHECK_NOTNULL(replicate_msg_.get());
}

void ConsensusRound::NotifyReplicationFinished(const Status& status) {
  if (PREDICT_TRUE(continuation_)) {
    continuation_->ReplicationFinished(status);
  }
}

ConsensusRound* Consensus::NewRound(gscoped_ptr<ReplicateMsg> replicate_msg,
                                    ConsensusCommitContinuation* commit_continuation) {
  return new ConsensusRound(this, replicate_msg.Pass(), commit_continuation);
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
