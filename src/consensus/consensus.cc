// Copyright (c) 2013, Cloudera, inc.

#include "consensus/consensus.h"
#include "util/task_executor.h"

namespace kudu {
namespace consensus {

using std::tr1::shared_ptr;

ConsensusRound::ConsensusRound(Consensus* consensus,
                               gscoped_ptr<OperationPB> replicate_op,
                               const std::tr1::shared_ptr<FutureCallback>& replicate_callback,
                               const std::tr1::shared_ptr<FutureCallback>& commit_callback)
    : consensus_(consensus),
      replicate_op_(replicate_op.Pass()),
      replicate_callback_(replicate_callback),
      commit_callback_(commit_callback),
      continuation_(NULL) {
}

ConsensusRound::ConsensusRound(Consensus* consensus,
                               gscoped_ptr<OperationPB> replicate_op)
    : consensus_(consensus),
      replicate_op_(replicate_op.Pass()),
      continuation_(NULL) {
}

Status ConsensusRound::Commit(gscoped_ptr<CommitMsg> commit) {
  commit_op_.reset(new OperationPB());
  if (leader_commit_op_.get() != NULL) {
    commit_op_->mutable_id()->CopyFrom(leader_commit_op_->id());
    commit->set_timestamp(leader_commit_op_->commit().timestamp());
  }
  commit_op_->set_allocated_commit(commit.release());
  commit_op_->mutable_commit()->mutable_commited_op_id()->CopyFrom(replicate_op_->id());
  return consensus_->Commit(this);
}

ConsensusRound* Consensus::NewRound(gscoped_ptr<ReplicateMsg> entry,
                                    const std::tr1::shared_ptr<FutureCallback>& repl_callback,
                                    const std::tr1::shared_ptr<FutureCallback>& commit_callback) {
  gscoped_ptr<OperationPB> op(new OperationPB());
  op->set_allocated_replicate(entry.release());
  return new ConsensusRound(this, op.Pass(), repl_callback, commit_callback);
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
