// Copyright (c) 2013, Cloudera, inc.

#include "consensus/consensus.h"
#include "util/task_executor.h"

namespace kudu {
namespace consensus {

using std::tr1::shared_ptr;

ConsensusContext::ConsensusContext(Consensus* consensus,
                                   gscoped_ptr<OperationPB> replicate_op,
                                   const std::tr1::shared_ptr<FutureCallback>& replicate_callback,
                                   const std::tr1::shared_ptr<FutureCallback>& commit_callback)
    : consensus_(consensus),
      replicate_op_(replicate_op.Pass()),
      replicate_callback_(replicate_callback),
      commit_callback_(commit_callback) {
}

Status ConsensusContext::Commit(gscoped_ptr<CommitMsg> commit) {
  commit_op_.reset(new OperationPB());
  commit_op_->set_allocated_commit(commit.release());
  commit_op_->mutable_commit()->mutable_commited_op_id()->CopyFrom(replicate_op_->id());
  return consensus_->Commit(this, commit_op_.get());
}

} // namespace consensus
} // namespace kudu
