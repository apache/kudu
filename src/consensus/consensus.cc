// Copyright (c) 2013, Cloudera, inc.

#include "consensus/consensus.h"
#include "util/task_executor.h"

namespace kudu {
namespace consensus {

using std::tr1::shared_ptr;

ConsensusContext::ConsensusContext(Consensus* consensus,
                                   gscoped_ptr<ReplicateMsg> replicate_msg,
                                   const std::tr1::shared_ptr<FutureCallback>& replicate_callback,
                                   const std::tr1::shared_ptr<FutureCallback>& commit_callback)
    : consensus_(consensus),
      replicate_msg_(replicate_msg.Pass()),
      replicate_callback_(replicate_callback),
      commit_callback_(commit_callback) {
}

void ConsensusContext::Commit(gscoped_ptr<CommitMsg> commit) {
  commit->mutable_commited_op_id()->CopyFrom(replicate_msg_->id());
  commit_msg_.reset(commit.release());
  consensus_->Commit(this, commit.get());
}

}  // end namespace kudu
}  // end namespace consensus
