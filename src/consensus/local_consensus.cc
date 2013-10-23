// Copyright (c) 2013, Cloudera, inc.

#include "consensus/local_consensus.h"
#include "consensus/tasks/tasks.h"

namespace kudu {
namespace consensus {

using std::tr1::shared_ptr;
using log::LogEntry;
using base::subtle::Barrier_AtomicIncrement;

LocalConsensus::LocalConsensus(const ConsensusOptions& options,
                               log::Log* log)
    : log_(log),
      log_executor_(TaskExecutor::CreateNew(1)),
      commit_executor_(TaskExecutor::CreateNew(1)),
      next_op_id_(log->last_entry_id().index() + 1) {
}

Status LocalConsensus::Start() {
  return Status::OK();
}

Status LocalConsensus::Shutdown() {
  log_executor_->Shutdown();
  commit_executor_->Shutdown();
  RETURN_NOT_OK(log_->Close());
  VLOG(1) << "LocalConsensus Shutdown!";
  return Status::OK();
}

Status LocalConsensus::Append(
    gscoped_ptr<ReplicateMsg> entry,
    const std::tr1::shared_ptr<FutureCallback>& repl_callback,
    const std::tr1::shared_ptr<FutureCallback>& commit_callback,
    gscoped_ptr<ConsensusContext>* context) {

  // create the new op id for the entry.
  OpId* op_id = entry->mutable_id();
  op_id->set_term(0);
  op_id->set_index(Barrier_AtomicIncrement(&next_op_id_, 1) - 1);

  // create the consensus context for this round
  gscoped_ptr<ConsensusContext> new_context(new ConsensusContext(this, entry.Pass(),
                                                                 repl_callback,
                                                                 commit_callback));

  // initiate the log task that will log the entry and set the
  // replicate callback on it.
  shared_ptr<Task> log_task(new LogTask(log_, new_context->replicate_msg()));
  shared_ptr<Future> future;
  RETURN_NOT_OK(log_executor_->Submit(log_task, &future));
  future->AddListener(repl_callback);

  context->reset(new_context.release());
  return Status::OK();
}

Status LocalConsensus::Commit(ConsensusContext* context, CommitMsg *commit) {

  // entry for the CommitMsg
  OpId commit_id;
  commit_id.set_term(0);
  commit_id.set_index(Barrier_AtomicIncrement(&next_op_id_, 1) - 1);
  commit->mutable_id()->CopyFrom(commit_id);

  // the commit callback is the very last thing to execute in a transaction
  // so it needs to free all resources. We need release it from the
  // ConsensusContext or we'd get a cycle. (callback would free the
  // TransactionContext which would free the ConsensusContext, which in turn
  // would try to free the callback).
  shared_ptr<FutureCallback> commit_clbk = context->commit_callback();
  context->release_commit_callback();

  // Initiate the commit task.
  shared_ptr<Task> commit_task(new CommitTask(log_, commit));
  shared_ptr<Future> commit_future;
  RETURN_NOT_OK(log_executor_->Submit(commit_task, &commit_future));

  commit_future->AddListener(commit_clbk);
  return Status::OK();
}

Status LocalConsensus::LocalCommit(CommitMsg* commit_msg,
                                   std::tr1::shared_ptr<kudu::Future>* commit_future) {
  // entry for the CommitMsg, note that while LocalConsensus may assign a
  // 'consensus' id to the commit message, a distributed implementation may
  // not as other replicas will have no knowledge of this update.
  OpId* commit_id = commit_msg->mutable_id();
  commit_id->set_term(0);
  commit_id->set_index(Barrier_AtomicIncrement(&next_op_id_, 1) - 1);

  // Initiate the commit task.
  shared_ptr<Task> commit_task(new CommitTask(log_, commit_msg));
  RETURN_NOT_OK(log_executor_->Submit(commit_task, commit_future));
  return Status::OK();
}

} // end namespace consensus
} // end namespace kudu
