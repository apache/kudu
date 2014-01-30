// Copyright (c) 2013, Cloudera, inc.

#include "consensus/local_consensus.h"

#include <boost/thread/locks.hpp>

#include "consensus/tasks/tasks.h"

namespace kudu {
namespace consensus {

using base::subtle::Barrier_AtomicIncrement;
using consensus::CHANGE_CONFIG_OP;
using log::Log;
using log::LogEntry;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using std::tr1::shared_ptr;
using tserver::ChangeConfigRequestPB;
using tserver::ChangeConfigResponsePB;

LocalConsensus::LocalConsensus(const ConsensusOptions& options)
    : log_(NULL),
      log_executor_(TaskExecutor::CreateNew("log exec", 1)),
      commit_executor_(TaskExecutor::CreateNew("commit exec", 1)),
      next_op_id_(-1) {
}

Status LocalConsensus::Init(const QuorumPeerPB& peer,
                            Log* log) {
  CHECK_EQ(state_, kNotInitialized);
  peer_ = peer;
  log_ = log;
  state_ = kInitializing;
  next_op_id_ = log->last_entry_id().index() + 1;
  return Status::OK();
}

Status LocalConsensus::Start(const metadata::QuorumPB& initial_quorum,
                             gscoped_ptr<metadata::QuorumPB>* running_quorum) {

  CHECK_EQ(state_, kInitializing);

  CHECK(initial_quorum.local()) << "Local consensus must be passed a local quorum";
  CHECK_LE(initial_quorum.peers_size(), 1);

  // Because this is the local consensus we always push the configuration,
  // in the dist. impl. we only try and push if we're leader.
  gscoped_ptr<ReplicateMsg> replicate_msg(new ReplicateMsg);
  replicate_msg->set_op_type(CHANGE_CONFIG_OP);
  ChangeConfigRequestPB* req = replicate_msg->mutable_change_config_request();
  req->set_tablet_id(log_->current_header()->tablet_meta().oid());
  req->mutable_new_config()->CopyFrom(initial_quorum);

  shared_ptr<LatchCallback> replicate_clbk(new LatchCallback);
  shared_ptr<LatchCallback> commit_clbk(new LatchCallback);
  state_ = kConfiguring;

  gscoped_ptr<ConsensusContext> ctx;
  RETURN_NOT_OK(Append(replicate_msg.Pass(), replicate_clbk, commit_clbk, &ctx));
  RETURN_NOT_OK(replicate_clbk->Wait());

  ChangeConfigResponsePB resp;
  gscoped_ptr<CommitMsg> commit_msg(new CommitMsg);
  commit_msg->set_op_type(CHANGE_CONFIG_OP);
  commit_msg->mutable_commited_op_id()->CopyFrom(ctx->replicate_msg()->id());
  commit_msg->mutable_change_config_response()->CopyFrom(resp);
  RETURN_NOT_OK(ctx->Commit(commit_msg.Pass()));

  RETURN_NOT_OK(commit_clbk->Wait());
  running_quorum->reset(new QuorumPB(initial_quorum));
  state_ = kRunning;
  return Status::OK();
}

Status LocalConsensus::Append(
    gscoped_ptr<ReplicateMsg> entry,
    const shared_ptr<FutureCallback>& repl_callback,
    const shared_ptr<FutureCallback>& commit_callback,
    gscoped_ptr<ConsensusContext>* context) {
  DCHECK_GE(state_, kConfiguring);

  // TODO add a test for this once we get delayed executors (KUDU-52)
  boost::lock_guard<simple_spinlock> lock(lock_);

  // create the new op id for the entry.
  OpId* op_id = entry->mutable_id();
  op_id->set_term(0);
  op_id->set_index(next_op_id_++);

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

  // TODO add a test for this once we get delayed executors (KUDU-52)
  boost::lock_guard<simple_spinlock> lock(lock_);

  // entry for the CommitMsg
  OpId commit_id;
  commit_id.set_term(0);
  commit_id.set_index(next_op_id_++);
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
  // Initiate the commit task.
  shared_ptr<Task> commit_task(new CommitTask(log_, commit_msg));
  RETURN_NOT_OK(log_executor_->Submit(commit_task, commit_future));
  return Status::OK();
}

Status LocalConsensus::Shutdown() {
  log_executor_->Shutdown();
  commit_executor_->Shutdown();
  RETURN_NOT_OK(log_->Close());
  VLOG(1) << "LocalConsensus Shutdown!";
  return Status::OK();
}

} // end namespace consensus
} // end namespace kudu
