// Copyright (c) 2013, Cloudera, inc.

#include "consensus/local_consensus.h"

#include <boost/thread/locks.hpp>
#include <boost/assign/list_of.hpp>
#include "consensus/log.h"
#include "server/metadata.h"

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
    : ConsensusBase(options),
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

  // FIXME: Seems like a hack to get the current tablet ID from the Log.
  req->set_tablet_id(log_->tablet_id());
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
  commit_msg->mutable_commited_op_id()->CopyFrom(ctx->replicate_op()->id());
  commit_msg->mutable_change_config_response()->CopyFrom(resp);
  RETURN_NOT_OK(ctx->Commit(commit_msg.Pass()));

  RETURN_NOT_OK(commit_clbk->Wait());
  running_quorum->reset(new QuorumPB(initial_quorum));

  quorum_ = initial_quorum;
  state_ = kRunning;
  return Status::OK();
}

Status LocalConsensus::Append(
    gscoped_ptr<ReplicateMsg> entry,
    const shared_ptr<FutureCallback>& repl_callback,
    const shared_ptr<FutureCallback>& commit_callback,
    gscoped_ptr<ConsensusContext>* context) {
  DCHECK_GE(state_, kConfiguring);

  LogEntry* reserved_entry;
  gscoped_ptr<ConsensusContext> new_context;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);

    gscoped_ptr<OperationPB> replicate_op(new OperationPB);
    replicate_op->set_allocated_replicate(entry.release());

    // create the new op id for the entry.
    OpId* op_id = replicate_op->mutable_id();
    op_id->set_term(0);
    op_id->set_index(next_op_id_++);

    // create the consensus context for this round
    new_context.reset(new ConsensusContext(this, replicate_op.Pass(),
                                           repl_callback,
                                           commit_callback));

    // [Once group commit is implemented,] this will reserve the
    // correct slot in the log for the replication operation.
    // [Currently Reserve() submits a task to an execute inside Log
    // that appends and syncs the operation.]
    RETURN_NOT_OK(log_->Reserve(boost::assign::list_of(new_context->replicate_op()),
                                &reserved_entry));
  }
  RETURN_NOT_OK(log_->AsyncAppend(reserved_entry, repl_callback));

  context->reset(new_context.release());
  return Status::OK();
}

Status LocalConsensus::Update(ReplicaUpdateContext* context) {
  return Status::NotSupported("LocalConsensus does not support Update() calls.");
}

Status LocalConsensus::Commit(ConsensusContext* context, OperationPB* commit_op) {

  DCHECK(commit_op->has_commit()) << "A commit operation must have a commit.";

  LogEntry* reserved_entry;
  shared_ptr<FutureCallback> commit_clbk;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);

    // entry for the CommitMsg
    OpId* commit_id = commit_op->mutable_id();
    commit_id->set_term(0);
    commit_id->set_index(next_op_id_++);

    // the commit callback is the very last thing to execute in a transaction
    // so it needs to free all resources. We need release it from the
    // ConsensusContext or we'd get a cycle. (callback would free the
    // TransactionContext which would free the ConsensusContext, which in turn
    // would try to free the callback).
    commit_clbk = context->commit_callback();
    context->release_commit_callback();

    // [Once group commit is implemented, this will] reserve the
    // correct slot in the log for the commit operation.
    // [Currently Reserve() submits a task to an execute inside Log
    // that appends and syncs the operation.]
    RETURN_NOT_OK(log_->Reserve(boost::assign::list_of(commit_op),
                                &reserved_entry));
  }
  RETURN_NOT_OK(log_->AsyncAppend(reserved_entry, commit_clbk));
  return Status::OK();
}

Status LocalConsensus::LocalCommit(const vector<OperationPB*>& commit_ops,
                                   const shared_ptr<FutureCallback>& commit_callback) {
  LogEntry* reserved_entry;
  RETURN_NOT_OK(log_->Reserve(commit_ops, &reserved_entry));
  RETURN_NOT_OK(log_->AsyncAppend(reserved_entry, commit_callback));
  return Status::OK();
}

Status LocalConsensus::Shutdown() {
  RETURN_NOT_OK(log_->Close());
  VLOG(1) << "LocalConsensus Shutdown!";
  return Status::OK();
}

} // end namespace consensus
} // end namespace kudu
