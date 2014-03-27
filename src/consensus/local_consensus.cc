// Copyright (c) 2013, Cloudera, inc.

#include "consensus/local_consensus.h"

#include <boost/thread/locks.hpp>
#include <boost/assign/list_of.hpp>
#include "consensus/log.h"
#include "server/metadata.h"
#include "server/clock.h"

namespace kudu {
namespace consensus {

using base::subtle::Barrier_AtomicIncrement;
using consensus::CHANGE_CONFIG_OP;
using log::Log;
using log::LogEntryBatch;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using std::tr1::shared_ptr;
using tserver::ChangeConfigRequestPB;
using tserver::ChangeConfigResponsePB;

LocalConsensus::LocalConsensus(const ConsensusOptions& options)
    : options_(options),
      next_op_id_index_(-1),
      state_(kNotInitialized),
      log_(NULL) {
}

Status LocalConsensus::Init(const QuorumPeerPB& peer,
                            const scoped_refptr<server::Clock>& clock,
                            ReplicaTransactionFactory* txn_factory,
                            Log* log) {
  CHECK_EQ(state_, kNotInitialized);
  peer_ = peer;
  clock_ = clock;
  log_ = log;
  state_ = kInitializing;
  OpId initial;
  Status s = log->GetLastEntryOpId(&initial);
  if (s.ok()) {
    // We are continuing after previously running.
  } else if (s.IsNotFound()) {
    // This is our very first startup! Sally forth!
    initial = log::MinimumOpId();
  } else {
    LOG(FATAL) << "Unexpected status from Log::GetLastEntryOpId(): "
               << s.ToString();
  }
  next_op_id_index_ = initial.index() + 1;
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

  gscoped_ptr<ConsensusContext> ctx(NewContext(replicate_msg.Pass(),
                                               replicate_clbk,
                                               commit_clbk));
  RETURN_NOT_OK(Replicate(ctx.get()));
  RETURN_NOT_OK(replicate_clbk->Wait());

  ChangeConfigResponsePB resp;
  gscoped_ptr<CommitMsg> commit_msg(new CommitMsg);
  commit_msg->set_op_type(CHANGE_CONFIG_OP);
  commit_msg->mutable_commited_op_id()->CopyFrom(ctx->replicate_op()->id());
  commit_msg->mutable_change_config_response()->CopyFrom(resp);
  clock_->Now().EncodeToString(commit_msg->mutable_timestamp());
  RETURN_NOT_OK(ctx->Commit(commit_msg.Pass()));


  RETURN_NOT_OK(commit_clbk->Wait());
  running_quorum->reset(new QuorumPB(initial_quorum));

  quorum_ = initial_quorum;
  state_ = kRunning;
  return Status::OK();
}

Status LocalConsensus::Replicate(ConsensusContext* context) {
  DCHECK_GE(state_, kConfiguring);

  LogEntryBatch* reserved_entry_batch;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);

    // create the new op id for the entry.
    OpId* cur_op_id = DCHECK_NOTNULL(context->replicate_op())->mutable_id();
    cur_op_id->set_term(0);
    cur_op_id->set_index(next_op_id_index_++);

    // TODO: Register TransactionContext (not currently passed) to avoid Log GC
    // race between Append() and Apply(). See KUDU-152.

    // Reserve the correct slot in the log for the replication operation.
    RETURN_NOT_OK(log_->Reserve(boost::assign::list_of(context->replicate_op()),
                                &reserved_entry_batch));
  }
  // Serialize and mark the message as ready to be appended.
  // When the Log actually fsync()s this message to disk, 'repl_callback'
  // is triggered.
  RETURN_NOT_OK(log_->AsyncAppend(reserved_entry_batch, context->replicate_callback()));

  return Status::OK();
}

Status LocalConsensus::Update(const ConsensusRequestPB* request,
                              ConsensusResponsePB* response) {
  return Status::NotSupported("LocalConsensus does not support Update() calls.");
}

Status LocalConsensus::Commit(ConsensusContext* context) {

  OperationPB* commit_op = DCHECK_NOTNULL(context->commit_op());
  DCHECK(commit_op->has_commit()) << "A commit operation must have a commit.";

  LogEntryBatch* reserved_entry_batch;
  shared_ptr<FutureCallback> commit_clbk;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);

    // entry for the CommitMsg
    OpId* commit_id = commit_op->mutable_id();
    commit_id->set_term(0);
    commit_id->set_index(next_op_id_index_++);

    // The commit callback is the very last thing to execute in a transaction
    // so it needs to free all resources. We need release it from the
    // ConsensusContext or we'd get a cycle. (callback would free the
    // TransactionContext which would free the ConsensusContext, which in turn
    // would try to free the callback).
    commit_clbk = context->commit_callback();
    context->release_commit_callback();

    // Reserve the correct slot in the log for the commit operation.
    RETURN_NOT_OK(log_->Reserve(boost::assign::list_of(commit_op),
                                &reserved_entry_batch));
  }
  // Serialize and mark the message as ready to be appended.
  // When the Log actually fsync()s this message to disk, 'commit_clbk'
  // is triggered.
  RETURN_NOT_OK(log_->AsyncAppend(reserved_entry_batch, commit_clbk));
  return Status::OK();
}

void LocalConsensus::Shutdown() {
  WARN_NOT_OK(log_->Close(), "Error closing the Log.");
  VLOG(1) << "LocalConsensus Shutdown!";
}

} // end namespace consensus
} // end namespace kudu
