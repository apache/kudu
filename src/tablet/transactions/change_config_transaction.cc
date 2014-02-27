// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transactions/change_config_transaction.h"
#include "tablet/transactions/write_util.h"

#include "common/wire_protocol.h"
#include "rpc/rpc_context.h"
#include "tablet/tablet.h"
#include "tablet/tablet_peer.h"
#include "tablet/tablet_metrics.h"
#include "tserver/tserver.pb.h"
#include "util/trace.h"

namespace kudu {
namespace tablet {

using boost::bind;
using boost::shared_lock;
using consensus::ReplicateMsg;
using consensus::CommitMsg;
using consensus::OP_ABORT;
using consensus::CHANGE_CONFIG_OP;
using metadata::QuorumPB;
using strings::Substitute;
using tserver::TabletServerErrorPB;

LeaderChangeConfigTransaction::LeaderChangeConfigTransaction(
    ChangeConfigTransactionContext* tx_ctx,
    consensus::Consensus* consensus,
    TaskExecutor* prepare_executor,
    TaskExecutor* apply_executor,
    simple_spinlock& prepare_replicate_lock,
    boost::mutex* config_lock)
: LeaderTransaction(consensus,
                    prepare_executor,
                    apply_executor,
                    prepare_replicate_lock),
  tx_ctx_(tx_ctx),
  config_lock_(config_lock) {
}

void LeaderChangeConfigTransaction::NewReplicateMsg(gscoped_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(CHANGE_CONFIG_OP);
  (*replicate_msg)->mutable_change_config_request()->CopyFrom(*tx_ctx()->request());
}

Status LeaderChangeConfigTransaction::Prepare() {
  TRACE("PREPARE CHANGE CONFIG: Starting");

  tx_ctx_->acquire_config_lock(config_lock_);

  // now that we've acquired the lock set the transaction timestamp
  tx_ctx_->set_timestamp(tx_ctx_->tablet_peer()->clock()->Now());

  const QuorumPB& old_quorum = tx_ctx_->tablet_peer()->tablet()->metadata()->Quorum();
  const QuorumPB& new_quorum = tx_ctx_->request()->new_config();

  Status s;
  if (old_quorum.seqno() >= new_quorum.seqno()) {
    s = Status::IllegalState(Substitute("New Quorum configuration has a "
        "lower sequence number than the old configuration. Old: $0. New: $1",
        old_quorum.DebugString(), new_quorum.DebugString()));
  }

  TRACE("PREPARE CHANGE CONFIG: finished (Status: $0)", s.ToString());

  if (!s.ok()) {
     tx_ctx_->completion_callback()->set_error(s, TabletServerErrorPB::INVALID_CONFIG);
  }
  return s;
}

void LeaderChangeConfigTransaction::PrepareFailedPreCommitHooks(
    gscoped_ptr<CommitMsg>* commit_msg) {
  // Release the meta lock (no effect if no locks were acquired).
  tx_ctx_->release_config_lock();

  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(OP_ABORT);
  (*commit_msg)->mutable_change_config_response()->CopyFrom(*tx_ctx_->response());
  tx_ctx_->timestamp().EncodeToString((*commit_msg)->mutable_timestamp());
}

Status LeaderChangeConfigTransaction::Apply() {
  TRACE("APPLY CHANGE CONFIG: Starting");

  // change the config in the tablet metadata.
  tx_ctx_->tablet_peer()->tablet()->metadata()->SetQuorum(tx_ctx_->request()->new_config());

  gscoped_ptr<CommitMsg> commit(new CommitMsg());
  commit->set_op_type(CHANGE_CONFIG_OP);
  tx_ctx_->timestamp().EncodeToString(commit->mutable_timestamp());

  TRACE("APPLY CHANGE CONFIG: finished, triggering COMMIT");

  RETURN_NOT_OK(tx_ctx_->consensus_ctx()->Commit(commit.Pass()));
  // NB: do not use tx_ctx_ after this point, because the commit may have
  // succeeded, in which case the context may have been torn down.
  return Status::OK();
}

void LeaderChangeConfigTransaction::ApplySucceeded() {
  // Now that all of the changes have been applied and the commit is durable
  // make the changes visible to readers.
  TRACE("APPLY CHANGE CONFIG: apply finished");

  tx_ctx()->commit();
  LeaderTransaction::ApplySucceeded();
}

}  // namespace tablet
}  // namespace kudu
