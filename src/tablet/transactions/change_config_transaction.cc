// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transactions/change_config_transaction.h"
#include "tablet/transactions/write_util.h"

#include "common/wire_protocol.h"
#include "rpc/rpc_context.h"
#include "tablet/tablet.h"
#include "tablet/tablet_peer.h"
#include "tablet/tablet_metrics.h"
#include "tserver/tserver.pb.h"
#include "util/semaphore.h"
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

string ChangeConfigTransactionState::ToString() const {
  return Substitute("ChangeConfigTransactionState [timestamp=$0, request=$1]",
                    timestamp().ToString(),
                    request_ == NULL ? "(none)" : request_->ShortDebugString());
}

ChangeConfigTransaction::ChangeConfigTransaction(ChangeConfigTransactionState* tx_state,
                                                 DriverType type,
                                                 Semaphore* config_sem)
    : Transaction(tx_state, type, Transaction::CHANGE_CONFIG_TXN),
      tx_state_(tx_state),
      config_sem_(config_sem) {
}

void ChangeConfigTransaction::NewReplicateMsg(gscoped_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(CHANGE_CONFIG_OP);
  (*replicate_msg)->mutable_change_config_request()->CopyFrom(*state()->request());
}

Status ChangeConfigTransaction::Prepare() {
  TRACE("PREPARE CHANGE CONFIG: Starting");

  tx_state_->acquire_config_sem(config_sem_);

  // now that we've acquired the semaphore, set the transaction timestamp
  tx_state_->set_timestamp(tx_state_->tablet_peer()->clock()->Now());

  const QuorumPB& old_quorum = tx_state_->tablet_peer()->tablet()->metadata()->Quorum();
  const QuorumPB& new_quorum = tx_state_->request()->new_config();

  Status s;
  if (old_quorum.seqno() >= new_quorum.seqno()) {
    s = Status::IllegalState(Substitute("New Quorum configuration has a "
        "lower sequence number than the old configuration. Old: $0. New: $1",
        old_quorum.DebugString(), new_quorum.DebugString()));
  }

  TRACE("PREPARE CHANGE CONFIG: finished (Status: $0)", s.ToString());

  if (!s.ok()) {
    tx_state_->completion_callback()->set_error(s, TabletServerErrorPB::INVALID_CONFIG);
  }
  return s;
}

void ChangeConfigTransaction::NewCommitAbortMessage(gscoped_ptr<CommitMsg>* commit_msg) {
  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(OP_ABORT);
  (*commit_msg)->mutable_change_config_response()->CopyFrom(*tx_state_->response());
  (*commit_msg)->set_timestamp(tx_state_->timestamp().ToUint64());
}

Status ChangeConfigTransaction::Apply(gscoped_ptr<CommitMsg>* commit_msg) {
  TRACE("APPLY CHANGE CONFIG: Starting");

  // change the config in the tablet metadata.
  tx_state_->tablet_peer()->tablet()->metadata()->SetQuorum(tx_state_->request()->new_config());
  RETURN_NOT_OK(tx_state_->tablet_peer()->tablet()->metadata()->Flush());

  // Notify the peer that the consensus state (in this case the role) has changed.
  tx_state_->tablet_peer()->ConsensusStateChanged();

  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(CHANGE_CONFIG_OP);
  (*commit_msg)->set_timestamp(tx_state_->timestamp().ToUint64());
  return Status::OK();
}

void ChangeConfigTransaction::Finish() {
  // Now that all of the changes have been applied and the commit is durable
  // make the changes visible to readers.
  TRACE("APPLY CHANGE CONFIG: apply finished");
  state()->commit();
}

string ChangeConfigTransaction::ToString() const {
  return Substitute("ChangeConfigTransaction [state=$0]", tx_state_->ToString());
}

}  // namespace tablet
}  // namespace kudu
