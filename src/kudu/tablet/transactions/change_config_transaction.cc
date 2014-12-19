// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tablet/transactions/change_config_transaction.h"

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/server/hybrid_clock.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/semaphore.h"

namespace kudu {
namespace tablet {

using boost::bind;
using boost::shared_lock;
using consensus::ReplicateMsg;
using consensus::CommitMsg;
using consensus::OP_ABORT;
using consensus::CHANGE_CONFIG_OP;
using consensus::DriverType;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using strings::Substitute;
using tserver::TabletServerErrorPB;

Status ChangeConfigTransactionState::set_old_quorum(metadata::QuorumPB quorum) {
  RETURN_NOT_OK(consensus::VerifyQuorum(quorum, consensus::COMMITTED_QUORUM));
  old_quorum_.CopyFrom(quorum);
  return Status::OK();
}

string ChangeConfigTransactionState::ToString() const {
  boost::lock_guard<simple_spinlock> l(txn_state_lock_);
  return Substitute("ChangeConfigTransactionState [opid=$0, timestamp=$1, request=$2]",
                    consensus_round_->id().ShortDebugString(),
                    has_timestamp() ? timestamp().ToString() : "NULL",
                    request_ == NULL ? "(none)" : request_->ShortDebugString());
}

void ChangeConfigTransactionState::commit() {
  release_config_sem();
  // Make the request NULL since after this transaction commits
  // the request may be deleted at any moment.
  boost::lock_guard<simple_spinlock> l(txn_state_lock_);
  request_ = NULL;
  response_ = NULL;
}

ChangeConfigTransaction::ChangeConfigTransaction(ChangeConfigTransactionState* tx_state,
                                                 DriverType type,
                                                 Semaphore* config_sem)
    : Transaction(tx_state, type, Transaction::CHANGE_CONFIG_TXN),
      state_(tx_state),
      config_sem_(config_sem) {
}

void ChangeConfigTransaction::NewReplicateMsg(gscoped_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(CHANGE_CONFIG_OP);
  (*replicate_msg)->mutable_change_config_request()->CopyFrom(*state()->request());
}

Status ChangeConfigTransaction::Prepare() {
  TRACE("PREPARE CHANGE CONFIG: Starting");

  state_->acquire_config_sem(config_sem_);

  DCHECK(state_->old_quorum().IsInitialized());
  DCHECK(state_->request()->has_new_config());
  Status s = consensus::VerifyQuorum(state_->request()->new_config(),
                                     consensus::UNCOMMITTED_QUORUM);

  TRACE("PREPARE CHANGE CONFIG: finished (Status: $0)", s.ToString());

  if (!s.ok()) {
    state_->completion_callback()->set_error(s, TabletServerErrorPB::INVALID_CONFIG);
  }

  return s;
}

Status ChangeConfigTransaction::Start() {
  // now that we've acquired the semaphore, set the transaction timestamp
  state_->set_timestamp(state_->tablet_peer()->clock()->Now());
  TRACE("START. Timestamp: $0", server::HybridClock::GetPhysicalValue(state_->timestamp()));
  return Status::OK();
}

void ChangeConfigTransaction::NewCommitAbortMessage(gscoped_ptr<CommitMsg>* commit_msg) {
  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(OP_ABORT);
  (*commit_msg)->mutable_change_config_response()->CopyFrom(*state_->response());
}

Status ChangeConfigTransaction::Apply(gscoped_ptr<CommitMsg>* commit_msg) {
  TRACE("APPLY CHANGE CONFIG: Starting");

  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(CHANGE_CONFIG_OP);
  (*commit_msg)->set_timestamp(state_->timestamp().ToUint64());
  return Status::OK();
}

void ChangeConfigTransaction::Finish() {
  // Now that all of the changes have been applied and the commit is durable
  // make the changes visible to readers.
  TRACE("APPLY CHANGE CONFIG: apply finished");

  // Notify the peer that the consensus state has changed.
  if (VLOG_IS_ON(2)) {
    string tablet_id = state_->tablet_peer()->consensus()->tablet_id();
    string peer_uuid = state_->tablet_peer()->consensus()->peer_uuid();
    VLOG(2) << Substitute("T $0 P $1: Committing ChangeConfigTransaction: $2",
                          tablet_id, peer_uuid, ToString());
  }

  // We set the opid_index in the quorum here just for the benefit of logging.
  // The Consensus implementation has identical logic that is actually used to
  // persist this config change to the ConsensusMetadata file.
  QuorumPB new_quorum = state_->request()->new_config();
  new_quorum.set_opid_index(state_->consensus_round()->id().index());
  string tablet_id = state_->tablet_peer()->tablet_id();
  string uuid = state_->tablet_peer()->permanent_uuid();
  LOG(INFO) << Substitute("T $0 P $1: Configuration changed", tablet_id, uuid) << std::endl
            << "Changed from: " << state_->old_quorum().ShortDebugString() << std::endl
            << "Changed to: " << new_quorum.ShortDebugString();

  QuorumPeerPB::Role new_role = consensus::GetRoleInQuorum(uuid, new_quorum);
  state_->tablet_peer()->ConsensusStateChanged(new_role);
  state()->commit();
}

string ChangeConfigTransaction::ToString() const {
  return Substitute("ChangeConfigTransaction [type=$0, state=$1]",
                    DriverType_Name(type()),
                    state_->ToString());
}

}  // namespace tablet
}  // namespace kudu
