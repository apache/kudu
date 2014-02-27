// Copyright (c) 2013, Cloudera, inc.

#include "tablet/tablet_peer.h"

#include "consensus/local_consensus.h"
#include "consensus/log.h"
#include "consensus/opid_anchor_registry.h"
#include "gutil/strings/substitute.h"
#include "gutil/sysinfo.h"
#include "tablet/transactions/alter_schema_transaction.h"
#include "tablet/transactions/change_config_transaction.h"
#include "tablet/transactions/write_transaction.h"
#include "tablet/tablet_metrics.h"
#include "util/metrics.h"
#include "util/trace.h"

namespace kudu {
namespace tablet {

using consensus::ConsensusOptions;
using consensus::LocalConsensus;
using log::Log;
using log::OpIdAnchorRegistry;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using metadata::TabletMetadata;

// ============================================================================
//  Tablet Peer
// ============================================================================
TabletPeer::TabletPeer()
    : // prepare executor has a single thread as prepare must be done in order
      // of submission
      prepare_executor_(TaskExecutor::CreateNew("prepare exec", 1)) {
  apply_executor_.reset(TaskExecutor::CreateNew("apply exec", base::NumCPUs()));
  state_ = metadata::BOOTSTRAPPING;
}

Status TabletPeer::Init(const shared_ptr<Tablet>& tablet,
                        const scoped_refptr<server::Clock>& clock,
                        const QuorumPeerPB& quorum_peer,
                        gscoped_ptr<Log> log,
                        gscoped_ptr<OpIdAnchorRegistry> opid_anchor_registry) {


  {
    boost::lock_guard<simple_spinlock> lock(internal_state_lock_);
    state_ = metadata::CONFIGURING;
    tablet_ = tablet;
    clock_ = clock;
    quorum_peer_ = quorum_peer;
    log_.reset(log.release());
    opid_anchor_registry_.reset(opid_anchor_registry.release());
    // TODO support different consensus implementations (possibly by adding
    // a TabletPeerOptions).
    consensus_.reset(new LocalConsensus(ConsensusOptions()));
  }

  DCHECK(tablet_) << "A TabletPeer must be provided with a Tablet";
  DCHECK(log_) << "A TabletPeer must be provided with a Log";
  DCHECK(opid_anchor_registry_) << "A TabletPeer must be provided with a OpIdAnchorRegistry";

  RETURN_NOT_OK(consensus_->Init(quorum_peer_, clock, log_.get()));

  // set consensus on the tablet to that it can store local state changes
  // in the log.
  tablet_->SetConsensus(consensus_.get());
  return Status::OK();
}

Status TabletPeer::Start(const QuorumPB& quorum) {

  // Prevent any SubmitChangeConfig calls to try and modify the config
  // until consensus is booted and the actual configuration is stored in
  // the tablet meta.
  boost::lock_guard<boost::mutex> config_lock(config_lock_);

  gscoped_ptr<QuorumPB> actual_config;

  RETURN_NOT_OK(consensus_->Start(quorum, &actual_config));
  tablet_->metadata()->SetQuorum(*actual_config.get());
  RETURN_NOT_OK(tablet_->metadata()->Flush());

  {
    boost::lock_guard<simple_spinlock> lock(internal_state_lock_);
    state_ = metadata::RUNNING;
  }
  return Status::OK();
}

Status TabletPeer::Shutdown() {
  if (consensus_) {
    Status s = consensus_->Shutdown();
    if (!s.ok()) {
      LOG(WARNING) << "Consensus shutdown failed: " << s.ToString();
    }
  }
  prepare_executor_->Shutdown();
  apply_executor_->Shutdown();
  VLOG(1) << "TablePeer: " << tablet_->metadata()->oid() << " Shutdown!";
  return Status::OK();
}

Status TabletPeer::SubmitWrite(WriteTransactionContext *tx_ctx) {
  // TODO keep track of the transaction somewhere so that we can cancel transactions
  // when we change leaders and/or want to quiesce a tablet.
  LeaderWriteTransaction* transaction = new LeaderWriteTransaction(tx_ctx,
                                                                   consensus_.get(),
                                                                   prepare_executor_.get(),
                                                                   apply_executor_.get(),
                                                                   prepare_replicate_lock_);
  // transaction deletes itself on delete/abort
  return transaction->Execute();
}

Status TabletPeer::SubmitAlterSchema(AlterSchemaTransactionContext *tx_ctx) {
  // TODO keep track of the transaction somewhere so that we can cancel transactions
  // when we change leaders and/or want to quiesce a tablet.
  LeaderAlterSchemaTransaction* transaction =
    new LeaderAlterSchemaTransaction(tx_ctx, consensus_.get(),
                                     prepare_executor_.get(),
                                     apply_executor_.get(),
                                     prepare_replicate_lock_);
  // transaction deletes itself on delete/abort
  return transaction->Execute();
}

Status TabletPeer::SubmitChangeConfig(ChangeConfigTransactionContext *tx_ctx) {
  // TODO keep track of the transaction somewhere so that we can cancel transactions
  // when we change leaders and/or want to quiesce a tablet.
  LeaderChangeConfigTransaction* transaction =
      new LeaderChangeConfigTransaction(tx_ctx,
                                        consensus_.get(),
                                        prepare_executor_.get(),
                                        apply_executor_.get(),
                                        prepare_replicate_lock_,
                                        &config_lock_);
  // transaction deletes itself on delete/abort
  return transaction->Execute();
}

}  // namespace tablet
}  // namespace kudu
