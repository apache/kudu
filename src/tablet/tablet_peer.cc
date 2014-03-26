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
#include "tablet/tablet_bootstrap.h"
#include "tablet/tablet.pb.h"
#include "util/metrics.h"
#include "util/stopwatch.h"
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
TabletPeer::TabletPeer(const TabletMetadata& meta)
  : status_listener_(new TabletStatusListener(meta)),
    // prepare executor has a single thread as prepare must be done in order
    // of submission
    prepare_executor_(TaskExecutor::CreateNew("prepare exec", 1)),
    config_sem_(1) {
  apply_executor_.reset(TaskExecutor::CreateNew("apply exec", base::NumCPUs()));
  state_ = metadata::BOOTSTRAPPING;
}

TabletPeer::~TabletPeer() {
}

Status TabletPeer::Init(const shared_ptr<Tablet>& tablet,
                        const scoped_refptr<server::Clock>& clock,
                        const QuorumPeerPB& quorum_peer,
                        gscoped_ptr<Log> log,
                        OpIdAnchorRegistry* opid_anchor_registry) {

  {
    boost::lock_guard<simple_spinlock> lock(internal_state_lock_);
    state_ = metadata::CONFIGURING;
    tablet_ = tablet;
    clock_ = clock;
    quorum_peer_ = quorum_peer;
    log_.reset(log.release());
    opid_anchor_registry_ = opid_anchor_registry;
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
  boost::lock_guard<Semaphore> config_lock(config_sem_);

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
  // TODO: KUDU-183: Keep track of the pending tasks and send an "abort" message.
  LOG_SLOW_EXECUTION(WARNING, 1000,
      strings::Substitute("TabletPeer: tablet $0: Waiting for Transactions to complete",
                          tablet_ != NULL ? tablet_->tablet_id() : "")) {
    txn_tracker_.WaitForAllToFinish();
  }

  if (consensus_) {
    Status s = consensus_->Shutdown();
    if (!s.ok()) {
      LOG(WARNING) << "Consensus shutdown failed: " << s.ToString();
    }
  }
  prepare_executor_->Shutdown();
  apply_executor_->Shutdown();
  if (VLOG_IS_ON(1)) {
    if (tablet_) {
      VLOG(1) << "TabletPeer: " << tablet_->metadata()->oid() << " Shutdown!";
    }
  }
  return Status::OK();
}

Status TabletPeer::SubmitWrite(WriteTransactionContext *tx_ctx) {
  // TODO keep track of the transaction somewhere so that we can cancel transactions
  // when we change leaders and/or want to quiesce a tablet.
  LeaderWriteTransaction* transaction = new LeaderWriteTransaction(&txn_tracker_, tx_ctx,
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
    new LeaderAlterSchemaTransaction(&txn_tracker_, tx_ctx, consensus_.get(),
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
      new LeaderChangeConfigTransaction(&txn_tracker_, tx_ctx,
                                        consensus_.get(),
                                        prepare_executor_.get(),
                                        apply_executor_.get(),
                                        prepare_replicate_lock_,
                                        &config_sem_);
  // transaction deletes itself on delete/abort
  return transaction->Execute();
}

void TabletPeer::GetTabletStatusPB(TabletStatusPB* status_pb_out) const {
  boost::lock_guard<simple_spinlock> lock(internal_state_lock_);
  DCHECK(status_pb_out != NULL);
  DCHECK(status_listener_.get() != NULL);
  status_pb_out->set_tablet_id(status_listener_->tablet_id());
  status_pb_out->set_table_name(status_listener_->table_name());
  status_pb_out->set_last_status(status_listener_->last_status());
  status_pb_out->set_start_key(status_listener_->start_key());
  status_pb_out->set_end_key(status_listener_->end_key());
  status_pb_out->set_state(state_);
  if (tablet() != NULL) {
    status_pb_out->set_estimated_on_disk_size(tablet()->EstimateOnDiskSize());
  }
}


}  // namespace tablet
}  // namespace kudu
