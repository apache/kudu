// Copyright (c) 2013, Cloudera, inc.

#include "tablet/tablet_peer.h"

#include "consensus/local_consensus.h"
#include "gutil/strings/substitute.h"
#include "tablet/transactions/write_transaction.h"
#include "tablet/tablet_metrics.h"
#include "util/metrics.h"
#include "util/trace.h"

namespace kudu {
namespace tablet {

using base::subtle::Barrier_AtomicIncrement;
using consensus::CommitMsg;
using consensus::ConsensusOptions;
using consensus::ConsensusContext;
using consensus::LocalConsensus;
using consensus::OpId;
using consensus::ReplicateMsg;
using consensus::OP_ABORT;
using consensus::WRITE_OP;
using log::Log;
using log::LogOptions;
using tserver::TabletServerErrorPB;

// ============================================================================
//  Tablet Peer
// ============================================================================
TabletPeer::TabletPeer(const shared_ptr<Tablet>& tablet,
                       gscoped_ptr<Log> log)
    : tablet_(tablet),
      log_(log.Pass()),
      // prepare executor has a single thread as prepare must be done in order
      // of submission
      prepare_executor_(TaskExecutor::CreateNew(1)) {
  DCHECK(tablet_) << "A TabletPeer must be provided with a Tablet";
  DCHECK(log_) << "A TabletPeer must be provided with a Log";

  errno = 0;
  int n_cpus = sysconf(_SC_NPROCESSORS_CONF);
  CHECK_EQ(errno, 0) << ErrnoToString(errno);
  CHECK_GT(n_cpus, 0);
  apply_executor_.reset(TaskExecutor::CreateNew(n_cpus));
}

// TODO a distributed implementation of consensus will need to receive the
// configuration before Init().
Status TabletPeer::Init() {

  // TODO support different consensus implementations (possibly by adding
  // a TabletPeerOptions).
  consensus_.reset(new LocalConsensus(ConsensusOptions(), log_.get()));

  // set consensus on the tablet to that it can store local state changes
  // in the log.
  tablet_->SetConsensus(consensus_.get());
  return Status::OK();
}

Status TabletPeer::Start() {
  // just return OK since we're only using LocalConsensus.
  return Status::OK();
}

Status TabletPeer::Shutdown() {
  Status s = consensus_->Shutdown();
  if (!s.ok()) {
    LOG(WARNING) << "Consensus shutdown failed: " << s.ToString();
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

}  // namespace tablet
}  // namespace kudu
