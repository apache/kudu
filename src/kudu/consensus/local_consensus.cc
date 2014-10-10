// Copyright (c) 2013, Cloudera, inc.

#include "kudu/consensus/local_consensus.h"

#include <boost/thread/locks.hpp>
#include <boost/assign/list_of.hpp>
#include <iostream>

#include "kudu/consensus/log.h"
#include "kudu/server/metadata.h"
#include "kudu/server/clock.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace consensus {

using base::subtle::Barrier_AtomicIncrement;
using consensus::CHANGE_CONFIG_OP;
using log::Log;
using log::LogEntryBatch;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using std::tr1::shared_ptr;

LocalConsensus::LocalConsensus(const ConsensusOptions& options,
                               gscoped_ptr<ConsensusMetadata> cmeta,
                               const string& peer_uuid,
                               const scoped_refptr<server::Clock>& clock,
                               ReplicaTransactionFactory* txn_factory,
                               Log* log)
    : peer_uuid_(peer_uuid),
      options_(options),
      cmeta_(cmeta.Pass()),
      next_op_id_index_(-1),
      state_(kInitializing),
      txn_factory_(DCHECK_NOTNULL(txn_factory)),
      log_(DCHECK_NOTNULL(log)) {
  CHECK(cmeta_) << "Passed ConsensusMetadata object is NULL";
}

Status LocalConsensus::Start(const ConsensusBootstrapInfo& info) {
  CHECK_EQ(state_, kInitializing);

  boost::lock_guard<simple_spinlock> lock(lock_);

  const QuorumPB& initial_quorum = cmeta_->pb().committed_quorum();
  CHECK(initial_quorum.local()) << "Local consensus must be passed a local quorum";
  RETURN_NOT_OK_PREPEND(VerifyQuorum(initial_quorum),
                        "Invalid quorum found in LocalConsensus::Start()");

  next_op_id_index_ = info.last_id.index() + 1;

  gscoped_ptr<QuorumPB> new_quorum(new QuorumPB);
  new_quorum->CopyFrom(initial_quorum);
  new_quorum->mutable_peers(0)->set_role(QuorumPeerPB::LEADER);
  new_quorum->set_seqno(initial_quorum.seqno() + 1);

  state_ = kRunning;

  NullCallback* null_clbk = new NullCallback;

  // Initiate a config change transaction, as in the distributed case.
  RETURN_NOT_OK(txn_factory_->SubmitConsensusChangeConfig(
      new_quorum.Pass(),
      null_clbk->AsStatusCallback()));

  TRACE("Consensus started");
  return Status::OK();
}

Status LocalConsensus::Replicate(ConsensusRound* context) {
  DCHECK_GE(state_, kConfiguring);

  consensus::ReplicateMsg* msg = context->replicate_msg();

  OpId* cur_op_id = DCHECK_NOTNULL(msg)->mutable_id();
  cur_op_id->set_term(0);

  // Pre-cache the ByteSize outside of the lock, since this is somewhat
  // expensive.
  ignore_result(msg->ByteSize());

  LogEntryBatch* reserved_entry_batch;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);

    // create the new op id for the entry.
    cur_op_id->set_index(next_op_id_index_++);
    // Reserve the correct slot in the log for the replication operation.
    // It's important that we do this under the same lock as we generate
    // the op id, so that we log things in-order.
    gscoped_ptr<log::LogEntryBatchPB> entry_batch;
    log::CreateBatchFromAllocatedOperations(&msg, 1, &entry_batch);

    RETURN_NOT_OK(log_->Reserve(entry_batch.Pass(), &reserved_entry_batch));
  }
  // Serialize and mark the message as ready to be appended.
  // When the Log actually fsync()s this message to disk, 'repl_callback'
  // is triggered.
  RETURN_NOT_OK(log_->AsyncAppend(reserved_entry_batch,
                                  context->replicate_callback()->AsStatusCallback()));
  return Status::OK();
}

metadata::QuorumPeerPB::Role LocalConsensus::role() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return cmeta_->pb().committed_quorum().peers().begin()->role();
}

Status LocalConsensus::Update(const ConsensusRequestPB* request,
                              ConsensusResponsePB* response) {
  return Status::NotSupported("LocalConsensus does not support Update() calls.");
}

Status LocalConsensus::RequestVote(const VoteRequestPB* request,
                                   VoteResponsePB* response) {
  return Status::NotSupported("LocalConsensus does not support RequestVote() calls.");
}

Status LocalConsensus::Commit(gscoped_ptr<CommitMsg> commit,
                              const StatusCallback& cb) {
  return log_->AsyncAppendCommit(commit.Pass(), cb);
}

metadata::QuorumPB LocalConsensus::Quorum() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return cmeta_->pb().committed_quorum();
}

Status LocalConsensus::PersistQuorum(const QuorumPB& quorum) {
  RETURN_NOT_OK_PREPEND(VerifyQuorum(quorum),
                        "Invalid quorum passed to LocalConsensus::PersistQuorum()");
  TRACE(strings::Substitute("Persisting new quorum with seqno $0", quorum.seqno()));
  boost::lock_guard<simple_spinlock> lock(lock_);
  CHECK_LT(cmeta_->pb().committed_quorum().seqno(), quorum.seqno())
    << "Quorum seqnos not monotonic: "
    << "old quorum: " << cmeta_->pb().committed_quorum().ShortDebugString() << "; "
    << "new quorum: " << quorum.ShortDebugString();

  cmeta_->mutable_pb()->mutable_committed_quorum()->CopyFrom(quorum);
  return cmeta_->Flush();
}

void LocalConsensus::Shutdown() {
  VLOG(1) << "LocalConsensus Shutdown!";
}

void LocalConsensus::DumpStatusHtml(std::ostream& out) const {
  out << "<h1>Local Consensus Status</h1>\n";

  boost::lock_guard<simple_spinlock> lock(lock_);
  out << "next op: " << next_op_id_index_;
}

} // end namespace consensus
} // end namespace kudu
