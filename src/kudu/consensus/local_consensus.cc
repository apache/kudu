// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/consensus/local_consensus.h"

#include <boost/thread/locks.hpp>
#include <boost/assign/list_of.hpp>
#include <iostream>

#include "kudu/consensus/log.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/server/metadata.h"
#include "kudu/server/clock.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace consensus {

using base::subtle::Barrier_AtomicIncrement;
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
      log_(DCHECK_NOTNULL(log)),
      clock_(clock) {
  CHECK(cmeta_) << "Passed ConsensusMetadata object is NULL";
}

Status LocalConsensus::Start(const ConsensusBootstrapInfo& info) {
  CHECK_EQ(state_, kInitializing);

  CHECK(info.orphaned_replicates.empty())
      << "LocalConsensus does not handle orphaned operations on start.";

  gscoped_ptr<ConsensusRound> round;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);

    const QuorumPB& initial_quorum = cmeta_->pb().committed_quorum();
    CHECK(initial_quorum.local()) << "Local consensus must be passed a local quorum";
    RETURN_NOT_OK_PREPEND(VerifyQuorum(initial_quorum, COMMITTED_QUORUM),
                          "Invalid quorum found in LocalConsensus::Start()");

    next_op_id_index_ = info.last_id.index() + 1;

    gscoped_ptr<QuorumPB> new_quorum(new QuorumPB);
    new_quorum->CopyFrom(initial_quorum);
    new_quorum->clear_opid_index();
    new_quorum->mutable_peers(0)->set_role(QuorumPeerPB::LEADER);

    ReplicateMsg* replicate = new ReplicateMsg;
    replicate->set_op_type(CHANGE_CONFIG_OP);
    ChangeConfigRequestPB* cc_req = replicate->mutable_change_config_request();
    cc_req->set_tablet_id(options_.tablet_id);
    cc_req->mutable_old_config()->CopyFrom(initial_quorum);
    cc_req->mutable_new_config()->CopyFrom(*new_quorum);

    replicate->mutable_id()->set_term(0);
    replicate->mutable_id()->set_index(next_op_id_index_);
    replicate->set_timestamp(clock_->Now().ToUint64());

    round.reset(new ConsensusRound(this, make_scoped_refptr_replicate(replicate)));
    state_ = kRunning;
  }

  ConsensusRound* round_ptr = round.get();
  RETURN_NOT_OK(txn_factory_->StartReplicaTransaction(round.Pass()));
  Status s = Replicate(round_ptr);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to replicate initial change config transaction: " << s.ToString();
    return s;
  }

  TRACE("Consensus started");
  return Status::OK();
}

Status LocalConsensus::Replicate(ConsensusRound* round) {
  DCHECK_GE(state_, kConfiguring);

  consensus::ReplicateMsg* msg = round->replicate_msg();

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
    log::CreateBatchFromAllocatedOperations(
        boost::assign::list_of(round->replicate_scoped_refptr()), &entry_batch);

    RETURN_NOT_OK(log_->Reserve(log::REPLICATE, entry_batch.Pass(),
                                &reserved_entry_batch));

    // Local consensus transactions are always committed so we
    // can just persist the quorum, if this is a change config.
    if (round->replicate_msg()->op_type() == CHANGE_CONFIG_OP) {
      QuorumPB new_quorum = round->replicate_msg()->change_config_request().new_config();
      DCHECK(!new_quorum.has_opid_index());
      new_quorum.set_opid_index(round->replicate_msg()->id().index());
      cmeta_->mutable_pb()->mutable_committed_quorum()->CopyFrom(new_quorum);
      CHECK_OK(cmeta_->Flush());
    }
  }
  // Serialize and mark the message as ready to be appended.
  // When the Log actually fsync()s this message to disk, 'repl_callback'
  // is triggered.
  RETURN_NOT_OK(log_->AsyncAppend(
      reserved_entry_batch,
      Bind(&ConsensusRound::NotifyReplicationFinished, Unretained(round))));
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

metadata::QuorumPB LocalConsensus::Quorum() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return cmeta_->pb().committed_quorum();
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
