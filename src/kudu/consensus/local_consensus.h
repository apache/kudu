// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CONSENSUS_LOCAL_CONSENSUS_H_
#define KUDU_CONSENSUS_LOCAL_CONSENSUS_H_

#include <boost/thread/locks.hpp>
#include <string>
#include <vector>

#include "kudu/consensus/consensus.h"

namespace kudu {

class FutureCallback;

namespace metadata {
class TabletServerPB;
}

namespace consensus {

// Local implementation of Consensus. This is mostly for testing purposes/
// using in single node quorums if/when applicable.
//
// NOTE: While this implementation has a lot less overhead running on a single
// node than a true consensus implementation in the same situation, this
// implementation will not be able to be reconfigured to accept more nodes
// while a true consensus implementation will.
//
// This class is not thread safe.
class LocalConsensus : public Consensus {
 public:
  explicit LocalConsensus(const ConsensusOptions& options);

  virtual Status Init(const metadata::QuorumPeerPB& peer,
                      const scoped_refptr<server::Clock>& clock,
                      ReplicaTransactionFactory* txn_factory,
                      log::Log* log) OVERRIDE;

  virtual Status Start(const metadata::QuorumPB& initial_quorum,
                       const ConsensusBootstrapInfo& bootstrap_info,
                       gscoped_ptr<metadata::QuorumPB>* running_quorum) OVERRIDE;

  virtual Status Replicate(ConsensusRound* context) OVERRIDE;

  metadata::QuorumPeerPB::Role role() const OVERRIDE {
    return metadata::QuorumPeerPB::LEADER;
  }

  virtual std::string peer_uuid() const OVERRIDE {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return peer_.permanent_uuid();
  }

  metadata::QuorumPB Quorum() const OVERRIDE {
    return quorum_;
  }

  virtual void Shutdown() OVERRIDE;

  virtual void DumpStatusHtml(std::ostream& out) const OVERRIDE;

  //
  //  NOT IMPLEMENTED IN LOCAL CONSENSUS
  //
  virtual Status Update(const ConsensusRequestPB* request,
                        ConsensusResponsePB* response) OVERRIDE;

  virtual Status RequestVote(const VoteRequestPB* request,
                             VoteResponsePB* response) OVERRIDE;

  metadata::QuorumPB CurrentQuorum() const {
    return quorum_;
  }

  virtual Status Commit(ConsensusRound* context) OVERRIDE;

 private:

  metadata::QuorumPeerPB peer_;
  metadata::QuorumPB quorum_;

  const ConsensusOptions options_;

  int64 next_op_id_index_;
  // lock serializes the commit id generation and subsequent
  // task (log) submission as well as replicate id generation
  // and subsequent task submission.
  mutable simple_spinlock op_id_lock_;

  State state_;
  log::Log* log_;
  scoped_refptr<server::Clock> clock_;

  // lock serializes the commit id generation and subsequent
  // task (log) submission as well as replicate id generation
  // and subsequent task submission.
  mutable simple_spinlock lock_;

};

} // namespace consensus
} // namespace kudu

#endif /* KUDU_CONSENSUS_LOCAL_CONSENSUS_H_ */
