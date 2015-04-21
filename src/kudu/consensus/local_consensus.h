// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CONSENSUS_LOCAL_CONSENSUS_H_
#define KUDU_CONSENSUS_LOCAL_CONSENSUS_H_

#include <boost/thread/locks.hpp>
#include <string>
#include <vector>

#include "kudu/consensus/consensus.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/util/locks.h"

namespace kudu {

class FsManager;
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
  explicit LocalConsensus(const ConsensusOptions& options,
                          gscoped_ptr<ConsensusMetadata> cmeta,
                          const std::string& peer_uuid,
                          const scoped_refptr<server::Clock>& clock,
                          ReplicaTransactionFactory* txn_factory,
                          log::Log* log);

  virtual Status Start(const ConsensusBootstrapInfo& info) OVERRIDE;

  virtual Status EmulateElection() OVERRIDE { return Status::OK(); }

  virtual Status StartElection() OVERRIDE { return Status::OK(); }

  virtual Status Replicate(ConsensusRound* context) OVERRIDE;

  virtual QuorumPeerPB::Role role() const OVERRIDE;

  virtual std::string peer_uuid() const OVERRIDE {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return peer_uuid_;
  }

  virtual std::string tablet_id() const OVERRIDE {
    return options_.tablet_id;
  }

  virtual QuorumPB Quorum() const OVERRIDE;

  virtual void Shutdown() OVERRIDE;

  virtual void DumpStatusHtml(std::ostream& out) const OVERRIDE;

  //
  //  NOT IMPLEMENTED IN LOCAL CONSENSUS
  //
  virtual Status Update(const ConsensusRequestPB* request,
                        ConsensusResponsePB* response) OVERRIDE;

  virtual Status RequestVote(const VoteRequestPB* request,
                             VoteResponsePB* response) OVERRIDE;
 private:

  const std::string peer_uuid_;
  QuorumPB quorum_;

  const ConsensusOptions options_;
  const gscoped_ptr<ConsensusMetadata> cmeta_;

  // Protects 'next_op_id_index_', 'state_', etc.
  mutable simple_spinlock lock_;

  // Protected by lock_
  int64 next_op_id_index_;

  State state_;
  ReplicaTransactionFactory* txn_factory_;
  log::Log* log_;
  scoped_refptr<server::Clock> clock_;

};

} // namespace consensus
} // namespace kudu

#endif /* KUDU_CONSENSUS_LOCAL_CONSENSUS_H_ */
