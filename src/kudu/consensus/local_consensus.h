// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CONSENSUS_LOCAL_CONSENSUS_H_
#define KUDU_CONSENSUS_LOCAL_CONSENSUS_H_

#include <boost/thread/locks.hpp>
#include <string>
#include <vector>

#include "kudu/consensus/consensus.h"
#include "kudu/consensus/consensus_meta.h"

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

  virtual Status Start(const metadata::QuorumPB& initial_quorum,
                       const OpId& last_committed_op_id) OVERRIDE;

  virtual Status Replicate(ConsensusRound* context) OVERRIDE;

  virtual metadata::QuorumPeerPB::Role role() const OVERRIDE;

  virtual std::string peer_uuid() const OVERRIDE {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return peer_uuid_;
  }

  virtual metadata::QuorumPB Quorum() const OVERRIDE;

  virtual void Shutdown() OVERRIDE;

  virtual void DumpStatusHtml(std::ostream& out) const OVERRIDE;

  //
  //  NOT IMPLEMENTED IN LOCAL CONSENSUS
  //
  virtual Status Update(const ConsensusRequestPB* request,
                        ConsensusResponsePB* response) OVERRIDE;

  virtual Status RequestVote(const VoteRequestPB* request,
                             VoteResponsePB* response) OVERRIDE;

 protected:
  virtual Status Commit(ConsensusRound* context) OVERRIDE;

  virtual Status PersistQuorum(const metadata::QuorumPB& quorum) OVERRIDE;

 private:

  const std::string peer_uuid_;
  metadata::QuorumPB quorum_;

  const ConsensusOptions options_;
  const gscoped_ptr<ConsensusMetadata> cmeta_;

  int64 next_op_id_index_;
  // lock serializes the commit id generation and subsequent
  // task (log) submission as well as replicate id generation
  // and subsequent task submission.
  mutable simple_spinlock op_id_lock_;

  State state_;
  ReplicaTransactionFactory* txn_factory_;
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
