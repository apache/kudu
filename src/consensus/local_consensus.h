// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CONSENSUS_LOCAL_CONSENSUS_H_
#define KUDU_CONSENSUS_LOCAL_CONSENSUS_H_

#include <vector>

#include "consensus/consensus_base.h"

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
class LocalConsensus : public ConsensusBase {
 public:
  explicit LocalConsensus(const ConsensusOptions& options);

  virtual Status Init(const metadata::QuorumPeerPB& peer,
                      const scoped_refptr<server::Clock>& clock,
                      log::Log* log);

  virtual Status Start(const metadata::QuorumPB& initial_quorum,
                       gscoped_ptr<metadata::QuorumPB>* running_quorum);

  virtual Status Append(gscoped_ptr<ReplicateMsg> entry,
                        const std::tr1::shared_ptr<FutureCallback>& repl_callback,
                        const std::tr1::shared_ptr<FutureCallback>& commit_callback,
                        OpId* op_id,
                        gscoped_ptr<ConsensusContext>* context) OVERRIDE;

  Status Update(ReplicaUpdateContext* context);

  Status Commit(ConsensusContext* context, OperationPB* commit_op);

  Status Shutdown();

  uint8_t n_majority() const {
    return 1;
  }

  uint8_t num_participants() const {
    return 1;
  }

  bool is_leader() const {
    return true;
  }

  metadata::QuorumPeerPB CurrentLeader() const {
    return peer_;
  }

  metadata::QuorumPB CurrentQuorum() const {
    return quorum_;
  }

 private:
  metadata::QuorumPeerPB peer_;
  metadata::QuorumPB quorum_;
  int64 next_op_id_index_;

  scoped_refptr<server::Clock> clock_;

  // lock serializes the commit id generation and subsequent
  // task (log) submission as well as replicate id generation
  // and subsequent task submission.
  mutable simple_spinlock lock_;

};

} // namespace consensus
} // namespace kudu

#endif /* KUDU_CONSENSUS_LOCAL_CONSENSUS_H_ */
