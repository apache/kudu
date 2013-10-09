// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CONSENSUS_LOCAL_CONSENSUS_H_
#define KUDU_CONSENSUS_LOCAL_CONSENSUS_H_

#include "consensus/log.h"
#include "consensus/consensus.h"
#include "consensus/consensus.pb.h"

namespace kudu {

class TaskExecutor;
class FutureCallback;

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
  LocalConsensus(const ConsensusOptions& options, log::Log* log);

  Status Start();

  Status Append(gscoped_ptr<ReplicateMsg> entry,
                const std::tr1::shared_ptr<FutureCallback>& repl_callback,
                const std::tr1::shared_ptr<FutureCallback>& commit_callback,
                gscoped_ptr<ConsensusContext>* context);

  Status LocalCommit(CommitMsg* commit_msg,
                     std::tr1::shared_ptr<kudu::Future>* commit_future);

  Status Commit(ConsensusContext* context, CommitMsg *commit);

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

  const QuorumPeerPB &current_leader() const {
    return peer_;
  }

 private:
  log::Log* log_;
  QuorumPeerPB peer_;
  gscoped_ptr<TaskExecutor> log_executor_;
  gscoped_ptr<TaskExecutor> commit_executor_;
  base::subtle::Atomic64 next_op_id_;

};

} // namespace consensus
} // namespace kudu

#endif /* KUDU_CONSENSUS_LOCAL_CONSENSUS_H_ */
