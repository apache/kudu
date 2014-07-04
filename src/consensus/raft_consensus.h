// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_RAFT_CONSENSUS_H_
#define KUDU_CONSENSUS_RAFT_CONSENSUS_H_

#include <string>
#include <utility>
#include <vector>

#include "consensus/log.h"
#include "consensus/consensus.h"
#include "consensus/consensus.pb.h"
#include "consensus/consensus_peers.h"
#include "consensus/consensus_queue.h"
#include "consensus/raft_consensus_state.h"
#include "server/clock.h"
#include "tserver/tserver_service.proxy.h"
#include "util/blocking_queue.h"
#include "util/net/net_util.h"

namespace kudu {

typedef boost::lock_guard<simple_spinlock> Lock;
typedef gscoped_ptr<Lock> ScopedLock;

class TaskExecutor;
class FutureCallback;
class HostPort;
class ReplicaState;

namespace rpc {
class Messenger;
}

namespace consensus {
class ReplicaState;

class RaftConsensus : public Consensus {
 public:
  class ConsensusFaultHooks;

  typedef std::tr1::unordered_map<std::string, Peer*> PeersMap;

  RaftConsensus(const ConsensusOptions& options,
                gscoped_ptr<PeerProxyFactory> peer_proxy_factory,
                const MetricContext& metric_ctx);

  virtual Status Init(const metadata::QuorumPeerPB& peer,
                      const scoped_refptr<server::Clock>& clock,
                      ReplicaTransactionFactory* txn_factory,
                      log::Log* log) OVERRIDE;

  virtual Status Start(const metadata::QuorumPB& initial_quorum,
                       const ConsensusBootstrapInfo& bootstrap_info,
                       gscoped_ptr<metadata::QuorumPB>* running_quorum) OVERRIDE;

  virtual Status Replicate(ConsensusRound* context) OVERRIDE;

  virtual Status Update(const ConsensusRequestPB* request,
                        ConsensusResponsePB* response) OVERRIDE;

  virtual Status RequestVote(const VoteRequestPB* request,
                             VoteResponsePB* response) OVERRIDE;

  virtual log::Log* log() { return log_; }

  virtual metadata::QuorumPeerPB::Role role() const OVERRIDE;

  virtual string peer_uuid() const OVERRIDE;

  virtual metadata::QuorumPB Quorum() const OVERRIDE;

  virtual void Shutdown() OVERRIDE;

  // Returns the replica state for tests. This should never be used outside of
  // tests, in particular calling the LockFor* methods on the returned object
  // can cause consensus to deadlock.
  ReplicaState* GetReplicaStateForTests();

  // Signals all peers of the current quorum that there is a new request pending.
  void SignalRequestToPeers(bool force_if_queue_empty = false);

  // Registers a callback that will be triggered when the operation with 'op_id'
  // is replicated.
  Status RegisterOnReplicateCallback(
      const OpId& op_id,
      const std::tr1::shared_ptr<FutureCallback>& repl_callback);

  // Registers a callback that will be triggered when the operation with 'op_id'
  // is considered committed.
  // NOTE: 'op_id' is the id of the operation to commit, not the id of the commit
  // itself.
  Status RegisterOnCommitCallback(
      const OpId& op_id,
      const std::tr1::shared_ptr<FutureCallback>& commit_callback);

  virtual ~RaftConsensus();

 protected:
  virtual Status Commit(ConsensusRound* context) OVERRIDE;

 private:
  friend class ReplicaState;
  friend class RaftConsensusTest;
  FRIEND_TEST(RaftConsensusTest, TestReplicasHandleCommunicationErrors);

  Status ChangeConfig(metadata::QuorumPB new_config);

  void ClosePeers();

  // Updates the state in a replica by storing the received operations in the log
  // and triggering the required transactions. This method won't return until all
  // operations have been stored in the log and all Prepares() have been completed,
  // and a replica cannot accept any more Update() requests until this is done.
  Status UpdateReplica(const ConsensusRequestPB* request,
                       ConsensusStatusPB* status);

  // A leader commit, which appends to the message queue. Must be called
  // after LockForCommit().
  Status LeaderCommitUnlocked(ConsensusRound* context, OperationPB* commit_op);

  // A replica commit, which just stores in the local log. Must be called
  // after LockForCommit().
  Status ReplicaCommitUnlocked(ConsensusRound* context, OperationPB* commit_op);

  // Updates 'peers_' according to the new quorum config.
  Status CreateOrUpdatePeersUnlocked();

  // Pushes a new quorum configuration to a majority of peers. Contrary to write operations,
  // this actually waits for the commit round to reach a majority of peers, so that we know
  // we can proceed. If this returns Status::OK(), a majority of peers have accepted the new
  // configuration. The peer cannot perform any additional operations until this succeeds.
  Status PushConfigurationToPeersUnlocked();

  OperationStatusTracker* CreateLeaderOnlyOperationStatusUnlocked(
      gscoped_ptr<OperationPB> operation,
      const std::tr1::shared_ptr<FutureCallback>& commit_callback);

  OperationStatusTracker* CreateLeaderOnlyOperationStatusUnlocked(
      gscoped_ptr<OperationPB> operation);

  OpId GetLastOpIdFromLog();

  log::Log* log_;
  scoped_refptr<server::Clock> clock_;
  gscoped_ptr<PeerProxyFactory> peer_proxy_factory_;
  // The peers in the consensus quorum.
  PeersMap peers_;
  // The queue of messages that must be sent to peers.
  PeerMessageQueue queue_;
  gscoped_ptr<ThreadPool> callback_pool_;

  gscoped_ptr<ReplicaState> state_;

  // TODO hack to serialize updates due to repeated/out-of-order messages
  // should probably be refactored out.
  mutable simple_spinlock update_lock_;

  DISALLOW_COPY_AND_ASSIGN(RaftConsensus);
};

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_RAFT_CONSENSUS_H_ */
