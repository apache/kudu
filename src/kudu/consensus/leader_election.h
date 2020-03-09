// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef KUDU_CONSENSUS_LEADER_ELECTION_H
#define KUDU_CONSENSUS_LEADER_ELECTION_H

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <unordered_map>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {

// The vote a peer has given.
enum ElectionVote {
  VOTE_DENIED = 0,
  VOTE_GRANTED = 1,
};

// Simple class to count votes (in-memory, not persisted to disk).
// This class is not thread safe and requires external synchronization.
class VoteCounter {
 public:
  // Create new VoteCounter with the given majority size.
  VoteCounter(int num_voters, int majority_size);
  virtual ~VoteCounter() {}

  // Register a peer's vote.
  //
  // If the voter already has a vote recorded, but it has a different value than
  // the vote specified, returns Status::IllegalArgument.
  //
  // If the same vote is duplicated, 'is_duplicate' is set to true.
  // Otherwise, it is set to false.
  // If an OK status is not returned, the value in 'is_duplicate' is undefined.
  virtual Status RegisterVote(
      const std::string& voter_uuid, ElectionVote vote, bool* is_duplicate);

  // Return whether the vote is decided yet.
  virtual bool IsDecided() const;

  // Return decision iff IsDecided() returns true.
  // If vote is not yet decided, returns Status::IllegalState().
  virtual Status GetDecision(ElectionVote* decision) const;

  // Return the total of "Yes" and "No" votes.
  int GetTotalVotesCounted() const;

  // Return total number of expected votes.
  int GetTotalExpectedVotes() const { return num_voters_; }

  // Return true iff GetTotalVotesCounted() == num_voters_;
  bool AreAllVotesIn() const;

 protected:
  int num_voters_;

  typedef std::map<std::string, ElectionVote> VoteMap;
  VoteMap votes_; // Voting record.

 private:
  friend class VoteCounterTest;

  const int majority_size_;
  int yes_votes_; // Accumulated yes votes, for quick counting.
  int no_votes_;  // Accumulated no votes.

  DISALLOW_COPY_AND_ASSIGN(VoteCounter);
};

class FlexibleVoteCounter : public VoteCounter {
 public:
  FlexibleVoteCounter(RaftConfigPB config);

  // Synchronization is done by the LeaderElection class. Therefore, VoteCounter
  // class doesn't need to take care of thread safety of its book-keeping
  // variables.
  Status RegisterVote(
      const std::string& voter_uuid, ElectionVote vote,
      bool* is_duplicate) override;
  bool IsDecided() const override;
  Status GetDecision(ElectionVote* decision) const override;
 private:
  // Returns a couple of booleans - the first denotes if the election quorum
  // has been satisfied and the second denotes if it can still be satisfied.
  std::pair<bool, bool> GetQuorumState() const;

  // Fetches topology information required by the flexible vote counter.
  void FetchTopologyInfo(
      std::map<std::string, int>* voter_distribution,
      std::map<std::string, int>* le_quorum_requirement);

  // Mapping from each region to number of active voters.
  std::map<std::string, int> voter_distribution_;

  // Mapping from each region to number of yes votes required.
  std::map<std::string, int> le_quorum_requirement_;

  // Vote count per region.
  std::map<std::string, int> yes_vote_count_, no_vote_count_;

  // Config at the beginning of the leader election.
  const RaftConfigPB config_;

  // UUID to region map derived from RaftConfigPB
  std::map<std::string, std::string> uuid_to_region_;

  DISALLOW_COPY_AND_ASSIGN(FlexibleVoteCounter);
};

// The result of a leader election.
struct ElectionResult {
 public:
  ElectionResult(VoteRequestPB vote_request, ElectionVote decision,
                 ConsensusTerm highest_term, const std::string& message);

  // The vote request that was sent to the voters for this election.
  const VoteRequestPB vote_request;

  // The overall election GRANTED/DENIED decision of the configuration.
  const ElectionVote decision;

  // The highest term seen from any voter.
  const ConsensusTerm highest_voter_term;

  // Human-readable explanation of the vote result, if any.
  const std::string message;
};

// Driver class to run a leader election.
//
// The caller must pass a callback to the driver, which will be called exactly
// once when a Yes/No decision has been made, except in case of Shutdown()
// on the Messenger or test ThreadPool, in which case no guarantee of a
// callback is provided. In that case, we should not care about the election
// result, because the server is ostensibly shutting down.
//
// For a "Yes" decision, a majority of voters must grant their vote.
//
// A "No" decision may be caused by either one of the following:
// - One of the peers replies with a higher term before a decision is made.
// - A majority of the peers votes "No".
//
// Any votes that come in after a decision has been made and the callback has
// been invoked are logged but ignored. Note that this somewhat strays from the
// letter of the Raft paper, in that replies that come after a "Yes" decision
// do not immediately cause the candidate/leader to step down, but this keeps
// our implementation and API simple, and the newly-minted leader will soon
// discover that it must step down when it attempts to replicate its first
// message to the peers.
//
// This class is thread-safe.
class LeaderElection : public RefCountedThreadSafe<LeaderElection> {
 public:
  typedef std::function<void(const ElectionResult&)> ElectionDecisionCallback;

  // Set up a new leader election driver.
  //
  // 'proxy_factory' must not go out of scope while LeaderElection is alive.
  //
  // The 'vote_counter' must be initialized with the candidate's own yes vote.
  LeaderElection(RaftConfigPB config,
                 PeerProxyFactory* proxy_factory,
                 VoteRequestPB request,
                 gscoped_ptr<VoteCounter> vote_counter,
                 MonoDelta timeout,
                 ElectionDecisionCallback decision_callback);

  // Run the election: send the vote request to followers.
  void Run();

 private:
  friend class RefCountedThreadSafe<LeaderElection>;

  struct VoterState {
    std::string peer_uuid;
    std::shared_ptr<PeerProxy> proxy;

    // If constructing the proxy failed (e.g. due to a DNS resolution issue)
    // then 'proxy' will be NULL, and 'proxy_status' will contain the error.
    Status proxy_status;

    rpc::RpcController rpc;
    VoteRequestPB request;
    VoteResponsePB response;

    std::string PeerInfo() const;
  };

  typedef std::unordered_map<std::string, VoterState*> VoterStateMap;
  typedef simple_spinlock Lock;

  // This class is refcounted.
  ~LeaderElection();

  // Check to see if a decision has been made. If so, invoke decision callback.
  // Calls the callback outside of holding a lock.
  void CheckForDecision();

  // Callback called when the RPC responds.
  void VoteResponseRpcCallback(const std::string& voter_uuid);

  // Record vote from specified peer.
  void RecordVoteUnlocked(const VoterState& state, ElectionVote vote);

  // Handle a peer that reponded with a term greater than the election term.
  void HandleHigherTermUnlocked(const VoterState& state);

  // Log and record a granted vote.
  void HandleVoteGrantedUnlocked(const VoterState& state);

  // Log the reason for a denied vote and record it.
  void HandleVoteDeniedUnlocked(const VoterState& state);

  // Returns a string to be prefixed to all log entries.
  // This method accesses const members and is thread safe.
  std::string LogPrefix() const;

  // Helper to reference the term we are running the election for.
  ConsensusTerm election_term() const { return request_.candidate_term(); }

  // All non-const fields are protected by 'lock_'.
  Lock lock_;

  // The result returned by the ElectionDecisionCallback.
  // NULL if not yet known.
  gscoped_ptr<ElectionResult> result_;

  // Whether we have responded via the callback yet.
  bool has_responded_;

  // Active Raft configuration at election start time.
  const RaftConfigPB config_;

  // Factory used in the creation of new proxies.
  PeerProxyFactory* proxy_factory_;

  // Election request to send to voters.
  const VoteRequestPB request_;

  // Object to count the votes.
  const gscoped_ptr<VoteCounter> vote_counter_;

  // Timeout for sending RPCs.
  const MonoDelta timeout_;

  // Callback invoked to notify the caller of an election decision.
  const ElectionDecisionCallback decision_callback_;

  // Map of UUID -> VoterState.
  VoterStateMap voter_state_;

  // The highest term seen from a voter so far (or 0 if no votes).
  int64_t highest_voter_term_;
};

} // namespace consensus
} // namespace kudu

#endif /* KUDU_CONSENSUS_LEADER_ELECTION_H */
