// Copyright (c) 2014, Cloudera, inc.

#ifndef KUDU_CONSENSUS_LEADER_ELECTION_H
#define KUDU_CONSENSUS_LEADER_ELECTION_H

#include <map>
#include <string>

#include "kudu/gutil/macros.h"

namespace kudu {
class Status;

namespace consensus {

// Simple class to count votes (in-memory, not persisted to disk).
// This class is not thread safe and requires external synchronization.
class VoteCounter {
 public:
  // The vote a peer has given.
  enum Vote {
    VOTE_DENIED,
    VOTE_GRANTED,
  };

  // Create new VoteCounter with the given majority size.
  VoteCounter(int num_voters, int majority_size);

  // Register a peer's vote.
  //
  // If the voter already has a vote recorded, but it has a different value than
  // the vote specified, returns Status::IllegalArgument.
  //
  // If the same vote is duplicated, 'is_duplicate' is set to true.
  // Otherwise, it is set to false.
  // If an OK status is not returned, the value in 'is_duplicate' is undefined.
  Status RegisterVote(const std::string& voter_uuid, Vote vote, bool* is_duplicate);

  // Return whether the vote is decided yet.
  bool IsDecided() const;

  // Return decision iff IsDecided() returns true.
  // If vote is not yet decided, returns Status::IllegalState().
  Status GetDecision(Vote* decision) const;

  // Return the total of "Yes" and "No" votes.
  int GetTotalVotesCounted() const;

  // Return true iff GetTotalVotesCounted() == num_voters_;
  bool AreAllVotesIn() const;

 private:
  friend class LeaderElectionTest;

  typedef std::map<std::string, Vote> VoteMap;

  const int num_voters_;
  const int majority_size_;
  VoteMap votes_; // Voting record.
  int yes_votes_; // Accumulated yes votes, for quick counting.
  int no_votes_;  // Accumulated no votes.

  DISALLOW_COPY_AND_ASSIGN(VoteCounter);
};

} // namespace consensus
} // namespace kudu

#endif /* KUDU_CONSENSUS_LEADER_ELECTION_H */
