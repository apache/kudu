// Copyright (c) 2014, Cloudera, inc.

#include "kudu/consensus/leader_election.h"

#include <boost/bind.hpp>
#include <boost/foreach.hpp>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {

using strings::Substitute;

///////////////////////////////////////////////////
// VoteCounter
///////////////////////////////////////////////////

VoteCounter::VoteCounter(int num_voters, int majority_size)
  : num_voters_(num_voters),
    majority_size_(majority_size),
    yes_votes_(0),
    no_votes_(0) {
  DCHECK_LE(majority_size, num_voters);
  DCHECK_GT(num_voters_, 0);
  DCHECK_GT(majority_size_, 0);
}

Status VoteCounter::RegisterVote(const std::string& voter_uuid, Vote vote, bool* is_duplicate) {
  // Handle repeated votes.
  if (PREDICT_FALSE(ContainsKey(votes_, voter_uuid))) {
    // Detect changed votes.
    Vote prior_vote = votes_[voter_uuid];
    if (PREDICT_FALSE(prior_vote != vote)) {
      string msg = Substitute("Peer $0 voted a different way twice in the same election. "
                              "First vote: $1, second vote: $2.",
                              voter_uuid, prior_vote, vote);
      return Status::InvalidArgument(msg);
    }

    // This was just a duplicate. Allow the caller to log it but don't change
    // the voting record.
    *is_duplicate = true;
    return Status::OK();
  }

  // Sanity check to ensure we did not exceed the allowed number of voters.
  if (PREDICT_FALSE(yes_votes_ + no_votes_ == num_voters_)) {
    // More unique voters than allowed!
    return Status::InvalidArgument(Substitute(
        "Vote from peer $0 would cause the number of votes to exceed the expected number of "
        "voters, which is $1. Votes already received from the following peers: {$2}",
        voter_uuid,
        num_voters_,
        JoinKeysIterator(votes_.begin(), votes_.end(), ", ")));
  }

  // This is a valid vote, so store it.
  InsertOrDie(&votes_, voter_uuid, vote);
  switch (vote) {
    case VOTE_GRANTED:
      ++yes_votes_;
      break;
    case VOTE_DENIED:
      ++no_votes_;
      break;
  }
  *is_duplicate = false;
  return Status::OK();
}

bool VoteCounter::IsDecided() const {
  return (yes_votes_ >= majority_size_ || no_votes_ >= majority_size_);
}

Status VoteCounter::GetDecision(Vote* decision) const {
  if (yes_votes_ >= majority_size_) {
    *decision = VOTE_GRANTED;
    return Status::OK();
  }
  if (no_votes_ >= majority_size_) {
    *decision = VOTE_DENIED;
    return Status::OK();
  }
  return Status::IllegalState("Vote not yet decided");
}

int VoteCounter::GetTotalVotesCounted() const {
  return yes_votes_ + no_votes_;
}

bool VoteCounter::AreAllVotesIn() const {
  return GetTotalVotesCounted() == num_voters_;
}

} // namespace consensus
} // namespace kudu
