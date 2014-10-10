// Copyright (c) 2014, Cloudera, inc.

#include "kudu/consensus/leader_election.h"

#include <string>
#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace consensus {

using std::string;
using std::vector;
using strings::Substitute;

class LeaderElectionTest : public KuduTest {

 protected:
  vector<string> GenVoterUUIDs(int num_voters);
  void AssertUndecided(const VoteCounter& counter);
  void AssertVoteCount(const VoteCounter& counter, int yes_votes, int no_votes);
};

vector<string> LeaderElectionTest::GenVoterUUIDs(int num_voters) {
  vector<string> voter_uuids;
  for (int i = 0; i < num_voters; i++) {
    voter_uuids.push_back(Substitute("peer-$0", i));
  }
  return voter_uuids;
}

void LeaderElectionTest::AssertUndecided(const VoteCounter& counter) {
  ASSERT_FALSE(counter.IsDecided());
  VoteCounter::Vote decision;
  Status s = counter.GetDecision(&decision);
  ASSERT_TRUE(s.IsIllegalState());
  ASSERT_STR_CONTAINS(s.ToString(), "Vote not yet decided");
}

void LeaderElectionTest::AssertVoteCount(const VoteCounter& counter, int yes_votes, int no_votes) {
  ASSERT_EQ(yes_votes, counter.yes_votes_);
  ASSERT_EQ(no_votes, counter.no_votes_);
  ASSERT_EQ(yes_votes + no_votes, counter.GetTotalVotesCounted());
}

// Test basic vote counting functionality with an early majority.
TEST_F(LeaderElectionTest, TestVoteCounter_EarlyDecision) {
  const int kNumVoters = 3;
  const int kMajoritySize = 2;
  vector<string> voter_uuids = GenVoterUUIDs(kNumVoters);

  // "Yes" decision.
  {
    // Start off undecided.
    VoteCounter counter(kNumVoters, kMajoritySize);
    ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
    ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 0, 0));
    ASSERT_FALSE(counter.AreAllVotesIn());

    // First yes vote.
    bool duplicate;
    ASSERT_OK(counter.RegisterVote(voter_uuids[0], VoteCounter::VOTE_GRANTED, &duplicate));
    ASSERT_FALSE(duplicate);
    ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
    ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 1, 0));
    ASSERT_FALSE(counter.AreAllVotesIn());

    // Second yes vote wins it in a quorum of 3.
    ASSERT_OK(counter.RegisterVote(voter_uuids[1], VoteCounter::VOTE_GRANTED, &duplicate));
    ASSERT_FALSE(duplicate);
    ASSERT_TRUE(counter.IsDecided());
    VoteCounter::Vote decision;
    ASSERT_OK(counter.GetDecision(&decision));
    ASSERT_TRUE(decision == VoteCounter::VOTE_GRANTED);
    ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 2, 0));
    ASSERT_FALSE(counter.AreAllVotesIn());
  }

  // "No" decision.
  {
    // Start off undecided.
    VoteCounter counter(kNumVoters, kMajoritySize);
    ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
    ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 0, 0));
    ASSERT_FALSE(counter.AreAllVotesIn());

    // First no vote.
    bool duplicate;
    ASSERT_OK(counter.RegisterVote(voter_uuids[0], VoteCounter::VOTE_DENIED, &duplicate));
    ASSERT_FALSE(duplicate);
    ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
    ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 0, 1));
    ASSERT_FALSE(counter.AreAllVotesIn());

    // Second no vote loses it in a quorum of 3.
    ASSERT_OK(counter.RegisterVote(voter_uuids[1], VoteCounter::VOTE_DENIED, &duplicate));
    ASSERT_FALSE(duplicate);
    ASSERT_TRUE(counter.IsDecided());
    VoteCounter::Vote decision;
    ASSERT_OK(counter.GetDecision(&decision));
    ASSERT_TRUE(decision == VoteCounter::VOTE_DENIED);
    ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 0, 2));
    ASSERT_FALSE(counter.AreAllVotesIn());
  }
}

// Test basic vote counting functionality with the last vote being the deciding vote.
TEST_F(LeaderElectionTest, TestVoteCounter_LateDecision) {
  const int kNumVoters = 5;
  const int kMajoritySize = 3;
  vector<string> voter_uuids = GenVoterUUIDs(kNumVoters);

  // Start off undecided.
  VoteCounter counter(kNumVoters, kMajoritySize);
  ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 0, 0));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Add single yes vote, still undecided.
  bool duplicate;
  ASSERT_OK(counter.RegisterVote(voter_uuids[0], VoteCounter::VOTE_GRANTED, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 1, 0));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Attempt duplicate vote.
  ASSERT_OK(counter.RegisterVote(voter_uuids[0], VoteCounter::VOTE_GRANTED, &duplicate));
  ASSERT_TRUE(duplicate);
  ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 1, 0));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Attempt to change vote.
  Status s = counter.RegisterVote(voter_uuids[0], VoteCounter::VOTE_DENIED, &duplicate);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "voted a different way twice");
  LOG(INFO) << "Expected vote-changed error: " << s.ToString();
  ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 1, 0));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Add more votes...
  ASSERT_OK(counter.RegisterVote(voter_uuids[1], VoteCounter::VOTE_DENIED, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 1, 1));
  ASSERT_FALSE(counter.AreAllVotesIn());

  ASSERT_OK(counter.RegisterVote(voter_uuids[2], VoteCounter::VOTE_GRANTED, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 2, 1));
  ASSERT_FALSE(counter.AreAllVotesIn());

  ASSERT_OK(counter.RegisterVote(voter_uuids[3], VoteCounter::VOTE_DENIED, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 2, 2));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Win the election.
  ASSERT_OK(counter.RegisterVote(voter_uuids[4], VoteCounter::VOTE_GRANTED, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_TRUE(counter.IsDecided());
  VoteCounter::Vote decision;
  ASSERT_OK(counter.GetDecision(&decision));
  ASSERT_TRUE(decision == VoteCounter::VOTE_GRANTED);
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 3, 2));
  ASSERT_TRUE(counter.AreAllVotesIn());

  // Attempt to vote with > the whole quorum.
  s = counter.RegisterVote("some-random-node", VoteCounter::VOTE_GRANTED, &duplicate);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "cause the number of votes to exceed the expected number");
  LOG(INFO) << "Expected voters-exceeded error: " << s.ToString();
  ASSERT_TRUE(counter.IsDecided());
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 3, 2));
  ASSERT_TRUE(counter.AreAllVotesIn());
}

}  // namespace consensus
}  // namespace kudu
