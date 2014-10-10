// Copyright (c) 2014, Cloudera, inc.

#include "kudu/consensus/leader_election.h"

#include <string>
#include <vector>

#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/server/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace consensus {

using metadata::QuorumPeerPB;
using std::string;
using std::tr1::unordered_map;
using std::vector;
using strings::Substitute;

namespace {

// Generate list of voter uuids.
static vector<string> GenVoterUUIDs(int num_voters) {
  vector<string> voter_uuids;
  for (int i = 0; i < num_voters; i++) {
    voter_uuids.push_back(Substitute("peer-$0", i));
  }
  return voter_uuids;
}

} // namespace

////////////////////////////////////////
// LeaderElectionTest
////////////////////////////////////////

class LeaderElectionTest : public KuduTest {
 public:
  LeaderElectionTest()
    : tablet_id_("test-tablet"),
      latch_(1) {
  }

  ~LeaderElectionTest() {
    STLDeleteValues(&proxies_);
  }

  void ElectionCallback(const ElectionResult& result);

 protected:
  void InitUUIDs(int num_voters);
  void InitNoOpPeerProxies();
  void InitDelayableMockedProxies(bool enable_delay);
  gscoped_ptr<VoteCounter> InitVoteCounter(int num_voters, int majority_size);

  // Voter 0 is the high-term voter.
  scoped_refptr<LeaderElection> SetUpElectionWithHighTermVoter(ConsensusTerm election_term);

  // Predetermine the election results using the specified number of
  // grant / deny / error responses.
  // num_grant must be at least 1, for the candidate to vote for itself.
  // num_grant + num_deny + num_error must add up to an odd number.
  scoped_refptr<LeaderElection> SetUpElectionWithGrantDenyErrorVotes(ConsensusTerm election_term,
                                                                int num_grant,
                                                                int num_deny,
                                                                int num_error);

  const string tablet_id_;
  string candidate_uuid_;
  vector<string> voter_uuids_;
  unordered_map<string, PeerProxy*> proxies_;

  CountDownLatch latch_;
  gscoped_ptr<ElectionResult> result_;
};

void LeaderElectionTest::ElectionCallback(const ElectionResult& result) {
  result_.reset(new ElectionResult(result));
  latch_.CountDown();
}

void LeaderElectionTest::InitUUIDs(int num_voters) {
  voter_uuids_ = GenVoterUUIDs(num_voters);
  candidate_uuid_ = voter_uuids_[num_voters - 1];
  voter_uuids_.pop_back();
}

void LeaderElectionTest::InitNoOpPeerProxies() {
  BOOST_FOREACH(const string& uuid, voter_uuids_) {
    QuorumPeerPB peer_pb;
    peer_pb.set_permanent_uuid(uuid);
    PeerProxy* proxy = new NoOpTestPeerProxy(peer_pb);
    InsertOrDie(&proxies_, uuid, proxy);
  }
}

void LeaderElectionTest::InitDelayableMockedProxies(bool enable_delay) {
  BOOST_FOREACH(const string& uuid, voter_uuids_) {
    DelayablePeerProxy<MockedPeerProxy>* proxy =
        new DelayablePeerProxy<MockedPeerProxy>(new MockedPeerProxy());
    if (enable_delay) {
      proxy->DelayResponse();
    }
    InsertOrDie(&proxies_, uuid, proxy);
  }
}

gscoped_ptr<VoteCounter> LeaderElectionTest::InitVoteCounter(int num_voters, int majority_size) {
  gscoped_ptr<VoteCounter> counter(new VoteCounter(num_voters, majority_size));
  bool duplicate;
  CHECK_OK(counter->RegisterVote(candidate_uuid_, VOTE_GRANTED, &duplicate));
  CHECK(!duplicate);
  return counter.Pass();
}

scoped_refptr<LeaderElection> LeaderElectionTest::SetUpElectionWithHighTermVoter(
    ConsensusTerm election_term) {
  const int kNumVoters = 3;
  const int kMajoritySize = 2;

  InitUUIDs(kNumVoters);
  InitDelayableMockedProxies(true);
  gscoped_ptr<VoteCounter> counter = InitVoteCounter(kNumVoters, kMajoritySize);

  VoteResponsePB response;
  response.set_responder_uuid(voter_uuids_[0]);
  response.set_responder_term(election_term + 1);
  response.set_vote_granted(false);
  response.mutable_consensus_error()->set_code(ConsensusErrorPB::INVALID_TERM);
  StatusToPB(Status::InvalidArgument("Bad term"),
      response.mutable_consensus_error()->mutable_status());
  down_cast<DelayablePeerProxy<MockedPeerProxy>*>(proxies_[voter_uuids_[0]])
      ->proxy()->set_vote_response(response);

  response.Clear();
  response.set_responder_uuid(voter_uuids_[1]);
  response.set_responder_term(election_term);
  response.set_vote_granted(true);
  down_cast<DelayablePeerProxy<MockedPeerProxy>*>(proxies_[voter_uuids_[1]])
      ->proxy()->set_vote_response(response);

  VoteRequestPB request;
  request.set_candidate_uuid(candidate_uuid_);
  request.set_candidate_term(election_term);
  request.set_tablet_id(tablet_id_);

  scoped_refptr<LeaderElection> election(
      new LeaderElection(proxies_, request, counter.Pass(),
                         Bind(&LeaderElectionTest::ElectionCallback,
                              Unretained(this))));
  return election;
}

scoped_refptr<LeaderElection> LeaderElectionTest::SetUpElectionWithGrantDenyErrorVotes(
    ConsensusTerm election_term, int num_grant, int num_deny, int num_error) {
  const int kNumVoters = num_grant + num_deny + num_error;
  CHECK_GE(num_grant, 1);       // Gotta vote for yourself.
  CHECK_EQ(1, kNumVoters % 2);  // Quorum size must be odd.
  const int kMajoritySize = (kNumVoters / 2) + 1;

  InitUUIDs(kNumVoters);
  InitDelayableMockedProxies(false); // Don't delay the vote responses.
  gscoped_ptr<VoteCounter> counter = InitVoteCounter(kNumVoters, kMajoritySize);
  int num_grant_followers = num_grant - 1;

  // Set up mocked responses based on the params specified in the method arguments.
  int voter_index = 0;
  while (voter_index < voter_uuids_.size()) {
    VoteResponsePB response;
    if (num_grant_followers > 0) {
      response.set_responder_uuid(voter_uuids_[voter_index]);
      response.set_responder_term(election_term);
      response.set_vote_granted(true);
      --num_grant_followers;
    } else if (num_deny > 0) {
      response.set_responder_uuid(voter_uuids_[voter_index]);
      response.set_responder_term(election_term);
      response.set_vote_granted(false);
      response.mutable_consensus_error()->set_code(ConsensusErrorPB::LAST_OPID_TOO_OLD);
      StatusToPB(Status::InvalidArgument("Last OpId"),
          response.mutable_consensus_error()->mutable_status());
      --num_deny;
    } else if (num_error > 0) {
      response.mutable_error()->set_code(tserver::TabletServerErrorPB::TABLET_NOT_FOUND);
      StatusToPB(Status::NotFound("Unknown Tablet"),
          response.mutable_error()->mutable_status());
      --num_error;
    } else {
      LOG(FATAL) << "Unexpected fallthrough";
    }

    down_cast<DelayablePeerProxy<MockedPeerProxy>*>(proxies_[voter_uuids_[voter_index]])
        ->proxy()->set_vote_response(response);
    ++voter_index;
  }

  VoteRequestPB request;
  request.set_candidate_uuid(candidate_uuid_);
  request.set_candidate_term(election_term);
  request.set_tablet_id(tablet_id_);

  scoped_refptr<LeaderElection> election(
      new LeaderElection(proxies_, request, counter.Pass(),
                         Bind(&LeaderElectionTest::ElectionCallback,
                              Unretained(this))));
  return election;
}

// All peers respond "yes", no failures.
TEST_F(LeaderElectionTest, TestPerfectElection) {
  const int kNumVoters = 5;
  const int kMajoritySize = 3;
  const ConsensusTerm kElectionTerm = 2;

  InitUUIDs(kNumVoters);
  InitNoOpPeerProxies();
  gscoped_ptr<VoteCounter> counter = InitVoteCounter(kNumVoters, kMajoritySize);

  VoteRequestPB request;
  request.set_candidate_uuid(candidate_uuid_);
  request.set_candidate_term(kElectionTerm);
  request.set_tablet_id(tablet_id_);

  scoped_refptr<LeaderElection> election(
      new LeaderElection(proxies_, request, counter.Pass(),
                         Bind(&LeaderElectionTest::ElectionCallback,
                              Unretained(this))));
  election->Run();
  latch_.Wait();

  ASSERT_EQ(kElectionTerm, result_->election_term);
  ASSERT_EQ(VOTE_GRANTED, result_->decision);
}

// Test leader election when we encounter a peer with a higher term before we
// have arrived at a majority decision.
TEST_F(LeaderElectionTest, TestHigherTermBeforeDecision) {
  const ConsensusTerm kElectionTerm = 2;
  scoped_refptr<LeaderElection> election = SetUpElectionWithHighTermVoter(kElectionTerm);
  election->Run();

  // This guy has a higher term.
  ASSERT_OK(down_cast<DelayablePeerProxy<MockedPeerProxy>*>(proxies_[voter_uuids_[0]])
      ->Respond(TestPeerProxy::kRequestVote));
  latch_.Wait();

  ASSERT_EQ(kElectionTerm, result_->election_term);
  ASSERT_EQ(VOTE_DENIED, result_->decision);
  ASSERT_TRUE(result_->has_higher_term);
  ASSERT_EQ(kElectionTerm + 1, result_->higher_term);
  LOG(INFO) << "Election lost. Reason: " << result_->message;

  // This guy will vote "yes".
  ASSERT_OK(down_cast<DelayablePeerProxy<MockedPeerProxy>*>(proxies_[voter_uuids_[1]])
      ->Respond(TestPeerProxy::kRequestVote));
}

// Test leader election when we encounter a peer with a higher term after we
// have arrived at a majority decision of "yes".
TEST_F(LeaderElectionTest, TestHigherTermAfterDecision) {
  const ConsensusTerm kElectionTerm = 2;
  scoped_refptr<LeaderElection> election = SetUpElectionWithHighTermVoter(kElectionTerm);
  election->Run();

  // This guy will vote "yes".
  ASSERT_OK(down_cast<DelayablePeerProxy<MockedPeerProxy>*>(proxies_[voter_uuids_[1]])
      ->Respond(TestPeerProxy::kRequestVote));
  latch_.Wait();

  ASSERT_EQ(kElectionTerm, result_->election_term);
  ASSERT_EQ(VOTE_GRANTED, result_->decision);
  ASSERT_FALSE(result_->has_higher_term);
  ASSERT_TRUE(result_->message.empty());
  LOG(INFO) << "Election won.";

  // This guy has a higher term.
  ASSERT_OK(down_cast<DelayablePeerProxy<MockedPeerProxy>*>(proxies_[voter_uuids_[0]])
      ->Respond(TestPeerProxy::kRequestVote));
}

// Out-of-date OpId "vote denied" case.
TEST_F(LeaderElectionTest, TestWithDenyVotes) {
  const ConsensusTerm kElectionTerm = 2;
  const int kNumGrant = 2;
  const int kNumDeny = 3;
  const int kNumError = 0;
  scoped_refptr<LeaderElection> election =
      SetUpElectionWithGrantDenyErrorVotes(kElectionTerm, kNumGrant, kNumDeny, kNumError);
  LOG(INFO) << "Running";
  election->Run();

  latch_.Wait();
  ASSERT_EQ(kElectionTerm, result_->election_term);
  ASSERT_EQ(VOTE_DENIED, result_->decision);
  ASSERT_FALSE(result_->has_higher_term);
  ASSERT_TRUE(result_->message.empty());
  LOG(INFO) << "Election denied.";
}

// Count errors as denied votes.
TEST_F(LeaderElectionTest, TestWithErrorVotes) {
  const ConsensusTerm kElectionTerm = 2;
  const int kNumGrant = 1;
  const int kNumDeny = 0;
  const int kNumError = 4;
  scoped_refptr<LeaderElection> election =
      SetUpElectionWithGrantDenyErrorVotes(kElectionTerm, kNumGrant, kNumDeny, kNumError);
  election->Run();

  latch_.Wait();
  ASSERT_EQ(kElectionTerm, result_->election_term);
  ASSERT_EQ(VOTE_DENIED, result_->decision);
  ASSERT_FALSE(result_->has_higher_term);
  ASSERT_TRUE(result_->message.empty());
  LOG(INFO) << "Election denied.";
}

////////////////////////////////////////
// VoteCounterTest
////////////////////////////////////////

class VoteCounterTest : public KuduTest {
 protected:
  static void AssertUndecided(const VoteCounter& counter);
  static void AssertVoteCount(const VoteCounter& counter, int yes_votes, int no_votes);
};

void VoteCounterTest::AssertUndecided(const VoteCounter& counter) {
  ASSERT_FALSE(counter.IsDecided());
  ElectionVote decision;
  Status s = counter.GetDecision(&decision);
  ASSERT_TRUE(s.IsIllegalState());
  ASSERT_STR_CONTAINS(s.ToString(), "Vote not yet decided");
}

void VoteCounterTest::AssertVoteCount(const VoteCounter& counter, int yes_votes, int no_votes) {
  ASSERT_EQ(yes_votes, counter.yes_votes_);
  ASSERT_EQ(no_votes, counter.no_votes_);
  ASSERT_EQ(yes_votes + no_votes, counter.GetTotalVotesCounted());
}

// Test basic vote counting functionality with an early majority.
TEST_F(VoteCounterTest, TestVoteCounter_EarlyDecision) {
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
    ASSERT_OK(counter.RegisterVote(voter_uuids[0], VOTE_GRANTED, &duplicate));
    ASSERT_FALSE(duplicate);
    ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
    ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 1, 0));
    ASSERT_FALSE(counter.AreAllVotesIn());

    // Second yes vote wins it in a quorum of 3.
    ASSERT_OK(counter.RegisterVote(voter_uuids[1], VOTE_GRANTED, &duplicate));
    ASSERT_FALSE(duplicate);
    ASSERT_TRUE(counter.IsDecided());
    ElectionVote decision;
    ASSERT_OK(counter.GetDecision(&decision));
    ASSERT_TRUE(decision == VOTE_GRANTED);
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
    ASSERT_OK(counter.RegisterVote(voter_uuids[0], VOTE_DENIED, &duplicate));
    ASSERT_FALSE(duplicate);
    ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
    ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 0, 1));
    ASSERT_FALSE(counter.AreAllVotesIn());

    // Second no vote loses it in a quorum of 3.
    ASSERT_OK(counter.RegisterVote(voter_uuids[1], VOTE_DENIED, &duplicate));
    ASSERT_FALSE(duplicate);
    ASSERT_TRUE(counter.IsDecided());
    ElectionVote decision;
    ASSERT_OK(counter.GetDecision(&decision));
    ASSERT_TRUE(decision == VOTE_DENIED);
    ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 0, 2));
    ASSERT_FALSE(counter.AreAllVotesIn());
  }
}

// Test basic vote counting functionality with the last vote being the deciding vote.
TEST_F(VoteCounterTest, TestVoteCounter_LateDecision) {
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
  ASSERT_OK(counter.RegisterVote(voter_uuids[0], VOTE_GRANTED, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 1, 0));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Attempt duplicate vote.
  ASSERT_OK(counter.RegisterVote(voter_uuids[0], VOTE_GRANTED, &duplicate));
  ASSERT_TRUE(duplicate);
  ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 1, 0));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Attempt to change vote.
  Status s = counter.RegisterVote(voter_uuids[0], VOTE_DENIED, &duplicate);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "voted a different way twice");
  LOG(INFO) << "Expected vote-changed error: " << s.ToString();
  ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 1, 0));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Add more votes...
  ASSERT_OK(counter.RegisterVote(voter_uuids[1], VOTE_DENIED, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 1, 1));
  ASSERT_FALSE(counter.AreAllVotesIn());

  ASSERT_OK(counter.RegisterVote(voter_uuids[2], VOTE_GRANTED, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 2, 1));
  ASSERT_FALSE(counter.AreAllVotesIn());

  ASSERT_OK(counter.RegisterVote(voter_uuids[3], VOTE_DENIED, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_NO_FATAL_FAILURE(AssertUndecided(counter));
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 2, 2));
  ASSERT_FALSE(counter.AreAllVotesIn());

  // Win the election.
  ASSERT_OK(counter.RegisterVote(voter_uuids[4], VOTE_GRANTED, &duplicate));
  ASSERT_FALSE(duplicate);
  ASSERT_TRUE(counter.IsDecided());
  ElectionVote decision;
  ASSERT_OK(counter.GetDecision(&decision));
  ASSERT_TRUE(decision == VOTE_GRANTED);
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 3, 2));
  ASSERT_TRUE(counter.AreAllVotesIn());

  // Attempt to vote with > the whole quorum.
  s = counter.RegisterVote("some-random-node", VOTE_GRANTED, &duplicate);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "cause the number of votes to exceed the expected number");
  LOG(INFO) << "Expected voters-exceeded error: " << s.ToString();
  ASSERT_TRUE(counter.IsDecided());
  ASSERT_NO_FATAL_FAILURE(AssertVoteCount(counter, 3, 2));
  ASSERT_TRUE(counter.AreAllVotesIn());
}

}  // namespace consensus
}  // namespace kudu
