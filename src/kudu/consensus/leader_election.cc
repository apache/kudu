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

// **************   NOTICE  *******************************************
// Facebook 2019 - Notice of Changes
// This file has been modified to extract only the Raft implementation
// out of Kudu into a fork known as kuduraft.
// ********************************************************************

#include "kudu/consensus/leader_election.h"

#include <algorithm>
#include <mutex>
#include <ostream>
#include <type_traits>
#include <vector>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <glog/logging.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_controller.h"
//#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {

using std::string;
using std::vector;
using strings::Substitute;

///////////////////////////////////////////////////
// VoteCounter & FlexibleVoteCounter
///////////////////////////////////////////////////

VoteCounter::VoteCounter(int num_voters, int majority_size)
  : num_voters_(num_voters),
    majority_size_(majority_size),
    yes_votes_(0),
    no_votes_(0) {
  CHECK_LE(majority_size, num_voters);
  CHECK_GT(num_voters_, 0);
  CHECK_GT(majority_size_, 0);
}

Status VoteCounter::RegisterVote(const std::string& voter_uuid, ElectionVote vote,
                                 bool* is_duplicate) {
  // Handle repeated votes.
  if (PREDICT_FALSE(ContainsKey(votes_, voter_uuid))) {
    // Detect changed votes.
    ElectionVote prior_vote = votes_[voter_uuid];
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
  return yes_votes_ >= majority_size_ ||
         no_votes_ > num_voters_ - majority_size_;
}

Status VoteCounter::GetDecision(ElectionVote* decision) const {
  if (yes_votes_ >= majority_size_) {
    *decision = VOTE_GRANTED;
    return Status::OK();
  }
  if (no_votes_ > num_voters_ - majority_size_) {
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

void FlexibleVoteCounter::FetchTopologyInfo(
    std::map<std::string, int>* voter_distribution,
    std::map<std::string, int>* le_quorum_requirement) {
  CHECK(voter_distribution);
  CHECK(le_quorum_requirement);
  voter_distribution->clear();
  le_quorum_requirement->clear();
  // TODO(ritwikyadav): Populate the out params by reading config.
}

FlexibleVoteCounter::FlexibleVoteCounter(RaftConfigPB config)
  : VoteCounter(0, 0),
    config_(std::move(config)) {
  num_voters_ = 0;

  FetchTopologyInfo(&voter_distribution_, &le_quorum_requirement_);
  for (
      const std::pair<std::string, int>& regional_voter_count :
      voter_distribution_) {
    CHECK_GT(regional_voter_count.second, 0);
    num_voters_ += regional_voter_count.second;
    yes_vote_count_.emplace(regional_voter_count.first, 0);
    no_vote_count_.emplace(regional_voter_count.first, 0);
  }

  CHECK_GT(num_voters_, 0);

  // Flag to check if the quorum specification is non-empty for all
  // specifications.
  bool empty_quorum = false;
  for (
      const std::pair<std::string, int>& regional_quorum_count :
      le_quorum_requirement_) {
    const std::map<std::string, int>::const_iterator total_voters =
        voter_distribution_.find(regional_quorum_count.first);
    // Make sure that voters are present in the region specified by quorum
    // requirement and their number is enough.
    CHECK(total_voters != voter_distribution_.end());
    CHECK_LE(regional_quorum_count.second, total_voters->second);
    if (regional_quorum_count.second <= 0) {
      empty_quorum = true;
    }
  }
  CHECK(!empty_quorum);

  for (const RaftPeerPB& peer : config_.peers()) {
    if (peer.member_type() == RaftPeerPB::VOTER) {
      uuid_to_region_.emplace(peer.permanent_uuid(), peer.attrs().region());
    }
  }
}

Status FlexibleVoteCounter::RegisterVote(
    const std::string& voter_uuid, ElectionVote vote, bool* is_duplicate) {
  Status s = VoteCounter::RegisterVote(voter_uuid, vote, is_duplicate);

  // No book-keeping required for duplicate votes.
  if (*is_duplicate) {
    return s;
  }

  if (!ContainsKey(uuid_to_region_, voter_uuid)) {
    return Status::InvalidArgument(
        Substitute("UUID {$0} not present in config.", voter_uuid));
  }

  const std::string& region = uuid_to_region_[voter_uuid];
  switch (vote) {
    case VOTE_GRANTED:
      InsertIfNotPresent(&yes_vote_count_, region, 0);
      yes_vote_count_[region]++;
      break;
    case VOTE_DENIED:
      InsertIfNotPresent(&no_vote_count_, region, 0);
      no_vote_count_[region]++;
      break;
  }

  return s;
}

std::pair<bool, bool> FlexibleVoteCounter::GetQuorumState() const {
  bool quorum_satisfied = true;
  bool quorum_satisfaction_possible = true;
  for (
      const std::pair<std::string, int>& regional_voter_count :
      le_quorum_requirement_) {
    const string region = regional_voter_count.first;

    const std::map<std::string, int>::const_iterator regional_yes_count =
        yes_vote_count_.find(region);
    const std::map<std::string, int>::const_iterator regional_no_count =
        no_vote_count_.find(region);
    const std::map<std::string, int>::const_iterator total_region_count =
        voter_distribution_.find(region);

    CHECK(regional_yes_count != yes_vote_count_.end());
    CHECK(regional_no_count != no_vote_count_.end());
    CHECK(total_region_count != voter_distribution_.end());

    if (regional_yes_count->second < regional_voter_count.second) {
      quorum_satisfied = false;
    }
    if (regional_no_count->second + regional_voter_count.second >
        total_region_count->second) {
      quorum_satisfaction_possible = false;
    }
  }
  return std::make_pair(quorum_satisfied, quorum_satisfaction_possible);
}

bool FlexibleVoteCounter::IsDecided() const {
  const std::pair<bool, bool> quorum_state = GetQuorumState();
  return quorum_state.first || !quorum_state.second;
}

Status FlexibleVoteCounter::GetDecision(ElectionVote* decision) const {
  const std::pair<bool, bool> quorum_state = GetQuorumState();
  if (quorum_state.first) {
    *decision = VOTE_GRANTED;
    return Status::OK();
  }
  if (!quorum_state.second) {
    *decision = VOTE_DENIED;
    return Status::OK();
  }
  return Status::IllegalState("Vote not yet decided");
}

///////////////////////////////////////////////////
// ElectionResult
///////////////////////////////////////////////////

ElectionResult::ElectionResult(VoteRequestPB vote_request, ElectionVote decision,
                               ConsensusTerm highest_voter_term, const std::string& message)
  : vote_request(std::move(vote_request)),
    decision(decision),
    highest_voter_term(highest_voter_term),
    message(message) {
  DCHECK(!message.empty());
}

///////////////////////////////////////////////////
// LeaderElection::VoterState
///////////////////////////////////////////////////

string LeaderElection::VoterState::PeerInfo() const {
  std::string info = peer_uuid;
  if (proxy) {
    strings::SubstituteAndAppend(&info, " ($0)", proxy->PeerName());
  }
  return info;
}

///////////////////////////////////////////////////
// LeaderElection
///////////////////////////////////////////////////

LeaderElection::LeaderElection(RaftConfigPB config,
                               PeerProxyFactory* proxy_factory,
                               VoteRequestPB request,
                               gscoped_ptr<VoteCounter> vote_counter,
                               MonoDelta timeout,
                               ElectionDecisionCallback decision_callback)
    : has_responded_(false),
      config_(std::move(config)),
      proxy_factory_(proxy_factory),
      request_(std::move(request)),
      vote_counter_(std::move(vote_counter)),
      timeout_(timeout),
      decision_callback_(std::move(decision_callback)),
      highest_voter_term_(0) {
}

LeaderElection::~LeaderElection() {
  std::lock_guard<Lock> guard(lock_);
  DCHECK(has_responded_); // We must always call the callback exactly once.
  STLDeleteValues(&voter_state_);
}

void LeaderElection::Run() {
  VLOG_WITH_PREFIX(1) << "Running leader election.";

  // Initialize voter state tracking.
  vector<string> other_voter_uuids;
  voter_state_.clear();
  for (const RaftPeerPB& peer : config_.peers()) {
    if (request_.candidate_uuid() == peer.permanent_uuid()) {
      DCHECK_EQ(peer.member_type(), RaftPeerPB::VOTER)
          << Substitute("non-voter member $0 tried to start an election; "
                        "Raft config {$1}",
                        peer.permanent_uuid(),
                        pb_util::SecureShortDebugString(config_));
      continue;
    }
    if (peer.member_type() != RaftPeerPB::VOTER) {
      continue;
    }
    other_voter_uuids.emplace_back(peer.permanent_uuid());

    gscoped_ptr<VoterState> state(new VoterState());
    state->peer_uuid = peer.permanent_uuid();
    state->proxy_status = proxy_factory_->NewProxy(peer, &state->proxy);
    InsertOrDie(&voter_state_, peer.permanent_uuid(), state.release());
  }

  // Ensure that the candidate has already voted for itself.
  CHECK_EQ(1, vote_counter_->GetTotalVotesCounted()) << "Candidate must vote for itself first";

  // Ensure that existing votes + future votes add up to the expected total.
  CHECK_EQ(vote_counter_->GetTotalVotesCounted() + other_voter_uuids.size(),
           vote_counter_->GetTotalExpectedVotes())
      << "Expected different number of voters. Voter UUIDs: ["
      << JoinStringsIterator(other_voter_uuids.begin(), other_voter_uuids.end(), ", ")
      << "]; RaftConfig: {" << pb_util::SecureShortDebugString(config_) << "}";

  // Check if we have already won the election (relevant if this is a
  // single-node configuration, since we always pre-vote for ourselves).
  CheckForDecision();

  // The rest of the code below is for a typical multi-node configuration.
  for (const auto& voter_uuid : other_voter_uuids) {
    VoterState* state = nullptr;
    {
      std::lock_guard<Lock> guard(lock_);
      state = FindOrDie(voter_state_, voter_uuid);
      // Safe to drop the lock because voter_state_ is not mutated outside of
      // the constructor / destructor. We do this to avoid deadlocks below.
    }

    // If we failed to construct the proxy, just record a 'NO' vote with the status
    // that indicates why it failed.
    if (!state->proxy_status.ok()) {
      LOG_WITH_PREFIX(WARNING) << "Was unable to construct an RPC proxy to peer "
                               << state->PeerInfo() << ": " << state->proxy_status.ToString()
                               << ". Counting it as a 'NO' vote.";
      {
        std::lock_guard<Lock> guard(lock_);
        RecordVoteUnlocked(*state, VOTE_DENIED);
      }
      CheckForDecision();
      continue;
    }

    // Send the RPC request.
    LOG_WITH_PREFIX(INFO) << "Requesting "
                          << (request_.is_pre_election() ? "pre-" : "")
                          << "vote from peer " << state->PeerInfo();
    state->rpc.set_timeout(timeout_);

    state->request = request_;
    state->request.set_dest_uuid(voter_uuid);

    state->proxy->RequestConsensusVoteAsync(
        &state->request,
        &state->response,
        &state->rpc,
        // We use gutil Bind() for the refcounting and boost::bind to adapt the
        // gutil Callback to a thunk.
        boost::bind(&Closure::Run,
                    Bind(&LeaderElection::VoteResponseRpcCallback, this, voter_uuid)));
  }
}

void LeaderElection::CheckForDecision() {
  bool to_respond = false;
  {
    std::lock_guard<Lock> guard(lock_);
    // Check if the vote has been newly decided.
    if (!result_ && vote_counter_->IsDecided()) {
      ElectionVote decision;
      CHECK_OK(vote_counter_->GetDecision(&decision));
      LOG_WITH_PREFIX(INFO) << "Election decided. Result: candidate "
                << ((decision == VOTE_GRANTED) ? "won." : "lost.");
      string msg = (decision == VOTE_GRANTED) ?
          "achieved majority votes" : "could not achieve majority";
      result_.reset(new ElectionResult(request_, decision, highest_voter_term_, msg));
    }
    // Check whether to respond. This can happen as a result of either getting
    // a majority vote or of something invalidating the election, like
    // observing a higher term.
    if (result_ && !has_responded_) {
      has_responded_ = true;
      to_respond = true;
    }
  }

  // Respond outside of the lock.
  if (to_respond) {
    // This is thread-safe since result_ is write-once.
    decision_callback_(*result_);
  }
}

void LeaderElection::VoteResponseRpcCallback(const std::string& voter_uuid) {
  {
    std::lock_guard<Lock> guard(lock_);
    VoterState* state = FindOrDie(voter_state_, voter_uuid);

    // Check for RPC errors.
    if (!state->rpc.status().ok()) {
      LOG_WITH_PREFIX(WARNING) << "RPC error from VoteRequest() call to peer "
                               << state->PeerInfo() << ": "
                               << state->rpc.status().ToString();
      RecordVoteUnlocked(*state, VOTE_DENIED);

    // Check for tablet errors.
    } else if (state->response.has_error()) {
#ifdef FB_DO_NOT_REMOVE
      LOG_WITH_PREFIX(WARNING) << "Tablet error from VoteRequest() call to peer "
                               << state->PeerInfo() << ": "
                               << StatusFromPB(state->response.error().status()).ToString();
#endif
      RecordVoteUnlocked(*state, VOTE_DENIED);

    // If the peer changed their IP address, we shouldn't count this vote since
    // our knowledge of the configuration is in an inconsistent state.
    } else if (PREDICT_FALSE(voter_uuid != state->response.responder_uuid())) {
      LOG_WITH_PREFIX(DFATAL) << "Received vote response from peer "
                              << state->PeerInfo() << ": "
                              << "we thought peer had UUID " << voter_uuid
                              << " but its actual UUID is "
                              << state->response.responder_uuid();
      RecordVoteUnlocked(*state, VOTE_DENIED);

    } else {
      // No error: count actual votes.

      highest_voter_term_ = std::max(highest_voter_term_, state->response.responder_term());
      if (state->response.vote_granted()) {
        HandleVoteGrantedUnlocked(*state);
      } else {
        HandleVoteDeniedUnlocked(*state);
      }
    }
  }

  // Check for a decision outside the lock.
  CheckForDecision();
}

void LeaderElection::RecordVoteUnlocked(const VoterState& state, ElectionVote vote) {
  DCHECK(lock_.is_locked());

  // Record the vote.
  bool duplicate;
  Status s = vote_counter_->RegisterVote(state.peer_uuid, vote, &duplicate);
  if (!s.ok()) {
    LOG_WITH_PREFIX(WARNING) << "Error registering vote for peer "
                             << state.PeerInfo() << ": " << s.ToString();
    return;
  }
  if (duplicate) {
    // Note: This is DFATAL because at the time of writing we do not support
    // retrying vote requests, so this should be impossible. It may be valid to
    // receive duplicate votes in the future if we implement retry.
    LOG_WITH_PREFIX(DFATAL) << "Duplicate vote received from peer " << state.PeerInfo();
  }
}

void LeaderElection::HandleHigherTermUnlocked(const VoterState& state) {
  DCHECK(lock_.is_locked());
  DCHECK_GT(state.response.responder_term(), election_term());

  string msg = Substitute("Vote denied by peer $0 with higher term. Message: $1",
                          state.PeerInfo(),
                          StatusFromPB(state.response.consensus_error().status()).ToString());
  LOG_WITH_PREFIX(WARNING) << msg;

  if (!result_) {
    LOG_WITH_PREFIX(INFO) << "Cancelling election due to peer responding with higher term";
    result_.reset(new ElectionResult(request_, VOTE_DENIED,
                                     state.response.responder_term(), msg));
  }
}

void LeaderElection::HandleVoteGrantedUnlocked(const VoterState& state) {
  DCHECK(lock_.is_locked());
  if (!request_.is_pre_election()) {
    DCHECK_EQ(state.response.responder_term(), election_term());
  }
  DCHECK(state.response.vote_granted());

  LOG_WITH_PREFIX(INFO) << "Vote granted by peer " << state.PeerInfo();
  RecordVoteUnlocked(state, VOTE_GRANTED);
}

void LeaderElection::HandleVoteDeniedUnlocked(const VoterState& state) {
  DCHECK(lock_.is_locked());
  DCHECK(!state.response.vote_granted());

  // If one of the voters responds with a greater term than our own, and we
  // have not yet triggered the decision callback, it cancels the election.
  if (state.response.responder_term() > election_term()) {
    return HandleHigherTermUnlocked(state);
  }

  LOG_WITH_PREFIX(INFO) << "Vote denied by peer " << state.PeerInfo() << ". Message: "
            << StatusFromPB(state.response.consensus_error().status()).ToString();
  RecordVoteUnlocked(state, VOTE_DENIED);
}

std::string LeaderElection::LogPrefix() const {
  return Substitute("T $0 P $1 [CANDIDATE]: Term $2 $3election: ",
                    request_.tablet_id(),
                    request_.candidate_uuid(),
                    request_.candidate_term(),
                    request_.is_pre_election() ? "pre-" : "");
}

} // namespace consensus
} // namespace kudu
