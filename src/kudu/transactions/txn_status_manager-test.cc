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

#include "kudu/transactions/txn_status_manager.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet_replica-test-base.h"
#include "kudu/tablet/txn_coordinator.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/transactions/txn_status_tablet.h"
#include "kudu/util/barrier.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::consensus::ConsensusBootstrapInfo;
using kudu::tablet::ParticipantIdsByTxnId;
using kudu::tablet::TabletReplicaTestBase;
using kudu::transactions::TxnStatePB;
using kudu::transactions::TxnStatusEntryPB;
using kudu::tserver::TabletServerErrorPB;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_set;
using std::vector;

METRIC_DECLARE_entity(tablet);

namespace kudu {
namespace transactions {

namespace {
const char* kOwner = "gru";
const char* kParticipant = "minion";
string ParticipantId(int i) {
  return Substitute("$0$1", kParticipant, i);
}
} // anonymous namespace

class TxnStatusManagerTest : public TabletReplicaTestBase {
 public:
  TxnStatusManagerTest()
      : TabletReplicaTestBase(TxnStatusTablet::GetSchemaWithoutIds()) {}

  void SetUp() override {
    NO_FATALS(TabletReplicaTestBase::SetUp());
    ConsensusBootstrapInfo info;
    ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
    txn_manager_.reset(new TxnStatusManager(tablet_replica_.get()));
  }
 protected:
  unique_ptr<TxnStatusManager> txn_manager_;
};

// Test our ability to start transactions and register participants, with some
// corner cases.
TEST_F(TxnStatusManagerTest, TestStartTransactions) {
  const string kParticipant1 = ParticipantId(1);
  const string kParticipant2 = ParticipantId(2);
  const ParticipantIdsByTxnId expected_prts_by_txn_id = {
    { 1, {} },
    { 3, { kParticipant1, kParticipant2 } },
  };

  ASSERT_TRUE(txn_manager_->GetParticipantsByTxnIdForTests().empty());

  TabletServerErrorPB ts_error;
  for (const auto& txn_id_and_prts : expected_prts_by_txn_id) {
    const auto& txn_id = txn_id_and_prts.first;
    ASSERT_OK(txn_manager_->BeginTransaction(txn_id, kOwner, &ts_error));
    for (const auto& prt : txn_id_and_prts.second) {
      ASSERT_OK(txn_manager_->RegisterParticipant(txn_id, prt, kOwner, &ts_error));
    }
  }
  // Registering a participant that's already open is harmless, presuming the
  // participant is still open.
  ASSERT_OK(txn_manager_->RegisterParticipant(3, kParticipant1, kOwner, &ts_error));

  // Starting a transaction that's already been started should result in an
  // error, even if it's not currently in flight.
  Status s = txn_manager_->BeginTransaction(1, kOwner, &ts_error);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  s = txn_manager_->BeginTransaction(2, kOwner, &ts_error);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();

  // Registering participants to transactions that don't exist should also
  // result in errors.
  s = txn_manager_->RegisterParticipant(2, kParticipant1, kOwner, &ts_error);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  // The underlying participants map should only reflect the successful
  // operations.
  ASSERT_EQ(expected_prts_by_txn_id,
            txn_manager_->GetParticipantsByTxnIdForTests());
  ASSERT_EQ(3, txn_manager_->highest_txn_id());
  {
    // Reload the TxnStatusManager from disk and verify the state.
    TxnStatusManager txn_manager_reloaded(tablet_replica_.get());
    ASSERT_OK(txn_manager_reloaded.LoadFromTablet());
    ASSERT_EQ(expected_prts_by_txn_id,
              txn_manager_reloaded.GetParticipantsByTxnIdForTests());
    ASSERT_EQ(3, txn_manager_reloaded.highest_txn_id());
  }

  // Now rebuild the underlying replica and rebuild the TxnStatusManager.
  ASSERT_OK(RestartReplica());
  {
    TxnStatusManager txn_manager_reloaded(tablet_replica_.get());
    ASSERT_OK(txn_manager_reloaded.LoadFromTablet());
    ASSERT_EQ(expected_prts_by_txn_id,
              txn_manager_reloaded.GetParticipantsByTxnIdForTests());
    ASSERT_EQ(3, txn_manager_reloaded.highest_txn_id());
  }
}

TEST_F(TxnStatusManagerTest, TestStartTransactionsConcurrently) {
  simple_spinlock lock;
  const int kParallelTxnsPerBatch = 10;
  const int kBatchesToStart = 10;
  vector<int64_t> successful_txn_ids;
  successful_txn_ids.reserve(kParallelTxnsPerBatch * kBatchesToStart);

  // Put together the batches of transaction IDs we're going to start.
  vector<vector<int64_t>> txns_to_insert;
  for (int i = 0; i < kBatchesToStart; i++) {
    vector<int64_t> txns_in_batch(kParallelTxnsPerBatch);
    std::iota(txns_in_batch.begin(), txns_in_batch.end(), i * kParallelTxnsPerBatch);
    std::random_shuffle(txns_in_batch.begin(), txns_in_batch.end());
    txns_to_insert.emplace_back(std::move(txns_in_batch));
  }

  // From multiple threads, begin txns and record any that return with a
  // success.
  vector<thread> threads;
  vector<std::unique_ptr<Barrier>> barriers;
  threads.reserve(kParallelTxnsPerBatch);
  barriers.reserve(kBatchesToStart);
  for (int b = 0; b < kBatchesToStart; b++) {
    // NOTE: we allocate these on the heap since we disallow assignment of
    // barriers.
    barriers.emplace_back(new Barrier(kParallelTxnsPerBatch));
  }
  for (int i = 0; i < kParallelTxnsPerBatch; i++) {
    threads.emplace_back([&, i] {
      for (int b = 0; b < kBatchesToStart; b++) {
        // Synchronize the threads so we're inserting to a single range at a
        // time.
        barriers[b]->Wait();
        auto txn_id = txns_to_insert[b][i];
        TabletServerErrorPB ts_error;
        Status s = txn_manager_->BeginTransaction(txn_id, kOwner, &ts_error);
        if (s.ok()) {
          std::lock_guard<simple_spinlock> l(lock);
          successful_txn_ids.emplace_back(txn_id);
        }
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  // Verify that only txns that returned success ended up in the
  // TxnStatusManager
  ParticipantIdsByTxnId prts_by_txn_id = txn_manager_->GetParticipantsByTxnIdForTests();
  EXPECT_EQ(successful_txn_ids.size(), prts_by_txn_id.size());
  for (const auto& txn_id : successful_txn_ids) {
    EXPECT_TRUE(ContainsKey(prts_by_txn_id, txn_id));
  }
  // As a sanity check, there should have been at least one success per batch,
  // though there may have been multiple failures if the threads raced for the
  // highest transaction ID.
  ASSERT_GE(successful_txn_ids.size(), kBatchesToStart);
}

TEST_F(TxnStatusManagerTest, TestRegisterParticipantsConcurrently) {
  const int kParticipantsInParallel = 10;
  const int kUniqueParticipantIds = 5;
  simple_spinlock lock;
  vector<string> successful_participants;
  successful_participants.reserve(kParticipantsInParallel);

  const int64_t kTxnId = 1;
  vector<thread> threads;
  CountDownLatch begun_txn(1);
  threads.reserve(1 + kParticipantsInParallel);
  threads.emplace_back([&] {
    TabletServerErrorPB ts_error;
    CHECK_OK(txn_manager_->BeginTransaction(kTxnId, kOwner, &ts_error));
    begun_txn.CountDown();
  });

  // Register a bunch of participants in parallel, including some duplicates,
  // keeping track of the ones that yielded success.
  for (int i = 0; i < kParticipantsInParallel; i++) {
    threads.emplace_back([&, i] {
      if (i % 2) {
        // In some threads, wait for the transaction to have begun, to ensure
        // at least some of the participant registrations succeed.
        begun_txn.Wait();
      }
      string prt = ParticipantId(i % kUniqueParticipantIds);
      TabletServerErrorPB ts_error;
      Status s = txn_manager_->RegisterParticipant(kTxnId, prt, kOwner, &ts_error);
      if (s.ok()) {
        std::lock_guard<simple_spinlock> l(lock);
        successful_participants.emplace_back(std::move(prt));
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  // Verify that only participant registrations that returned success ended up
  // in the TxnStatusManager.
  ParticipantIdsByTxnId prts_by_txn_id = txn_manager_->GetParticipantsByTxnIdForTests();
  ASSERT_EQ(1, prts_by_txn_id.size());
  const auto& txn_id_and_prts = *prts_by_txn_id.begin();
  ASSERT_EQ(kTxnId, txn_id_and_prts.first);

  const auto& participants = txn_id_and_prts.second;
  unordered_set<string> successful_prts(
      successful_participants.begin(), successful_participants.end());
  EXPECT_EQ(successful_prts.size(), participants.size());

  for (const auto& prt : participants) {
    EXPECT_TRUE(ContainsKey(successful_prts, prt));
  }
  ASSERT_GT(successful_prts.size(), 0);
}

TEST_F(TxnStatusManagerTest, TestUpdateStateConcurrently) {
  const int kNumTransactions = 10;
  const int kNumUpdatesInParallel = 20;
  for (int i = 0; i < kNumTransactions; i++) {
    TabletServerErrorPB ts_error;
    ASSERT_OK(txn_manager_->BeginTransaction(i, kOwner, &ts_error));
  }
  typedef std::pair<int64_t, TxnStatePB> IdAndUpdate;
  vector<IdAndUpdate> all_updates;
  for (int i = 0; i < kNumTransactions; i++) {
    all_updates.emplace_back(std::make_pair(i, TxnStatePB::ABORTED));
    all_updates.emplace_back(std::make_pair(i, TxnStatePB::COMMIT_IN_PROGRESS));
    all_updates.emplace_back(std::make_pair(i, TxnStatePB::COMMITTED));
  }
  ThreadSafeRandom rng(SeedRandom());
  vector<IdAndUpdate> updates;
  ReservoirSample(all_updates, kNumUpdatesInParallel, std::set<IdAndUpdate>(), &rng, &updates);
  vector<Status> statuses(kNumUpdatesInParallel);
  vector<thread> threads;
  threads.reserve(kNumUpdatesInParallel);
  // Start a bunch of threads that update transaction states.
  for (int i = 0; i < kNumUpdatesInParallel; i++) {
    threads.emplace_back([&, i] {
      const auto& txn_id = updates[i].first;
      TabletServerErrorPB ts_error;
      switch (updates[i].second) {
        case TxnStatePB::ABORTED:
          statuses[i] = txn_manager_->AbortTransaction(txn_id, kOwner, &ts_error);
          break;
        case TxnStatePB::COMMIT_IN_PROGRESS:
          statuses[i] = txn_manager_->BeginCommitTransaction(txn_id, kOwner, &ts_error);
          break;
        case TxnStatePB::COMMITTED:
          statuses[i] = txn_manager_->FinalizeCommitTransaction(txn_id);
          break;
        default:
          FAIL() << "bad update";
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  // Collect the transaction IDs per successful update.
  unordered_set<int64_t> txns_with_abort;
  unordered_set<int64_t> txns_with_begin_commit;
  unordered_set<int64_t> txns_with_finalize_commit;
  for (int i = 0; i < kNumUpdatesInParallel; i++) {
    const auto& txn_id = updates[i].first;
    if (!statuses[i].ok()) {
      continue;
    }
    switch (updates[i].second) {
      case TxnStatePB::ABORTED:
        EmplaceIfNotPresent(&txns_with_abort, txn_id);
        break;
      case TxnStatePB::COMMIT_IN_PROGRESS:
        EmplaceIfNotPresent(&txns_with_begin_commit, txn_id);
        break;
      case TxnStatePB::COMMITTED:
        EmplaceIfNotPresent(&txns_with_finalize_commit, txn_id);
        break;
      default:
        FAIL() << "bad update";
    }
  }
  for (int i = 0; i < kNumTransactions; i++) {
    // If there's a finalize commit and an abort commit, only one can succeed.
    if (ContainsKey(txns_with_abort, i)) {
      ASSERT_FALSE(ContainsKey(txns_with_finalize_commit, i));
    }
    // If there's a finalize commit, it can only succeed if there's also been a
    // successful request to begin the commit.
    if (ContainsKey(txns_with_finalize_commit, i)) {
      ASSERT_TRUE(ContainsKey(txns_with_begin_commit, i));
      ASSERT_FALSE(ContainsKey(txns_with_abort, i));
    }
  }
}

// This test scenario verifies basic functionality of the
// TxnStatusManager::GetTransactionStatus() method.
TEST_F(TxnStatusManagerTest, GetTransactionStatus) {
  {
    TabletServerErrorPB ts_error;
    ASSERT_OK(txn_manager_->BeginTransaction(1, kOwner, &ts_error));

    TxnStatusEntryPB txn_status;
    ASSERT_OK(txn_manager_->GetTransactionStatus(1, kOwner, &txn_status));
    ASSERT_TRUE(txn_status.has_state());
    ASSERT_EQ(TxnStatePB::OPEN, txn_status.state());
    ASSERT_TRUE(txn_status.has_user());
    ASSERT_EQ(kOwner, txn_status.user());

    ASSERT_OK(txn_manager_->BeginCommitTransaction(1, kOwner, &ts_error));
    ASSERT_OK(txn_manager_->GetTransactionStatus(1, kOwner, &txn_status));
    ASSERT_TRUE(txn_status.has_state());
    ASSERT_EQ(TxnStatePB::COMMIT_IN_PROGRESS, txn_status.state());
    ASSERT_TRUE(txn_status.has_user());
    ASSERT_EQ(kOwner, txn_status.user());

    ASSERT_OK(txn_manager_->FinalizeCommitTransaction(1));
    ASSERT_OK(txn_manager_->GetTransactionStatus(1, kOwner, &txn_status));
    ASSERT_TRUE(txn_status.has_state());
    ASSERT_EQ(TxnStatePB::COMMITTED, txn_status.state());
    ASSERT_TRUE(txn_status.has_user());
    ASSERT_EQ(kOwner, txn_status.user());
  }

  {
    TabletServerErrorPB ts_error;
    ASSERT_OK(txn_manager_->BeginTransaction(2, kOwner, &ts_error));
    ASSERT_OK(txn_manager_->AbortTransaction(2, kOwner, &ts_error));

    TxnStatusEntryPB txn_status;
    ASSERT_OK(txn_manager_->GetTransactionStatus(2, kOwner, &txn_status));
    ASSERT_TRUE(txn_status.has_state());
    ASSERT_EQ(TxnStatePB::ABORTED, txn_status.state());
    ASSERT_TRUE(txn_status.has_user());
    ASSERT_EQ(kOwner, txn_status.user());
  }

  // Start another transaction and start its commit phase.
  TabletServerErrorPB ts_error;
  ASSERT_OK(txn_manager_->BeginTransaction(3, kOwner, &ts_error));
  ASSERT_OK(txn_manager_->BeginCommitTransaction(3, kOwner, &ts_error));

  // Start just another transaction.
  ASSERT_OK(txn_manager_->BeginTransaction(4, kOwner, &ts_error));

  // Make the TxnStatusManager start from scratch.
  ASSERT_OK(RestartReplica());

  // Committed, aborted, and in-flight transactions should be known to the
  // TxnStatusManager even after restarting the underlying replica and
  // rebuilding the TxnStatusManager from scratch.
  {
    TxnStatusEntryPB txn_status;
    ASSERT_OK(txn_manager_->GetTransactionStatus(1, kOwner, &txn_status));
    ASSERT_TRUE(txn_status.has_state());
    ASSERT_EQ(TxnStatePB::COMMITTED, txn_status.state());
    ASSERT_TRUE(txn_status.has_user());
    ASSERT_EQ(kOwner, txn_status.user());

    ASSERT_OK(txn_manager_->GetTransactionStatus(2, kOwner, &txn_status));
    ASSERT_TRUE(txn_status.has_state());
    ASSERT_EQ(TxnStatePB::ABORTED, txn_status.state());
    ASSERT_TRUE(txn_status.has_user());
    ASSERT_EQ(kOwner, txn_status.user());

    ASSERT_OK(txn_manager_->GetTransactionStatus(3, kOwner, &txn_status));
    ASSERT_TRUE(txn_status.has_state());
    ASSERT_EQ(TxnStatePB::COMMIT_IN_PROGRESS, txn_status.state());
    ASSERT_TRUE(txn_status.has_user());
    ASSERT_EQ(kOwner, txn_status.user());

    ASSERT_OK(txn_manager_->GetTransactionStatus(4, kOwner, &txn_status));
    ASSERT_TRUE(txn_status.has_state());
    ASSERT_EQ(TxnStatePB::OPEN, txn_status.state());
    ASSERT_TRUE(txn_status.has_user());
    ASSERT_EQ(kOwner, txn_status.user());
  }

  // Supplying wrong user.
  {
    TxnStatusEntryPB txn_status;
    auto s = txn_manager_->GetTransactionStatus(1, "stranger", &txn_status);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  }

  // Supplying not-yet-used transaction ID.
  {
    TxnStatusEntryPB txn_status;
    auto s = txn_manager_->GetTransactionStatus(0, kOwner, &txn_status);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }

  // Supplying wrong user and not-yet-used transaction ID.
  {
    TxnStatusEntryPB txn_status;
    auto s = txn_manager_->GetTransactionStatus(0, "stranger", &txn_status);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }
}

// Test that performing actions as the wrong user will return errors.
TEST_F(TxnStatusManagerTest, TestWrongUser) {
  const string kWrongUser = "stranger";
  TabletServerErrorPB ts_error;
  ASSERT_OK(txn_manager_->BeginTransaction(1, kOwner, &ts_error));
  ASSERT_OK(txn_manager_->RegisterParticipant(1, ParticipantId(1), kOwner, &ts_error));

  // First, any other call to begin the transaction should be rejected,
  // regardless of user.
  Status s = txn_manager_->BeginTransaction(1, kWrongUser, &ts_error);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();

  // All actions should be rejected if performed by the wrong user.
  s = txn_manager_->RegisterParticipant(1, ParticipantId(1), kWrongUser, &ts_error);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  s = txn_manager_->RegisterParticipant(1, ParticipantId(2), kWrongUser, &ts_error);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  s = txn_manager_->BeginCommitTransaction(1, kWrongUser, &ts_error);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  s = txn_manager_->AbortTransaction(1, kWrongUser, &ts_error);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ParticipantIdsByTxnId prts_by_txn_id = txn_manager_->GetParticipantsByTxnIdForTests();
  ParticipantIdsByTxnId kExpectedPrtsByTxnId = { { 1, { ParticipantId(1) } } };
  ASSERT_EQ(kExpectedPrtsByTxnId, prts_by_txn_id);
}

// Test that we can only update a transaction's state when it's in an
// appropriate state.
TEST_F(TxnStatusManagerTest, TestUpdateTransactionState) {
  const int64_t kTxnId1 = 1;
  TabletServerErrorPB ts_error;
  ASSERT_OK(txn_manager_->BeginTransaction(kTxnId1, kOwner, &ts_error));

  // Redundant calls are benign.
  ASSERT_OK(txn_manager_->BeginCommitTransaction(kTxnId1, kOwner, &ts_error));
  ASSERT_OK(txn_manager_->BeginCommitTransaction(kTxnId1, kOwner, &ts_error));
  ASSERT_OK(txn_manager_->AbortTransaction(kTxnId1, kOwner, &ts_error));
  ASSERT_OK(txn_manager_->AbortTransaction(kTxnId1, kOwner, &ts_error));

  // We can't begin or finalize a commit if we've aborted.
  Status s = txn_manager_->BeginCommitTransaction(kTxnId1, kOwner, &ts_error);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  s = txn_manager_->FinalizeCommitTransaction(kTxnId1);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();

  // We can't finalize a commit that hasn't begun committing.
  const int64_t kTxnId2 = 2;
  ASSERT_OK(txn_manager_->BeginTransaction(kTxnId2, kOwner, &ts_error));
  s = txn_manager_->FinalizeCommitTransaction(kTxnId2);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();

  // We can't abort a transaction that has finished committing.
  ASSERT_OK(txn_manager_->BeginCommitTransaction(kTxnId2, kOwner, &ts_error));
  ASSERT_OK(txn_manager_->FinalizeCommitTransaction(kTxnId2));
  s = txn_manager_->AbortTransaction(kTxnId2, kOwner, &ts_error);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();

  // Redundant finalize calls are also benign.
  ASSERT_OK(txn_manager_->FinalizeCommitTransaction(kTxnId2));

  // Calls to begin committing should return an error if we've already
  // finalized the commit.
  s = txn_manager_->BeginCommitTransaction(kTxnId2, kOwner, &ts_error);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
}

// Test that we can only add participants to a transaction when it's in an
// appropriate state.
TEST_F(TxnStatusManagerTest, TestRegisterParticipantsWithStates) {
  TabletServerErrorPB ts_error;
  const int64_t kTxnId1 = 1;

  // We can't register a participant to a transaction that hasn't started.
  Status s = txn_manager_->RegisterParticipant(kTxnId1, ParticipantId(1), kOwner, &ts_error);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  ASSERT_OK(txn_manager_->BeginTransaction(kTxnId1, kOwner, &ts_error));
  ASSERT_OK(txn_manager_->RegisterParticipant(kTxnId1, ParticipantId(1), kOwner, &ts_error));

  // Registering the same participant is idempotent and benign.
  ASSERT_OK(txn_manager_->RegisterParticipant(kTxnId1, ParticipantId(1), kOwner, &ts_error));

  // We can't register participants when we've already begun committing.
  ASSERT_OK(txn_manager_->BeginCommitTransaction(kTxnId1, kOwner, &ts_error));
  s = txn_manager_->RegisterParticipant(kTxnId1, ParticipantId(2), kOwner, &ts_error);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();

  // We can't register participants when we've finished committnig.
  ASSERT_OK(txn_manager_->FinalizeCommitTransaction(kTxnId1));
  s = txn_manager_->RegisterParticipant(kTxnId1, ParticipantId(2), kOwner, &ts_error);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();

  // We can't register participants when we've aborted the transaction.
  const int64_t kTxnId2 = 2;
  ASSERT_OK(txn_manager_->BeginTransaction(kTxnId2, kOwner, &ts_error));
  ASSERT_OK(txn_manager_->RegisterParticipant(kTxnId2, ParticipantId(1), kOwner, &ts_error));
  ASSERT_OK(txn_manager_->AbortTransaction(kTxnId2, kOwner, &ts_error));
  s = txn_manager_->RegisterParticipant(kTxnId2, ParticipantId(2), kOwner, &ts_error);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
}

} // namespace transactions
} // namespace kudu

