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

#include "kudu/transactions/txn_status_tablet.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet_replica-test-base.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using kudu::consensus::ConsensusBootstrapInfo;
using kudu::pb_util::SecureShortDebugString;
using kudu::tablet::TabletReplicaTestBase;
using std::ostream;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace transactions {

namespace {

const char kOwner[] = "bojack";
const char kParticipant[] = "peanutbutter";
string ParticipantId(int i) {
  return Substitute("$0$1", kParticipant, i);
}

// Simple representation of an entry in the transaction status tablet.
struct SimpleEntry {
  int64_t txn_id;
  TxnStatusEntryPB txn_pb;
  vector<ParticipantIdAndPB> prt_pbs;

  // Convenience method to create a SimpleEntry.
  static SimpleEntry Create(int64_t txn_id, const string& user, TxnStatePB txn_state_pb,
                            vector<std::pair<string, TxnStatePB>> participants) {
    TxnStatusEntryPB txn_pb;
    txn_pb.set_state(txn_state_pb);
    txn_pb.set_user(user);
    vector<ParticipantIdAndPB> prt_pbs;
    for (auto& id_and_state : participants) {
      TxnParticipantEntryPB prt_pb;
      prt_pb.set_state(id_and_state.second);
      prt_pbs.emplace_back(std::make_pair(std::move(id_and_state.first), std::move(prt_pb)));
    }
    return { txn_id, std::move(txn_pb), std::move(prt_pbs) };
  }

  bool operator==(const SimpleEntry& other) const {
    return ToString() == other.ToString();
  }

  friend ostream& operator<<(ostream& out, const SimpleEntry& e) {
    out << e.ToString();
    return out;
  }

  string ToString() const {
    vector<string> prt_strs;
    for (const auto& id_and_prt : prt_pbs) {
      prt_strs.emplace_back(Substitute("($0, {$1})", id_and_prt.first,
                            SecureShortDebugString(id_and_prt.second)));
    }
    return Substitute("($0, {$1}, [$2])", txn_id,
        SecureShortDebugString(txn_pb), JoinStrings(prt_strs, ","));
  }
};

class SimpleTransactionsVisitor : public TransactionsVisitor {
 public:
  void VisitTransactionEntries(int64_t txn_id, TxnStatusEntryPB status_entry_pb,
                                 vector<ParticipantIdAndPB> participants) override {
    entries_.emplace_back(SimpleEntry{ txn_id, std::move(status_entry_pb),
                                       std::move(participants) });
  }
  vector<SimpleEntry> ReleaseEntries() {
    return std::move(entries_);
  }
 private:
  vector<SimpleEntry> entries_;
};

} // anonymous namespace

class TxnStatusTabletTest : public TabletReplicaTestBase {
 public:
  TxnStatusTabletTest()
      : TabletReplicaTestBase(TxnStatusTablet::GetSchemaWithoutIds()) {}

  void SetUp() override {
    NO_FATALS(TabletReplicaTestBase::SetUp());
    ConsensusBootstrapInfo info;
    ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
    status_tablet_.reset(new TxnStatusTablet(tablet_replica_.get()));
  }

 protected:
  unique_ptr<TxnStatusTablet> status_tablet_;
};

TEST_F(TxnStatusTabletTest, TestWriteTransactions) {
  // We can make multiple calls to add a single transaction. This will only
  // insert a single row to the table.
  ASSERT_OK(status_tablet_->AddNewTransaction(1, kOwner));
  ASSERT_OK(status_tablet_->AddNewTransaction(1, kOwner));

  // The storage abstraction doesn't prevent us from writing a new transaction
  // entry for a lower transaction ID.
  ASSERT_OK(status_tablet_->AddNewTransaction(5, kOwner));
  ASSERT_OK(status_tablet_->AddNewTransaction(2, kOwner));

  // Also try updating the status of one of our transaction entries.
  TxnStatusEntryPB status_entry_pb;
  status_entry_pb.set_user(kOwner);
  status_entry_pb.set_state(TxnStatePB::ABORTED);
  ASSERT_OK(status_tablet_->UpdateTransaction(2, status_entry_pb));
  status_entry_pb.set_state(TxnStatePB::COMMITTED);
  ASSERT_OK(status_tablet_->UpdateTransaction(2, status_entry_pb));

  // The stored entries should be sorted, de-duplicated, and have the latest
  // values.
  const vector<SimpleEntry> kExpectedEntries({
      SimpleEntry::Create(1, kOwner, TxnStatePB::OPEN, {}),
      SimpleEntry::Create(2, kOwner, TxnStatePB::COMMITTED, {}),
      SimpleEntry::Create(5, kOwner, TxnStatePB::OPEN, {}),
  });

  // Now iterate through the entries.
  SimpleTransactionsVisitor visitor;
  ASSERT_OK(status_tablet_->VisitTransactions(&visitor));
  vector<SimpleEntry> entries = visitor.ReleaseEntries();
  EXPECT_EQ(kExpectedEntries, entries);
}

TEST_F(TxnStatusTabletTest, TestWriteParticipants) {
  ASSERT_OK(status_tablet_->AddNewTransaction(1, kOwner));

  // Participants will be de-duplicated.
  ASSERT_OK(status_tablet_->AddNewParticipant(1, ParticipantId(1)));
  ASSERT_OK(status_tablet_->AddNewParticipant(1, ParticipantId(1)));

  // There aren't ordering constraints for registering participant IDs.
  ASSERT_OK(status_tablet_->AddNewParticipant(1, ParticipantId(5)));
  ASSERT_OK(status_tablet_->AddNewParticipant(1, ParticipantId(2)));

  // Try updating the status of one of our participant entries.
  TxnParticipantEntryPB prt_entry_pb;
  prt_entry_pb.set_state(TxnStatePB::ABORTED);
  ASSERT_OK(status_tablet_->UpdateParticipant(1, ParticipantId(2), prt_entry_pb));
  prt_entry_pb.set_state(TxnStatePB::COMMITTED);
  ASSERT_OK(status_tablet_->UpdateParticipant(1, ParticipantId(2), prt_entry_pb));

  const vector<SimpleEntry> kExpectedEntries({
      SimpleEntry::Create(1, kOwner, TxnStatePB::OPEN, {
          { ParticipantId(1), TxnStatePB::OPEN },
          { ParticipantId(2), TxnStatePB::COMMITTED },
          { ParticipantId(5), TxnStatePB::OPEN },
      }),
  });
  SimpleTransactionsVisitor visitor;
  ASSERT_OK(status_tablet_->VisitTransactions(&visitor));
  vector<SimpleEntry> entries = visitor.ReleaseEntries();
  EXPECT_EQ(kExpectedEntries, entries);
}

// Test that a participant entry can't be visited without a corresponding
// status entry.
TEST_F(TxnStatusTabletTest, TestFailedVisitor) {
  ASSERT_OK(status_tablet_->AddNewParticipant(1, ParticipantId(1)));
  SimpleTransactionsVisitor visitor;
  Status s = status_tablet_->VisitTransactions(&visitor);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "missing transaction status entry");

  // Now try again but with the transaction ID written.
  ASSERT_OK(status_tablet_->AddNewTransaction(1, kOwner));
  ASSERT_OK(status_tablet_->VisitTransactions(&visitor));

  // And again with a new transaction ID.
  ASSERT_OK(status_tablet_->AddNewParticipant(2, ParticipantId(2)));
  s = status_tablet_->VisitTransactions(&visitor);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "missing transaction status entry");
}

// Test that we can write in parallel and read in parallel from the transaction
// storage tablet.
TEST_F(TxnStatusTabletTest, TestMultithreadedAccess) {
  const int kNumThreads = 10;
  const int kNumParticipantsPerTransaction = 5;
  vector<thread> threads;
  vector<Status> statuses(kNumThreads);
#define RET_IF_NOT_OK(s) do { \
    Status _s = (s); \
    if (!_s.ok()) { \
      statuses[i] = _s; \
      return; \
    } \
  } while (0)

  // Start multiple threads that add a transaction and a bunch of participants,
  // storing any errors we see.
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, i] {
      RET_IF_NOT_OK(status_tablet_->AddNewTransaction(i, kOwner));
      for (int p = 0; p < kNumParticipantsPerTransaction; p++) {
        RET_IF_NOT_OK(status_tablet_->AddNewParticipant(i, Substitute("prt-$0", p)));
      }
    });
  }
  std::for_each(threads.begin(), threads.end(), [] (thread& t) { t.join(); });
  // There should have been no issues inserting.
  for (const auto& s : statuses) {
    EXPECT_OK(s);
  }
  threads.clear();

  // Now try visiting the transaction status tablet from multiple threads,
  // verifying we get back the correct number of transactions and participants.
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, i] {
      SimpleTransactionsVisitor visitor;
      RET_IF_NOT_OK(status_tablet_->VisitTransactions(&visitor));
      Status s;
      const auto entries = visitor.ReleaseEntries();
      if (entries.size() != kNumThreads) {
        RET_IF_NOT_OK(Status::IllegalState(Substitute("got $0 transactions", entries.size())));
      }
      for (const auto& e : entries) {
        if (e.prt_pbs.size() != kNumParticipantsPerTransaction) {
          RET_IF_NOT_OK(Status::IllegalState(Substitute("txn $0 had $1 participants",
                                             e.txn_id, e.prt_pbs.size())));
        }
      }
    });
  }
  std::for_each(threads.begin(), threads.end(), [] (thread& t) { t.join(); });
  for (const auto& s : statuses) {
    EXPECT_OK(s);
  }
}

} // namespace transactions
} // namespace kudu
