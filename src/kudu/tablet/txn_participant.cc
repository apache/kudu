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

#include "kudu/tablet/txn_participant.h"

#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"

using kudu::log::LogAnchorRegistry;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

void Txn::AcquireWriteLock(std::unique_lock<rw_semaphore>* txn_lock) {
  std::unique_lock<rw_semaphore> l(state_lock_);
  *txn_lock = std::move(l);
}

scoped_refptr<Txn> TxnParticipant::GetOrCreateTransaction(int64_t txn_id,
                                                          LogAnchorRegistry* log_anchor_registry) {
  // TODO(awong): add a 'user' field to these transactions.
  std::lock_guard<simple_spinlock> l(lock_);
  return LookupOrInsertNewSharedPtr(&txns_, txn_id, txn_id, log_anchor_registry);
}

void TxnParticipant::ClearIfInitFailed(int64_t txn_id) {
  std::lock_guard<simple_spinlock> l(lock_);
  Txn* txn = FindPointeeOrNull(txns_, txn_id);
  // NOTE: If this is the only reference to the transaction, we can forego
  // locking the state.
  if (txn && txn->HasOneRef() && txn->state() == Txn::kInitializing) {
    txns_.erase(txn_id);
  }
}

bool TxnParticipant::ClearIfCompleteForTests(int64_t txn_id) {
  std::lock_guard<simple_spinlock> l(lock_);
  Txn* txn = FindPointeeOrNull(txns_, txn_id);
  // NOTE: If this is the only reference to the transaction, we can forego
  // locking the state.
  if (txn && txn->HasOneRef() &&
      (txn->state() == Txn::kAborted ||
       txn->state() == Txn::kCommitted)) {
    txns_.erase(txn_id);
    return true;
  }
  return false;
}

vector<TxnParticipant::TxnEntry> TxnParticipant::GetTxnsForTests() const {
  vector<TxnEntry> txns;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    for (const auto& txn_id_and_scoped_txn : txns_) {
      const auto& scoped_txn = txn_id_and_scoped_txn.second;
      txns.emplace_back(TxnEntry{
        txn_id_and_scoped_txn.first,
        scoped_txn->state(),
        scoped_txn->commit_timestamp(),
      });
    }
  }
  std::sort(txns.begin(), txns.end(),
      [] (const TxnEntry& a, const TxnEntry& b) { return a.txn_id < b.txn_id; });
  return txns;
}

} // namespace tablet
} // namespace kudu
