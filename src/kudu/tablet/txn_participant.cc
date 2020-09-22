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
#include <ostream>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/common/timestamp.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/txn_metadata.h"

using kudu::log::LogAnchorRegistry;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

Txn::~Txn() {
  CHECK_OK(log_anchor_registry_->UnregisterIfAnchored(&begin_commit_anchor_));
  // As a sanity check, make sure our state makes sense: if we have an MVCC op
  // for commit, we should have started to commit.
  if (commit_op_) {
    DCHECK(state_ == kCommitInProgress ||
           state_ == kCommitted ||
           state_ == kAborted) << StateToString(state_);
  }
}

void Txn::AcquireWriteLock(std::unique_lock<rw_semaphore>* txn_lock) {
  std::unique_lock<rw_semaphore> l(state_lock_);
  *txn_lock = std::move(l);
}

void TxnParticipant::CreateOpenTransaction(int64_t txn_id,
                                           LogAnchorRegistry* log_anchor_registry) {
  std::lock_guard<simple_spinlock> l(lock_);
  EmplaceOrDie(&txns_, txn_id, new Txn(txn_id, log_anchor_registry,
                                       tablet_metadata_, Txn::kOpen));
}

scoped_refptr<Txn> TxnParticipant::GetOrCreateTransaction(int64_t txn_id,
                                                          LogAnchorRegistry* log_anchor_registry) {
  // TODO(awong): add a 'user' field to these transactions.
  std::lock_guard<simple_spinlock> l(lock_);
  return LookupOrInsertNewSharedPtr(&txns_, txn_id, txn_id, log_anchor_registry,
                                    tablet_metadata_);
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

bool TxnParticipant::ClearIfComplete(int64_t txn_id) {
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
  // Include any transactions that are in a terminal state by first updating
  // any now-terminal transactions that we already created entries for above.
  auto txn_metas = tablet_metadata_->GetTxnMetadata();
  for (auto& txn_entry : txns) {
    auto* txn_meta = FindPointeeOrNull(txn_metas, txn_entry.txn_id);
    if (txn_meta) {
      txn_metas.erase(txn_entry.txn_id);
      if (txn_meta->aborted()) {
        txn_entry.state = Txn::kAborted;
        txn_entry.commit_timestamp = -1;
        continue;
      }
      const auto& commit_ts = txn_meta->commit_timestamp();
      if (commit_ts) {
        txn_entry.state = Txn::kCommitted;
        txn_entry.commit_timestamp = commit_ts->value();
        continue;
      }
    }
  }
  // Create entries for the rest of the terminal transactions.
  for (const auto& id_and_meta : txn_metas) {
    const auto& txn_id = id_and_meta.first;
    const auto& txn_meta = id_and_meta.second;
    TxnEntry txn_entry;
    txn_entry.txn_id = txn_id;
    if (txn_meta->aborted()) {
      txn_entry.state = Txn::kAborted;
      txn_entry.commit_timestamp = -1;
      txns.emplace_back(std::move(txn_entry));
      continue;
    }
    const auto& commit_ts = txn_meta->commit_timestamp();
    if (commit_ts) {
      txn_entry.state = Txn::kCommitted;
      txn_entry.commit_timestamp = commit_ts->value();
      txns.emplace_back(txn_entry);
      continue;
    }
  }
  std::sort(txns.begin(), txns.end(),
      [] (const TxnEntry& a, const TxnEntry& b) { return a.txn_id < b.txn_id; });
  return txns;
}

} // namespace tablet
} // namespace kudu
