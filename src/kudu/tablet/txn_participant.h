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
#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/locks.h"
#include "kudu/util/rw_semaphore.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tablet {

// Tracks the state associated with a transaction.
//
// This class will primarily be accessed via op drivers. As such, locking
// primitives are exposed publicly, to be called in different stages of
// replication.
class Txn : public RefCountedThreadSafe<Txn> {
 public:
  enum State {
    // Each transaction starts in this state. While in this state, the
    // transaction is not yet ready to be used, e.g. the initial op to begin
    // the transaction may not have successfully replicated yet.
    kInitializing,

    // Each transaction is moved into this state once they are ready to begin
    // accepting ops.
    kOpen,

    // A transaction is moved into this state when a client has signified the
    // intent to begin committing it. While in this state, the transaction may
    // not accept new ops.
    kCommitInProgress,

    // A transaction is moved into this state when it becomes finalized -- all
    // participants have acknowledged the intent to commit and have guaranteed
    // that all ops in the transaction will succeed. While in this state, the
    // transaction may not accept new ops and may not be aborted.
    kCommitted,

    // A transaction is moved into this state when a client has signified
    // intent to cancel the transaction. While in this state, the transaction
    // may not accept new ops, begin committing, or finalize a commit.
    kAborted,
  };
  static const char* StateToString(State s) {
    switch (s) {
      case kInitializing: return "INITIALIZING";
      case kOpen: return "OPEN";
      case kCommitInProgress: return "COMMIT_IN_PROGRESS";
      case kCommitted: return "COMMITTED";
      case kAborted: return "ABORTED";
    }
    __builtin_unreachable();
  }

  Txn() : state_(kInitializing), commit_timestamp_(-1) {}

  // Takes the state lock in write mode and returns it. As transaction state is
  // meant to be driven via an op driver, lock acquisition is expected to be
  // serialized in a single thread.
  void AcquireWriteLock(std::unique_lock<rw_semaphore>* txn_lock);

  // Validates that the transaction is in the appropriate state to perform the
  // given operation. Should be called while holding the state lock before
  // replicating a participant op.
  Status ValidateBeginTransaction() const {
    DCHECK(state_lock_.is_locked());
    if (PREDICT_FALSE(state_ != kInitializing)) {
      return Status::IllegalState(
          strings::Substitute("Cannot begin transaction in state: $0",
                              StateToString(state_)));
    }
    return Status::OK();
  }
  Status ValidateBeginCommit() const {
    DCHECK(state_lock_.is_locked());
    RETURN_NOT_OK(CheckFinishedInitializing());
    if (PREDICT_FALSE(state_ != kOpen)) {
      return Status::IllegalState(
          strings::Substitute("Cannot begin committing transaction in state: $0",
                              StateToString(state_)));
    }
    return Status::OK();
  }
  Status ValidateFinalize() const {
    DCHECK(state_lock_.is_locked());
    RETURN_NOT_OK(CheckFinishedInitializing());
    if (PREDICT_FALSE(state_ != kCommitInProgress)) {
      return Status::IllegalState(
          strings::Substitute("Cannot finalize transaction in state: $0",
                              StateToString(state_)));
    }
    return Status::OK();
  }
  Status ValidateAbort() const {
    DCHECK(state_lock_.is_locked());
    RETURN_NOT_OK(CheckFinishedInitializing());
    if (PREDICT_FALSE(state_ != kOpen &&
                      state_ != kCommitInProgress)) {
      return Status::IllegalState(
          strings::Substitute("Cannot abort transaction in state: $0",
                              StateToString(state_)));
    }
    return Status::OK();
  }

  // Applies the given state transitions. Should be called while holding the
  // state lock in write mode after successfully replicating a participant op.
  void BeginTransaction() {
    SetState(kOpen);
  }
  void BeginCommit() {
    SetState(kCommitInProgress);
  }
  void FinalizeCommit(int64_t finalized_commit_timestamp) {
    SetState(kCommitted);
    commit_timestamp_ = finalized_commit_timestamp;
  }
  void AbortTransaction() {
    SetState(kAborted);
  }

  // Simple accessors for state. No locks are required to call these.
  State state() const {
    return state_;
  }
  int64_t commit_timestamp() const {
    return commit_timestamp_;
  }

 private:
  friend class RefCountedThreadSafe<Txn>;

  // Sets the transaction state.
  void SetState(State s) {
    DCHECK(state_lock_.is_write_locked());
    state_ = s;
  }

  // Returns an error if the transaction has not finished initializing.
  Status CheckFinishedInitializing() const {
    if (PREDICT_FALSE(state_ == kInitializing)) {
      return Status::NotFound("Transaction hasn't been successfully started");
    }
    return Status::OK();
  }

  // Lock protecting access to 'state_' and 'commit_timestamp'. Ops that intend
  // on mutating 'state_' must take this lock in write mode. Ops that intend on
  // reading 'state_' and relying on it remaining constant must take this lock
  // in read mode.
  mutable rw_semaphore state_lock_;
  std::atomic<State> state_;

  // If this transaction was successfully committed, the timestamp at which the
  // transaction should be applied, and -1 otherwise.
  std::atomic<int64_t> commit_timestamp_;

  DISALLOW_COPY_AND_ASSIGN(Txn);
};

// Tracks the on-going transactions in which a given tablet is participating.
class TxnParticipant {
 public:
  // Convenience struct representing a Txn of this participant. This is useful
  // for testing, as it easy to construct.
  struct TxnEntry {
    int64_t txn_id;
    Txn::State state;
    int64_t commit_timestamp;
  };

  // Gets the transaction state for the given transaction ID, creating it in
  // the kInitializing state if one doesn't already exist.
  scoped_refptr<Txn> GetOrCreateTransaction(int64_t txn_id);

  // Removes the given transaction if it failed to initialize, e.g. the op that
  // created it failed to replicate, leaving it in the kInitializing state but
  // with no op actively mutating it.
  //
  // It is expected that the caller, e.g. a ParticipantOp, has released any Txn
  // references before calling this, ensuring that when we check the state of
  // the Txn, we can thread-safely determine whether it has been abandoned.
  void ClearIfInitFailed(int64_t txn_id);

  // Returns the transactions, sorted by transaction ID.
  std::vector<TxnEntry> GetTxnsForTests() const;

 private:
  // Protects insertions and removals from 'txns_'.
  mutable simple_spinlock lock_;

  // Maps from transaction ID to the corresponding transaction state.
  std::unordered_map<int64_t, scoped_refptr<Txn>> txns_;
};

inline bool operator==(const TxnParticipant::TxnEntry& lhs, const TxnParticipant::TxnEntry& rhs) {
  return lhs.txn_id == rhs.txn_id &&
      lhs.state == rhs.state &&
      lhs.commit_timestamp == rhs.commit_timestamp;
}

} // namespace tablet
} // namespace kudu
