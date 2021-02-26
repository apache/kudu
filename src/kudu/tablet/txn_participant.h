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
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <boost/optional/optional.hpp>

#include "kudu/common/timestamp.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/rw_semaphore.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tablet {

// NOTE: we define the type explicitly so we can forward declare this enum.
enum TxnState : int8_t {

  // Not a real state; useful for representing optionality.
  kNone,

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

const char* TxnStateToString(TxnState s);

// Tracks the state associated with a transaction.
//
// This class will primarily be accessed via op drivers. As such, locking
// primitives are exposed publicly, to be called in different stages of
// replication.
class Txn : public RefCountedThreadSafe<Txn> {
 public:

  // Constructs a transaction instance with the given transaction ID and WAL
  // anchor registry.
  //
  // The WAL anchor registry is used to ensure that the WAL segment that
  // contains the participant op that replays to the transaction's current
  // in-memory state is not GCed, allowing us to rebuild this transaction's
  // in-memory state upon rebooting a server.
  Txn(int64_t txn_id, log::LogAnchorRegistry* log_anchor_registry,
      scoped_refptr<TabletMetadata> tablet_metadata, TxnState init_state = kInitializing)
      : txn_id_(txn_id),
        log_anchor_registry_(log_anchor_registry),
        tablet_metadata_(std::move(tablet_metadata)),
        state_(init_state),
        commit_timestamp_(-1) {}
  ~Txn();

  // Takes the state lock and returns it. As transaction state is meant to be
  // driven via an op driver, lock acquisition is expected to be serialized in
  // a single thread.
  void AcquireWriteLock(std::unique_lock<rw_semaphore>* txn_lock);
  void AcquireReadLock(shared_lock<rw_semaphore>* txn_lock);

  // Validates that the transaction is in the appropriate state to perform the
  // given operation. Should be called while holding the state lock before
  // replicating a participant op.
  Status ValidateBeginTransaction(tserver::TabletServerErrorPB::Code* code) const {
    DCHECK(state_lock_.is_locked());
    if (PREDICT_FALSE(state_ == kOpen)) {
      *code = tserver::TabletServerErrorPB::TXN_OP_ALREADY_APPLIED;
      return Status::IllegalState(
          strings::Substitute("Transaction $0 already open", txn_id_));
    }
    if (PREDICT_FALSE(tablet_metadata_->HasTxnMetadata(txn_id_))) {
      *code = tserver::TabletServerErrorPB::TXN_ILLEGAL_STATE;
      return Status::IllegalState(
          strings::Substitute("Transaction metadata for transaction $0 already exists",
                              txn_id_));
    }
    if (PREDICT_FALSE(state_ != kInitializing)) {
      *code = tserver::TabletServerErrorPB::TXN_ILLEGAL_STATE;
      return Status::IllegalState(
          strings::Substitute("Cannot begin transaction in state: $0",
                              TxnStateToString(state_)));
    }
    return Status::OK();
  }
  Status ValidateBeginCommit(tserver::TabletServerErrorPB::Code* code,
                             Timestamp* begin_commit_ts) const {
    DCHECK(state_lock_.is_locked());
    boost::optional<Timestamp> already_applied_timestamp;
    if (PREDICT_FALSE(state_ == kInitializing)) {
      Timestamp timestamp;
      TxnState meta_state;
      if (!tablet_metadata_->HasTxnMetadata(txn_id_, &meta_state, &timestamp)) {
        return Status::IllegalState("Transaction hasn't been successfully started");
      }
      if (PREDICT_FALSE(meta_state != kCommitted && meta_state != kCommitInProgress)) {
        *code = tserver::TabletServerErrorPB::TXN_ILLEGAL_STATE;
        return Status::IllegalState(
            strings::Substitute("Cannot begin committing transaction in state: $0",
                                TxnStateToString(state_)));
      }
      // There's no in-flight transaction, but we've already committed the
      // transaction and persisted a commit timestamp. Return the commit
      // timestamp.
      already_applied_timestamp = timestamp;
    }
    // If we're in the process of committing, return the commit timestamp we
    // have available to us.
    if (PREDICT_FALSE(state_ == kCommitInProgress)) {
      already_applied_timestamp = DCHECK_NOTNULL(commit_op_)->timestamp();
    }
    if (PREDICT_FALSE(state_ == kCommitted)) {
      DCHECK_NE(-1, commit_timestamp_);
      already_applied_timestamp = Timestamp(commit_timestamp_);
    }
    if (already_applied_timestamp) {
      DCHECK_NE(Timestamp::kInvalidTimestamp, *already_applied_timestamp);
      *begin_commit_ts = *already_applied_timestamp;
      *code = tserver::TabletServerErrorPB::TXN_OP_ALREADY_APPLIED;
      return Status::IllegalState(
          strings::Substitute("Transaction $0 commit already in progress", txn_id_));
    }
    // If the transaction is otherwise not open, return an error.
    if (PREDICT_FALSE(state_ != kOpen)) {
      *code = tserver::TabletServerErrorPB::TXN_ILLEGAL_STATE;
      return Status::IllegalState(
          strings::Substitute("Cannot begin committing transaction in state: $0",
                              TxnStateToString(state_)));
    }
    return Status::OK();
  }
  Status ValidateFinalize(tserver::TabletServerErrorPB::Code* code) const {
    DCHECK(state_lock_.is_locked());
    RETURN_NOT_OK(CheckFinishedInitializing(code, kCommitted));
    if (PREDICT_FALSE(state_ == kCommitted)) {
      *code = tserver::TabletServerErrorPB::TXN_OP_ALREADY_APPLIED;
      return Status::IllegalState(
          strings::Substitute("Transaction $0 has already been committed", txn_id_));
    }
    if (PREDICT_FALSE(state_ != kCommitInProgress)) {
      *code = tserver::TabletServerErrorPB::TXN_ILLEGAL_STATE;
      return Status::IllegalState(
          strings::Substitute("Cannot finalize transaction in state: $0",
                              TxnStateToString(state_)));
    }
    return Status::OK();
  }
  Status ValidateAbort(tserver::TabletServerErrorPB::Code* code) const {
    DCHECK(state_lock_.is_locked());
    RETURN_NOT_OK(CheckFinishedInitializing(code, kAborted));
    if (PREDICT_FALSE(state_ == kAborted)) {
      *code = tserver::TabletServerErrorPB::TXN_OP_ALREADY_APPLIED;
      return Status::IllegalState(
          strings::Substitute("Transaction $0 has already been aborted", txn_id_));
    }
    if (PREDICT_FALSE(state_ != kOpen &&
                      state_ != kCommitInProgress)) {
      *code = tserver::TabletServerErrorPB::TXN_ILLEGAL_STATE;
      return Status::IllegalState(
          strings::Substitute("Cannot abort transaction in state: $0",
                              TxnStateToString(state_)));
    }
    return Status::OK();
  }

  // Applies the given state transitions. Should be called while holding the
  // state lock in write mode after successfully replicating a participant op.
  void BeginTransaction() {
    SetState(kOpen);
  }
  void BeginCommit(const consensus::OpId& op_id) {
    CHECK_OK(log_anchor_registry_->RegisterOrUpdate(
        op_id.index(), strings::Substitute("BEGIN_COMMIT-$0-$1", txn_id_, this),
        &begin_commit_anchor_));
    SetState(kCommitInProgress);
  }
  void FinalizeCommit(int64_t finalized_commit_timestamp) {
    commit_timestamp_ = finalized_commit_timestamp;
    SetState(kCommitted);
    log_anchor_registry_->UnregisterIfAnchored(&begin_commit_anchor_);
  }
  void AbortTransaction() {
    SetState(kAborted);
    log_anchor_registry_->UnregisterIfAnchored(&begin_commit_anchor_);
  }

  // Simple accessors for state. No locks are required to call these.
  TxnState state() const {
    return state_;
  }
  int64_t commit_timestamp() const {
    return commit_timestamp_;
  }
  int64_t txn_id() const {
    return txn_id_;
  }

  void SetCommitOp(std::unique_ptr<ScopedOp> commit_op) {
    DCHECK(nullptr == commit_op_.get());
    commit_op_ = std::move(commit_op);
  }

  ScopedOp* commit_op() {
    return commit_op_.get();
  }

 private:
  friend class RefCountedThreadSafe<Txn>;

  // Sets the transaction state.
  void SetState(TxnState s) {
    DCHECK(state_lock_.is_write_locked());
    state_ = s;
  }

  // Returns an error if the transaction has not finished initializing. This
  // may mean that an in-flight transaction didn't exist so we created a new
  // one, in which case we need to check if there's existing metadata for it.
  //
  // NOTE: we only need to check the expected metadata state when we're
  // aborting or finalizing a commit, since these end the in-flight
  // transaction. In other cases, we should be able to check the state of the
  // in-flight transaction.
  Status CheckFinishedInitializing(
      tserver::TabletServerErrorPB::Code* code,
      TxnState expected_metadata_state = kNone) const {
    DCHECK(expected_metadata_state == kNone ||
           expected_metadata_state == kAborted ||
           expected_metadata_state == kCommitted);
    if (PREDICT_FALSE(state_ == kInitializing)) {
      TxnState meta_state;
      if (tablet_metadata_->HasTxnMetadata(txn_id_, &meta_state)) {
        if (expected_metadata_state != kNone && expected_metadata_state == meta_state) {
          *code = tserver::TabletServerErrorPB::TXN_OP_ALREADY_APPLIED;
          return Status::IllegalState(
              strings::Substitute("Transaction metadata for transaction $0 already set",
                                  txn_id_));
        }
        // We created this transaction as a part of this op (i.e. it was not
        // already in flight), and there is existing metadata for it.
        *code = tserver::TabletServerErrorPB::TXN_ILLEGAL_STATE;
        return Status::IllegalState(
            strings::Substitute("Transaction metadata for transaction $0 already exists",
                                txn_id_));
      }
      // TODO(awong): add another code for this?
      return Status::IllegalState("Transaction hasn't been successfully started");
    }
    return Status::OK();
  }

  // Transaction ID for this transaction.
  const int64_t txn_id_;

  // Log anchor registry with which to anchor WAL segments, and an anchor to
  // update upon applying a state change.
  log::LogAnchorRegistry* log_anchor_registry_;

  // Anchor used to prevent GC of a BEGIN_COMMIT op.
  log::LogAnchor begin_commit_anchor_;

  // Tablet metadata used to persist this transaction's metadata.
  scoped_refptr<TabletMetadata> tablet_metadata_;

  // Lock protecting access to 'state_' and 'commit_timestamp'. Ops that intend
  // on mutating 'state_' must take this lock in write mode. Ops that intend on
  // reading 'state_' and relying on it remaining constant must take this lock
  // in read mode.
  mutable rw_semaphore state_lock_;
  std::atomic<TxnState> state_;

  // If this transaction was successfully committed, the timestamp at which the
  // transaction should be applied, and -1 otherwise.
  std::atomic<int64_t> commit_timestamp_;

  // Scoped op with a lifecycle that spans between the BEGIN_COMMIT op and
  // corresponding FINALIZE_COMMIT or ABORT_TXN op, used to ensure that
  // scanners wait until we finish the transaction if we've begun committing,
  // before proceeding with a scan. This ensures scans on this participant are
  // repeatable.
  std::unique_ptr<ScopedOp> commit_op_;

  DISALLOW_COPY_AND_ASSIGN(Txn);
};

// Tracks the on-going transactions in which a given tablet is participating.
class TxnParticipant {
 public:
  explicit TxnParticipant(scoped_refptr<TabletMetadata> tmeta)
      : tablet_metadata_(std::move(tmeta)) {}

  // Convenience struct representing a Txn of this participant. This is useful
  // for testing, as it easy to construct.
  struct TxnEntry {
    int64_t txn_id;
    TxnState state;
    int64_t commit_timestamp;
  };

  // Creates a transaction in kOpen.
  void CreateOpenTransaction(int64_t txn_id,
                             log::LogAnchorRegistry* log_anchor_registry);

  // Gets the transaction for the given transaction ID, creating it in
  // the kInitializing state if one doesn't already exist.
  scoped_refptr<Txn> GetOrCreateTransaction(int64_t txn_id,
                                            log::LogAnchorRegistry* log_anchor_registry);

  // Gets the transaction for the given transaction ID, or returns null if it
  // does not exist.
  scoped_refptr<Txn> GetTransaction(int64_t txn_id);

  // Removes the given transaction if it failed to initialize, e.g. the op that
  // created it failed to replicate, leaving it in the kInitializing state but
  // with no op actively mutating it.
  //
  // It is expected that the caller, e.g. a ParticipantOp, has released any Txn
  // references before calling this, ensuring that when we check the state of
  // the Txn, we can thread-safely determine whether it has been abandoned.
  void ClearIfInitFailed(int64_t txn_id);

  // Removes the given transaction if it is in a terminal state, i.e. it is
  // either kAborted or kCommitted, freeing any WAL anchors it may have held.
  // Assumes there are no active op drivers updating state (i.e. that the
  // transaction reference in our map is the only one).
  //
  // Returns whether or not this call actually cleared the transaction (i.e.
  // returns 'false' if the transaction was not found)..
  bool ClearIfComplete(int64_t txn_id);

  // Returns the transactions, sorted by transaction ID. This returns both
  // in-flight transactions tracked by 'txns_' as well as transactions that
  // have terminated and persisted metadata via abort or commit.
  std::vector<TxnEntry> GetTxnsForTests() const;

 private:
  // Protects insertions and removals from 'txns_'.
  mutable simple_spinlock lock_;

  // Maps from transaction ID to the corresponding transaction state.
  std::unordered_map<int64_t, scoped_refptr<Txn>> txns_;

  // Tablet metadata used to persist this transaction's metadata.
  scoped_refptr<TabletMetadata> tablet_metadata_;
};

inline bool operator==(const TxnParticipant::TxnEntry& lhs, const TxnParticipant::TxnEntry& rhs) {
  return lhs.txn_id == rhs.txn_id &&
      lhs.state == rhs.state &&
      lhs.commit_timestamp == rhs.commit_timestamp;
}

} // namespace tablet
} // namespace kudu
