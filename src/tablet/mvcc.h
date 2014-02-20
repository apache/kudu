// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_MVCC_H
#define KUDU_TABLET_MVCC_H

#include <gtest/gtest.h>
#include <inttypes.h>
#include <tr1/unordered_set>
#include <string>

#include "gutil/endian.h"
#include "gutil/macros.h"
#include "gutil/strings/substitute.h"
#include "util/memcmpable_varint.h"
#include "util/locks.h"
#include "util/status.h"

namespace kudu {
namespace tablet {

class MvccManager;

using std::tr1::unordered_set;
using std::string;

// Type-safe container for txids (to prevent inadvertently passing
// other integers)
class txid_t {
 public:
  typedef uint64_t val_type;

  txid_t() :
    v(kInvalidTxId.v)
  {}

  explicit txid_t(uint64_t val) :
    v(val)
  {}


  bool operator ==(const txid_t &other) const {
    return v == other.v;
  }

  // Decode a txid from the given input slice.
  // Mutates the slice to point after the decoded txid.
  // Returns true upon success.
  bool DecodeFrom(Slice *input) {
    return GetMemcmpableVarint64(input, &v);
  }

  // Encode the txid to the given buffer.
  void EncodeTo(faststring *dst) const {
    PutMemcmpableVarint64(dst, v);
  }

  int CompareTo(const txid_t &other) const {
    if (v < other.v) {
      return -1;
    } else if (v > other.v) {
      return 1;
    }

    return 0;
  }

  string ToString() const {
    return strings::Substitute("$0", v);
  }

  // Encodes this txid_t and appends it to the provided string.
  void EncodeToString(string* encode_to) const {
    faststring buf;
    EncodeTo(&buf);
    encode_to->append(reinterpret_cast<const char* >(buf.data()), buf.size());
  }

  Status DecodeFromString(const string& decode_from) {
    Slice slice(decode_from);
    if (!DecodeFrom(&slice)) {
      return Status::Corruption("Cannot decode txid.");
    }
    return Status::OK();
  }

  // An invalid transaction ID -- txid_t types initialize to this variable.
  static const txid_t kInvalidTxId;

  // The maximum txid.
  static const txid_t kMax;

  // The minimum txid.
  static const txid_t kMin;

 private:
  friend class MvccManager;
  friend class MvccSnapshot;
  FRIEND_TEST(TestMvcc, TestMvccBasic);
  FRIEND_TEST(TestMvcc, TestMvccMultipleInFlight);
  FRIEND_TEST(TestMvcc, TestScopedTransaction);

  val_type v;
};


inline std::ostream &operator <<(std::ostream &o, const txid_t &txid) {
  return o << txid.ToString();
}

// A snapshot of the current MVCC state, which can determine whether
// a transaction ID should be considered visible.
class MvccSnapshot {
 public:
  MvccSnapshot();

  // Create a snapshot with the current state of the given manager
  explicit MvccSnapshot(const MvccManager &manager);

  // Create a snapshot at a specific txid_t
  explicit MvccSnapshot(const txid_t& txid);

  // Create a snapshot which considers all transactions as committed.
  // This is mostly useful in test contexts.
  static MvccSnapshot CreateSnapshotIncludingAllTransactions();

  // Return true if the given transaction ID should be considered committed
  // in this snapshot.
  bool IsCommitted(const txid_t& txid) const;

  // Returns true if this snapshot may have any committed transactions with ID
  // equal to or higher than the provided 'txid'.
  // This is mostly useful to avoid scanning REDO deltas in certain cases.
  // If MayHaveCommittedTransactionsAtOrAfter(delta_stats.min) returns true
  // it means that there might be transactions that need to be applied in the
  // context of this snapshot; otherwise no scanning is necessary.
  bool MayHaveCommittedTransactionsAtOrAfter(const txid_t& txid) const;

  // Returns true if this snapshot may have any uncommitted transactions with ID
  // lower than the provided 'txid'.
  // This is mostly useful to avoid scanning UNDO deltas in certain cases.
  // If MayHaveUncommittedTransactionsBefore(delta_stats.max) returns false it
  // means that all UNDO delta transactions are committed in the context of this
  // snapshot and no scanning is necessary; otherwise there might be some
  // transactions that need to be undone.
  bool MayHaveUncommittedTransactionsBefore(const txid_t& txid) const;

  // Return a string representation of the set of committed transactions
  // in this snapshot, suitable for debug printouts.
  string ToString() const;

  // Return the number of transactions in flight during this snapshot.
  size_t num_transactions_in_flight() const;

 private:
  friend class MvccManager;
  FRIEND_TEST(TestMvcc, TestMayHaveCommittedTransactionsAtOrAfter);
  FRIEND_TEST(TestMvcc, TestMayHaveUncommittedTransactionsBefore);

  // Summary rule:
  // A transaction T is committed if and only if:
  //    T < all_committed_before_txid_
  // or (T < none_committed_after_txid && !txids_in_flight.contains(T))

  // A transaction ID below which all transactions have been committed.
  // For any txid X, if X < all_committed_txid_, then X is committed.
  txid_t all_committed_before_txid_;

  // A transaction ID above which no transactions have been committed.
  // For any txid X, if X >= none_committed_after_txid_, then X is not committed.
  txid_t none_committed_after_txid_;

  // The current set of transactions which are in flight.
  // For any txid X, if X is in txids_in_flight_, it is not yet committed.
  unordered_set<txid_t::val_type> txids_in_flight_;

};

// Coordinator of MVCC transactions. Threads wishing to make updates use
// the MvccManager to obtain a unique txid, usually through the ScopedTransaction
// class defined below.
//
// NOTE: There is no support for transaction abort/rollback, since
// transaction support is quite simple. Transactions are only used to
// defer visibility of updates until commit time, and allow iterators to
// operate on a snapshot which contains only committed transactions.
class MvccManager {
 public:
  MvccManager();

  // Begin a new transaction, assigning it a transaction ID.
  // Callers should generally prefer using the ScopedTransaction class defined
  // below, which will automatically finish the transaction when it goes out
  // of scope.
  txid_t StartTransaction();

  // Commit the given transaction.
  //
  // If the transaction is not currently in-flight, this will trigger an
  // assertion error. It is an error to commit the same transaction more
  // than once.
  void CommitTransaction(txid_t txid);

  // Take a snapshot of the current MVCC state, which indicates which
  // transactions have been committed at the time of this call.
  void TakeSnapshot(MvccSnapshot *snapshot) const;

 private:
  DISALLOW_COPY_AND_ASSIGN(MvccManager);

  typedef simple_spinlock LockType;
  mutable LockType lock_;
  MvccSnapshot cur_snap_;
};

// A scoped handle to a running transaction.
// When this object goes out of scope, the transaction is automatically
// committed.
class ScopedTransaction {
 public:
  // Create a new transaction from the given MvccManager.
  //
  // The MvccManager must remain valid for the lifetime of this object.
  explicit ScopedTransaction(MvccManager *manager);

  // Commit the transaction referenced by this scoped object, if it hasn't
  // already been committed.
  ~ScopedTransaction();

  txid_t txid() const {
    return txid_;
  }

  // Commit the in-flight transaction.
  void Commit();

 private:
  DISALLOW_COPY_AND_ASSIGN(ScopedTransaction);

  bool committed_;
  MvccManager * const manager_;
  const txid_t txid_;
};


} // namespace tablet
} // namespace kudu

#endif
