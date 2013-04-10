// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_MVCC_H
#define KUDU_TABLET_MVCC_H

#include "gutil/endian.h"

#include "util/memcmpable_varint.h"
#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>
#include <inttypes.h>
#include <string>
#include <tr1/unordered_set>

namespace kudu {
namespace tablet {

class MvccManager;

using std::tr1::unordered_set;
using std::string;


// The printf format specifier for txid values
#define TXID_PRINT_FORMAT PRIu64

// Type-safe container for txids (to prevent inadvertently passing
// other integers)
struct txid_t {
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

  // An invalid transaction ID -- txid_t types initialize to this variable.
  static const txid_t kInvalidTxId;

  // The maximum txid.
  static const txid_t kMax;

  val_type v;

};


inline std::ostream &operator <<(std::ostream &o, const txid_t &txid) {
  return o << txid.v;
}

// A snapshot of the current MVCC state, which can determine whether
// a transaction ID should be considered visible.
class MvccSnapshot {
 public:
  MvccSnapshot();

  // Create a snapshot with the current state of the given manager
  MvccSnapshot(const MvccManager &manager);

  // Create a snapshot which considers all transactions as committed.
  // This is mostly useful in test contexts.
  static MvccSnapshot CreateSnapshotIncludingAllTransactions();

  // Return true if the given transaction ID should be considered committed
  // in this snapshot.
  bool IsCommitted(txid_t txid) const;

  // Return a string representation of the set of committed transactions
  // in this snapshot, suitable for debug printouts.
  string ToString() const;

 private:
  friend class MvccManager;

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
class MvccManager : boost::noncopyable {
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
  mutable boost::mutex lock_;
  MvccSnapshot cur_snap_;
};

// A scoped handle to a running transaction.
// When this object goes out of scope, the transaction is automatically
// committed.
class ScopedTransaction : boost::noncopyable {
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
  bool committed_;
  MvccManager * const manager_;
  const txid_t txid_;
};


} // namespace tablet
} // namespace kudu

#endif
