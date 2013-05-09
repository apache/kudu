// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_DELTA_KEY_H
#define KUDU_TABLET_DELTA_KEY_H

#include "common/rowid.h"
#include "gutil/endian.h"
#include "tablet/mvcc.h"

namespace kudu {
namespace tablet {

// Each entry in the delta memstore or delta files is keyed by the rowid
// which has been updated, as well as the txid which performed the update.
class DeltaKey {
 public:
  DeltaKey() :
    row_idx_(-1)
  {}

  DeltaKey(rowid_t id, const txid_t &txid) :
    row_idx_(id),
    txid_(txid) {
  }


  // Encode this key into the given buffer.
  //
  // The encoded form of a DeltaKey is guaranteed to share the same sort
  // order as the DeltaKey itself when compared using memcmp(), so it may
  // be used as a string key in indexing structures, etc.
  void EncodeTo(faststring *dst) const {
    EncodeRowId(dst, row_idx_);
    txid_.EncodeTo(dst);
  }


  // Decode a DeltaKey object from its serialized form.
  //
  // The slice 'key' should contain the encoded key at its beginning, and may
  // contain further data after that.
  // The 'key' slice is mutated so that, upon return, the decoded key has been removed from
  // its beginning.
  Status DecodeFrom(Slice *key) {
    Slice orig(*key);
    if (!PREDICT_TRUE(DecodeRowId(key, &row_idx_))) {
      return Status::Corruption("Bad delta key: bad rowid", orig.ToDebugString(20));
    }

    if (!PREDICT_TRUE(txid_.DecodeFrom(key))) {
      return Status::Corruption("Bad delta key: bad txid", orig.ToDebugString(20));
    }

    return Status::OK();
  }

  string ToString() const {
    return StringPrintf("(row %u@tx%"TXID_PRINT_FORMAT")",
                        row_idx_, txid_.v);
  }

  // Compare this key to another key. Delta keys are sorted by ascending rowid,
  // then ascending txid.
  int CompareTo(const DeltaKey &other) {
    if (row_idx_ < other.row_idx_) {
      return -1;
    } else if (row_idx_ > other.row_idx_ ) {
      return 1;
    }

    return txid_.CompareTo(other.txid_);
  }

  rowid_t row_idx() const { return row_idx_; }

  const txid_t &txid() const { return txid_; }

 private:
  // The row which has been updated.
  rowid_t row_idx_;

  // The transaction ID which applied the update.
  txid_t txid_;
};

} // namespace tablet
} // namespace kudu

#endif
