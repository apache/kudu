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

#include <cstddef>
#include <string>

#include "kudu/common/rowid.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

class faststring;

namespace tablet {

// The type of the delta.
enum DeltaType {
  // REDO delta files contain the mutations that were applied
  // since the base data was last flushed/compacted. REDO deltas
  // are sorted by increasing op timestamp.
  REDO,
  // UNDO delta files contain the mutations that were applied
  // prior to the time the base data was last flushed/compacted
  // and allow to execute point-in-time snapshot scans. UNDO
  // deltas are sorted by decreasing op timestamp.
  UNDO
};

const char* DeltaType_Name(DeltaType t);

// An alternate representation of the raw DeltaType. By templating on
// DeltaTypeSelector instead of DeltaType, it's far easier to reference the
// DeltaType at runtime. For example:
//
// template <typename T>
// void Foo() {
//   cout << T::kTag == REDO ? "REDO" : "UNDO" << endl;
// }
//
// Foo<DeltaTypeSelector<REDO>>(); // prints 'REDO'
template <DeltaType Type>
struct DeltaTypeSelector {
  static constexpr DeltaType kTag = Type;
};

// Each entry in the delta memrowset or delta files is keyed by the rowid
// which has been updated, as well as the timestamp which performed the update.
class DeltaKey {
 public:
  DeltaKey() :
    row_idx_(-1)
  {}

  DeltaKey(rowid_t id, Timestamp timestamp)
      : row_idx_(id), timestamp_(timestamp) {}

  // Encode this key into the given buffer.
  //
  // The encoded form of a DeltaKey is guaranteed to share the same sort
  // order as the DeltaKey itself when compared using memcmp(), so it may
  // be used as a string key in indexing structures, etc.
  void EncodeTo(faststring *dst) const {
    EncodeRowId(dst, row_idx_);
    timestamp_.EncodeTo(dst);
  }


  // Decode a DeltaKey object from its serialized form.
  //
  // The slice 'key' should contain the encoded key at its beginning, and may
  // contain further data after that.
  // The 'key' slice is mutated so that, upon return, the decoded key has been removed from
  // its beginning.
  //
  // This function is called frequently, so is marked HOT to encourage inlining.
  Status DecodeFrom(Slice *key) ATTRIBUTE_HOT {
    Slice orig(*key);
    if (!PREDICT_TRUE(DecodeRowId(key, &row_idx_))) {
      // Out-of-line the error case to keep this function small and inlinable.
      return DeltaKeyError(orig, "bad rowid");
    }

    if (!PREDICT_TRUE(timestamp_.DecodeFrom(key))) {
      // Out-of-line the error case to keep this function small and inlinable.
      return DeltaKeyError(orig, "bad timestamp");
    }

    return Status::OK();
  }

  std::string ToString() const {
    return strings::Substitute("(row $0@ts$1)", row_idx_, timestamp_.ToString());
  }

  // Compare this key to another key. Delta keys are sorted by ascending rowid,
  // then ascending timestamp, except if this is an undo delta key, in which case the
  // the keys are sorted by ascending rowid and then by _descending_ timestamp so that
  // the op closer to the base data comes first.
  template<DeltaType Type>
  int CompareTo(const DeltaKey &other) const;

  rowid_t row_idx() const { return row_idx_; }

  const Timestamp &timestamp() const { return timestamp_; }

 private:
  // Out-of-line error construction used by DecodeFrom.
  static Status DeltaKeyError(const Slice& orig, const char* err);

  // The row which has been updated.
  rowid_t row_idx_;

  // The timestamp of the op which applied the update.
  Timestamp timestamp_;
};

template<>
inline int DeltaKey::CompareTo<REDO>(const DeltaKey &other) const {
  if (row_idx_ < other.row_idx_) {
    return -1;
  } else if (row_idx_ > other.row_idx_) {
    return 1;
  }

  return timestamp_.CompareTo(other.timestamp_);
}

template<>
inline int DeltaKey::CompareTo<UNDO>(const DeltaKey &other) const {
  if (row_idx_ < other.row_idx_) {
    return -1;
  } else if (row_idx_ > other.row_idx_) {
    return 1;
  }

  return other.timestamp_.CompareTo(timestamp_);
}

template<DeltaType Type>
struct DeltaKeyLessThanFunctor {
  bool operator() (const DeltaKey& a, const DeltaKey& b) const {
    return a.CompareTo<Type>(b) < 0;
  }
};

template<DeltaType Type>
struct DeltaKeyEqualToFunctor {
  bool operator() (const DeltaKey& a, const DeltaKey& b) const {
    return a.CompareTo<Type>(b) == 0;
  }
};

struct DeltaKeyHashFunctor {
  size_t operator() (const DeltaKey& key) const {
    return (key.row_idx() * 31ULL) + key.timestamp().ToUint64();
  }
};

} // namespace tablet
} // namespace kudu
