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

#include "kudu/gutil/macros.h"
#include "kudu/util/array_view.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

class Arena;
class ConstContiguousRow;
class Schema;

// Wrapper around an encoded Key as well as the component raw key columns that
// went into encoding it.
//
// This is a POD object that can be safely allocated on an Arena. It assumes
// that all of its referred-to data is also on the same Arena or otherwise has a
// lifetime that extends longer than this object.
class EncodedKey {
 public:
  // Construct an EncodedKey object in the given Arena corresponding to the
  // given Row. The lifetime of the row must outlive the lifetime of this
  // EncodedKey object.
  static EncodedKey* FromContiguousRow(const ConstContiguousRow& row, Arena* arena);

  // Decode the encoded key specified in 'encoded', which must correspond to the
  // provided schema.
  // The returned EncodedKey object and its referred-to row data is allocated
  // from 'arena' and returned in '*result'. If allocation fails or the encoding
  // is invalid, returns a bad Status.
  static Status DecodeEncodedString(const Schema& schema,
                                    Arena* arena,
                                    const Slice& encoded,
                                    EncodedKey** result);

  // Given an EncodedKey, increment it to the next lexicographically greater
  // EncodedKey. The returned key is allocated from the arena.
  static Status IncrementEncodedKey(const Schema& tablet_schema, EncodedKey** key, Arena* arena);

  const Slice& encoded_key() const { return encoded_key_; }

  ArrayView<const void* const> raw_keys() const { return raw_keys_; }

  size_t num_key_columns() const { return num_key_cols_; }

  std::string Stringify(const Schema &schema) const;

  // Tests whether this EncodedKey is within the bounds given by 'start'
  // and 'end'.
  //
  // The empty bound has special significance: it's both the lowest value
  // (if in 'start') and the highest (if in 'end').
  bool InRange(const Slice& start, const Slice& end) const {
    return (start <= encoded_key_ && (end.empty() || encoded_key_ < end));
  }

  static std::string RangeToString(const EncodedKey* lower,
                                   const EncodedKey* upper);

  static std::string RangeToStringWithSchema(const EncodedKey* lower,
                                             const EncodedKey* upper,
                                             const Schema& schema);
  // Constructs a new EncodedKey.
  //
  // NOTE: External callers should prefer using EncodedKey::FromContiguousRow
  // or EncodedKeyBuilder. This is public only to support allocation from the
  // Arena class.
  //
  // NOTE: num_key_cols is the number of key columns for
  // the schema, but this may be different from the size of raw_keys
  // in which case raw_keys represents the supplied prefix of a
  // composite key.
  EncodedKey(Slice encoded_key, ArrayView<const void*> raw_keys, size_t num_key_cols);

 private:
  friend class EncodedKeyBuilder;

  const Slice encoded_key_;
  const int num_key_cols_;
  const ArrayView<const void*> raw_keys_;
};

// A builder for encoded key: creates an encoded key from
// one or more key columns specified as raw pointers.
class EncodedKeyBuilder {
 public:
  // 'schema' must remain valid for the lifetime of the EncodedKeyBuilder.
  //
  // 'arena' must remain live for the lifetime of the EncodedKey objects
  // returned by BuildEncodedKey().
  EncodedKeyBuilder(const Schema* schema, Arena* arena);

  void Reset();

  void AddColumnKey(const void *raw_key);

  EncodedKey *BuildEncodedKey();

 private:
  DISALLOW_COPY_AND_ASSIGN(EncodedKeyBuilder);

  const Schema* schema_;
  Arena* const arena_;

  faststring encoded_key_;
  const size_t num_key_cols_;
  size_t idx_;
  const void** raw_keys_;
};

}  // namespace kudu
