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
#ifndef KUDU_CLIENT_SCAN_PREDICATE_H
#define KUDU_CLIENT_SCAN_PREDICATE_H

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/macros.h"
#include "kudu/util/slice.h"
#else
#include "kudu/client/stubs.h"
#endif

// NOTE: using stdint.h instead of cstdint because this file is supposed
//       to be processed by a compiler lacking C++11 support.
#include <stdint.h>

#include <cstddef>

#include "kudu/client/hash.h"
#include "kudu/util/kudu_export.h"
#include "kudu/util/status.h"

namespace kudu {
namespace client {

/// @brief A representation of comparison predicate for Kudu queries.
///
/// Call KuduTable::NewComparisonPredicate() to create a predicate object.
class KUDU_EXPORT KuduPredicate {
 public:
  /// @brief Supported comparison operators.
  enum ComparisonOp {
    LESS_EQUAL,
    GREATER_EQUAL,
    EQUAL,
    LESS,
    GREATER,
  };

  ~KuduPredicate();

  /// @return A new, identical, KuduPredicate object.
  KuduPredicate* Clone() const;

  /// @brief Forward declaration for the embedded PIMPL class.
  ///
  /// The PIMPL class has to be public since it's actually just an interface,
  /// and gcc gives an error trying to derive from a private nested class.
  class KUDU_NO_EXPORT Data;

 private:
  friend class ComparisonPredicateData;
  friend class ErrorPredicateData;
  friend class InListPredicateData;
  friend class IsNotNullPredicateData;
  friend class IsNullPredicateData;
  friend class KuduTable;
  friend class ScanConfiguration;

  explicit KuduPredicate(Data* d);

  Data* data_;
  DISALLOW_COPY_AND_ASSIGN(KuduPredicate);
};

/// @brief Bloom filter to be used with IN Bloom filter predicate.
///
/// A Bloom filter is a space-efficient probabilistic data-structure used to
/// test set membership with a possibility of false positive matches.
///
/// Create a new KuduBloomFilter using @c KuduBloomFilterBuilder class and
/// populate column values that need to be scanned using
/// @c KuduBloomFilter::Insert() function.
///
/// Supply the populated KuduBloomFilter to
/// @c KuduTable::NewInBloomFilterPredicate() to create an IN Bloom filter
/// predicate.
class KUDU_EXPORT KuduBloomFilter {
 public:
  ~KuduBloomFilter();

  /// Insert key to the Bloom filter.
  ///
  /// @param[in] key
  ///   Column value as a @c Slice to insert into the Bloom filter.
  void Insert(const Slice& key);

 private:
  friend class InBloomFilterPredicateData;
  friend class KuduBloomFilterBuilder;

  class KUDU_NO_EXPORT Data;

  // Owned ptr as per the PIMPL pattern.
  Data* data_;

  /// Clone the Bloom filter.
  ///
  /// @return Raw pointer to the cloned Bloom filter. Caller owns the
  /// Bloom filter till it's passed to
  /// @c KuduTable::NewInBloomFilterPredicate()
  KuduBloomFilter* Clone() const;

  KuduBloomFilter();

  /// Construct a Bloom filter from the KuduBloomFilter::Data pointer.
  ///
  /// @param other_data
  ///   Constructed KuduBloomFilter takes ownership of the pointer.
  explicit KuduBloomFilter(Data* other_data);

  DISALLOW_COPY_AND_ASSIGN(KuduBloomFilter);
};

/// @brief Builder class to help build @c KuduBloomFilter to be used with
/// IN Bloom filter predicate.
class KUDU_EXPORT KuduBloomFilterBuilder {
 public:
  /// @param [in] num_keys
  ///   Expected number of elements to be inserted in the Bloom filter.
  explicit KuduBloomFilterBuilder(size_t num_keys);
  ~KuduBloomFilterBuilder();

  /// @param [in] fpp
  ///   Desired false positive probability between 0.0 and 1.0.
  ///   If not provided, defaults to 0.01.
  /// @return Reference to the updated object.
  KuduBloomFilterBuilder& false_positive_probability(double fpp);

  /// @param [in] hash_algorithm
  ///   Hash algorithm used to hash keys before inserting to the Bloom filter.
  ///   If not provided, defaults to FAST_HASH.
  /// @note Currently only FAST_HASH is supported.
  /// @return Reference to the updated object.
  KuduBloomFilterBuilder& hash_algorithm(HashAlgorithm hash_algorithm);

  /// @param [in] hash_seed
  ///   Seed used with hash algorithm to hash the keys before inserting to
  ///   the Bloom filter.
  ///   If not provided, defaults to 0.
  /// @return Reference to the updated object.
  KuduBloomFilterBuilder& hash_seed(uint32_t hash_seed);

  /// Build a new Bloom filter to be used with IN Bloom filter predicate.
  ///
  /// @param [out] bloom_filter
  ///   On success, the created Bloom filter raw pointer. Caller owns the Bloom
  ///   filter until it's passed to @c KuduTable::NewInBloomFilterPredicate().
  /// @return On success, Status::OK() with the created Bloom filter in
  ///   @c bloom_filter output parameter. On failure to allocate memory or
  ///   invalid arguments, corresponding error status.
  Status Build(KuduBloomFilter** bloom_filter);

 private:
  class KUDU_NO_EXPORT Data;

  // Owned ptr as per the PIMPL pattern.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduBloomFilterBuilder);
};

} // namespace client
} // namespace kudu
#endif // KUDU_CLIENT_SCAN_PREDICATE_H
