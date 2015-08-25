// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.
#ifndef KUDU_COMMON_SCAN_SPEC_H
#define KUDU_COMMON_SCAN_SPEC_H

#include <string>
#include <vector>

#include "kudu/common/scan_predicate.h"
#include "kudu/common/encoded_key.h"

namespace kudu {

using std::vector;

class ScanSpec {
 public:
  ScanSpec()
    : lower_bound_key_(NULL),
      exclusive_upper_bound_key_(NULL),
      cache_blocks_(true) {
  }

  typedef vector<ColumnRangePredicate> PredicateList;

  void AddPredicate(const ColumnRangePredicate &pred);


  // Set the lower bound (inclusive) primary key for the scan.
  // Does not take ownership of 'key', which must remain valid.
  // If called multiple times, the most restrictive key will be used.
  void SetLowerBoundKey(const EncodedKey* key);

  // Set the upper bound (exclusive) primary key for the scan.
  // Does not take ownership of 'key', which must remain valid.
  // If called multiple times, the most restrictive key will be used.
  void SetExclusiveUpperBoundKey(const EncodedKey* key);

  const vector<ColumnRangePredicate> &predicates() const {
    return predicates_;
  }

  // Return a pointer to the list of predicates in this scan spec.
  //
  // Callers may use this during predicate pushdown to remove predicates
  // from their caller if they're able to apply them lower down the
  // iterator tree.
  vector<ColumnRangePredicate> *mutable_predicates() {
    return &predicates_;
  }

  const EncodedKey* lower_bound_key() const {
    return lower_bound_key_;
  }
  const EncodedKey* exclusive_upper_bound_key() const {
    return exclusive_upper_bound_key_;
  }

  bool cache_blocks() const {
    return cache_blocks_;
  }

  void set_cache_blocks(bool cache_blocks) {
    cache_blocks_ = cache_blocks;
  }

  std::string ToString() const;
  std::string ToStringWithSchema(const Schema& s) const;

 private:
  // Helper for the ToString*() methods. 's' may be NULL.
  std::string ToStringWithOptionalSchema(const Schema* s) const;

  vector<ColumnRangePredicate> predicates_;
  const EncodedKey* lower_bound_key_;
  const EncodedKey* exclusive_upper_bound_key_;
  bool cache_blocks_;
};


} // namespace kudu

#endif
