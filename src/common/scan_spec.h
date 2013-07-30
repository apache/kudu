// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_COMMON_SCAN_SPEC_H
#define KUDU_COMMON_SCAN_SPEC_H

#include <vector>

#include "common/scan_predicate.h"
#include "common/schema.h"
#include "util/auto_release_pool.h"

namespace kudu {

using std::vector;

class ScanSpec {
 public:
  typedef vector<ColumnRangePredicate> PredicateList;

  void AddPredicate(const ColumnRangePredicate &pred);

  // The ScanSpec does not take ownership of the range. The range
  // object must remain valid as long as this ScanSpec.
  void AddEncodedRange(const EncodedKeyRange *range);

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

  bool has_encoded_ranges() const {
    return !encoded_ranges_.empty();
  }

  const vector<const EncodedKeyRange *> &encoded_ranges() const {
    return encoded_ranges_;
  }

 private:
  vector<ColumnRangePredicate> predicates_;
  vector<const EncodedKeyRange *> encoded_ranges_;
};

// Encodes a list of column predicates into EncodedKeyRange objects.
// Uses an AutoReleasePool to allocate new EncodedKeyRange instances,
// which means the lifetime of RangePredicateEncoder must be >= the
// lifetime of any classes that access the EncodedKeyRange instances.
class RangePredicateEncoder {
 public:
  explicit RangePredicateEncoder(const Schema &key_schema);

  void EncodeRangePredicates(ScanSpec *spec);

 private:
  const Schema key_schema_;
  AutoReleasePool pool_;
};

} // namespace kudu

#endif
