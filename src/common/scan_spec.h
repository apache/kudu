// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_COMMON_SCAN_SPEC_H
#define KUDU_COMMON_SCAN_SPEC_H

#include <vector>

#include "common/scan_predicate.h"
#include "common/schema.h"

namespace kudu {

using std::vector;

class ScanSpec {
 public:
  typedef vector<ColumnRangePredicate> PredicateList;

  ScanSpec();

  void AddPredicate(const ColumnRangePredicate &pred);

  // Given the address of a vector, encode any predicates on the key
  // column in this scan spec and store the address in the vector.
  //
  // Calling object must manage the specified vector, including
  // deallocating members of the vector when the caller goes out of
  // scope, using ElementDeleter or STLDeleteElements from
  // gutil/stl_util.h
  //
  // TODO: have an iterator-level object auto-release pool (re-using
  // code from Impala) so that a *encoded no longer has to be passed
  // in; move and instead add an AddEncodedKeyRange() method.
  void EncodeKeyRanges(const Schema &key_schema,
                       vector<EncodedKeyRange *> *encoded);

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
    return encoded_ranges_ != NULL;
  }

  const vector<EncodedKeyRange *> &encoded_ranges() const {
    return *encoded_ranges_;
  }

 private:
  vector<ColumnRangePredicate> predicates_;
  vector<EncodedKeyRange *> *encoded_ranges_;
};

} // namespace kudu

#endif
