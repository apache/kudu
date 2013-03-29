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

  void AddPredicate(const ColumnRangePredicate &pred);

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

private:
  vector<ColumnRangePredicate> predicates_;
  
};

} // namespace kudu

#endif
