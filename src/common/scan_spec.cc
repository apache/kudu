// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "common/scan_predicate.h"
#include "common/scan_spec.h"
#include "common/schema.h"

namespace kudu {


void ScanSpec::AddPredicate(const ColumnRangePredicate &pred) {
  predicates_.push_back(pred);
}


} // namespace kudu
