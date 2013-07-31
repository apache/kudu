// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "common/scan_spec.h"

namespace kudu {

void ScanSpec::AddPredicate(const ColumnRangePredicate &pred) {
  predicates_.push_back(pred);
}

void ScanSpec::AddEncodedRange(const EncodedKeyRange *range) {
  encoded_ranges_.push_back(range);
}

} // namespace kudu
