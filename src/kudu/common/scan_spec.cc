// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "kudu/common/scan_spec.h"

#include <string>
#include <vector>

#include "kudu/gutil/strings/join.h"

using std::vector;
using std::string;

namespace kudu {

void ScanSpec::AddPredicate(const ColumnRangePredicate &pred) {
  predicates_.push_back(pred);
}

void ScanSpec::AddEncodedRange(const EncodedKeyRange *range) {
  encoded_ranges_.push_back(range);
}

string ScanSpec::ToString() const {
  vector<string> preds;

  BOOST_FOREACH(const EncodedKeyRange* key_range, encoded_ranges_) {
    preds.push_back(key_range->ToString());
  }

  BOOST_FOREACH(const ColumnRangePredicate& pred, predicates_) {
    preds.push_back(pred.ToString());
  }
  return JoinStrings(preds, "\n");
}

} // namespace kudu
