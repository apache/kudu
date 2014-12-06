// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
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

void ScanSpec::SetLowerBoundKey(const EncodedKey* key) {
  if (lower_bound_key_ == NULL ||
      key->encoded_key().compare(lower_bound_key_->encoded_key()) > 0) {
    lower_bound_key_ = key;
  }
}
void ScanSpec::SetUpperBoundKey(const EncodedKey* key) {
  if (upper_bound_key_ == NULL ||
      key->encoded_key().compare(upper_bound_key_->encoded_key()) < 0) {
    upper_bound_key_ = key;
  }
}

string ScanSpec::ToString() const {
  vector<string> preds;

  if (lower_bound_key_ || upper_bound_key_) {
    preds.push_back(EncodedKey::RangeToString(lower_bound_key_, upper_bound_key_));
  }

  BOOST_FOREACH(const ColumnRangePredicate& pred, predicates_) {
    preds.push_back(pred.ToString());
  }
  return JoinStrings(preds, "\n");
}

} // namespace kudu
