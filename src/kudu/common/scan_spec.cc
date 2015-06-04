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
void ScanSpec::SetExclusiveUpperBoundKey(const EncodedKey* key) {
  if (exclusive_upper_bound_key_ == NULL ||
      key->encoded_key().compare(exclusive_upper_bound_key_->encoded_key()) < 0) {
    exclusive_upper_bound_key_ = key;
  }
}

string ScanSpec::ToString() const {
  return ToStringWithOptionalSchema(NULL);
}

string ScanSpec::ToStringWithSchema(const Schema& s) const {
  return ToStringWithOptionalSchema(&s);
}

string ScanSpec::ToStringWithOptionalSchema(const Schema* s) const {
  vector<string> preds;

  if (lower_bound_key_ || exclusive_upper_bound_key_) {
    if (s) {
      preds.push_back(EncodedKey::RangeToStringWithSchema(
                          lower_bound_key_,
                          exclusive_upper_bound_key_,
                          *s));
    } else {
      preds.push_back(EncodedKey::RangeToString(
                          lower_bound_key_,
                          exclusive_upper_bound_key_));
    }
  }

  BOOST_FOREACH(const ColumnRangePredicate& pred, predicates_) {
    preds.push_back(pred.ToString());
  }
  return JoinStrings(preds, "\n");
}

} // namespace kudu
