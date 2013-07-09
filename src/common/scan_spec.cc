// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "common/scan_predicate.h"
#include "common/scan_spec.h"
#include "common/schema.h"

namespace kudu {

ScanSpec::ScanSpec() :
    encoded_ranges_(NULL) {
}

void ScanSpec::AddPredicate(const ColumnRangePredicate &pred) {
  predicates_.push_back(pred);
}

void ScanSpec::EncodeKeyRanges(const Schema &key_schema,
                               vector<EncodedKeyRange *> *encoded) {
  DCHECK_EQ(encoded_ranges_, static_cast<vector<EncodedKeyRange *> *>(NULL));
  DCHECK_EQ(encoded->size(), 0);

  if (key_schema.num_key_columns() > 1) {
    VLOG(1) << "Encoding predicates not supported for composite keys";
    return;
  }

  // TODO (afeinberg) : support compound keys
  const ColumnSchema &key_col = key_schema.column(0);
  for (ScanSpec::PredicateList::iterator iter = predicates_.begin();
       iter != predicates_.end();) {
    const ColumnRangePredicate &pred = *iter;
    if (pred.column().Equals(key_col)) {
      VLOG(1) << "Pushing down predicate " << pred.ToString() << " on key column";
      EncodedKey *lower_bound, *upper_bound;
      lower_bound = upper_bound = NULL;
      const ValueRange &range = pred.range();
      if (range.has_lower_bound()) {
        lower_bound = new EncodedKey(key_schema, range.lower_bound());
      }
      if (range.has_upper_bound()) {
        upper_bound = new EncodedKey(key_schema, range.upper_bound());
      }
      encoded->push_back(new EncodedKeyRange(lower_bound, upper_bound));
      iter = predicates_.erase(iter);
    } else {
      ++iter;
    }
  }

  encoded_ranges_ = encoded;
}

} // namespace kudu
