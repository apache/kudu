// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "common/scan_spec.h"
#include "common/schema.h"

namespace kudu {

void ScanSpec::AddPredicate(const ColumnRangePredicate &pred) {
  predicates_.push_back(pred);
}

void ScanSpec::AddEncodedRange(const EncodedKeyRange *range) {
  encoded_ranges_.push_back(range);
}

RangePredicateEncoder::RangePredicateEncoder(const Schema &key_schema)
  : key_schema_(key_schema) {
}

void RangePredicateEncoder::EncodeRangePredicates(ScanSpec *spec) {
  DCHECK_EQ(spec->encoded_ranges().size(), 0);

  if (key_schema_.num_key_columns() > 1) {
    VLOG(1) << "Encoding predicates not supported for composite keys";
    return;
  }

  // TODO (afeinberg) : support compound keys
  const ColumnSchema &key_col = key_schema_.column(0);
  ScanSpec::PredicateList *preds = spec->mutable_predicates();
  for (ScanSpec::PredicateList::iterator iter = preds->begin();
       iter != preds->end();) {
    const ColumnRangePredicate &pred = *iter;
    if (pred.column().Equals(key_col)) {
      VLOG(1) << "Pushing down predicate " << pred.ToString() << " on key column";
      EncodedKey *lower_bound, *upper_bound;
      lower_bound = upper_bound = NULL;
      const ValueRange &range = pred.range();
      if (range.has_lower_bound()) {
        lower_bound = new EncodedKey(key_schema_, range.lower_bound());
      }
      if (range.has_upper_bound()) {
        upper_bound = new EncodedKey(key_schema_, range.upper_bound());
      }
      EncodedKeyRange *r = new EncodedKeyRange(lower_bound, upper_bound);
      pool_.Add(r);
      spec->AddEncodedRange(r);
      iter = preds->erase(iter);
    } else {
      ++iter;
    }
  }
}

} // namespace kudu
