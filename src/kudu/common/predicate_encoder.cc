// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.

#include "kudu/common/predicate_encoder.h"

namespace kudu {

RangePredicateEncoder::RangePredicateEncoder(const Schema* key_schema)
    : key_schema_(key_schema),
      lower_builder_(key_schema_),
      upper_builder_(key_schema_) {
}

void RangePredicateEncoder::EncodeRangePredicates(ScanSpec *spec, bool erase_pushed) {
  DCHECK_EQ(spec->encoded_ranges().size(), 0);

  int num_key_cols = key_schema_->num_key_columns();
  const ColumnRangePredicate *key_preds[num_key_cols];
  ExtractPredicatesOnKeys(*spec, key_preds);

  int prefix_end = CountKeyPrefixEqualities(key_preds);
  bool has_equality_prefix = prefix_end != -1;
  lower_builder_.Reset();
  for (int i = 0; i <= prefix_end; ++i) {
    lower_builder_.AddColumnKey(key_preds[i]->range().lower_bound());
  }

  EncodedKey *upper = NULL;
  EncodedKey *lower = NULL;
  if (prefix_end == num_key_cols - 1) {
    // If entire scan spec is an equality predicate
    lower = lower_builder_.BuildEncodedKey();
    upper = lower;
  } else {
    if (key_preds[prefix_end + 1] != NULL) {
      // If the entire composite key is not an equality predicate:
      const ValueRange &range = key_preds[prefix_end + 1]->range();
      if (range.has_lower_bound() && range.has_upper_bound()) {
        // If there is an upper and lower bound on the last column
        // of the composite key, set encoded key's lower bound to
        // equality prefix + lower bound and encoded key's upper bound
        // to equality prefix + upper bound.
        // e.g.,
        // year=2006, month=01, day >= 01 && day <= 02 becomes
        // lower=(2006,01,01) upper=(2006,01,02)
        upper_builder_.AssignCopy(lower_builder_);
        upper_builder_.AddColumnKey(range.upper_bound());
        if (prefix_end < num_key_cols - 2) {
          upper = upper_builder_.BuildSuccessorEncodedKey();
        } else {
          upper = upper_builder_.BuildEncodedKey();
        }
        lower_builder_.AddColumnKey(range.lower_bound());
        lower = lower_builder_.BuildEncodedKey();
      } else if (range.has_lower_bound()) {
        // If only the lower bound is specified, but no upper bound
        // is specified, set the lower bound to equality_prefix +
        // lower_bound and set the upper bound to successor of
        // equality_prefix
        // e.g.,
        // year=2006, month=01, day >= 01 becomes
        // lower=(2006,01,01) upper=(2006,02)
        upper_builder_.AssignCopy(lower_builder_);
        upper = upper_builder_.BuildSuccessorEncodedKey();
        lower_builder_.AddColumnKey(range.lower_bound());
        lower = lower_builder_.BuildEncodedKey();
      } else {
        // If there is only an upper bound
        // Set last column of the upper bound to the upper
        // bound of the predicate on the last key column
        // e.g.,
        // year=2006, month=01, day <= 15 becomes
        // lower=(2006,01) upper=(2006,01,15)
        //
        // year <= 2006 becomes
        // lower=<none> upper=2007
        upper_builder_.AssignCopy(lower_builder_);
        lower = lower_builder_.BuildEncodedKey();
        upper_builder_.AddColumnKey(range.upper_bound());
        if (prefix_end < num_key_cols - 2) {
          upper = upper_builder_.BuildSuccessorEncodedKey();
        } else {
          upper = upper_builder_.BuildEncodedKey();
        }
      }
    } else if (num_key_cols > 1 && has_equality_prefix) {
      // If not all columns of a composite key are specified, but an
      // equality exists, set the upper bound to a immediate successor
      // of the lower bound year
      // e.g.,
      // year=2006, month=01 becomes
      // lower=(2006,01) upper=(2006,02)
      upper_builder_.AssignCopy(lower_builder_);
      lower = lower_builder_.BuildEncodedKey();
      upper = upper_builder_.BuildSuccessorEncodedKey();
    }
  }

  if (upper == NULL && lower == NULL) {
    VLOG(1) << "No predicates could be pushed down!";
    return;
  }

  if (erase_pushed) {
    ErasePushedPredicates(spec, key_preds);
  }

  EncodedKeyRange *r = new EncodedKeyRange(lower, upper);
  pool_.Add(r);
  spec->AddEncodedRange(r);
}

void RangePredicateEncoder::ExtractPredicatesOnKeys(
    const ScanSpec &spec, const ColumnRangePredicate **key_preds) const {
  int num_key_cols = key_schema_->num_key_columns();
  for (int i = 0; i < num_key_cols; ++i) {
    key_preds[i] = NULL;
  }
  BOOST_FOREACH(const ColumnRangePredicate &pred, spec.predicates()) {
    int idx = key_schema_->find_column(pred.column().name());
    if (idx != -1 && idx < num_key_cols) {
      if (key_preds[idx] != NULL) {
        VLOG(1) << "Since we can only push down a single predicate "
                << "per column key, " << key_preds[idx]->ToString()
                << " will not be pushed down; pushing down "
                << pred.ToString() << " instead.";
      }
      key_preds[idx] = &pred;
    }
  }
}

int RangePredicateEncoder::CountKeyPrefixEqualities(
    const ColumnRangePredicate **key_preds) const {
  int prefix_end = -1;
  int num_key_cols = key_schema_->num_key_columns();
  for (int i = 0; i < num_key_cols; ++i) {
    const ColumnRangePredicate *pred = key_preds[i];
    if (pred != NULL) {
      const ValueRange &range = pred->range();
      if (range.IsEquality()) {
        prefix_end = i;
        continue;
      }
    }
    break;
  }
  // We're not able to push down any key predicates beyond the one that
  // follows the equality. For example, with keys (a, b, c), the predicate
  // a=1, b>3, c>3 can only be pushed down against a (the equality prefix)
  // and b (the first key column following the equality prefix)
  for (int i = prefix_end + 2; i < num_key_cols; ++i) {
    key_preds[i] = NULL;
  }

  return prefix_end;
}

void RangePredicateEncoder::ErasePushedPredicates(
    ScanSpec *spec, const ColumnRangePredicate **key_preds) const {
  int num_key_cols = key_schema_->num_key_columns();
  ScanSpec::PredicateList* preds = spec->mutable_predicates();
  ScanSpec::PredicateList::iterator it = preds->begin();
  for (int i = 0; i < num_key_cols; ++i) {
    const ColumnRangePredicate& pred = *it;
    if (key_preds[i] == &pred) {
      ++it;
      continue;
    }
    break;
  }
  // Nothing will be erased if it == preds->begin() anyway.
  preds->erase(preds->begin(), it);
}

} // namespace kudu
