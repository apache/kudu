// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_COMMON_PREDICATE_ENCODER_H
#define KUDU_COMMON_PREDICATE_ENCODER_H

#include <vector>

#include "common/encoded_key.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "util/auto_release_pool.h"

namespace kudu {

using std::vector;

// Encodes a list of column predicates into EncodedKeyRange objects.
// Uses an AutoReleasePool to allocate new EncodedKeyRange instances,
// which means the lifetime of RangePredicateEncoder must be >= the
// lifetime of any classes that access the EncodedKeyRange instances.
class RangePredicateEncoder {
 public:
  explicit RangePredicateEncoder(const Schema &key_schema);

  // Encodes the predicates found in 'spec' into a key range which is
  // then emitted back into 'spec'.
  //
  // If 'erase_pushed' is true, pushed predicates are removed from 'spec'.
  void EncodeRangePredicates(ScanSpec *spec, bool erase_pushed);

 private:

  // Collects any predicates that apply
  void ExtractPredicatesOnKeys(const ScanSpec &spec,
                               const ColumnRangePredicate **key_preds) const;

  // Returns the number of contiguous equalities in the key prefix or
  // -1 if none are found; mutates key_preds to NULL-out any
  // predicates which come after the key predicates which may be
  // pushed down.
  int CountKeyPrefixEqualities(const ColumnRangePredicate **key_preds) const;

  // Erases any predicates we've encoded from the predicate list
  void ErasePushedPredicates(ScanSpec *spec,
                             const ColumnRangePredicate **key_preds) const;

  const Schema key_schema_;
  EncodedKeyBuilder lower_builder_;
  EncodedKeyBuilder upper_builder_;
  AutoReleasePool pool_;
};

} // namespace kudu

#endif
