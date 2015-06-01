// Copyright (c) 2014, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/scan_predicate.h"

#include "kudu/common/scan_predicate.h"

namespace kudu {

namespace client {

KuduColumnRangePredicate::KuduColumnRangePredicate(const KuduColumnSchema &col,
                                                   const void* lower_bound,
                                                   const void* upper_bound)
  : pred_(new ColumnRangePredicate(*col.col_, lower_bound, upper_bound)) {
}

KuduColumnRangePredicate::KuduColumnRangePredicate(const KuduColumnRangePredicate& other)
  : pred_(NULL) {
  CopyFrom(other);
}

KuduColumnRangePredicate::~KuduColumnRangePredicate() {
  delete pred_;
}

KuduColumnRangePredicate& KuduColumnRangePredicate::operator=(
    const KuduColumnRangePredicate& other) {
  if (&other != this) {
    CopyFrom(other);
  }
  return *this;
}

void KuduColumnRangePredicate::CopyFrom(const KuduColumnRangePredicate& other) {
  delete pred_;
  pred_ = new ColumnRangePredicate(*other.pred_);
}

} // namespace client
} // namespace kudu
