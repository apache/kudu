// Copyright (c) 2014, Cloudera,inc.

#include "client/scan_predicate.h"

#include "common/scan_predicate.h"

namespace kudu {

namespace client {

KuduColumnRangePredicate::KuduColumnRangePredicate(const KuduColumnSchema &col,
                                                   const void* lower_bound,
                                                   const void* upper_bound) {
  pred_.reset(new ColumnRangePredicate(*col.col_, lower_bound, upper_bound));
}

KuduColumnRangePredicate::KuduColumnRangePredicate(const KuduColumnRangePredicate& other) {
  CopyFrom(other);
}

KuduColumnRangePredicate::~KuduColumnRangePredicate() {}

KuduColumnRangePredicate& KuduColumnRangePredicate::operator=(
    const KuduColumnRangePredicate& other) {
  if (&other != this) {
    CopyFrom(other);
  }
  return *this;
}

void KuduColumnRangePredicate::CopyFrom(const KuduColumnRangePredicate& other) {
  pred_.reset(new ColumnRangePredicate(*other.pred_));

}

} // namespace client
} // namespace kudu
