// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_SCAN_PREDICATE_H
#define KUDU_CLIENT_SCAN_PREDICATE_H

#include "kudu/client/schema.h"
#include "kudu/util/kudu_export.h"

namespace kudu {

class ColumnRangePredicate;

namespace client {

class KUDU_EXPORT KuduColumnRangePredicate {
 public:
  KuduColumnRangePredicate(const KuduColumnSchema &col,
                           const void* lower_bound,
                           const void* upper_bound);
  KuduColumnRangePredicate(const KuduColumnRangePredicate& other);
  ~KuduColumnRangePredicate();

  KuduColumnRangePredicate& operator=(const KuduColumnRangePredicate& other);
  void CopyFrom(const KuduColumnRangePredicate& other);

 private:
  friend class KuduScanner;

  // Owned.
  ColumnRangePredicate* pred_;
};

} // namespace client
} // namespace kudu
#endif // KUDU_CLIENT_SCAN_PREDICATE_H
