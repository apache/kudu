// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_SCAN_PREDICATE_H
#define KUDU_CLIENT_SCAN_PREDICATE_H

#include "client/schema.h"

namespace kudu {

class ColumnRangePredicate;
class LocalLineItemDAO;

namespace client {

class KuduColumnRangePredicate {
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
  friend class kudu::LocalLineItemDAO;

  gscoped_ptr<ColumnRangePredicate> pred_;
};

} // namespace client
} // namespace kudu
#endif // KUDU_CLIENT_SCAN_PREDICATE_H
