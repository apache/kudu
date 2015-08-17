// Copyright (c) 2014, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/scan_predicate.h"
#include "kudu/client/scan_predicate-internal.h"
#include "kudu/client/value.h"
#include "kudu/client/value-internal.h"

#include "kudu/common/scan_spec.h"
#include "kudu/common/scan_predicate.h"

#include "kudu/gutil/strings/substitute.h"

using strings::Substitute;

namespace kudu {
namespace client {

KuduPredicate::KuduPredicate(Data* d)
  : data_(d) {
}

KuduPredicate::~KuduPredicate() {
  delete data_;
}

KuduPredicate::Data::Data() {
}

KuduPredicate::Data::~Data() {
}

KuduPredicate* KuduPredicate::Clone() const {
  return new KuduPredicate(data_->Clone());
}

ComparisonPredicateData::ComparisonPredicateData(const ColumnSchema& col,
                                                 KuduPredicate::ComparisonOp op,
                                                 KuduValue* val) :
  col_(col),
  op_(op),
  val_(val) {
}
ComparisonPredicateData::~ComparisonPredicateData() {
}


Status ComparisonPredicateData::AddToScanSpec(ScanSpec* spec) {
  void* val_void;
  RETURN_NOT_OK(val_->data_->CheckTypeAndGetPointer(col_.name(),
                                                    col_.type_info()->physical_type(),
                                                    &val_void));

  void* lower_bound = NULL;
  void* upper_bound = NULL;
  switch (op_) {
    case KuduPredicate::LESS_EQUAL:
      upper_bound = val_void;
      break;
    case KuduPredicate::GREATER_EQUAL:
      lower_bound = val_void;
      break;
    case KuduPredicate::EQUAL:
      lower_bound = upper_bound = val_void;
      break;
    default:
      return Status::InvalidArgument(Substitute("invalid comparison op: $0", op_));
  }

  ColumnRangePredicate p(col_, lower_bound, upper_bound);
  spec->AddPredicate(p);

  return Status::OK();
}

} // namespace client
} // namespace kudu
