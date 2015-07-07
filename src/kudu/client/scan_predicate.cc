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


ComparisonPredicateData::ComparisonPredicateData(const ColumnSchema& col,
                                                 KuduPredicate::ComparisonOp op,
                                                 KuduValue* val) :
  col_(col),
  op_(op),
  val_(val) {
}
ComparisonPredicateData::~ComparisonPredicateData() {
}

Status ComparisonPredicateData::CheckValType(KuduValue::Data::Type type,
                                             const char* type_str) const {
  if (val_->data_->type_ != type) {
    return Status::InvalidArgument(
        Substitute("non-$0 predicate on $1 column", type_str, type_str),
        col_.name());
  }
  return Status::OK();
}

Status ComparisonPredicateData::CheckAndPointToBool(void** val_void) {
  RETURN_NOT_OK(CheckValType(KuduValue::Data::INT, "bool"));
  int64_t int_val = val_->data_->int_val_;
  if (int_val != 0 && int_val != 1) {
    return Status::InvalidArgument(
        Substitute("predicate value $0 out of range for boolean column '$1'",
                   int_val, col_.name()));
  }
  *val_void = &val_->data_->int_val_;
  return Status::OK();
}

Status ComparisonPredicateData::CheckAndPointToInt(size_t int_size,
                                                   void** val_void) {
  RETURN_NOT_OK(CheckValType(KuduValue::Data::INT, "int"));

  int64_t int_min, int_max;
  if (int_size == 8) {
    int_min = MathLimits<int64_t>::kMin;
    int_max = MathLimits<int64_t>::kMax;
  } else {
    size_t int_bits = int_size * 8 - 1;
    int_max = (1LL << int_bits) - 1;
    int_min = -int_max - 1;
  }

  int64_t int_val = val_->data_->int_val_;
  if (int_val < int_min || int_val > int_max) {
    return Status::InvalidArgument(
        Substitute("predicate value $0 out of range for $1-bit signed integer column '$2'",
                   int_val, int_size * 8, col_.name()));
  }

  *val_void = &val_->data_->int_val_;
  return Status::OK();
}

Status ComparisonPredicateData::CheckAndPointToString(void** val_void) {
  RETURN_NOT_OK(CheckValType(KuduValue::Data::SLICE, "string"));
  *val_void = &val_->data_->slice_val_;
  return Status::OK();
}

Status ComparisonPredicateData::AddToScanSpec(ScanSpec* spec) {
  void* val_void;
  switch (col_.type_info()->type()) {
    case INT8:
    case INT16:
    case INT32:
    case INT64:
      RETURN_NOT_OK(CheckAndPointToInt(col_.type_info()->size(), &val_void));
      break;

    case BOOL:
      RETURN_NOT_OK(CheckAndPointToBool(&val_void));
      break;

    case FLOAT:
      RETURN_NOT_OK(CheckValType(KuduValue::Data::FLOAT, "float"));
      val_void = &val_->data_->float_val_;
      break;

    case DOUBLE:
      RETURN_NOT_OK(CheckValType(KuduValue::Data::DOUBLE, "double"));
      val_void = &val_->data_->double_val_;
      break;

    case STRING:
      RETURN_NOT_OK(CheckAndPointToString(&val_void));
      break;
    default:
      return Status::InvalidArgument("cannot apply predicate on type",
                                     col_.type_info()->name());
  }

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
