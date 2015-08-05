// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/value.h"
#include "kudu/client/value-internal.h"
#include "kudu/gutil/strings/substitute.h"
#include <string>

using std::string;
using strings::Substitute;

namespace kudu {
namespace client {

KuduValue::KuduValue(Data* d)
: data_(d) {
}

KuduValue::~KuduValue() {
  if (data_->type_ == Data::SLICE) {
    delete[] data_->slice_val_.data();
  }
  delete data_;
}

KuduValue* KuduValue::Clone() const {
  switch (data_->type_) {
    case Data::INT:
      return KuduValue::FromInt(data_->int_val_);
    case Data::DOUBLE:
      return KuduValue::FromDouble(data_->double_val_);
    case Data::FLOAT:
      return KuduValue::FromFloat(data_->float_val_);
    case Data::SLICE:
      return KuduValue::CopyString(data_->slice_val_);
  }
  LOG(FATAL);
}

KuduValue* KuduValue::FromInt(int64_t v) {
  Data* d = new Data;
  d->type_ = Data::INT;
  d->int_val_ = v;

  return new KuduValue(d);
}

KuduValue* KuduValue::FromDouble(double v) {
  Data* d = new Data;
  d->type_ = Data::DOUBLE;
  d->double_val_ = v;

  return new KuduValue(d);
}


KuduValue* KuduValue::FromFloat(float v) {
  Data* d = new Data;
  d->type_ = Data::FLOAT;
  d->float_val_ = v;

  return new KuduValue(d);
}

KuduValue* KuduValue::FromBool(bool v) {
  Data* d = new Data;
  d->type_ = Data::INT;
  d->int_val_ = v ? 1 : 0;

  return new KuduValue(d);
}

KuduValue* KuduValue::CopyString(Slice s) {
  uint8_t* copy = new uint8_t[s.size()];
  memcpy(copy, s.data(), s.size());

  Data* d = new Data;
  d->type_ = Data::SLICE;
  d->slice_val_ = Slice(copy, s.size());

  return new KuduValue(d);
}

Status KuduValue::Data::CheckTypeAndGetPointer(const string& col_name,
                                               DataType t,
                                               void** val_void) {
  const TypeInfo* ti = GetTypeInfo(t);
  switch (t) {
    case kudu::INT8:
    case kudu::INT16:
    case kudu::INT32:
    case kudu::INT64:
      RETURN_NOT_OK(CheckAndPointToInt(col_name, ti->size(), val_void));
      break;

    case kudu::BOOL:
      RETURN_NOT_OK(CheckAndPointToBool(col_name, val_void));
      break;

    case kudu::FLOAT:
      RETURN_NOT_OK(CheckValType(col_name, KuduValue::Data::FLOAT, "float"));
      *val_void = &float_val_;
      break;

    case kudu::DOUBLE:
      RETURN_NOT_OK(CheckValType(col_name, KuduValue::Data::DOUBLE, "double"));
      *val_void = &double_val_;
      break;

    case kudu::BINARY:
      RETURN_NOT_OK(CheckAndPointToString(col_name, val_void));
      break;

    default:
      return Status::InvalidArgument(Substitute("cannot determine value for column $0 (type $1)",
                                                col_name, ti->name()));
  }
  return Status::OK();
}

Status KuduValue::Data::CheckValType(const string& col_name,
                                     KuduValue::Data::Type type,
                                     const char* type_str) const {
  if (type_ != type) {
    return Status::InvalidArgument(
        Substitute("non-$0 value for $0 column $1", type_str, col_name));
  }
  return Status::OK();
}

Status KuduValue::Data::CheckAndPointToBool(const string& col_name,
                                            void** val_void) {
  RETURN_NOT_OK(CheckValType(col_name, KuduValue::Data::INT, "bool"));
  int64_t int_val = int_val_;
  if (int_val != 0 && int_val != 1) {
    return Status::InvalidArgument(
        Substitute("value $0 out of range for boolean column '$1'",
                   int_val, col_name));
  }
  *val_void = &int_val_;
  return Status::OK();
}

Status KuduValue::Data::CheckAndPointToInt(const string& col_name,
                                           size_t int_size,
                                           void** val_void) {
  RETURN_NOT_OK(CheckValType(col_name, KuduValue::Data::INT, "int"));

  int64_t int_min, int_max;
  if (int_size == 8) {
    int_min = MathLimits<int64_t>::kMin;
    int_max = MathLimits<int64_t>::kMax;
  } else {
    size_t int_bits = int_size * 8 - 1;
    int_max = (1LL << int_bits) - 1;
    int_min = -int_max - 1;
  }

  int64_t int_val = int_val_;
  if (int_val < int_min || int_val > int_max) {
    return Status::InvalidArgument(
        Substitute("value $0 out of range for $1-bit signed integer column '$2'",
                   int_val, int_size * 8, col_name));
  }

  *val_void = &int_val_;
  return Status::OK();
}

Status KuduValue::Data::CheckAndPointToString(const string& col_name,
                                              void** val_void) {
  RETURN_NOT_OK(CheckValType(col_name, KuduValue::Data::SLICE, "string"));
  *val_void = &slice_val_;
  return Status::OK();
}

} // namespace client
} // namespace kudu
