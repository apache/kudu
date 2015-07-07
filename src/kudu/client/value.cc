// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/value.h"
#include "kudu/client/value-internal.h"

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


} // namespace client
} // namespace kudu
