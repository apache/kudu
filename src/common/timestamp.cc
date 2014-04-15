// Copyright (c) 2013, Cloudera, inc.

#include "common/timestamp.h"

#include "util/faststring.h"
#include "util/memcmpable_varint.h"
#include "util/slice.h"
#include "util/status.h"
#include "gutil/strings/substitute.h"
#include "gutil/mathlimits.h"

namespace kudu {

const Timestamp Timestamp::kMin(MathLimits<Timestamp::val_type>::kMin);
const Timestamp Timestamp::kMax(MathLimits<Timestamp::val_type>::kMax);
const Timestamp Timestamp::kInitialTimestamp(MathLimits<Timestamp::val_type>::kMin + 1);
const Timestamp Timestamp::kInvalidTimestamp(MathLimits<Timestamp::val_type>::kMax - 1);

bool Timestamp::DecodeFrom(Slice *input) {
  return GetMemcmpableVarint64(input, &v);
}

void Timestamp::EncodeTo(faststring *dst) const {
  PutMemcmpableVarint64(dst, v);
}

int Timestamp::CompareTo(const Timestamp &other) const {
  if (v < other.v) {
    return -1;
  } else if (v > other.v) {
    return 1;
  }
  return 0;
}

string Timestamp::ToString() const {
  return strings::Substitute("$0", v);
}

uint64_t Timestamp::ToUint64() const {
  return v;
}

Status Timestamp::FromUint64(uint64_t value) {
  v = value;
  return Status::OK();
}

}  // namespace kudu


