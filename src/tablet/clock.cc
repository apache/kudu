// Copyright (c) 2013, Cloudera, inc.

#include "tablet/clock.h"

#include "util/memcmpable_varint.h"
#include "util/status.h"
#include "gutil/strings/substitute.h"
#include "gutil/mathlimits.h"

namespace kudu {
namespace tablet {

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

void Timestamp::EncodeToString(string* encode_to) const {
  faststring buf;
  EncodeTo(&buf);
  encode_to->append(reinterpret_cast<const char*>(buf.data()), buf.size());
}

Status Timestamp::DecodeFromString(const string& decode_from) {
  Slice slice(decode_from);
  if (!DecodeFrom(&slice)) {
    return Status::Corruption("Cannot decode timestamp.");
  }
  return Status::OK();
}

}  // namespace tablet
}  // namespace kudu

