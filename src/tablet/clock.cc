// Copyright (c) 2013, Cloudera, inc.

#include "tablet/clock.h"

#include "util/memcmpable_varint.h"
#include "util/status.h"
#include "gutil/strings/substitute.h"
#include "gutil/mathlimits.h"

namespace kudu {
namespace tablet {

const txid_t txid_t::kMin(MathLimits<txid_t::val_type>::kMin);
const txid_t txid_t::kMax(MathLimits<txid_t::val_type>::kMax);
const txid_t txid_t::kInitialTxId(MathLimits<txid_t::val_type>::kMin + 1);
const txid_t txid_t::kInvalidTxId(MathLimits<txid_t::val_type>::kMax - 1);

bool txid_t::DecodeFrom(Slice *input) {
  return GetMemcmpableVarint64(input, &v);
}

void txid_t::EncodeTo(faststring *dst) const {
  PutMemcmpableVarint64(dst, v);
}

int txid_t::CompareTo(const txid_t &other) const {
  if (v < other.v) {
    return -1;
  } else if (v > other.v) {
    return 1;
  }
  return 0;
}

string txid_t::ToString() const {
  return strings::Substitute("$0", v);
}

void txid_t::EncodeToString(string* encode_to) const {
  faststring buf;
  EncodeTo(&buf);
  encode_to->append(reinterpret_cast<const char*>(buf.data()), buf.size());
}

Status txid_t::DecodeFromString(const string& decode_from) {
  Slice slice(decode_from);
  if (!DecodeFrom(&slice)) {
    return Status::Corruption("Cannot decode txid.");
  }
  return Status::OK();
}

}  // namespace tablet
}  // namespace kudu

