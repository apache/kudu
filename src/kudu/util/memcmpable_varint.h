// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// This is an alternate varint format, borrowed from sqlite4, that differs from the
// varint in util/coding.h in that its serialized form can be compared with memcmp(),
// yielding the same result as comparing the original integers.
//
// The serialized form also has the property that multiple such varints can be strung
// together to form a composite key, which itself is memcmpable.
//
// See memcmpable_varint.cc for further description.

#ifndef KUDU_UTIL_MEMCMPABLE_VARINT_H
#define KUDU_UTIL_MEMCMPABLE_VARINT_H

#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"

namespace kudu {

void PutMemcmpableVarint64(faststring *dst, uint64_t value);

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.
bool GetMemcmpableVarint64(Slice *input, uint64_t *value);

} // namespace kudu

#endif
