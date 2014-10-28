// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_COMMON_ROWID_H
#define KUDU_COMMON_ROWID_H

#include <inttypes.h>

#include "kudu/util/memcmpable_varint.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"

namespace kudu {

// Type to represent the ordinal ID of a row within a RowSet.
// This type should be used instead of uint32_t when referring to row indexes
// for better clarity.
//
// TODO: Currently we only support up to 4B rows per RowSet - some work
// is necessary to support larger RowSets without overflow.
typedef uint32_t rowid_t;

// Substitution to use in printf() format arguments.
#define ROWID_PRINT_FORMAT PRIu32

// Serialize a rowid into the 'dst' buffer.
// The serialized form of row IDs is comparable using memcmp().
inline void EncodeRowId(faststring *dst, rowid_t rowid) {
  PutMemcmpableVarint64(dst, rowid);
}


// Decode a varint-encoded rowid from the given Slice, mutating the
// Slice to advance past the decoded data upon return.
//
// Returns false if the Slice is too short.
inline bool DecodeRowId(Slice *s, rowid_t *rowid) {
  uint64_t tmp;
  bool ret = GetMemcmpableVarint64(s, &tmp);
  DCHECK_LT(tmp, 1ULL << 32);
  *rowid = tmp;
  return ret;
}

} // namespace kudu

#endif
