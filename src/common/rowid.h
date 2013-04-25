// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_COMMON_ROWID_H
#define KUDU_COMMON_ROWID_H

namespace kudu {

// Type to represent the ordinal ID of a row within a layer.
// This type should be used instead of uint32_t when referring to row indexes
// for better clarity.
//
// TODO: Currently we only support up to 4B rows per layer - some work
// is necessary to support larger layers without overflow.
typedef uint32_t rowid_t;

} // namespace kudu

#endif
