// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/common/id_mapping.h"

#include "kudu/util/malloc.h"

namespace kudu {

size_t IdMapping::memory_footprint_excluding_this() const {
  return kudu_malloc_usable_size(entries_.data());
}

size_t IdMapping::memory_footprint_including_this() const {
  return kudu_malloc_usable_size(this) + memory_footprint_excluding_this();
}

} // namespace kudu
