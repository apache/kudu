// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/once.h"

#include "kudu/util/malloc.h"

namespace kudu {

size_t KuduOnceDynamic::memory_footprint_excluding_this() const {
  return status_.memory_footprint_excluding_this();
}

size_t KuduOnceDynamic::memory_footprint_including_this() const {
  return kudu_malloc_usable_size(this) + memory_footprint_excluding_this();
}

} // namespace kudu
