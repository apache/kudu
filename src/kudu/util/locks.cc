// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/locks.h"

#include "kudu/util/malloc.h"

namespace kudu {

size_t percpu_rwlock::memory_footprint_excluding_this() const {
  // Because locks_ is a dynamic array of non-trivially-destructable types,
  // the returned pointer from new[] isn't guaranteed to point at the start of
  // a memory block, rendering it useless for malloc_usable_size().
  //
  // Rather than replace locks_ with a vector or something equivalent, we'll
  // just measure the memory footprint using sizeof(), with the understanding
  // that we might be inaccurate due to malloc "slop".
  //
  // See https://code.google.com/p/address-sanitizer/issues/detail?id=395 for
  // more details.
  return n_cpus_ * sizeof(padded_lock);
}

size_t percpu_rwlock::memory_footprint_including_this() const {
  return kudu_malloc_usable_size(this) + memory_footprint_excluding_this();
}

} // namespace kudu
