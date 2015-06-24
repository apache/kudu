// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/util/malloc.h"

#include <malloc.h>

namespace kudu {

int64_t kudu_malloc_usable_size(const void* obj) {
  return malloc_usable_size(const_cast<void*>(obj));
}

} // namespace kudu
