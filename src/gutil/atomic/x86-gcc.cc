// Copyright 2010 Google Inc. All Rights Reserved.

// ThreadSanitizer provides own implementation of atomics.
#ifndef THREAD_SANITIZER

#include "gutil/atomic/atomic.h"  // IWYU pragma: keep
// IWYU pragma: no_include "concurrent/atomic/atomic_internal.h"
// IWYU pragma: no_include "concurrent/atomic/x86-gcc.h"

#include "gutil/atomicops.h"
#include <glog/logging.h>
#include "gutil/logging-inl.h"
#include "gutil/macros.h"

namespace concurrent {

namespace detail {

namespace {

COMPILE_ASSERT(builtin_type<uintptr_t>::value, not_builtin);
COMPILE_ASSERT(atomic_t<uintptr_t>::is_lockfree, not_lockfree);
COMPILE_ASSERT(atomic_t<uintptr_t>::same_size, not_samesize);

COMPILE_ASSERT(builtin_type<AtomicWord>::value, not_builtin);
COMPILE_ASSERT(atomic_t<AtomicWord>::is_lockfree, not_lockfree);
COMPILE_ASSERT(atomic_t<AtomicWord>::same_size, not_samesize);

COMPILE_ASSERT(builtin_type<base::subtle::Atomic64>::value, not_builtin);
COMPILE_ASSERT(atomic_t<base::subtle::Atomic64>::is_lockfree, not_lockfree);
COMPILE_ASSERT(atomic_t<base::subtle::Atomic64>::same_size, not_samesize);

}  // namespace

void DieOfBadMemoryOrder(int order) {
  LOG(FATAL) << "Invalid memory_order " << order;
}

}  // namespace detail
}  // namespace concurrent

#endif  // #ifndef THREAD_SANITIZER
