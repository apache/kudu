// Copyright 2010 Google Inc. All Rights Reserved.
//
// Locking implementation of the atomic primitives, used by
// - memory_order_seq_cst operations on all types
// - all operations on large (>64 bit) types
//
// The atomic_*_internal<> functors (see concurrent/atomic/atomic_internal.h)
// are implemented in terms of atomic_*_locking(Mutex* global,Mutex* local,...)
// functions. The 'global' lock is the shared seq_cst_mutex for the
// memory_order_seq_cst operations and NULL otherwise, and the 'local' lock
// is the lock associated with the object.
//
// IWYU pragma: private, include "concurrent/atomic/atomic.h"
// IWYU pragma: friend concurrent/atomic/.*

#ifndef CONCURRENT_ATOMIC_LOCKING_H_
#define CONCURRENT_ATOMIC_LOCKING_H_

#include <string.h>

#include "gutil/macros.h"
#include "gutil/mutex.h"

namespace concurrent {
namespace detail {

template <typename T> struct atomic_t;
template <typename V, bool L> struct atomic_compare_exchange_strong_internal;
template <typename V, bool L> struct atomic_compare_exchange_weak_internal;
template <typename V, bool L> struct atomic_exchange_internal;
template <typename V, bool L> struct atomic_fetch_add_internal;
template <typename V, bool L> struct atomic_fetch_and_internal;
template <typename V, bool L> struct atomic_fetch_or_internal;
template <typename V, bool L> struct atomic_fetch_xor_internal;
template <typename V, bool L> struct atomic_test_and_set_internal;
template <typename V, bool L> struct atomic_load_internal;
template <typename V, bool L> struct atomic_store_internal;

// Global shared mutex for emulating sequentially consistent memory ordering
extern Mutex seq_cst_mutex;


// Barrier

inline void atomic_thread_fence_seq_cst() {
  seq_cst_mutex.Lock();
  seq_cst_mutex.Unlock();
}


// CAS

template <typename V>
bool atomic_compare_exchange_strong_locking(Mutex* local,
                                            void volatile* p,
                                            void* expected,
                                            V desired) {
  local->Lock();
  if (0 == memcmp(const_cast<void*>(p), expected, sizeof(V))) {
    memcpy(const_cast<void*>(p), &desired, sizeof(V));
    local->Unlock();
    return true;
  } else {
    memcpy(expected, const_cast<void*>(p), sizeof(V));
    local->Unlock();
    return false;
  }
}

template <typename V>
struct atomic_compare_exchange_strong_internal<V, false> {
  inline bool operator()(atomic_t<V> volatile* p,
                         V* expected,
                         V desired,
                         int order) {
    if (order == memory_order_seq_cst) {
      return atomic_compare_exchange_strong_locking
          (const_cast<Mutex*>(&p->m), p->address(), expected, desired);
    } else {
      return atomic_compare_exchange_strong_locking
          (const_cast<Mutex*>(&p->m), p->address(), expected, desired);
    }
  }
};

template <typename V>
struct atomic_compare_exchange_weak_internal<V, false>
    : public atomic_compare_exchange_strong_internal<V, false> {
};


// +=

template <typename V>
V atomic_fetch_add_locking(Mutex* local, void volatile* p, V diff) {
  V old;
  V updated;
  local->Lock();
  memcpy(&old, const_cast<void*>(p), sizeof(V));
  memcpy(&updated, const_cast<void*>(p), sizeof(V));
  updated += diff;
  memcpy(const_cast<void*>(p), &updated, sizeof(V));
  local->Unlock();
  return old;
}

template <typename V>
struct atomic_fetch_add_internal<V, false> {
  inline V operator()(atomic_t<V> volatile* p, V diff, int order) {
    if (order == memory_order_seq_cst) {
      return atomic_fetch_add_locking
          (const_cast<Mutex*>(&p->m), p->address(), diff);
    } else {
      return atomic_fetch_add_locking
          (const_cast<Mutex*>(&p->m), p->address(), diff);
    }
  }
};


// &=

template <typename V>
V atomic_fetch_and_locking(Mutex* local, void volatile* p, V bits) {
  V old;
  V updated;
  local->Lock();
  memcpy(&old, const_cast<void*>(p), sizeof(V));
  memcpy(&updated, const_cast<void*>(p), sizeof(V));
  updated &= bits;
  memcpy(const_cast<void*>(p), &updated, sizeof(V));
  local->Unlock();
  return old;
}

template <typename V>
struct atomic_fetch_and_internal<V, false> {
  inline V operator()(atomic_t<V> volatile* p, V bits, int order) {
    if (order == memory_order_seq_cst) {
      return atomic_fetch_and_locking
          (const_cast<Mutex*>(&p->m), p->address(), bits);
    } else {
      return atomic_fetch_and_locking
          (const_cast<Mutex*>(&p->m), p->address(), bits);
    }
  }
};


// |=

template <typename V>
V atomic_fetch_or_locking(Mutex* local, void volatile* p, V bits) {
  V old;
  V updated;
  local->Lock();
  memcpy(&old, const_cast<void*>(p), sizeof(V));
  memcpy(&updated, const_cast<void*>(p), sizeof(V));
  updated |= bits;
  memcpy(const_cast<void*>(p), &updated, sizeof(V));
  local->Unlock();
  return old;
}

template <typename V>
struct atomic_fetch_or_internal<V, false> {
  inline V operator()(atomic_t<V> volatile* p, V bits, int order) {
    if (order == memory_order_seq_cst) {
      return atomic_fetch_or_locking
          (const_cast<Mutex*>(&p->m), p->address(), bits);
    } else {
      return atomic_fetch_or_locking
          (const_cast<Mutex*>(&p->m), p->address(), bits);
    }
  }
};


// ^=

template <typename V>
V atomic_fetch_xor_locking(Mutex* local, void volatile* p, V bits) {
  V old;
  V updated;
  local->Lock();
  memcpy(&old, const_cast<void*>(p), sizeof(V));
  memcpy(&updated, const_cast<void*>(p), sizeof(V));
  updated ^= bits;
  memcpy(const_cast<void*>(p), &updated, sizeof(V));
  local->Unlock();
  return old;
}

template <typename V>
struct atomic_fetch_xor_internal<V, false> {
  inline V operator()(atomic_t<V> volatile* p, V bits, int order) {
    if (order == memory_order_seq_cst) {
      return atomic_fetch_xor_locking
          (const_cast<Mutex*>(&p->m), p->address(), bits);
    } else {
      return atomic_fetch_xor_locking
          (const_cast<Mutex*>(&p->m), p->address(), bits);
    }
  }
};


// Test and Set

template <typename V>
V atomic_test_and_set_locking(Mutex* local, void volatile* p) {
  V old;
  local->Lock();
  memcpy(&old, const_cast<void*>(p), sizeof(V));
  V updated = true;
  memcpy(const_cast<void*>(p), &updated, sizeof(V));
  local->Unlock();
  return old;
}

template <typename V>
struct atomic_test_and_set_internal<V, false> {
  inline V operator()(atomic_t<V> volatile* p, int order) {
    if (order == memory_order_seq_cst) {
      return atomic_test_and_set_locking<V>
          (const_cast<Mutex*>(&p->m), p->address());
    } else {
      return atomic_test_and_set_locking<V>
          (const_cast<Mutex*>(&p->m), p->address());
    }
  }
};


// Load

template <typename V>
V atomic_load_locking(Mutex* local, void volatile* p) {
  // TODO(user): the std doesn't require a default ctor,
  //               but then we'd need aligned storage...
  V tmp;
  local->Lock();
  memcpy(&tmp, const_cast<void*>(p), sizeof(V));
  local->Unlock();
  return tmp;
}

template <typename V>
struct atomic_load_internal<V, false> {
  inline V operator()(atomic_t<V> volatile const* p, int order) {
    if (order == memory_order_seq_cst) {
      return atomic_load_locking<V>(const_cast<Mutex*>(&p->m), p->address());
    } else {
      return atomic_load_locking<V>(const_cast<Mutex*>(&p->m), p->address());
    }
  }
};


// Exchange

template <typename V>
V atomic_exchange_locking(Mutex *local, void volatile* p, V desired) {
  V tmp;
  local->Lock();
  memcpy(&tmp, const_cast<void*>(p), sizeof(V));
  memcpy(const_cast<void*>(p), &desired, sizeof(V));
  local->Unlock();
  return tmp;
}

template <typename V>
struct atomic_exchange_internal<V, false> {
  inline V operator()(atomic_t<V> volatile* p, V desired, int order) {
    if (order == memory_order_seq_cst) {
      return atomic_exchange_locking(const_cast<Mutex*>(&p->m),
                                     p->address(),
                                     desired);
    } else {
      return atomic_exchange_locking(const_cast<Mutex*>(&p->m),
                                     p->address(),
                                     desired);
    }
  }
};


// Store

template <typename V>
void atomic_store_locking(Mutex *local, void volatile* p, V value) {
  local->Lock();
  memcpy(const_cast<void*>(p), &value, sizeof(V));
  local->Unlock();
}

template <typename V>
struct atomic_store_internal<V, false> {
  inline void operator()(atomic_t<V> volatile* p, V value, int order) {
    if (order == memory_order_seq_cst) {
      atomic_store_locking(const_cast<Mutex*>(&p->m), p->address(), value);
    } else {
      atomic_store_locking(const_cast<Mutex*>(&p->m), p->address(), value);
    }
  }
};


}  // namespace detail
}  // namespace concurrent

#endif  // CONCURRENT_ATOMIC_LOCKING_H_
