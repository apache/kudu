// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#ifdef __GLIBCXX__
#include <ext/alloc_traits.h>  // IWYU pragma: export
#endif

#include <memory>

// It isn't possible to use std::make_shared() with a class that has private
// constructors. Moreover, the standard workarounds are inelegant when said
// class has non-default constructors. As such, we employ a simple solution:
// declare the class as a friend to std::make_shared()'s internal allocator.
// This approach is non-portable and must be implemented separately for each
// supported STL implementation.
//
// Note: due to friendship restrictions on partial template specialization,
// it isn't possible to befriend just the allocation function; the entire
// allocator class must be befriended.
//
// See http://stackoverflow.com/q/8147027 for a longer discussion.

#ifdef __GLIBCXX__
  // In libstdc++, new_allocator is defined as a class (ext/new_allocator.h)
  // but forward declared as a struct (ext/alloc_traits.h). Clang complains
  // about this when -Wmismatched-tags is set, which gcc doesn't support
  // (which probably explains why the discrepancy exists in the first place).
  // We can temporarily disable this warning via pragmas [1], but we must
  // not expose them to gcc due to its poor handling of the _Pragma() C99
  // operator [2].
  //
  // 1. http://clang.llvm.org/docs/UsersManual.html#controlling-diagnostics-via-pragmas
  // 2. https://gcc.gnu.org/bugzilla/show_bug.cgi?id=60875
  #ifdef __clang__
    #define ALLOW_MAKE_SHARED(T)                                \
      _Pragma("clang diagnostic push")                          \
      _Pragma("clang diagnostic ignored \"-Wmismatched-tags\"") \
      friend class __gnu_cxx::new_allocator<T>                  \
      _Pragma("clang diagnostic pop")
  #else
    #define ALLOW_MAKE_SHARED(T) \
      friend class __gnu_cxx::new_allocator<T>
  #endif
#elif defined(_LIBCPP_VERSION)
  #define ALLOW_MAKE_SHARED(T) \
    friend class std::__1::__libcpp_compressed_pair_imp<std::__1::allocator<T>, T, 1>
#else
  #error "Need to implement ALLOW_MAKE_SHARED for your platform!"
#endif
