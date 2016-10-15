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

#ifndef KUDU_CLIENT_SHARED_PTR_H
#define KUDU_CLIENT_SHARED_PTR_H

/// @file shared_ptr.h
/// @brief Smart pointer typedefs for externally-faced code.
///
/// Kudu uses c++11 features internally, but provides a client interface which
/// does not require c++11. We use std::tr1::shared_ptr in our public interface
/// to hold shared instances of KuduClient, KuduSession, and KuduTable.
///
/// However, if building with libc++ (e.g. if building on macOS), the TR1 APIs
/// are not implemented. As a workaround, we use std::shared_ptr with libc++.
///
/// In order to allow applications to compile against Kudu with libstdc++ as
/// well as with libc++, macros are provided that will resolve to the correct
/// namespace in either case. Clients are encouraged to use these macros in
/// order to ensure that applications compile universally.

// This include is not used directly, but we need to include some C++ header in
// order to ensure the _LIBCPP_VERSION macro is defined appropriately.
#include <string>

#if defined(_LIBCPP_VERSION)

#include <memory> // IWYU pragma: export

namespace kudu {
namespace client {
namespace sp {
  using std::shared_ptr;
  using std::weak_ptr;
  using std::enable_shared_from_this;
}
}
}

#else
#include <tr1/memory> // IWYU pragma: export

namespace kudu {
namespace client {
namespace sp {
  using std::tr1::shared_ptr;
  using std::tr1::weak_ptr;
  using std::tr1::enable_shared_from_this;
}
}
}
#endif

#endif // define KUDU_CLIENT_SHARED_PTR_H
