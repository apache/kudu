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

#include <functional>
#include <utility>

namespace kudu {
namespace util {

// This class allows for calling of the specified void functions in its
// constructor and destructor correspondingly, which makes it helpful
// in building initialization and wrap-up sequencing within a namespace
// scope of the same translation unit. By the C++ standard, objects defined
// at namespace scope are guaranteed to be initialized in an order in strict
// accordance with that of their definitions in a given translation unit,
// and the destructors are invoked in the reverse order. This applies
// to the global namespace scope of a single translation unit as well.
class EntryExitWrapper final {
 public:
  EntryExitWrapper(std::function<void()> init_func,
                   std::function<void()> fini_func)
      : init_func_(std::move(init_func)),
        fini_func_(std::move(fini_func)) {
    if (init_func_) {
      init_func_();
    }
  }

  ~EntryExitWrapper() {
    if (fini_func_) {
      fini_func_();
    }
  }

 private:
   const std::function<void()> init_func_;
   const std::function<void()> fini_func_;
};

} // namespace util
} // namespace kudu
