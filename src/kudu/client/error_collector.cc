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

#include "kudu/client/client.h"
#include "kudu/client/error_collector.h"

#include <mutex>
#include <vector>

#include "kudu/gutil/stl_util.h"

namespace kudu {
namespace client {
namespace internal {

ErrorCollector::~ErrorCollector() {
  STLDeleteElements(&errors_);
}

void ErrorCollector::AddError(gscoped_ptr<KuduError> error) {
  std::lock_guard<simple_spinlock> l(lock_);
  errors_.push_back(error.release());
}

size_t ErrorCollector::CountErrors() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return errors_.size();
}

void ErrorCollector::GetErrors(std::vector<KuduError*>* errors, bool* overflowed) {
  std::lock_guard<simple_spinlock> l(lock_);
  if (errors != nullptr) {
    errors->clear();
    errors->swap(errors_);
  }
  if (overflowed != nullptr) {
    *overflowed = false;
  }
}

} // namespace internal
} // namespace client
} // namespace kudu
