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

#include "kudu/client/error_collector.h"

#include <mutex>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/error-internal.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

using strings::Substitute;

namespace kudu {
namespace client {
namespace internal {

ErrorCollector::ErrorCollector()
    : max_mem_size_bytes_(kMemSizeNoLimit),
      mem_size_bytes_(0),
      dropped_errors_cnt_(0) {
}

ErrorCollector::~ErrorCollector() {
  STLDeleteElements(&errors_);
}

Status ErrorCollector::SetMaxMemSize(size_t size_bytes) {
  std::lock_guard<simple_spinlock> l(lock_);
  if (dropped_errors_cnt_ > 0) {
    // The error collector has dropped some errors already: do not allow
    // to change the limit on memory size in this case at all. We want
    // to preserve consistency in the set of accumulated errors -- do not
    // allow a situation when the accumulated errors contain 'holes'.
    return Status::IllegalState(
        "cannot set new limit: already dropped some errors");
  }
  if (size_bytes != kMemSizeNoLimit && size_bytes < mem_size_bytes_) {
    // Do not overlow the error collector post-factum.
    return Status::IllegalState(
        Substitute("new limit of $0 bytes: already accumulated errors "
                   "for $1 bytes", size_bytes, mem_size_bytes_));
  }
  max_mem_size_bytes_ = size_bytes;

  return Status::OK();
}

void ErrorCollector::AddError(gscoped_ptr<KuduError> error) {
  std::lock_guard<simple_spinlock> l(lock_);
  const size_t error_size_bytes = error->data_->failed_op_->SizeInBuffer();

  // If the maximum limit on the memory size is set, check whether the
  // total-size-to-be would be greater than the specified limit after adding
  // a new item into the collection: if yes, then drop this and all subsequent
  // errors.
  //
  // Once an error has been dropped, drop all the subsequent ones even if they
  // would fit into the available memory -- this is to preserve consistent
  // sequencing of the accumulated errors.
  if (max_mem_size_bytes_ != kMemSizeNoLimit &&
      (dropped_errors_cnt_ > 0 ||
       error_size_bytes + mem_size_bytes_ > max_mem_size_bytes_)) {
    ++dropped_errors_cnt_;
    return;
  }
  mem_size_bytes_ += error_size_bytes;
  errors_.push_back(error.release());
}

size_t ErrorCollector::CountErrors() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return errors_.size() + dropped_errors_cnt_;
}

void ErrorCollector::GetErrors(std::vector<KuduError*>* errors,
                               bool* overflowed) {
  std::lock_guard<simple_spinlock> l(lock_);
  if (overflowed != nullptr) {
    *overflowed = (dropped_errors_cnt_ != 0);
  }
  if (errors != nullptr) {
    errors->clear();
    errors->swap(errors_);
  } else {
    STLDeleteElements(&errors_);
  }
  dropped_errors_cnt_ = 0;
  mem_size_bytes_ = 0;
}

} // namespace internal
} // namespace client
} // namespace kudu
