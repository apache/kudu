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

#include <memory>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {
namespace client {

class ClientUnitTest_TestErrorCollector_Test;
class KuduError;
class KuduInsert;

namespace internal {

class ErrorCollector : public RefCountedThreadSafe<ErrorCollector> {
 public:
  static const size_t kMemSizeNoLimit = 0;

  ErrorCollector();

  // See KuduSession::SetErrorBufferSpace() for details.
  Status SetMaxMemSize(size_t size_bytes);

  virtual void AddError(std::unique_ptr<KuduError> error);

  // See KuduSession for details.
  size_t CountErrors() const;

  // See KuduSession for details.
  void GetErrors(std::vector<KuduError*>* errors, bool* overflowed);

 protected:
  virtual ~ErrorCollector();

 private:
  friend class ::kudu::client::ClientUnitTest_TestErrorCollector_Test;
  friend class RefCountedThreadSafe<ErrorCollector>;

  mutable simple_spinlock lock_;
  std::vector<KuduError*> errors_;
  size_t max_mem_size_bytes_;
  size_t mem_size_bytes_;
  size_t dropped_errors_cnt_;

  DISALLOW_COPY_AND_ASSIGN(ErrorCollector);
};

} // namespace internal
} // namespace client
} // namespace kudu
