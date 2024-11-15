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

#include "kudu/fs/error_manager.h"

#include <string>
#include <utility>

using std::string;

namespace kudu {
namespace fs {

// Default error-handling callback that no-ops.
static void DoNothingErrorNotification(const string& /* uuid */,
                                       const string& /* tenant_id */) {}

FsErrorManager::FsErrorManager() {
  callbacks_.fill(&DoNothingErrorNotification);
}

void FsErrorManager::SetErrorNotificationCb(ErrorHandlerType e, ErrorNotificationCb cb) {
  DCHECK_LT(e, callbacks_.max_size());
  std::lock_guard l(lock_);
  callbacks_[e] = std::move(cb);
}

void FsErrorManager::UnsetErrorNotificationCb(ErrorHandlerType e) {
  DCHECK_LT(e, callbacks_.max_size());
  std::lock_guard l(lock_);
  callbacks_[e] = &DoNothingErrorNotification;
}

void FsErrorManager::RunErrorNotificationCb(ErrorHandlerType e,
                                            const string& uuid,
                                            const string& tenant_id) const {
  DCHECK_LT(e, callbacks_.max_size());
  std::lock_guard l(lock_);
  callbacks_[e](uuid, tenant_id);
}

}  // namespace fs
}  // namespace kudu
