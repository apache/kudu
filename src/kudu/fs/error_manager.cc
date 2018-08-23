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

#include <mutex>
#include <string>
#include <utility>

#include "kudu/gutil/bind.h"
#include "kudu/gutil/map-util.h"

using std::string;

namespace kudu {
namespace fs {

// Default error-handling callback that no-ops.
static void DoNothingErrorNotification(const string& /* uuid */) {}

FsErrorManager::FsErrorManager() {
  InsertOrDie(&callbacks_, ErrorHandlerType::DISK_ERROR, Bind(DoNothingErrorNotification));
  InsertOrDie(&callbacks_, ErrorHandlerType::NO_AVAILABLE_DISKS, Bind(DoNothingErrorNotification));
}

void FsErrorManager::SetErrorNotificationCb(ErrorHandlerType e, ErrorNotificationCb cb) {
  std::lock_guard<Mutex> l(lock_);
  EmplaceOrUpdate(&callbacks_, e, std::move(cb));
}

void FsErrorManager::UnsetErrorNotificationCb(ErrorHandlerType e) {
  std::lock_guard<Mutex> l(lock_);
  EmplaceOrUpdate(&callbacks_, e, Bind(DoNothingErrorNotification));
}

void FsErrorManager::RunErrorNotificationCb(ErrorHandlerType e, const string& uuid) const {
  std::lock_guard<Mutex> l(lock_);
  FindOrDie(callbacks_, e).Run(uuid);
}

}  // namespace fs
}  // namespace kudu
