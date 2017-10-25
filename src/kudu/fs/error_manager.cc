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

#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/fs/error_manager.h"
#include "kudu/gutil/bind.h"

using std::string;

namespace kudu {

namespace fs {

// Default error-handling callback that no-ops.
static void DoNothingErrorNotification(const string& /* uuid */) {}

FsErrorManager::FsErrorManager() :
  disk_cb_(Bind(DoNothingErrorNotification)),
  tablet_cb_(Bind(DoNothingErrorNotification)) {}

void FsErrorManager::SetErrorNotificationCb(ErrorHandlerType e, ErrorNotificationCb cb) {
  std::lock_guard<Mutex> l(lock_);
  switch (e) {
    case ErrorHandlerType::DISK:
      disk_cb_ = std::move(cb);
      return;
    case ErrorHandlerType::TABLET:
      tablet_cb_ = std::move(cb);
      return;
    default:
      LOG(FATAL) << "Unknown error handler type!";
  }
}

void FsErrorManager::UnsetErrorNotificationCb(ErrorHandlerType e) {
  std::lock_guard<Mutex> l(lock_);
  switch (e) {
    case ErrorHandlerType::DISK:
      disk_cb_ = Bind(DoNothingErrorNotification);
      return;
    case ErrorHandlerType::TABLET:
      tablet_cb_ = Bind(DoNothingErrorNotification);
      return;
    default:
      LOG(FATAL) << "Unknown error handler type!";
  }
}

void FsErrorManager::RunErrorNotificationCb(ErrorHandlerType e, const string& uuid) const {
  std::lock_guard<Mutex> l(lock_);
  switch (e) {
    case ErrorHandlerType::DISK:
      disk_cb_.Run(uuid);
      return;
    case ErrorHandlerType::TABLET:
      tablet_cb_.Run(uuid);
      return;
    default:
      LOG(FATAL) << "Unknown error handler type!";
  }
}

}  // namespace fs
}  // namespace kudu
