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

#include <map>
#include <string>

#include "kudu/fs/block_manager_util.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/gutil/callback_forward.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/status.h"

namespace kudu {
namespace fs {

// Callback to error-handling code. The input string is the UUID a failed
// component.
//
// e.g. the ErrorNotificationCb for disk failure handling takes the UUID of a
// directory, marks it failed, and shuts down the tablets in that directory.
typedef Callback<void(const std::string&)> ErrorNotificationCb;
static void DoNothingErrorNotification(const std::string& /* uuid */) {}

// Evaluates the expression and handles it if it results in an error.
// Returns if the status is an error.
#define RETURN_NOT_OK_HANDLE_ERROR(status_expr) do { \
  const Status& _s = (status_expr); \
  if (PREDICT_TRUE(_s.ok())) { \
    break; \
  } \
  HandleError(_s); \
  return _s; \
} while (0);

// Evaluates the expression and runs 'err_handler' if it results in a disk
// failure. Returns if the expression results in an error.
#define RETURN_NOT_OK_HANDLE_DISK_FAILURE(status_expr, err_handler) do { \
  const Status& _s = (status_expr); \
  if (PREDICT_TRUE(_s.ok())) { \
    break; \
  } \
  if (_s.IsDiskFailure()) { \
    (err_handler); \
  } \
  return _s; \
} while (0);

// Evaluates the expression and runs 'err_handler' if it results in a disk
// failure.
#define HANDLE_DISK_FAILURE(status_expr, err_handler) do { \
  const Status& _s = (status_expr); \
  if (PREDICT_FALSE(_s.IsDiskFailure())) { \
    (err_handler); \
  } \
} while (0);

// When certain operations fail, the side effects of the error can span
// multiple layers, many of which we prefer to keep separate. The FsErrorManager
// registers and runs error handlers without adding cross-layer dependencies.
//
// e.g. the TSTabletManager registers a callback to handle disk failure.
// Blocks and other entities that may hit disk failures can call it without
// knowing about the TSTabletManager.
class FsErrorManager {
 public:
  FsErrorManager()
    : notify_cb_(Bind(DoNothingErrorNotification)) {}

  // Sets the error notification callback.
  //
  // This should be called when the callback's callee is initialized.
  void SetErrorNotificationCb(ErrorNotificationCb cb) {
    notify_cb_ = std::move(cb);
  }

  // Resets the error notification callback.
  //
  // This must be called before the callback's callee is destroyed.
  void UnsetErrorNotificationCb() {
    notify_cb_ = Bind(DoNothingErrorNotification);
  }

  // Runs the error notification callback.
  //
  // 'uuid' is the full UUID of the component that failed.
  void RunErrorNotificationCb(const std::string& uuid) const {
    notify_cb_.Run(uuid);
  }

  // Runs the error notification callback with the UUID of 'dir'.
  void RunErrorNotificationCb(const DataDir* dir) const {
    notify_cb_.Run(dir->instance()->metadata()->path_set().uuid());
  }

 private:
   // Callback to be run when an error occurs.
   ErrorNotificationCb notify_cb_;
};

}  // namespace fs
}  // namespace kudu
