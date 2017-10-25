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

#include <string>

#include <glog/logging.h>

#include "kudu/fs/block_manager_util.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/port.h"
#include "kudu/util/mutex.h"

namespace kudu {
namespace fs {

// Callback to error-handling code. The input string is the UUID a failed
// component.
//
// e.g. the ErrorNotificationCb for disk failure handling takes the UUID of a
// directory, marks it failed, and shuts down the tablets in that directory.
typedef Callback<void(const std::string&)> ErrorNotificationCb;

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

enum ErrorHandlerType {
  // For errors that affect a disk and all of its tablets (e.g. disk failure).
  DISK,

  // For errors that affect a single tablet (e.g. failure to create a block).
  TABLET
};

// When certain operations fail, the side effects of the error can span multiple
// layers, many of which we prefer to keep separate. The FsErrorManager
// registers and runs error handlers without adding cross-layer dependencies.
// Additionally, it enforces one callback is run at a time, and that each
// callback fully completes before returning.
//
// e.g. the TSTabletManager registers a callback to handle disk failure.
// Blocks and other entities that may hit disk failures can call it without
// knowing about the TSTabletManager.
class FsErrorManager {
 public:
  // TODO(awong): Register an actual error-handling function for tablet. Some
  // errors may surface indirectly due to disk errors, but may not
  // necessarily be caused by the failure of a specific disk.
  //
  // For example, if all of the disks in a tablet's directory group have
  // already failed, the tablet would not be able to create a new block and
  // return an error, despite CreateNewBlock() not actually touching disk.
  // Before CreateNewBlock() returns, in order to satisfy various saftey
  // checks surrounding the state of tablet post-failure, it must wait for
  // disk failure handling of the failed disks to return.
  //
  // While this callback is a no-op, it serves to enforce that any
  // error-handling caused by ERROR1 that may have indirectly caused ERROR2
  // must complete before ERROR2 can be returned to its caller.
  FsErrorManager();

  // Sets the error notification callback.
  //
  // This should be called when the callback's callee is initialized.
  void SetErrorNotificationCb(ErrorHandlerType e, ErrorNotificationCb cb);

  // Resets the error notification callback.
  //
  // This must be called before the callback's callee is destroyed.
  void UnsetErrorNotificationCb(ErrorHandlerType e);

  // Runs the error notification callback.
  //
  // 'uuid' is the full UUID of the component that failed.
  void RunErrorNotificationCb(ErrorHandlerType e, const std::string& uuid) const;

  // Runs the error notification callback with the UUID of 'dir'.
  void RunErrorNotificationCb(ErrorHandlerType e, const DataDir* dir) const {
    DCHECK_EQ(e, ErrorHandlerType::DISK);
    RunErrorNotificationCb(e, dir->instance()->metadata()->path_set().uuid());
  }

 private:
   // Callbacks to be run when an error occurs.
  ErrorNotificationCb disk_cb_;
  ErrorNotificationCb tablet_cb_;

   // Protects calls to notifications, enforcing that a single callback runs at
   // a time.
   mutable Mutex lock_;
};

}  // namespace fs
}  // namespace kudu
