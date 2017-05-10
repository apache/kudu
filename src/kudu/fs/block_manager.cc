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

#include "kudu/fs/block_manager.h"

#include <mutex>

#include <glog/logging.h>

#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/metrics.h"

DEFINE_bool(block_coalesce_close, false,
            "Coalesce synchronization of data during CloseBlocks()");
TAG_FLAG(block_coalesce_close, experimental);

DEFINE_bool(block_manager_lock_dirs, true,
            "Lock the data block directories to prevent concurrent usage. "
            "Note that read-only concurrent usage is still allowed.");
TAG_FLAG(block_manager_lock_dirs, unsafe);

DEFINE_int64(block_manager_max_open_files, -1,
             "Maximum number of open file descriptors to be used for data "
             "blocks. If -1, Kudu will use 40% of its resource limit as per "
             "getrlimit(). This is a soft limit. It is an error to use a "
             "value of 0.");
TAG_FLAG(block_manager_max_open_files, advanced);
TAG_FLAG(block_manager_max_open_files, evolving);

static bool ValidateMaxOpenFiles(const char* /*flagname*/, int64_t value) {
  if (value == 0) {
    LOG(ERROR) << "Invalid max open files: cannot be 0";
    return false;
  }
  return true;
}
DEFINE_validator(block_manager_max_open_files, &ValidateMaxOpenFiles);

using strings::Substitute;

namespace kudu {
namespace fs {

BlockManagerOptions::BlockManagerOptions()
  : read_only(false) {
}

BlockManagerOptions::~BlockManagerOptions() {
}

int64_t GetFileCacheCapacityForBlockManager(Env* env) {
  // Maximize this process' open file limit first, if possible.
  static std::once_flag once;
  std::call_once(once, [&]() {
    env->IncreaseOpenFileLimit();
  });

  // See block_manager_max_open_files.
  if (FLAGS_block_manager_max_open_files == -1) {
    return (2 * env->GetOpenFileLimit()) / 5;
  }
  int64_t file_limit = env->GetOpenFileLimit();
  LOG_IF(FATAL, FLAGS_block_manager_max_open_files > file_limit) <<
      Substitute(
          "Configured open file limit (block_manager_max_open_files) $0 "
          "exceeds process fd limit (ulimit) $1",
          FLAGS_block_manager_max_open_files, file_limit);
  return FLAGS_block_manager_max_open_files;
}

} // namespace fs
} // namespace kudu
