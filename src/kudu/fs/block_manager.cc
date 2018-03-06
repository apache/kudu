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
#include <ostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"

// The default value is optimized for throughput in the case that
// there are multiple drives backing the tablet. By asynchronously
// flushing each block before issuing any fsyncs, the IO across
// disks is done in parallel.
//
// This increases throughput but can harm latency in the case that
// there are few disks and the WAL is on the same disk as the
// data blocks. The default is chosen based on the assumptions that:
// - latency is leveled across machines by Raft
// - latency-sensitive applications can devote a disk to the WAL
// - super-sensitive applications can devote an SSD to the WAL.
// - users could always change this to "never", which slows down
//   throughput but may improve write latency.
DEFINE_string(block_manager_preflush_control, "finalize",
              "Controls when to pre-flush a block. Valid values are 'finalize', "
              "'close', or 'never'. If 'finalize', blocks will be pre-flushed "
              "when writing is finished. If 'close', blocks will be pre-flushed "
              "when their transaction is committed. If 'never', blocks will "
              "never be pre-flushed but still be flushed when closed.");
TAG_FLAG(block_manager_preflush_control, experimental);

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
  : read_only(false) {}

int64_t GetFileCacheCapacityForBlockManager(Env* env) {
  // Maximize this process' open file limit first, if possible.
  static std::once_flag once;
  std::call_once(once, [&]() {
    env->IncreaseResourceLimit(Env::ResourceLimitType::OPEN_FILES_PER_PROCESS);
  });

  int64_t rlimit =
      env->GetResourceLimit(Env::ResourceLimitType::OPEN_FILES_PER_PROCESS);
  // See block_manager_max_open_files.
  if (FLAGS_block_manager_max_open_files == -1) {
    return (2 * rlimit) / 5;
  }
  LOG_IF(FATAL, FLAGS_block_manager_max_open_files > rlimit) <<
      Substitute(
          "Configured open file limit (block_manager_max_open_files) $0 "
          "exceeds process open file limit (ulimit) $1",
          FLAGS_block_manager_max_open_files, rlimit);
  return FLAGS_block_manager_max_open_files;
}

} // namespace fs
} // namespace kudu
