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
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "kudu/gutil/callback_forward.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/atomic.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {
template<typename T>
class AtomicGauge;
class Env;
class MetricEntity;
class ThreadPool;

namespace fs {
class PathInstanceMetadataFile;

struct DataDirMetrics {
  explicit DataDirMetrics(const scoped_refptr<MetricEntity>& entity);

  scoped_refptr<AtomicGauge<uint64_t>> data_dirs_full;
};

// Representation of a data directory in use by the block manager.
class DataDir {
 public:
  DataDir(Env* env,
          DataDirMetrics* metrics,
          std::string dir,
          std::unique_ptr<PathInstanceMetadataFile> metadata_file,
          std::unique_ptr<ThreadPool> pool);
  ~DataDir();

  // Shuts down this dir's thread pool, waiting for any closures submitted via
  // ExecClosure() to finish first.
  void Shutdown();

  // Run a task on this dir's thread pool.
  //
  // Normally the task is performed asynchronously. However, if submission to
  // the pool fails, it runs synchronously on the current thread.
  void ExecClosure(const Closure& task);

  // Waits for any outstanding closures submitted via ExecClosure() to finish.
  void WaitOnClosures();

  // Tests whether the data directory is full by comparing the free space of
  // its underlying filesystem with a predefined "reserved" space value.
  //
  // If 'mode' is EXPIRED_ONLY, performs the test only if the dir was last
  // determined to be full some time ago. If 'mode' is ALWAYS, the test is
  // performed regardless.
  //
  // Only returns a bad Status in the event of a real error; fullness is
  // reflected via is_full().
  enum class RefreshMode {
    EXPIRED_ONLY,
    ALWAYS,
  };
  Status RefreshIsFull(RefreshMode mode);

  const std::string& dir() const { return dir_; }

  const PathInstanceMetadataFile* instance() const {
    return metadata_file_.get();
  }

  bool is_full() const {
    std::lock_guard<simple_spinlock> l(lock_);
    return is_full_;
  }

 private:
  Env* env_;
  DataDirMetrics* metrics_;
  const std::string dir_;
  const std::unique_ptr<PathInstanceMetadataFile> metadata_file_;
  const std::unique_ptr<ThreadPool> pool_;

  bool is_shutdown_;

  // Protects 'last_check_is_full_' and 'is_full_'.
  mutable simple_spinlock lock_;
  MonoTime last_check_is_full_;
  bool is_full_;

  DISALLOW_COPY_AND_ASSIGN(DataDir);
};

// Encapsulates knowledge of data directory management on behalf of block
// managers.
class DataDirManager {
 public:
  // Flags for Create().
  static const int FLAG_CREATE_TEST_HOLE_PUNCH = 0x1;
  static const int FLAG_CREATE_FSYNC = 0x2;

  enum class LockMode {
    MANDATORY,
    OPTIONAL,
    NONE,
  };

  DataDirManager(Env* env,
                 scoped_refptr<MetricEntity> metric_entity,
                 std::string block_manager_type,
                 std::vector<std::string> paths);
  ~DataDirManager();

  // Shuts down all directories' thread pools.
  void Shutdown();

  // Initializes the data directories on disk.
  //
  // Returns an error if initialized directories already exist.
  Status Create(int flags);

  // Opens existing data directories from disk.
  //
  // Returns an error if the number of on-disk data directories found exceeds
  // 'max_data_dirs', or if 'mode' is MANDATORY and locks could not be taken.
  Status Open(int max_data_dirs, LockMode mode);

  // Retrieves the next data directory that isn't full. Directories are rotated
  // via round-robin. Full directories are skipped.
  //
  // Returns an error if all data directories are full, or upon filesystem
  // error. On success, 'dir' is guaranteed to be set.
  Status GetNextDataDir(DataDir** dir);

  // Finds a data directory by uuid index, returning nullptr if it can't be
  // found.
  //
  // More information on uuid indexes and their relation to data directories
  // can be found next to PathSetPB in fs.proto.
  DataDir* FindDataDirByUuidIndex(uint16_t uuid_idx) const;

  // Finds a uuid index by data directory, returning false if it can't be found.
  bool FindUuidIndexByDataDir(DataDir* dir,
                              uint16_t* uuid_idx) const;

  const std::vector<std::unique_ptr<DataDir>>& data_dirs() const {
    return data_dirs_;
  }

 private:
  Env* env_;
  const std::string block_manager_type_;
  const std::vector<std::string> paths_;

  std::unique_ptr<DataDirMetrics> metrics_;

  std::vector<std::unique_ptr<DataDir>> data_dirs_;

  AtomicInt<int32_t> data_dirs_next_;

  typedef std::unordered_map<uint16_t, DataDir*> UuidIndexMap;
  UuidIndexMap data_dir_by_uuid_idx_;

  typedef std::unordered_map<DataDir*, uint16_t> ReverseUuidIndexMap;
  ReverseUuidIndexMap uuid_idx_by_data_dir_;

  DISALLOW_COPY_AND_ASSIGN(DataDirManager);
};

} // namespace fs
} // namespace kudu
