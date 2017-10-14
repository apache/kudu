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

#include "kudu/fs/data_dirs.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <numeric>
#include <ostream>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/fs/block_manager.h"
#include "kudu/fs/block_manager_util.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util_prod.h"
#include "kudu/util/threadpool.h"

DEFINE_int32(fs_target_data_dirs_per_tablet, 0,
              "Indicates the target number of data dirs to spread each "
              "tablet's data across. If greater than the number of data dirs "
              "available, data will be striped across those available. The "
              "default value 0 indicates striping should occur across all "
              "healthy data directories.");
DEFINE_validator(fs_target_data_dirs_per_tablet,
    [](const char* /*n*/, int32_t v) { return v >= 0; });
TAG_FLAG(fs_target_data_dirs_per_tablet, advanced);
TAG_FLAG(fs_target_data_dirs_per_tablet, experimental);

DEFINE_int64(fs_data_dirs_reserved_bytes, -1,
             "Number of bytes to reserve on each data directory filesystem for "
             "non-Kudu usage. The default, which is represented by -1, is that "
             "1% of the disk space on each disk will be reserved. Any other "
             "value specified represents the number of bytes reserved and must "
             "be greater than or equal to 0. Explicit percentages to reserve "
             "are not currently supported");
DEFINE_validator(fs_data_dirs_reserved_bytes, [](const char* /*n*/, int64_t v) { return v >= -1; });
TAG_FLAG(fs_data_dirs_reserved_bytes, runtime);
TAG_FLAG(fs_data_dirs_reserved_bytes, evolving);

DEFINE_int32(fs_data_dirs_full_disk_cache_seconds, 30,
             "Number of seconds we cache the full-disk status in the block manager. "
             "During this time, writes to the corresponding root path will not be attempted.");
TAG_FLAG(fs_data_dirs_full_disk_cache_seconds, advanced);
TAG_FLAG(fs_data_dirs_full_disk_cache_seconds, evolving);

DEFINE_bool(fs_lock_data_dirs, true,
            "Lock the data directories to prevent concurrent usage. "
            "Note that read-only concurrent usage is still allowed.");
TAG_FLAG(fs_lock_data_dirs, unsafe);
TAG_FLAG(fs_lock_data_dirs, evolving);

METRIC_DEFINE_gauge_uint64(server, data_dirs_failed,
                           "Data Directories Failed",
                           kudu::MetricUnit::kDataDirectories,
                           "Number of data directories whose disks are currently "
                           "in a failed state");
METRIC_DEFINE_gauge_uint64(server, data_dirs_full,
                           "Data Directories Full",
                           kudu::MetricUnit::kDataDirectories,
                           "Number of data directories whose disks are currently full");

DECLARE_bool(enable_data_block_fsync);
DECLARE_string(block_manager);

namespace kudu {

namespace fs {

using internal::DataDirGroup;
using std::default_random_engine;
using std::iota;
using std::set;
using std::shuffle;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;


namespace {

const char kHolePunchErrorMsg[] =
    "Error during hole punch test. The log block manager requires a "
    "filesystem with hole punching support such as ext4 or xfs. On el6, "
    "kernel version 2.6.32-358 or newer is required. To run without hole "
    "punching (at the cost of some efficiency and scalability), reconfigure "
    "Kudu with --block_manager=file. Refer to the Kudu documentation for more "
    "details. Raw error message follows";

Status CheckHolePunch(Env* env, const string& path) {
  // Arbitrary constants.
  static uint64_t kFileSize = 4096 * 4;
  static uint64_t kHoleOffset = 4096;
  static uint64_t kHoleSize = 8192;
  static uint64_t kPunchedFileSize = kFileSize - kHoleSize;

  // Open the test file.
  string filename = JoinPathSegments(path, "hole_punch_test_file");
  unique_ptr<RWFile> file;
  RWFileOptions opts;
  RETURN_NOT_OK(env->NewRWFile(opts, filename, &file));

  // The file has been created; delete it on exit no matter what happens.
  auto file_deleter = MakeScopedCleanup([&]() {
    WARN_NOT_OK(env->DeleteFile(filename),
                "Could not delete file " + filename);
  });

  // Preallocate it, making sure the file's size is what we'd expect.
  uint64_t sz;
  RETURN_NOT_OK(file->PreAllocate(0, kFileSize, RWFile::CHANGE_FILE_SIZE));
  RETURN_NOT_OK(env->GetFileSizeOnDisk(filename, &sz));
  if (sz != kFileSize) {
    return Status::IOError(Substitute(
        "Unexpected pre-punch file size for $0: expected $1 but got $2",
        filename, kFileSize, sz));
  }

  // Punch the hole, testing the file's size again.
  RETURN_NOT_OK(file->PunchHole(kHoleOffset, kHoleSize));
  RETURN_NOT_OK(env->GetFileSizeOnDisk(filename, &sz));
  if (sz != kPunchedFileSize) {
    return Status::IOError(Substitute(
        "Unexpected post-punch file size for $0: expected $1 but got $2",
        filename, kPunchedFileSize, sz));
  }

  return Status::OK();
}

// Wrapper for env_util::DeleteTmpFilesRecursively that is suitable for parallel
// execution on a data directory's thread pool (which requires the return value
// be void).
void DeleteTmpFilesRecursively(Env* env, const string& path) {
  WARN_NOT_OK(env_util::DeleteTmpFilesRecursively(env, path),
              "Error while deleting temp files");
}

} // anonymous namespace

#define GINIT(x) x(METRIC_##x.Instantiate(entity, 0))
DataDirMetrics::DataDirMetrics(const scoped_refptr<MetricEntity>& entity)
  : GINIT(data_dirs_failed),
    GINIT(data_dirs_full) {
}
#undef GINIT

DataDir::DataDir(Env* env,
                 DataDirMetrics* metrics,
                 DataDirFsType fs_type,
                 string dir,
                 unique_ptr<PathInstanceMetadataFile> metadata_file,
                 unique_ptr<ThreadPool> pool)
    : env_(env),
      metrics_(metrics),
      fs_type_(fs_type),
      dir_(std::move(dir)),
      metadata_file_(std::move(metadata_file)),
      pool_(std::move(pool)),
      is_shutdown_(false),
      is_full_(false) {
}

DataDir::~DataDir() {
  Shutdown();
}

void DataDir::Shutdown() {
  if (is_shutdown_) {
    return;
  }

  WaitOnClosures();
  pool_->Shutdown();
  is_shutdown_ = true;
}

void DataDir::ExecClosure(const Closure& task) {
  Status s = pool_->SubmitClosure(task);
  if (!s.ok()) {
    WARN_NOT_OK(
        s, "Could not submit task to thread pool, running it synchronously");
    task.Run();
  }
}

void DataDir::WaitOnClosures() {
  pool_->Wait();
}

Status DataDir::RefreshIsFull(RefreshMode mode) {
  switch (mode) {
    case RefreshMode::EXPIRED_ONLY: {
      std::lock_guard<simple_spinlock> l(lock_);
      DCHECK(last_check_is_full_.Initialized());
      MonoTime expiry = last_check_is_full_ + MonoDelta::FromSeconds(
          FLAGS_fs_data_dirs_full_disk_cache_seconds);
      if (!is_full_ || MonoTime::Now() < expiry) {
        break;
      }
      FALLTHROUGH_INTENDED; // Root was previously full, check again.
    }
    case RefreshMode::ALWAYS: {
      Status s = env_util::VerifySufficientDiskSpace(
          env_, dir_, 0, FLAGS_fs_data_dirs_reserved_bytes);
      bool is_full_new;
      if (PREDICT_FALSE(s.IsIOError() && s.posix_code() == ENOSPC)) {
        LOG(WARNING) << Substitute(
            "Insufficient disk space under path $0: creation of new data "
            "blocks under this path can be retried after $1 seconds: $2",
            dir_, FLAGS_fs_data_dirs_full_disk_cache_seconds, s.ToString());
        s = Status::OK();
        is_full_new = true;
      } else {
        is_full_new = false;
      }
      RETURN_NOT_OK_PREPEND(s, "Could not refresh fullness"); // Catch other types of IOErrors, etc.
      {
        std::lock_guard<simple_spinlock> l(lock_);
        if (metrics_ && is_full_ != is_full_new) {
          metrics_->data_dirs_full->IncrementBy(is_full_new ? 1 : -1);
        }
        is_full_ = is_full_new;
        last_check_is_full_ = MonoTime::Now();
      }
      break;
    }
    default:
      LOG(FATAL) << "Unknown check mode";
  }
  return Status::OK();
}

const char* DataDirManager::kDataDirName = "data";

DataDirManagerOptions::DataDirManagerOptions()
  : block_manager_type(FLAGS_block_manager),
    read_only(false) {
}

vector<string> DataDirManager::GetRootNames(const CanonicalizedRootsList& root_list) {
  vector<string> roots;
  std::transform(root_list.begin(), root_list.end(), std::back_inserter(roots),
    [&] (const CanonicalizedRootAndStatus& r) { return r.path; });
  return roots;
}

DataDirManager::DataDirManager(Env* env,
                               DataDirManagerOptions opts,
                               CanonicalizedRootsList canonicalized_data_roots)
    : env_(env),
      block_manager_type_(std::move(opts.block_manager_type)),
      read_only_(opts.read_only),
      canonicalized_data_fs_roots_(std::move(canonicalized_data_roots)),
      rng_(GetRandomSeed32()) {
  DCHECK_GT(canonicalized_data_fs_roots_.size(), 0);

  if (opts.metric_entity) {
    metrics_.reset(new DataDirMetrics(opts.metric_entity));
  }
}

DataDirManager::~DataDirManager() {
  Shutdown();
}

void DataDirManager::WaitOnClosures() {
  for (const auto& dd : data_dirs_) {
    dd->WaitOnClosures();
  }
}

void DataDirManager::Shutdown() {
  // We may be waiting here for a while on outstanding closures.
  LOG_SLOW_EXECUTION(INFO, 1000,
                     Substitute("waiting on $0 block manager thread pools",
                                data_dirs_.size())) {
    for (const auto& dd : data_dirs_) {
      dd->Shutdown();
    }
  }
}

Status DataDirManager::OpenExistingForTests(Env* env, vector<string> data_fs_roots,
                                            DataDirManagerOptions opts,
                                            unique_ptr<DataDirManager>* dd_manager) {
  CanonicalizedRootsList roots;
  for (string& root : data_fs_roots) {
    roots.push_back({ root, Status::OK() });
  }
  return DataDirManager::OpenExisting(env, std::move(roots), std::move(opts), dd_manager);
}

Status DataDirManager::OpenExisting(Env* env, CanonicalizedRootsList data_fs_roots,
                                    DataDirManagerOptions opts,
                                    unique_ptr<DataDirManager>* dd_manager) {
  unique_ptr<DataDirManager> dm;
  dm.reset(new DataDirManager(env, std::move(opts), std::move(data_fs_roots)));
  RETURN_NOT_OK(dm->Open());
  dd_manager->swap(dm);
  return Status::OK();
}

Status DataDirManager::CreateNewForTests(Env* env, vector<string> data_fs_roots,
                                         DataDirManagerOptions opts,
                                         unique_ptr<DataDirManager>* dd_manager) {
  CanonicalizedRootsList roots;
  for (string& root : data_fs_roots) {
    roots.push_back({ root, Status::OK() });
  }
  return DataDirManager::CreateNew(env, std::move(roots), std::move(opts), dd_manager);
}

Status DataDirManager::CreateNew(Env* env, CanonicalizedRootsList data_fs_roots,
                                 DataDirManagerOptions opts,
                                 unique_ptr<DataDirManager>* dd_manager) {
  unique_ptr<DataDirManager> dm;
  dm.reset(new DataDirManager(env, std::move(opts), std::move(data_fs_roots)));
  RETURN_NOT_OK(dm->Create());
  RETURN_NOT_OK(dm->Open());
  dd_manager->swap(dm);
  return Status::OK();
}

Status DataDirManager::Create() {
  CHECK(!read_only_);

  vector<string> dirs_to_delete;
  vector<string> files_to_delete;
  auto deleter = MakeScopedCleanup([&]() {
    // Delete files first so that the directories will be empty when deleted.
    for (const auto& f : files_to_delete) {
      WARN_NOT_OK(env_->DeleteFile(f), "Could not delete file " + f);
    }
    // Delete directories in reverse order since parent directories will have
    // been added before child directories.
    for (auto it = dirs_to_delete.rbegin(); it != dirs_to_delete.rend(); it++) {
      WARN_NOT_OK(env_->DeleteDir(*it), "Could not delete dir " + *it);
    }
  });

  // The UUIDs and indices will be included in every instance file.
  ObjectIdGenerator gen;
  vector<string> all_uuids(canonicalized_data_fs_roots_.size());
  for (string& u : all_uuids) {
    u = gen.Next();
  }
  int idx = 0;

  // Ensure the data dirs exist and create the instance files.
  for (const auto& root : canonicalized_data_fs_roots_) {
    RETURN_NOT_OK_PREPEND(root.status, "Could not create directory manager with disks failed");
    string data_dir = JoinPathSegments(root.path, kDataDirName);
    bool created;
    RETURN_NOT_OK_PREPEND(env_util::CreateDirIfMissing(env_, data_dir, &created),
        Substitute("Could not create directory $0", data_dir));
    if (created) {
      dirs_to_delete.emplace_back(data_dir);
    }

    if (block_manager_type_ == "log") {
      RETURN_NOT_OK_PREPEND(CheckHolePunch(env_, data_dir), kHolePunchErrorMsg);
    }

    string instance_filename = JoinPathSegments(data_dir, kInstanceMetadataFileName);
    PathInstanceMetadataFile metadata(env_, block_manager_type_,
                                      instance_filename);
    RETURN_NOT_OK_PREPEND(metadata.Create(all_uuids[idx], all_uuids), instance_filename);
    files_to_delete.emplace_back(instance_filename);

    idx++;
  }

  // Ensure newly created directories are synchronized to disk.
  if (FLAGS_enable_data_block_fsync) {
    RETURN_NOT_OK_PREPEND(env_util::SyncAllParentDirs(
        env_, dirs_to_delete, files_to_delete), "could not sync data directories");
  }

  // Success: don't delete any files.
  deleter.cancel();
  return Status::OK();
}

Status DataDirManager::Open() {
  vector<PathInstanceMetadataFile*> instances;
  vector<unique_ptr<DataDir>> dds;
  LockMode lock_mode;
  if (!FLAGS_fs_lock_data_dirs) {
    lock_mode = LockMode::NONE;
  } else if (read_only_) {
    lock_mode = LockMode::OPTIONAL;
  } else {
    lock_mode = LockMode::MANDATORY;
  }
  const int kMaxDataDirs = block_manager_type_ == "file" ? (1 << 16) - 1 : kint32max;

  int i = 0;
  // Create a directory for all data dirs specified by the user.
  for (const auto& root : canonicalized_data_fs_roots_) {
    string data_dir = JoinPathSegments(root.path, kDataDirName);
    string instance_filename = JoinPathSegments(data_dir, kInstanceMetadataFileName);
    // Open and lock the data dir's metadata instance file.
    gscoped_ptr<PathInstanceMetadataFile> instance(
        new PathInstanceMetadataFile(env_, block_manager_type_,
                                     instance_filename));
    if (PREDICT_FALSE(!root.status.ok())) {
      instance->SetInstanceFailed(root.status);
    } else {
      RETURN_NOT_OK_PREPEND(instance->LoadFromDisk(),
                            Substitute("Could not open $0", instance_filename));
    }

    // Try locking the directory.
    if (lock_mode != LockMode::NONE) {
      Status s = instance->Lock();
      if (!s.ok()) {
        Status new_status = s.CloneAndPrepend(Substitute(
            "Could not lock $0", instance_filename));
        if (lock_mode == LockMode::OPTIONAL) {
          LOG(WARNING) << new_status.ToString();
          LOG(WARNING) << "Proceeding without lock";
        } else {
          DCHECK(LockMode::MANDATORY == lock_mode);
          return new_status;
        }
      }
    }

    // Crash if the metadata directory, i.e. the first specified by the user, failed.
    if (!instance->healthy() && i == 0) {
      return Status::IOError(Substitute("Could not open directory manager; "
          "metadata directory failed: $0", instance->health_status().ToString()));
    }

    instances.push_back(instance.get());

    // Create a per-dir thread pool.
    gscoped_ptr<ThreadPool> pool;
    RETURN_NOT_OK(ThreadPoolBuilder(Substitute("data dir $0", i))
                  .set_max_threads(1)
                  .Build(&pool));

    // Figure out what filesystem the data directory is on.
    DataDirFsType fs_type = DataDirFsType::OTHER;
    if (instance->healthy()) {
      bool result;
      RETURN_NOT_OK(env_->IsOnExtFilesystem(data_dir, &result));
      if (result) {
        fs_type = DataDirFsType::EXT;
      } else {
        RETURN_NOT_OK(env_->IsOnXfsFilesystem(data_dir, &result));
        if (result) {
          fs_type = DataDirFsType::XFS;
        }
      }
    }

    // Create the data directory in-memory structure itself.
    unique_ptr<DataDir> dd(new DataDir(
        env_, metrics_.get(), fs_type, data_dir,
        unique_ptr<PathInstanceMetadataFile>(instance.release()),
        unique_ptr<ThreadPool>(pool.release())));

    dds.emplace_back(std::move(dd));
    i++;
  }

  // Check integrity and determine which instances need updating.
  RETURN_NOT_OK_PREPEND(
      PathInstanceMetadataFile::CheckIntegrity(instances),
      Substitute("Could not verify integrity of files: $0",
                 JoinStrings(GetDataDirs(), ",")));

  // Use the per-dir thread pools to delete temporary files in parallel.
  for (const auto& dd : dds) {
    if (dd->instance()->healthy()) {
      dd->ExecClosure(Bind(&DeleteTmpFilesRecursively, env_, dd->dir()));
    }
  }
  for (const auto& dd : dds) {
    dd->WaitOnClosures();
  }

  // Build in-memory maps of on-disk state.
  UuidByRootMap uuid_by_root;
  UuidByUuidIndexMap uuid_by_idx;
  UuidIndexByUuidMap idx_by_uuid;
  UuidIndexMap dd_by_uuid_idx;
  ReverseUuidIndexMap uuid_idx_by_dd;
  TabletsByUuidIndexMap tablets_by_uuid_idx_map;
  FailedDataDirSet failed_data_dirs;

  const auto insert_to_maps = [&] (int idx, string uuid, DataDir* dd) {
    InsertOrDie(&uuid_by_root, DirName(dd->dir()), uuid);
    InsertOrDie(&uuid_by_idx, idx, uuid);
    InsertOrDie(&idx_by_uuid, uuid, idx);
    InsertOrDie(&dd_by_uuid_idx, idx, dd);
    InsertOrDie(&uuid_idx_by_dd, dd, idx);
    InsertOrDie(&tablets_by_uuid_idx_map, idx, {});
  };

  vector<DataDir*> unassigned_dirs;
  // Assign a uuid index to each healthy instance.
  for (const auto& dd : dds) {
    if (PREDICT_FALSE(!dd->instance()->healthy())) {
      // Keep track of failed directories so we can assign them UUIDs later.
      unassigned_dirs.push_back(dd.get());
      continue;
    }
    const PathSetPB& path_set = dd->instance()->metadata()->path_set();
    int idx = -1;
    for (int i = 0; i < path_set.all_uuids_size(); i++) {
      if (path_set.uuid() == path_set.all_uuids(i)) {
        idx = i;
        break;
      }
    }
    DCHECK_NE(idx, -1); // Guaranteed by CheckIntegrity().
    if (idx > kMaxDataDirs) {
      return Status::NotSupported(
          Substitute("Block manager supports a maximum of $0 paths", kMaxDataDirs));
    }
    insert_to_maps(idx, path_set.uuid(), dd.get());
  }

  // If the uuid index was not assigned, assign it to a failed directory. Use
  // the 'all_uuids' of the first data directory, as it is guaranteed to be
  // healthy.
  PathSetPB path_set = dds[0]->instance()->metadata()->path_set();
  int failed_dir_idx = 0;
  for (int uuid_idx = 0; uuid_idx < path_set.all_uuids_size(); uuid_idx++) {
    if (!ContainsKey(uuid_by_idx, uuid_idx)) {
      const string& unassigned_uuid = path_set.all_uuids(uuid_idx);
      DCHECK_LT(failed_dir_idx, unassigned_dirs.size());
      insert_to_maps(uuid_idx, unassigned_uuid, unassigned_dirs[failed_dir_idx]);

      // Record the directory as failed.
      if (metrics_) {
        metrics_->data_dirs_failed->IncrementBy(1);
      }
      InsertOrDie(&failed_data_dirs, uuid_idx);
      failed_dir_idx++;
    }
  }
  CHECK_EQ(unassigned_dirs.size(), failed_dir_idx);

  data_dirs_.swap(dds);
  uuid_by_idx_.swap(uuid_by_idx);
  idx_by_uuid_.swap(idx_by_uuid);
  data_dir_by_uuid_idx_.swap(dd_by_uuid_idx);
  uuid_idx_by_data_dir_.swap(uuid_idx_by_dd);
  tablets_by_uuid_idx_map_.swap(tablets_by_uuid_idx_map);
  failed_data_dirs_.swap(failed_data_dirs);
  uuid_by_root_.swap(uuid_by_root);

  // From this point onwards, the above in-memory maps must be consistent with
  // the main path set.

  // Initialize the 'fullness' status of the data directories.
  for (const auto& dd : data_dirs_) {
    int uuid_idx;
    CHECK(FindUuidIndexByDataDir(dd.get(), &uuid_idx));
    if (ContainsKey(failed_data_dirs_, uuid_idx)) {
      continue;
    }
    Status refresh_status = dd->RefreshIsFull(DataDir::RefreshMode::ALWAYS);
    if (PREDICT_FALSE(!refresh_status.ok())) {
      if (refresh_status.IsDiskFailure()) {
        RETURN_NOT_OK(MarkDataDirFailed(uuid_idx, refresh_status.ToString()));
        continue;
      }
      return refresh_status;
    }
  }

  return Status::OK();
}

Status DataDirManager::LoadDataDirGroupFromPB(const std::string& tablet_id,
                                              const DataDirGroupPB& pb) {
  std::lock_guard<percpu_rwlock> lock(dir_group_lock_);
  DataDirGroup group_from_pb = DataDirGroup::FromPB(pb, idx_by_uuid_);
  DataDirGroup* other = InsertOrReturnExisting(&group_by_tablet_map_,
                                               tablet_id,
                                               group_from_pb);
  if (other != nullptr) {
    return Status::AlreadyPresent("Tried to load directory group for tablet but one is already "
                                  "registered", tablet_id);
  }
  for (int uuid_idx : group_from_pb.uuid_indices()) {
    InsertOrDie(&FindOrDie(tablets_by_uuid_idx_map_, uuid_idx), tablet_id);
  }
  return Status::OK();
}

Status DataDirManager::CreateDataDirGroup(const string& tablet_id,
                                          DirDistributionMode mode) {
  std::lock_guard<percpu_rwlock> write_lock(dir_group_lock_);
  if (ContainsKey(group_by_tablet_map_, tablet_id)) {
    return Status::AlreadyPresent("Tried to create directory group for tablet but one is already "
                                  "registered", tablet_id);
  }
  // Adjust the disk group size to fit within the total number of data dirs.
  int group_target_size;
  if (FLAGS_fs_target_data_dirs_per_tablet == 0) {
    group_target_size = data_dirs_.size();
  } else {
    group_target_size = std::min(FLAGS_fs_target_data_dirs_per_tablet,
                                 static_cast<int>(data_dirs_.size()));
  }
  vector<int> group_indices;
  if (mode == DirDistributionMode::ACROSS_ALL_DIRS) {
    // If using all dirs, add all regardless of directory state.
    AppendKeysFromMap(data_dir_by_uuid_idx_, &group_indices);
  } else {
    // Randomly select directories, giving preference to those with fewer tablets.
    if (PREDICT_FALSE(!failed_data_dirs_.empty())) {
      group_target_size = std::min(group_target_size,
          static_cast<int>(data_dirs_.size()) - static_cast<int>(failed_data_dirs_.size()));

      // A size of 0 would indicate no healthy disks, which should crash the server.
      DCHECK_GE(group_target_size, 0);
      if (group_target_size == 0) {
        return Status::IOError("No healthy data directories available", "", ENODEV);
      }
    }
    RETURN_NOT_OK(GetDirsForGroupUnlocked(group_target_size, &group_indices));
    if (PREDICT_FALSE(group_indices.empty())) {
      return Status::IOError("All healthy data directories are full", "", ENOSPC);
    }
    if (PREDICT_FALSE(group_indices.size() < FLAGS_fs_target_data_dirs_per_tablet)) {
      LOG(WARNING) << Substitute("Could only allocate $0 dirs of requested $1 for tablet $2 ($3 "
                                 "dirs total, $4 full, $5 failed).", group_indices.size(),
                                 FLAGS_fs_target_data_dirs_per_tablet, tablet_id, data_dirs_.size(),
                                 metrics_->data_dirs_full.get(), metrics_->data_dirs_failed.get());
    }
  }
  InsertOrDie(&group_by_tablet_map_, tablet_id, DataDirGroup(group_indices));
  for (int uuid_idx : group_indices) {
    InsertOrDie(&FindOrDie(tablets_by_uuid_idx_map_, uuid_idx), tablet_id);
  }
  return Status::OK();
}

Status DataDirManager::GetNextDataDir(const CreateBlockOptions& opts, DataDir** dir) {
  shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
  const vector<int>* group_uuid_indices;
  vector<int> valid_uuid_indices;
  if (PREDICT_TRUE(!opts.tablet_id.empty())) {
    // Get the data dir group for the tablet.
    DataDirGroup* group = FindOrNull(group_by_tablet_map_, opts.tablet_id);
    if (group == nullptr) {
      return Status::NotFound("Tried to get directory but no directory group "
                              "registered for tablet", opts.tablet_id);
    }
    if (PREDICT_TRUE(failed_data_dirs_.empty())) {
      group_uuid_indices = &group->uuid_indices();
    } else {
      RemoveUnhealthyDataDirsUnlocked(group->uuid_indices(), &valid_uuid_indices);
      group_uuid_indices = &valid_uuid_indices;
      if (valid_uuid_indices.empty()) {
        return Status::IOError("No healthy directories exist in tablet's "
                               "directory group", opts.tablet_id, ENODEV);
      }
    }
  } else {
    // This should only be reached by some tests; in cases where there is no
    // natural tablet_id, select a data dir from any of the directories.
    CHECK(IsGTest());
    AppendKeysFromMap(data_dir_by_uuid_idx_, &valid_uuid_indices);
    group_uuid_indices = &valid_uuid_indices;
  }
  vector<int> random_indices(group_uuid_indices->size());
  iota(random_indices.begin(), random_indices.end(), 0);
  shuffle(random_indices.begin(), random_indices.end(), default_random_engine(rng_.Next()));

  // Randomly select a member of the group that is not full.
  for (int i : random_indices) {
    int uuid_idx = (*group_uuid_indices)[i];
    DataDir* candidate = FindOrDie(data_dir_by_uuid_idx_, uuid_idx);
    RETURN_NOT_OK(candidate->RefreshIsFull(DataDir::RefreshMode::EXPIRED_ONLY));
    if (!candidate->is_full()) {
      *dir = candidate;
      return Status::OK();
    }
  }
  string tablet_id_str = "";
  if (PREDICT_TRUE(!opts.tablet_id.empty())) {
    tablet_id_str = Substitute("$0's ", opts.tablet_id);
  }
  string dirs_state_str = Substitute("$0 failed", failed_data_dirs_.size());
  if (metrics_) {
    dirs_state_str = Substitute("$0 full, $1",
                                metrics_->data_dirs_full->value(), dirs_state_str);
  }
  return Status::IOError(Substitute("No directories available to add to $0directory group ($1 "
                        "dirs total, $2).", tablet_id_str, data_dirs_.size(), dirs_state_str),
                        "", ENOSPC);
}

void DataDirManager::DeleteDataDirGroup(const std::string& tablet_id) {
  std::lock_guard<percpu_rwlock> lock(dir_group_lock_);
  DataDirGroup* group = FindOrNull(group_by_tablet_map_, tablet_id);
  if (group == nullptr) {
    return;
  }
  // Remove the tablet_id from every data dir in its group.
  for (int uuid_idx : group->uuid_indices()) {
    FindOrDie(tablets_by_uuid_idx_map_, uuid_idx).erase(tablet_id);
  }
  group_by_tablet_map_.erase(tablet_id);
}

bool DataDirManager::GetDataDirGroupPB(const std::string& tablet_id,
                                       DataDirGroupPB* pb) const {
  shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
  const DataDirGroup* group = FindOrNull(group_by_tablet_map_, tablet_id);
  if (group != nullptr) {
    group->CopyToPB(uuid_by_idx_, pb);
    return true;
  }
  return false;
}

Status DataDirManager::GetDirsForGroupUnlocked(int target_size,
                                               vector<int>* group_indices) {
  DCHECK(dir_group_lock_.is_locked());
  vector<int> candidate_indices;
  for (auto& e : data_dir_by_uuid_idx_) {
    if (ContainsKey(failed_data_dirs_, e.first)) {
      continue;
    }
    RETURN_NOT_OK(e.second->RefreshIsFull(DataDir::RefreshMode::ALWAYS));
    // TODO(awong): If a disk is unhealthy at the time of group creation, the
    // resulting group may be below targeted size. Add functionality to resize
    // groups. See KUDU-2040 for more details.
    if (!e.second->is_full()) {
      candidate_indices.push_back(e.first);
    }
  }
  while (group_indices->size() < target_size && !candidate_indices.empty()) {
    shuffle(candidate_indices.begin(), candidate_indices.end(), default_random_engine(rng_.Next()));
    if (candidate_indices.size() == 1 ||
        FindOrDie(tablets_by_uuid_idx_map_, candidate_indices[0]).size() <
            FindOrDie(tablets_by_uuid_idx_map_, candidate_indices[1]).size()) {
      group_indices->push_back(candidate_indices[0]);
      candidate_indices.erase(candidate_indices.begin());
    } else {
      group_indices->push_back(candidate_indices[1]);
      candidate_indices.erase(candidate_indices.begin() + 1);
    }
  }
  return Status::OK();
}

DataDir* DataDirManager::FindDataDirByUuidIndex(int uuid_idx) const {
  DCHECK_LT(uuid_idx, data_dirs_.size());
  return FindPtrOrNull(data_dir_by_uuid_idx_, uuid_idx);
}

bool DataDirManager::FindUuidIndexByDataDir(DataDir* dir, int* uuid_idx) const {
  return FindCopy(uuid_idx_by_data_dir_, dir, uuid_idx);
}

bool DataDirManager::FindUuidIndexByRoot(const string& root, int* uuid_idx) const {
  string uuid;
  if (FindUuidByRoot(root, &uuid)) {
    return FindUuidIndexByUuid(uuid, uuid_idx);
  }
  return false;
}

bool DataDirManager::FindUuidIndexByUuid(const string& uuid, int* uuid_idx) const {
  return FindCopy(idx_by_uuid_, uuid, uuid_idx);
}

bool DataDirManager::FindUuidByRoot(const string& root, string* uuid) const {
  return FindCopy(uuid_by_root_, root, uuid);
}

set<string> DataDirManager::FindTabletsByDataDirUuidIdx(int uuid_idx) const {
  DCHECK_LT(uuid_idx, data_dirs_.size());
  shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
  const set<string>* tablet_set_ptr = FindOrNull(tablets_by_uuid_idx_map_, uuid_idx);
  if (tablet_set_ptr) {
    return *tablet_set_ptr;
  }
  return {};
}

void DataDirManager::MarkDataDirFailedByUuid(const string& uuid) {
  int uuid_idx;
  CHECK(FindUuidIndexByUuid(uuid, &uuid_idx));
  WARN_NOT_OK(MarkDataDirFailed(uuid_idx), "Failed to handle disk failure");
}

Status DataDirManager::MarkDataDirFailed(int uuid_idx, const string& error_message) {
  DCHECK_LT(uuid_idx, data_dirs_.size());
  std::lock_guard<percpu_rwlock> lock(dir_group_lock_);
  DataDir* dd = FindDataDirByUuidIndex(uuid_idx);
  DCHECK(dd);
  if (InsertIfNotPresent(&failed_data_dirs_, uuid_idx)) {
    if (failed_data_dirs_.size() == data_dirs_.size()) {
      // TODO(awong): pass 'error_message' as a Status instead of an string so
      // we can avoid returning this artificial status.
      return Status::IOError(Substitute("All data dirs have failed: ", error_message));
    }
    if (metrics_) {
      metrics_->data_dirs_failed->IncrementBy(1);
    }
    string error_prefix = "";
    if (!error_message.empty()) {
      error_prefix = Substitute("$0: ", error_message);
    }
    LOG(ERROR) << error_prefix << Substitute("Directory $0 marked as failed", dd->dir());
  }
  return Status::OK();
}

bool DataDirManager::IsDataDirFailed(int uuid_idx) const {
  DCHECK_LT(uuid_idx, data_dirs_.size());
  shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
  return ContainsKey(failed_data_dirs_, uuid_idx);
}

bool DataDirManager::IsTabletInFailedDir(const string& tablet_id) const {
  const set<int> failed_dirs = GetFailedDataDirs();
  for (int failed_dir : failed_dirs) {
    if (ContainsKey(FindTabletsByDataDirUuidIdx(failed_dir), tablet_id)) {
      return true;
    }
  }
  return false;
}

void DataDirManager::RemoveUnhealthyDataDirsUnlocked(const vector<int>& uuid_indices,
                                                     vector<int>* healthy_indices) const {
  if (PREDICT_TRUE(failed_data_dirs_.empty())) {
    return;
  }
  healthy_indices->clear();
  for (int uuid_idx : uuid_indices) {
    DCHECK_LT(uuid_idx, data_dirs_.size());
    if (!ContainsKey(failed_data_dirs_, uuid_idx)) {
      healthy_indices->emplace_back(uuid_idx);
    }
  }
}

vector<string> DataDirManager::GetDataRoots() const {
  return GetRootNames(canonicalized_data_fs_roots_);
}

vector<string> DataDirManager::GetDataDirs() const {
  return JoinPathSegmentsV(GetDataRoots(), kDataDirName);
}

} // namespace fs
} // namespace kudu
