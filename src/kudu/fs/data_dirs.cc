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
#include <deque>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/fs/block_manager.h"
#include "kudu/fs/block_manager_util.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/atomic.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/random_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util_prod.h"
#include "kudu/util/threadpool.h"

DEFINE_int32(fs_target_data_dirs_per_tablet, 0,
              "Indicates the target number of data dirs to spread each "
              "tablet's data across. If greater than the number of data dirs "
              "available, data will be striped across those available. The "
              "default value 0 indicates striping should occur across all "
              "data directories.");
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

METRIC_DEFINE_gauge_uint64(server, data_dirs_failed,
                           "Data Directories Failed",
                           kudu::MetricUnit::kDataDirectories,
                           "Number of data directories whose disks are currently "
                           "in a failed state");
METRIC_DEFINE_gauge_uint64(server, data_dirs_full,
                           "Data Directories Full",
                           kudu::MetricUnit::kDataDirectories,
                           "Number of data directories whose disks are currently full");

namespace kudu {

namespace fs {

using env_util::ScopedFileDeleter;
using internal::DataDirGroup;
using std::default_random_engine;
using std::deque;
using std::iota;
using std::shuffle;
using std::string;
using std::unique_ptr;
using std::unordered_set;
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
  ScopedFileDeleter file_deleter(env, filename);

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

  pool_->Wait();
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
      RETURN_NOT_OK(s); // Catch other types of IOErrors, etc.
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

DataDirManager::DataDirManager(Env* env,
                               scoped_refptr<MetricEntity> metric_entity,
                               string block_manager_type,
                               vector<string> paths)
    : env_(env),
      block_manager_type_(std::move(block_manager_type)),
      paths_(std::move(paths)),
      rng_(GetRandomSeed32()) {
  DCHECK_GT(paths_.size(), 0);

  if (metric_entity) {
    metrics_.reset(new DataDirMetrics(metric_entity));
  }
}

DataDirManager::~DataDirManager() {
  Shutdown();
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

Status DataDirManager::Create(int flags) {
  deque<ScopedFileDeleter*> delete_on_failure;
  ElementDeleter d(&delete_on_failure);

  // The UUIDs and indices will be included in every instance file.
  ObjectIdGenerator gen;
  vector<string> all_uuids(paths_.size());
  for (string& u : all_uuids) {
    u = gen.Next();
  }
  int idx = 0;

  // Ensure the data paths exist and create the instance files.
  unordered_set<string> to_sync;
  for (const auto& p : paths_) {
    bool created;
    RETURN_NOT_OK_PREPEND(env_util::CreateDirIfMissing(env_, p, &created),
                          Substitute("Could not create directory $0", p));
    if (created) {
      delete_on_failure.push_front(new ScopedFileDeleter(env_, p));
      to_sync.insert(DirName(p));
    }

    if (flags & FLAG_CREATE_TEST_HOLE_PUNCH) {
      RETURN_NOT_OK_PREPEND(CheckHolePunch(env_, p), kHolePunchErrorMsg);
    }

    string instance_filename = JoinPathSegments(p, kInstanceMetadataFileName);
    PathInstanceMetadataFile metadata(env_, block_manager_type_,
                                      instance_filename);
    RETURN_NOT_OK_PREPEND(metadata.Create(all_uuids[idx], all_uuids), instance_filename);
    delete_on_failure.push_front(new ScopedFileDeleter(env_, instance_filename));
    idx++;
  }

  // Ensure newly created directories are synchronized to disk.
  if (flags & FLAG_CREATE_FSYNC) {
    for (const string& dir : to_sync) {
      RETURN_NOT_OK_PREPEND(env_->SyncDir(dir),
                            Substitute("Unable to synchronize directory $0", dir));
    }
  }

  // Success: don't delete any files.
  for (ScopedFileDeleter* deleter : delete_on_failure) {
    deleter->Cancel();
  }
  return Status::OK();
}

Status DataDirManager::Open(int max_data_dirs, LockMode mode) {
  vector<PathInstanceMetadataFile*> instances;
  vector<unique_ptr<DataDir>> dds;

  int i = 0;
  for (const auto& p : paths_) {
    // Open and lock the data dir's metadata instance file.
    string instance_filename = JoinPathSegments(p, kInstanceMetadataFileName);
    gscoped_ptr<PathInstanceMetadataFile> instance(
        new PathInstanceMetadataFile(env_, block_manager_type_,
                                     instance_filename));
    RETURN_NOT_OK_PREPEND(instance->LoadFromDisk(),
                          Substitute("Could not open $0", instance_filename));
    if (mode != LockMode::NONE) {
      Status s = instance->Lock();
      if (!s.ok()) {
        Status new_status = s.CloneAndPrepend(Substitute(
            "Could not lock $0", instance_filename));
        if (mode == LockMode::OPTIONAL) {
          LOG(WARNING) << new_status.ToString();
          LOG(WARNING) << "Proceeding without lock";
        } else {
          DCHECK(LockMode::MANDATORY == mode);
          RETURN_NOT_OK(new_status);
        }
      }
    }
    instances.push_back(instance.get());

    // Create a per-dir thread pool.
    gscoped_ptr<ThreadPool> pool;
    RETURN_NOT_OK(ThreadPoolBuilder(Substitute("data dir $0", i))
                  .set_max_threads(1)
                  .Build(&pool));

    // Figure out what filesystem the data directory is on.
    DataDirFsType fs_type = DataDirFsType::OTHER;
    bool result;
    RETURN_NOT_OK(env_->IsOnExtFilesystem(p, &result));
    if (result) {
      fs_type = DataDirFsType::EXT;
    } else {
      RETURN_NOT_OK(env_->IsOnXfsFilesystem(p, &result));
      if (result) {
        fs_type = DataDirFsType::XFS;
      }
    }

    // Create the data directory in-memory structure itself.
    unique_ptr<DataDir> dd(new DataDir(
        env_, metrics_.get(), fs_type, p,
        unique_ptr<PathInstanceMetadataFile>(instance.release()),
        unique_ptr<ThreadPool>(pool.release())));

    // Initialize the 'fullness' status of the data directory.
    RETURN_NOT_OK(dd->RefreshIsFull(DataDir::RefreshMode::ALWAYS));

    dds.emplace_back(std::move(dd));
    i++;
  }

  RETURN_NOT_OK_PREPEND(PathInstanceMetadataFile::CheckIntegrity(instances),
                        Substitute("Could not verify integrity of files: $0",
                                   JoinStrings(paths_, ",")));

  // Use the per-dir thread pools to delete temporary files in parallel.
  for (const auto& dd : dds) {
    dd->ExecClosure(Bind(&DeleteTmpFilesRecursively, env_, dd->dir()));
  }
  for (const auto& dd : dds) {
    dd->WaitOnClosures();
  }

  // Build uuid index and data directory maps.
  UuidByUuidIndexMap uuid_by_idx;
  UuidIndexByUuidMap idx_by_uuid;
  UuidIndexMap dd_by_uuid_idx;
  ReverseUuidIndexMap uuid_idx_by_dd;
  for (const auto& dd : dds) {
    const PathSetPB& path_set = dd->instance()->metadata()->path_set();
    uint32_t idx = -1;
    for (int i = 0; i < path_set.all_uuids_size(); i++) {
      if (path_set.uuid() == path_set.all_uuids(i)) {
        idx = i;
        break;
      }
    }
    DCHECK_NE(idx, -1); // Guaranteed by CheckIntegrity().
    if (idx > max_data_dirs) {
      return Status::NotSupported(
          Substitute("Block manager supports a maximum of $0 paths", max_data_dirs));
    }
    InsertOrDie(&uuid_by_idx, idx, path_set.uuid());
    InsertOrDie(&idx_by_uuid, path_set.uuid(), idx);
    InsertOrDie(&dd_by_uuid_idx, idx, dd.get());
    InsertOrDie(&uuid_idx_by_dd, dd.get(), idx);
    InsertOrDie(&tablets_by_uuid_idx_map_, idx, {});
  }

  data_dirs_.swap(dds);
  uuid_by_idx_.swap(uuid_by_idx);
  idx_by_uuid_.swap(idx_by_uuid);
  data_dir_by_uuid_idx_.swap(dd_by_uuid_idx);
  uuid_idx_by_data_dir_.swap(uuid_idx_by_dd);
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
  for (uint16_t uuid_idx : group_from_pb.uuid_indices()) {
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
  int group_target_size = std::min(FLAGS_fs_target_data_dirs_per_tablet,
                                   static_cast<int>(data_dirs_.size()));
  vector<uint16_t> group_indices;
  if (group_target_size == 0 || mode == DirDistributionMode::ACROSS_ALL_DIRS) {
    // If using all dirs, add all regardless of directory state.
    AppendKeysFromMap(data_dir_by_uuid_idx_, &group_indices);
  } else {
    // Randomly select directories, giving preference to those with fewer tablets.
    if (PREDICT_FALSE(!failed_data_dirs_.empty())) {
      group_target_size = std::min(group_target_size,
          static_cast<int>(data_dirs_.size()) - static_cast<int>(failed_data_dirs_.size()));
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
  for (uint16_t uuid_idx : group_indices) {
    InsertOrDie(&FindOrDie(tablets_by_uuid_idx_map_, uuid_idx), tablet_id);
  }
  return Status::OK();
}

Status DataDirManager::GetNextDataDir(const CreateBlockOptions& opts, DataDir** dir) {
  shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
  const vector<uint16_t>* group_uuid_indices;
  vector<uint16_t> valid_uuid_indices;
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
    uint16_t uuid_idx = (*group_uuid_indices)[i];
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
  return Status::IOError(Substitute("No directories available to add to $0directory group ($1 dirs "
                         "total, $2 full, $3 failed).", tablet_id_str, data_dirs_.size(),
                         metrics_->data_dirs_full.get(), metrics_->data_dirs_failed.get()), "",
                         ENOSPC);
}

void DataDirManager::DeleteDataDirGroup(const std::string& tablet_id) {
  std::lock_guard<percpu_rwlock> lock(dir_group_lock_);
  DataDirGroup* group = FindOrNull(group_by_tablet_map_, tablet_id);
  if (group == nullptr) {
    return;
  }
  // Remove the tablet_id from every data dir in its group.
  for (uint16_t uuid_idx : group->uuid_indices()) {
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
                                               vector<uint16_t>* group_indices) {
  vector<uint16_t> candidate_indices;
  for (auto& e : data_dir_by_uuid_idx_) {
    RETURN_NOT_OK(e.second->RefreshIsFull(DataDir::RefreshMode::ALWAYS));
    // TODO(awong): If a disk is unhealthy at the time of group creation, the
    // resulting group may be below targeted size. Add functionality to resize
    // groups. See KUDU-2040 for more details.
    if (!e.second->is_full() && !ContainsKey(failed_data_dirs_, e.first)) {
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

DataDir* DataDirManager::FindDataDirByUuidIndex(uint16_t uuid_idx) const {
  DCHECK_LT(uuid_idx, data_dirs_.size());
  return FindPtrOrNull(data_dir_by_uuid_idx_, uuid_idx);
}

bool DataDirManager::FindUuidIndexByDataDir(DataDir* dir, uint16_t* uuid_idx) const {
  return FindCopy(uuid_idx_by_data_dir_, dir, uuid_idx);
}

set<string> DataDirManager::FindTabletsByDataDirUuidIdx(uint16_t uuid_idx) {
  DCHECK_LT(uuid_idx, data_dirs_.size());
  shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
  set<string>* tablet_set_ptr = FindOrNull(tablets_by_uuid_idx_map_, uuid_idx);
  if (tablet_set_ptr) {
    return *tablet_set_ptr;
  }
  return {};
}

void DataDirManager::MarkDataDirFailed(uint16_t uuid_idx, const string& error_message) {
  DCHECK_LT(uuid_idx, data_dirs_.size());
  std::lock_guard<percpu_rwlock> lock(dir_group_lock_);
  DataDir* dd = FindDataDirByUuidIndex(uuid_idx);
  DCHECK(dd);
  if (InsertIfNotPresent(&failed_data_dirs_, uuid_idx)) {
    if (metrics_) {
      metrics_->data_dirs_failed->IncrementBy(1);
    }
    string error_prefix = "";
    if (!error_message.empty()) {
      error_prefix = Substitute("$0: ", error_message);
    }
    LOG(ERROR) << error_prefix << Substitute("Directory $0 marked as failed", dd->dir());
  }
}

bool DataDirManager::IsDataDirFailed(uint16_t uuid_idx) const {
  DCHECK_LT(uuid_idx, data_dirs_.size());
  shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
  return ContainsKey(failed_data_dirs_, uuid_idx);
}

void DataDirManager::RemoveUnhealthyDataDirsUnlocked(const vector<uint16_t>& uuid_indices,
                                                     vector<uint16_t>* healthy_indices) const {
  if (PREDICT_TRUE(failed_data_dirs_.empty())) {
    return;
  }
  healthy_indices->clear();
  for (uint16_t uuid_idx : uuid_indices) {
    DCHECK_LT(uuid_idx, data_dirs_.size());
    if (!ContainsKey(failed_data_dirs_, uuid_idx)) {
      healthy_indices->emplace_back(uuid_idx);
    }
  }
}

} // namespace fs
} // namespace kudu
