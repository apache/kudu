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
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/common.h>

#include "kudu/fs/block_manager.h"
#include "kudu/fs/dir_util.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/bind.h"
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
#include "kudu/util/pb_util.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util_prod.h"
#include "kudu/util/threadpool.h"

DEFINE_int32(fs_target_data_dirs_per_tablet, 3,
             "Indicates the target number of data dirs to spread each "
             "tablet's data across. If greater than the number of data dirs "
             "available, data will be striped across those available. A "
             "value of 0 indicates striping should occur across all healthy "
             "data dirs. Using fewer data dirs per tablet means a single "
             "drive failure will be less likely to affect a given tablet.");
DEFINE_validator(fs_target_data_dirs_per_tablet,
    [](const char* /*n*/, int32_t v) { return v >= 0; });
TAG_FLAG(fs_target_data_dirs_per_tablet, advanced);
TAG_FLAG(fs_target_data_dirs_per_tablet, evolving);

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

DEFINE_int32(fs_data_dirs_available_space_cache_seconds, 10,
             "Number of seconds we cache the available disk space in the block manager. ");
TAG_FLAG(fs_data_dirs_available_space_cache_seconds, advanced);
TAG_FLAG(fs_data_dirs_available_space_cache_seconds, evolving);

DEFINE_bool(fs_lock_data_dirs, true,
            "Lock the data directories to prevent concurrent usage. "
            "Note that read-only concurrent usage is still allowed.");
TAG_FLAG(fs_lock_data_dirs, unsafe);
TAG_FLAG(fs_lock_data_dirs, evolving);

DEFINE_bool(fs_data_dirs_consider_available_space, true,
            "Whether to consider available space when selecting a data "
            "directory during tablet or data block creation.");
TAG_FLAG(fs_data_dirs_consider_available_space, runtime);
TAG_FLAG(fs_data_dirs_consider_available_space, evolving);

DEFINE_uint64(fs_max_thread_count_per_data_dir, 8,
              "Maximum work thread per data directory.");
TAG_FLAG(fs_max_thread_count_per_data_dir, advanced);

METRIC_DEFINE_gauge_uint64(server, data_dirs_failed,
                           "Data Directories Failed",
                           kudu::MetricUnit::kDataDirectories,
                           "Number of data directories whose disks are currently "
                           "in a failed state",
                           kudu::MetricLevel::kWarn);
METRIC_DEFINE_gauge_uint64(server, data_dirs_full,
                           "Data Directories Full",
                           kudu::MetricUnit::kDataDirectories,
                           "Number of data directories whose disks are currently full",
                           kudu::MetricLevel::kWarn);

DECLARE_bool(enable_data_block_fsync);
DECLARE_string(block_manager);

namespace kudu {

namespace fs {

using internal::DataDirGroup;
using std::default_random_engine;
using std::pair;
using std::set;
using std::shuffle;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;
using strings::SubstituteAndAppend;


namespace {

// Wrapper for env_util::DeleteTmpFilesRecursively that is suitable for parallel
// execution on a data directory's thread pool (which requires the return value
// be void).
void DeleteTmpFilesRecursively(Env* env, const string& path) {
  WARN_NOT_OK(env_util::DeleteTmpFilesRecursively(env, path),
              "Error while deleting temp files");
}

} // anonymous namespace

////////////////////////////////////////////////////////////
// DataDirMetrics
////////////////////////////////////////////////////////////

#define GINIT(x) x(METRIC_##x.Instantiate(entity, 0))
DataDirMetrics::DataDirMetrics(const scoped_refptr<MetricEntity>& entity)
  : GINIT(data_dirs_failed),
    GINIT(data_dirs_full) {
}
#undef GINIT

////////////////////////////////////////////////////////////
// DataDir
////////////////////////////////////////////////////////////

DataDir::DataDir(Env* env,
                 DataDirMetrics* metrics,
                 DataDirFsType fs_type,
                 string dir,
                 unique_ptr<DirInstanceMetadataFile> metadata_file,
                 unique_ptr<ThreadPool> pool)
    : env_(env),
      metrics_(metrics),
      fs_type_(fs_type),
      dir_(std::move(dir)),
      metadata_file_(std::move(metadata_file)),
      pool_(std::move(pool)),
      is_shutdown_(false),
      is_full_(false),
      available_bytes_(0) {
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

Status DataDir::RefreshAvailableSpace(RefreshMode mode) {
  switch (mode) {
    case RefreshMode::EXPIRED_ONLY: {
      std::lock_guard<simple_spinlock> l(lock_);
      DCHECK(last_space_check_.Initialized());
      MonoTime expiry = last_space_check_ + MonoDelta::FromSeconds(
          FLAGS_fs_data_dirs_available_space_cache_seconds);
      if (MonoTime::Now() < expiry) {
        break;
      }
      FALLTHROUGH_INTENDED; // Root was previously full, check again.
    }
    case RefreshMode::ALWAYS: {
      int64_t available_bytes_new;
      Status s = env_util::VerifySufficientDiskSpace(
          env_, dir_, 0, FLAGS_fs_data_dirs_reserved_bytes, &available_bytes_new);
      bool is_full_new;
      if (PREDICT_FALSE(s.IsIOError() && s.posix_code() == ENOSPC)) {
        LOG(WARNING) << Substitute(
            "Insufficient disk space under path $0: creation of new data "
            "blocks under this path can be retried after $1 seconds: $2",
            dir_, FLAGS_fs_data_dirs_available_space_cache_seconds, s.ToString());
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
        last_space_check_ = MonoTime::Now();
        available_bytes_ = available_bytes_new;
      }
      break;
    }
    default:
      LOG(FATAL) << "Unknown check mode";
  }
  return Status::OK();
}

////////////////////////////////////////////////////////////
// DataDirGroup
////////////////////////////////////////////////////////////

DataDirGroup::DataDirGroup() {}

DataDirGroup::DataDirGroup(vector<int> uuid_indices)
    : uuid_indices_(std::move(uuid_indices)) {}

Status DataDirGroup::LoadFromPB(const UuidIndexByUuidMap& uuid_idx_by_uuid,
                                const DataDirGroupPB& pb) {
  vector<int> uuid_indices;
  for (const auto& uuid : pb.uuids()) {
    int uuid_idx;
    if (!FindCopy(uuid_idx_by_uuid, uuid, &uuid_idx)) {
      return Status::NotFound(Substitute(
          "could not find data dir with uuid $0", uuid));
    }
    uuid_indices.emplace_back(uuid_idx);
  }

  uuid_indices_ = std::move(uuid_indices);
  return Status::OK();
}

Status DataDirGroup::CopyToPB(const UuidByUuidIndexMap& uuid_by_uuid_idx,
                              DataDirGroupPB* pb) const {
  DCHECK(pb);
  DataDirGroupPB group;
  for (auto uuid_idx : uuid_indices_) {
    string uuid;
    if (!FindCopy(uuid_by_uuid_idx, uuid_idx, &uuid)) {
      return Status::NotFound(Substitute(
          "could not find data dir with uuid index $0", uuid_idx));
    }
    group.mutable_uuids()->Add(std::move(uuid));
  }

  *pb = std::move(group);
  return Status::OK();
}

////////////////////////////////////////////////////////////
// DataDirManagerOptions
////////////////////////////////////////////////////////////

DataDirManagerOptions::DataDirManagerOptions()
    : block_manager_type(FLAGS_block_manager),
      read_only(false),
      update_instances(UpdateInstanceBehavior::UPDATE_AND_IGNORE_FAILURES) {
}

////////////////////////////////////////////////////////////
// DataDirManager
////////////////////////////////////////////////////////////

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
      opts_(std::move(opts)),
      canonicalized_data_fs_roots_(std::move(canonicalized_data_roots)),
      rng_(GetRandomSeed32()) {
  DCHECK_GT(canonicalized_data_fs_roots_.size(), 0);
  DCHECK(opts_.update_instances == UpdateInstanceBehavior::DONT_UPDATE || !opts_.read_only);

  if (opts_.metric_entity) {
    metrics_.reset(new DataDirMetrics(opts_.metric_entity));
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
  for (const auto& r : data_fs_roots) {
    roots.push_back({ r, Status::OK() });
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
  for (const auto& r : data_fs_roots) {
    roots.push_back({ r, Status::OK() });
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
  CHECK(!opts_.read_only);

  vector<string> all_uuids;
  for (const auto& r : canonicalized_data_fs_roots_) {
    RETURN_NOT_OK_PREPEND(r.status, "Could not create directory manager with disks failed");
  }
  vector<unique_ptr<DirInstanceMetadataFile>> loaded_instances;
  bool has_existing_instances;
  RETURN_NOT_OK(LoadInstances(&loaded_instances, &has_existing_instances));
  if (has_existing_instances) {
    return Status::AlreadyPresent("instance files already exist");
  }

  // If none of the instances exist, we can assume this is a new deployment and
  // we should try creating some a new set of instance files.
  RETURN_NOT_OK_PREPEND(CreateNewDirectoriesAndUpdateInstances(std::move(loaded_instances)),
                        "could not create new data directories");
  return Status::OK();
}

Status DataDirManager::CreateNewDirectoriesAndUpdateInstances(
    vector<unique_ptr<DirInstanceMetadataFile>> instances) {
  CHECK(!opts_.read_only);
  CHECK_NE(UpdateInstanceBehavior::DONT_UPDATE, opts_.update_instances);

  vector<string> created_dirs;
  vector<string> created_files;
  auto deleter = MakeScopedCleanup([&]() {
    // Delete files first so that the directories will be empty when deleted.
    for (const auto& f : created_files) {
      WARN_NOT_OK(env_->DeleteFile(f), "Could not delete file " + f);
    }
    // Delete directories in reverse order since parent directories will have
    // been added before child directories.
    for (auto it = created_dirs.rbegin(); it != created_dirs.rend(); it++) {
      WARN_NOT_OK(env_->DeleteDir(*it), "Could not delete dir " + *it);
    }
  });

  // First, de-duplicate the instance UUIDs. If we have duplicates, something's
  // wrong. Maybe an operator manually duplicated some instance files.
  set<string> all_uuids;
  for (const auto& instance : instances) {
    InsertIfNotPresent(&all_uuids, instance->uuid());
  }
  if (all_uuids.size() != instances.size()) {
    return Status::InvalidArgument(
        Substitute("instance files contain duplicate UUIDs: $0 directories provided, "
                   "$1 unique UUIDs found ($2)", instances.size(), all_uuids.size(),
                   JoinStrings(all_uuids, ", ")));
  }

  // Determine which instance files are healthy (and can thus be updated), and
  // which don't exist. Create any that don't exist.
  //
  // Note: we don't bother trying to create/update the instance if the file is
  // otherwise unhealthy.
  vector<unique_ptr<DirInstanceMetadataFile>> healthy_instances;
  for (auto& instance : instances) {
    if (instance->healthy()) {
      healthy_instances.emplace_back(std::move(instance));
      continue;
    }
    if (instance->health_status().IsNotFound()) {
      bool created_dir = false;
      RETURN_NOT_OK(instance->Create(all_uuids, &created_dir));
      if (created_dir) {
        created_dirs.emplace_back(instance->dir());
      }
      created_files.emplace_back(instance->path());
    }
  }

  // Go through the healthy instances and look for instances that don't have
  // the full complete set of instance UUIDs.
  vector<unique_ptr<DirInstanceMetadataFile>> instances_to_update;
  for (auto& instance : healthy_instances) {
    DCHECK(instance->healthy());
    const auto& dir_set = instance->metadata()->dir_set();
    set<string> instance_uuids;
    for (int i = 0; i < dir_set.all_uuids_size(); i++) {
      InsertIfNotPresent(&instance_uuids, dir_set.all_uuids(i));
    }
    // If an instance file disagrees with the expected UUIDs, rewrite it.
    if (all_uuids != instance_uuids) {
      instances_to_update.emplace_back(std::move(instance));
    }
  }

  // If any of the instance files need to be updated because they didn't match
  // the expected set of UUIDs, update them now.
  // Note: Having a consistent set of instance files isn't a correctness
  // requirement, but it can be useful for degbugging.
  if (!instances_to_update.empty()) {
    RETURN_NOT_OK(UpdateHealthyInstances(instances_to_update, all_uuids));
  }

  // Ensure newly created directories are synchronized to disk.
  if (FLAGS_enable_data_block_fsync) {
    WARN_NOT_OK(env_util::SyncAllParentDirs(env_, created_dirs, created_files),
                "could not sync newly created data directories");
  }

  // Success: don't delete any files.
  deleter.cancel();
  return Status::OK();
}

Status DataDirManager::UpdateHealthyInstances(
    const vector<unique_ptr<DirInstanceMetadataFile>>& instances_to_update,
    const set<string>& new_all_uuids) {
  unordered_map<string, string> copies_to_restore;
  unordered_set<string> copies_to_delete;
  auto cleanup = MakeScopedCleanup([&] {
    for (const auto& f : copies_to_delete) {
      WARN_NOT_OK(env_->DeleteFile(f), Substitute("Could not delete file $0", f));
    }
    for (const auto& copy_and_original : copies_to_restore) {
      const auto& copy_filename = copy_and_original.first;
      const auto& original_filename = copy_and_original.second;
      WARN_NOT_OK(env_->RenameFile(copy_filename, original_filename),
          Substitute("Could not restore file $0 from $1", original_filename, copy_filename));
    }
  });
  // Make a copy of every existing instance metadata file. This is done before
  // performing any updates, so that if there's a failure while copying,
  // there's no metadata to restore.
  //
  // We'll keep track of the copies so we can delete them on success, or use
  // them to restore on failure.
  WritableFileOptions opts;
  opts.sync_on_close = true;
  for (const auto& instance : instances_to_update) {
    if (!instance->healthy()) {
      continue;
    }
    const string& instance_filename = instance->path();
    string copy_filename = instance_filename + kTmpInfix;
    Status s = env_util::CopyFile(env_, instance_filename, copy_filename, opts);
    if (PREDICT_FALSE(!s.ok())) {
      s = s.CloneAndPrepend("unable to backup existing instance file");
      instance->SetInstanceFailed(s);
      LOG(WARNING) << s.ToString();
      continue;
    }
    InsertOrDie(&copies_to_delete, copy_filename);
  }

  // Update the instance metadata files with the new set of UUIDs.
  for (const auto& instance : instances_to_update) {
    if (!instance->healthy()) {
      continue;
    }
    const string& instance_filename = instance->path();
    string copy_filename = instance_filename + kTmpInfix;

    // Put together the PB and perform the update.
    DirInstanceMetadataPB new_pb = *instance->metadata();
    new_pb.mutable_dir_set()->mutable_all_uuids()->Clear();
    for (const auto& uuid : new_all_uuids) {
      new_pb.mutable_dir_set()->add_all_uuids(uuid);
    }

    // We're about to update the file; if we fail midway, we should try to
    // restore them from our backups if we can.
    InsertOrDie(&copies_to_restore, copy_filename, instance_filename);
    CHECK_EQ(1, copies_to_delete.erase(copy_filename));
    Status s = pb_util::WritePBContainerToPath(
        env_, instance_filename, new_pb, pb_util::OVERWRITE,
        FLAGS_enable_data_block_fsync ? pb_util::SYNC : pb_util::NO_SYNC);
    // We've failed to update for some reason, so restore our original file.
    // Since we're renaming our copy, we don't have to delete it.
    if (PREDICT_FALSE(!s.ok())) {
      s = s.CloneAndPrepend("unable to update instance file");
      instance->SetInstanceFailed(s);
      LOG(WARNING) << Substitute("unable to overwrite existing instance file $0: $1",
                                 instance_filename, s.ToString());
    }
  }

  // If we are not tolerating errors (e.g. we're running the update_dirs tool)
  // and we've hit an error, return now and clean up what we've changed.
  if (opts_.update_instances == UpdateInstanceBehavior::UPDATE_AND_ERROR_ON_FAILURE) {
    for (const auto& instance : instances_to_update) {
      RETURN_NOT_OK_PREPEND(instance->health_status(),
          "at least one instance file failed to update");
    }
  }

  // Success; we only need to delete our copies.
  InsertKeysFromMap(copies_to_restore, &copies_to_delete);
  copies_to_restore.clear();
  return Status::OK();
}

Status DataDirManager::LoadInstances(
    vector<unique_ptr<DirInstanceMetadataFile>>* instance_files,
    bool* has_existing_instances) {
  LockMode lock_mode;
  if (!FLAGS_fs_lock_data_dirs) {
    lock_mode = LockMode::NONE;
  } else if (opts_.read_only) {
    lock_mode = LockMode::OPTIONAL;
  } else {
    lock_mode = LockMode::MANDATORY;
  }
  vector<string> missing_roots_tmp;
  vector<unique_ptr<DirInstanceMetadataFile>> loaded_instances;
  ObjectIdGenerator gen;
  for (int i = 0; i < canonicalized_data_fs_roots_.size(); i++) {
    const auto& root = canonicalized_data_fs_roots_[i];
    string data_dir = JoinPathSegments(root.path, kDataDirName);
    string instance_filename = JoinPathSegments(data_dir, kInstanceMetadataFileName);

    // Initialize the instance with a backup UUID. In case the load fails, this
    // will be the UUID for our instnace.
    string backup_uuid = gen.Next();
    unique_ptr<DirInstanceMetadataFile> instance(
        new DirInstanceMetadataFile(env_, std::move(backup_uuid), opts_.block_manager_type,
                                     instance_filename));
    if (PREDICT_FALSE(!root.status.ok())) {
      instance->SetInstanceFailed(root.status);
    } else {
      // This may return OK and mark 'instance' as unhealthy if the file could
      // not be loaded (e.g. not found, disk errors).
      RETURN_NOT_OK_PREPEND(instance->LoadFromDisk(),
                            Substitute("could not load $0", instance_filename));
    }

    // Try locking the instance.
    if (instance->healthy() && lock_mode != LockMode::NONE) {
      // This may return OK and mark 'instance' as unhealthy if the file could
      // not be locked due to non-locking issues (e.g. disk errors).
      Status s = instance->Lock();
      if (!s.ok()) {
        if (lock_mode == LockMode::OPTIONAL) {
          LOG(WARNING) << s.ToString();
          LOG(WARNING) << "Proceeding without lock";
        } else {
          DCHECK(LockMode::MANDATORY == lock_mode);
          return s;
        }
      }
    }
    loaded_instances.emplace_back(std::move(instance));
  }

  int num_healthy_instances = 0;
  for (const auto& instance : loaded_instances) {
    if (instance->healthy()) {
      num_healthy_instances++;
    }
  }
  if (has_existing_instances) {
    *has_existing_instances = num_healthy_instances > 0;
  }
  instance_files->swap(loaded_instances);
  return Status::OK();
}

Status DataDirManager::PopulateDirectoryMaps(const vector<unique_ptr<DataDir>>& dds) {
  // Helper lambda to add a directory to the maps.
  const auto insert_to_maps = [&] (const string& uuid, int idx, DataDir* dd) {
    if (!dd->instance()->healthy()) {
      if (metrics_) {
        metrics_->data_dirs_failed->IncrementBy(1);
      }
      InsertOrDie(&failed_data_dirs_, idx);
    }
    InsertOrDie(&uuid_by_root_, DirName(dd->dir()), uuid);
    InsertOrDie(&uuid_by_idx_, idx, uuid);
    InsertOrDie(&idx_by_uuid_, uuid, idx);
    InsertOrDie(&data_dir_by_uuid_idx_, idx, dd);
    InsertOrDie(&uuid_idx_by_data_dir_, dd, idx);
    InsertOrDie(&tablets_by_uuid_idx_map_, idx, {});
  };

  if (opts_.block_manager_type == "file") {
    // When assigning directories for the file block manager, the UUID indexes
    // must match what exists in the instance files' list of UUIDs.
    unordered_map<string, int> uuid_to_idx;
    for (const auto& dd : dds) {
      // Find a healthy instance file and use its set of UUIDs.
      if (dd->instance()->healthy()) {
        const auto& dir_set = dd->instance()->metadata()->dir_set();
        VLOG(1) << Substitute("using dir set $0 as reference: $1",
            dd->instance()->path(), pb_util::SecureDebugString(dir_set));
        for (int idx = 0; idx < dir_set.all_uuids_size(); idx++) {
          const string& uuid = dir_set.all_uuids(idx);
          InsertIfNotPresent(&uuid_to_idx, uuid, idx);
        }
        break;
      }
    }
    // We should have the same number of UUID assignments as directories.
    if (dds.size() != uuid_to_idx.size()) {
      return Status::Corruption(
          Substitute("instance file is corrupted: $0 unique UUIDs expected, got $1",
                     dds.size(), uuid_to_idx.size()));
    }
    // Keep track of any dirs that were not referenced in the dir set. These
    // are presumably from instance files we failed to read. We'll assign them
    // indexes of those that remain.
    vector<DataDir*> unassigned_dirs;
    for (const auto& dd : dds) {
      const auto& uuid = dd->instance()->uuid();
      int* idx = FindOrNull(uuid_to_idx, uuid);
      if (idx) {
        insert_to_maps(uuid, *idx, dd.get());
        uuid_to_idx.erase(uuid);
      } else {
        LOG(WARNING) << Substitute("instance $0 has unknown UUID $1",
                                   dd->instance()->path(), uuid);
        unassigned_dirs.emplace_back(dd.get());
      }
    }
    DCHECK_EQ(unassigned_dirs.size(), uuid_to_idx.size());
    int unassigned_dir_idx = 0;
    for (const auto& failed_uuid_and_idx : uuid_to_idx) {
      insert_to_maps(failed_uuid_and_idx.first, failed_uuid_and_idx.second,
                     unassigned_dirs[unassigned_dir_idx++]);
    }
  } else {
    // Go through our instances and assign them each a UUID index.
    for (int idx = 0; idx < dds.size(); idx++) {
      DataDir* dd = dds[idx].get();
      insert_to_maps(dd->instance()->uuid(), idx, dd);
    }
  }
  return Status::OK();
}

Status DataDirManager::Open() {
  const int kMaxDataDirs = opts_.block_manager_type == "file" ? (1 << 16) - 1 : kint32max;
  if (canonicalized_data_fs_roots_.size() > kMaxDataDirs) {
    return Status::InvalidArgument(Substitute("too many directories provided $0, max is $1",
                                              canonicalized_data_fs_roots_.size(), kMaxDataDirs));
  }

  vector<unique_ptr<DirInstanceMetadataFile>> loaded_instances;
  // Load the instance files from disk.
  bool has_existing_instances;
  RETURN_NOT_OK_PREPEND(LoadInstances(&loaded_instances, &has_existing_instances),
      "failed to load instance files");
  if (!has_existing_instances) {
    return Status::NotFound(
        "could not open directory manager, no healthy data directories found");
  }
  // Note: the file block manager should not be updated because its block
  // indexing algorithm depends on a fixed set of directories.
  if (!opts_.read_only && opts_.block_manager_type != "file" &&
      opts_.update_instances != UpdateInstanceBehavior::DONT_UPDATE) {
    RETURN_NOT_OK_PREPEND(
        CreateNewDirectoriesAndUpdateInstances(
            std::move(loaded_instances)),
            "could not add new data directories");
    RETURN_NOT_OK_PREPEND(LoadInstances(&loaded_instances, &has_existing_instances),
                          "failed to load instance files after updating");
    if (!has_existing_instances) {
      return Status::IOError(
          "could not open directory manager, no healthy data directories found");
    }
  }

  // All instances are present and accounted for. Time to create the in-memory
  // data directory structures.
  vector<unique_ptr<DataDir>> dds;
  for (int i = 0; i < loaded_instances.size(); i++) {
    auto& instance = loaded_instances[i];
    const string data_dir = instance->dir();

    // Figure out what filesystem the data directory is on.
    DataDirFsType fs_type = DataDirFsType::OTHER;
    if (instance->healthy()) {
      bool result = false;
      Status fs_check = env_->IsOnExtFilesystem(data_dir, &result);
      if (fs_check.ok()) {
        if (result) {
          fs_type = DataDirFsType::EXT;
        } else {
          fs_check = env_->IsOnXfsFilesystem(data_dir, &result);
          if (fs_check.ok() && result) {
            fs_type = DataDirFsType::XFS;
          }
        }
      }
      // If we hit a disk error, consider the directory failed.
      if (PREDICT_FALSE(fs_check.IsDiskFailure())) {
        instance->SetInstanceFailed(fs_check.CloneAndPrepend("failed to check FS type"));
      } else {
        RETURN_NOT_OK(fs_check);
      }
    }

    // Create a per-dir thread pool.
    unique_ptr<ThreadPool> pool;
    RETURN_NOT_OK(ThreadPoolBuilder(Substitute("data dir $0", i))
                  .set_max_threads(FLAGS_fs_max_thread_count_per_data_dir)
                  .set_trace_metric_prefix("data dirs")
                  .Build(&pool));
    unique_ptr<DataDir> dd(new DataDir(
        env_, metrics_.get(), fs_type, data_dir, std::move(instance),
        std::move(pool)));
    dds.emplace_back(std::move(dd));
  }

  // Use the per-dir thread pools to delete temporary files in parallel.
  for (const auto& dd : dds) {
    if (dd->instance()->healthy()) {
      dd->ExecClosure(Bind(&DeleteTmpFilesRecursively, env_, dd->dir()));
    }
  }
  for (const auto& dd : dds) {
    dd->WaitOnClosures();
  }

  RETURN_NOT_OK(PopulateDirectoryMaps(dds));
  data_dirs_ = std::move(dds);

  // From this point onwards, the in-memory maps are the source of truth about
  // the state of each data dir.

  // Initialize the 'fullness' status of the data directories.
  for (const auto& dd : data_dirs_) {
    int uuid_idx;
    CHECK(FindUuidIndexByDataDir(dd.get(), &uuid_idx));
    if (ContainsKey(failed_data_dirs_, uuid_idx)) {
      continue;
    }
    Status refresh_status = dd->RefreshAvailableSpace(DataDir::RefreshMode::ALWAYS);
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
  DataDirGroup group_from_pb;
  RETURN_NOT_OK_PREPEND(group_from_pb.LoadFromPB(idx_by_uuid_, pb), Substitute(
      "could not load data dir group for tablet $0", tablet_id));
  DataDirGroup* other = InsertOrReturnExisting(&group_by_tablet_map_,
                                               tablet_id,
                                               group_from_pb);
  if (other != nullptr) {
    return Status::AlreadyPresent(Substitute(
        "tried to load directory group for tablet $0 but one is already registered",
        tablet_id));
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
    GetDirsForGroupUnlocked(group_target_size, &group_indices);
    if (PREDICT_FALSE(group_indices.empty())) {
      return Status::IOError("All healthy data directories are full", "", ENOSPC);
    }
    if (PREDICT_FALSE(group_indices.size() < FLAGS_fs_target_data_dirs_per_tablet)) {
      string msg = Substitute("Could only allocate $0 dirs of requested $1 for tablet "
                              "$2. $3 dirs total", group_indices.size(),
                              FLAGS_fs_target_data_dirs_per_tablet, tablet_id, data_dirs_.size());
      if (metrics_) {
        SubstituteAndAppend(&msg, ", $0 dirs full, $1 dirs failed",
                            metrics_->data_dirs_full->value(), metrics_->data_dirs_failed->value());
      }
      LOG(INFO) << msg;
    }
  }
  InsertOrDie(&group_by_tablet_map_, tablet_id, DataDirGroup(group_indices));
  for (int uuid_idx : group_indices) {
    InsertOrDie(&FindOrDie(tablets_by_uuid_idx_map_, uuid_idx), tablet_id);
  }
  return Status::OK();
}

Status DataDirManager::GetDirForBlock(const CreateBlockOptions& opts, DataDir** dir,
                                      int* new_target_group_size) const {
  shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
  vector<int> healthy_uuid_indices;
  const DataDirGroup* group = nullptr;
  DataDirGroup group_for_tests;
  if (PREDICT_TRUE(!opts.tablet_id.empty())) {
    // Get the data dir group for the tablet.
    group = FindOrNull(group_by_tablet_map_, opts.tablet_id);
    if (group == nullptr) {
      return Status::NotFound("Tried to get directory but no directory group "
                              "registered for tablet", opts.tablet_id);
    }
  } else {
    // This should only be reached by some tests; in cases where there is no
    // natural tablet_id, select a data dir from any of the directories.
    CHECK(IsGTest());
    vector<int> all_uuid_indices;
    AppendKeysFromMap(data_dir_by_uuid_idx_, &all_uuid_indices);
    group_for_tests = DataDirGroup(std::move(all_uuid_indices));
    group = &group_for_tests;
  }
  // Within a given directory group, filter out the ones that are failed.
  if (PREDICT_TRUE(failed_data_dirs_.empty())) {
    healthy_uuid_indices = group->uuid_indices();
  } else {
    RemoveUnhealthyDataDirsUnlocked(group->uuid_indices(), &healthy_uuid_indices);
    if (healthy_uuid_indices.empty()) {
      return Status::IOError("No healthy directories exist in tablet's "
                             "directory group", opts.tablet_id, ENODEV);
    }
  }
  // Within a given directory group, filter out the ones that are full.
  vector<DataDir*> candidate_dirs;
  for (auto uuid_idx : healthy_uuid_indices) {
    DataDir* candidate = FindOrDie(data_dir_by_uuid_idx_, uuid_idx);
    Status s = candidate->RefreshAvailableSpace(DataDir::RefreshMode::EXPIRED_ONLY);
    WARN_NOT_OK(s, Substitute("failed to refresh fullness of $0", candidate->dir()));
    if (s.ok() && !candidate->is_full()) {
      candidate_dirs.emplace_back(candidate);
    }
  }

  // If all the directories in the group are full, return an ENOSPC error.
  if (PREDICT_FALSE(candidate_dirs.empty())) {
    DCHECK(group);
    size_t num_total = group->uuid_indices().size();
    size_t num_full = 0;
    for (const auto& idx : group->uuid_indices()) {
      if (FindOrDie(data_dir_by_uuid_idx_, idx)->is_full()) {
        num_full++;
      }
    }
    size_t num_failed = num_total - num_full;
    *new_target_group_size = num_total + 1;
    return Status::IOError(
        Substitute("No directories available in $0's directory group ($1 dirs "
                   "total, $2 failed, $3 full)",
                   opts.tablet_id, num_total, num_failed, num_full),
        "", ENOSPC);
  }
  if (candidate_dirs.size() == 1) {
    *dir = candidate_dirs[0];
    return Status::OK();
  }
  // Pick two randomly and select the one with more space.
  shuffle(candidate_dirs.begin(), candidate_dirs.end(),
          default_random_engine(rng_.Next()));
  *dir = PREDICT_TRUE(FLAGS_fs_data_dirs_consider_available_space) &&
         candidate_dirs[0]->available_bytes() > candidate_dirs[1]->available_bytes() ?
           candidate_dirs[0] : candidate_dirs[1];
  return Status::OK();
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

Status DataDirManager::GetDataDirGroupPB(const string& tablet_id,
                                         DataDirGroupPB* pb) const {
  shared_lock<rw_spinlock> lock(dir_group_lock_.get_lock());
  const DataDirGroup* group = FindOrNull(group_by_tablet_map_, tablet_id);
  if (group == nullptr) {
    return Status::NotFound(Substitute(
        "could not find data dir group for tablet $0", tablet_id));
  }
  RETURN_NOT_OK(group->CopyToPB(uuid_by_idx_, pb));
  return Status::OK();
}

Status DataDirManager::GetDirAddIfNecessary(const CreateBlockOptions& opts, DataDir** dir) {
  int new_target_group_size = 0;
  Status s = GetDirForBlock(opts, dir, &new_target_group_size);
  if (PREDICT_TRUE(s.ok())) {
    return Status::OK();
  }
  const string& tablet_id = opts.tablet_id;
  if (tablet_id.empty()) {
    // This should only be reached by some tests; in cases where there is no
    // natural tablet_id. Just return whatever error we got.
    CHECK(IsGTest());
    return s;
  }
  // If we failed for a reason other than lack of space in the data dir gruop,
  // return the error.
  if (!s.IsIOError() || s.posix_code() != ENOSPC) {
    return s;
  }
  // If we couldn't get a directory because the group is out of space, try
  // adding a new directory to the group.
  DCHECK_GT(new_target_group_size, 0);
  std::lock_guard<percpu_rwlock> l(dir_group_lock_);
  const DataDirGroup& group = FindOrDie(group_by_tablet_map_, tablet_id);
  // If we're already at the new target group size (e.g. because another
  // thread has added a directory), just return the newly added directory.
  if (new_target_group_size <= group.uuid_indices().size()) {
    *dir = FindOrDie(data_dir_by_uuid_idx_, group.uuid_indices().back());
    return Status::OK();
  }
  vector<int> group_uuid_indices = group.uuid_indices();
  GetDirsForGroupUnlocked(new_target_group_size, &group_uuid_indices);
  if (PREDICT_FALSE(group_uuid_indices.size() < new_target_group_size)) {
    // If we couldn't add to the group, return an error.
    int num_total = data_dirs_.size();
    int num_failed = failed_data_dirs_.size();
    int num_full = 0;
    for (const auto& dd : data_dirs_) {
      if (dd->is_full()) num_full++;
    }
    return Status::IOError(
        Substitute("No directories could be added ($0 dirs total, $1 failed, $2 full): $3",
        num_total, num_failed, num_full, s.ToString()), "", ENODEV);
  }
  // Update the groups. The tablet ID should not be associated with the data
  // dir already, and we should update the existing data dir group.
  // NOTE: it is not the responsibility of the DataDirManager to persist groups
  // to disk. On-disk representations of data dir groups (e.g. those in the
  // TabletSuperBlockPB) should be re-written upon modifying them.
  int new_uuid_idx = group_uuid_indices.back();
  InsertOrDie(&FindOrDie(tablets_by_uuid_idx_map_, new_uuid_idx), tablet_id);
  CHECK(!EmplaceOrUpdate(&group_by_tablet_map_, tablet_id, DataDirGroup(group_uuid_indices)));
  *dir = FindOrDie(data_dir_by_uuid_idx_, new_uuid_idx);
  LOG(INFO) << Substitute("Added $0 to $1's directory group: $2",
                          (*dir)->dir(), tablet_id, s.ToString());
  return Status::OK();
}

void DataDirManager::GetDirsForGroupUnlocked(int target_size,
                                             vector<int>* group_indices) {
  DCHECK(dir_group_lock_.is_locked());
  vector<int> candidate_indices;
  unordered_set<int> existing_group_indices(group_indices->begin(), group_indices->end());
  for (auto& e : data_dir_by_uuid_idx_) {
    int uuid_idx = e.first;
    DCHECK_LT(uuid_idx, data_dirs_.size());
    if (ContainsKey(existing_group_indices, uuid_idx) ||
        ContainsKey(failed_data_dirs_, uuid_idx)) {
      continue;
    }
    DataDir* dd = e.second;
    Status s = dd->RefreshAvailableSpace(DataDir::RefreshMode::ALWAYS);
    WARN_NOT_OK(s, Substitute("failed to refresh fullness of $0", dd->dir()));
    if (s.ok() && !dd->is_full()) {
      // TODO(awong): If a disk is unhealthy at the time of group creation, the
      // resulting group may be below targeted size. Add functionality to
      // resize groups. See KUDU-2040 for more details.
      candidate_indices.push_back(uuid_idx);
    }
  }
  while (group_indices->size() < target_size && !candidate_indices.empty()) {
    shuffle(candidate_indices.begin(), candidate_indices.end(), default_random_engine(rng_.Next()));
    if (candidate_indices.size() == 1) {
      group_indices->push_back(candidate_indices[0]);
      candidate_indices.erase(candidate_indices.begin());
    } else {
      int tablets_in_first = FindOrDie(tablets_by_uuid_idx_map_, candidate_indices[0]).size();
      int tablets_in_second = FindOrDie(tablets_by_uuid_idx_map_, candidate_indices[1]).size();
      int selected_index = 0;
      if (tablets_in_first == tablets_in_second &&
          PREDICT_TRUE(FLAGS_fs_data_dirs_consider_available_space)) {
        int64_t space_in_first = FindOrDie(data_dir_by_uuid_idx_,
                                           candidate_indices[0])->available_bytes();
        int64_t space_in_second = FindOrDie(data_dir_by_uuid_idx_,
                                            candidate_indices[1])->available_bytes();
        selected_index = space_in_first > space_in_second ? 0 : 1;
      } else {
        selected_index = tablets_in_first < tablets_in_second ? 0 : 1;
      }
      group_indices->push_back(candidate_indices[selected_index]);
      candidate_indices.erase(candidate_indices.begin() + selected_index);
    }
  }
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

Status DataDirManager::FindDataDirsByTabletId(const string& tablet_id,
                                              vector<string>* data_dirs) const {
  CHECK(data_dirs);
  DataDirGroupPB group;
  RETURN_NOT_OK(GetDataDirGroupPB(tablet_id, &group));
  vector<string> data_dirs_tmp;
  data_dirs_tmp.reserve(group.uuids_size());
  for (const auto& uuid : group.uuids()) {
    int uuid_idx;
    if (!FindUuidIndexByUuid(uuid, &uuid_idx)) {
      return Status::NotFound("unable to find index for UUID", uuid);
    }
    const auto* data_dir = FindDataDirByUuidIndex(uuid_idx);
    if (!data_dir) {
      return Status::NotFound(
          Substitute("unable to find data dir for UUID $0 with index $1",
                     uuid, uuid_idx));
    }
    data_dirs_tmp.emplace_back(data_dir->dir());
  }
  std::sort(data_dirs_tmp.begin(), data_dirs_tmp.end());
  *data_dirs = std::move(data_dirs_tmp);
  return Status::OK();
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
