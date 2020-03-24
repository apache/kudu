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
#include <memory>
#include <mutex>
#include <ostream>
#include <random>
#include <set>
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
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
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


////////////////////////////////////////////////////////////
// DataDirMetrics
////////////////////////////////////////////////////////////

#define GINIT(member, x) member = METRIC_##x.Instantiate(metric_entity, 0)
DataDirMetrics::DataDirMetrics(const scoped_refptr<MetricEntity>& metric_entity) {
  GINIT(dirs_failed, data_dirs_failed);
  GINIT(dirs_full, data_dirs_full);
}
#undef GINIT

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
// DataDir
////////////////////////////////////////////////////////////

DataDir::DataDir(Env* env, DirMetrics* metrics, FsType fs_type, std::string dir,
                 std::unique_ptr<DirInstanceMetadataFile> metadata_file,
                 std::unique_ptr<ThreadPool> pool)
    : Dir(env, metrics, fs_type, std::move(dir), std::move(metadata_file), std::move(pool)) {}

std::unique_ptr<Dir> DataDirManager::CreateNewDir(
    Env* env, DirMetrics* metrics, FsType fs_type,
    std::string dir, std::unique_ptr<DirInstanceMetadataFile> metadata_file,
    std::unique_ptr<ThreadPool> pool) {
  return unique_ptr<Dir>(new DataDir(env, metrics, fs_type, std::move(dir),
                                     std::move(metadata_file), std::move(pool)));
}

int DataDir::available_space_cache_secs() const {
  return FLAGS_fs_data_dirs_available_space_cache_seconds;
}

int DataDir::reserved_bytes() const {
  return FLAGS_fs_data_dirs_reserved_bytes;
}

////////////////////////////////////////////////////////////
// DataDirManager
////////////////////////////////////////////////////////////

DataDirManagerOptions::DataDirManagerOptions()
    : DirManagerOptions(FLAGS_block_manager) {}

DataDirManager::DataDirManager(Env* env,
                               const DataDirManagerOptions& opts,
                               CanonicalizedRootsList canonicalized_data_roots)
    : DirManager(env, opts.metric_entity ?
                          unique_ptr<DirMetrics>(new DataDirMetrics(opts.metric_entity)) : nullptr,
                 FLAGS_fs_max_thread_count_per_data_dir,
                 opts, std::move(canonicalized_data_roots)) {}

Status DataDirManager::OpenExistingForTests(Env* env,
                                            vector<string> data_fs_roots,
                                            const DataDirManagerOptions& opts,
                                            unique_ptr<DataDirManager>* dd_manager) {
  CanonicalizedRootsList roots;
  for (const auto& r : data_fs_roots) {
    roots.push_back({ r, Status::OK() });
  }
  return DataDirManager::OpenExisting(env, std::move(roots), opts, dd_manager);
}

Status DataDirManager::OpenExisting(Env* env, CanonicalizedRootsList data_fs_roots,
                                    const DataDirManagerOptions& opts,
                                    unique_ptr<DataDirManager>* dd_manager) {
  unique_ptr<DataDirManager> dm;
  dm.reset(new DataDirManager(env, opts, std::move(data_fs_roots)));
  RETURN_NOT_OK(dm->Open());
  dd_manager->swap(dm);
  return Status::OK();
}

Status DataDirManager::CreateNewForTests(Env* env, vector<string> data_fs_roots,
                                         const DataDirManagerOptions& opts,
                                         unique_ptr<DataDirManager>* dd_manager) {
  CanonicalizedRootsList roots;
  for (const auto& r : data_fs_roots) {
    roots.push_back({ r, Status::OK() });
  }
  return DataDirManager::CreateNew(env, std::move(roots), opts, dd_manager);
}

Status DataDirManager::CreateNew(Env* env, CanonicalizedRootsList data_fs_roots,
                                 const DataDirManagerOptions& opts,
                                 unique_ptr<DataDirManager>* dd_manager) {
  unique_ptr<DataDirManager> dm;
  dm.reset(new DataDirManager(env, opts, std::move(data_fs_roots)));
  RETURN_NOT_OK(dm->Create());
  RETURN_NOT_OK(dm->Open());
  dd_manager->swap(dm);
  return Status::OK();
}

bool DataDirManager::sync_dirs() const {
  return FLAGS_enable_data_block_fsync;
}

bool DataDirManager::lock_dirs() const {
  return FLAGS_fs_lock_data_dirs;
}

int DataDirManager::max_dirs() const {
  return opts_.dir_type == "file" ? (1 << 16) - 1 : kint32max;
}

Status DataDirManager::PopulateDirectoryMaps(const vector<unique_ptr<Dir>>& dirs) {
  if (opts_.dir_type == "log") {
    return DirManager::PopulateDirectoryMaps(dirs);
  }
  DCHECK_EQ("file", opts_.dir_type);
  // When assigning directories for the file block manager, the UUID indexes
  // must match what exists in the instance files' list of UUIDs.
  unordered_map<string, int> uuid_to_idx;
  for (const auto& dd : dirs) {
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
  if (dirs.size() != uuid_to_idx.size()) {
    return Status::Corruption(
        Substitute("instance file is corrupted: $0 unique UUIDs expected, got $1",
                    dirs.size(), uuid_to_idx.size()));
  }
  // Keep track of any dirs that were not referenced in the dir set. These
  // are presumably from instance files we failed to read. We'll assign them
  // indexes of those that remain.
  vector<Dir*> unassigned_dirs;
  for (const auto& dd : dirs) {
    const auto& uuid = dd->instance()->uuid();
    int* idx = FindOrNull(uuid_to_idx, uuid);
    if (idx) {
      InsertToMaps(uuid, *idx, dd.get());
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
    InsertToMaps(failed_uuid_and_idx.first, failed_uuid_and_idx.second,
                    unassigned_dirs[unassigned_dir_idx++]);
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
    group_target_size = dirs_.size();
  } else {
    group_target_size = std::min(FLAGS_fs_target_data_dirs_per_tablet,
                                 static_cast<int>(dirs_.size()));
  }
  vector<int> group_indices;
  if (mode == DirDistributionMode::ACROSS_ALL_DIRS) {
    // If using all dirs, add all regardless of directory state.
    AppendKeysFromMap(dir_by_uuid_idx_, &group_indices);
  } else {
    // Randomly select directories, giving preference to those with fewer tablets.
    if (PREDICT_FALSE(!failed_dirs_.empty())) {
      group_target_size = std::min(group_target_size,
          static_cast<int>(dirs_.size()) - static_cast<int>(failed_dirs_.size()));

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
                              FLAGS_fs_target_data_dirs_per_tablet, tablet_id, dirs_.size());
      if (metrics_) {
        SubstituteAndAppend(&msg, ", $0 dirs full, $1 dirs failed",
                            metrics_->dirs_full->value(), metrics_->dirs_failed->value());
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

Status DataDirManager::GetDirForBlock(const CreateBlockOptions& opts, Dir** dir,
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
    AppendKeysFromMap(dir_by_uuid_idx_, &all_uuid_indices);
    group_for_tests = DataDirGroup(std::move(all_uuid_indices));
    group = &group_for_tests;
  }
  // Within a given directory group, filter out the ones that are failed.
  if (PREDICT_TRUE(failed_dirs_.empty())) {
    healthy_uuid_indices = group->uuid_indices();
  } else {
    RemoveUnhealthyDataDirsUnlocked(group->uuid_indices(), &healthy_uuid_indices);
    if (healthy_uuid_indices.empty()) {
      return Status::IOError("No healthy directories exist in tablet's "
                             "directory group", opts.tablet_id, ENODEV);
    }
  }
  // Within a given directory group, filter out the ones that are full.
  vector<Dir*> candidate_dirs;
  for (auto uuid_idx : healthy_uuid_indices) {
    Dir* candidate = FindOrDie(dir_by_uuid_idx_, uuid_idx);
    Status s = candidate->RefreshAvailableSpace(Dir::RefreshMode::EXPIRED_ONLY);
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
      if (FindOrDie(dir_by_uuid_idx_, idx)->is_full()) {
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

Status DataDirManager::GetDirAddIfNecessary(const CreateBlockOptions& opts, Dir** dir) {
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
    *dir = FindOrDie(dir_by_uuid_idx_, group.uuid_indices().back());
    return Status::OK();
  }
  vector<int> group_uuid_indices = group.uuid_indices();
  GetDirsForGroupUnlocked(new_target_group_size, &group_uuid_indices);
  if (PREDICT_FALSE(group_uuid_indices.size() < new_target_group_size)) {
    // If we couldn't add to the group, return an error.
    int num_total = dirs_.size();
    int num_failed = failed_dirs_.size();
    int num_full = 0;
    for (const auto& dd : dirs_) {
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
  *dir = FindOrDie(dir_by_uuid_idx_, new_uuid_idx);
  LOG(INFO) << Substitute("Added $0 to $1's directory group: $2",
                          (*dir)->dir(), tablet_id, s.ToString());
  return Status::OK();
}

void DataDirManager::GetDirsForGroupUnlocked(int target_size,
                                             vector<int>* group_indices) {
  DCHECK(dir_group_lock_.is_locked());
  vector<int> candidate_indices;
  unordered_set<int> existing_group_indices(group_indices->begin(), group_indices->end());
  for (auto& e : dir_by_uuid_idx_) {
    int uuid_idx = e.first;
    DCHECK_LT(uuid_idx, dirs_.size());
    if (ContainsKey(existing_group_indices, uuid_idx) ||
        ContainsKey(failed_dirs_, uuid_idx)) {
      continue;
    }
    Dir* dd = e.second;
    Status s = dd->RefreshAvailableSpace(Dir::RefreshMode::ALWAYS);
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
        int64_t space_in_first = FindOrDie(dir_by_uuid_idx_,
                                           candidate_indices[0])->available_bytes();
        int64_t space_in_second = FindOrDie(dir_by_uuid_idx_,
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
    const auto* data_dir = FindDirByUuidIndex(uuid_idx);
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

void DataDirManager::RemoveUnhealthyDataDirsUnlocked(const vector<int>& uuid_indices,
                                                     vector<int>* healthy_indices) const {
  if (PREDICT_TRUE(failed_dirs_.empty())) {
    return;
  }
  healthy_indices->clear();
  for (int uuid_idx : uuid_indices) {
    if (!ContainsKey(failed_dirs_, uuid_idx)) {
      healthy_indices->emplace_back(uuid_idx);
    }
  }
}

} // namespace fs
} // namespace kudu
