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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <set>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"

namespace kudu {
class Env;
class ThreadPool;

namespace fs {

typedef std::unordered_map<uint16_t, std::string> UuidByUuidIndexMap;
typedef std::unordered_map<std::string, uint16_t> UuidIndexByUuidMap;

class PathInstanceMetadataFile;
struct CreateBlockOptions;

const char kInstanceMetadataFileName[] = "block_manager_instance";

namespace internal {

// A DataDirGroup is a group of directories used by an entity for block
// placement. A group is represented in memory by a list of 2-byte indices,
// which index into the list of all UUIDs found in a PathSetPB. A group is
// represented on-disk as a list of full UUIDs, and as such, when writing or
// reading from disk, a mapping is needed to translate between index and UUID.
//
// The same directory may appear in multiple DataDirGroups.
class DataDirGroup {
 public:
  explicit DataDirGroup(std::vector<uint16_t> uuid_indices)
      : uuid_indices_(std::move(uuid_indices)) {}

  static DataDirGroup FromPB(const DataDirGroupPB& pb,
                             const UuidIndexByUuidMap& uuid_idx_by_uuid) {
    std::vector<uint16_t> uuid_indices;
    for (const std::string& uuid : pb.uuids()) {
      uuid_indices.push_back(FindOrDie(uuid_idx_by_uuid, uuid));
    }
    return DataDirGroup(std::move(uuid_indices));
  }

  void CopyToPB(const UuidByUuidIndexMap& uuid_by_uuid_idx,
                DataDirGroupPB* pb) const {
    DCHECK(pb);
    DataDirGroupPB group;
    for (uint16_t uuid_idx : uuid_indices_) {
      group.add_uuids(FindOrDie(uuid_by_uuid_idx, uuid_idx));
    }
    pb->Swap(&group);
  }

  const std::vector<uint16_t>& uuid_indices() const { return uuid_indices_; }

 private:
  // UUID indices corresponding to the data directories within the group.
  const std::vector<uint16_t> uuid_indices_;
};

}  // namespace internal

// Detected type of filesystem.
enum class DataDirFsType {
  // ext2, ext3, or ext4.
  EXT,

  // SGI xfs.
  XFS,

  // None of the above.
  OTHER
};

struct DataDirMetrics {
  explicit DataDirMetrics(const scoped_refptr<MetricEntity>& entity);

  scoped_refptr<AtomicGauge<uint64_t>> data_dirs_failed;
  scoped_refptr<AtomicGauge<uint64_t>> data_dirs_full;
};

// Representation of a data directory in use by the block manager.
class DataDir {
 public:
  DataDir(Env* env,
          DataDirMetrics* metrics,
          DataDirFsType fs_type,
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

  DataDirFsType fs_type() const { return fs_type_; }

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
  const DataDirFsType fs_type_;
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

  enum class DirDistributionMode {
    ACROSS_ALL_DIRS,
    USE_FLAG_SPEC,
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

  // Deserializes a DataDirGroupPB and associates the resulting DataDirGroup
  // with a tablet_id.
  //
  // Results in an error if the tablet already exists.
  Status LoadDataDirGroupFromPB(const std::string& tablet_id,
                                const DataDirGroupPB& pb);

  // Creates a new data dir group for the specified tablet. Adds data
  // directories to this new group until the limit specified by
  // fs_target_data_dirs_per_tablet, or until there is no more space.
  //
  // If 'mode' is ACROSS_ALL_DIRS, ignores the above flag and stripes across
  // all disks. This behavior is only used when loading a superblock with no
  // DataDirGroup, allowing for backwards compatability with data from older
  // version of Kudu.
  //
  // Results in an error if all disks are full or if the tablet already has a
  // data dir group associated with it. If returning with an error, the
  // DataDirManager will be unchanged.
  Status CreateDataDirGroup(const std::string& tablet_id,
                            DirDistributionMode mode = DirDistributionMode::USE_FLAG_SPEC);

  // Deletes the group for the specified tablet. Maps from tablet_id to group
  // and data dir to tablet set are cleared of all references to the tablet.
  void DeleteDataDirGroup(const std::string& tablet_id);

  // Serializes the DataDirGroupPB associated with the given tablet_id. Returns
  // false if none exist.
  bool GetDataDirGroupPB(const std::string& tablet_id, DataDirGroupPB* pb) const;

  // Returns a random directory from the specfied option's data dir group. If
  // there is no room in the group, returns an error.
  Status GetNextDataDir(const CreateBlockOptions& opts, DataDir** dir);

  // Finds a data directory by uuid index, returning nullptr if it can't be
  // found.
  //
  // More information on uuid indexes and their relation to data directories
  // can be found next to PathSetPB in fs.proto.
  DataDir* FindDataDirByUuidIndex(uint16_t uuid_idx) const;

  // Finds a uuid index by data directory, returning false if it can't be found.
  bool FindUuidIndexByDataDir(DataDir* dir,
                              uint16_t* uuid_idx) const;

  // Finds a uuid index by UUID, returning false if it can't be found.
  bool FindUuidIndexByUuid(const std::string& uuid, uint16_t* uuid_idx) const;

  // Returns a list of all data dirs.
  const std::vector<std::unique_ptr<DataDir>>& data_dirs() const {
    return data_dirs_;
  }

  // Finds the set of tablet_ids in the data dir specified by 'uuid_idx' and
  // returns a copy, returning an empty set if none are found.
  std::set<std::string> FindTabletsByDataDirUuidIdx(uint16_t uuid_idx);

  // Adds 'uuid_idx' to the set of failed data directories. This directory will
  // no longer be used. Logs an error message prefixed with 'error_message'
  // describing what directories are affected.
  void MarkDataDirFailed(uint16_t uuid_idx, const std::string& error_message = "");

  // Returns whether or not the 'uuid_idx' refers to a failed directory.
  bool IsDataDirFailed(uint16_t uuid_idx) const;

  const std::set<uint16_t> GetFailedDataDirs() const {
    shared_lock<rw_spinlock> group_lock(dir_group_lock_.get_lock());
    return failed_data_dirs_;
  }

 private:
  FRIEND_TEST(DataDirGroupTest, TestCreateGroup);
  FRIEND_TEST(DataDirGroupTest, TestLoadFromPB);
  FRIEND_TEST(DataDirGroupTest, TestLoadBalancingBias);
  FRIEND_TEST(DataDirGroupTest, TestLoadBalancingDistribution);
  FRIEND_TEST(DataDirGroupTest, TestFailedDirNotAddedToGroup);

  // Repeatedly selects directories from those available to put into a new
  // DataDirGroup until 'group_indices' reaches 'target_size' elements.
  // Selection is based on "The Power of Two Choices in Randomized Load
  // Balancing", selecting two directories randomly and choosing the one with
  // less load, quantified as the number of unique tablets in the directory.
  // The resulting behavior fills directories that have fewer tablets stored on
  // them while not completely neglecting those with more tablets.
  //
  // 'group_indices' is an output that stores the list of uuid_indices to be
  // added. Although this function does not itself change DataDirManager state,
  // its expected usage warrants that it is called within the scope of a
  // lock_guard of dir_group_lock_.
  Status GetDirsForGroupUnlocked(int target_size, std::vector<uint16_t>* group_indices);

  // Goes through the data dirs in 'uuid_indices' and populates
  // 'healthy_indices' with those that haven't failed.
  void RemoveUnhealthyDataDirsUnlocked(const std::vector<uint16_t>& uuid_indices,
                                       std::vector<uint16_t>* healthy_indices) const;

  Env* env_;
  const std::string block_manager_type_;
  const std::vector<std::string> paths_;

  std::unique_ptr<DataDirMetrics> metrics_;

  std::vector<std::unique_ptr<DataDir>> data_dirs_;

  typedef std::unordered_map<uint16_t, DataDir*> UuidIndexMap;
  UuidIndexMap data_dir_by_uuid_idx_;

  typedef std::unordered_map<DataDir*, uint16_t> ReverseUuidIndexMap;
  ReverseUuidIndexMap uuid_idx_by_data_dir_;

  typedef std::unordered_map<std::string, internal::DataDirGroup> TabletDataDirGroupMap;
  TabletDataDirGroupMap group_by_tablet_map_;

  typedef std::unordered_map<uint16_t, std::set<std::string>> TabletsByUuidIndexMap;
  TabletsByUuidIndexMap tablets_by_uuid_idx_map_;

  UuidByUuidIndexMap uuid_by_idx_;
  UuidIndexByUuidMap idx_by_uuid_;

  typedef std::set<uint16_t> FailedDataDirSet;
  FailedDataDirSet failed_data_dirs_;

  // Lock protecting access to the dir group maps and to failed_data_dirs_.
  // A percpu_rwlock is used so threads attempting to read (e.g. to get the
  // next data directory for a Flush()) do not block each other, while threads
  // attempting to write (e.g. to create a new tablet, thereby creating a new
  // data directory group) block all threads.
  mutable percpu_rwlock dir_group_lock_;

  // RNG used to select directories.
  ThreadSafeRandom rng_;

  DISALLOW_COPY_AND_ASSIGN(DataDirManager);
};

} // namespace fs
} // namespace kudu
