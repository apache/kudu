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

#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/callback.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"

namespace kudu {

class DataDirGroupPB;
class Env;
class ThreadPool;

// We pass around the results of canonicalization to indicate to the
// DataDirManager which, if any, failed to canonicalize.
//
// TODO(awong): move the canonicalization of directories into the
// DataDirManager so we can avoid this extra plumbing.
struct CanonicalizedRootAndStatus {
  std::string path;
  Status status;
};
typedef std::vector<CanonicalizedRootAndStatus> CanonicalizedRootsList;

namespace fs {

typedef std::unordered_map<int, std::string> UuidByUuidIndexMap;
typedef std::unordered_map<std::string, int> UuidIndexByUuidMap;

struct CreateBlockOptions;
class DirInstanceMetadataFile;

const char kInstanceMetadataFileName[] = "block_manager_instance";
const char kDataDirName[] = "data";

namespace internal {

// A DataDirGroup is a group of directories used by an entity for block
// placement. A group is represented in memory by a list of indices which index
// into the list of all UUIDs found in a DirSetPB. A group is represented
// on-disk as a list of full UUIDs, and as such, when writing or reading from
// disk, a mapping is needed to translate between index and UUID.
//
// The same directory may appear in multiple DataDirGroups.
class DataDirGroup {
 public:
  DataDirGroup();

  explicit DataDirGroup(std::vector<int> uuid_indices);

  // Reloads the DataDirGroup with UUID indices for the UUIDs in 'pb' by
  // looking them up in 'uuid_idx_by_uuid'.
  //
  // Returns an error if a uuid cannot be found.
  Status LoadFromPB(const UuidIndexByUuidMap& uuid_idx_by_uuid,
                    const DataDirGroupPB& pb);

  // Writes this group's UUIDs to 'pb', looking them up via index in
  // 'uuid_by_uuid_idx'.
  //
  // Returns an error if an index cannot be found.
  Status CopyToPB(const UuidByUuidIndexMap& uuid_by_uuid_idx,
                  DataDirGroupPB* pb) const;

  const std::vector<int>& uuid_indices() const { return uuid_indices_; }

 private:
  // UUID indices corresponding to the data directories within the group.
  std::vector<int> uuid_indices_;
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

// Defines the behavior when opening a directory manager that has an
// inconsistent or incomplete set of instance files.
enum UpdateInstanceBehavior {
  // If the data directories don't match the on-disk dir sets, update the
  // on-disk data to match if not in read-only mode.
  UPDATE_AND_IGNORE_FAILURES,

  // Like UPDATE_AND_IGNORE_FAILURES, but will return an error if any of the updates to the
  // on-disk files fail.
  UPDATE_AND_ERROR_ON_FAILURE,

  // If the data directories don't match the on-disk dir sets, continue without
  // updating the on-disk data.
  DONT_UPDATE
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
          std::unique_ptr<DirInstanceMetadataFile> metadata_file,
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
  Status RefreshAvailableSpace(RefreshMode mode);

  DataDirFsType fs_type() const { return fs_type_; }

  const std::string& dir() const { return dir_; }

  const DirInstanceMetadataFile* instance() const {
    return metadata_file_.get();
  }

  bool is_full() const {
    std::lock_guard<simple_spinlock> l(lock_);
    return is_full_;
  }

  int64_t available_bytes() {
    std::lock_guard<simple_spinlock> l(lock_);
    return available_bytes_;
  }

 private:
  Env* env_;
  DataDirMetrics* metrics_;
  const DataDirFsType fs_type_;
  const std::string dir_;
  const std::unique_ptr<DirInstanceMetadataFile> metadata_file_;
  const std::unique_ptr<ThreadPool> pool_;

  bool is_shutdown_;

  // Protects 'last_space_check_', 'is_full_' and available_bytes_.
  mutable simple_spinlock lock_;
  MonoTime last_space_check_;
  bool is_full_;

  // The available bytes of this dir, updated by RefreshAvailableSpace.
  int64_t available_bytes_;

  DISALLOW_COPY_AND_ASSIGN(DataDir);
};

// Directory manager creation options.
struct DataDirManagerOptions {
  DataDirManagerOptions();

  // The block manager type the directory manager should support.
  // Must be either "file" or "log".
  //
  // Defaults to the value of FLAGS_block_manager.
  std::string block_manager_type;

  // The entity under which all metrics should be grouped. If null, metrics
  // will not be produced.
  //
  // Defaults to null.
  scoped_refptr<MetricEntity> metric_entity;

  // Whether the directory manager should only allow reading.
  //
  // Defaults to false.
  bool read_only;

  // Whether to update the on-disk instances when opening directories if
  // inconsistencies are detected.
  //
  // Defaults to UPDATE_AND_IGNORE_FAILURES.
  UpdateInstanceBehavior update_instances;
};

// Encapsulates knowledge of data directory management on behalf of block
// managers.
class DataDirManager {
 public:
  enum class LockMode {
    MANDATORY,
    OPTIONAL,
    NONE,
  };

  enum class DirDistributionMode {
    ACROSS_ALL_DIRS,
    USE_FLAG_SPEC,
  };

  // Public static initializers for use in tests. When used, data_fs_roots is
  // expected to be the successfully canonicalized directories.
  static Status CreateNewForTests(Env* env, std::vector<std::string> data_fs_roots,
                                  DataDirManagerOptions opts,
                                  std::unique_ptr<DataDirManager>* dd_manager);
  static Status OpenExistingForTests(Env* env, std::vector<std::string> data_fs_roots,
                                     DataDirManagerOptions opts,
                                     std::unique_ptr<DataDirManager>* dd_manager);

  // Constructs a directory manager and creates its necessary files on-disk.
  //
  // Returns an error if any of the directories already exist.
  static Status CreateNew(Env* env, CanonicalizedRootsList data_fs_roots,
                          DataDirManagerOptions opts,
                          std::unique_ptr<DataDirManager>* dd_manager);

  // Constructs a directory manager and indexes the files found on-disk.
  //
  // Returns an error if the number of on-disk directories found exceeds the
  // max allowed, or if locks need to be acquired and cannot be.
  static Status OpenExisting(Env* env, CanonicalizedRootsList data_fs_roots,
                             DataDirManagerOptions opts,
                             std::unique_ptr<DataDirManager>* dd_manager);

  // Returns the root names from the input 'root_list'.
  static std::vector<std::string> GetRootNames(const CanonicalizedRootsList& root_list);

  ~DataDirManager();

  // Shuts down all directories' thread pools.
  void Shutdown();

  // Waits on all directories' thread pools.
  void WaitOnClosures();

  // Returns a list of all data dirs.
  const std::vector<std::unique_ptr<DataDir>>& data_dirs() const {
    return data_dirs_;
  }

  // ==========================================================================
  // Tablet Placement
  // ==========================================================================

  // Deserializes a DataDirGroupPB and associates the resulting DataDirGroup
  // with a tablet_id.
  //
  // Returns an error if the tablet already exists or if a data dir in the
  // group is missing.
  Status LoadDataDirGroupFromPB(const std::string& tablet_id,
                                const DataDirGroupPB& pb);

  // Serializes the DataDirGroupPB associated with the given tablet_id.
  //
  // Returns an error if the tablet was not already registered or if a data dir
  // is missing.
  Status GetDataDirGroupPB(const std::string& tablet_id, DataDirGroupPB* pb) const;

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

  // Returns a dir for block placement in the data dir group specified in
  // 'opts'. If none exists, adds a new dir to the group and returns the dir,
  // and if none can be added, returns an error.
  Status GetDirAddIfNecessary(const CreateBlockOptions& opts, DataDir** dir);

  // Finds the set of tablet_ids in the data dir specified by 'uuid_idx' and
  // returns a copy, returning an empty set if none are found.
  std::set<std::string> FindTabletsByDataDirUuidIdx(int uuid_idx) const;

  // Returns in 'data_dirs' a sorted list of the directory names for the data
  // dirs of the tablet specified by 'tablet_id'.
  Status FindDataDirsByTabletId(const std::string& tablet_id,
                                std::vector<std::string>* data_dirs) const;

  // ==========================================================================
  // Directory Health
  // ==========================================================================

  // Adds 'uuid_idx' to the set of failed data directories. This directory will
  // no longer be used. Logs an error message prefixed with 'error_message'
  // describing what directories are affected.
  //
  // Returns an error if all directories have failed.
  Status MarkDataDirFailed(int uuid_idx, const std::string& error_message = "");

  // Fails the directory specified by 'uuid' and logs a warning if all
  // directories have failed.
  void MarkDataDirFailedByUuid(const std::string& uuid);

  // Returns whether or not the 'uuid_idx' refers to a failed directory.
  bool IsDataDirFailed(int uuid_idx) const;

  // Returns whether the tablet's data is spread across a failed directory.
  bool IsTabletInFailedDir(const std::string& tablet_id) const;

  const std::set<int> GetFailedDataDirs() const {
    shared_lock<rw_spinlock> group_lock(dir_group_lock_.get_lock());
    return failed_data_dirs_;
  }

  // ==========================================================================
  // Directory Paths
  // ==========================================================================

  // Return a list of the canonicalized root directory names.
  std::vector<std::string> GetDataRoots() const;

  // Return a list of the canonicalized data directory names.
  std::vector<std::string> GetDataDirs() const;

  // ==========================================================================
  // Representation Conversion
  // ==========================================================================

  // Finds a data directory by uuid index, returning null if it can't be found.
  //
  // More information on uuid indexes and their relation to data directories
  // can be found next to DirSetPB in fs.proto.
  DataDir* FindDataDirByUuidIndex(int uuid_idx) const;

  // Finds a uuid index by data directory, returning false if it can't be found.
  bool FindUuidIndexByDataDir(DataDir* dir, int* uuid_idx) const;

  // Finds a uuid index by root path, returning false if it can't be found.
  bool FindUuidIndexByRoot(const std::string& root, int* uuid_idx) const;

  // Finds a uuid index by UUID, returning false if it can't be found.
  bool FindUuidIndexByUuid(const std::string& uuid, int* uuid_idx) const;

  // Finds a UUID by canonicalized root name, returning false if it can't be found.
  bool FindUuidByRoot(const std::string& root, std::string* uuid) const;

 private:
  FRIEND_TEST(DataDirsTest, TestCreateGroup);
  FRIEND_TEST(DataDirsTest, TestLoadFromPB);
  FRIEND_TEST(DataDirsTest, TestLoadBalancingBias);
  FRIEND_TEST(DataDirsTest, TestLoadBalancingDistribution);
  FRIEND_TEST(DataDirsTest, TestFailedDirNotAddedToGroup);

  // Constructs a directory manager.
  DataDirManager(Env* env,
                 DataDirManagerOptions opts,
                 CanonicalizedRootsList canonicalized_data_roots);

  // Initializes the data directories on disk. Returns an error if initialized
  // directories already exist.
  //
  // Note: this doesn't initialize any in-memory state for the directory
  // manager.
  Status Create();

  // Opens existing instance files from disk and indexes the files found.
  //
  // Returns an error if the number of on-disk directories found exceeds the
  // max allowed, if locks need to be acquired and cannot be, or if there are
  // no healthy directories.
  //
  // If appropriate, this will create any missing directories and rewrite
  // existing instance files to be consistent with each other.
  Status Open();

  // Populates the maps to index the given directories.
  Status PopulateDirectoryMaps(const std::vector<std::unique_ptr<DataDir>>& dds);

  // Loads the instance files for each directory root.
  //
  // On success, 'instance_files' contains instance objects, including those
  // that failed to load because they were missing or because of a disk
  // error; they are still considered "loaded" and are labeled unhealthy
  // internally. 'has_existing_instances' is set to true if any of the instance
  // files are healthy.
  //
  // Returns an error if an instance file fails in an irreconcileable way (e.g.
  // the file is locked).
  Status LoadInstances(
      std::vector<std::unique_ptr<DirInstanceMetadataFile>>* instance_files,
      bool* has_existing_instances);

  // Takes the set of instance files, does some basic verification on them,
  // creates any that don't exist on disk, and updates any that have a
  // different set of UUIDs stored than the expected set.
  //
  // Returns an error if there is a configuration error, e.g. if the existing
  // instances believe there should be a different block size.
  //
  // If in UPDATE_AND_IGNORE_FAILURES mode, an error is not returned in the event of a disk
  // error. Instead, it is up to the caller to reload the instance files and
  // proceed if healthy enough.
  //
  // If in UPDATE_AND_ERROR_ON_FAILURE mode, a failure to update instances will
  // surface as an error.
  Status CreateNewDirectoriesAndUpdateInstances(
      std::vector<std::unique_ptr<DirInstanceMetadataFile>> instances);

  // Updates the on-disk instance files specified by 'instances_to_update'
  // (presumably those whose 'all_uuids' field doesn't match 'new_all_uuids')
  // using the contents of 'new_all_uuids', skipping any unhealthy instance
  // files.
  //
  // If in UPDATE_AND_IGNORE_FAILURES mode, this is best effort. If any of the instance
  // updates fail (e.g. due to a disk error) in this mode, this will log a
  // warning about the failed updates and return OK.
  //
  // If in UPDATE_AND_ERROR_ON_FAILURE mode, any failure will immediately attempt
  // to clean up any altered state and return with an error.
  Status UpdateHealthyInstances(
      const std::vector<std::unique_ptr<DirInstanceMetadataFile>>& instances_to_update,
      const std::set<std::string>& new_all_uuids);

  // Returns a random directory in the data dir group specified in 'opts',
  // giving preference to those with more free space. If there is no room in
  // the group, returns an IOError with the ENOSPC posix code and returns the
  // new target size for the data dir group.
  Status GetDirForBlock(const CreateBlockOptions& opts, DataDir** dir,
                        int* new_target_group_size) const;

  // Repeatedly selects directories from those available to put into a new
  // DataDirGroup until 'group_indices' reaches 'target_size' elements.
  //
  // Selection is based on "The Power of Two Choices in Randomized Load
  // Balancing", selecting two directories randomly and choosing the one with
  // less load, quantified as the number of unique tablets in the directory.
  // Ties are broken by choosing the directory with more free space. The
  // resulting behavior fills directories that have fewer tablets stored on
  // them while not completely neglecting those with more tablets.
  //
  // 'group_indices' is an in/out parameter that stores the list of UUID
  // indices to be added; UUID indices that are already in 'group_indices' are
  // not considered. Although this function does not itself change
  // DataDirManager state, its expected usage warrants that it is called within
  // the scope of a lock_guard of dir_group_lock_.
  void GetDirsForGroupUnlocked(int target_size, std::vector<int>* group_indices);

  // Goes through the data dirs in 'uuid_indices' and populates
  // 'healthy_indices' with those that haven't failed.
  void RemoveUnhealthyDataDirsUnlocked(const std::vector<int>& uuid_indices,
                                       std::vector<int>* healthy_indices) const;

  // The environment to be used for all data directory operations.
  Env* env_;

  // The options that the DataDirManager was created with.
  const DataDirManagerOptions opts_;

  // The canonicalized roots provided to the constructor, taken verbatim.
  //
  // - The first data root is used as the metadata root.
  // - Common roots in the collections have been deduplicated.
  const CanonicalizedRootsList canonicalized_data_fs_roots_;

  std::unique_ptr<DataDirMetrics> metrics_;

  std::vector<std::unique_ptr<DataDir>> data_dirs_;

  typedef std::unordered_map<std::string, std::string> UuidByRootMap;
  UuidByRootMap uuid_by_root_;

  typedef std::unordered_map<int, DataDir*> UuidIndexMap;
  UuidIndexMap data_dir_by_uuid_idx_;

  typedef std::unordered_map<DataDir*, int> ReverseUuidIndexMap;
  ReverseUuidIndexMap uuid_idx_by_data_dir_;

  typedef std::unordered_map<std::string, internal::DataDirGroup> TabletDataDirGroupMap;
  TabletDataDirGroupMap group_by_tablet_map_;

  typedef std::unordered_map<int, std::set<std::string>> TabletsByUuidIndexMap;
  TabletsByUuidIndexMap tablets_by_uuid_idx_map_;

  UuidByUuidIndexMap uuid_by_idx_;
  UuidIndexByUuidMap idx_by_uuid_;

  typedef std::set<int> FailedDataDirSet;
  FailedDataDirSet failed_data_dirs_;

  // Lock protecting access to the dir group maps and to failed_data_dirs_.
  // A percpu_rwlock is used so threads attempting to read (e.g. to get the
  // next data directory for a Flush()) do not block each other, while threads
  // attempting to write (e.g. to create a new tablet, thereby creating a new
  // data directory group) block all threads.
  mutable percpu_rwlock dir_group_lock_;

  // RNG used to select directories.
  mutable ThreadSafeRandom rng_;

  DISALLOW_COPY_AND_ASSIGN(DataDirManager);
};

} // namespace fs
} // namespace kudu
