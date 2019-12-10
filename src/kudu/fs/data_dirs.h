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
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/fs/dir_manager.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"

namespace kudu {

class DataDirGroupPB;
class Env;
class MetricEntity;
class ThreadPool;

namespace fs {

class DirInstanceMetadataFile;
struct CreateBlockOptions;

const char kInstanceMetadataFileName[] = "block_manager_instance";
const char kDataDirName[] = "data";

struct DataDirMetrics : public DirMetrics {
  explicit DataDirMetrics(const scoped_refptr<MetricEntity>& metric_entity);
};

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

// Instantiation of a directory that uses the appropriate gflags.
class DataDir : public Dir {
 public:
  DataDir(Env* env,
          DirMetrics* metrics,
          FsType fs_type,
          std::string dir,
          std::unique_ptr<DirInstanceMetadataFile> metadata_file,
          std::unique_ptr<ThreadPool> pool);

  int available_space_cache_secs() const override;
  int reserved_bytes() const override;
};

struct DataDirManagerOptions : public DirManagerOptions {
  DataDirManagerOptions();
};

// Encapsulates knowledge of data directory management on behalf of block
// managers.
class DataDirManager : public DirManager {
 public:
  enum class DirDistributionMode {
    ACROSS_ALL_DIRS,
    USE_FLAG_SPEC,
  };

  // Public static initializers for use in tests. When used, data_fs_roots is
  // expected to be the successfully canonicalized directories.
  static Status CreateNewForTests(Env* env,
                                  std::vector<std::string> data_fs_roots,
                                  const DataDirManagerOptions& opts,
                                  std::unique_ptr<DataDirManager>* dd_manager);
  static Status OpenExistingForTests(Env* env,
                                     std::vector<std::string> data_fs_roots,
                                     const DataDirManagerOptions& opts,
                                     std::unique_ptr<DataDirManager>* dd_manager);

  // Constructs a directory manager and creates its necessary files on-disk.
  //
  // Returns an error if any of the directories already exist.
  static Status CreateNew(Env* env, CanonicalizedRootsList data_fs_roots,
                          const DataDirManagerOptions& opts,
                          std::unique_ptr<DataDirManager>* dd_manager);

  // Constructs a directory manager and indexes the files found on-disk.
  //
  // Returns an error if the number of on-disk directories found exceeds the
  // max allowed, or if locks need to be acquired and cannot be.
  static Status OpenExisting(Env* env, CanonicalizedRootsList data_fs_roots,
                             const DataDirManagerOptions& opts,
                             std::unique_ptr<DataDirManager>* dd_manager);

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
  Status GetDirAddIfNecessary(const CreateBlockOptions& opts, Dir** dir);

  // Returns in 'data_dirs' a sorted list of the directory names for the data
  // dirs of the tablet specified by 'tablet_id'.
  Status FindDataDirsByTabletId(const std::string& tablet_id,
                                std::vector<std::string>* data_dirs) const;

  // Create a new data directory.
  std::unique_ptr<Dir> CreateNewDir(Env* env,
                                    DirMetrics* metrics,
                                    FsType fs_type,
                                    std::string dir,
                                    std::unique_ptr<DirInstanceMetadataFile> metadata_file,
                                    std::unique_ptr<ThreadPool> pool) override;

 private:
  FRIEND_TEST(DataDirsTest, TestCreateGroup);
  FRIEND_TEST(DataDirsTest, TestLoadFromPB);
  FRIEND_TEST(DataDirsTest, TestLoadBalancingBias);
  FRIEND_TEST(DataDirsTest, TestLoadBalancingDistribution);
  FRIEND_TEST(DataDirsTest, TestFailedDirNotAddedToGroup);

  // Populates the maps to index the given directories.
  Status PopulateDirectoryMaps(const std::vector<std::unique_ptr<Dir>>& dirs) override;

  const char* dir_name() const override {
    return kDataDirName;
  }

  const char* instance_metadata_filename() const override {
    return kInstanceMetadataFileName;
  }

  bool sync_dirs() const override;
  bool lock_dirs() const override;
  int max_dirs() const override;

  // Constructs a directory manager.
  DataDirManager(Env* env,
                 const DataDirManagerOptions& opts,
                 CanonicalizedRootsList canonicalized_data_roots);

  // Returns a random directory in the data dir group specified in 'opts',
  // giving preference to those with more free space. If there is no room in
  // the group, returns an IOError with the ENOSPC posix code and returns the
  // new target size for the data dir group.
  Status GetDirForBlock(const CreateBlockOptions& opts, Dir** dir,
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

  typedef std::unordered_map<std::string, internal::DataDirGroup> TabletDataDirGroupMap;
  TabletDataDirGroupMap group_by_tablet_map_;

  DISALLOW_COPY_AND_ASSIGN(DataDirManager);
};

} // namespace fs
} // namespace kudu
