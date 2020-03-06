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
#include <iosfwd>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/fs/dir_manager.h"
#include "kudu/fs/error_manager.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

DECLARE_bool(enable_data_block_fsync);

namespace kudu {

class BlockId;
class FileCache;
class InstanceMetadataPB;
class MemTracker;

namespace fs {

class BlockManager;
class DataDirManager;
class FsManagerTestBase_TestDuplicatePaths_Test;
class FsManagerTestBase_TestEIOWhileRunningUpdateDirsTool_Test;
class FsManagerTestBase_TestIsolatedMetadataDir_Test;
class FsManagerTestBase_TestMetadataDirInDataRoot_Test;
class FsManagerTestBase_TestMetadataDirInWALRoot_Test;
class FsManagerTestBase_TestOpenWithDuplicateInstanceFiles_Test;
class ReadableBlock;
class WritableBlock;
struct CreateBlockOptions;
struct FsReport;

} // namespace fs

namespace itest {
class MiniClusterFsInspector;
} // namespace itest

namespace tserver {
class MiniTabletServerTest_TestFsLayoutEndToEnd_Test;
} // namespace tserver

// Options that control the behavior of FsManager.
struct FsManagerOpts {
  // Creates a new FsManagerOpts with default values.
  FsManagerOpts();

  // Creates a new FsManagerOpts with default values except 'wal_root' and
  // 'data_roots', which are both initialized to 'root'.
  //
  // Should only be used in unit tests.
  explicit FsManagerOpts(const std::string& root);

  // The entity under which all metrics should be grouped. If null, metrics
  // will not be produced.
  //
  // Defaults to null.
  scoped_refptr<MetricEntity> metric_entity;

  // The memory tracker under which all new memory trackers will be parented.
  // If null, new memory trackers will be parented to the root tracker.
  //
  // Defaults to null.
  std::shared_ptr<MemTracker> parent_mem_tracker;

  // The directory root where WALs will be stored. Cannot be empty.
  std::string wal_root;

  // The directory root where data blocks will be stored. If empty, Kudu will
  // use the WAL root.
  std::vector<std::string> data_roots;

  // The directory root where metadata will be stored. If empty, Kudu will use
  // the WAL root, or the first configured data root if metadata already exists
  // in it from a previous deployment (the only option in Kudu 1.6 and below
  // was to use the first data root).
  std::string metadata_root;

  // The block manager type. Must be either "file" or "log".
  //
  // Defaults to the value of FLAGS_block_manager.
  std::string block_manager_type;

  // Whether or not read-write operations should be allowed.
  //
  // Defaults to false.
  bool read_only;

  // Whether to update the on-disk instances when opening directories if
  // inconsistencies are detected.
  //
  // Defaults to UPDATE_AND_IGNORE_FAILURES.
  fs::UpdateInstanceBehavior update_instances;

  // The file cache to be used for long-lived opened files (e.g. in the block
  // manager). If null, opened files will not be cached.
  //
  // Defaults to null.
  FileCache* file_cache;

  // Whether or not to skip opening the block manager. FsManager operations that
  // require the block manager will crash.
  //
  // Default to false.
  bool skip_block_manager;
};

// FsManager provides helpers to read data and metadata files,
// and it's responsible for abstracting the file-system layout.
//
// The user should not be aware of where files are placed,
// but instead should interact with the storage in terms of "open the block xyz"
// or "write a new schema metadata file for table kwz".
//
// The current layout is:
//    <kudu.root.dir>/data/
//    <kudu.root.dir>/data/<prefix-0>/<prefix-2>/<prefix-4>/<name>
class FsManager {
 public:
  static const char *kWalFileNamePrefix;
  static const char *kWalsRecoveryDirSuffix;

  FsManager(Env* env, FsManagerOpts opts);
  ~FsManager();

  // ==========================================================================
  //  Initialization
  // ==========================================================================

  // Initializes and loads the instance metadata files, and verifies that they
  // are all matching, returning any root paths that do not have metadata
  // files. Sets 'metadata_' on success, and returns NotFound if none of the
  // metadata files could be read. This must be called before calling uuid().
  //
  // This only partially initialize the FsManager to expose the file
  // system's UUID. To do anything more than that, call Open() or
  // CreateInitialFileSystemLayout().
  Status PartialOpen(CanonicalizedRootsList* missing_roots = nullptr);

  // Initializes and loads the basic filesystem metadata, checking it for
  // inconsistencies. If found, and if the FsManager was not constructed in
  // read-only mode, an attempt will be made to repair them.
  //
  // If 'report' is not null, it will be populated with the results of the
  // check (and repair, if applicable); otherwise, the results of the check
  // will be logged and the presence of fatal inconsistencies will manifest as
  // a returned error.
  //
  // If the filesystem has not been initialized, returns NotFound. In that
  // case, CreateInitialFileSystemLayout() may be used to initialize the
  // on-disk and in-memory structures.
  Status Open(fs::FsReport* report = nullptr);

  // Create the initial filesystem layout. If 'uuid' is provided, uses it as
  // uuid of the filesystem. Otherwise generates one at random.
  //
  // Returns an error if the file system is already initialized.
  Status CreateInitialFileSystemLayout(
      boost::optional<std::string> uuid = boost::none);

  // ==========================================================================
  //  Error handling helpers
  // ==========================================================================

  // Registers an error-handling callback with the FsErrorManager.
  //
  // If a disk failure is detected, this callback will be invoked with the
  // relevant DataDir's UUID as its input parameter.
  void SetErrorNotificationCb(fs::ErrorHandlerType e, fs::ErrorNotificationCb cb);

  // Unregisters the error-handling callback with the FsErrorManager.
  //
  // This must be called before the callback's callee is destroyed. Calls to
  // this are idempotent and are safe even if a callback has not been set.
  void UnsetErrorNotificationCb(fs::ErrorHandlerType e);

  // ==========================================================================
  //  Data read/write interfaces
  // ==========================================================================

  // Creates a new block based on the options specified in 'opts'.
  //
  // Block will be synced on close.
  Status CreateNewBlock(const fs::CreateBlockOptions& opts,
                        std::unique_ptr<fs::WritableBlock>* block);

  Status OpenBlock(const BlockId& block_id,
                   std::unique_ptr<fs::ReadableBlock>* block);

  Status DeleteBlock(const BlockId& block_id);

  bool BlockExists(const BlockId& block_id) const;

  // ==========================================================================
  //  on-disk path
  // ==========================================================================
  std::vector<std::string> GetDataRootDirs() const;

  std::string GetWalsRootDir() const {
    DCHECK(initted_);
    return JoinPathSegments(canonicalized_wal_fs_root_.path, kWalDirName);
  }

  std::string GetTabletWalDir(const std::string& tablet_id) const {
    return JoinPathSegments(GetWalsRootDir(), tablet_id);
  }

  std::string GetTabletWalRecoveryDir(const std::string& tablet_id) const;

  std::string GetWalSegmentFileName(const std::string& tablet_id,
                                    uint64_t sequence_number) const;

  // Return the directory where tablet superblocks should be stored.
  std::string GetTabletMetadataDir() const;

  // Return the path for a specific tablet's superblock.
  std::string GetTabletMetadataPath(const std::string& tablet_id) const;

  // List the tablet IDs in the metadata directory.
  Status ListTabletIds(std::vector<std::string>* tablet_ids);

  // Return the path where InstanceMetadataPB is stored.
  std::string GetInstanceMetadataPath(const std::string& root) const;

  // Return the directory where the consensus metadata is stored.
  std::string GetConsensusMetadataDir() const {
    DCHECK(initted_);
    return JoinPathSegments(canonicalized_metadata_fs_root_.path, kConsensusMetadataDirName);
  }

  // Return the path where ConsensusMetadataPB is stored.
  std::string GetConsensusMetadataPath(const std::string& tablet_id) const {
    return JoinPathSegments(GetConsensusMetadataDir(), tablet_id);
  }

  Env* env() { return env_; }

  bool read_only() const {
    return opts_.read_only;
  }

  // Return the UUID persisted in the local filesystem. If PartialOpen() or
  // Open() have not been called, this will crash.
  const std::string& uuid() const;

  // ==========================================================================
  //  file-system helpers
  // ==========================================================================
  bool Exists(const std::string& path) const {
    return env_->FileExists(path);
  }

  Status ListDir(const std::string& path, std::vector<std::string> *objects) const {
    return env_->GetChildren(path, objects);
  }

  fs::DataDirManager* dd_manager() const {
    return dd_manager_.get();
  }

  fs::BlockManager* block_manager() {
    DCHECK(block_manager_);
    return block_manager_.get();
  }

  // Prints the file system trees under the file system roots.
  void DumpFileSystemTree(std::ostream& out);

 private:
  FRIEND_TEST(fs::FsManagerTestBase, TestDuplicatePaths);
  FRIEND_TEST(fs::FsManagerTestBase, TestEIOWhileRunningUpdateDirsTool);
  FRIEND_TEST(fs::FsManagerTestBase, TestIsolatedMetadataDir);
  FRIEND_TEST(fs::FsManagerTestBase, TestMetadataDirInWALRoot);
  FRIEND_TEST(fs::FsManagerTestBase, TestMetadataDirInDataRoot);
  FRIEND_TEST(fs::FsManagerTestBase, TestOpenWithDuplicateInstanceFiles);
  FRIEND_TEST(tserver::MiniTabletServerTest, TestFsLayoutEndToEnd);
  friend class itest::MiniClusterFsInspector; // for access to directory names

  // Initializes, sanitizes, and canonicalizes the filesystem roots.
  // Determines the correct filesystem root for tablet-specific metadata.
  Status Init();

  // Select and create an instance of the appropriate block manager.
  //
  // Does not actually perform any on-disk operations.
  void InitBlockManager();

  // Creates filesystem roots from 'canonicalized_roots', writing new on-disk
  // instances using 'metadata'.
  //
  //
  // All created directories and files will be appended to 'created_dirs' and
  // 'created_files' respectively. It is the responsibility of the caller to
  // synchronize the directories containing these newly created file objects.
  Status CreateFileSystemRoots(CanonicalizedRootsList canonicalized_roots,
                               const InstanceMetadataPB& metadata,
                               std::vector<std::string>* created_dirs,
                               std::vector<std::string>* created_files);

  // Create a new InstanceMetadataPB.
  Status CreateInstanceMetadata(boost::optional<std::string> uuid,
                                InstanceMetadataPB* metadata);

  // Save a InstanceMetadataPB to the filesystem.
  // Does not mutate the current state of the fsmanager.
  Status WriteInstanceMetadata(const InstanceMetadataPB& metadata,
                               const std::string& root);

  // ==========================================================================
  //  file-system helpers
  // ==========================================================================

  // Prints the file system tree for the objects in 'objects' under the given
  // 'path'. Prints lines with the given 'prefix'.
  void DumpFileSystemTree(std::ostream& out,
                          const std::string& prefix,
                          const std::string& path,
                          const std::vector<std::string>& objects);

  // Deletes leftover temporary files in all "special" top-level directories
  // (e.g. WAL root directory).
  //
  // Logs warnings in case of errors.
  void CleanTmpFiles();

  // Checks that the permissions of the root data directories conform to the
  // configured umask, and tightens them as necessary if they do not.
  void CheckAndFixPermissions();

  // Returns true if 'fname' is a valid tablet ID.
  bool IsValidTabletId(const std::string& fname);

  static const char *kDataDirName;
  static const char *kTabletMetadataDirName;
  static const char *kWalDirName;
  static const char *kInstanceMetadataFileName;
  static const char *kInstanceMetadataMagicNumber;
  static const char *kTabletSuperBlockMagicNumber;
  static const char *kConsensusMetadataDirName;

  // The environment to be used for all filesystem operations.
  Env* env_;

  // The options that the FsManager was created with.
  const FsManagerOpts opts_;

  // Canonicalized forms of the root directories. Constructed during Init()
  // with ordering maintained.
  //
  // - The first data root is used as the metadata root.
  // - Common roots in the collections have been deduplicated.
  CanonicalizedRootAndStatus canonicalized_wal_fs_root_;
  CanonicalizedRootAndStatus canonicalized_metadata_fs_root_;
  CanonicalizedRootsList canonicalized_data_fs_roots_;
  CanonicalizedRootsList canonicalized_all_fs_roots_;

  std::unique_ptr<InstanceMetadataPB> metadata_;

  std::unique_ptr<fs::FsErrorManager> error_manager_;
  std::unique_ptr<fs::DataDirManager> dd_manager_;
  std::unique_ptr<fs::BlockManager> block_manager_;

  ObjectIdGenerator oid_generator_;

  bool initted_;

  DISALLOW_COPY_AND_ASSIGN(FsManager);
};

} // namespace kudu

