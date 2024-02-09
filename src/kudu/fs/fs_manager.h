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

#include <atomic>
#include <cstdint>
#include <iosfwd>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/dir_manager.h"
#include "kudu/fs/error_manager.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/env.h"
#include "kudu/util/locks.h"          // for percpu_rwlock
#include "kudu/util/metrics.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

namespace kudu {
namespace security {
class KeyProvider;
}  // namespace security
}  // namespace kudu

DECLARE_bool(enable_data_block_fsync);

namespace kudu {

class BlockId;
class FileCache;
class InstanceMetadataPB;
class InstanceMetadataPB_TenantMetadataPB;
class MemTracker;
class Timer;

namespace fs {

class FsManagerTestBase_TestDuplicatePaths_Test;
class FsManagerTestBase_TestEIOWhileRunningUpdateDirsTool_Test;
class FsManagerTestBase_TestIsolatedMetadataDir_Test;
class FsManagerTestBase_TestMetadataDirInDataRoot_Test;
class FsManagerTestBase_TestMetadataDirInWALRoot_Test;
class FsManagerTestBase_TestOpenWithDuplicateInstanceFiles_Test;
class FsManagerTestBase_TestTenantAccountOperation_Test;
struct FsReport;

} // namespace fs

namespace itest {
class MiniClusterFsInspector;
} // namespace itest

namespace tserver {
class MiniTabletServerTest_TestFsLayoutEndToEnd_Test;
} // namespace tserver

namespace tools {
Status UpdateEncryptionKeyInfo(Env* env);
} // namespace tools

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

  explicit FsManager(Env* env, FsManagerOpts opts = {});
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
  //
  // If 'read_instance_metadata_files' and 'read_data_directories' are not nullptr,
  // they will be populated with time spent reading the instance metadata files
  // and time spent reading data directories respectively.
  //
  // If 'containers_processed' and 'containers_total' are not nullptr, they will
  // be populated with total containers attempted to be opened/processed and
  // total containers present respectively in the subsequent calls made to
  // the block manager.
  Status Open(fs::FsReport* report = nullptr,
              Timer* read_instance_metadata_files = nullptr,
              Timer* read_data_directories = nullptr,
              std::atomic<int>* containers_processed = nullptr,
              std::atomic<int>* containers_total = nullptr );

  // Create the initial filesystem layout. If 'uuid' is provided, uses it as
  // uuid of the filesystem. Otherwise generates one at random. If 'tenant_name',
  // 'tenant_id', 'encryption_key', 'encryption_key_iv', and 'encryption_key_version' are
  // provided, they are used as the tenant info of the filesystem. If 'encryption_key',
  // 'encryption_key_iv', and 'encryption_key_version' are provided, they are used as the
  // server key info of the filesystem. Otherwise, if only '--encrypt_data_at_rest'
  // is enabled, generates one server key at random. If both '--enable_multi_tenancy'
  // and '--encrypt_data_at_rest' set at same time, generates one default tenant at random.
  //
  // Returns an error if the file system is already initialized.
  Status CreateInitialFileSystemLayout(
      std::optional<std::string> uuid = std::nullopt,
      std::optional<std::string> tenant_name = std::nullopt,
      std::optional<std::string> tenant_id = std::nullopt,
      std::optional<std::string> encryption_key = std::nullopt,
      std::optional<std::string> encryption_key_iv = std::nullopt,
      std::optional<std::string> encryption_key_version = std::nullopt);

  // ==========================================================================
  //  Error handling helpers
  // ==========================================================================

  // Registers an error-handling callback with the FsErrorManager.
  //
  // If a disk failure is detected, this callback will be invoked with the
  // relevant Dir's UUID as its input parameter.
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
  // If the tenant id is not specified, we treat it as the default tenant.
  //
  // Block will be synced on close.
  Status CreateNewBlock(const fs::CreateBlockOptions& opts,
                        std::unique_ptr<fs::WritableBlock>* block,
                        const std::string& tenant_id = fs::kDefaultTenantID);

  // If the tenant id is not specified, we treat it as the default tenant.
  Status OpenBlock(const BlockId& block_id,
                   std::unique_ptr<fs::ReadableBlock>* block,
                   const std::string& tenant_id = fs::kDefaultTenantID);

  // If the tenant id is not specified, we treat it as the default tenant.
  bool BlockExists(const BlockId& block_id,
                   const std::string& tenant_id = fs::kDefaultTenantID);

  // ==========================================================================
  //  on-disk path
  // ==========================================================================
  // Get the data subdirectories for each data root, which belong to the tenant
  // specified by the tenant id.
  // If the tenant id is not specified, we treat it as the default tenant.
  std::vector<std::string> GetDataRootDirs(
      const std::string& tenant_id = fs::kDefaultTenantID) const;

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

  // Get env to do read/write.
  // Different tenant owns different env.
  // Return nullptr if search fail when '--enable_multi_tenancy' enabled.
  //
  // If the tenant id is not specified, we treat it as the default tenant.
  Env* GetEnv(const std::string& tenant_id = fs::kDefaultTenantID) const;

  bool read_only() const {
    return opts_.read_only;
  }

  // Return the UUID persisted in the local filesystem. If PartialOpen() or
  // Open() have not been called, this will crash.
  const std::string& uuid() const;

  // Copy the metadata_ to metadata.
  void CopyMetadata(
    std::unique_ptr<InstanceMetadataPB>* metadata);

  // ==========================================================================
  //  tenant helpers
  // ==========================================================================

  // A new tenant must have a tenant name and tenant ID. If the tenant key information
  // is missing and the key generation service is available, we can use the relevant
  // key generation service to generate the key information for it.
  //
  // The validation of tenant name and ID needs to be conducted on the master side.
  Status AddTenant(const std::string& tenant_name,
                   const std::string& tenant_id,
                   std::optional<std::string> tenant_key,
                   std::optional<std::string> tenant_key_iv,
                   std::optional<std::string> tenant_key_version);

  // As the tenant ID is globally unique and cannot be changed, while the tenant
  // name can be changed, we use the tenant ID as the parameter to delete a tenant.
  //
  // TODO(kedeng):
  //     All data owned by a tenant should be deleted when the tenant is removed.
  Status RemoveTenant(const std::string& tenant_id);

  // Get all the tenant id including the default tenant.
  std::vector<std::string> GetAllTenants() const;

  // Use to get the total count of all the tenants.
  int32 tenants_count() const;

  // Use to confirm whether there is tenants information in metadata.
  bool is_tenants_exist() const;

  // Use to get the existence of the specific tenant.
  bool is_tenant_exist(const std::string& tenant_id) const;

  // Return the initialization name for the tenant.
  // If PartialOpen() or Open() have not been called, this will
  // crash. If the tenant does not exist, it returns an empty string.
  std::string tenant_name(const std::string& tenant_id = fs::kDefaultTenantID) const;

  // Return the tenant key persisted on the local filesystem corresponding to the
  // tenant_id. After the tenant key is decrypted, it can be used to encrypt/decrypt
  // file keys on the filesystem. If PartialOpen() or Open() have not been called, this
  // will crash. If the tenant does not exist, it returns an empty string.
  std::string tenant_key(const std::string& tenant_id = fs::kDefaultTenantID) const;

  // Return the initialization vector for the tenant key.
  // If PartialOpen() or Open() have not been called, this will
  // crash. If the tenant does not exist, it returns an empty string.
  std::string tenant_key_iv(const std::string& tenant_id = fs::kDefaultTenantID) const;

  // Return the version of the tenant key.
  // If PartialOpen() or Open() have not been called, this will
  // crash. If the tenant does not exist, it returns an empty string.
  std::string tenant_key_version(
      const std::string& tenant_id = fs::kDefaultTenantID) const;

  // Return the server key persisted on the local filesystem.
  //
  // NOTE :
  // During the first upgrade to the multi-tenant version, if it is detected
  // that the relevant configuration items of server key have a value, we will
  // use it to initialize the default tenant, and the original server key related
  // configuration items will be retained at the same time.
  const std::string& server_key() const;

  // Return the initialization vector for the server key.
  const std::string& server_key_iv() const;

  // Return the version of the server key.
  const std::string& server_key_version() const;

  // ==========================================================================
  //  file-system helpers
  // ==========================================================================
  // Used to judge whether a certain path exists.
  //
  // If the tenant id is not specified, we treat it as the default tenant.
  bool Exists(const std::string& path,
              const std::string& tenant_id = fs::kDefaultTenantID) const {
    return GetEnv(tenant_id)->FileExists(path);
  }

  // Get the dir list belongs to the tenant.
  //
  // If the tenant id is not specified, we treat it as the default tenant.
  Status ListDir(const std::string& path,
                 std::vector<std::string> *objects,
                 const std::string& tenant_id = fs::kDefaultTenantID) const {
    return GetEnv(tenant_id)->GetChildren(path, objects);
  }

  // Search the tenant's dd manager.
  //
  // If the tenant id is not specified, we treat it as the default tenant.
  scoped_refptr<fs::DataDirManager> dd_manager(
      const std::string& tenant_id = fs::kDefaultTenantID) const;

  // Get block manager by tenant id.
  // If the tenant does not exist, add it to the metadata, create a new block
  // manager and new dd manager for corresponding.
  //
  // If the tenant id is not specified, we treat it as the default tenant.
  scoped_refptr<fs::BlockManager> block_manager(
      const std::string& tenant_id = fs::kDefaultTenantID) const;

  // Prints the file system trees under the file system roots.
  //
  // If the tenant id is not specified, we treat it as the default tenant.
  void DumpFileSystemTree(std::ostream& out,
                          const std::string& tenant_id = fs::kDefaultTenantID);

  bool meta_on_xfs() const {
    return meta_on_xfs_;
  }

 private:
  FRIEND_TEST(fs::FsManagerTestBase, TestDuplicatePaths);
  FRIEND_TEST(fs::FsManagerTestBase, TestEIOWhileRunningUpdateDirsTool);
  FRIEND_TEST(fs::FsManagerTestBase, TestIsolatedMetadataDir);
  FRIEND_TEST(fs::FsManagerTestBase, TestMetadataDirInWALRoot);
  FRIEND_TEST(fs::FsManagerTestBase, TestMetadataDirInDataRoot);
  FRIEND_TEST(fs::FsManagerTestBase, TestOpenWithDuplicateInstanceFiles);
  FRIEND_TEST(fs::FsManagerTestBase, TestTenantAccountOperation);
  FRIEND_TEST(tserver::MiniTabletServerTest, TestFsLayoutEndToEnd);
  friend class itest::MiniClusterFsInspector; // for access to directory names
  friend Status tools::UpdateEncryptionKeyInfo(Env* env); // for update the metadata

  // Initializes, sanitizes, and canonicalizes the filesystem roots.
  // Determines the correct filesystem root for tablet-specific metadata.
  Status Init();

  // Select and create an instance of the appropriate block manager.
  // Search for the block manager corresponding to the tenant first, if no one exist,
  // create a new one and then return it.
  //
  // Does not actually perform any on-disk operations.
  scoped_refptr<fs::BlockManager> InitBlockManager(
      const std::string& tenant_id = fs::kDefaultTenantID);

  // Search the tenant's block manager.
  scoped_refptr<fs::BlockManager> SearchBlockManager(const std::string& tenant_id) const {
    std::lock_guard<LockType> lock(bm_lock_);
    scoped_refptr<fs::BlockManager> block_manager(FindPtrOrNull(block_manager_map_, tenant_id));
    return block_manager;
  }

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
  Status CreateInstanceMetadata(std::optional<std::string> uuid,
                                std::optional<std::string> tenant_name,
                                std::optional<std::string> tenant_id,
                                std::optional<std::string> encryption_key,
                                std::optional<std::string> encryption_key_iv,
                                std::optional<std::string> encryption_key_version,
                                InstanceMetadataPB* metadata);

  // Save a InstanceMetadataPB to the filesystem.
  // Does not mutate the current state of the fsmanager.
  Status WriteInstanceMetadata(const InstanceMetadataPB& metadata,
                               const std::string& root);

  // To support multi-tenant scenarios and non-encrypted scenarios, some interfaces
  // have added a tenant ID parameter.
  // This interface is used to confirm the validity of the tenant ID. The default
  // tenant ID is valid in any scenario, while a non-default tenant ID is only valid
  // when multi-tenant features are enabled.
  //
  // If valid, it returns true, otherwise it returns false.
  bool VertifyTenant(const std::string& tenant_id) const;

  // We record tenant information in metadata and persist it to disk. When we add a
  // new tenant, the records on the disk need to be updated at first, and then
  // update the metadata in memory if success.
  //
  // Called only in the encryption enable scenario when a new tenant appears.
  Status AddTenantMetadata(const std::string& tenant_name,
                           const std::string& tenant_id,
                           const std::string& tenant_key,
                           const std::string& tenant_key_iv,
                           const std::string& tenant_key_version);

  // Remove the tenant recorded in the memory and the disk.
  Status RemoveTenantMetadata(const std::string& tenant_id);

  // Update metadata after adding tenant or removing tenant.
  Status UpdateMetadata(
    std::unique_ptr<InstanceMetadataPB>& metadata);

  // Search tenant for metadata by tenant id.
  const InstanceMetadataPB_TenantMetadataPB* GetTenant(const std::string& tenant_id) const;
  // Except that the caller must hold metadata_rwlock_.
  const InstanceMetadataPB_TenantMetadataPB* GetTenantUnlock(const std::string& tenant_id) const;

  // Return the tenant id persisted on the local filesystem.
  // Except that the caller must hold metadata_rwlock_.
  std::string tenant_name_unlock(const std::string& tenant_id = fs::kDefaultTenantID) const;

  // Return the tenant key persisted on the local filesystem.
  // Except that the caller must hold metadata_rwlock_.
  std::string tenant_key_unlock(const std::string& tenant_id = fs::kDefaultTenantID) const;

  // Return the initialization vector for the tenant key unlock.
  // Except that the caller must hold metadata_rwlock_.
  std::string tenant_key_iv_unlock(
      const std::string& tenant_id = fs::kDefaultTenantID) const;

  // Return the version of the tenant key unlock.
  // Except that the caller must hold metadata_rwlock_.
  std::string tenant_key_version_unlock(
      const std::string& tenant_id = fs::kDefaultTenantID) const;

  // Use for update the format and the stamp in the metadata.
  // If you need to ensure call security, you must lock it by yourself.
  static void UpdateMetadataFormatAndStampUnlock(InstanceMetadataPB* metadata);

  // ==========================================================================
  //  file-system helpers
  // ==========================================================================

  // Create a new env for the tenant if search fail when '--enable_multi_tenancy' enabled.
  Env* AddEnv(const std::string& tenant_id);

  // Set encryption key for the env of tenant.
  Status SetEncryptionKey(const std::string& tenant_id);
  // Except that the caller must hold md_lock_.
  Status SetEncryptionKeyUnlock(const std::string& tenant_id);

  // Create data dir manager for tenant and add it to dd manager map.
  //
  // If the tenant id is not specified, we treat it as the default tenant.
  Status CreateNewDataDirManager(const std::string& tenant_id = fs::kDefaultTenantID);

  // Open data dir manager for tenant and add it to dd manager map.
  //
  // If the tenant id is not specified, we treat it as the default tenant.
  Status OpenDataDirManager(const std::string& tenant_id = fs::kDefaultTenantID);

  // Prints the file system tree for the objects in 'objects' under the given
  // 'path'. Prints lines with the given 'prefix'.
  void DumpFileSystemTree(std::ostream& out,
                          const std::string& prefix,
                          const std::string& path,
                          const std::vector<std::string>& objects);

  // Init and open block manager for tenant and add it to block manager map.
  //
  // If the tenant id is not specified, we treat it as the default tenant.
  Status InitAndOpenBlockManager(fs::FsReport* report = nullptr,
                                 std::atomic<int>* containers_processed = nullptr,
                                 std::atomic<int>* containers_total = nullptr,
                                 const std::string& tenant_id = fs::kDefaultTenantID,
                                 fs::BlockManager::MergeReport need_merage =
                                     fs::BlockManager::MergeReport::NOT_REQUIRED);

  // Add data dir manager to the 'dd_manager_map_' keyed by tenant_id.
  // Return 'AlreadyPresent' if the tenant present.
  //
  // If the tenant id is not specified, we treat it as the default tenant.
  Status AddDataDirManager(scoped_refptr<fs::DataDirManager> dd_manager,
                           const std::string& tenant_id = fs::kDefaultTenantID);

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

  // Different tenants should own different data storage paths.
  CanonicalizedRootsList get_canonicalized_data_fs_roots(const std::string& tenant_id) const;

  static const char *kDataDirName;
  static const char *kTabletMetadataDirName;
  static const char *kWalDirName;
  static const char *kInstanceMetadataFileName;
  static const char *kConsensusMetadataDirName;

  typedef rw_spinlock LockType;

  // Lock protecting the env_map_ below.
  mutable LockType env_lock_;
  // The environment to be used for all filesystem operations.
  // Different tenant use different env.
  typedef std::map<std::string, std::shared_ptr<Env>> EnvMap;
  // The environment for default tenant, which belongs to kudu and must never be deleted.
  Env* env_;
  // The map records the env information of all tenants except the default tenant.
  // They must be destroyed when not used anymore.
  EnvMap env_map_;

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

  // Lock protecting the 'metadata_'.
  mutable percpu_rwlock metadata_rwlock_;
  // Belongs to the default tenant.
  std::unique_ptr<InstanceMetadataPB> metadata_;

  // Shared by all the block managers.
  scoped_refptr<fs::FsErrorManager> error_manager_;
  // Lock protecting 'dd_manager_map_' below.
  mutable LockType ddm_lock_;
  typedef scoped_refptr<fs::DataDirManager> ScopedDDManagerPtr;
  typedef std::map<std::string, ScopedDDManagerPtr> DataDirManagerMap;
  DataDirManagerMap dd_manager_map_;

  // Lock protecting 'block_manager_map_'.
  mutable LockType bm_lock_;
  typedef scoped_refptr<fs::BlockManager> ScopedBlockManagerPtr;
  typedef std::map<std::string, ScopedBlockManagerPtr> BlockManagerMap;
  BlockManagerMap block_manager_map_;

  std::unique_ptr<security::KeyProvider> key_provider_;

  ObjectIdGenerator oid_generator_;

  bool initted_;

  // Cache whether or not the metadata directory is on an XFS directory.
  bool meta_on_xfs_;

  DISALLOW_COPY_AND_ASSIGN(FsManager);
};

} // namespace kudu

