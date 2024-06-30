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

#include "kudu/fs/fs_manager.h"

#include <cinttypes>
#include <ctime>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/default_key_provider.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/file_block_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_report.h"
#include "kudu/fs/key_provider.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/fs/ranger_kms_key_provider.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/timer.h"

DEFINE_bool(cmeta_fsync_override_on_xfs, true,
            "Whether to ignore --cmeta_force_fsync and instead always flush if Kudu detects "
            "the server is on XFS. This can prevent consensus metadata corruption in the "
            "event of sudden server failure. Disabling this flag may cause data loss in "
            "the event of a system crash. See KUDU-2195 for more details.");
TAG_FLAG(cmeta_fsync_override_on_xfs, experimental);
TAG_FLAG(cmeta_fsync_override_on_xfs, advanced);

DEFINE_bool(enable_data_block_fsync, true,
            "Whether to enable fsync() of data blocks, metadata, and their parent directories. "
            "Disabling this flag may cause data loss in the event of a system crash.");
TAG_FLAG(enable_data_block_fsync, unsafe);

#if defined(__linux__)
DEFINE_string(block_manager, "log", "Which block manager to use for storage. Valid options are "
                                    "'file'"
#if defined(NO_ROCKSDB)
                                    "and 'log'"
#else
                                    ", 'log' and 'logr'"
#endif
                                    ". The 'file' block manager is not suitable for production use "
                                    "due to scaling limitations.");
#else
DEFINE_string(block_manager, "file", "Which block manager to use for storage. "
              "Only the file block manager is supported for non-Linux systems.");
#endif
static bool ValidateBlockManagerType(const char* /*flagname*/, const std::string& value) {
  return ContainsKey(kudu::fs::BlockManager::block_manager_types(), value);
}
DEFINE_validator(block_manager, &ValidateBlockManagerType);
TAG_FLAG(block_manager, advanced);

DEFINE_string(fs_wal_dir, "",
              "Directory with write-ahead logs. If this is not specified, the "
              "program will not start. May be the same as fs_data_dirs");
TAG_FLAG(fs_wal_dir, stable);
DEFINE_string(fs_data_dirs, "",
              "Comma-separated list of directories with data blocks. If this "
              "is not specified, fs_wal_dir will be used as the sole data "
              "block directory.");
TAG_FLAG(fs_data_dirs, stable);
DEFINE_string(fs_metadata_dir, "",
              "Directory with metadata. If this is not specified, for "
              "compatibility with Kudu 1.6 and below, Kudu will check the "
              "first entry of fs_data_dirs for metadata and use it as the "
              "metadata directory if any exists. If none exists, fs_wal_dir "
              "will be used as the metadata directory.");
TAG_FLAG(fs_metadata_dir, stable);

DEFINE_int64(fs_wal_dir_reserved_bytes, -1,
             "Number of bytes to reserve on the log directory filesystem for "
             "non-Kudu usage. The default, which is represented by -1, is that "
             "1% of the disk space on each disk will be reserved. Any other "
             "value specified represents the number of bytes reserved and must "
             "be greater than or equal to 0. Explicit percentages to reserve "
             "are not currently supported");
DEFINE_validator(fs_wal_dir_reserved_bytes, [](const char* /*n*/, int64_t v) { return v >= -1; });
TAG_FLAG(fs_wal_dir_reserved_bytes, runtime);

METRIC_DEFINE_gauge_int64(server, log_block_manager_containers_processing_time_startup,
                          "Time taken to open all log block containers during server startup",
                          kudu::MetricUnit::kMilliseconds,
                          "The total time taken by the server to open all the container"
                          "files during the startup",
                          kudu::MetricLevel::kDebug);

DEFINE_string(encryption_key_provider, "default",
              "Key provider implementation to generate and decrypt server keys. "
              "Valid values are: 'default' (not for production usage), and 'ranger-kms'.");

DEFINE_validator(encryption_key_provider, [](const char* /*n*/, const std::string& value) {
  return value == "default" || value == "ranger-kms";
});

DEFINE_string(ranger_kms_url, "",
              "Comma-separated list of Ranger KMS server URLs. Must be set when "
              "'encryption_key_provider' is set to 'ranger-kms'.");

DEFINE_string(encryption_cluster_key_name, "kudu_cluster_key",
              "Name of the cluster key that is used to encrypt server encryption keys as "
              "stored in Ranger KMS.");

bool ValidateRangerKMSFlags() {
  if (FLAGS_encryption_key_provider == "ranger-kms") {
    if (FLAGS_ranger_kms_url.empty() ||
        FLAGS_encryption_cluster_key_name.empty() ||
        static_cast<std::vector<std::string>>(strings::Split(
            FLAGS_ranger_kms_url, ",", strings::SkipEmpty())).empty()) {
      LOG(ERROR) << "If 'encryption_key_provider' is set to 'ranger-kms', then "
                    "'ranger_kms_url' and 'encryption_cluster_key_name' must also be set.";
      return false;
    }
  }
  return true;
}

GROUP_FLAG_VALIDATOR(validate_ranger_kms_flags, ValidateRangerKMSFlags);

DECLARE_bool(enable_multi_tenancy);
DECLARE_bool(encrypt_data_at_rest);
DECLARE_int32(encryption_key_length);

using kudu::fs::BlockManager;
using kudu::fs::BlockManagerOptions;
using kudu::fs::CreateBlockOptions;
using kudu::fs::DataDirManager;
using kudu::fs::DataDirManagerOptions;
using kudu::fs::ErrorHandlerType;
using kudu::fs::ErrorNotificationCb;
using kudu::fs::FsErrorManager;
using kudu::fs::FileBlockManager;
using kudu::fs::FsReport;
using kudu::fs::LogBlockManagerNativeMeta;
#if !defined(NO_ROCKSDB)
using kudu::fs::LogBlockManagerRdbMeta;
#endif
using kudu::fs::ReadableBlock;
using kudu::fs::UpdateInstanceBehavior;
using kudu::fs::WritableBlock;
using kudu::pb_util::SecureDebugString;
using kudu::security::DefaultKeyProvider;
using kudu::security::RangerKMSKeyProvider;
using std::optional;
using std::ostream;
using std::shared_lock;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::a2b_hex;
using strings::Substitute;

namespace kudu {

// ==========================================================================
//  FS Paths
// ==========================================================================
const char *FsManager::kWalDirName = "wals";
const char *FsManager::kWalFileNamePrefix = "wal";
const char *FsManager::kWalsRecoveryDirSuffix = ".recovery";
const char *FsManager::kTabletMetadataDirName = "tablet-meta";
const char *FsManager::kDataDirName = "data";
const char *FsManager::kInstanceMetadataFileName = "instance";
const char *FsManager::kConsensusMetadataDirName = "consensus-meta";

FsManagerOpts::FsManagerOpts()
  : wal_root(FLAGS_fs_wal_dir),
    metadata_root(FLAGS_fs_metadata_dir),
    block_manager_type(FLAGS_block_manager),
    read_only(false),
    update_instances(UpdateInstanceBehavior::UPDATE_AND_IGNORE_FAILURES),
    file_cache(nullptr),
    skip_block_manager(false) {
  data_roots = strings::Split(FLAGS_fs_data_dirs, ",", strings::SkipEmpty());
}

FsManagerOpts::FsManagerOpts(const string& root)
  : wal_root(root),
    data_roots({ root }),
    block_manager_type(FLAGS_block_manager),
    read_only(false),
    update_instances(UpdateInstanceBehavior::UPDATE_AND_IGNORE_FAILURES),
    file_cache(nullptr),
    skip_block_manager(false) {}

FsManager::FsManager(Env* env, FsManagerOpts opts)
  : env_(DCHECK_NOTNULL(env)),
    opts_(std::move(opts)),
    error_manager_(new FsErrorManager()),
    initted_(false),
    meta_on_xfs_(false) {
  DCHECK(opts_.update_instances == UpdateInstanceBehavior::DONT_UPDATE ||
         !opts_.read_only) << "FsManager can only be for updated if not in read-only mode";
  if (FLAGS_encrypt_data_at_rest) {
    if (FLAGS_encryption_key_provider == "ranger-kms") {
      key_provider_.reset(new RangerKMSKeyProvider(FLAGS_ranger_kms_url,
                                                   FLAGS_encryption_cluster_key_name));
    } else {
      key_provider_.reset(new DefaultKeyProvider());
    }
  }
}

FsManager::~FsManager() {
  {
    std::lock_guard<LockType> lock(ddm_lock_);
    dd_manager_map_.clear();
  }
  {
    std::lock_guard<LockType> lock(bm_lock_);
    block_manager_map_.clear();
  }
  {
    std::lock_guard<LockType> lock(env_lock_);
    env_map_.clear();
  }
}

void FsManager::SetErrorNotificationCb(ErrorHandlerType e, ErrorNotificationCb cb) {
  error_manager_->SetErrorNotificationCb(e, std::move(cb));
}

void FsManager::UnsetErrorNotificationCb(ErrorHandlerType e) {
  error_manager_->UnsetErrorNotificationCb(e);
}

Status FsManager::Init() {
  if (initted_) {
    return Status::OK();
  }

  // The wal root must be set.
  if (opts_.wal_root.empty()) {
    return Status::IOError("Write-ahead log directory (fs_wal_dir) not provided");
  }

  // Deduplicate all of the roots.
  unordered_set<string> all_roots = { opts_.wal_root };
  all_roots.insert(opts_.data_roots.begin(), opts_.data_roots.end());

  // If the metadata root not set, Kudu will either use the wal root or the
  // first data root, in which case we needn't canonicalize additional roots.
  if (!opts_.metadata_root.empty()) {
    all_roots.insert(opts_.metadata_root);
  }

  // Build a map of original root --> canonicalized root, sanitizing each
  // root as we go and storing the canonicalization status.
  typedef unordered_map<string, CanonicalizedRootAndStatus> RootMap;
  RootMap canonicalized_roots;
  for (const string& root : all_roots) {
    if (root.empty()) {
      return Status::IOError("Empty string provided for path");
    }
    if (root[0] != '/') {
      return Status::IOError(
          Substitute("Relative path $0 provided", root));
    }
    string root_copy = root;
    StripWhiteSpace(&root_copy);
    if (root != root_copy) {
      return Status::IOError(
          Substitute("Path $0 contains illegal whitespace", root));
    }

    // Strip the basename when canonicalizing, as it may not exist. The
    // dirname, however, must exist.
    string canonicalized;
    Status s = GetEnv()->Canonicalize(DirName(root), &canonicalized);
    if (PREDICT_FALSE(!s.ok())) {
      if (s.IsNotFound() || s.IsDiskFailure()) {
        // If the directory fails to canonicalize due to disk failure, store
        // the non-canonicalized form and the returned error.
        canonicalized = DirName(root);
      } else {
        return s.CloneAndPrepend(Substitute("Failed to canonicalize $0", root));
      }
    }
    canonicalized = JoinPathSegments(canonicalized, BaseName(root));
    InsertOrDie(&canonicalized_roots, root, { canonicalized, s });
  }

  // All done, use the map to set the canonicalized state.

  canonicalized_wal_fs_root_ = FindOrDie(canonicalized_roots, opts_.wal_root);
  unordered_set<string> unique_roots;
  if (!opts_.data_roots.empty()) {
    for (const string& data_fs_root : opts_.data_roots) {
      const auto& root = FindOrDie(canonicalized_roots, data_fs_root);
      if (InsertIfNotPresent(&unique_roots, root.path)) {
        canonicalized_data_fs_roots_.emplace_back(root);
        canonicalized_all_fs_roots_.emplace_back(root);
      }
    }
  } else {
    LOG(INFO) << "Data directories (fs_data_dirs) not provided";
    LOG(INFO) << "Using write-ahead log directory (fs_wal_dir) as data directory";
    canonicalized_data_fs_roots_.emplace_back(canonicalized_wal_fs_root_);
  }
  if (InsertIfNotPresent(&unique_roots, canonicalized_wal_fs_root_.path)) {
    canonicalized_all_fs_roots_.emplace_back(canonicalized_wal_fs_root_);
  }

  // Decide on a metadata root to use.
  if (opts_.metadata_root.empty()) {
    // Check the first data root for metadata.
    const string meta_dir_in_data_root = JoinPathSegments(canonicalized_data_fs_roots_[0].path,
                                                          kTabletMetadataDirName);
    // If there is already metadata in the first data root, use it. Otherwise,
    // use the WAL root.
    LOG(INFO) << "Metadata directory not provided";
    if (GetEnv()->FileExists(meta_dir_in_data_root)) {
      canonicalized_metadata_fs_root_ = canonicalized_data_fs_roots_[0];
      LOG(INFO) << "Using existing metadata directory in first data directory";
    } else {
      canonicalized_metadata_fs_root_ = canonicalized_wal_fs_root_;
      LOG(INFO) << "Using write-ahead log directory (fs_wal_dir) as metadata directory";
    }
  } else {
    // Keep track of the explicitly-defined metadata root.
    canonicalized_metadata_fs_root_ = FindOrDie(canonicalized_roots, opts_.metadata_root);
    if (InsertIfNotPresent(&unique_roots, canonicalized_metadata_fs_root_.path)) {
      canonicalized_all_fs_roots_.emplace_back(canonicalized_metadata_fs_root_);
    }
  }

  // The server cannot start if the WAL root or metadata root failed to
  // canonicalize.
  const string& wal_root = canonicalized_wal_fs_root_.path;
  RETURN_NOT_OK_PREPEND(canonicalized_wal_fs_root_.status,
      Substitute("Write-ahead log directory $0 failed to canonicalize", wal_root));
  const string& meta_root = canonicalized_metadata_fs_root_.path;
  RETURN_NOT_OK_PREPEND(canonicalized_metadata_fs_root_.status,
      Substitute("Metadata directory $0 failed to canonicalize", meta_root));

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "WAL root: " << canonicalized_wal_fs_root_.path;
    VLOG(1) << "Metadata root: " << canonicalized_metadata_fs_root_.path;
    VLOG(1) << "Data roots: " <<
      JoinStrings(DataDirManager::GetRootNames(canonicalized_data_fs_roots_), ",");
    VLOG(1) << "All roots: " <<
      JoinStrings(DataDirManager::GetRootNames(canonicalized_all_fs_roots_), ",");
  }

  initted_ = true;
  return Status::OK();
}

scoped_refptr<BlockManager> FsManager::InitBlockManager(const string& tenant_id) {
  auto block_manager = SearchBlockManager(tenant_id);
  if (block_manager) {
    return block_manager;
  }

  BlockManagerOptions bm_opts;
  bm_opts.metric_entity = opts_.metric_entity;
  bm_opts.parent_mem_tracker = opts_.parent_mem_tracker;
  bm_opts.read_only = opts_.read_only;
  if (opts_.block_manager_type == "file") {
    block_manager.reset(new FileBlockManager(
        GetEnv(tenant_id), dd_manager(tenant_id), error_manager_,
        opts_.file_cache, std::move(bm_opts), tenant_id));
  } else if (opts_.block_manager_type == "log") {
    block_manager.reset(new LogBlockManagerNativeMeta(
        GetEnv(tenant_id), dd_manager(tenant_id), error_manager_,
        opts_.file_cache, std::move(bm_opts), tenant_id));
#if !defined(NO_ROCKSDB)
  } else if (opts_.block_manager_type == "logr") {
    block_manager.reset(new LogBlockManagerRdbMeta(
        GetEnv(tenant_id), dd_manager(tenant_id), error_manager_,
        opts_.file_cache, std::move(bm_opts), tenant_id));
#endif
  } else {
    LOG(FATAL) << "Unknown block_manager_type: " << opts_.block_manager_type;
  }

  {
    std::lock_guard<LockType> lock(bm_lock_);
    block_manager_map_[tenant_id] = block_manager;
  }

  return block_manager;
}

Status FsManager::PartialOpen(CanonicalizedRootsList* missing_roots) {
  RETURN_NOT_OK(Init());
  string reference_instance_path;
  for (auto& root : canonicalized_all_fs_roots_) {
    if (!root.status.ok()) {
      continue;
    }
    unique_ptr<InstanceMetadataPB> pb(new InstanceMetadataPB);
    Status s = pb_util::ReadPBContainerFromPath(GetEnv(), GetInstanceMetadataPath(root.path),
                                                pb.get(), pb_util::NOT_SENSITIVE);
    if (PREDICT_FALSE(!s.ok())) {
      if (s.IsNotFound()) {
        if (missing_roots) {
          missing_roots->emplace_back(root);
        }
        continue;
      }
      if (s.IsDiskFailure()) {
        root.status = s.CloneAndPrepend("Failed to open instance file");
        continue;
      }
      return s;
    }

    if (!metadata_) {
      metadata_.reset(pb.release());
      reference_instance_path = root.path;
    } else if (pb->uuid() != metadata_->uuid()) {
      return Status::Corruption(Substitute(
          "Mismatched UUIDs across filesystem roots: The path $0 contains UUID $1 vs. "
          "the path $2 contains UUID $3; configuring multiple Kudu processes with the same "
          "directory is not supported",
          reference_instance_path, metadata_->uuid() , root.path, pb->uuid()));
    }
  }
  if (!metadata_) {
    return Status::NotFound("could not find a healthy instance file");
  }
  const auto& meta_root_path = canonicalized_metadata_fs_root_.path;
  const auto bad_meta_fs_msg =
      Substitute("Could not determine file system of metadata directory $0",
                 meta_root_path);
  Status s = GetEnv()->IsOnXfsFilesystem(meta_root_path, &meta_on_xfs_);
  if (FLAGS_cmeta_fsync_override_on_xfs) {
    // Err on the side of visibility if we expect a behavior change based on
    // the file system.
    RETURN_NOT_OK_PREPEND(s, bad_meta_fs_msg);
  } else {
    WARN_NOT_OK(s, bad_meta_fs_msg);
  }
  VLOG(1) << Substitute("Detected metadata directory is $0on an XFS mount: $1",
                        meta_on_xfs_ ? "" : "not ", meta_root_path);
  return Status::OK();
}

Status FsManager::Open(FsReport* report, Timer* read_instance_metadata_files,
                       Timer* read_data_directories,
                       std::atomic<int>* containers_processed,
                       std::atomic<int>* containers_total) {
  if (read_instance_metadata_files) {
    read_instance_metadata_files->Start();
  }
  // Load and verify the instance metadata files.
  //
  // Done first to minimize side effects in the case that the configured roots
  // are not yet initialized on disk.
  CanonicalizedRootsList missing_roots;
  RETURN_NOT_OK(PartialOpen(&missing_roots));

  // Ensure all of the ancillary directories exist.
  vector<string> ancillary_dirs = { GetWalsRootDir(),
                                    GetTabletMetadataDir(),
                                    GetConsensusMetadataDir() };
  for (const auto& d : ancillary_dirs) {
    bool is_dir;
    RETURN_NOT_OK_PREPEND(GetEnv()->IsDirectory(d, &is_dir),
                          Substitute("could not verify required directory $0", d));
    if (!is_dir) {
      return Status::Corruption(
          Substitute("Required directory $0 exists but is not a directory", d));
    }
  }

  // In the event of failure, delete everything we created.
  vector<string> created_dirs;
  vector<string> created_files;
  auto deleter = MakeScopedCleanup([&]() {
    // Delete files first so that the directories will be empty when deleted.
    for (const auto& f : created_files) {
      WARN_NOT_OK(GetEnv()->DeleteFile(f), "Could not delete file " + f);
    }
    // Delete directories in reverse order since parent directories will have
    // been added before child directories.
    for (auto it = created_dirs.rbegin(); it != created_dirs.rend(); it++) {
      WARN_NOT_OK(GetEnv()->DeleteDir(*it), "Could not delete dir " + *it);
    }
  });

  // Create any missing roots, if desired.
  if (!opts_.read_only &&
      opts_.update_instances != UpdateInstanceBehavior::DONT_UPDATE) {
    Status s = CreateFileSystemRoots(
        missing_roots, *metadata_, &created_dirs, &created_files);
    static const string kUnableToCreateMsg = "unable to create missing filesystem roots";
    if (opts_.update_instances == UpdateInstanceBehavior::UPDATE_AND_IGNORE_FAILURES) {
      // We only warn on error here -- regardless of errors, we might be in
      // good enough shape to open the DataDirManager.
      WARN_NOT_OK(s, kUnableToCreateMsg);
    } else if (opts_.update_instances == UpdateInstanceBehavior::UPDATE_AND_ERROR_ON_FAILURE) {
      RETURN_NOT_OK_PREPEND(s, kUnableToCreateMsg);
    }
  }
  if (read_instance_metadata_files) {
    read_instance_metadata_files->Stop();
  }

  string decrypted_key;
  bool tenants_exist = is_tenants_exist();
  if (tenants_exist && key_provider_) {
    if (!FLAGS_enable_multi_tenancy) {
      return Status::IllegalState(
          "The '--enable_multi_tenancy' should set for the existed tenants.");
    }

    if (FLAGS_encrypt_data_at_rest && tenant_key(fs::kDefaultTenantID).empty()) {
      return Status::IllegalState(
          "Data at rest encryption is enabled and tenants exist, but no tenant key found");
    }

    // TODO(kedeng) :
    //     After implementing tenant management, different tenants need to be handled here.
    //
    // The priority of tenant key is higher than that of server key.
    RETURN_NOT_OK(
        key_provider_->DecryptEncryptionKey(this->tenant_key(fs::kDefaultTenantID),
                                            this->tenant_key_iv(fs::kDefaultTenantID),
                                            this->tenant_key_version(fs::kDefaultTenantID),
                                            &decrypted_key));
  } else if (!server_key().empty() && key_provider_) {
    // Just check whether the upgrade operation is needed for '--enable_multi_tenancy'.
    if (FLAGS_enable_multi_tenancy) {
      return Status::IllegalState(
          "--enable_multi_tenancy is set, but no tenants exist.");
    }

    string server_key;
    RETURN_NOT_OK(key_provider_->DecryptEncryptionKey(this->server_key(),
                                                      this->server_key_iv(),
                                                      this->server_key_version(),
                                                      &decrypted_key));
  } else if (server_key().empty() && FLAGS_encrypt_data_at_rest) {
    return Status::IllegalState(
        "--encrypt_data_at_rest is set, but no server key found.");
  }

  if (!decrypted_key.empty()) {
    // 'decrypted_key' is a hexadecimal string and SetEncryptionKey expects bits
    // (hex / 2 = bytes * 8 = bits).
    GetEnv()->SetEncryptionKey(reinterpret_cast<const uint8_t*>(
                                 a2b_hex(decrypted_key).c_str()),
                               decrypted_key.length() * 4);
  }

  // Open the directory manager if it has not been opened already.
  RETURN_NOT_OK(OpenDataDirManager());

  // Only clean temporary files after the data dir manager successfully opened.
  // This ensures that we were able to obtain the exclusive directory locks
  // on the data directories before we start deleting files.
  if (!opts_.read_only) {
    CleanTmpFiles();
    CheckAndFixPermissions();
  }

  // Set an initial error handler to mark data directories as failed.
  error_manager_->SetErrorNotificationCb(
      ErrorHandlerType::DISK_ERROR, [this](const string& uuid, const string& tenant_id) {
        this->dd_manager(tenant_id)->MarkDirFailedByUuid(uuid);
      });

  // Finally, initialize and open the block manager if needed.
  if (!opts_.skip_block_manager) {
    if (read_data_directories) {
      read_data_directories->Start();
    }
    RETURN_NOT_OK(InitAndOpenBlockManager(report,
                                          containers_processed,
                                          containers_total,
                                          fs::kDefaultTenantID,
                                          BlockManager::MergeReport::REQUIRED));
    if (read_data_directories) {
      read_data_directories->Stop();
      if (opts_.metric_entity && FsManager::IsLogType(opts_.block_manager_type)) {
        METRIC_log_block_manager_containers_processing_time_startup.Instantiate(opts_.metric_entity,
            (read_data_directories->TimeElapsed()).ToMilliseconds());
      }
    }
  }
  // Report wal and metadata directories.
  if (report) {
    report->wal_dir = canonicalized_wal_fs_root_.path;
    report->metadata_dir = canonicalized_metadata_fs_root_.path;
  }

  if (FLAGS_enable_data_block_fsync) {
    // Files/directories created by the directory manager in the fs roots have
    // been synchronized, so now is a good time to sync the roots themselves.
    WARN_NOT_OK(env_util::SyncAllParentDirs(GetEnv(), created_dirs, created_files),
                "could not sync newly created fs roots");
  }

  LOG(INFO) << "Opened local filesystem: " <<
      JoinStrings(DataDirManager::GetRootNames(canonicalized_all_fs_roots_), ",")
            << std::endl << SecureDebugString(*metadata_);

  if (!created_dirs.empty()) {
    LOG(INFO) << "New directories created while opening local filesystem: " <<
        JoinStrings(created_dirs, ", ");
  }
  if (!created_files.empty()) {
    LOG(INFO) << "New files created while opening local filesystem: " <<
        JoinStrings(created_files, ", ");
  }

  // Success: do not delete any missing roots created.
  deleter.cancel();
  return Status::OK();
}

Status FsManager::AddDataDirManager(scoped_refptr<DataDirManager> dd_manager,
                                    const string& tenant_id) {
  std::lock_guard<LockType> lock(ddm_lock_);
  scoped_refptr<DataDirManager> ddm(FindPtrOrNull(dd_manager_map_, tenant_id));
  if (ddm) {
    return Status::AlreadyPresent(Substitute("Tenant $0 already exists.", tenant_id));
  }
  dd_manager_map_[tenant_id] = std::move(dd_manager);
  return Status::OK();
}

CanonicalizedRootsList FsManager::get_canonicalized_data_fs_roots(const string& tenant_id) const {
  if (tenant_id == fs::kDefaultTenantID) {
    return canonicalized_data_fs_roots_;
  }

  // TODO(kedeng):
  //   Different tenants should own different data storage paths, and the new solution
  // needs to be compatible with existing implementation details.
  return canonicalized_data_fs_roots_;
}

Status FsManager::CreateNewDataDirManager(const string& tenant_id) {
  CHECK(!dd_manager(tenant_id));

  scoped_refptr<DataDirManager> ddm = nullptr;
  DataDirManagerOptions dm_opts;
  dm_opts.metric_entity = opts_.metric_entity;
  dm_opts.read_only = opts_.read_only;
  dm_opts.dir_type = opts_.block_manager_type;
  dm_opts.tenant_id = tenant_id;
  LOG_TIMING(INFO, "creating directory manager") {
  RETURN_NOT_OK(DataDirManager::CreateNew(GetEnv(tenant_id),
      get_canonicalized_data_fs_roots(tenant_id), dm_opts, &ddm));
  }
  RETURN_NOT_OK(AddDataDirManager(ddm, tenant_id));

  return Status::OK();
}

Status FsManager::OpenDataDirManager(const string& tenant_id) {
  if (!dd_manager(tenant_id)) {
    scoped_refptr<DataDirManager> ddm = nullptr;
    DataDirManagerOptions dm_opts;
    dm_opts.metric_entity = opts_.metric_entity;
    dm_opts.read_only = opts_.read_only;
    dm_opts.dir_type = opts_.block_manager_type;
    dm_opts.update_instances = opts_.update_instances;
    dm_opts.tenant_id = tenant_id;
    LOG_TIMING(INFO, "opening directory manager") {
    RETURN_NOT_OK(DataDirManager::OpenExisting(GetEnv(tenant_id),
        get_canonicalized_data_fs_roots(tenant_id), dm_opts, &ddm));
    }
    RETURN_NOT_OK(AddDataDirManager(ddm, tenant_id));
  }
  return Status::OK();
}

Status FsManager::InitAndOpenBlockManager(FsReport* report,
                                          std::atomic<int>* containers_processed,
                                          std::atomic<int>* containers_total,
                                          const string& tenant_id,
                                          BlockManager::MergeReport need_merage) {
  auto block_manager = InitBlockManager(tenant_id);
  DCHECK(block_manager);
  LOG_TIMING(INFO, "opening block manager") {
    if (opts_.block_manager_type == "file") {
      RETURN_NOT_OK(block_manager->Open(report, need_merage, nullptr, nullptr));
    } else {
      RETURN_NOT_OK(block_manager->Open(report, need_merage,
                                        containers_processed, containers_total));
    }
  }
  return Status::OK();
}

void FsManager::CopyMetadata(unique_ptr<InstanceMetadataPB>* metadata) {
  shared_lock<rw_spinlock> md_lock(metadata_rwlock_.get_lock());
  (*metadata)->CopyFrom(*metadata_);
}

Status FsManager::UpdateMetadata(unique_ptr<InstanceMetadataPB>& metadata) {
  // In the event of failure, rollback everything we changed.
  // <string, string>   <=>   <old instance file, backup instance file>
  unordered_map<string, string> changed_dirs;
  auto rollbacker = MakeScopedCleanup([&]() {
    // Delete new files first so that the backup files could do rollback.
    for (const auto& changed : changed_dirs) {
      WARN_NOT_OK(GetEnv()->DeleteFile(changed.first),
                  Substitute("Could not delete file $0 for rollback.", changed.first));
      VLOG(1) << "Delete file: " << changed.first << " for rollback.";

      WARN_NOT_OK(GetEnv()->RenameFile(changed.second, changed.first),
                  Substitute("Could not rename file $0 for rollback.", changed.second));
      VLOG(1) << "Rename file: " << changed.second << " to " << changed.first << " for rollback.";
    }
  });

  for (const auto& root : canonicalized_all_fs_roots_) {
    if (!root.status.ok()) {
      continue;
    }
    // Backup the metadata at first.
    const string old_path = GetInstanceMetadataPath(root.path);
    string tmp_path = Substitute("$0-$1", old_path, GetCurrentTimeMicros());
    WARN_NOT_OK(GetEnv()->RenameFile(old_path, tmp_path),
                Substitute("Could not rename file $0, ", old_path));
    changed_dirs[old_path] = tmp_path;
    // Write the instance metadata with latest data.
    RETURN_NOT_OK_PREPEND(WriteInstanceMetadata(*metadata, root.path),
                          Substitute("Fail to rewrite the instance metadata for path: $0, "
                          "now try to rollback all the instance metadata file.", old_path));
  }

  {
    // Update the records in memory.
    std::lock_guard<percpu_rwlock> lock(metadata_rwlock_);
    metadata_ = std::move(metadata);
  }

  // If all op success, remove the backup data.
  for (const auto& changed : changed_dirs) {
    WARN_NOT_OK(GetEnv()->DeleteFile(changed.second), Substitute("Could not delete file $0, ",
                                                             changed.second));
  }

  // Success: don't rollback any files.
  rollbacker.cancel();
  return Status::OK();
}

void FsManager::UpdateMetadataFormatAndStampUnlock(InstanceMetadataPB* metadata) {
  string time_str;
  StringAppendStrftime(&time_str, "%Y-%m-%d %H:%M:%S", time(nullptr), false);
  string hostname;
  if (!GetHostname(&hostname).ok()) {
    hostname = "<unknown host>";
  }
  metadata->set_format_stamp(Substitute("Formatted at $0 on $1", time_str, hostname));
}

bool FsManager::IsLogType(const std::string& block_manager_type) {
  return block_manager_type != "file"
      && ContainsKey(BlockManager::block_manager_types(), block_manager_type);
}

Status FsManager::AddTenantMetadata(const string& tenant_name,
                                    const string& tenant_id,
                                    const string& tenant_key,
                                    const string& tenant_key_iv,
                                    const string& tenant_key_version) {
  unique_ptr<InstanceMetadataPB> metadata(new InstanceMetadataPB);
  {
    shared_lock<rw_spinlock> md_lock(metadata_rwlock_.get_lock());
    metadata->CopyFrom(*metadata_);
  }
  InstanceMetadataPB::TenantMetadataPB* tenant_metadata = metadata->add_tenants();
  tenant_metadata->set_tenant_name(tenant_name);
  tenant_metadata->set_tenant_id(tenant_id);
  tenant_metadata->set_tenant_key(tenant_key);
  tenant_metadata->set_tenant_key_iv(tenant_key_iv);
  tenant_metadata->set_tenant_key_version(tenant_key_version);
  UpdateMetadataFormatAndStampUnlock(metadata.get());

  return UpdateMetadata(metadata);
}

Status FsManager::RemoveTenantMetadata(const string& tenant_id) {
  if (!is_tenant_exist(tenant_id)) {
    return Status::NotFound(Substitute("$0: tenant not found", tenant_id));
  }

  unique_ptr<InstanceMetadataPB> metadata(new InstanceMetadataPB);
  {
    shared_lock<rw_spinlock> md_lock(metadata_rwlock_.get_lock());
    metadata->CopyFrom(*metadata_);
  }
  for (int i = 0; i < metadata->tenants_size(); i++) {
    if (metadata->tenants(i).tenant_id() == tenant_id) {
      metadata->mutable_tenants()->DeleteSubrange(i, 1);
      break;
    }
  }
  UpdateMetadataFormatAndStampUnlock(metadata.get());

  return UpdateMetadata(metadata);
}

Status FsManager::CreateInitialFileSystemLayout(optional<string> uuid,
                                                optional<string> tenant_name,
                                                optional<string> tenant_id,
                                                optional<string> encryption_key,
                                                optional<string> encryption_key_iv,
                                                optional<string> encryption_key_version) {
  CHECK(!opts_.read_only);

  RETURN_NOT_OK(Init());

  // In the event of failure, delete everything we created.
  vector<string> created_dirs;
  vector<string> created_files;
  auto deleter = MakeScopedCleanup([&]() {
    // Delete files first so that the directories will be empty when deleted.
    for (const auto& f : created_files) {
      WARN_NOT_OK(GetEnv()->DeleteFile(f), "Could not delete file " + f);
    }
    // Delete directories in reverse order since parent directories will have
    // been added before child directories.
    for (auto it = created_dirs.rbegin(); it != created_dirs.rend(); it++) {
      WARN_NOT_OK(GetEnv()->DeleteDir(*it), "Could not delete dir " + *it);
    }
  });

  // Create the filesystem roots.
  //
  // Files/directories created will NOT be synchronized to disk.
  InstanceMetadataPB metadata;
  string tid = tenant_id ? *tenant_id : fs::kDefaultTenantID;
  RETURN_NOT_OK_PREPEND(CreateInstanceMetadata(std::move(uuid),
                                               std::move(tenant_name),
                                               std::move(tenant_id),
                                               std::move(encryption_key),
                                               std::move(encryption_key_iv),
                                               std::move(encryption_key_version),
                                               &metadata),
                        "unable to create instance metadata");
  RETURN_NOT_OK_PREPEND(FsManager::CreateFileSystemRoots(
      canonicalized_all_fs_roots_, metadata, &created_dirs, &created_files),
                        "unable to create file system roots");

  // Create ancillary directories.
  vector<string> ancillary_dirs = { GetWalsRootDir(),
                                    GetTabletMetadataDir(),
                                    GetConsensusMetadataDir() };
  for (const string& dir : ancillary_dirs) {
    bool created;
    RETURN_NOT_OK_PREPEND(env_util::CreateDirIfMissing(GetEnv(), dir, &created),
                          Substitute("Unable to create directory $0", dir));
    if (created) {
      created_dirs.emplace_back(dir);
    }
  }

  // Create the directory manager.
  //
  // All files/directories created will be synchronized to disk.
  RETURN_NOT_OK(CreateNewDataDirManager(tid));

  if (FLAGS_enable_data_block_fsync) {
    // Files/directories created by the directory manager in the fs roots have
    // been synchronized, so now is a good time to sync the roots themselves.
    WARN_NOT_OK(env_util::SyncAllParentDirs(GetEnv(), created_dirs, created_files),
                "could not sync newly created fs roots");
  }

  // Success: don't delete any files.
  deleter.cancel();
  return Status::OK();
}

Status FsManager::CreateFileSystemRoots(
    CanonicalizedRootsList canonicalized_roots,
    const InstanceMetadataPB& metadata,
    vector<string>* created_dirs,
    vector<string>* created_files) {
  CHECK(!opts_.read_only);

  // It's OK if a root already exists as long as there's nothing in it.
  vector<string> non_empty_roots;
  for (const auto& root : canonicalized_roots) {
    if (!root.status.ok()) {
      return Status::IOError("cannot create FS layout; at least one directory "
                             "failed to canonicalize", root.path);
    }
    if (!GetEnv()->FileExists(root.path)) {
      // We'll create the directory below.
      continue;
    }
    bool is_empty;
    RETURN_NOT_OK_PREPEND(env_util::IsDirectoryEmpty(GetEnv(), root.path, &is_empty),
                          "unable to check if FSManager root is empty");
    if (!is_empty) {
      non_empty_roots.emplace_back(root.path);
    }
  }

  if (!non_empty_roots.empty()) {
    return Status::AlreadyPresent(
        Substitute("FSManager roots already exist: $0", JoinStrings(non_empty_roots, ",")));
  }

  // All roots are either empty or non-existent. Create missing roots and all
  // subdirectories.
  for (const auto& root : canonicalized_roots) {
    if (!root.status.ok()) {
      continue;
    }
    string root_name = root.path;
    bool created;
    RETURN_NOT_OK_PREPEND(env_util::CreateDirIfMissing(GetEnv(), root_name, &created),
                          "unable to create FSManager root");
    if (created) {
      created_dirs->emplace_back(root_name);
    }
    RETURN_NOT_OK_PREPEND(WriteInstanceMetadata(metadata, root_name),
                          "unable to write instance metadata");
    created_files->emplace_back(GetInstanceMetadataPath(root_name));
  }
  return Status::OK();
}

Status FsManager::CreateInstanceMetadata(optional<string> uuid,
                                         optional<string> tenant_name,
                                         optional<string> tenant_id,
                                         optional<string> encryption_key,
                                         optional<string> encryption_key_iv,
                                         optional<string> encryption_key_version,
                                         InstanceMetadataPB* metadata) {
  if (uuid) {
    string canonicalized_uuid;
    RETURN_NOT_OK(oid_generator_.Canonicalize(*uuid, &canonicalized_uuid));
    metadata->set_uuid(canonicalized_uuid);
  } else {
    metadata->set_uuid(oid_generator_.Next());
  }

  if (tenant_name && tenant_id && encryption_key && encryption_key_iv && encryption_key_version) {
    // The tenant key info exist.
    //
    // If all the fileds, which means tenant_name/tenant_id/encryption_key/encryption_key_iv/
    // encryption_key_version, are specified, we treat it as tenant key info of multi-tenancy.
    InstanceMetadataPB::TenantMetadataPB* tenant_metadata = metadata->add_tenants();
    tenant_metadata->set_tenant_name(*tenant_name);
    tenant_metadata->set_tenant_id(*tenant_id);
    tenant_metadata->set_tenant_key(*encryption_key);
    tenant_metadata->set_tenant_key_iv(*encryption_key_iv);
    tenant_metadata->set_tenant_key_version(*encryption_key_version);
  } else if (!tenant_name && !tenant_id &&
             encryption_key && encryption_key_iv && encryption_key_version) {
    // The server key info exist.
    //
    // If all the fileds(encryption_key/encryption_key_iv/encryption_key_version) are specified
    // except tenant_name/tenant_id, we treat it as server key info for encryption.
    metadata->set_server_key(*encryption_key);
    metadata->set_server_key_iv(*encryption_key_iv);
    metadata->set_server_key_version(*encryption_key_version);
  } else {
    if (encryption_key || encryption_key_iv || encryption_key_version) {
      // There is incomplete encrypted key information.
      // (Tenant name or tenant id is not important in this case.)
      return Status::InvalidArgument(
          "'encryption_key', 'encryption_key_iv', and 'encryption_key_version' must be specified "
          "together (either all of them must be specified, or none of them).");
    }

    // No encrypted key information exist.
    if (FLAGS_encrypt_data_at_rest && FLAGS_enable_multi_tenancy) {
      // The multi_tenancy enabled.
      //
      // Use kDefaultTenant to encrypt data if '--enable_multi_tenancy' set with
      // '--encrypt_data_at_rest'.
      string key;
      string key_iv;
      string key_version;
      RETURN_NOT_OK_PREPEND(
          key_provider_->GenerateEncryptionKey(&key,
                                               &key_iv,
                                               &key_version),
          "failed to generate encrypted default tenant key");
      InstanceMetadataPB::TenantMetadataPB* tenant_metadata = metadata->add_tenants();
      tenant_metadata->set_tenant_name(fs::kDefaultTenantName);
      tenant_metadata->set_tenant_id(fs::kDefaultTenantID);
      tenant_metadata->set_tenant_key(key);
      tenant_metadata->set_tenant_key_iv(key_iv);
      tenant_metadata->set_tenant_key_version(key_version);
    } else if (FLAGS_encrypt_data_at_rest) {
      // The encrypt_data_at_rest enabled.
      //
      // Generate encrypted server key to encrypt data if only set '--encrypt_data_at_rest'.
      string key_version;
      RETURN_NOT_OK_PREPEND(
          key_provider_->GenerateEncryptionKey(metadata->mutable_server_key(),
                                               metadata->mutable_server_key_iv(),
                                               &key_version),
          "failed to generate encrypted server key");
      metadata->set_server_key_version(key_version);
    }
  }

  UpdateMetadataFormatAndStampUnlock(metadata);
  return Status::OK();
}

Status FsManager::WriteInstanceMetadata(const InstanceMetadataPB& metadata,
                                        const string& root) {
  const string path = GetInstanceMetadataPath(root);

  // The instance metadata is written effectively once per TS, so the
  // durability cost is negligible.
  RETURN_NOT_OK(pb_util::WritePBContainerToPath(GetEnv(), path,
                                                metadata,
                                                pb_util::NO_OVERWRITE,
                                                pb_util::SYNC,
                                                pb_util::NOT_SENSITIVE));
  LOG(INFO) << "Generated new instance metadata in path " << path << ":\n"
            << SecureDebugString(metadata);
  return Status::OK();
}

const string& FsManager::uuid() const {
  return CHECK_NOTNULL(metadata_.get())->uuid();
}

const string& FsManager::server_key() const {
  return CHECK_NOTNULL(metadata_.get())->server_key();
}

const string& FsManager::server_key_iv() const {
  return CHECK_NOTNULL(metadata_.get())->server_key_iv();
}

const string& FsManager::server_key_version() const {
  return CHECK_NOTNULL(metadata_.get())->server_key_version();
}

bool FsManager::VertifyTenant(const std::string& tenant_id) const {
  if (tenant_id == fs::kDefaultTenantID) {
    return true;
  }

  return FLAGS_enable_multi_tenancy && is_tenant_exist(tenant_id);
}

int32 FsManager::tenants_count() const {
  shared_lock<rw_spinlock> md_lock(metadata_rwlock_.get_lock());
  return metadata_->tenants_size();
}

bool FsManager::is_tenants_exist() const {
  shared_lock<rw_spinlock> md_lock(metadata_rwlock_.get_lock());
  return metadata_->tenants_size() > 0;
}

bool FsManager::is_tenant_exist(const string& tenant_id) const {
  return GetTenant(tenant_id) != nullptr;
}

const InstanceMetadataPB_TenantMetadataPB* FsManager::GetTenantUnlock(
    const string& tenant_id) const {
  const InstanceMetadataPB::TenantMetadataPB* tenant = nullptr;
  for (const auto& tdata : metadata_->tenants()) {
    if (tdata.tenant_id() == tenant_id) {
      tenant = &tdata;
      break;
    }
  }

  return tenant;
}

const InstanceMetadataPB_TenantMetadataPB* FsManager::GetTenant(
    const string& tenant_id) const {
  shared_lock<rw_spinlock> md_lock(metadata_rwlock_.get_lock());
  return GetTenantUnlock(tenant_id);
}

string FsManager::tenant_name_unlock(const string& tenant_id) const {
  const InstanceMetadataPB::TenantMetadataPB* tenant = GetTenantUnlock(tenant_id);
  return tenant ? tenant->tenant_name() : string("");
}

string FsManager::tenant_name(const string& tenant_id) const {
  const InstanceMetadataPB::TenantMetadataPB* tenant = GetTenant(tenant_id);
  return tenant ? tenant->tenant_name() : string("");
}

string FsManager::tenant_key_unlock(const string& tenant_id) const {
  const InstanceMetadataPB::TenantMetadataPB* tenant = GetTenantUnlock(tenant_id);
  return tenant ? tenant->tenant_key() : string("");
}

string FsManager::tenant_key(const string& tenant_id) const {
  const InstanceMetadataPB::TenantMetadataPB* tenant = GetTenant(tenant_id);
  return tenant ? tenant->tenant_key() : string("");
}

string FsManager::tenant_key_iv_unlock(const string& tenant_id) const {
  const InstanceMetadataPB::TenantMetadataPB* tenant = GetTenantUnlock(tenant_id);
  return tenant ? tenant->tenant_key_iv() : string("");
}

string FsManager::tenant_key_iv(const string& tenant_id) const {
  const InstanceMetadataPB::TenantMetadataPB* tenant = GetTenant(tenant_id);
  return tenant ? tenant->tenant_key_iv() : string("");
}

string FsManager::tenant_key_version_unlock(const string& tenant_id) const {
  const InstanceMetadataPB::TenantMetadataPB* tenant = GetTenantUnlock(tenant_id);
  return tenant ? tenant->tenant_key_version() : string("");
}

string FsManager::tenant_key_version(const string& tenant_id) const {
  const InstanceMetadataPB::TenantMetadataPB* tenant = GetTenant(tenant_id);
  return tenant ? tenant->tenant_key_version() : string("");
}

Status FsManager::AddTenant(const string& tenant_name,
                            const string& tenant_id,
                            optional<string> tenant_key,
                            optional<string> tenant_key_iv,
                            optional<string> tenant_key_version) {
  CHECK(FLAGS_enable_multi_tenancy);
  if (is_tenant_exist(tenant_id)) {
    return Status::AlreadyPresent(Substitute("Tenant $0 already exists.", tenant_id));
  }

  if ((!tenant_key || !tenant_key_iv || !tenant_key_version) && key_provider_) {
    // Generate tenant key info if missing something of tenant.
    RETURN_NOT_OK_PREPEND(key_provider_->GenerateTenantKey(tenant_id,
                                                           &(*tenant_key),
                                                           &(*tenant_key_iv),
                                                           &(*tenant_key_version)),
                          Substitute("Failed to generate encrypted tenant key for tenant: $0.",
                                     tenant_id));
  }

  // Update the metadata.
  RETURN_NOT_OK_PREPEND(AddTenantMetadata(tenant_name,
                                          tenant_id,
                                          *tenant_key,
                                          *tenant_key_iv,
                                          *tenant_key_version),
                        Substitute("Fail to update metadata for add tenant: $0.", tenant_id));

  // Make sure env is available for create dd manager.
  if (!AddEnv(tenant_id)) {
    return Status::Corruption(Substitute("Fail to add env for tenant with id: $0.", tenant_id));
  }
  RETURN_NOT_OK_PREPEND(SetEncryptionKey(tenant_id),
                        Substitute("Unable to set encryption key for tenant: $0.", tenant_id));

  // Create new dd manager and add the new dd manager to the dd manager map.
  //
  // TODO(kedeng):
  //     The new tenant should have its own dd manager instead of sharing the default tenant's
  // dd manager. This needs to be implemented along with having different storage paths for
  // different tenants.
  RETURN_NOT_OK_PREPEND(OpenDataDirManager(tenant_id),
                        Substitute("Unable to create and open data dir manager for tenant: $0.",
                                   tenant_id));

  // Create new block manager and add the new block manager to the block manager map.
  RETURN_NOT_OK_PREPEND(InitAndOpenBlockManager(nullptr,
                                                nullptr,
                                                nullptr,
                                                tenant_id),
                        Substitute("Unable to open block manager for tenant: $0.", tenant_id));

  return Status::OK();
}

Status FsManager::RemoveTenant(const string& tenant_id) {
  if (!VertifyTenant(tenant_id)) {
    return Status::NotSupported(
        Substitute("Not support for removing tenant for id: $0.", tenant_id));
  }

  if (tenant_id == fs::kDefaultTenantID) {
    return Status::NotSupported("Remove default tenant is not allowed.");
  }

  return RemoveTenantMetadata(tenant_id);
}

vector<string> FsManager::GetAllTenants() const {
  vector<string> tenant_ids;
  shared_lock<rw_spinlock> md_lock(metadata_rwlock_.get_lock());
  for (const auto& tdata : metadata_->tenants()) {
    tenant_ids.push_back(tdata.tenant_id());
  }

  return tenant_ids;
}

Env* FsManager::AddEnv(const std::string& tenant_id) {
  if (tenant_id == fs::kDefaultTenantID) {
    return env_;
  }

  std::lock_guard<LockType> lock(env_lock_);
  auto env = FindPtrOrNull(env_map_, tenant_id);
  if (env) {
    return env.get();
  }

  // Create new env and add the new env to the env map.
  LOG(INFO) << "Create new env for tenant: " << tenant_id;
  shared_ptr<Env> new_env = Env::NewSharedEnv();
  env_map_[tenant_id] = new_env;
  return new_env.get();
}

Env* FsManager::GetEnv(const std::string& tenant_id) const {
  if (tenant_id == fs::kDefaultTenantID) {
    return env_;
  }

  std::lock_guard<LockType> lock(env_lock_);
  auto env = FindPtrOrNull(env_map_, tenant_id);
  if (env) {
    return env.get();
  }

  LOG(ERROR) << "'The --enable_multi_tenancy' is " << FLAGS_enable_multi_tenancy
              << " for tenant: " << tenant_id << ", and we fail to search the env.";
  return nullptr;
}

scoped_refptr<DataDirManager> FsManager::dd_manager(const string& tenant_id) const {
  std::lock_guard<LockType> lock(ddm_lock_);
  scoped_refptr<DataDirManager> dd_manager(FindPtrOrNull(dd_manager_map_, tenant_id));
  return dd_manager;
}

vector<string> FsManager::GetDataRootDirs(const string& tenant_id) const {
  if (!VertifyTenant(tenant_id)) {
    LOG(ERROR) << "Unable to get data root dirs for non existing tenant: "
               << tenant_id << ", exit.";
    return {};
  }
  // Get the data subdirectory for each data root.
  return dd_manager(tenant_id)->GetDirs();
}

string FsManager::GetTabletMetadataDir() const {
  DCHECK(initted_);
  return JoinPathSegments(canonicalized_metadata_fs_root_.path, kTabletMetadataDirName);
}

string FsManager::GetTabletMetadataPath(const string& tablet_id) const {
  return JoinPathSegments(GetTabletMetadataDir(), tablet_id);
}

bool FsManager::IsValidTabletId(const string& fname) {
  // Prevent warning logs for hidden files or ./..
  if (PREDICT_FALSE(HasPrefixString(fname, "."))) {
    VLOG(1) << "Ignoring hidden file in tablet metadata dir: " << fname;
    return false;
  }

  string canonicalized_uuid;
  Status s = oid_generator_.Canonicalize(fname, &canonicalized_uuid);
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << "Ignoring file in tablet metadata dir: " << fname << ": " <<
                 s.message().ToString();
    return false;
  }

  if (PREDICT_FALSE(fname != canonicalized_uuid)) {
    LOG(WARNING) << "Ignoring file in tablet metadata dir: " << fname << ": " <<
                 Substitute("canonicalized uuid $0 does not match file name",
                            canonicalized_uuid);
    return false;
  }

  return true;
}

Status FsManager::ListTabletIds(vector<string>* tablet_ids) {
  string dir = GetTabletMetadataDir();
  vector<string> children;
  RETURN_NOT_OK_PREPEND(ListDir(dir, &children),
                        Substitute("Couldn't list tablets in metadata directory $0", dir));

  // Suppose all children are valid tablet metadata.
  tablet_ids->reserve(children.size());
  for (auto& child : children) {
    if (PREDICT_FALSE(!IsValidTabletId(child))) {
      continue;
    }
    tablet_ids->emplace_back(std::move(child));
  }
  return Status::OK();
}

string FsManager::GetInstanceMetadataPath(const string& root) const {
  return JoinPathSegments(root, kInstanceMetadataFileName);
}

string FsManager::GetTabletWalRecoveryDir(const string& tablet_id) const {
  string path = JoinPathSegments(GetWalsRootDir(), tablet_id);
  StrAppend(&path, kWalsRecoveryDirSuffix);
  return path;
}

string FsManager::GetWalSegmentFileName(const string& tablet_id,
                                        uint64_t sequence_number) const {
  return JoinPathSegments(GetTabletWalDir(tablet_id),
                          strings::Substitute("$0-$1",
                                              kWalFileNamePrefix,
                                              StringPrintf("%09" PRIu64, sequence_number)));
}

void FsManager::CleanTmpFiles() {
  DCHECK(!opts_.read_only);
  // Temporary files in the Block Manager directories are cleaned during
  // Block Manager startup.
  for (const auto& s : { GetWalsRootDir(),
                         GetTabletMetadataDir(),
                         GetConsensusMetadataDir() }) {
    WARN_NOT_OK(env_util::DeleteTmpFilesRecursively(GetEnv(), s),
                Substitute("Error deleting tmp files in $0", s));
  }
}

void FsManager::CheckAndFixPermissions() {
  for (const auto& root : canonicalized_all_fs_roots_) {
    if (!root.status.ok()) {
      continue;
    }
    WARN_NOT_OK(GetEnv()->EnsureFileModeAdheresToUmask(root.path),
                Substitute("could not check and fix permissions for path: $0",
                           root.path));
  }
}

scoped_refptr<BlockManager> FsManager::block_manager(const std::string& tenant_id) const {
  if (!VertifyTenant(tenant_id)) {
    LOG(ERROR) << "Do AddTenant for " << tenant_id
               << " first before calling 'block_manager()'.";
    return nullptr;
  }

  auto block_manager = SearchBlockManager(tenant_id);
  return block_manager;
}

// ==========================================================================
//  Dump/Debug utils
// ==========================================================================

void FsManager::DumpFileSystemTree(ostream& out, const string& tenant_id) {
  DCHECK(initted_);

  if (!VertifyTenant(tenant_id)) {
    LOG(ERROR) << "Unable to dump file system tree for non existing tenant: "
               << tenant_id << ", exit.";
    return;
  }

  for (const auto& root : canonicalized_all_fs_roots_) {
    if (!root.status.ok()) {
      continue;
    }
    out << "File-System Root: " << root.path << std::endl;

    vector<string> objects;
    Status s = GetEnv(tenant_id)->GetChildren(root.path, &objects);
    if (!s.ok()) {
      LOG(ERROR) << "Unable to list the fs-tree: " << s.ToString();
      return;
    }

    DumpFileSystemTree(out, "|-", root.path, objects);
  }
}

void FsManager::DumpFileSystemTree(ostream& out, const string& prefix,
                                   const string& path, const vector<string>& objects) {
  for (const string& name : objects) {
    if (name == "." || name == "..") continue;

    vector<string> sub_objects;
    string sub_path = JoinPathSegments(path, name);
    Status s = GetEnv()->GetChildren(sub_path, &sub_objects);
    if (s.ok()) {
      out << prefix << name << "/" << std::endl;
      DumpFileSystemTree(out, prefix + "---", sub_path, sub_objects);
    } else {
      out << prefix << name << std::endl;
    }
  }
}

Status FsManager::SetEncryptionKeyUnlock(const string& tenant_id) {
  // Set encryption key for tenant.
  if (is_tenant_exist(tenant_id) && key_provider_) {
    string tenant_key;
    RETURN_NOT_OK(key_provider_->DecryptEncryptionKey(tenant_key_unlock(tenant_id),
                                                      tenant_key_iv_unlock(tenant_id),
                                                      tenant_key_version_unlock(tenant_id),
                                                      &tenant_key));
    // 'tenant_key' is a hexadecimal string and SetEncryptionKey expects bits
    // (hex / 2 = bytes * 8 = bits).
    GetEnv(tenant_id)->SetEncryptionKey(reinterpret_cast<const uint8_t*>(
                                          a2b_hex(tenant_key).c_str()),
                                        tenant_key.length() * 4);
  }
  return Status::OK();
}

Status FsManager::SetEncryptionKey(const string& tenant_id) {
  shared_lock<rw_spinlock> md_lock(metadata_rwlock_.get_lock());
  return SetEncryptionKeyUnlock(tenant_id);
}

// ==========================================================================
//  Data read/write interfaces
// ==========================================================================

Status FsManager::CreateNewBlock(const CreateBlockOptions& opts,
                                 unique_ptr<WritableBlock>* block,
                                 const string& tenant_id) {
  if (!VertifyTenant(tenant_id)) {
    return Status::NotFound(Substitute("$0: tenant not found", tenant_id));
  }

  auto bm = block_manager(tenant_id);
  CHECK(!opts_.read_only);
  DCHECK(bm);
  return bm->CreateBlock(opts, block);
}

Status FsManager::OpenBlock(const BlockId& block_id,
                            unique_ptr<ReadableBlock>* block,
                            const string& tenant_id) {
  if (!VertifyTenant(tenant_id)) {
    return Status::NotFound(Substitute("$0: tenant not found", tenant_id));
  }

  auto bm = block_manager(tenant_id);
  DCHECK(bm);
  return bm->OpenBlock(block_id, block);
}

bool FsManager::BlockExists(const BlockId& block_id, const string& tenant_id) {
  if (!VertifyTenant(tenant_id)) {
    return false;
  }

  auto bm = block_manager(tenant_id);
  DCHECK(bm);
  unique_ptr<ReadableBlock> block;
  return bm->OpenBlock(block_id, &block).ok();
}

} // namespace kudu
