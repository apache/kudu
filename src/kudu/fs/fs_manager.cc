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

#include <algorithm>
#include <cinttypes>
#include <ctime>
#include <initializer_list>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/file_block_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_report.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"

DEFINE_bool(enable_data_block_fsync, true,
            "Whether to enable fsync() of data blocks, metadata, and their parent directories. "
            "Disabling this flag may cause data loss in the event of a system crash.");
TAG_FLAG(enable_data_block_fsync, unsafe);

#if defined(__linux__)
DEFINE_string(block_manager, "log", "Which block manager to use for storage. "
              "Valid options are 'file' and 'log'. The file block manager is not suitable for "
              "production use due to scaling limitations.");
#else
DEFINE_string(block_manager, "file", "Which block manager to use for storage. "
              "Only the file block manager is supported for non-Linux systems.");
#endif
static bool ValidateBlockManagerType(const char* /*flagname*/, const std::string& value) {
  for (const std::string& type : kudu::fs::BlockManager::block_manager_types()) {
    if (type == value) return true;
  }
  return false;
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

using kudu::fs::BlockManagerOptions;
using kudu::fs::CreateBlockOptions;
using kudu::fs::DataDirManager;
using kudu::fs::DataDirManagerOptions;
using kudu::fs::ErrorHandlerType;
using kudu::fs::ErrorNotificationCb;
using kudu::fs::FsErrorManager;
using kudu::fs::FileBlockManager;
using kudu::fs::FsReport;
using kudu::fs::LogBlockManager;
using kudu::fs::ReadableBlock;
using kudu::fs::UpdateInstanceBehavior;
using kudu::fs::WritableBlock;
using kudu::pb_util::SecureDebugString;
using std::ostream;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
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
    update_instances(UpdateInstanceBehavior::UPDATE_AND_IGNORE_FAILURES) {
  data_roots = strings::Split(FLAGS_fs_data_dirs, ",", strings::SkipEmpty());
}

FsManagerOpts::FsManagerOpts(const string& root)
  : wal_root(root),
    data_roots({ root }),
    block_manager_type(FLAGS_block_manager),
    read_only(false),
    update_instances(UpdateInstanceBehavior::UPDATE_AND_IGNORE_FAILURES) {}

FsManager::FsManager(Env* env, const string& root_path)
  : env_(DCHECK_NOTNULL(env)),
    opts_(FsManagerOpts(root_path)),
    error_manager_(new FsErrorManager()),
    initted_(false) {}

FsManager::FsManager(Env* env, FsManagerOpts opts)
  : env_(DCHECK_NOTNULL(env)),
    opts_(std::move(opts)),
    error_manager_(new FsErrorManager()),
    initted_(false) {
DCHECK(opts_.update_instances == UpdateInstanceBehavior::DONT_UPDATE ||
       !opts_.read_only) << "FsManager can only be for updated if not in read-only mode";
}

FsManager::~FsManager() {}

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
    Status s = env_->Canonicalize(DirName(root), &canonicalized);
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
    if (env_->FileExists(meta_dir_in_data_root)) {
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

void FsManager::InitBlockManager() {
  BlockManagerOptions bm_opts;
  bm_opts.metric_entity = opts_.metric_entity;
  bm_opts.parent_mem_tracker = opts_.parent_mem_tracker;
  bm_opts.read_only = opts_.read_only;
  if (opts_.block_manager_type == "file") {
    block_manager_.reset(new FileBlockManager(
        env_, dd_manager_.get(), error_manager_.get(), std::move(bm_opts)));
  } else {
    block_manager_.reset(new LogBlockManager(
        env_, dd_manager_.get(), error_manager_.get(), std::move(bm_opts)));
  }
}

Status FsManager::PartialOpen(CanonicalizedRootsList* missing_roots) {
  RETURN_NOT_OK(Init());

  for (auto& root : canonicalized_all_fs_roots_) {
    if (!root.status.ok()) {
      continue;
    }
    unique_ptr<InstanceMetadataPB> pb(new InstanceMetadataPB);
    Status s = pb_util::ReadPBContainerFromPath(env_, GetInstanceMetadataPath(root.path),
                                                pb.get());
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
    } else if (pb->uuid() != metadata_->uuid()) {
      return Status::Corruption(Substitute(
          "Mismatched UUIDs across filesystem roots: $0 vs. $1; configuring "
          "multiple Kudu processes with the same directory is not supported",
          metadata_->uuid(), pb->uuid()));
    }
  }

  if (!metadata_) {
    return Status::NotFound("could not find a healthy instance file");
  }
  return Status::OK();
}

Status FsManager::Open(FsReport* report) {
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
    RETURN_NOT_OK_PREPEND(env_->IsDirectory(d, &is_dir),
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
      WARN_NOT_OK(env_->DeleteFile(f), "Could not delete file " + f);
    }
    // Delete directories in reverse order since parent directories will have
    // been added before child directories.
    for (auto it = created_dirs.rbegin(); it != created_dirs.rend(); it++) {
      WARN_NOT_OK(env_->DeleteDir(*it), "Could not delete dir " + *it);
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

  // Open the directory manager if it has not been opened already.
  if (!dd_manager_) {
    DataDirManagerOptions dm_opts;
    dm_opts.metric_entity = opts_.metric_entity;
    dm_opts.read_only = opts_.read_only;
    dm_opts.dir_type = opts_.block_manager_type;
    dm_opts.update_instances = opts_.update_instances;
    LOG_TIMING(INFO, "opening directory manager") {
      RETURN_NOT_OK(DataDirManager::OpenExisting(env_,
          canonicalized_data_fs_roots_, dm_opts, &dd_manager_));
    }
  }

  // Only clean temporary files after the data dir manager successfully opened.
  // This ensures that we were able to obtain the exclusive directory locks
  // on the data directories before we start deleting files.
  if (!opts_.read_only) {
    CleanTmpFiles();
    CheckAndFixPermissions();
  }

  // Set an initial error handler to mark data directories as failed.
  error_manager_->SetErrorNotificationCb(ErrorHandlerType::DISK_ERROR,
      Bind(&DataDirManager::MarkDirFailedByUuid, Unretained(dd_manager_.get())));

  // Finally, initialize and open the block manager.
  InitBlockManager();
  LOG_TIMING(INFO, "opening block manager") {
    RETURN_NOT_OK(block_manager_->Open(report));
  }

  // Report wal and metadata directories.
  if (report) {
    report->wal_dir = canonicalized_wal_fs_root_.path;
    report->metadata_dir = canonicalized_metadata_fs_root_.path;
  }

  if (FLAGS_enable_data_block_fsync) {
    // Files/directories created by the directory manager in the fs roots have
    // been synchronized, so now is a good time to sync the roots themselves.
    WARN_NOT_OK(env_util::SyncAllParentDirs(env_, created_dirs, created_files),
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

Status FsManager::CreateInitialFileSystemLayout(boost::optional<string> uuid) {
  CHECK(!opts_.read_only);

  RETURN_NOT_OK(Init());

  // In the event of failure, delete everything we created.
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

  // Create the filesystem roots.
  //
  // Files/directories created will NOT be synchronized to disk.
  InstanceMetadataPB metadata;
  RETURN_NOT_OK_PREPEND(CreateInstanceMetadata(std::move(uuid), &metadata),
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
    RETURN_NOT_OK_PREPEND(env_util::CreateDirIfMissing(env_, dir, &created),
                          Substitute("Unable to create directory $0", dir));
    if (created) {
      created_dirs.emplace_back(dir);
    }
  }

  // Create the directory manager.
  //
  // All files/directories created will be synchronized to disk.
  DataDirManagerOptions dm_opts;
  dm_opts.metric_entity = opts_.metric_entity;
  dm_opts.read_only = opts_.read_only;
  LOG_TIMING(INFO, "creating directory manager") {
    RETURN_NOT_OK_PREPEND(DataDirManager::CreateNew(
        env_, canonicalized_data_fs_roots_, dm_opts, &dd_manager_),
                          "Unable to create directory manager");
  }

  if (FLAGS_enable_data_block_fsync) {
    // Files/directories created by the directory manager in the fs roots have
    // been synchronized, so now is a good time to sync the roots themselves.
    WARN_NOT_OK(env_util::SyncAllParentDirs(env_, created_dirs, created_files),
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
    if (!env_->FileExists(root.path)) {
      // We'll create the directory below.
      continue;
    }
    bool is_empty;
    RETURN_NOT_OK_PREPEND(env_util::IsDirectoryEmpty(env_, root.path, &is_empty),
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
    RETURN_NOT_OK_PREPEND(env_util::CreateDirIfMissing(env_, root_name, &created),
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

Status FsManager::CreateInstanceMetadata(boost::optional<string> uuid,
                                         InstanceMetadataPB* metadata) {
  if (uuid) {
    string canonicalized_uuid;
    RETURN_NOT_OK(oid_generator_.Canonicalize(uuid.get(), &canonicalized_uuid));
    metadata->set_uuid(canonicalized_uuid);
  } else {
    metadata->set_uuid(oid_generator_.Next());
  }

  string time_str;
  StringAppendStrftime(&time_str, "%Y-%m-%d %H:%M:%S", time(nullptr), false);
  string hostname;
  if (!GetHostname(&hostname).ok()) {
    hostname = "<unknown host>";
  }
  metadata->set_format_stamp(Substitute("Formatted at $0 on $1", time_str, hostname));
  return Status::OK();
}

Status FsManager::WriteInstanceMetadata(const InstanceMetadataPB& metadata,
                                        const string& root) {
  const string path = GetInstanceMetadataPath(root);

  // The instance metadata is written effectively once per TS, so the
  // durability cost is negligible.
  RETURN_NOT_OK(pb_util::WritePBContainerToPath(env_, path,
                                                metadata,
                                                pb_util::NO_OVERWRITE,
                                                pb_util::SYNC));
  LOG(INFO) << "Generated new instance metadata in path " << path << ":\n"
            << SecureDebugString(metadata);
  return Status::OK();
}

const string& FsManager::uuid() const {
  return CHECK_NOTNULL(metadata_.get())->uuid();
}

vector<string> FsManager::GetDataRootDirs() const {
  // Get the data subdirectory for each data root.
  return dd_manager_->GetDirs();
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
  if (HasPrefixString(fname, ".")) {
    VLOG(1) << "Ignoring hidden file in tablet metadata dir: " << fname;
    return false;
  }

  string canonicalized_uuid;
  Status s = oid_generator_.Canonicalize(fname, &canonicalized_uuid);

  if (!s.ok()) {
    LOG(WARNING) << "Ignoring file in tablet metadata dir: " << fname << ": " <<
                 s.message().ToString();
    return false;
  }

  if (fname != canonicalized_uuid) {
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

  vector<string> tablets;
  for (const string& child : children) {
    if (!IsValidTabletId(child)) {
      continue;
    }
    tablet_ids->push_back(child);
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
    WARN_NOT_OK(env_util::DeleteTmpFilesRecursively(env_, s),
                Substitute("Error deleting tmp files in $0", s));
  }
}

void FsManager::CheckAndFixPermissions() {
  for (const auto& root : canonicalized_all_fs_roots_) {
    if (!root.status.ok()) {
      continue;
    }
    WARN_NOT_OK(env_->EnsureFileModeAdheresToUmask(root.path),
                Substitute("could not check and fix permissions for path: $0",
                           root.path));
  }
}

// ==========================================================================
//  Dump/Debug utils
// ==========================================================================

void FsManager::DumpFileSystemTree(ostream& out) {
  DCHECK(initted_);

  for (const auto& root : canonicalized_all_fs_roots_) {
    if (!root.status.ok()) {
      continue;
    }
    out << "File-System Root: " << root.path << std::endl;

    vector<string> objects;
    Status s = env_->GetChildren(root.path, &objects);
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
    Status s = env_->GetChildren(sub_path, &sub_objects);
    if (s.ok()) {
      out << prefix << name << "/" << std::endl;
      DumpFileSystemTree(out, prefix + "---", sub_path, sub_objects);
    } else {
      out << prefix << name << std::endl;
    }
  }
}

// ==========================================================================
//  Data read/write interfaces
// ==========================================================================

Status FsManager::CreateNewBlock(const CreateBlockOptions& opts, unique_ptr<WritableBlock>* block) {
  CHECK(!opts_.read_only);

  return block_manager_->CreateBlock(opts, block);
}

Status FsManager::OpenBlock(const BlockId& block_id, unique_ptr<ReadableBlock>* block) {
  return block_manager_->OpenBlock(block_id, block);
}

bool FsManager::BlockExists(const BlockId& block_id) const {
  unique_ptr<ReadableBlock> block;
  return block_manager_->OpenBlock(block_id, &block).ok();
}

std::ostream& operator<<(std::ostream& o, const BlockId& block_id) {
  return o << block_id.ToString();
}

} // namespace kudu
