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
#include <deque>
#include <iostream>
#include <map>
#include <set>
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
#include "kudu/fs/log_block_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
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
#include "kudu/util/stopwatch.h"
#include "kudu/util/website_util.h"

DEFINE_bool(enable_data_block_fsync, true,
            "Whether to enable fsync() of data blocks, metadata, and their parent directories. "
            "Disabling this flag may cause data loss in the event of a system crash.");
TAG_FLAG(enable_data_block_fsync, unsafe);

#if defined(__linux__)
DEFINE_string(block_manager, "log", "Which block manager to use for storage. "
              "Valid options are 'file' and 'log'.");
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

using kudu::env_util::ScopedFileDeleter;
using kudu::fs::BlockManagerOptions;
using kudu::fs::CreateBlockOptions;
using kudu::fs::DataDirManager;
using kudu::fs::DataDirManagerOptions;
using kudu::fs::FsErrorManager;
using kudu::fs::FileBlockManager;
using kudu::fs::FsReport;
using kudu::fs::LogBlockManager;
using kudu::fs::ReadableBlock;
using kudu::fs::WritableBlock;
using kudu::pb_util::SecureDebugString;
using std::map;
using std::ostream;
using std::set;
using std::string;
using std::unique_ptr;
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
const char *FsManager::kCorruptedSuffix = ".corrupted";
const char *FsManager::kInstanceMetadataFileName = "instance";
const char *FsManager::kConsensusMetadataDirName = "consensus-meta";

FsManagerOpts::FsManagerOpts()
  : wal_path(FLAGS_fs_wal_dir),
    block_manager_type(FLAGS_block_manager),
    read_only(false) {
  data_paths = strings::Split(FLAGS_fs_data_dirs, ",", strings::SkipEmpty());
}

FsManagerOpts::~FsManagerOpts() {
}

FsManager::FsManager(Env* env, const string& root_path)
  : env_(DCHECK_NOTNULL(env)),
    read_only_(false),
    block_manager_type_(FLAGS_block_manager),
    wal_fs_root_(root_path),
    data_fs_roots_({ root_path }),
    metric_entity_(nullptr),
    error_manager_(new FsErrorManager()),
    initted_(false) {
}

FsManager::FsManager(Env* env,
                     const FsManagerOpts& opts)
  : env_(DCHECK_NOTNULL(env)),
    read_only_(opts.read_only),
    block_manager_type_(opts.block_manager_type),
    wal_fs_root_(opts.wal_path),
    data_fs_roots_(opts.data_paths),
    metric_entity_(opts.metric_entity),
    parent_mem_tracker_(opts.parent_mem_tracker),
    error_manager_(new FsErrorManager()),
    initted_(false) {
}

FsManager::~FsManager() {
}

void FsManager::SetErrorNotificationCb(fs::ErrorNotificationCb cb) {
  error_manager_->SetErrorNotificationCb(std::move(cb));
}

void FsManager::UnsetErrorNotificationCb() {
  error_manager_->UnsetErrorNotificationCb();
}

Status FsManager::Init() {
  if (initted_) {
    return Status::OK();
  }

  // The wal root must be set.
  if (wal_fs_root_.empty()) {
    return Status::IOError("Write-ahead log directory (fs_wal_dir) not provided");
  }

  // Deduplicate all of the roots.
  set<string> all_roots = { wal_fs_root_ };
  all_roots.insert(data_fs_roots_.begin(), data_fs_roots_.end());

  // Build a map of original root --> canonicalized root, sanitizing each
  // root as we go and storing the canonicalization status.
  typedef map<string, CanonicalizedRootAndStatus> RootMap;
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
      if (s.IsDiskFailure()) {
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
  canonicalized_wal_fs_root_ = FindOrDie(canonicalized_roots, wal_fs_root_);
  if (!data_fs_roots_.empty()) {
    set<string> unique_roots;
    for (const string& data_fs_root : data_fs_roots_) {
      const auto& root = FindOrDie(canonicalized_roots, data_fs_root);
      if (InsertIfNotPresent(&unique_roots, root.path)) {
        canonicalized_data_fs_roots_.emplace_back(root);
        canonicalized_all_fs_roots_.emplace_back(root);
      }
    }
    canonicalized_metadata_fs_root_ = FindOrDie(canonicalized_roots, data_fs_roots_[0]);
    if (!ContainsKey(unique_roots, canonicalized_wal_fs_root_.path)) {
      canonicalized_all_fs_roots_.emplace_back(canonicalized_wal_fs_root_);
    }
  } else {
    LOG(INFO) << "Data directories (fs_data_dirs) not provided";
    LOG(INFO) << "Using write-ahead log directory (fs_wal_dir) as data directory";
    canonicalized_metadata_fs_root_ = canonicalized_wal_fs_root_;
    canonicalized_data_fs_roots_.emplace_back(canonicalized_wal_fs_root_);
    canonicalized_all_fs_roots_.emplace_back(canonicalized_wal_fs_root_);
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
  BlockManagerOptions opts;
  opts.metric_entity = metric_entity_;
  opts.parent_mem_tracker = parent_mem_tracker_;
  opts.read_only = read_only_;
  if (block_manager_type_ == "file") {
    block_manager_.reset(new FileBlockManager(env_, dd_manager_.get(), error_manager_.get(), opts));
  } else {
    block_manager_.reset(new LogBlockManager(env_, dd_manager_.get(), error_manager_.get(), opts));
  }
}

Status FsManager::Open(FsReport* report) {
  RETURN_NOT_OK(Init());

  // Load and verify the instance metadata files.
  //
  // Done first to minimize side effects in the case that the configured roots
  // are not yet initialized on disk.
  for (auto& root : canonicalized_all_fs_roots_) {
    if (!root.status.ok()) {
      continue;
    }
    gscoped_ptr<InstanceMetadataPB> pb(new InstanceMetadataPB);
    Status s = pb_util::ReadPBContainerFromPath(env_, GetInstanceMetadataPath(root.path),
                                                pb.get());
    if (PREDICT_FALSE(!s.ok())) {
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

  // Remove leftover temporary files from the WAL root and fix permissions.
  //
  // Temporary files in the data directory roots will be removed by the block
  // manager.
  if (!read_only_) {
    CleanTmpFiles();
    CheckAndFixPermissions();
  }

  // Open the directory manager if it has not been opened already.
  if (!dd_manager_) {
    DataDirManagerOptions dm_opts;
    dm_opts.metric_entity = metric_entity_;
    dm_opts.block_manager_type = block_manager_type_;
    dm_opts.read_only = read_only_;
    LOG_TIMING(INFO, "opening directory manager") {
      RETURN_NOT_OK(DataDirManager::OpenExisting(env_,
          canonicalized_data_fs_roots_, dm_opts, &dd_manager_));
    }
  }

  // Set an initial error handler to mark data directories as failed.
  error_manager_->SetErrorNotificationCb(Bind(&DataDirManager::MarkDataDirFailedByUuid,
                                              Unretained(dd_manager_.get())));

  // Finally, initialize and open the block manager.
  InitBlockManager();
  LOG_TIMING(INFO, "opening block manager") {
    RETURN_NOT_OK(block_manager_->Open(report));
  }

  // Before returning, ensure the metadata directory has not failed.
  // TODO(awong): avoid this single point of failure by spreading metadata
  // across directories.
  uint16_t metadata_idx;
  CHECK(dd_manager_->FindUuidIndexByRoot(canonicalized_metadata_fs_root_.path, &metadata_idx));
  if (ContainsKey(dd_manager_->GetFailedDataDirs(), metadata_idx)) {
    return Status::IOError("Cannot open the FS layout; metadata directory failed");
  }

  LOG(INFO) << "Opened local filesystem: " <<
    JoinStrings(DataDirManager::GetRootNames(canonicalized_all_fs_roots_), ",")
            << std::endl << SecureDebugString(*metadata_);
  return Status::OK();
}

Status FsManager::CreateInitialFileSystemLayout(boost::optional<string> uuid) {
  CHECK(!read_only_);

  RETURN_NOT_OK(Init());

  // It's OK if a root already exists as long as there's nothing in it.
  for (const auto& root : canonicalized_all_fs_roots_) {
    if (!root.status.ok()) {
      return Status::IOError("Cannot create FS layout; at least one directory "
                             "failed to canonicalize", root.path);
    }
    if (!env_->FileExists(root.path)) {
      // We'll create the directory below.
      continue;
    }
    bool is_empty;
    RETURN_NOT_OK_PREPEND(IsDirectoryEmpty(root.path, &is_empty),
                          "Unable to check if FSManager root is empty");
    if (!is_empty) {
      return Status::AlreadyPresent(
          Substitute("FSManager root is not empty. See $0", KuduDocsTroubleshootingUrl()),
          root.path);
    }
  }

  // All roots are either empty or non-existent. Create missing roots and all
  // subdirectories.
  //
  // In the event of failure, delete everything we created.
  std::deque<ScopedFileDeleter*> delete_on_failure;
  ElementDeleter d(&delete_on_failure);

  InstanceMetadataPB metadata;
  RETURN_NOT_OK(CreateInstanceMetadata(std::move(uuid), &metadata));
  unordered_set<string> to_sync;
  for (const auto& root : canonicalized_all_fs_roots_) {
    if (!root.status.ok()) {
      continue;
    }
    string root_name = root.path;
    bool created;
    RETURN_NOT_OK_PREPEND(CreateDirIfMissing(root_name, &created),
                          "Unable to create FSManager root");
    if (created) {
      delete_on_failure.push_front(new ScopedFileDeleter(env_, root_name));
      to_sync.insert(DirName(root_name));
    }
    RETURN_NOT_OK_PREPEND(WriteInstanceMetadata(metadata, root_name),
                          "Unable to write instance metadata");
    delete_on_failure.push_front(new ScopedFileDeleter(
        env_, GetInstanceMetadataPath(root_name)));
  }

  // Initialize ancillary directories.
  vector<string> ancillary_dirs = { GetWalsRootDir(),
                                    GetTabletMetadataDir(),
                                    GetConsensusMetadataDir() };
  for (const string& dir : ancillary_dirs) {
    bool created;
    RETURN_NOT_OK_PREPEND(CreateDirIfMissing(dir, &created),
                          Substitute("Unable to create directory $0", dir));
    if (created) {
      delete_on_failure.push_front(new ScopedFileDeleter(env_, dir));
      to_sync.insert(DirName(dir));
    }
  }

  // Ensure newly created directories are synchronized to disk.
  if (FLAGS_enable_data_block_fsync) {
    for (const string& dir : to_sync) {
      RETURN_NOT_OK_PREPEND(env_->SyncDir(dir),
                            Substitute("Unable to synchronize directory $0", dir));
    }
  }

  // And lastly, create the directory manager.
  DataDirManagerOptions opts;
  opts.metric_entity = metric_entity_;
  opts.read_only = read_only_;
  LOG_TIMING(INFO, "creating directory manager") {
    RETURN_NOT_OK_PREPEND(DataDirManager::CreateNew(env_,
        canonicalized_data_fs_roots_, opts, &dd_manager_), "Unable to create directory manager");
  }

  // Success: don't delete any files.
  for (ScopedFileDeleter* deleter : delete_on_failure) {
    deleter->Cancel();
  }
  return Status::OK();
}

Status FsManager::CreateInstanceMetadata(boost::optional<string> uuid,
                                         InstanceMetadataPB* metadata) {
  ObjectIdGenerator oid_generator;
  if (uuid) {
    string canonicalized_uuid;
    RETURN_NOT_OK(oid_generator.Canonicalize(uuid.get(), &canonicalized_uuid));
    metadata->set_uuid(canonicalized_uuid);
  } else {
    metadata->set_uuid(oid_generator.Next());
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

Status FsManager::IsDirectoryEmpty(const string& path, bool* is_empty) {
  vector<string> children;
  RETURN_NOT_OK(env_->GetChildren(path, &children));
  for (const string& child : children) {
    if (child == "." || child == "..") {
      continue;
    } else {
      *is_empty = false;
      return Status::OK();
    }
  }
  *is_empty = true;
  return Status::OK();
}

Status FsManager::CreateDirIfMissing(const string& path, bool* created) {
  return env_util::CreateDirIfMissing(env_, path, created);
}

const string& FsManager::uuid() const {
  return CHECK_NOTNULL(metadata_.get())->uuid();
}

vector<string> FsManager::GetDataRootDirs() const {
  // Get the data subdirectory for each data root.
  return dd_manager_->GetDataDirs();
}

string FsManager::GetTabletMetadataDir() const {
  DCHECK(initted_);
  return JoinPathSegments(canonicalized_metadata_fs_root_.path, kTabletMetadataDirName);
}

string FsManager::GetTabletMetadataPath(const string& tablet_id) const {
  return JoinPathSegments(GetTabletMetadataDir(), tablet_id);
}

namespace {
// Return true if 'fname' is a valid tablet ID.
bool IsValidTabletId(const string& fname) {
  if (fname.find(kTmpInfix) != string::npos ||
      fname.find(kOldTmpInfix) != string::npos) {
    LOG(WARNING) << "Ignoring tmp file in tablet metadata dir: " << fname;
    return false;
  }

  if (HasPrefixString(fname, ".")) {
    // Hidden file or ./..
    VLOG(1) << "Ignoring hidden file in tablet metadata dir: " << fname;
    return false;
  }

  return true;
}
} // anonymous namespace

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
  DCHECK(!read_only_);
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
  CHECK(!read_only_);

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
