// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/fs/fs_manager.h"

#include <iostream>
#include <map>
#include <tr1/memory>
#include <tr1/unordered_set>

#include <boost/foreach.hpp>
#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <google/protobuf/message.h>

#include "kudu/fs/block_id.h"
#include "kudu/fs/block_id-inl.h"
#include "kudu/fs/file_block_manager.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/strtoint.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/env_util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"

DEFINE_bool(enable_data_block_fsync, true,
            "Whether to enable fsync() of data blocks, metadata, and their parent directories. "
            "Disabling this flag may cause data loss in the event of a system crash.");

DEFINE_string(block_manager, "log", "Which block manager to use for storage. "
              "Valid options are 'file' and 'log'.");

using google::protobuf::Message;
using strings::Substitute;
using std::map;
using std::tr1::shared_ptr;
using std::tr1::unordered_set;
using kudu::fs::CreateBlockOptions;
using kudu::fs::FileBlockManager;
using kudu::fs::LogBlockManager;
using kudu::fs::ReadableBlock;
using kudu::fs::WritableBlock;

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

static const char* const kTmpSuffix = ".tmp";

FsManager::FsManager(Env* env, const string& root_path)
  : env_(env),
    wal_fs_root_(root_path),
    data_fs_roots_(boost::assign::list_of(root_path).convert_to_container<vector<string> >()),
    metric_entity_(NULL),
    initted_(false) {
}

FsManager::FsManager(Env* env,
                     const scoped_refptr<MetricEntity>& metric_entity,
                     const string& wal_path,
                     const vector<string>& data_paths)
  : env_(env),
    wal_fs_root_(wal_path),
    data_fs_roots_(data_paths),
    metric_entity_(metric_entity),
    initted_(false) {
}

FsManager::~FsManager() {
}

Status FsManager::Init() {
  if (initted_) {
    return Status::OK();
  }

  // Deduplicate all of the roots.
  set<string> all_roots;
  all_roots.insert(wal_fs_root_);
  BOOST_FOREACH(const string& data_fs_root, data_fs_roots_) {
    all_roots.insert(data_fs_root);
  }

  // Build a map of original root --> canonicalized root, sanitizing each
  // root a bit as we go.
  typedef map<string, string> RootMap;
  RootMap canonicalized_roots;
  BOOST_FOREACH(const string& root, all_roots) {
    if (root.empty()) {
      return Status::IOError("Empty string provided for filesystem root");
    }
    if (root[0] != '/') {
      return Status::IOError(
          Substitute("Relative path $0 provided for filesystem root", root));
    }
    {
      string root_copy = root;
      StripWhiteSpace(&root_copy);
      if (root != root_copy) {
        return Status::IOError(
                  Substitute("Filesystem root $0 contains illegal whitespace", root));
      }
    }

    // Strip the basename when canonicalizing, as it may not exist. The
    // dirname, however, must exist.
    string canonicalized;
    RETURN_NOT_OK(env_->Canonicalize(DirName(root), &canonicalized));
    canonicalized = JoinPathSegments(canonicalized, BaseName(root));
    InsertOrDie(&canonicalized_roots, root, canonicalized);
  }

  // All done, use the map to set the canonicalized state.
  canonicalized_wal_fs_root_ = FindOrDie(canonicalized_roots, wal_fs_root_);
  canonicalized_metadata_fs_root_ = FindOrDie(canonicalized_roots, data_fs_roots_[0]);
  BOOST_FOREACH(const string& data_fs_root, data_fs_roots_) {
    canonicalized_data_fs_roots_.insert(FindOrDie(canonicalized_roots, data_fs_root));
  }
  BOOST_FOREACH(const RootMap::value_type& e, canonicalized_roots) {
    canonicalized_all_fs_roots_.insert(e.second);
  }

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "WAL root: " << canonicalized_wal_fs_root_;
    VLOG(1) << "Metadata root: " << canonicalized_metadata_fs_root_;
    VLOG(1) << "Data roots: " << canonicalized_data_fs_roots_;
    VLOG(1) << "All roots: " << canonicalized_all_fs_roots_;
  }
  initted_ = true;
  return Status::OK();
}

void FsManager::InitBlockManager() {
  if (FLAGS_block_manager == "file") {
    block_manager_.reset(new FileBlockManager(env_, metric_entity_, GetDataRootDirs()));
  } else if (FLAGS_block_manager == "log") {
    block_manager_.reset(new LogBlockManager(env_, metric_entity_, GetDataRootDirs()));
  } else {
    LOG(FATAL) << "Invalid block manager: " << FLAGS_block_manager;
  }
}

Status FsManager::Open() {
  RETURN_NOT_OK(Init());
  BOOST_FOREACH(const string& root, canonicalized_all_fs_roots_) {
    gscoped_ptr<InstanceMetadataPB> pb(new InstanceMetadataPB);
    RETURN_NOT_OK(pb_util::ReadPBContainerFromPath(env_, GetInstanceMetadataPath(root),
                                                   pb.get()));
    if (!metadata_) {
      metadata_.reset(pb.release());
    } else if (pb->uuid() != metadata_->uuid()) {
      return Status::Corruption(Substitute(
          "Mismatched UUIDs across filesystem roots: $0 vs. $1",
          metadata_->uuid(), pb->uuid()));
    }
  }

  InitBlockManager();
  RETURN_NOT_OK(block_manager_->Open());
  LOG(INFO) << "Opened local filesystem: " << JoinStrings(canonicalized_all_fs_roots_, ",")
            << std::endl << metadata_->DebugString();
  return Status::OK();
}

Status FsManager::CreateInitialFileSystemLayout() {
  RETURN_NOT_OK(Init());

  // Initialize each root dir.
  //
  // It's OK if a root already exists as long as there's no instance
  // metadata. However, no subdirectories should exist.
  InstanceMetadataPB metadata;
  CreateInstanceMetadata(&metadata);
  BOOST_FOREACH(const string& root, canonicalized_all_fs_roots_) {
    RETURN_NOT_OK_PREPEND(CreateDirIfMissing(root),
                          "Unable to create FSManager root");

    if (env_->FileExists(GetInstanceMetadataPath(root))) {
      return Status::AlreadyPresent("Instance metadata already present", root);
    } else {
      RETURN_NOT_OK_PREPEND(WriteInstanceMetadata(metadata, root),
                            "Unable to write instance metadata");
    }
  }

  // Initialize wals dir.
  RETURN_NOT_OK_PREPEND(env_->CreateDir(GetWalsRootDir()),
                        "Unable to create WAL directory");

  // Initialize tablet metadata directory.
  RETURN_NOT_OK_PREPEND(env_->CreateDir(GetTabletMetadataDir()),
                        "Unable to create tablet metadata directory");

  // Initialize consensus metadata dir.
  RETURN_NOT_OK_PREPEND(env_->CreateDir(GetConsensusMetadataDir()),
                        "Unable to create consensus metadata directory");

  InitBlockManager();
  RETURN_NOT_OK_PREPEND(block_manager_->Create(), "Unable to create block manager");
  return Status::OK();
}

void FsManager::CreateInstanceMetadata(InstanceMetadataPB* metadata) {
  ObjectIdGenerator oid_generator;
  metadata->set_uuid(oid_generator.Next());

  string time_str;
  StringAppendStrftime(&time_str, "%Y-%m-%d %H:%M:%S", time(NULL), false);
  string hostname;
  if (!GetHostname(&hostname).ok()) {
    hostname = "<unknown host>";
  }
  metadata->set_format_stamp(Substitute("Formatted at $0 on $1", time_str, hostname));
}

Status FsManager::WriteInstanceMetadata(const InstanceMetadataPB& metadata,
                                        const string& root) {
  const string path = GetInstanceMetadataPath(root);

  // The instance metadata is written effectively once per TS, so the
  // durability cost is negligible.
  RETURN_NOT_OK(pb_util::WritePBContainerToPath(env_, path,
                                                metadata, pb_util::SYNC));
  LOG(INFO) << "Generated new instance metadata in path " << path << ":\n"
            << metadata.DebugString();
  return Status::OK();
}

const string& FsManager::uuid() const {
  return CHECK_NOTNULL(metadata_.get())->uuid();
}

vector<string> FsManager::GetDataRootDirs() const {
  DCHECK(initted_);

  // Add the data subdirectory to each data root.
  std::vector<std::string> data_paths;
  BOOST_FOREACH(const string& data_fs_root, canonicalized_data_fs_roots_) {
    data_paths.push_back(JoinPathSegments(data_fs_root, kDataDirName));
  }
  return data_paths;
}

string FsManager::GetTabletMetadataDir() const {
  DCHECK(initted_);
  return JoinPathSegments(canonicalized_metadata_fs_root_, kTabletMetadataDirName);
}

string FsManager::GetTabletMetadataPath(const string& tablet_id) const {
  return JoinPathSegments(GetTabletMetadataDir(), tablet_id);
}

namespace {
// Return true if 'fname' is a valid tablet ID.
bool IsValidTabletId(const std::string& fname) {
  if (HasSuffixString(fname, kTmpSuffix)) {
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
  BOOST_FOREACH(const string& child, children) {
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
                                              StringPrintf("%09lu", sequence_number)));
}


// ==========================================================================
//  Dump/Debug utils
// ==========================================================================

void FsManager::DumpFileSystemTree(ostream& out) {
  DCHECK(initted_);

  BOOST_FOREACH(const string& root, canonicalized_all_fs_roots_) {
    out << "File-System Root: " << root << std::endl;

    std::vector<string> objects;
    Status s = env_->GetChildren(root, &objects);
    if (!s.ok()) {
      LOG(ERROR) << "Unable to list the fs-tree: " << s.ToString();
      return;
    }

    DumpFileSystemTree(out, "|-", root, objects);
  }
}

void FsManager::DumpFileSystemTree(ostream& out, const string& prefix,
                                   const string& path, const vector<string>& objects) {
  BOOST_FOREACH(const string& name, objects) {
    if (name == "." || name == "..") continue;

    std::vector<string> sub_objects;
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

Status FsManager::CreateNewBlock(gscoped_ptr<WritableBlock>* block) {
  return block_manager_->CreateBlock(block);
}

Status FsManager::OpenBlock(const BlockId& block_id, gscoped_ptr<ReadableBlock>* block) {
  return block_manager_->OpenBlock(block_id, block);
}

Status FsManager::DeleteBlock(const BlockId& block_id) {
  return block_manager_->DeleteBlock(block_id);
}

bool FsManager::BlockExists(const BlockId& block_id) const {
  gscoped_ptr<ReadableBlock> block;
  return block_manager_->OpenBlock(block_id, &block).ok();
}

std::ostream& operator<<(std::ostream& o, const BlockId& block_id) {
  return o << block_id.ToString();
}

} // namespace kudu
