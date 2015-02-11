// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/fs/fs_manager.h"

#include <iostream>
#include <tr1/memory>
#include <tr1/unordered_set>

#include <boost/foreach.hpp>
#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <google/protobuf/message.h>

#include "kudu/fs/block_id.h"
#include "kudu/fs/block_id-inl.h"
#include "kudu/fs/file_block_manager.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strtoint.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/env_util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"

DEFINE_bool(enable_data_block_fsync, true,
            "Whether to enable fsync() of data blocks, metadata, and their parent directories. "
            "Disabling this flag may cause data loss in the event of a system crash.");

DEFINE_string(block_manager, "file", "Which block manager to use for storage. "
              "Valid options are 'file' and 'log'.");

using google::protobuf::Message;
using strings::Substitute;
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
const char *FsManager::kMasterBlockDirName = "master-blocks";
const char *FsManager::kDataDirName = "data";
const char *FsManager::kCorruptedSuffix = ".corrupted";
const char *FsManager::kInstanceMetadataFileName = "instance";
const char *FsManager::kConsensusMetadataDirName = "consensus-meta";

FsManager::FsManager(Env* env, const string& root_path)
  : env_(env) {
  InitRoots(root_path, boost::assign::list_of(root_path));
  InitBlockManager();
}

FsManager::FsManager(Env* env, const string& wal_path,
          const vector<string>& data_paths)
  : env_(env) {
  InitRoots(wal_path, data_paths);
  InitBlockManager();
}

FsManager::~FsManager() {
}

void FsManager::InitRoots(const string& wal_fs_root,
                          const vector<string>& data_fs_roots) {
  {
    string wal_fs_root_copy = wal_fs_root;
    StripWhiteSpace(&wal_fs_root_copy);
    CHECK(!wal_fs_root_copy.empty());
    CHECK_EQ('/', wal_fs_root_copy[0]);
    wal_fs_root_ = wal_fs_root_copy;
  }

  CHECK(!data_fs_roots.empty());
  BOOST_FOREACH(const string& data_fs_root, data_fs_roots) {
    string data_fs_root_copy = data_fs_root;
    StripWhiteSpace(&data_fs_root_copy);
    CHECK(!data_fs_root_copy.empty());
    CHECK_EQ('/', data_fs_root_copy[0]);
    data_fs_roots_.push_back(data_fs_root_copy);
  }
  metadata_fs_root_ = data_fs_roots_[0];
}

void FsManager::InitBlockManager() {
  // Add the data subdirectory to each data root.
  vector<string> data_paths;
  BOOST_FOREACH(const string& data_fs_root, data_fs_roots_) {
    data_paths.push_back(JoinPathSegments(data_fs_root, kDataDirName));
  }

  if (FLAGS_block_manager == "file") {
    block_manager_.reset(new FileBlockManager(env_, data_paths));
  } else if (FLAGS_block_manager == "log") {
    block_manager_.reset(new LogBlockManager(env_, data_paths));
  } else {
    LOG(FATAL) << "Invalid block manager: " << FLAGS_block_manager;
  }
}

Status FsManager::Open() {
  vector<string> roots(GetAllFilesystemRoots());
  BOOST_FOREACH(const string& root, roots) {
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

  RETURN_NOT_OK(block_manager_->Open());
  LOG(INFO) << "Opened local filesystem: " << JoinStrings(roots, ",")
            << std::endl << metadata_->DebugString();
  return Status::OK();
}

Status FsManager::CreateInitialFileSystemLayout() {
  vector<string> roots = GetAllFilesystemRoots();

  // Initialize each root dir.
  //
  // It's OK if a root already exists as long as there's no instance
  // metadata. However, no subdirectories should exist.
  InstanceMetadataPB metadata;
  CreateInstanceMetadata(&metadata);
  BOOST_FOREACH(const string& root, roots) {
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

  // Initialize master block dir.
  RETURN_NOT_OK_PREPEND(env_->CreateDir(GetMasterBlockDir()),
                        "Unable to create master block directory");

  // Initialize consensus metadata dir.
  RETURN_NOT_OK_PREPEND(env_->CreateDir(GetConsensusMetadataDir()),
                        "Unable to create consensus metadata directory");

  RETURN_NOT_OK_PREPEND(block_manager_->Create(), "Unable to create block manager");
  return Status::OK();
}

void FsManager::CreateInstanceMetadata(InstanceMetadataPB* metadata) {
  metadata->set_uuid(oid_generator_.Next());

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

std::vector<std::string> FsManager::GetAllFilesystemRoots() const {
  std::set<std::string> deduplicated_roots;
  deduplicated_roots.insert(wal_fs_root_);
  deduplicated_roots.insert(metadata_fs_root_);
  deduplicated_roots.insert(data_fs_roots_.begin(), data_fs_roots_.end());
  return std::vector<std::string>(deduplicated_roots.begin(),
                                  deduplicated_roots.end());
}

const string& FsManager::uuid() const {
  return CHECK_NOTNULL(metadata_.get())->uuid();
}

string FsManager::GetBlockPath(const BlockId& block_id) const {
  CHECK(!block_id.IsNull());
  string path = GetDataRootDir();
  path = JoinPathSegments(path, block_id.hash0());
  path = JoinPathSegments(path, block_id.hash1());
  path = JoinPathSegments(path, block_id.hash2());
  path = JoinPathSegments(path, block_id.ToString());
  return path;
}

string FsManager::GetMasterBlockDir() const {
  return JoinPathSegments(metadata_fs_root_, kMasterBlockDirName);
}

string FsManager::GetMasterBlockPath(const string& tablet_id) const {
  return JoinPathSegments(GetMasterBlockDir(), tablet_id);
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
  vector<string> roots = GetAllFilesystemRoots();
  BOOST_FOREACH(const string& root, roots) {
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

BlockId FsManager::GenerateBlockId() {
  return BlockId(oid_generator_.Next());
}

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

Status FsManager::CreateBlockDir(const BlockId& block_id) {
  CHECK(!block_id.IsNull());

  string root_dir = GetDataRootDir();
  bool root_created = false;
  RETURN_NOT_OK(CreateDirIfMissing(root_dir, &root_created));

  string path0 = JoinPathSegments(root_dir, block_id.hash0());
  bool path0_created = false;
  RETURN_NOT_OK(CreateDirIfMissing(path0, &path0_created));

  string path1 = JoinPathSegments(path0, block_id.hash1());
  bool path1_created = false;
  RETURN_NOT_OK(CreateDirIfMissing(path1, &path1_created));

  string path2 = JoinPathSegments(path1, block_id.hash2());
  bool path2_created = false;
  RETURN_NOT_OK(CreateDirIfMissing(path2, &path2_created));

  // No need to fsync path2 at this point, it should be fsync()ed when files are
  // written into it.
  if (FLAGS_enable_data_block_fsync) {
    if (path2_created) RETURN_NOT_OK(env_->SyncDir(path1));
    if (path1_created) RETURN_NOT_OK(env_->SyncDir(path0));
    if (path0_created) RETURN_NOT_OK(env_->SyncDir(root_dir));
    if (root_created) RETURN_NOT_OK(env_->SyncDir(DirName(root_dir))); // Parent of root_dir.
  }

  return Status::OK();
}

// ==========================================================================
//  Metadata read/write interfaces
// ==========================================================================
//
// TODO: Route through BlockManager, but that means potentially supporting block renaming.

Status FsManager::WriteMetadataBlock(const BlockId& block_id, const Message& msg) {
  RETURN_NOT_OK(CreateBlockDir(block_id));
  VLOG(1) << "Writing Metadata Block: " << block_id.ToString();

  // Write the new metadata file
  shared_ptr<WritableFile> wfile;
  string path = GetBlockPath(block_id);
  return pb_util::WritePBContainerToPath(env_, path, msg,
      FLAGS_enable_data_block_fsync ? pb_util::SYNC : pb_util::NO_SYNC);
}

Status FsManager::ReadMetadataBlock(const BlockId& block_id, Message* msg) {
  VLOG(1) << "Reading Metadata Block " << block_id.ToString();

  string path = GetBlockPath(block_id);
  Status s = pb_util::ReadPBContainerFromPath(env_, path, msg);
  if (s.IsNotFound()) {
    return s;
  }
  if (!s.ok()) {
    // TODO: Is this failed due to an I/O Problem or because the file is corrupted?
    //       if is an I/O problem we shouldn't try another one.
    //       Add a (length, checksum) block at the end of the PB.
    LOG(WARNING) << "Unable to read '" << block_id.ToString() << "' metadata block"
                 << " (" << s.ToString() << "): marking as corrupted";
    WARN_NOT_OK(env_->RenameFile(path, path + kCorruptedSuffix),
                "Unable to rename aside corrupted file " + path);
    return s.CloneAndPrepend("Unable to read '" + block_id.ToString() + "' metadata block");
  }
  return Status::OK();
}

std::ostream& operator<<(std::ostream& o, const BlockId& block_id) {
  return o << block_id.ToString();
}

} // namespace kudu
