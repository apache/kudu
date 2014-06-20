// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <iostream>

#include "gutil/strings/join.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "gutil/strtoint.h"
#include "gutil/walltime.h"
#include "server/fsmanager.h"
#include "server/metadata.pb.h"
#include "util/env_util.h"
#include "util/net/net_util.h"
#include "util/path_util.h"
#include "util/pb_util.h"

using kudu::metadata::InstanceMetadataPB;
using strings::Substitute;

namespace kudu {

string BlockId::JoinStrings(const vector<BlockId>& blocks) {
  vector<string> strings;
  strings.reserve(blocks.size());
  BOOST_FOREACH(const BlockId& block, blocks) {
    strings.push_back(block.ToString());
  }
  return ::JoinStrings(strings, ",");
}

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

const char *FsMetadataTypeToString(FsMetadataType type) {
  switch (type) {
    case kFsMetaTabletData:
      return "data";
  }

  /* Never reached */
  DCHECK(0);
  return(NULL);
}

FsManager::FsManager(Env *env, const string& root_path)
  : env_(env), root_path_(root_path) {
}

FsManager::~FsManager() {
}

Status FsManager::Open() {
  gscoped_ptr<InstanceMetadataPB> pb(new InstanceMetadataPB);
  RETURN_NOT_OK(pb_util::ReadPBFromPath(env_, GetInstanceMetadataPath(), pb.get()));
  metadata_.reset(pb.release());
  LOG(INFO) << "Opened local filesystem: " << root_path_
            << std::endl << metadata_->DebugString();
  return Status::OK();
}

Status FsManager::CreateInitialFileSystemLayout() {
  // Initialize root
  RETURN_NOT_OK(CreateDirIfMissing(root_path_));

  // Initialize wals dir
  RETURN_NOT_OK(CreateDirIfMissing(GetWalsRootDir()));

  // Initialize data dir
  RETURN_NOT_OK(CreateDirIfMissing(GetDataRootDir()));

  // Initialize master block dir
  RETURN_NOT_OK(CreateDirIfMissing(GetMasterBlockDir()));

  if (!env_->FileExists(GetInstanceMetadataPath())) {
    RETURN_NOT_OK(CreateAndWriteInstanceMetadata());
  }

  return Status::OK();
}

Status FsManager::CreateAndWriteInstanceMetadata() {
  InstanceMetadataPB new_instance;
  new_instance.set_uuid(GenerateName());

  string time_str;
  StringAppendStrftime(&time_str, "%Y-%m-%d %H:%M:%S", time(NULL), false);
  string hostname;
  if (!GetHostname(&hostname).ok()) {
    hostname = "<unknown host>";
  }
  new_instance.set_format_stamp(Substitute("Formatted at $0 on $1", time_str, hostname));

  const string path = GetInstanceMetadataPath();

  RETURN_NOT_OK(pb_util::WritePBToPath(env_, path, new_instance));
  LOG(INFO) << "Generated new instance metadata in path " << path << ":\n"
            << new_instance.DebugString();
  return Status::OK();
}

const string& FsManager::uuid() const {
  return CHECK_NOTNULL(metadata_.get())->uuid();
}

string FsManager::GetMasterBlockDir() const {
  return JoinPathSegments(GetRootDir(), kMasterBlockDirName);
}

string FsManager::GetMasterBlockPath(const std::string& tablet_id) const {
  return JoinPathSegments(GetMasterBlockDir(), tablet_id);
}

string FsManager::GetInstanceMetadataPath() const {
  return JoinPathSegments(GetRootDir(), kInstanceMetadataFileName);
}

// ==========================================================================
//  Dump/Debug utils
// ==========================================================================

void FsManager::DumpFileSystemTree(ostream& out) {
  out << "File-System Root: " << root_path_ << std::endl;

  std::vector<string> objects;
  Status s = env_->GetChildren(root_path_, &objects);
  if (!s.ok()) {
    LOG(ERROR) << "Unable to list the fs-tree: " << s.ToString();
    return;
  }

  DumpFileSystemTree(out, "|-", root_path_, objects);
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

Status FsManager::CreateNewBlock(shared_ptr<WritableFile> *writer, BlockId *block_id) {
  // TODO: Add a NewWritableFile without O_TRUNC and remove the loop that is not atomic anyway...
  string path;
  do {
    block_id->SetId(GenerateName());
    RETURN_NOT_OK(CreateBlockDir(*block_id));
    path = GetBlockPath(*block_id);
  } while (env_->FileExists(path));
  VLOG(1) << "NewBlock: " << block_id->ToString();
  return env_util::OpenFileForWrite(env_, path, writer);
}

Status FsManager::OpenBlock(const BlockId& block_id, shared_ptr<RandomAccessFile> *reader) {
  VLOG(1) << "OpenBlock: " << block_id.ToString();
  return env_util::OpenFileForRandom(env_, GetBlockPath(block_id), reader);
}

// ==========================================================================
//  Metadata read/write interfaces
// ==========================================================================

Status FsManager::WriteMetadataBlock(const BlockId& block_id, const MessageLite& msg) {
  RETURN_NOT_OK(CreateBlockDir(block_id));
  VLOG(1) << "Writing Metadata Block: " << block_id.ToString();

  // Write the new metadata file
  shared_ptr<WritableFile> wfile;
  string path = GetBlockPath(block_id);
  return pb_util::WritePBToPath(env_, path, msg);
}

Status FsManager::ReadMetadataBlock(const BlockId& block_id, MessageLite *msg) {
  VLOG(1) << "Reading Metadata Block " << block_id.ToString();

  string path = GetBlockPath(block_id);
  Status s = pb_util::ReadPBFromPath(env_, path, msg);
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
