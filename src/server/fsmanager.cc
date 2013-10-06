// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <glog/logging.h>

#include "gutil/strings/strip.h"
#include "gutil/strings/numbers.h"
#include "gutil/strtoint.h"
#include "server/fsmanager.h"
#include "util/env_util.h"
#include "util/pb_util.h"

namespace kudu {

// ==========================================================================
//  FS Paths
// ==========================================================================
const char *FsManager::kWalsDirName = "wals";
const char *FsManager::kMasterBlockDirName = "master-blocks";
const char *FsManager::kDataDirName = "data";
const char *FsManager::kCorruptedSuffix = ".corrupted";

const char *FsMetadataTypeToString(FsMetadataType type) {
  switch (type) {
    case kFsMetaTabletData:
      return "data";
  }

  /* Never reached */
  DCHECK(0);
  return(NULL);
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

  return Status::OK();
}

string FsManager::GetMasterBlockDir() const {
  return env_->JoinPathSegments(GetRootDir(), kMasterBlockDirName);
}

string FsManager::GetMasterBlockPath(const std::string& tablet_id) const {
  return env_->JoinPathSegments(GetMasterBlockDir(), tablet_id);
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
    string sub_path = env_->JoinPathSegments(path, name);
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
  RETURN_NOT_OK(env_util::OpenFileForWrite(env_, path, &wfile));
  if (!pb_util::SerializeToWritableFile(msg, wfile.get())) {
    return Status::IOError("Unable to write the new '" + block_id.ToString() + "' metadata block");
  }
  wfile->Close();

  return Status::OK();
}

Status FsManager::ReadMetadataBlock(const BlockId& block_id, MessageLite *msg) {
  VLOG(1) << "Reading Metadata Block " << block_id.ToString();

  shared_ptr<SequentialFile> rfile;
  string path = GetBlockPath(block_id);
  RETURN_NOT_OK(env_util::OpenFileForSequential(env_, path, &rfile));
  if (pb_util::ParseFromSequentialFile(msg, rfile.get())) {
    return Status::OK();
  }

  // TODO: Is this failed due to an I/O Problem or because the file is corrupted?
  //       if is an I/O problem we shouldn't try another one.
  //       Add a (length, checksum) block at the end of the PB.
  LOG(WARNING) << "Unable to read '"+ block_id.ToString() +"' metadata block, marking as corrupted";
  env_->RenameFile(path, path + kCorruptedSuffix);
  return Status::Corruption("Unable to read '" + block_id.ToString() + "' metadata block");
}

// ==========================================================================
//  Wal read/write interfaces
// ==========================================================================

Status FsManager::NewWalFile(const string& server, const string& prefix, uint64_t timestamp,
                             shared_ptr<WritableFile> *writer) {
  string path = GetWalFilePath(server, prefix, timestamp);
  if (env_->FileExists(path)) {
    return Status::AlreadyPresent("Another WAL with the same timestamp already exists. ",
      "server=" + server + " timestamp=" +  boost::lexical_cast<string>(timestamp));
  }
  // TODO: The dir should be created by the new file...
  CreateWalsDir(server, prefix, timestamp);
  return env_util::OpenFileForWrite(env_, path, writer);
}

Status FsManager::OpenWalFile(const string& server, const string& prefix, uint64_t timestamp,
                              shared_ptr<RandomAccessFile> *reader) {
  return env_util::OpenFileForRandom(env_, GetWalFilePath(server, prefix, timestamp), reader);
}

} // namespace kudu
