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
const char *FsManager::kDataDirName = "data";
const char *FsManager::kTablesDirName = "tables";
const char *FsManager::kTableMetaDirName = "meta";
const char *FsManager::kTableTabletsDirName = "tablets";
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

  // Initialize tables dir
  RETURN_NOT_OK(CreateDirIfMissing(GetTablesRootDir()));

  return Status::OK();
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
                                   const string& path, const vector<string>& objects)
{
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

Status FsManager::WriteMetadataFile(const string& base_path, const string& meta,
                                    const MessageLite& msg)
{
  std::vector<std::string> meta_files;
  vector<uint64_t> available_ids;

  RETURN_NOT_OK(env_->GetChildren(base_path, &meta_files));
  size_t seq_id = GetLastMetadataSeqId(meta_files, meta, &available_ids);

  // TODO: Add a NewWritableFile without O_TRUNC and remove the loop that is not atomic anyway...
  RETURN_NOT_OK(CreateDirIfMissing(base_path));
  string file_path;
  do {
    file_path = GetMetadataFilePath(base_path, meta, ++seq_id);
  } while (env_->FileExists(file_path));
  VLOG(1) << "Creating Metadata " << seq_id << " " << file_path;

  // Write the new metadata file
  shared_ptr<WritableFile> wfile;
  RETURN_NOT_OK(env_util::OpenFileForWrite(env_, file_path, &wfile));
  if (!pb_util::SerializeToWritableFile(msg, wfile.get())) {
    return Status::IOError("Unable to write the new '" + meta + "' manifest");
  }
  wfile->Close();

  // Cleanup older versions
  while (available_ids.size() > 0) {
    uint64_t seq_id = available_ids.back();
    available_ids.pop_back();
    env_->DeleteFile(GetMetadataFilePath(base_path, meta, seq_id));
  }

  return Status::OK();
}

Status FsManager::ReadMetadataFile(const string& base_path, const string& meta, MessageLite *msg) {
  std::vector<std::string> meta_files;
  vector<uint64_t> available_ids;

  RETURN_NOT_OK(env_->GetChildren(base_path, &meta_files));
  GetLastMetadataSeqId(meta_files, meta, &available_ids);

  // Try to open the latest "readable" metadata
  while (available_ids.size() > 0) {
    uint64_t seq_id = available_ids.back();
    available_ids.pop_back();

    string path = GetMetadataFilePath(base_path, meta, seq_id);
    VLOG(1) << "Opening Metadata " << seq_id << " " << path;
    shared_ptr<SequentialFile> rfile;
    RETURN_NOT_OK(env_util::OpenFileForSequential(env_, path, &rfile));
    if (pb_util::ParseFromSequentialFile(msg, rfile.get())) {
      return Status::OK();
    }

    // TODO: Is this failed due to an I/O Problem or because the file is corrupted?
    //       if is an I/O problem we shouldn't try another one.
    LOG(WARNING) << "Unable to read '" + meta + "' manifest: " << path;
    env_->RenameFile(path, path + kCorruptedSuffix);
  }

  return Status::IOError("Unable to read '" + meta + "' manifest");
}

Status FsManager::DeleteMetadataFile(const string& base_path, const string& meta) {
  std::vector<std::string> meta_files;
  vector<uint64_t> available_ids;
  RETURN_NOT_OK(env_->GetChildren(base_path, &meta_files));
  GetLastMetadataSeqId(meta_files, meta, &available_ids);
  BOOST_FOREACH(uint64_t file_id, available_ids) {
    RETURN_NOT_OK(env_->DeleteFile(GetMetadataFilePath(base_path, meta, file_id)));
  }
  return Status::OK();
}

uint64_t FsManager::GetLastMetadataSeqId(const vector<string>& meta_files, const string& meta,
                                         vector<uint64_t> *available_ids)
{
  string prefix = meta + ".";

  available_ids->clear();
  BOOST_FOREACH(const string& name, meta_files) {
    string suffix;
    if (TryStripPrefixString(name, prefix, &suffix)) {
      uint64 file_id;
      if (!safe_strtou64(suffix, &file_id)) {
        continue;
      }

      available_ids->push_back(file_id);
    }
  }
  std::sort(available_ids->begin(), available_ids->end());
  return available_ids->empty() ? 0 : available_ids->back();
}

// ==========================================================================
//  Wal read/write interfaces
// ==========================================================================

Status FsManager::NewWalFile(const string& server, const string& prefix, uint64_t timestamp,
                             shared_ptr<WritableFile> *writer)
{
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
                              shared_ptr<RandomAccessFile> *reader)
{
  return env_util::OpenFileForRandom(env_, GetWalFilePath(server, prefix, timestamp), reader);
}

} // namespace kudu
