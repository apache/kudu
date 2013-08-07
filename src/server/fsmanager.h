// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_COMMON_FS_MANAGER_H
#define KUDU_COMMON_FS_MANAGER_H

#include <boost/noncopyable.hpp>
#include <boost/lexical_cast.hpp>
#include <google/protobuf/message_lite.h>
#include <tr1/memory>
#include <string>
#include <vector>

#include "gutil/gscoped_ptr.h"
#include "gutil/strtoint.h"
#include "server/oid_generator.h"
#include "util/env.h"

namespace kudu {

using std::ostream;
using std::string;
using std::vector;
using std::tr1::shared_ptr;
using google::protobuf::MessageLite;

class BlockId {
 public:
  BlockId() {}
  explicit BlockId(const string& id) { SetId(id); }

  void SetId(const string& id) {
    CHECK_GE(id.size(), 8);
    id_ = id;
  }

  bool IsNull() const { return id_.empty(); }
  const string& ToString() const { return id_; }

  // Used for on-disk partition
  string hash0() const { return id_.substr(0, 2); }
  string hash1() const { return id_.substr(2, 2); }
  string hash2() const { return id_.substr(4, 2); }
  string hash3() const { return id_.substr(6, 2); }

  size_t hash() const {
    return (strto32(hash0().c_str(), NULL, 16) << 24) +
           (strto32(hash1().c_str(), NULL, 16) << 16) +
           (strto32(hash2().c_str(), NULL, 16) << 8) +
           strto32(hash3().c_str(), NULL, 16);
  }

  bool operator==(const BlockId& other) const {
    return id_ == other.id_;
  }

 private:
  string id_;
};

struct BlockIdHash {
  size_t operator()(const BlockId& block_id) const {
    return block_id.hash();
  }
};

enum FsMetadataType {
  // metadata file that contains the rowset blocks
  kFsMetaTabletData,
};

// Returns the on-disk name of the specified metadata type
const char *FsMetadataTypeToString(FsMetadataType type);

// FsManager provides helpers to read data and metadata files,
// and it's responsible for abstracting the file-system layout.
//
// The user should not be aware of where files are placed,
// but instead should interact with the storage in terms of "open the block xyz"
// or "write a new schema metadata file for table kwz".
//
// The FsManager should never be accessed directly, but instead the "Metadata"
// wrappers like "TableMetadata" or "TabletMetadata" should be used.
// Those wrappers are also responsible for writing every transaction like
// "add this new data file" to the related WALs.
// The TabletMetadata is also responsible for keeping track of the tablet files;
// each new file added/removed is added to the WAL and then flushed
// to the "tablet/data-files" meta file.
//
// The current layout is:
//    <kudu.root.dir>/data/
//    <kudu.root.dir>/data/<prefix-0>/<prefix-2>/<prefix-4>/<name>
class FsManager {
 public:
  FsManager(Env *env, const string& root_path)
    : env_(env), root_path_(root_path)
  {}

  Status CreateInitialFileSystemLayout();
  void DumpFileSystemTree(ostream& out);

  // ==========================================================================
  //  Data read/write interfaces
  // ==========================================================================

  Status CreateNewBlock(shared_ptr<WritableFile> *writer, BlockId *block_id);
  Status OpenBlock(const BlockId& block_id, shared_ptr<RandomAccessFile> *reader);

  Status DeleteBlock(const BlockId& block) {
    return env_->DeleteFile(GetBlockPath(block));
  }

  bool BlockExists(const BlockId& block) const {
    return env_->FileExists(GetBlockPath(block));
  }

  // ==========================================================================
  //  Metadata read/write interfaces
  // ==========================================================================

  Status WriteMetadataBlock(const BlockId& block_id, const MessageLite& msg);
  Status ReadMetadataBlock(const BlockId& block_id, MessageLite *msg);

  // ==========================================================================
  //  Wal read/write interfaces
  // ==========================================================================

  Status NewWalFile(const string& server, const string& prefix, uint64_t timestamp,
                    shared_ptr<WritableFile> *writer);
  Status OpenWalFile(const string& server, const string& prefix, uint64_t timestamp,
                     shared_ptr<RandomAccessFile> *reader);

  // ==========================================================================
  //  on-disk path
  // ==========================================================================
  const string& GetRootDir() const {
    return root_path_;
  }

  string GetDataRootDir() const {
    return env_->JoinPathSegments(GetRootDir(), kDataDirName);
  }

  string GetBlockPath(const BlockId& block_id) const {
    CHECK(!block_id.IsNull());
    string path = GetDataRootDir();
    path = env_->JoinPathSegments(path, block_id.hash0());
    path = env_->JoinPathSegments(path, block_id.hash1());
    path = env_->JoinPathSegments(path, block_id.hash2());
    path = env_->JoinPathSegments(path, block_id.ToString());
    return path;
  }

  string GetWalsRootDir() const {
    return env_->JoinPathSegments(root_path_, kWalsDirName);
  }

  string GetWalFilePath(const string& server, const string& prefix, uint64_t timestamp) const {
    string path = env_->JoinPathSegments(GetWalsRootDir(), server);
    path = env_->JoinPathSegments(path, boost::lexical_cast<string>(timestamp / kWalPartitionMillis));
    return env_->JoinPathSegments(path, prefix + "." + boost::lexical_cast<string>(timestamp));
  }

  // ==========================================================================
  //  Name generator
  // ==========================================================================

  string GenerateName() {
    return oid_generator_.Next();
  }

  Env *env() { return env_; }

  // ==========================================================================
  //  file-system helpers
  // ==========================================================================
  bool Exists(const string& path) const {
    return env_->FileExists(path);
  }

  Status ListDir(const string& path, vector<string> *objects) const {
    return env_->GetChildren(path, objects);
  }

  Status CreateDirIfMissing(const string& path) {
    Status s = env_->CreateDir(path);
    return s.IsAlreadyPresent() ? Status::OK() : s;
  }

 private:
  // ==========================================================================
  //  on-disk dir creations
  // ==========================================================================

  // TODO: This should be removed, and part of the file creation
  //       to ensure that the file can be created.
  Status CreateBlockDir(const BlockId& block_id) {
    CHECK(!block_id.IsNull());

    string path = GetDataRootDir();
    RETURN_NOT_OK(CreateDirIfMissing(path));

    path = env_->JoinPathSegments(path, block_id.hash0());
    RETURN_NOT_OK(CreateDirIfMissing(path));

    path = env_->JoinPathSegments(path, block_id.hash1());
    RETURN_NOT_OK(CreateDirIfMissing(path));

    path = env_->JoinPathSegments(path, block_id.hash2());
    return CreateDirIfMissing(path);
  }

  Status CreateWalsDir(const string& server, const string& prefix, uint64_t timestamp) {
    string path = GetWalsRootDir();
    RETURN_NOT_OK(CreateDirIfMissing(path));

    path = env_->JoinPathSegments(GetWalsRootDir(), server);
    RETURN_NOT_OK(CreateDirIfMissing(path));

    path = env_->JoinPathSegments(path, boost::lexical_cast<string>(timestamp / kWalPartitionMillis));
    return CreateDirIfMissing(path);
  }

  // ==========================================================================
  //  file-system helpers
  // ==========================================================================
  void DumpFileSystemTree(ostream& out, const string& prefix,
                          const string& path, const vector<string>& objects);

  static const char *kDataDirName;
  static const char *kWalsDirName;
  static const char *kCorruptedSuffix;
  static const uint64_t kWalPartitionMillis = 3600000;   // 1hour

  Env *env_;
  string root_path_;

  ObjectIdGenerator oid_generator_;

  DISALLOW_COPY_AND_ASSIGN(FsManager);
};

} // namespace kudu

#endif
