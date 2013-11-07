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
#include "gutil/strings/substitute.h"
#include "gutil/strtoint.h"
#include "server/oid_generator.h"
#include "util/env.h"

namespace kudu {

using std::ostream;
using std::string;
using std::vector;
using std::tr1::shared_ptr;
using google::protobuf::MessageLite;

namespace metadata {
class InstanceMetadataPB;
} // namespace metadata

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
  FsManager(Env *env, const string& root_path);

  ~FsManager();

  // Initialize and load the basic filesystem metadata.
  // If the file system has not been initialized, returns NotFound.
  // In that case, CreateInitialFileSystemLayout may be used to initialize
  // the on-disk structures.
  Status Open();

  // Create the initial filesystem layout.
  // This has no effect if the layout is already initialized.
  Status CreateInitialFileSystemLayout();
  void DumpFileSystemTree(ostream& out);

  // Return the UUID persisted in the local filesystem. If Open()
  // has not been called, this will crash.
  const std::string& uuid() const;

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

  string GetTabletWalDir(const std::string& tablet_id) const {
    return env_->JoinPathSegments(GetWalsRootDir(), tablet_id);
  }

  string GetTabletWalRecoveryDir(const std::string& tablet_id, uint64_t timestamp) const {
    string path = env_->JoinPathSegments(GetWalsRootDir(), tablet_id);
    path = env_->JoinPathSegments(path, strings::Substitute("$0-$1",
                                                            kWalsRecoveryDirPrefix,
                                                            boost::lexical_cast<string>(timestamp)));
    return path;
  }

  // Return the directory where tablet master blocks should be stored.
  string GetMasterBlockDir() const;

  // Return the path for a specific tablet's master block.
  string GetMasterBlockPath(const std::string& tablet_id) const;

  // Return the path where InstanceMetadataPB is stored.
  string GetInstanceMetadataPath() const;

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

  // Create a new InstanceMetadataPB and save it to the filesystem.
  // Does not mutate the current state of the fsmanager.
  Status CreateAndWriteInstanceMetadata();

  // ==========================================================================
  //  file-system helpers
  // ==========================================================================
  void DumpFileSystemTree(ostream& out, const string& prefix,
                          const string& path, const vector<string>& objects);

  static const char *kDataDirName;
  static const char *kMasterBlockDirName;
  static const char *kWalsDirName;
  static const char *kWalsRecoveryDirPrefix;
  static const char *kCorruptedSuffix;
  static const char *kInstanceMetadataFileName;

  Env *env_;
  string root_path_;

  ObjectIdGenerator oid_generator_;

  gscoped_ptr<metadata::InstanceMetadataPB> metadata_;

  DISALLOW_COPY_AND_ASSIGN(FsManager);
};

} // namespace kudu

#endif
