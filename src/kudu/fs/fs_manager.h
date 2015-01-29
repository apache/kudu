// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_FS_FS_MANAGER_H
#define KUDU_FS_FS_MANAGER_H

#include <iosfwd>
#include <string>
#include <vector>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/env.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"

DECLARE_bool(enable_data_block_fsync);

namespace google {
namespace protobuf {
class Message;
} // namespace protobuf
} // namespace google

namespace kudu {

namespace fs {
class BlockManager;
class ReadableBlock;
class WritableBlock;
} // namespace fs

class BlockId;

class InstanceMetadataPB;

// FsManager provides helpers to read data and metadata files,
// and it's responsible for abstracting the file-system layout.
//
// The user should not be aware of where files are placed,
// but instead should interact with the storage in terms of "open the block xyz"
// or "write a new schema metadata file for table kwz".
//
// The current layout is:
//    <kudu.root.dir>/data/
//    <kudu.root.dir>/data/<prefix-0>/<prefix-2>/<prefix-4>/<name>
class FsManager {
 public:
  static const char *kWalFileNamePrefix;
  static const char *kWalsRecoveryDirSuffix;

  FsManager(Env *env, const std::string& root_path);

  ~FsManager();

  // Initialize and load the basic filesystem metadata.
  // If the file system has not been initialized, returns NotFound.
  // In that case, CreateInitialFileSystemLayout may be used to initialize
  // the on-disk structures.
  Status Open();

  // Create the initial filesystem layout.
  // This has no effect if the layout is already initialized.
  Status CreateInitialFileSystemLayout();
  void DumpFileSystemTree(std::ostream& out);

  // Return the UUID persisted in the local filesystem. If Open()
  // has not been called, this will crash.
  const std::string& uuid() const;

  // ==========================================================================
  //  Data read/write interfaces
  // ==========================================================================

  // Creates a new anonymous block.
  //
  // Block will be synced on close.
  Status CreateNewBlock(gscoped_ptr<fs::WritableBlock>* block);

  Status OpenBlock(const BlockId& block_id,
                   gscoped_ptr<fs::ReadableBlock>* block);

  Status DeleteBlock(const BlockId& block_id);

  // Tries to delete all blocks in 'block_ids', returning the first failure.
  Status DeleteBlocks(const std::vector<BlockId>& block_ids);

  bool BlockExists(const BlockId& block_id) const;

  // ==========================================================================
  //  Metadata read/write interfaces
  // ==========================================================================

  Status WriteMetadataBlock(const BlockId& block_id,
                            const google::protobuf::Message& msg);
  Status ReadMetadataBlock(const BlockId& block_id,
                           google::protobuf::Message *msg);

  // ==========================================================================
  //  on-disk path
  // ==========================================================================
  const std::string& GetRootDir() const {
    return root_path_;
  }

  std::string GetDataRootDir() const {
    return JoinPathSegments(GetRootDir(), kDataDirName);
  }

  std::string GetBlockPath(const BlockId& block_id) const;

  std::string GetWalsRootDir() const {
    return JoinPathSegments(root_path_, kWalDirName);
  }

  std::string GetTabletWalDir(const std::string& tablet_id) const {
    return JoinPathSegments(GetWalsRootDir(), tablet_id);
  }

  std::string GetTabletWalRecoveryDir(const std::string& tablet_id) const;

  std::string GetWalSegmentFileName(const std::string& tablet_id,
                                    uint64_t sequence_number) const;

  // Return the directory where tablet master blocks should be stored.
  std::string GetMasterBlockDir() const;

  // Return the path for a specific tablet's master block.
  std::string GetMasterBlockPath(const std::string& tablet_id) const;

  // Return the path where InstanceMetadataPB is stored.
  std::string GetInstanceMetadataPath() const;

  // Return the directory where the consensus metadata is stored.
  std::string GetConsensusMetadataDir() const {
    return JoinPathSegments(root_path_, kConsensusMetadataDirName);
  }

  // Return the path where ConsensusMetadataPB is stored.
  std::string GetConsensusMetadataPath(const std::string& tablet_id) const {
    return JoinPathSegments(GetConsensusMetadataDir(), tablet_id);
  }

  // Generate a new block ID.
  BlockId GenerateBlockId();

  Env *env() { return env_; }

  // ==========================================================================
  //  file-system helpers
  // ==========================================================================
  bool Exists(const std::string& path) const {
    return env_->FileExists(path);
  }

  Status ListDir(const std::string& path, std::vector<std::string> *objects) const {
    return env_->GetChildren(path, objects);
  }

  Status CreateDirIfMissing(const std::string& path, bool* created = NULL) {
    Status s = env_->CreateDir(path);
    if (created != NULL) {
      *created = s.ok();
    }
    return s.IsAlreadyPresent() ? Status::OK() : s;
  }

 private:
  // Select and create an instance of the appropriate block manager.
  //
  // Does not actually perform any on-disk operations.
  void InitBlockManager();

  // Creates the parent directory hierarchy to contain the given block id.
  Status CreateBlockDir(const BlockId& block_id);

  // Create a new InstanceMetadataPB and save it to the filesystem.
  // Does not mutate the current state of the fsmanager.
  Status CreateAndWriteInstanceMetadata();

  // ==========================================================================
  //  file-system helpers
  // ==========================================================================
  void DumpFileSystemTree(std::ostream& out,
                          const std::string& prefix,
                          const std::string& path,
                          const std::vector<std::string>& objects);

  static const char *kDataDirName;
  static const char *kMasterBlockDirName;
  static const char *kWalDirName;
  static const char *kCorruptedSuffix;
  static const char *kInstanceMetadataFileName;
  static const char *kInstanceMetadataMagicNumber;
  static const char *kTabletSuperBlockMagicNumber;
  static const char *kConsensusMetadataDirName;

  Env *env_;
  std::string root_path_;

  ObjectIdGenerator oid_generator_;

  gscoped_ptr<InstanceMetadataPB> metadata_;

  gscoped_ptr<fs::BlockManager> block_manager_;

  DISALLOW_COPY_AND_ASSIGN(FsManager);
};

} // namespace kudu

#endif
