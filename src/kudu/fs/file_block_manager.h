// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_FS_FILE_BLOCK_MANAGER_H
#define KUDU_FS_FILE_BLOCK_MANAGER_H

#include <string>
#include <tr1/memory>
#include <tr1/unordered_set>
#include <vector>

#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/util/locks.h"
#include "kudu/util/oid_generator.h"

namespace kudu {

class Env;
class WritableFile;

namespace fs {

namespace internal {
class FileWritableBlock;
} // namespace internal

// A file-backed block storage implementation.
//
// This is a naive block implementation which maps each block to its own
// file on disk. To prevent the block directory from becoming too large,
// blocks are aggregated into a 3-level directory hierarchy.

// The file-backed block manager.
class FileBlockManager : public BlockManager {
 public:

  // Creates a new in-memory instance of a FileBlockManager.
  //
  // 'env' should remain alive for the lifetime of the block manager.
  FileBlockManager(Env* env, const std::string& root_path);

  virtual ~FileBlockManager();

  virtual Status Create() OVERRIDE;

  virtual Status Open() OVERRIDE;

  virtual Status CreateAnonymousBlock(const CreateBlockOptions& opts,
                                      gscoped_ptr<WritableBlock>* block) OVERRIDE;

  virtual Status CreateAnonymousBlock(gscoped_ptr<WritableBlock>* block) OVERRIDE;

  virtual Status CreateNamedBlock(const CreateBlockOptions& opts,
                                  const BlockId& block_id,
                                  gscoped_ptr<WritableBlock>* block) OVERRIDE;

  virtual Status CreateNamedBlock(const BlockId& block_id,
                                  gscoped_ptr<WritableBlock>* block) OVERRIDE;

  virtual Status OpenBlock(const BlockId& block_id,
                           gscoped_ptr<ReadableBlock>* block) OVERRIDE;

  virtual Status DeleteBlock(const BlockId& block_id) OVERRIDE;

  virtual Status CloseBlocks(const std::vector<WritableBlock*>& blocks) OVERRIDE;

 private:
  friend class internal::FileWritableBlock;

  // Creates the parent directory hierarchy for the block with the given id.
  Status CreateBlockDir(const BlockId& block_id, std::vector<std::string>* created_dirs);

  // Returns the path to a block with the given id.
  std::string GetBlockPath(const BlockId& block_id) const;

  // Creates a directory if it's not already present.
  //
  // On error, does not set 'created'.
  Status CreateDirIfMissing(const std::string& path, bool* created = NULL);

  // Synchronizes the metadata for a block with the given id.
  Status SyncMetadata(const BlockId& block_id);

  // Creates a new block.
  void CreateBlock(const BlockId& block_id, const std::string& path,
                   const std::vector<std::string>& created_dirs,
                   const std::tr1::shared_ptr<WritableFile>& writer,
                   const CreateBlockOptions& opts,
                   gscoped_ptr<WritableBlock>* block);

  Env* env() const { return env_; }

  // Protects 'dirty_dirs_'.
  mutable simple_spinlock lock_;

  // Tracks the block directories which are dirty from block creation. This
  // lets us perform some simple coalescing when synchronizing metadata.
  std::tr1::unordered_set<std::string> dirty_dirs_;

  // For manipulating files.
  Env* env_;

  // Filesystem path where all block directories are found.
  const std::string root_path_;

  // For generating block IDs.
  ObjectIdGenerator oid_generator_;

  DISALLOW_COPY_AND_ASSIGN(FileBlockManager);
};

} // namespace fs
} // namespace kudu

#endif
