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

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "kudu/fs/block_manager.h"
#include "kudu/fs/error_manager.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"

namespace kudu {

class BlockId;
class Env;
class FileCache;
class MemTracker;

namespace fs {
class DataDirManager;
struct FsReport;

namespace internal {
class FileBlockDeletionTransaction;
class FileBlockLocation;
class FileReadableBlock;
class FileWritableBlock;
struct BlockManagerMetrics;
} // namespace internal

// A file-backed block storage implementation.
//
// This is a naive block implementation which maps each block to its own
// file on disk. To prevent the block directory from becoming too large,
// blocks are aggregated into a 3-level directory hierarchy.
//
// The block manager can take advantage of multiple filesystem paths. A block
// written to a given path will be assigned an ID that includes enough
// information to uniquely identify the path's underlying disk. The ID is
// resolved back into a filesystem path when the block is opened for reading.
// The structure of this ID limits the block manager to at most 65,536 disks.
//
// When creating blocks, the block manager will place blocks based on the
// provided CreateBlockOptions.

// The file-backed block manager.
class FileBlockManager : public BlockManager {
 public:
  static constexpr const char* const name() { return "file"; }

  // Note: all objects passed as pointers should remain alive for the lifetime
  // of the block manager.
  FileBlockManager(Env* env,
                   scoped_refptr<DataDirManager> dd_manager,
                   scoped_refptr<FsErrorManager> error_manager,
                   FileCache* file_cache,
                   BlockManagerOptions opts,
                   std::string tenant_id);

  ~FileBlockManager() override;

  Status Open(FsReport* report, MergeReport need_merge,
              std::atomic<int>* containers_processed,
              std::atomic<int>* containers_total) override;

  Status CreateBlock(const CreateBlockOptions& opts,
                     std::unique_ptr<WritableBlock>* block) override;

  bool FindBlockPath(const BlockId& block_id, std::string* path) const override;

  Status OpenBlock(const BlockId& block_id,
                   std::unique_ptr<ReadableBlock>* block) override;

  std::unique_ptr<BlockCreationTransaction> NewCreationTransaction() override;

  std::shared_ptr<BlockDeletionTransaction> NewDeletionTransaction() override;

  Status GetAllBlockIds(std::vector<BlockId>* block_ids) override;

  void NotifyBlockId(BlockId block_id) override;

  scoped_refptr<FsErrorManager> error_manager() override { return error_manager_; }

  std::string tenant_id() const override { return tenant_id_; }

 private:
  friend class internal::FileBlockDeletionTransaction;
  friend class internal::FileBlockLocation;
  friend class internal::FileReadableBlock;
  friend class internal::FileWritableBlock;

  // Deletes an existing block, allowing its space to be reclaimed by the
  // filesystem. The change is immediately made durable.
  //
  // Blocks may be deleted while they are open for reading or writing;
  // the actual deletion will take place after the last open reader or
  // writer is closed.
  Status DeleteBlock(const BlockId& block_id);

  // Synchronizes the metadata for a block with the given location.
  Status SyncMetadata(const internal::FileBlockLocation& location);

  Env* env() const { return env_; }

  // For manipulating files.
  Env* env_;

  // Manages and owns the data directories in which the block manager will
  // place its blocks.
  scoped_refptr<DataDirManager> dd_manager_;

  // Manages callbacks used to handle disk failure.
  scoped_refptr<FsErrorManager> error_manager_;

  // The options that the FileBlockManager was created with.
  const BlockManagerOptions opts_;

  // Manages files opened for reading.
  FileCache* file_cache_;

  // For generating block IDs.
  ThreadSafeRandom rand_;
  std::atomic<uint64_t> next_block_id_;

  // Protects 'dirty_dirs_'.
  mutable simple_spinlock lock_;

  // Tracks the block directories which are dirty from block creation. This
  // lets us perform some simple coalescing when synchronizing metadata.
  std::unordered_set<std::string> dirty_dirs_;

  // Metric container for the block manager.
  // May be null if instantiated without metrics.
  std::unique_ptr<internal::BlockManagerMetrics> metrics_;

  // Tracks memory consumption of any allocations numerous enough to be
  // interesting.
  std::shared_ptr<MemTracker> mem_tracker_;

  // Which tenant this file block manager belongs to.
  std::string tenant_id_;

  DISALLOW_COPY_AND_ASSIGN(FileBlockManager);
};

} // namespace fs
} // namespace kudu
