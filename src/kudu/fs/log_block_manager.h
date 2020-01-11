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

#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>  // IWYU pragma: keep
#include <gtest/gtest_prod.h>
#include <sparsepp/spp.h>

#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/atomic.h"
#include "kudu/util/locks.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/status.h"

namespace kudu {

class Env;
class FileCache;

namespace fs {
class DataDirManager;
class Dir;
class FsErrorManager;
struct FsReport;

namespace internal {
class LogBlock;
class LogBlockContainer;
class LogBlockDeletionTransaction;
class LogWritableBlock;
struct LogBlockContainerLoadResult;
struct LogBlockManagerMetrics;
} // namespace internal

typedef scoped_refptr<internal::LogBlock> LogBlockRefPtr;
typedef scoped_refptr<internal::LogBlockContainer> LogBlockContainerRefPtr;

// A log-backed (i.e. sequentially allocated file) block storage
// implementation.
//
// This is a block storage implementation that attempts to reduce the
// number of files used by clustering blocks into large files known
// henceforth as containers. A container begins empty and is written to
// sequentially, block by block. When a container becomes full, it is set
// aside and a new container is created.
//
// Implementation details
// ----------------------
//
// A container is comprised of two files, one for metadata and one for
// data. Both are written to sequentially. During a write, the block's data
// is written as-is to the data file. After the block has been
// synchronized, a small record is written to the metadata file containing
// the block's ID and its location within the data file.
//
// Block deletions are handled similarly. When a block is deleted, a record
// is written describing the deletion, orphaning the old block data. The
// orphaned data can be reclaimed instantaneously via hole punching, or
// later via garbage collection. The latter is used when hole punching is
// not supported on the filesystem, or on next boot if there's a crash
// after deletion but before hole punching. The metadata file itself is not
// compacted, as it is expected to remain quite small even after a great
// many create/delete cycles.
//
// Data and metadata operations are carefully ordered to ensure the
// correctness of the persistent representation at all times. During the
// writable block lifecycle (i.e. when a block is being created), data
// operations come before metadata operations. In the event that a metadata
// operation fails, the result is an orphaned block that is detected and
// pruned in the next garbage collection cycle. Conversely, metadata
// operations precede the data operations when deleting a block. In the
// worst case, a failure in the latter yields more garbage data that can be
// deleted in a GC.
//
// Care is taken to keep the in-memory representation of the block manager
// in sync with its persistent representation. To wit, a block is only made
// available in memory if _all_ on-disk operations (including any necessary
// synchronization calls) are successful.
//
// Writes to containers are batched together through the use of block
// transactions: each writer will take ownership of an "available" container,
// write a block to the container, and release ownership of the container once
// the writer "finalizes" the block, making the container available to other
// writers. This can happen concurrently; multiple transactions can interleave
// writes to single container, provided each writer finalizes its block before
// the next writer reaches for a container. Once any of the writers is
// completely done with its IO, it can commit its transaction, syncing its
// blocks and the container to disk (potentially as others are writing!).
//
// In order to maintain on-disk consistency, if the above commit fails, the
// entire container is marked read-only, and any future writes to the container
// will fail. There is a tradeoff here to note--having concurrent writers
// grants better utilization for each container; however a failure to sync by
// any of the writers will cause the others to fail and potentially corrupt the
// underlying container.
//
// When a new block is created, a container is selected from the data
// directory group appropriate for the block, as indicated by hints in
// provided CreateBlockOptions (i.e. blocks for diskrowsets should be placed
// within its tablet's data directory group).
//
// All log block manager metadata requests are served from memory. When an
// existing block manager is opened, all on-disk container metadata is
// parsed to build a single in-memory map describing the existence and
// locations of various blocks. Each entry in the map consumes ~64 bytes,
// putting the memory overhead at ~610 MB for 10 million blocks.
//
// New blocks are placed on a filesystem block boundary, and the size of
// hole punch requests is rounded up to the nearest filesystem block size.
// Taken together, this guarantees that hole punching can actually reclaim
// disk space (instead of just zeroing the block's bytes on disk).
//
// Design trade-offs
// -----------------
// In general, log-backed block storage is optimized for sustained reads
// and writes. The idea is that all blocks in a given container contain
// related data and are generally read at once, reducing seeks for
// sustained scans. This comes at a cost: the containers need to be garbage
// collected every now and then, though newer systems can take advantage of
// filesystem hole punching (as described above) to reclaim space.
//
// The on-disk container metadata design favors simplicity and contiguous
// access over space consumption and scalability to a very large number of
// blocks. To be more specific, the separation of metadata from data allows
// for high performance sustained reads at block manager open time at a
// manageability cost: a container is not a single file, and needs multiple
// open fds to be of use. Moreover, the log-structured nature of the
// metadata is simple and performant at open time.
//
// Likewise, the default container placement policy favors simplicity over
// performance. In the future, locality hints will ensure that blocks
// pertaining to similar data are colocated, improving scan performance.
//
// The choice to serve all metadata requests from memory favors simplicity
// over memory consumption. With a very large number of blocks, the
// in-memory map may balloon in size and some sort of "spilling" behavior
// may be beneficial.

// TODO
// ----
// - Implement garbage collection fallback for hole punching.
// - Implement locality hints so that specific containers can be used for
//   groups of blocks (i.e. an entire column).
// - Implement failure recovery (i.e. metadata truncation and other
//   similarly recoverable errors).
// - Evaluate and implement a solution for data integrity (e.g. per-block
//   checksum).
// - Change the availability semantics to only mark a container as available if
//   the current writer has committed and synced its transaction.

// The log-backed block manager.
class LogBlockManager : public BlockManager {
 public:
  static const char* kContainerMetadataFileSuffix;
  static const char* kContainerDataFileSuffix;

  // Note: all objects passed as pointers should remain alive for the lifetime
  // of the block manager.
  LogBlockManager(Env* env,
                  DataDirManager* dd_manager,
                  FsErrorManager* error_manager,
                  FileCache* file_cache,
                  BlockManagerOptions opts);

  virtual ~LogBlockManager();

  Status Open(FsReport* report) override;

  Status CreateBlock(const CreateBlockOptions& opts,
                     std::unique_ptr<WritableBlock>* block) override;

  Status OpenBlock(const BlockId& block_id,
                   std::unique_ptr<ReadableBlock>* block) override;

  std::unique_ptr<BlockCreationTransaction> NewCreationTransaction() override;

  std::shared_ptr<BlockDeletionTransaction> NewDeletionTransaction() override;

  Status GetAllBlockIds(std::vector<BlockId>* block_ids) override;

  void NotifyBlockId(BlockId block_id) override;

  FsErrorManager* error_manager() override { return error_manager_; }

 private:
  FRIEND_TEST(LogBlockManagerTest, TestAbortBlock);
  FRIEND_TEST(LogBlockManagerTest, TestCloseFinalizedBlock);
  FRIEND_TEST(LogBlockManagerTest, TestCompactFullContainerMetadataAtStartup);
  FRIEND_TEST(LogBlockManagerTest, TestFinalizeBlock);
  FRIEND_TEST(LogBlockManagerTest, TestLIFOContainerSelection);
  FRIEND_TEST(LogBlockManagerTest, TestLookupBlockLimit);
  FRIEND_TEST(LogBlockManagerTest, TestMetadataTruncation);
  FRIEND_TEST(LogBlockManagerTest, TestParseKernelRelease);
  FRIEND_TEST(LogBlockManagerTest, TestBumpBlockIds);
  FRIEND_TEST(LogBlockManagerTest, TestReuseBlockIds);
  FRIEND_TEST(LogBlockManagerTest, TestFailMultipleTransactionsPerContainer);

  friend class internal::LogBlockContainer;
  friend class internal::LogBlockDeletionTransaction;
  friend class internal::LogWritableBlock;

  // Type for the actual block map used to store all live blocks.
  // We use sparse_hash_map<> here to reduce memory overhead.
  typedef MemTrackerAllocator<
      std::pair<const BlockId, LogBlockRefPtr>> BlockAllocator;
  typedef spp::sparse_hash_map<
      BlockId,
      LogBlockRefPtr,
      BlockIdHash,
      BlockIdEqual,
      BlockAllocator> BlockMap;

  // Simpler typedef for a block map which isn't tracked in the memory tracker.
  //
  // Only used during startup.
  typedef std::unordered_map<
      const BlockId,
      LogBlockRefPtr,
      BlockIdHash,
      BlockIdEqual> UntrackedBlockMap;

  // Map used to store live block records during container metadata processing.
  //
  // Only used during startup.
  typedef std::unordered_map<
      const BlockId,
      BlockRecordPB,
      BlockIdHash,
      BlockIdEqual> BlockRecordMap;

  // Adds an as of yet unseen container to this block manager.
  //
  // Must be called with 'lock_' held.
  void AddNewContainerUnlocked(const LogBlockContainerRefPtr& container);

  // Removes a previously added container from this block manager. The
  // container must be dead (i.e. full and without any live blocks).
  //
  // Must be called with 'lock_' held.
  void RemoveDeadContainerUnlocked(const std::string& container_name);

  // Variant of RemoveDeadContainerUnlocked that acquires 'lock_'.
  void RemoveDeadContainer(const std::string& container_name);

  // Returns a container appropriate for the given CreateBlockOptions, creating
  // a new container if necessary.
  //
  // After returning, the container is considered to be in use. When
  // writing is finished, call MakeContainerAvailable() to make it
  // available to other writers.
  Status GetOrCreateContainer(const CreateBlockOptions& opts,
                              LogBlockContainerRefPtr* container);

  // Indicate that this container is no longer in use and can be handed out
  // to other writers.
  void MakeContainerAvailable(LogBlockContainerRefPtr container);
  void MakeContainerAvailableUnlocked(LogBlockContainerRefPtr container);

  // Synchronizes a container's dirty metadata to disk, taking care not to
  // sync more than is necessary (using 'dirty_dirs_').
  Status SyncContainer(const internal::LogBlockContainer& container);

  // Attempts to claim 'block_id' for use in a new WritableBlock.
  //
  // Returns true if the given block ID was not in use (and marks it as in
  // use), false otherwise.
  bool TryUseBlockId(const BlockId& block_id);

  // Creates and adds a LogBlock to in-memory data structures.
  //
  // Returns the created LogBlock if it was successfully added or nullptr if a
  // block with that ID was already present.
  LogBlockRefPtr CreateAndAddLogBlock(
      LogBlockContainerRefPtr container,
      const BlockId& block_id,
      int64_t offset,
      int64_t length);

  // Adds a LogBlock for an already-constructed LogBlock object.
  //
  // Returns true if the LogBlock was successfully added, false if it was already present.
  bool AddLogBlock(LogBlockRefPtr lb);

  // Removes the given set of LogBlocks from in-memory data structures, and
  // appends the block deletion metadata to record the on-disk deletion.
  // The 'log_blocks' out parameter will be set with the LogBlocks that were
  // successfully removed. The 'deleted' out parameter will be set with the
  // blocks were already deleted, e.g encountered 'NotFound' error during removal.
  //
  // Returns the first deletion failure that was seen, if any.
  Status RemoveLogBlocks(std::vector<BlockId> block_ids,
                         std::vector<LogBlockRefPtr>* log_blocks,
                         std::vector<BlockId>* deleted);

  // Removes a LogBlock from in-memory data structures.
  // The 'lb' out parameter will be set with the successfully deleted LogBlock.
  //
  // Returns an error of LogBlock cannot be successfully removed.
  Status RemoveLogBlock(const BlockId& block_id,
                        LogBlockRefPtr* lb);

  // Simple wrapper of Repair(), used as a runnable function in thread.
  void RepairTask(Dir* dir, internal::LogBlockContainerLoadResult* result);

  // Repairs any inconsistencies for 'dir' described in 'report'.
  //
  // The following additional repairs will be performed:
  // 1. Blocks in 'need_repunching' will be punched out again.
  // 2. Containers in 'dead_containers' will be deleted from disk.
  // 3. Containers in 'low_live_block_containers' will have their metadata
  //    files compacted.
  //
  // Returns an error if repairing a fatal inconsistency failed.
  Status Repair(Dir* dir,
                FsReport* report,
                std::vector<LogBlockRefPtr> need_repunching,
                std::vector<LogBlockContainerRefPtr> dead_containers,
                std::unordered_map<
                    std::string,
                    std::vector<BlockRecordPB>> low_live_block_containers);

  // Rewrites a container metadata file, appending all entries in 'records'.
  // The new metadata file is created as a temporary file and renamed over the
  // existing file after it is fully written.
  //
  // On success, writes the difference in file sizes to 'file_bytes_delta'. On
  // failure, an effort is made to delete the temporary file.
  //
  // Note: the new file is synced but its parent directory is not.
  Status RewriteMetadataFile(const internal::LogBlockContainer& container,
                             const std::vector<BlockRecordPB>& records,
                             int64_t* file_bytes_delta);

  // Opens a particular data directory belonging to the block manager. The
  // results of consistency checking are written to 'results'.
  //
  // Success or failure is set in 'result_status'.
  void OpenDataDir(Dir* dir,
                   std::vector<std::unique_ptr<internal::LogBlockContainerLoadResult>>* results,
                   Status* result_status);

  // Reads records from one log block container in the data directory.
  // The result details will be collected into 'result'.
  void LoadContainer(Dir* dir,
                     LogBlockContainerRefPtr container,
                     internal::LogBlockContainerLoadResult* result);

  // Perform basic initialization.
  Status Init();

  ObjectIdGenerator* oid_generator() { return &oid_generator_; }

  Env* env() const { return env_; }

  // Returns the path of the given container. Only for use by tests.
  static std::string ContainerPathForTests(internal::LogBlockContainer* container);

  // Returns whether the given kernel release is vulnerable to KUDU-1508.
  static bool IsBuggyEl6Kernel(const std::string& kernel_release);

  // Finds an appropriate block limit from 'kPerFsBlockSizeBlockLimits'
  // using the given filesystem block size.
  static int64_t LookupBlockLimit(int64_t fs_block_size);

  const internal::LogBlockManagerMetrics* metrics() const { return metrics_.get(); }

  // For kernels affected by KUDU-1508, tracks a known good upper bound on the
  // number of blocks per container, given a particular filesystem block size.
  static const std::map<int64_t, int64_t> kPerFsBlockSizeBlockLimits;

  // For manipulating files.
  Env* env_;

  // Manages and owns the data directories in which the block manager will
  // place its blocks.
  DataDirManager* dd_manager_;

  // Manages callbacks used to handle disk failure.
  FsErrorManager* error_manager_;

  // The options that the LogBlockManager was created with.
  const BlockManagerOptions opts_;

  // Tracks memory consumption of any allocations numerous enough to be
  // interesting (e.g. LogBlocks).
  std::shared_ptr<MemTracker> mem_tracker_;

  // Block IDs container used to prevent collisions when creating new anonymous blocks.
  struct ManagedBlockShard {
    // Protects 'blocks_by_block_id' and 'open_block_ids'.
    std::unique_ptr<simple_spinlock> lock;

    // Maps block IDs to blocks that are now readable, either because they
    // already existed on disk when the block manager was opened, or because
    // they're WritableBlocks that were closed.
    std::unique_ptr<BlockMap> blocks_by_block_id;

    // Contains block IDs for WritableBlocks that are still open for writing.
    // When a WritableBlock is closed, its ID is moved to 'blocks_by_block_id'.
    BlockIdSet open_block_ids;
  };

  // Sharding block IDs containers.
  std::vector<ManagedBlockShard> managed_block_shards_;

  // Protects 'all_containers_by_name_', 'available_containers_by_data_dir_' and 'dirty_dirs'.
  mutable simple_spinlock lock_;

  // Maps a data directory to an upper bound on the number of blocks that a
  // container residing in that directory should observe, if one is necessary.
  std::unordered_map<const Dir*,
                     boost::optional<int64_t>> block_limits_by_data_dir_;

  // Manages files opened for reading.
  FileCache* file_cache_;

  // Holds (and owns) all containers loaded from disk.
  std::unordered_map<std::string,
                     LogBlockContainerRefPtr> all_containers_by_name_;

  // Holds only those containers that are currently available for writing,
  // excluding containers that are either in use or full.
  //
  // Does not own the containers.
  std::unordered_map<const Dir*,
                     std::deque<LogBlockContainerRefPtr>> available_containers_by_data_dir_;

  // Tracks dirty container directories.
  //
  // Synced and cleared by SyncMetadata().
  std::unordered_set<std::string> dirty_dirs_;

  // If true, the kernel is vulnerable to KUDU-1508.
  const bool buggy_el6_kernel_;

  // For generating container names.
  ObjectIdGenerator oid_generator_;

  // For generating block IDs.
  AtomicInt<uint64_t> next_block_id_;

  // Metrics for the block manager.
  //
  // May be null if instantiated without metrics.
  std::unique_ptr<internal::LogBlockManagerMetrics> metrics_;

  DISALLOW_COPY_AND_ASSIGN(LogBlockManager);
};

} // namespace fs
} // namespace kudu

