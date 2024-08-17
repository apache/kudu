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
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest_prod.h>
#include <sparsepp/spp.h>

#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/status.h"

namespace kudu {

class Env;
class FileCache;

namespace fs {
class Dir;
struct FsReport;

namespace internal {
class LogBlock;
class LogBlockContainer;
class LogBlockContainerNativeMeta;
#if !defined(NO_ROCKSDB)
class LogBlockContainerRdbMeta;
#endif
class LogBlockDeletionTransaction;
class LogWritableBlock;
struct LogBlockContainerLoadResult;
struct LogBlockManagerMetrics;
} // namespace internal

typedef scoped_refptr<internal::LogBlock> LogBlockRefPtr;
typedef scoped_refptr<internal::LogBlockContainer> LogBlockContainerRefPtr;
typedef scoped_refptr<internal::LogBlockContainerNativeMeta> LogBlockContainerNativeMetaRefPtr;
#if !defined(NO_ROCKSDB)
typedef scoped_refptr<internal::LogBlockContainerRdbMeta> LogBlockContainerRdbMetaRefPtr;
#endif
typedef std::unordered_map<std::string, std::vector<BlockRecordPB>> ContainerBlocksByName;
typedef std::unordered_map<std::string, LogBlockContainerRefPtr> ContainersByName;

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
// A container is comprised of two parts, one for metadata and one for
// data. During a write, the block's data is written as-is to the data file.
// After the block has been synchronized, a small record is written to the
// metadata part containing the block's ID and its location within the data
// file.
//
// Block deletions are handled similarly. When a block is deleted, a record
// is written describing the deletion, orphaning the old block data. The
// orphaned data can be reclaimed instantaneously via hole punching, or
// later via garbage collection. The latter is used when hole punching is
// not supported on the filesystem, or on next boot if there's a crash
// after deletion but before hole punching.
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
// The default container placement policy favors simplicity over
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

  ~LogBlockManager() override;

  Status Open(FsReport* report, MergeReport need_merage,
              std::atomic<int>* containers_processed,
              std::atomic<int>* containers_total) override;

  Status CreateBlock(const CreateBlockOptions& opts,
                     std::unique_ptr<WritableBlock>* block) override;

  Status OpenBlock(const BlockId& block_id,
                   std::unique_ptr<ReadableBlock>* block) override;

  bool FindBlockPath(const BlockId& block_id, std::string* path) const override;

  std::unique_ptr<BlockCreationTransaction> NewCreationTransaction() override;

  std::shared_ptr<BlockDeletionTransaction> NewDeletionTransaction() override;

  Status GetAllBlockIds(std::vector<BlockId>* block_ids) override;

  void NotifyBlockId(BlockId block_id) override;

  scoped_refptr<FsErrorManager> error_manager() override { return error_manager_; }

  std::string tenant_id() const override { return tenant_id_; }

 protected:
  // Note: all objects passed as pointers should remain alive for the lifetime
  // of the block manager.
  LogBlockManager(Env* env,
                  scoped_refptr<DataDirManager> dd_manager,
                  scoped_refptr<FsErrorManager> error_manager,
                  FileCache* file_cache,
                  BlockManagerOptions opts,
                  std::string tenant_id);

  FRIEND_TEST(LogBlockManagerNativeMetaTest,
              TestContainerBlockLimitingByMetadataSizeWithCompaction);
  FRIEND_TEST(LogBlockManagerNativeMetaTest, TestMetadataTruncation);
  FRIEND_TEST(LogBlockManagerTest, TestAbortBlock);
  FRIEND_TEST(LogBlockManagerTest, TestCloseFinalizedBlock);
  FRIEND_TEST(LogBlockManagerTest, TestCompactFullContainerMetadataAtStartup);
  FRIEND_TEST(LogBlockManagerTest, TestFinalizeBlock);
  FRIEND_TEST(LogBlockManagerTest, TestLIFOContainerSelection);
  FRIEND_TEST(LogBlockManagerTest, TestLookupBlockLimit);
  FRIEND_TEST(LogBlockManagerTest, TestMisalignedBlocksFuzz);
  FRIEND_TEST(LogBlockManagerTest, TestParseKernelRelease);
  FRIEND_TEST(LogBlockManagerTest, TestBumpBlockIds);
  FRIEND_TEST(LogBlockManagerTest, TestReuseBlockIds);
  FRIEND_TEST(LogBlockManagerTest, TestFailMultipleTransactionsPerContainer);

  friend class internal::LogBlockContainer;
  friend class internal::LogBlockContainerNativeMeta;
#if !defined(NO_ROCKSDB)
  friend class internal::LogBlockContainerRdbMeta;
#endif
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

  // Creates a new LogBlockContainer in 'dir', 'container' will be set as the newly created
  // container only if created successfully.
  //
  // Returns Status::OK() if created successfully, otherwise returns an error.
  virtual Status CreateContainer(Dir* dir, LogBlockContainerRefPtr* container) = 0;

  // Opens an existing LogBlockContainer in 'dir' identified by 'id', 'container' will be set as the
  // opened container only if created successfully.
  //
  // Returns Status::OK() if created successfully, otherwise returns an error, the error is recorded
  // in 'report'.
  virtual Status OpenContainer(Dir* dir,
                               FsReport* report,
                               const std::string& id,
                               LogBlockContainerRefPtr* container) = 0;

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
  // adds the block deletion metadata to record the on-disk deletion.
  // The 'log_blocks' out parameter will be set with the LogBlocks that were
  // successfully removed. The 'deleted' out parameter will be set with the
  // blocks were already deleted, e.g encountered 'NotFound' error during removal.
  //
  // Returns the first deletion failure that was seen, if any.
  Status RemoveLogBlocks(const std::vector<BlockId>& block_ids,
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
                const ContainerBlocksByName& low_live_block_containers);

  // Fetch all the containers we're going to repair.
  //
  // The issues to repair are described in 'report' and 'low_live_block_containers', the containers
  // are returned by 'containers_by_name'.
  virtual void FindContainersToRepair(FsReport* report,
                                      const ContainerBlocksByName& low_live_block_containers,
                                      ContainersByName* containers_by_name);

  // Do the repair work.
  //
  // The following repairs will be performed:
  // 1. Container data files in 'report->full_container_space_check' will be truncated.
  //
  // Returns an error if repairing a fatal inconsistency failed.
  virtual Status DoRepair(Dir* dir,
                          FsReport* report,
                          const ContainerBlocksByName& low_live_block_containers,
                          const ContainersByName& containers_by_name);

  // Rewrites a container metadata file, adding all entries in 'records'.
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
  //
  // If 'containers_processed' and 'containers_total' are not nullptr, they will
  // be populated with total containers attempted to be opened/processed and
  // total containers present respectively in the subsequent calls made to
  // the block manager.
  // TODO(achennaka): Implement a cleaner way to pass the atomic values to startup page
  // probably by using metrics.
  void OpenDataDir(Dir* dir,
                   std::vector<std::unique_ptr<internal::LogBlockContainerLoadResult>>* results,
                   Status* result_status,
                   std::atomic<int>* containers_processed = nullptr,
                   std::atomic<int>* containers_total = nullptr);

  // Estimate the count of containers according to the count of children directories.
  virtual size_t EstimateContainerCount(size_t children_count) const = 0;

  // Reads records from one log block container in the data directory.
  // The result details will be collected into 'result'.
  void LoadContainer(Dir* dir,
                     LogBlockContainerRefPtr container,
                     internal::LogBlockContainerLoadResult* result);

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
  scoped_refptr<DataDirManager> dd_manager_;

  // Manages callbacks used to handle disk failure.
  scoped_refptr<FsErrorManager> error_manager_;

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
                     std::optional<int64_t>> block_limits_by_data_dir_;

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
  std::atomic<uint64_t> next_block_id_;

  // Metrics for the block manager.
  //
  // May be null if instantiated without metrics.
  std::unique_ptr<internal::LogBlockManagerMetrics> metrics_;

  // Which tenant this log block manager belongs to.
  std::string tenant_id_;

  DISALLOW_COPY_AND_ASSIGN(LogBlockManager);
};

// The metadata part of a container is written to a native metadata file.
// The metadata file itself is not compacted, as it is expected to remain quite
// small even after many create/delete cycles.
//
// The on-disk container metadata design favors simplicity and contiguous
// access over space consumption and scalability to a very large number of
// blocks. To be more specific, the separation of metadata from data allows
// for high performance sustained reads at block manager open time at a
// manageability cost: a container is not a single file, and needs multiple
// open fds to be of use. Moreover, the log-structured nature of the
// metadata is simple and performant at open time.
class LogBlockManagerNativeMeta : public LogBlockManager {
 public:
  static constexpr const char* const name() { return "log"; }

  LogBlockManagerNativeMeta(Env* env,
                            scoped_refptr<DataDirManager> dd_manager,
                            scoped_refptr<FsErrorManager> error_manager,
                            FileCache* file_cache,
                            BlockManagerOptions opts,
                            std::string tenant_id)
      : LogBlockManager(env, std::move(dd_manager), std::move(error_manager),
                        file_cache, std::move(opts), std::move(tenant_id)) {
  }

 private:
  size_t EstimateContainerCount(size_t children_count) const override {
    return children_count / 2;
  }

  Status CreateContainer(Dir* dir, LogBlockContainerRefPtr* container) override;

  // Returns Status::Aborted() in the case that the metadata and data files
  // both appear to have no data (e.g. due to a crash just after creating
  // one of them but before writing any records).
  Status OpenContainer(Dir* dir,
                       FsReport* report,
                       const std::string& id,
                       LogBlockContainerRefPtr* container) override;

  void FindContainersToRepair(FsReport* report,
                              const ContainerBlocksByName& low_live_block_containers,
                              ContainersByName* containers_by_name) override;

  // Do the repair work.
  //
  // The following repairs will be performed:
  // 1. Repairs in LogBlockManager::DoRepair().
  // 2. Container metadata files in 'report->partial_record_check' will be truncated.
  // 3. Container data and metadata files in 'report->incomplete_container_check' will be deleted.
  // 4. Container metadata files in 'low_live_block_containers' will be compacted.
  //
  // Returns an error if repairing a fatal inconsistency failed.
  Status DoRepair(Dir* dir,
                  FsReport* report,
                  const ContainerBlocksByName& low_live_block_containers,
                  const ContainersByName& containers_by_name) override;

private:
  FRIEND_TEST(LogBlockManagerNativeMetaTest,
              TestContainerBlockLimitingByMetadataSizeWithCompaction);

  static bool ContainerShouldCompactForTests(internal::LogBlockContainer* container);
};

#if !defined(NO_ROCKSDB)
// All the container's metadata is written into a RocksDB instance which is
// shared by all containers in the same data directory.
// The metadata records in RocksDB are all of CREATE type, the records are
// deleted from RocksDB when the corresponding blocks are being removed from
// the block manager. The data in the RocksDB instance is compacted in
// background automatically, see https://github.com/facebook/rocksdb/wiki/Compaction.
//
// Comparing with the LogBlockManagerNativeMeta, the LogBlockManagerRdbMeta has a
// better performance on opening containers, the latter only contains live block
// records, while the former needs to scan through all the records in the metadata
// which may adversely affect the performance when the live block ratio is very low.
// RocksDB provides many configuration options, we can tune the performance, minimize
// the read/write and space amplification.
class LogBlockManagerRdbMeta : public LogBlockManager {
 public:
  static constexpr const char* const name() { return "logr"; }

  LogBlockManagerRdbMeta(Env* env,
                         scoped_refptr<DataDirManager> dd_manager,
                         scoped_refptr<FsErrorManager> error_manager,
                         FileCache* file_cache,
                         BlockManagerOptions opts,
                         std::string tenant_id)
      : LogBlockManager(env, std::move(dd_manager), std::move(error_manager),
                        file_cache, std::move(opts), std::move(tenant_id)) {
  }

 private:
  friend class internal::LogBlockContainerRdbMeta;
  friend class RdbMetadataLBMCorruptor;
  FRIEND_TEST(LogBlockManagerRdbMetaTest, TestHalfPresentContainer);

  size_t EstimateContainerCount(size_t children_count) const override {
    // TODO(yingchun): exclude the kRocksDBDirName directory when get the children count.
    return children_count;
  }

  Status CreateContainer(Dir* dir, LogBlockContainerRefPtr* container) override;

  // Returns Status::Aborted() in the case that the data part appears to have no
  // data (e.g. due to a crash just after creating it but before writing any
  // records).
  Status OpenContainer(Dir* dir,
                       FsReport* report,
                       const std::string& id,
                       LogBlockContainerRefPtr* container) override;

  void FindContainersToRepair(FsReport* report,
                              const ContainerBlocksByName& low_live_block_containers,
                              ContainersByName* containers_by_name) override;

  // Do the repair work.
  //
  // The following repairs will be performed:
  // 1. Repairs in LogBlockManager::DoRepair().
  // 2. Container data files in 'report->incomplete_container_check' will be deleted, and the
  //    records of these containers in RocksDB will be deleted as well.
  // 3. Container metadata records in 'report->corrupted_rdb_record_check' will be deleted from
  //    RocksDB.
  //
  // Returns an error if repairing a fatal inconsistency failed.
  Status DoRepair(Dir* dir,
                  FsReport* report,
                  const ContainerBlocksByName& low_live_block_containers,
                  const ContainersByName& containers_by_name) override;

  // Delete all the metadata part of a container.
  //
  // The metadata records are prefixed by the container's 'id' of the RocksDB instance in 'dir'.
  static Status DeleteContainerMetadata(Dir* dir, const std::string& id);

  // Construct the key used in RocksDB.
  //
  // The key is constructed by the container's 'container_id' and the block's 'block_id'.
  static std::string ConstructRocksDBKey(const std::string& container_id,
                                         const BlockId& block_id);
};
#endif

} // namespace fs
} // namespace kudu

