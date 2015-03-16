// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TABLET_TABLET_METADATA_H
#define KUDU_TABLET_TABLET_METADATA_H

#include <string>
#include <tr1/memory>
#include <tr1/unordered_set>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

namespace kudu {
namespace tablet {

class RowSetMetadata;
class RowSetMetadataUpdate;

typedef std::vector<std::tr1::shared_ptr<RowSetMetadata> > RowSetMetadataVector;
typedef std::tr1::unordered_set<int64_t> RowSetMetadataIds;
typedef std::vector<size_t> ColumnIndexes;


extern const int64 kNoDurableMemStore;

// Manages the "blocks tracking" for the specified tablet.
//
// TabletMetadata is owned by the Tablet. As new blocks are written to store
// the tablet's data, the Tablet calls Flush() to persist the block list
// on disk.
//
// At startup, the TSTabletManager will load a TabletMetadata for each
// master block found in the master block directory, and then instantiate
// tablets from this data.
class TabletMetadata : public RefCountedThreadSafe<TabletMetadata> {
 public:
  // Create metadata for a new tablet. This assumes that the given master block
  // has not been written before, and writes out the initial superblock with
  // the provided parameters.
  //
  // TODO: should this actually just generate a new unique master block, rather
  // than take as a parameter?
  static Status CreateNew(FsManager* fs_manager,
                          const TabletMasterBlockPB& master_block,
                          const std::string& table_name,
                          const Schema& schema,
                          const std::string& start_key, const std::string& end_key,
                          const TabletBootstrapStatePB& initial_remote_bootstrap_state,
                          scoped_refptr<TabletMetadata>* metadata);

  // Load existing metadata from disk given a master block.
  static Status Load(FsManager* fs_manager,
                     const TabletMasterBlockPB& master_block,
                     scoped_refptr<TabletMetadata>* metadata);

  // Load a tablet's master block from the file system.
  static Status OpenMasterBlock(Env* env,
                                const std::string& master_block_path,
                                const std::string& expected_tablet_id,
                                TabletMasterBlockPB* master_block);

  // Write the given master block onto the file system.
  static Status PersistMasterBlock(FsManager* fs,
                                   const TabletMasterBlockPB& pb);


  // Try to load an existing tablet. If it does not exist, create it.
  // If it already existed, verifies that the schema of the tablet matches the
  // provided 'schema'.
  //
  // This is mostly useful for tests which instantiate tablets directly.
  static Status LoadOrCreate(FsManager* fs_manager,
                             const TabletMasterBlockPB& master_block,
                             const std::string& table_name,
                             const Schema& schema,
                             const std::string& start_key, const std::string& end_key,
                             const TabletBootstrapStatePB& initial_remote_bootstrap_state,
                             scoped_refptr<TabletMetadata>* metadata);

  const std::string& oid() const {
    DCHECK_NE(state_, kNotLoadedYet);
    return master_block_.tablet_id();
  }
  const std::string& start_key() const {
    DCHECK_NE(state_, kNotLoadedYet);
    return start_key_;
  }
  const std::string& end_key() const {
    DCHECK_NE(state_, kNotLoadedYet);
    return end_key_;
  }

  const std::string& table_id() const {
    DCHECK_NE(state_, kNotLoadedYet);
    return master_block_.table_id();
  }

  const std::string& table_name() const;

  uint32_t schema_version() const;

  void SetSchema(const Schema& schema, uint32_t version);

  void SetTableName(const std::string& table_name);

  // Return the current schema of the metadata. Note that this returns
  // a copy so should not be used in a tight loop.
  Schema schema() const;

  // Update the remote bootstrapping state.
  void set_remote_bootstrap_state(TabletBootstrapStatePB state);

  // Return the remote bootstrapping state.
  TabletBootstrapStatePB remote_bootstrap_state() const;

  // Increments flush pin count by one: if flush pin count > 0,
  // metadata will _not_ be flushed to disk during Flush().
  void PinFlush();

  // Decrements flush pin count by one: if flush pin count is zero,
  // metadata will be flushed to disk during the next call to Flush()
  // or -- if Flush() had been called after a call to PinFlush() but
  // before this method was called -- Flush() will be called inside
  // this method.
  Status UnPinFlush();

  Status Flush();

  // Updates the metadata in the following ways:
  // 1. Adds rowsets from 'to_add'.
  // 2. Removes rowsets from 'to_remove'.
  // 3. Adds orphaned blocks from 'to_remove'.
  // 4. Updates the last durable MRS ID from 'last_durable_mrs_id',
  //    assuming it's not kNoMrsFlushed.
  static const int64_t kNoMrsFlushed = -1;
  Status UpdateAndFlush(const RowSetMetadataIds& to_remove,
                        const RowSetMetadataVector& to_add,
                        int64_t last_durable_mrs_id);

  // Adds the blocks referenced by 'block_ids' to 'orphaned_blocks'. They
  // are deleted from disk (and the list cleared) in a call to
  // DeleteOrphanedBlocks().
  void AddOrphanedBlocks(const std::vector<BlockId>& block_ids);

  // Create a new RowSetMetadata for this tablet.
  // Does not add the new rowset to the list of rowsets. Use one of the Update()
  // calls to do so.
  Status CreateRowSet(std::tr1::shared_ptr<RowSetMetadata> *rowset, const Schema& schema);

  const RowSetMetadataVector& rowsets() const { return rowsets_; }

  FsManager *fs_manager() const { return fs_manager_; }

  int64_t last_durable_mrs_id() { return last_durable_mrs_id_; }

  void SetLastDurableMrsIdForTests(int64_t mrs_id) { last_durable_mrs_id_ = mrs_id; }

  void SetPreFlushCallback(StatusClosure callback) { pre_flush_callback_ = callback; }

  // Sets *super_block to the serialized form of the current metadata.
  Status ToSuperBlock(TabletSuperBlockPB* super_block) const;

  // Fully replace a superblock (used for bootstrap).
  Status ReplaceSuperBlock(const TabletSuperBlockPB &pb);

  // ==========================================================================
  // Stuff used by the tests
  // ==========================================================================
  const RowSetMetadata *GetRowSetForTests(int64_t id) const;

  RowSetMetadata *GetRowSetForTests(int64_t id);

 private:
  friend class RefCountedThreadSafe<TabletMetadata>;

  // Compile time assert that no one deletes TabletMetadata objects.
  ~TabletMetadata();

  // TODO: get rid of this many-arg constructor in favor of a Load() and
  // New() factory functions -- it's sort of weird that when you're loading
  // from a master block, you're expected to pass in schema/start_key/end_key,
  // which are themselves already stored in the superblock as well.

  // Constructor for creating a new tablet.
  TabletMetadata(FsManager *fs_manager,
                 const TabletMasterBlockPB& master_block,
                 const std::string& table_name,
                 const Schema& schema,
                 const std::string& start_key,
                 const std::string& end_key,
                 const TabletBootstrapStatePB& remote_bootstrap_state);

  // Constructor for loading an existing tablet.
  TabletMetadata(FsManager *fs_manager, const TabletMasterBlockPB& master_block);

  Status LoadFromDisk();

  // Update state of metadata to that of the given superblock PB.
  Status LoadFromSuperBlock(const TabletSuperBlockPB& superblock);

  Status ReadSuperBlock(TabletSuperBlockPB *pb);

  // Fully replace superblock.
  // Requires 'flush_lock_'.
  Status ReplaceSuperBlockUnlocked(const TabletSuperBlockPB &pb);

  // Requires 'data_lock_'.
  Status UpdateUnlocked(const RowSetMetadataIds& to_remove,
                        const RowSetMetadataVector& to_add,
                        int64_t last_durable_mrs_id);

  // Requires 'data_lock_'.
  Status ToSuperBlockUnlocked(TabletSuperBlockPB* super_block,
                              const RowSetMetadataVector& rowsets) const;

  // Requires 'data_lock_'.
  void AddOrphanedBlocksUnlocked(const std::vector<BlockId>& block_ids);

  // Deletes all blocks found in 'orphaned_blocks_'. Those deleted (or not
  // found) will be removed from the list.
  //
  // Failures are logged, but are not fatal.
  void DeleteOrphanedBlocks();

  enum State {
    kNotLoadedYet,
    kNotWrittenYet,
    kInitialized
  };
  State state_;

  // Lock protecting the underlying data.
  typedef simple_spinlock LockType;
  mutable LockType data_lock_;

  // Lock protecting flushing the data to disk.
  // If taken together with 'data_lock_', must be acquired first.
  mutable Mutex flush_lock_;

  std::string start_key_;
  std::string end_key_;

  FsManager *fs_manager_;
  RowSetMetadataVector rowsets_;

  TabletMasterBlockPB master_block_;
  uint64_t sblk_sequence_;

  base::subtle::Atomic64 next_rowset_idx_;

  int64_t last_durable_mrs_id_;

  Schema schema_;
  uint32_t schema_version_;
  std::string table_name_;

  // Protected by 'data_lock_'.
  std::vector<BlockId> orphaned_blocks_;

  // The current state of remote bootstrap for the tablet.
  TabletBootstrapStatePB remote_bootstrap_state_;

  // If this counter is > 0 then Flush() will not write any data to
  // disk.
  int32_t num_flush_pins_;

  // Set if Flush() is called when num_flush_pins_ is > 0; if true,
  // then next UnPinFlush will call Flush() again to ensure the
  // metadata is persisted.
  bool needs_flush_;

  // A callback that, if set, is called before this metadata is flushed
  // to disk.
  StatusClosure pre_flush_callback_;

  DISALLOW_COPY_AND_ASSIGN(TabletMetadata);
};


} // namespace tablet
} // namespace kudu
#endif /* KUDU_TABLET_TABLET_METADATA_H */
