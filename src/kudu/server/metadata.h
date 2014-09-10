// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_METADATA_H
#define KUDU_TABLET_METADATA_H

#include <tr1/unordered_set>

#include <boost/thread/locks.hpp>
#include <string>
#include <utility>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/server/metadata.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {

namespace tools {
class FsTool;
}

namespace tablet {
class BootstrapTest;
}

using tablet::BootstrapTest;

// TODO: This is only used by tablet, maybe should be in the tablet namespace
namespace metadata {

class RowSetMetadata;
class RowSetMetadataUpdate;

typedef std::vector<shared_ptr<RowSetMetadata> > RowSetMetadataVector;
typedef std::tr1::unordered_set<int64_t> RowSetMetadataIds;
typedef std::vector<size_t> ColumnIndexes;
typedef std::tr1::unordered_map<size_t, shared_ptr<WritableFile> > ColumnWriters;

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
                          const QuorumPB& quorum,
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
                                   const metadata::TabletMasterBlockPB& pb);


  // Try to load an existing tablet. If it does not exist, create it.
  // If it already existed, verifies that the schema of the tablet matches the
  // provided 'schema'.
  //
  // This is mostly useful for tests which instantiate tablets directly.
  static Status LoadOrCreate(FsManager* fs_manager,
                             const TabletMasterBlockPB& master_block,
                             const std::string& table_name,
                             const Schema& schema,
                             const QuorumPB& quorum,
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

  void SetQuorum(const QuorumPB& quorum);

  // Return the current quorum config.
  // Note that this returns a copy so should not be used in a tight loop.
  QuorumPB Quorum() const;

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

  Status UpdateAndFlush(const RowSetMetadataIds& to_remove,
                        const RowSetMetadataVector& to_add,
                        shared_ptr<TabletSuperBlockPB> *super_block);

  // Updates the metadata adding 'to_add' rowsets, removing 'to_remove' rowsets
  // and updating the last durable MemRowSet. If 'super_block' is not NULL it
  // will be set to the newly created TabletSuperBlockPB.
  Status UpdateAndFlush(const RowSetMetadataIds& to_remove,
                        const RowSetMetadataVector& to_add,
                        int64_t last_durable_mrs_id,
                        shared_ptr<TabletSuperBlockPB> *super_block);

  // Create a new RowSetMetadata for this tablet.
  // Does not add the new rowset to the list of rowsets. Use one of the Update()
  // calls to do so.
  Status CreateRowSet(shared_ptr<RowSetMetadata> *rowset, const Schema& schema);

  // Sets 'dst' to a new RowSetMetadata which differs from 'src' RowSetMetadata
  // only by columns in 'col_indexes': new data blocks are created for columns in
  // 'col_indexes', while other columns -- as well as the bloom and ad hoc index blocks --
  // are shared with 'src'; sets 'writers' to a map from each column index in to a new
  // WritableFile representing the data writer for that column.
  Status CreateRowSetWithUpdatedColumns(const ColumnIndexes& col_indexes,
                                        const RowSetMetadata& src,
                                        shared_ptr<RowSetMetadata>* dst,
                                        ColumnWriters* writers);

  const RowSetMetadataVector& rowsets() const { return rowsets_; }

  FsManager *fs_manager() const { return fs_manager_; }

  int64_t last_durable_mrs_id() { return last_durable_mrs_id_; }

  void SetLastDurableMrsIdForTests(int64_t mrs_id) { last_durable_mrs_id_ = mrs_id; }

  // Creates a TabletSuperBlockPB that reflects the current tablet metadata
  // and sets 'super_block' to it.
  // FIXME: This should probably accept a gscoped_ptr*.
  Status ToSuperBlock(shared_ptr<TabletSuperBlockPB> *super_block) const;

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
                 const QuorumPB& quorum,
                 const std::string& start_key,
                 const std::string& end_key,
                 const TabletBootstrapStatePB& remote_bootstrap_state);

  // Constructor for loading an existing tablet.
  TabletMetadata(FsManager *fs_manager, const TabletMasterBlockPB& master_block);

  Status LoadFromDisk();

  // Update state of metadata to that of the given superblock PB.
  Status LoadFromSuperBlockUnlocked(const TabletSuperBlockPB& superblock);

  Status ReadSuperBlock(TabletSuperBlockPB *pb);

  // Fully replace superblock.
  // Calling thread must hold lock_.
  Status ReplaceSuperBlockUnlocked(const TabletSuperBlockPB &pb);

  Status UpdateAndFlushUnlocked(const RowSetMetadataIds& to_remove,
                                const RowSetMetadataVector& to_add,
                                int64_t last_durable_mrs_id,
                                shared_ptr<TabletSuperBlockPB> *super_block);

  Status ToSuperBlockUnlocked(shared_ptr<TabletSuperBlockPB> *super_block,
                              const RowSetMetadataVector& rowsets) const;

  enum State {
    kNotLoadedYet,
    kNotWrittenYet,
    kInitialized
  };
  State state_;

  typedef simple_spinlock LockType;
  mutable LockType lock_;

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

  metadata::QuorumPB quorum_;

  // The current state of remote bootstrap for the tablet.
  TabletBootstrapStatePB remote_bootstrap_state_;

  // If this counter is > 0 then Flush() will not write any data to
  // disk.
  int32_t num_flush_pins_;

  // Set if Flush() is called when num_flush_pins_ is > 0; if true,
  // then next UnPinFlush will call Flush() again to ensure the
  // metadata is persisted.
  bool needs_flush_;

  DISALLOW_COPY_AND_ASSIGN(TabletMetadata);
};


// Keeps tracks of the RowSet data blocks.
//
// On each tablet MemRowSet flush, a new RowSetMetadata is created,
// and the DiskRowSetWriter will create and write the "immutable" blocks for
// columns, bloom filter and adHoc-Index.
//
// Once the flush is completed and all the blocks are written,
// the RowSetMetadata will be flushed. Currently, there is only a block
// containing all the tablet metadata, so flushing the RowSetMetadata will
// trigger a full TabletMetadata flush.
//
// The RowSet has also a mutable part, the Delta Blocks, which contains
// the chain of updates applied to the "immutable" data in the RowSet.
// The DeltaTracker is responsible for flushing the Delta-Memstore,
// creating a new delta block, and flushing the RowSetMetadata.
//
// Since the only mutable part of the RowSetMetadata is the Delta-Tracking,
// There's a lock around the delta-blocks operations.
class RowSetMetadata {
 public:
  // Create a new RowSetMetadata
  static Status CreateNew(TabletMetadata* tablet_metadata,
                          int64_t id,
                          const Schema& schema,
                          gscoped_ptr<RowSetMetadata>* metadata);

  // Load metadata from a protobuf which was previously read from disk.
  static Status Load(TabletMetadata* tablet_metadata,
                     const RowSetDataPB& pb,
                     gscoped_ptr<RowSetMetadata>* metadata);

  Status Flush() { return tablet_metadata_->Flush(); }

  const std::string ToString() const;

  int64_t id() const { return id_; }

  const Schema& schema() const { return schema_; }

  // TODO: this is just a simple wrapper around FsManager.
  // We should provide this wrapper as part of FsManager and make this a pure
  // metadata container, without trying to funnel FS operations through it.
  Status OpenDataBlock(const BlockId& block_id,
                       shared_ptr<RandomAccessFile> *reader) {
    return fs_manager()->OpenBlock(block_id, reader);
  }

  Status NewBloomDataBlock(shared_ptr<WritableFile> *writer) {
    CHECK(bloom_block_.IsNull());
    return fs_manager()->CreateNewBlock(writer, &bloom_block_);
  }

  Status OpenBloomDataBlock(shared_ptr<RandomAccessFile> *reader) {
    return OpenDataBlock(bloom_block_, reader);
  }

  Status NewAdHocIndexDataBlock(shared_ptr<WritableFile> *writer) {
    CHECK(adhoc_index_block_.IsNull());
    return fs_manager()->CreateNewBlock(writer, &adhoc_index_block_);
  }

  Status OpenAdHocIndexDataBlock(shared_ptr<RandomAccessFile> *reader) {
    return OpenDataBlock(adhoc_index_block_, reader);
  }

  Status NewColumnDataBlock(size_t col_idx, shared_ptr<WritableFile> *writer) {
    BlockId block_id;
    CHECK_EQ(column_blocks_.size(), col_idx);
    RETURN_NOT_OK(fs_manager()->CreateNewBlock(writer, &block_id));
    column_blocks_.push_back(block_id);
    return Status::OK();
  }

  void SetColumnDataBlocks(const std::vector<BlockId>& blocks);

  Status OpenColumnDataBlock(size_t col_idx, shared_ptr<RandomAccessFile> *reader) {
    DCHECK_LT(col_idx, column_blocks_.size());
    return OpenDataBlock(column_blocks_[col_idx], reader);
  }

  Status NewDeltaDataBlock(shared_ptr<WritableFile> *writer, BlockId *block_id) {
    return fs_manager()->CreateNewBlock(writer, block_id);
  }

  Status CommitRedoDeltaDataBlock(int64_t dms_id,
                                  const BlockId& block_id);

  Status OpenRedoDeltaDataBlock(size_t index,
                                shared_ptr<RandomAccessFile> *reader,
                                uint64_t *size);

  vector<BlockId> redo_delta_blocks() const {
    boost::lock_guard<LockType> l(deltas_lock_);
    return redo_delta_blocks_;
  }

  vector<BlockId> undo_delta_blocks() const {
    boost::lock_guard<LockType> l(deltas_lock_);
    return undo_delta_blocks_;
  }

  Status CommitUndoDeltaDataBlock(const BlockId& block_id);

  Status OpenUndoDeltaDataBlock(size_t index,
                                shared_ptr<RandomAccessFile> *reader,
                                uint64_t *size);

  TabletMetadata *tablet_metadata() const { return tablet_metadata_; }

  int64_t last_durable_redo_dms_id() const { return last_durable_redo_dms_id_; }

  void SetLastDurableRedoDmsIdForTests(int64_t redo_dms_id) {
    last_durable_redo_dms_id_ = redo_dms_id;
  }

  bool HasColumnDataBlockForTests(size_t idx) const {
    return column_blocks_.size() > idx && fs_manager()->BlockExists(column_blocks_[idx]);
  }

  bool HasBloomDataBlockForTests() const {
    return !bloom_block_.IsNull() && fs_manager()->BlockExists(bloom_block_);
  }

  bool HasUndoDeltaBlockForTests(size_t idx) const {
    return undo_delta_blocks_.size() > idx &&
        fs_manager()->BlockExists(undo_delta_blocks_[idx]);
  }

  FsManager *fs_manager() const { return tablet_metadata_->fs_manager(); }

  // Atomically commit a set of changes to this object.
  Status CommitUpdate(const RowSetMetadataUpdate& update);

 private:
  explicit RowSetMetadata(TabletMetadata *tablet_metadata)
    : initted_(false),
      tablet_metadata_(tablet_metadata),
      last_durable_redo_dms_id_(kNoDurableMemStore) {
  }

  RowSetMetadata(TabletMetadata *tablet_metadata,
                 int64_t id, const Schema& schema)
    : initted_(true),
      id_(id),
      schema_(schema),
      tablet_metadata_(tablet_metadata),
      last_durable_redo_dms_id_(kNoDurableMemStore) {
    CHECK(schema.has_column_ids());
  }

  Status InitFromPB(const RowSetDataPB& pb);

  void ToProtobuf(RowSetDataPB *pb);

 private:
  DISALLOW_COPY_AND_ASSIGN(RowSetMetadata);

  bool initted_; // TODO initme


  typedef simple_spinlock LockType;
  mutable LockType deltas_lock_;

  const BlockId& column_block(size_t col_idx) const {
    return column_blocks_[col_idx];
  }

  int64_t id_;
  Schema schema_;
  BlockId bloom_block_;
  BlockId adhoc_index_block_;
  std::vector<BlockId> column_blocks_;
  std::vector<BlockId> redo_delta_blocks_;
  std::vector<BlockId> undo_delta_blocks_;
  TabletMetadata *tablet_metadata_;

  int64_t last_durable_redo_dms_id_;

  friend class TabletMetadata;
  friend class kudu::tools::FsTool;
};

// A set up of updates to be made to a RowSetMetadata object.
// Updates can be collected here, and then atomically applied to a RowSetMetadata
// using the CommitUpdate() function.
class RowSetMetadataUpdate {
 public:
  RowSetMetadataUpdate();
  ~RowSetMetadataUpdate();

  // Replace the subsequence of redo delta blocks with the new (compacted) delta blocks.
  // The replaced blocks must be a contiguous subsequence of the the full list,
  // since delta files cannot overlap in time.
  // 'to_add' may be empty, in which case the blocks in to_remove are simply removed
  // with no replacement.
  RowSetMetadataUpdate& ReplaceRedoDeltaBlocks(const std::vector<BlockId>& to_remove,
                                               const std::vector<BlockId>& to_add);

  RowSetMetadataUpdate& ReplaceColumnBlock(int col_idx, const BlockId& block_id);

 private:
  friend class RowSetMetadata;
  std::tr1::unordered_map<int, BlockId> cols_to_replace_;
  std::vector<BlockId> new_redo_blocks_;

  struct ReplaceDeltaBlocks {
    std::vector<BlockId> to_remove;
    std::vector<BlockId> to_add;
  };
  std::vector<ReplaceDeltaBlocks> replace_redo_blocks_;

  DISALLOW_COPY_AND_ASSIGN(RowSetMetadataUpdate);
};

} // namespace metadata
} // namespace kudu

#endif
