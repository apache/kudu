// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_TABLET_ROWSET_METADATA_H
#define KUDU_TABLET_ROWSET_METADATA_H

#include <boost/thread/locks.hpp>
#include <string>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/macros.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/util/env.h"

namespace kudu {

namespace tools {
class FsTool;
} // namespace tools

namespace tablet {

class RowSetMetadataUpdate;
class TabletMetadata;

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
// Metadata writeback can be lazy: usage should generally be:
//
//   1) create new files on disk (durably)
//   2) change in-memory state to point to new files
//   3) make corresponding change in RowSetMetadata in-memory
//   4) trigger asynchronous flush
//
//   callback: when metadata has been written:
//   1) remove old data files from disk
//   2) remove log anchors corresponding to previously in-memory data
//
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

  Status Flush();

  const std::string ToString() const;

  int64_t id() const { return id_; }

  const Schema& schema() const { return schema_; }

  void set_bloom_block(const BlockId& block_id) {
    DCHECK(bloom_block_.IsNull());
    bloom_block_ = block_id;
  }

  void set_adhoc_index_block(const BlockId& block_id) {
    DCHECK(adhoc_index_block_.IsNull());
    adhoc_index_block_ = block_id;
  }

  void SetColumnDataBlocks(const std::vector<BlockId>& blocks);

  Status CommitRedoDeltaDataBlock(int64_t dms_id, const BlockId& block_id);

  Status CommitUndoDeltaDataBlock(const BlockId& block_id);

  BlockId bloom_block() const {
    return bloom_block_;
  }

  BlockId adhoc_index_block() const {
    return adhoc_index_block_;
  }

  BlockId column_data_block(int col_idx) {
    DCHECK_LT(col_idx, column_blocks_.size());
    return column_blocks_[col_idx];
  }

  vector<BlockId> redo_delta_blocks() const {
    boost::lock_guard<LockType> l(deltas_lock_);
    return redo_delta_blocks_;
  }

  vector<BlockId> undo_delta_blocks() const {
    boost::lock_guard<LockType> l(deltas_lock_);
    return undo_delta_blocks_;
  }

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

} // namespace tablet
} // namespace kudu
#endif /* KUDU_TABLET_ROWSET_METADATA_H */
