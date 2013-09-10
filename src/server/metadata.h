// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_METADATA_H
#define KUDU_TABLET_METADATA_H

#include <tr1/unordered_set>

#include <string>
#include <utility>
#include <vector>

#include "common/schema.h"
#include "gutil/atomicops.h"
#include "server/fsmanager.h"
#include "server/metadata.pb.h"
#include "util/env.h"
#include "util/locks.h"
#include "util/status.h"

namespace kudu {

// TODO: This is only used by tablet, maybe should be in the tablet namespace
namespace metadata {

class RowSetMetadata;

typedef std::vector<shared_ptr<RowSetMetadata> > RowSetMetadataVector;
typedef std::tr1::unordered_set<int64_t> RowSetMetadataIds;

// Manages the "blocks tracking" for the specified tablet.
//
// The Master will send the bootstrap information required to
// initialize the tablet (tablet id, master-block, start-key, end-key).
// The Tablet-Server will receive the open request and it will create
// the Tablet with its own metadata.
//
// TabletMetadata is owned by the Tablet, and the tablet should call
// Load() and Flush() when necessary.
class TabletMetadata {
 public:
  TabletMetadata(FsManager *fs_manager, const TabletMasterBlockPB& master_block,
                 const string& start_key = "", const string& end_key = "")
    : start_key_(start_key), end_key_(end_key),
      fs_manager_(fs_manager),
      master_block_(master_block),
      sblk_id_(0),
      next_rowset_idx_(0),
      last_durable_mrs_id_(-1) {
  }

  const string& oid() const { return master_block_.tablet_id(); }
  const string& start_key() const { return start_key_; }
  const string& end_key() const { return end_key_; }

  Status Create();
  Status Load();

  Status Flush() { return UpdateAndFlush(RowSetMetadataIds(), RowSetMetadataVector()); }
  Status UpdateAndFlush(const RowSetMetadataIds& to_remove, const RowSetMetadataVector& to_add);

  Status UpdateAndFlush(const RowSetMetadataIds& to_remove,
                        const RowSetMetadataVector& to_add,
                        int64_t last_durable_mrs_id);

  // Create a new RowSetMetadata for this tablet.
  Status CreateRowSet(shared_ptr<RowSetMetadata> *rowset, const Schema& schema);

  const RowSetMetadataVector& rowsets() const { return rowsets_; }

  FsManager *fs_manager() const { return fs_manager_; }

  int64_t lastest_durable_mrs_id() { return last_durable_mrs_id_; }

  // ==========================================================================
  // Stuff used by the tests
  // ==========================================================================
  const RowSetMetadata *GetRowSetForTests(int64_t id) const;

 private:
  Status ReadSuperBlock(TabletSuperBlockPB *pb);

 private:
  DISALLOW_COPY_AND_ASSIGN(TabletMetadata);

  Status UpdateAndFlushUnlocked(const RowSetMetadataIds& to_remove,
                                const RowSetMetadataVector& to_add,
                                int64_t last_durable_mrs_id);

  typedef simple_spinlock LockType;
  LockType lock_;

  string start_key_;
  string end_key_;
  FsManager *fs_manager_;
  RowSetMetadataVector rowsets_;

  TabletMasterBlockPB master_block_;
  uint64_t sblk_id_;

  base::subtle::Atomic64 next_rowset_idx_;

  int64_t last_durable_mrs_id_;
};


// Keeps tracks of the RowSet data blocks.
//
// Each tablet MemRowSet flush a new RowSetMetadata is created,
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
// create a new delta block, and flush the RowSetMetadata.
//
// Since the only mutable part of the RowSetMetadata is the Delta-Tracking,
// There's a lock around the delta-blocks operations.
class RowSetMetadata {
 public:

  RowSetMetadata(TabletMetadata *tablet_metadata, int64_t id, const Schema& schema)
    : id_(id), schema_(schema), tablet_metadata_(tablet_metadata) {
  }

  Status Create();
  Status Load(const RowSetDataPB& pb);
  Status Flush() { return tablet_metadata_->Flush(); }

  const string ToString();

  int64_t id() const { return id_; }

  const Schema& schema() const { return schema_; }

  Status OpenDataBlock(const BlockId& block_id, shared_ptr<RandomAccessFile> *reader, uint64_t *size) {
    RETURN_NOT_OK(fs_manager()->OpenBlock(block_id, reader));
    return (*reader)->Size(size);
  }

  Status NewBloomDataBlock(shared_ptr<WritableFile> *writer) {
    CHECK(bloom_block_.IsNull());
    return fs_manager()->CreateNewBlock(writer, &bloom_block_);
  }

  Status OpenBloomDataBlock(shared_ptr<RandomAccessFile> *reader, uint64_t *size) {
    return OpenDataBlock(bloom_block_, reader, size);
  }

  Status NewAdHocIndexDataBlock(shared_ptr<WritableFile> *writer) {
    CHECK(adhoc_index_block_.IsNull());
    return fs_manager()->CreateNewBlock(writer, &adhoc_index_block_);
  }

  Status OpenAdHocIndexDataBlock(shared_ptr<RandomAccessFile> *reader, uint64_t *size) {
    return OpenDataBlock(adhoc_index_block_, reader, size);
  }

  Status NewColumnDataBlock(size_t col_idx, shared_ptr<WritableFile> *writer) {
    BlockId block_id;
    CHECK_EQ(column_blocks_.size(), col_idx);
    RETURN_NOT_OK(fs_manager()->CreateNewBlock(writer, &block_id));
    column_blocks_.push_back(block_id);
    return Status::OK();
  }

  Status OpenColumnDataBlock(size_t col_idx, shared_ptr<RandomAccessFile> *reader, uint64_t *size) {
    DCHECK_LT(col_idx, column_blocks_.size());
    return OpenDataBlock(column_blocks_[col_idx], reader, size);
  }

  Status NewDeltaDataBlock(shared_ptr<WritableFile> *writer, BlockId *block_id) {
    return fs_manager()->CreateNewBlock(writer, block_id);
  }

  Status CommitDeltaDataBlock(int64_t id, const BlockId& block_id);

  Status OpenDeltaDataBlock(size_t index,
                            shared_ptr<RandomAccessFile> *reader,
                            uint64_t *size,
                            int64_t *id);

  size_t delta_blocks_count() const;

  TabletMetadata *tablet_metadata() const { return tablet_metadata_; }

  bool HasColumnDataBlockForTests(size_t idx) const {
    return column_blocks_.size() > idx && fs_manager()->BlockExists(column_blocks_[idx]);
  }

  bool HasBloomDataBlockForTests() const {
    return !bloom_block_.IsNull() && fs_manager()->BlockExists(bloom_block_);
  }

 private:
  RowSetMetadata(TabletMetadata *tablet_metadata, int32_t id)
    : id_(id), tablet_metadata_(tablet_metadata) {
  }

  FsManager *fs_manager() const { return tablet_metadata_->fs_manager(); }

  Status ToProtobuf(RowSetDataPB *pb);

 private:
  DISALLOW_COPY_AND_ASSIGN(RowSetMetadata);

  typedef simple_spinlock LockType;
  mutable LockType deltas_lock_;

  int64_t id_;
  Schema schema_;
  BlockId bloom_block_;
  BlockId adhoc_index_block_;
  std::vector<BlockId> column_blocks_;
  std::vector<std::pair<int64_t, BlockId> > delta_blocks_;
  TabletMetadata *tablet_metadata_;

  friend class TabletMetadata;
};

} // namespace metadata
} // namespace kudu

#endif
