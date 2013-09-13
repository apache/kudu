// Copyright (c) 2013, Cloudera, inc.

#include "server/metadata.h"

#include <glog/logging.h>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread/mutex.hpp>
#include <algorithm>
#include <utility>
#include <tr1/unordered_map>

#include "common/wire_protocol.h"
#include "gutil/map-util.h"
#include "server/metadata.pb.h"
#include "server/metadata_util.h"

namespace kudu {
namespace metadata {

using base::subtle::Barrier_AtomicIncrement;

const int64 kNoDurableMrs = -1;

// ============================================================================
//  Tablet Metadata
// ============================================================================
Status TabletMetadata::Create() {
  return Status::OK();
}

Status TabletMetadata::ReadSuperBlock(TabletSuperBlockPB *pb) {
  TabletSuperBlockPB pb2;
  Status sa, sb;

  // Try to read the block_a if exists
  sa = fs_manager_->ReadMetadataBlock(BlockId(master_block_.block_a()), pb);

  // Try to read the block_b if exists
  sb = fs_manager_->ReadMetadataBlock(BlockId(master_block_.block_b()), &pb2);

  // Both super-blocks are valid, pick the latest
  if (sa.ok() && sb.ok()) {
    if (pb->id() < pb2.id()) {
      *pb = pb2;
    }
    return Status::OK();
  }

  // block-a is valid, block-b is not (may not exists or be corrupted)
  if (sa.ok() && !sb.ok()) {
    return Status::OK();
  }

  // block-b is valid, block-a is not (may not exists or be corrupted)
  if (!sa.ok() && sb.ok()) {
    *pb = pb2;
    return Status::OK();
  }

  // No super-block found
  if (sa.IsNotFound() && sb.IsNotFound()) {
    return Status::NotFound("Tablet '" + oid() + "' SuperBlock not found",
                            master_block_.DebugString());
  }

  // Both super-blocks are corrupted
  if (sa.IsCorruption() && sb.IsCorruption()) {
    return Status::NotFound("Tablet '" + oid() + "' SuperBlocks are corrupted",
                            master_block_.DebugString());
  }

  return sa;
}

Status TabletMetadata::Load() {
  DCHECK_EQ(rowsets_.size(), 0);

  TabletSuperBlockPB pb;
  RETURN_NOT_OK(ReadSuperBlock(&pb));

  // Verify that the tablet id matches with the one in the protobuf
  if (oid() != pb.oid()) {
    return Status::Corruption("Expected id=" + oid() + " found " + pb.oid(),
                              master_block_.DebugString());
  }

  sblk_id_ = pb.id() + 1;
  start_key_ = pb.start_key();
  end_key_ = pb.end_key();
  last_durable_mrs_id_ = pb.last_durable_mrs_id();

  BOOST_FOREACH(const RowSetDataPB& rowset_pb, pb.rowsets()) {
    shared_ptr<RowSetMetadata> rowset_meta(new RowSetMetadata(this, rowsets_.size()));
    RETURN_NOT_OK(rowset_meta->Load(rowset_pb));
    next_rowset_idx_ = std::max(next_rowset_idx_, rowset_meta->id() + 1);
    rowsets_.push_back(rowset_meta);
  }

  return Status::OK();
}

Status TabletMetadata::UpdateAndFlush(const RowSetMetadataIds& to_remove,
                                      const RowSetMetadataVector& to_add,
                                      shared_ptr<TabletSuperBlockPB> *super_block) {
  boost::lock_guard<LockType> l(lock_);
  return UpdateAndFlushUnlocked(to_remove, to_add, last_durable_mrs_id_, super_block);
}

Status TabletMetadata::UpdateAndFlush(const RowSetMetadataIds& to_remove,
                                      const RowSetMetadataVector& to_add,
                                      int64_t last_durable_mrs_id,
                                      shared_ptr<TabletSuperBlockPB> *super_block) {
  boost::lock_guard<LockType> l(lock_);
  return UpdateAndFlushUnlocked(to_remove, to_add, last_durable_mrs_id, super_block);
}

Status TabletMetadata::UpdateAndFlushUnlocked(
    const RowSetMetadataIds& to_remove,
    const RowSetMetadataVector& to_add,
    int64_t last_durable_mrs_id,
    shared_ptr<TabletSuperBlockPB> *super_block) {
  DCHECK_GE(last_durable_mrs_id, last_durable_mrs_id_);

  RowSetMetadataVector new_rowsets = rowsets_;
  RowSetMetadataVector::iterator it = new_rowsets.begin();
  while (it != new_rowsets.end()) {
    if (ContainsKey(to_remove, (*it)->id())) {
      it = new_rowsets.erase(it);
    } else {
      it++;
    }
  }

  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& meta, to_add) {
    new_rowsets.push_back(meta);
  }

  shared_ptr<TabletSuperBlockPB> pb(new TabletSuperBlockPB());
  RETURN_NOT_OK(ToSuperBlockUnlocked(&pb, new_rowsets));

  // Flush
  BlockId a_blk(master_block_.block_a());
  BlockId b_blk(master_block_.block_b());
  if (sblk_id_ & 1) {
    RETURN_NOT_OK(fs_manager_->WriteMetadataBlock(a_blk, *(pb.get())));
    fs_manager_->DeleteBlock(b_blk);
  } else {
    RETURN_NOT_OK(fs_manager_->WriteMetadataBlock(b_blk, *(pb.get())));
    fs_manager_->DeleteBlock(a_blk);
  }

  sblk_id_++;
  rowsets_ = new_rowsets;
  if (super_block != NULL) {
    super_block->swap(pb);
  }
  return Status::OK();
}

Status TabletMetadata::ToSuperBlock(shared_ptr<TabletSuperBlockPB> *super_block) {
  // acquire the lock so that rowsets_ doesn't get changed until we're finished.
  boost::lock_guard<LockType> l(lock_);
  return ToSuperBlockUnlocked(super_block, rowsets_);
}

Status TabletMetadata::ToSuperBlockUnlocked(shared_ptr<TabletSuperBlockPB> *super_block,
                                            const RowSetMetadataVector& rowsets) {

  // Convert to protobuf
  gscoped_ptr<TabletSuperBlockPB> pb(new TabletSuperBlockPB());
  pb->set_id(sblk_id_);
  pb->set_oid(oid());
  pb->set_start_key(start_key_);
  pb->set_end_key(end_key_);
  pb->set_last_durable_mrs_id(last_durable_mrs_id_);

  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& meta, rowsets) {
    meta->ToProtobuf(pb->add_rowsets());
  }

  super_block->reset(pb.release());
  return Status::OK();
}

Status TabletMetadata::CreateRowSet(shared_ptr<RowSetMetadata> *rowset, const Schema& schema) {
  AtomicWord rowset_idx = Barrier_AtomicIncrement(&next_rowset_idx_, 1) - 1;
  RowSetMetadata *meta = new RowSetMetadata(this, rowset_idx, schema);
  rowset->reset(meta);
  return Status::OK();
}

const RowSetMetadata *TabletMetadata::GetRowSetForTests(int64_t id) const {
  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& rowset_meta, rowsets_) {
    if (rowset_meta->id() == id) {
      return rowset_meta.get();
    }
  }
  return NULL;
}

// ============================================================================
//  RowSet Metadata
// ============================================================================
Status RowSetMetadata::Create() {
  return Status::OK();
}

Status RowSetMetadata::Load(const RowSetDataPB& pb) {
  id_ = pb.id();

  // Load Bloom File
  if (pb.has_bloom_block()) {
    bloom_block_ = BlockIdFromPB(pb.bloom_block());
  }

  // Load AdHoc Index File
  if (pb.has_adhoc_index_block()) {
    adhoc_index_block_ = BlockIdFromPB(pb.adhoc_index_block());
  }

  // Load Column Files
  int key_columns = 0;
  std::vector<ColumnSchema> cols;
  BOOST_FOREACH(const ColumnDataPB& col_pb, pb.columns()) {
    column_blocks_.push_back(BlockIdFromPB(col_pb.block()));
    cols.push_back(ColumnSchemaFromPB(col_pb.schema()));
    key_columns += !!col_pb.schema().is_key();
  }
  RETURN_NOT_OK(schema_.Reset(cols, key_columns));

  // Load Delta Files
  BOOST_FOREACH(const DeltaDataPB& delta_pb, pb.deltas()) {
    delta_blocks_.push_back(
      std::pair<int64_t, BlockId>(delta_pb.id(), BlockIdFromPB(delta_pb.block())));
  }

  return Status::OK();
}

Status RowSetMetadata::ToProtobuf(RowSetDataPB *pb) {
  pb->set_id(id_);

  // Write Column Files
  size_t idx = 0;
  BOOST_FOREACH(const BlockId& block_id, column_blocks_) {
    ColumnDataPB *col_data = pb->add_columns();
    col_data->set_id(idx);
    BlockIdToPB(block_id, col_data->mutable_block());
    ColumnSchemaToPB(schema_.column(idx), col_data->mutable_schema());
    idx++;
  }

  // Write Delta Files
  {
    typedef std::pair<uint32_t, BlockId> DeltaDataBlock;

    boost::lock_guard<LockType> l(deltas_lock_);
    BOOST_FOREACH(const DeltaDataBlock& delta_block, delta_blocks_) {
      DeltaDataPB *delta_pb = pb->add_deltas();
      delta_pb->set_id(delta_block.first);
      BlockIdToPB(delta_block.second, delta_pb->mutable_block());
    }
  }

  // Write Bloom File
  if (!bloom_block_.IsNull()) {
    BlockIdToPB(bloom_block_, pb->mutable_bloom_block());
  }

  // Write AdHoc Index
  if (!adhoc_index_block_.IsNull()) {
    BlockIdToPB(adhoc_index_block_, pb->mutable_adhoc_index_block());
  }

  return Status::OK();
}

const string RowSetMetadata::ToString() {
  return "RowSet(" + boost::lexical_cast<string>(id_) + ")";
}

Status RowSetMetadata::AtomicRemoveDeltaDataBlocks(size_t start_idx, size_t end_idx,
                                                   const vector<int64_t>& ids) {
  CHECK_EQ(end_idx - start_idx + 1, ids.size());
  CHECK_GE(end_idx, start_idx);
  boost::lock_guard<LockType> l(deltas_lock_);

  // First check that we're indeed removing the delta blocks for the delta stores we've
  // compacted and no other thread has modified this in the mean time. This should always
  // be true, hence the use of CHECK_EQ().
  vector<std::pair<int64_t, BlockId> >::iterator deltas_start = delta_blocks_.begin() + start_idx;
  vector<std::pair<int64_t, BlockId> >::iterator deltas_end = delta_blocks_.begin() + end_idx;
  vector<std::pair<int64_t, BlockId> >::const_iterator deltas_it = deltas_start;
  for (int id_idx = 0; deltas_it != deltas_end; ++id_idx, ++deltas_it) {
    CHECK_EQ(deltas_it->first, ids[id_idx]);
  }

  // Now we can safely remove the old deltas
  delta_blocks_.erase(deltas_start, deltas_end + 1);
  return Status::OK();
}

Status RowSetMetadata::CommitDeltaDataBlock(int64_t id, const BlockId& block_id) {
  boost::lock_guard<LockType> l(deltas_lock_);
  delta_blocks_.push_back(std::pair<int64_t, BlockId>(id, block_id));
  return Status::OK();
}

Status RowSetMetadata::OpenDeltaDataBlock(size_t index,
                                          shared_ptr<RandomAccessFile> *reader,
                                          uint64_t *size,
                                          int64_t *id) {
  boost::lock_guard<LockType> l(deltas_lock_);
  *id = delta_blocks_[index].first;
  return OpenDataBlock(delta_blocks_[index].second, reader, size);
}

size_t RowSetMetadata::delta_blocks_count() const {
  boost::lock_guard<LockType> l(deltas_lock_);
  return delta_blocks_.size();
}

} // namespace metadata
} // namespace kudu
