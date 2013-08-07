// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread/mutex.hpp>
#include <algorithm>
#include <utility>
#include <tr1/unordered_map>

#include "gutil/map-util.h"
#include "server/metadata.pb.h"
#include "server/metadata.h"
#include "server/metadata_util.h"

namespace kudu {
namespace metadata {

using base::subtle::Barrier_AtomicIncrement;

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
    return Status::NotFound("Tablet '" + oid_ + "' SuperBlock not found",
                            master_block_.DebugString());
  }

  // Both super-blocks are corrupted
  if (sa.IsCorruption() && sb.IsCorruption()) {
    return Status::NotFound("Tablet '" + oid_ + "' SuperBlocks are corrupted",
                            master_block_.DebugString());
  }

  return sa;
}

Status TabletMetadata::Load() {
  DCHECK_EQ(rowsets_.size(), 0);

  TabletSuperBlockPB pb;
  RETURN_NOT_OK(ReadSuperBlock(&pb));

  // Verify that the tablet id matches with the one in the protobuf
  if (oid_ != pb.oid()) {
    return Status::Corruption("Expected id=" + oid_ + " found " + pb.oid(),
                              master_block_.DebugString());
  }

  sblk_id_ = pb.id() + 1;
  start_key_ = pb.start_key();
  end_key_ = pb.end_key();

  BOOST_FOREACH(const RowSetDataPB& rowset_pb, pb.rowsets()) {
    shared_ptr<RowSetMetadata> rowset_meta(new RowSetMetadata(this, rowsets_.size()));
    RETURN_NOT_OK(rowset_meta->Load(rowset_pb));
    next_rowset_idx_ = std::max(next_rowset_idx_, rowset_meta->id() + 1);
    rowsets_.push_back(rowset_meta);
  }

  return Status::OK();
}

Status TabletMetadata::UpdateAndFlush(const RowSetMetadataIds& to_remove,
                                      const RowSetMetadataVector& to_add) {
  boost::lock_guard<LockType> l(lock_);

  // Convert to protobuf
  TabletSuperBlockPB pb;
  pb.set_id(sblk_id_);
  pb.set_oid(oid_);
  pb.set_start_key(start_key_);
  pb.set_end_key(end_key_);

  RowSetMetadataVector new_rowsets = rowsets_;
  RowSetMetadataVector::iterator it = new_rowsets.begin();
  while (it != new_rowsets.end()) {
    if (ContainsKey(to_remove, (*it)->id())) {
      it = new_rowsets.erase(it);
    } else {
      (*it)->ToProtobuf(pb.add_rowsets());
      it++;
    }
  }

  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& meta, to_add) {
    meta->ToProtobuf(pb.add_rowsets());
    new_rowsets.push_back(meta);
  }

  // Flush
  BlockId a_blk(master_block_.block_a());
  BlockId b_blk(master_block_.block_b());
  if (sblk_id_ & 1) {
    RETURN_NOT_OK(fs_manager_->WriteMetadataBlock(a_blk, pb));
    fs_manager_->DeleteBlock(b_blk);
  } else {
    RETURN_NOT_OK(fs_manager_->WriteMetadataBlock(b_blk, pb));
    fs_manager_->DeleteBlock(a_blk);
  }

  sblk_id_++;
  rowsets_ = new_rowsets;
  return Status::OK();
}

Status TabletMetadata::CreateRowSet(shared_ptr<RowSetMetadata> *rowset) {
  AtomicWord rowset_idx = Barrier_AtomicIncrement(&next_rowset_idx_, 1) - 1;
  RowSetMetadata *meta = new RowSetMetadata(this, rowset_idx);
  rowset->reset(meta);
  return Status::OK();
}

const RowSetMetadata *TabletMetadata::GetRowSetForTests(int32_t id) const {
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
  BOOST_FOREACH(const ColumnDataPB& col_pb, pb.columns()) {
    column_blocks_.push_back(BlockIdFromPB(col_pb.block()));
  }

  // Load Delta Files
  BOOST_FOREACH(const DeltaDataPB& delta_pb, pb.deltas()) {
    delta_blocks_.push_back(
      std::pair<uint32_t, BlockId>(delta_pb.id(), BlockIdFromPB(delta_pb.block())));
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

} // namespace metadata
} // namespace kudu
