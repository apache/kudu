// Copyright (c) 2012, Cloudera, inc.

#include "cfile.h"
#include "index_btree.h"

namespace kudu {
namespace cfile {


IndexTreeBuilder::IndexTreeBuilder(
  const WriterOptions *options,
  DataType type,
  Writer *writer) :
  options_(options),
  writer_(writer),
  type_info_(GetTypeInfo(type)) {

  idx_blocks_.push_back(CreateBlockBuilder(true));
}


IndexBlockBuilder *IndexTreeBuilder::CreateBlockBuilder(bool is_leaf) {
  return new IndexBlockBuilder(options_, is_leaf);
}

Status IndexTreeBuilder::Append(const void *key,
                                const BlockPointer &block) {
  return Append(key, block, 0);
}

Status IndexTreeBuilder::Append(const void *key, const BlockPointer &block_ptr,
              size_t level) {
  if (level >= idx_blocks_.size()) {
    // Need to create a new level
    CHECK(level == idx_blocks_.size()) <<
      "trying to create level " << level << " but size is only "
                                << idx_blocks_.size();
    VLOG(1) << "Creating level-" << level << " in index b-tree";
    idx_blocks_.push_back(CreateBlockBuilder(false));
  }

  IndexBlockBuilder &idx_block = idx_blocks_[level];
  idx_block.Add(key, block_ptr);

  size_t est_size = idx_block.EstimateEncodedSize();
  if (est_size > options_->block_size) {
    // This index block is full, flush it.
    BlockPointer index_block_ptr;
    RETURN_NOT_OK(FinishBlockAndPropagate(level));
  }

  return Status::OK();
}


Status IndexTreeBuilder::Finish(BTreeInfoPB *info) {
  // Now do the same for the positional index blocks, starting
  // with leaf
  LOG(INFO) << "flushing tree, b-tree has " <<
    idx_blocks_.size() << " levels";

  // Flush all but the root of the index.
  for (size_t i = 0; i < idx_blocks_.size() - 1; i++) {
    RETURN_NOT_OK(FinishBlockAndPropagate(i));
  }

  // Flush the root
  int root_level = idx_blocks_.size() - 1;
  BlockPointer ptr;
  Status s = FinishBlock(root_level, &ptr);
  if (!s.ok()) {
    LOG(ERROR) << "Unable to flush root index block";
    return s;
  }

  LOG(INFO) << "Flushed root index block: " << ptr.ToString();

  ptr.CopyToPB(info->mutable_root_block());
  return Status::OK();
}

Status IndexTreeBuilder::FinishBlockAndPropagate(size_t level) {
  IndexBlockBuilder &idx_block = idx_blocks_[level];

  char space[16]; // TODO make this dynamic
  void *first_in_idx_block = space;
  Status s = idx_block.GetFirstKey(first_in_idx_block);

  if (!s.ok()) {
    LOG(ERROR) << "Unable to get first key of level-" << level
               << " index block" << GetStackTrace();
    return s;
  }

  // Write to file.
  BlockPointer idx_block_ptr;
  RETURN_NOT_OK(FinishBlock(level, &idx_block_ptr));

  // Add to higher-level index.
  RETURN_NOT_OK(Append(first_in_idx_block, idx_block_ptr,
                       level + 1));

  return Status::OK();
}

// Finish the current block at the given level, writing it
// to the file. Return the location of the written block
// in 'written'.
Status IndexTreeBuilder::FinishBlock(size_t level, BlockPointer *written) {
  IndexBlockBuilder &idx_block = idx_blocks_[level];
  Slice data = idx_block.Finish();
  uint64_t inserted_off;
  Status s = writer_->AddBlock(data, &inserted_off, "idx");
  if (!s.ok()) {
    LOG(ERROR) << "Unable to append level-" << level << " index "
               << "block to file";
    return s;
  }

  *written = BlockPointer(inserted_off, data.size());

  // Reset this level block.
  idx_block.Reset();

  return Status::OK();
}



} // namespace cfile
} // namespace kudu
