// Copyright (c) 2012, Cloudera, inc.

#include "cfile/block_cache.h"
#include "cfile/cfile.h"
#include "cfile/cfile_reader.h"
#include "cfile/index_btree.h"

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
  return new IndexBlockBuilder(options_, type_info_.type(), is_leaf);
}

Status IndexTreeBuilder::Append(const void *key,
                                const BlockPointer &block) {
  return Append(key, block, 0);
}

Status IndexTreeBuilder::Append(
  const void *key, const BlockPointer &block_ptr,
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
  if (est_size > options_->index_block_size) {
    DCHECK(idx_block.Count() > 1)
      << "Index block full with only one entry - this would create "
      << "an infinite loop";
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

  // If the block doesn't have any data in it, we don't need to
  // write it out.
  // This happens if a lower-level block fills up exactly,
  // and then the file completes.
  //
  // TODO: add a test case which exercises this explicitly.
  if (idx_block.Count() == 0) {
    return Status::OK();
  }

  char space[type_info_.size()];
  void *first_in_idx_block = space;
  Status s = idx_block.GetFirstKey(first_in_idx_block);

  if (!s.ok()) {
    LOG(ERROR) << "Unable to get first key of level-" << level
               << " index block: " << s.ToString() << std::endl
               << GetStackTrace();
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

  vector<Slice> v;
  v.push_back(data);
  Status s = writer_->AddBlock(v, written, "index block");
  if (!s.ok()) {
    LOG(ERROR) << "Unable to append level-" << level << " index "
               << "block to file";
    return s;
  }

  // Reset this level block.
  idx_block.Reset();

  return Status::OK();
}

////////////////////////////////////////////////////////////

template <DataType KeyTypeEnum>
class TemplatizedIndexTreeIterator : public IndexTreeIterator {
public:
  typedef DataTypeTraits<KeyTypeEnum> KeyTypeTraits;
  typedef typename KeyTypeTraits::cpp_type KeyType;

  explicit TemplatizedIndexTreeIterator(
    const CFileReader *reader,
    const BlockPointer &root_blockptr)
    : reader_(reader),
      root_block_(root_blockptr)
  {}

  Status SeekAtOrBefore(const void *search_key) {
    KeyType key = *reinterpret_cast<const KeyType *>(search_key);
    return SeekDownward(key, root_block_, 0);
  }

  Status SeekToFirst() {
    return SeekToFirstDownward(root_block_, 0);
  }

  bool HasNext() {
    for (int i = seeked_indexes_.size() - 1;
         i >= 0;
         i--) {
      if (seeked_indexes_[i].iter.HasNext())
        return true;
    }
    return false;
  }

  Status Next() {
    CHECK(!seeked_indexes_.empty()) <<
      "not seeked";

    // Start at the bottom level of the BTree, calling Next(),
    // until one succeeds. If any does not succeed, then
    // that block is exhausted, and gets removed.
    while (!seeked_indexes_.empty()) {
      Status s = BottomIter()->Next();
      if (s.IsNotFound()) {
        seeked_indexes_.pop_back();
      } else if (s.ok()) {
        break;
      } else {
        // error
        return s;
      }
    }

    // If we're now empty, then the root block was exhausted,
    // so we're entirely out of data.
    if (seeked_indexes_.empty()) {
      return Status::NotFound("end of iterator");
    }

    // Otherwise, the last layer points to the valid
    // next block. Propagate downward if it is not a leaf.
    while (!BottomReader()->IsLeaf()) {
      RETURN_NOT_OK(LoadBlock(BottomIter()->GetCurrentBlockPointer(),
                              seeked_indexes_.size()));
      RETURN_NOT_OK(BottomIter()->SeekToIndex(0));
    }

    return Status::OK();
  }

  const void *GetCurrentKey() const {
    return seeked_indexes_.back().iter.GetCurrentKey();
  }

  const BlockPointer &GetCurrentBlockPointer() const {
    return seeked_indexes_.back().iter.GetCurrentBlockPointer();
  }

private:
  IndexBlockIterator<KeyTypeEnum> *BottomIter() {
    return &seeked_indexes_.back().iter;
  }

  IndexBlockReader<KeyTypeEnum> *BottomReader() {
    return &seeked_indexes_.back().reader;
  }

  IndexBlockIterator<KeyTypeEnum> *seeked_iter(int depth) {
    return &seeked_indexes_[depth].iter;
  }

  IndexBlockReader<KeyTypeEnum> *seeked_reader(int depth) {
    return &seeked_indexes_[depth].reader;
  }
  
  Status LoadBlock(const BlockPointer &block, int depth) {
    
    SeekedIndex *seeked;
    if (depth < seeked_indexes_.size()) {
      // We have a cached instance from previous seek.
      seeked = &seeked_indexes_[depth];

      if (seeked->block_ptr.offset() == block.offset()) {
        // We're already seeked to this block - no need to re-parse it.
        // This is handy on the root block as well as for the case
        // when a lot of requests are traversing down the same part of
        // the tree.
        return Status::OK();
      }

      // Seeked to a different block: reset the reader
      seeked->reader.Reset();
      seeked->iter.Reset();
    } else {
      // No cached instance, make a new one.
      seeked_indexes_.push_back(new SeekedIndex());
      seeked = &seeked_indexes_.back();
    }

    RETURN_NOT_OK(reader_->ReadBlock(block, &seeked->data));
    seeked->block_ptr = block;

    // Parse the new block.
    RETURN_NOT_OK(seeked->reader.Parse(seeked->data.data()));

    return Status::OK();
  }

  Status SeekDownward(const KeyType &search_key,
                      const BlockPointer &in_block,
                      int cur_depth) {

    // Read the block.
    RETURN_NOT_OK(LoadBlock(in_block, cur_depth));
    IndexBlockIterator<KeyTypeEnum> *iter = seeked_iter(cur_depth);

    RETURN_NOT_OK(iter->SeekAtOrBefore(search_key));

    // If the block is a leaf block, we're done,
    // otherwise recurse downward into next layer
    // of B-Tree
    if (seeked_reader(cur_depth)->IsLeaf()) {
      seeked_indexes_.resize(cur_depth + 1);
      return Status::OK();
    } else {
      return SeekDownward(search_key,
                          iter->GetCurrentBlockPointer(),
                          cur_depth + 1);
    }
  }

  Status SeekToFirstDownward(const BlockPointer &in_block,
                             int cur_depth) {
    // Read the block.
    RETURN_NOT_OK(LoadBlock(in_block, cur_depth));
    IndexBlockIterator<KeyTypeEnum> *iter = seeked_iter(cur_depth);

    RETURN_NOT_OK(iter->SeekToIndex(0));

    // If the block is a leaf block, we're done,
    // otherwise recurse downward into next layer
    // of B-Tree
    if (seeked_reader(cur_depth)->IsLeaf()) {
      seeked_indexes_.resize(cur_depth + 1);
      return Status::OK();
    } else {
      return SeekToFirstDownward(
        iter->GetCurrentBlockPointer(), cur_depth + 1);
    }
  }

  struct SeekedIndex {
    SeekedIndex() :
      iter(&reader)
    {}

    // Hold a copy of the underlying block data, which would
    // otherwise go out of scope. The reader and iter
    // do not themselves retain the data.
    BlockPointer block_ptr;
    BlockCacheHandle data;
    IndexBlockReader<KeyTypeEnum> reader;
    IndexBlockIterator<KeyTypeEnum> iter;
  };


  const CFileReader *reader_;

  BlockPointer root_block_;

  ptr_vector<SeekedIndex> seeked_indexes_;
};


IndexTreeIterator *IndexTreeIterator::Create(
    const CFileReader *reader,
    DataType type,
    const BlockPointer &root_blockptr) {
  switch (type) {
    case UINT32:
      return new TemplatizedIndexTreeIterator<UINT32>(reader, root_blockptr);
    case STRING:
      return new TemplatizedIndexTreeIterator<STRING>(reader, root_blockptr);
    default:
      CHECK(0) << "invalid type: " << type;
      break;
  }
}


} // namespace cfile
} // namespace kudu
