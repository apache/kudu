// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_CFILE_INDEX_BTREE_H
#define KUDU_CFILE_INDEX_BTREE_H

#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/noncopyable.hpp>
#include <memory>

#include "cfile.pb.h"
#include "index_block.h"
#include "util/logging.h"
#include "cfile_reader.h"

namespace kudu {
namespace cfile {

using boost::ptr_vector;

class CFileReader;

template <class KeyType>
class IndexTreeBuilder : boost::noncopyable {
public:
  typedef IndexBlockBuilder<KeyType> BlockBuilder;


  explicit IndexTreeBuilder(const WriterOptions *options,
                            Writer *writer) :
    options_(options),
    writer_(writer) {

    idx_blocks_.push_back(new BlockBuilder(options, true));
  }

  Status Append(const KeyType &key, const BlockPointer &block) {
    return Append(key, block, 0);
  }

  Status Finish(BTreeInfoPB *info) {
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


private:
  Status Append(const KeyType &key, const BlockPointer &block_ptr,
                size_t level) {
    if (level >= idx_blocks_.size()) {
      // Need to create a new level
      CHECK(level == idx_blocks_.size()) <<
        "trying to create level " << level << " but size is only "
                                  << idx_blocks_.size();
      VLOG(1) << "Creating level-" << level << " in index b-tree";
      idx_blocks_.push_back(new BlockBuilder(options_, false));
    }

    BlockBuilder &idx_block = idx_blocks_[level];
    idx_block.Add(key, block_ptr);

    size_t est_size = idx_block.EstimateEncodedSize();
    if (est_size > options_->block_size) {
      // This index block is full, flush it.
      BlockPointer index_block_ptr;
      RETURN_NOT_OK(FinishBlockAndPropagate(level));
    }

    return Status::OK();
  }

  // Finish the current block at the given index level, and then
  // propagate by inserting this block into the next higher-up
  // level index.
  Status FinishBlockAndPropagate(size_t level) {
    BlockBuilder &idx_block = idx_blocks_[level];

    KeyType first_in_idx_block;
    Status s = idx_block.GetFirstKey(&first_in_idx_block);
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
  Status FinishBlock(size_t level, BlockPointer *written) {
    BlockBuilder &idx_block = idx_blocks_[level];
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


  const WriterOptions *options_;
  Writer *writer_;

  ptr_vector<BlockBuilder> idx_blocks_;

};

template <class KeyType>
class IndexTreeIterator : boost::noncopyable {
public:
  explicit IndexTreeIterator(const CFileReader *reader,
                             const BlockPointer &root_blockptr) :
    reader_(reader),
    root_block_(root_blockptr) {
  }

  Status SeekAtOrBefore(const KeyType &search_key) {
    seeked_indexes_.clear();

    return SeekDownward(search_key, root_block_);
  }

  const KeyType &GetCurrentKey() {
    return seeked_indexes_.back().iter->GetCurrentKey();
  }

  const BlockPointer &GetCurrentBlockPointer() {
    return seeked_indexes_.back().iter->GetCurrentBlockPointer();
  }

private:
  Status SeekDownward(const KeyType &search_key,
                      const BlockPointer &in_block) {

    // Read the block.
    BlockData data;
    RETURN_NOT_OK(reader_->ReadBlock(in_block, &data));

    // Parse it and open iterator.
    IndexBlockReader<KeyType> *ibr;
    IndexBlockIterator<KeyType> *iter;
    {
      std::auto_ptr<IndexBlockReader<KeyType> > ibr_auto(
        new IndexBlockReader<KeyType>(data.slice()));
      RETURN_NOT_OK(ibr_auto->Parse());

      iter = ibr_auto->NewIterator();

      // If we successfully parsed and created an iterator,
      // we no longer need the auto_ptr to delete 'ibr'
      ibr = ibr_auto.release();

      // Add the block to the index iterator list.
      SeekedIndex *si = new SeekedIndex(data,
                                        ibr,
                                        iter);
      seeked_indexes_.push_back(si);
    }


    RETURN_NOT_OK(iter->SeekAtOrBefore(search_key));

    // If the block is a leaf block, we're done,
    // otherwise recurse downward into next layer
    // of B-Tree
    if (ibr->IsLeaf()) {
      return Status::OK();
    } else {
      return SeekDownward(search_key, iter->GetCurrentBlockPointer());
    }
  }

  struct SeekedIndex {
    SeekedIndex(const BlockData &data_,
                IndexBlockReader<KeyType> *reader_,
                IndexBlockIterator<KeyType> *iter_) :
      data(data_),
      reader(reader_),
      iter(iter_) {}

    // Hold a copy of the underlying block data, which would
    // otherwise go out of scope. The reader and iter
    // do not themselves retain the data.
    BlockData data;
    scoped_ptr<IndexBlockReader<KeyType> > reader;
    scoped_ptr<IndexBlockIterator<KeyType> > iter;
  };


  const CFileReader *reader_;

  BlockPointer root_block_;

  ptr_vector<SeekedIndex> seeked_indexes_;
};

} // namespace cfile
} // namespace kudu
#endif
