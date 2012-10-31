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
class Writer;

class IndexTreeBuilder {
public:
  explicit IndexTreeBuilder(
    const WriterOptions *options,
    DataType type,
    Writer *writer);

  Status Append(const void *key, const BlockPointer &block);
  Status Finish(BTreeInfoPB *info);
private:
  IndexBlockBuilder *CreateBlockBuilder(bool is_leaf);
  Status Append(const void *key, const BlockPointer &block_ptr,
                size_t level);

  // Finish the current block at the given index level, and then
  // propagate by inserting this block into the next higher-up
  // level index.
  Status FinishBlockAndPropagate(size_t level);

  // Finish the current block at the given level, writing it
  // to the file. Return the location of the written block
  // in 'written'.
  Status FinishBlock(size_t level, BlockPointer *written);

  const WriterOptions *options_;
  Writer *writer_;

  const TypeInfo &type_info_;

  ptr_vector<IndexBlockBuilder> idx_blocks_;

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

  bool HasNext() {
    for (int i = seeked_indexes_.size() - 1;
         i >= 0;
         i--) {
      if (seeked_indexes_[i].iter->HasNext())
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
      BlockData data;
      RETURN_NOT_OK(PushBlock(BottomIter()->GetCurrentBlockPointer()));
      RETURN_NOT_OK(BottomIter()->SeekToIndex(0));
    }

    return Status::OK();
  }

  const KeyType &GetCurrentKey() {
    return seeked_indexes_.back().iter->GetCurrentKey();
  }

  const BlockPointer &GetCurrentBlockPointer() {
    return seeked_indexes_.back().iter->GetCurrentBlockPointer();
  }

private:
  IndexBlockIterator<KeyType> *BottomIter() const {
    return seeked_indexes_.back().iter.get();
  }

  IndexBlockReader<KeyType> *BottomReader() const {
    return seeked_indexes_.back().reader.get();
  }
  
  Status PushBlock(const BlockPointer &block) {
    BlockData data;
    RETURN_NOT_OK(reader_->ReadBlock(block, &data));

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
    return Status::OK();
  }

  Status SeekDownward(const KeyType &search_key,
                      const BlockPointer &in_block) {

    // Read the block.
    RETURN_NOT_OK(PushBlock(in_block));
    RETURN_NOT_OK(BottomIter()->SeekAtOrBefore(search_key));

    // If the block is a leaf block, we're done,
    // otherwise recurse downward into next layer
    // of B-Tree
    if (BottomReader()->IsLeaf()) {
      return Status::OK();
    } else {
      return SeekDownward(search_key,
                          BottomIter()->GetCurrentBlockPointer());
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
