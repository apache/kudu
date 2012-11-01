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

  // Append the given key into the index.
  // The key is copied into the builder's internal
  // memory.
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

class IndexTreeIterator : boost::noncopyable {
public:

  virtual Status SeekAtOrBefore(const void *search_key) = 0;
  virtual bool HasNext() = 0;
  virtual Status Next() = 0;

  // Return a pointer to the key at which the iterator
  // is currently seeked. This pointer will become invalid
  // upon any non-const call.
  virtual const void *GetCurrentKey() const = 0;
  virtual const BlockPointer &GetCurrentBlockPointer() const = 0;

  virtual ~IndexTreeIterator() {}

  static IndexTreeIterator *Create(
    const CFileReader *reader,
    DataType type,
    const BlockPointer &idx_root);
};

} // namespace cfile
} // namespace kudu
#endif
