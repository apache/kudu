// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_CFILE_CFILE_READER_H
#define KUDU_CFILE_CFILE_READER_H

#include <boost/noncopyable.hpp>
#include <boost/shared_array.hpp>
#include <vector>
#include <tr1/memory>
#include <string>

#include "common/columnblock.h"
#include "common/types.h"
#include "cfile/block_cache.h"
#include "cfile/block_encodings.h"
#include "cfile/index_btree.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/port.h"
#include "util/env.h"
#include "util/memory/arena.h"
#include "util/object_pool.h"
#include "util/status.h"

namespace kudu {

class RandomAccessFile;

namespace cfile {

class CFileHeaderPB;
class CFileFooterPB;

using std::string;
using boost::shared_array;
using std::tr1::shared_ptr;


struct ReaderOptions {
};

class BlockCache;
class BlockCacheHandle;
class BlockDecoder;
class BlockPointer;
class CFileIterator;


class CFileReader : boost::noncopyable {
public:
  // Open the cfile at the given path.
  //
  // When this method is used, there is no need to explicitly
  // Init() the reader.
  static Status Open(Env *env, const string &path,
                     const ReaderOptions &options,
                     gscoped_ptr<CFileReader> *reader);

  Status Init();

  Status NewIterator(CFileIterator **iter) const;
  Status NewIterator(gscoped_ptr<CFileIterator> *iter) const {
    CFileIterator *iter_ptr;
    RETURN_NOT_OK(NewIterator(&iter_ptr));
    (*iter).reset(iter_ptr);
    return Status::OK();
  }

  // TODO: make this private? should only be used
  // by the iterator and index tree readers, I think.
  Status ReadBlock(const BlockPointer &ptr,
                   BlockCacheHandle *ret) const;

  // Return the number of rows in this cfile.
  // This is assumed to be reasonably fast (i.e does not scan
  // the data)
  Status CountRows(size_t *count) const;

  uint64_t file_size() const {
    return file_size_;
  }

  DataType data_type() const {
    CHECK_EQ(state_, kInitialized);
    return footer_->data_type();
  }

  const TypeInfo *type_info() const {
    DCHECK_EQ(state_, kInitialized);
    return type_info_;
  }

  const CFileHeaderPB &header() const {
    return *CHECK_NOTNULL(header_.get());
  }

  const CFileFooterPB &footer() const {
    return *CHECK_NOTNULL(footer_.get());
  }

  // Advanced access to the cfile. This is used by the
  // delta reader code. TODO: think about reorganizing this:
  // delta files can probably be done more cleanly.

  // Return true if there is a position-based index on this file.
  bool has_posidx() const { return footer_->has_posidx_info(); }
  BlockPointer posidx_root() const {
    DCHECK(has_posidx());
    return BlockPointer(footer_->posidx_info().root_block());
  }

  // Return true if there is a value-based index on this file.
  bool has_validx() const { return footer_->has_validx_info(); }
  BlockPointer validx_root() const {
    DCHECK(has_validx());
    return BlockPointer(footer_->validx_info().root_block());
  }

  

private:
  friend class CFileIterator;

  CFileReader(const ReaderOptions &options,
              const shared_ptr<RandomAccessFile> &file,
              uint64_t file_size);

  // Create a BlockDecoder for the data in this file.
  // Sets *bd to the newly created decoder, if successful.
  // Otherwise returns a non-OK Status.
  Status CreateBlockDecoder(BlockDecoder **bd, const Slice &slice) const;

  Status ReadMagicAndLength(uint64_t offset, uint32_t *len);
  Status ReadAndParseHeader();
  Status ReadAndParseFooter();

#ifdef __clang__
  __attribute__((__unused__))
#endif
  const ReaderOptions options_;
  const shared_ptr<RandomAccessFile> file_;
  const uint64_t file_size_;

  enum State {
    kUninitialized,
    kInitialized
  };
  State state_;

  gscoped_ptr<CFileHeaderPB> header_;
  gscoped_ptr<CFileFooterPB> footer_;

  const TypeInfo *type_info_;

  BlockCache *cache_;
  BlockCache::FileId cache_id_;
};


class CFileIterator : boost::noncopyable {
public:
  CFileIterator(const CFileReader *reader,
                const BlockPointer *posidx_root,
                const BlockPointer *validx_root);

  // Seek to the given ordinal entry in the file.
  // Entry 0 is the first entry written to the file.
  // If provided seek point is past the end of the file,
  // then returns a NotFound Status.
  // TODO: do we ever want to be able to seek to the end of the file?
  Status SeekToOrdinal(uint32_t ord_idx);

  // Seek to the given key, or to the entry directly following
  // it. If the largest key in the file is still less than
  // the given key, then returns a NotFound Status.
  //
  // Sets *exact_match to indicate whether the seek found the exact
  // key requested.
  //
  // If this iterator was constructed without no value index,
  // then this will return a NotSupported status.
  Status SeekAtOrAfter(const void *key,
                       bool *exact_match);

  // Get the ordinal index that the iterator is currently pointed to.
  //
  // Prior to calling PrepareBatch(), this returns the position after the last
  // seek. PrepareBatch() and Scan() do not change the position returned by this
  // function. FinishBatch() advances the ordinal to the position of the next
  // block to be prepared.
  uint32_t GetCurrentOrdinal() const;

  // Prepare to read up to *n into the given column block.
  // On return sets *n to the number of prepared rows, which is always
  // <= the requested value.
  //
  // This assumes that dst->size() >= *n on input.
  //
  // If there are at least dst->size() values remaining in the underlying file,
  // this will always return *n == dst->size(). In other words, this does not
  // ever result in a "short read".
  Status PrepareBatch(ColumnBlock *dst, size_t *n);

  // Copy values into the prepared column block.
  // Any indirected values (eg strings) are copied into the dst block's
  // arena.
  // This does _not_ advance the position in the underlying file. Multiple
  // calls to Scan() will re-read the same values.
  Status Scan();

  // Finish processing the current batch, advancing the iterators
  // such that the next call to PrepareBatch() will start where the previous
  // batch left off.
  Status FinishBatch();

  // Return true if the next call to PrepareBatch will return at least one row.
  bool HasNext() const;


  // Convenience method to prepare a batch, scan it, and finish it.
  Status CopyNextValues(size_t *n, ColumnBlock *dst);


private:
  struct PreparedBlock {
    BlockPointer dblk_ptr_;
    BlockCacheHandle dblk_data_;
    gscoped_ptr<BlockDecoder> dblk_;

    // The index of the first row in this block.
    uint32_t first_row_idx_;

    // When the block is first read, it is seeked to the proper position
    // and rewind_idx_ is set to that offset in the block. needs_rewind_
    // is initially false, but after any values are read from the block,
    // it becomes true. This indicates that dblk_ is pointed at a later
    // position in the block, and should be rewound if a second call to
    // Scan() is made.
    bool needs_rewind_;
    uint32_t rewind_idx_;

    uint32_t last_row_idx() const {
      return first_row_idx_ + dblk_->Count() - 1;
    }

    string ToString() const;
  };

  // Read the data block currently pointed to by idx_iter_
  // into the given PreparedBlock structure.
  //
  // This does not advance the iterator.
  Status ReadCurrentDataBlock(const IndexTreeIterator &idx_iter,
                              PreparedBlock *prep_block);

  // Read the data block currently pointed to by idx_iter_, and enqueue
  // it onto the end of the prepared_blocks_ deque.
  Status QueueCurrentDataBlock(const IndexTreeIterator &idx_iter);

  // Clear any seek-related state.
  void Unseek();

  const CFileReader *reader_;

  gscoped_ptr<IndexTreeIterator> posidx_iter_;
  gscoped_ptr<IndexTreeIterator> validx_iter_;

  // The currently in-use index iterator. This is equal to either
  // posidx_iter_.get(), validx_iter_.get(), or NULL if not seeked.
  IndexTreeIterator *seeked_;

  // Data blocks that contain data relevant to the currently Prepared
  // batch of rows.
  // These pointers are allocated from the prepared_block_pool_ below.
  vector<PreparedBlock *> prepared_blocks_;

  ObjectPool<PreparedBlock> prepared_block_pool_;
  typedef ObjectPool<PreparedBlock>::scoped_ptr pblock_pool_scoped_ptr;

  // State set up by PrepareBatch(...):
  ColumnBlock *prepared_dst_block_;
  uint32_t last_prepare_idx_;
  uint32_t last_prepare_count_;
};


} // namespace cfile
} // namespace kudu

#endif
