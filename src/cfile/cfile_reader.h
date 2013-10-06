// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_CFILE_CFILE_READER_H
#define KUDU_CFILE_CFILE_READER_H

#include <tr1/memory>
#include <string>
#include <vector>

#include "common/columnblock.h"
#include "common/types.h"
#include "cfile/block_cache.h"
#include "cfile/block_encodings.h"
#include "cfile/block_compression.h"
#include "cfile/cfile_util.h"
#include "cfile/index_btree.h"
#include "cfile/type_encodings.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "gutil/port.h"
#include "util/env.h"
#include "util/memory/arena.h"
#include "util/object_pool.h"
#include "util/rle-encoding.h"
#include "util/status.h"
#include "common/key_encoder.h"

namespace kudu {

class RandomAccessFile;

namespace cfile {

class CFileHeaderPB;
class CFileFooterPB;

using std::string;
using std::tr1::shared_ptr;

class BlockCache;
class BlockCacheHandle;
class BlockDecoder;
class BlockPointer;
class CFileIterator;


class CFileReader {
 public:
  // Open the cfile at the given path.
  //
  // When this method is used, there is no need to explicitly
  // Init() the reader.
  static Status Open(Env *env, const string &path,
                     const ReaderOptions &options,
                     gscoped_ptr<CFileReader> *reader);

  static Status Open(const shared_ptr<RandomAccessFile>& file,
                     uint64_t file_size,
                     const ReaderOptions& options,
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
  Status CountRows(rowid_t *count) const;

  // Retrieve the given metadata entry into 'val'.
  // Returns true if the entry was found, otherwise returns false.
  //
  // Note that this implementation is currently O(n), so should not be used
  // in a hot path.
  bool GetMetadataEntry(const string &key, string *val);

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

  const TypeEncodingInfo *type_encoding_info() const {
    DCHECK_EQ(state_, kInitialized);
    return type_encoding_info_;
  }

  bool is_nullable() const {
    return footer().is_type_nullable();
  }

  const CFileHeaderPB &header() const {
    return *DCHECK_NOTNULL(header_.get());
  }

  const CFileFooterPB &footer() const {
    return *DCHECK_NOTNULL(footer_.get());
  }

  bool is_compressed() const {
    return footer().compression() != cfile::NO_COMPRESSION;
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
  DISALLOW_COPY_AND_ASSIGN(CFileReader);

  friend class CFileIterator;

  CFileReader(const ReaderOptions &options,
              const shared_ptr<RandomAccessFile> &file,
              uint64_t file_size);

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

  gscoped_ptr<CompressedBlockDecoder> block_uncompressor_;

  const TypeInfo *type_info_;
  const TypeEncodingInfo *type_encoding_info_;
  const KeyEncoder *key_encoder_;

  BlockCache *cache_;
  BlockCache::FileId cache_id_;

};

// Column Iterator interface used by the CFileSet.
// Implemented by the CFileIterator, DefaultColumnValueIterator
// and the ColumnValueTypeAdaptorIterator.
// It is used to fill the data requested by the projection.
class ColumnIterator {
 public:
  // Statistics on the amount of IO done by the iterator.
  struct IOStatistics {
    IOStatistics();
    string ToString() const;

    // The number of data blocks which were read by this iterator.
    uint32_t data_blocks_read;

    // The number of rows which were read from disk -- regardless of whether
    // they were decoded/materialized.
    uint64_t rows_read;

    // TODO: flesh this out with index blocks, number of bytes read,
    // rows
  };

  virtual ~ColumnIterator() {}

  // Seek to the given ordinal entry in the file.
  // Entry 0 is the first entry written to the file.
  // If provided seek point is past the end of the file,
  // then returns a NotFound Status.
  // TODO: do we ever want to be able to seek to the end of the file?
  virtual Status SeekToOrdinal(rowid_t ord_idx) = 0;

  // Return true if this reader is currently seeked.
  // If the iterator is not seeked, it is an error to call any functions except
  // for seek (including GetCurrentOrdinal).
  virtual bool seeked() const = 0;

  // Get the ordinal index that the iterator is currently pointed to.
  //
  // Prior to calling PrepareBatch(), this returns the position after the last
  // seek. PrepareBatch() and Scan() do not change the position returned by this
  // function. FinishBatch() advances the ordinal to the position of the next
  // block to be prepared.
  virtual rowid_t GetCurrentOrdinal() const = 0;

  // Prepare to read up to *n into the given column block.
  // On return sets *n to the number of prepared rows, which is always
  // <= the requested value.
  //
  // This assumes that dst->size() >= *n on input.
  //
  // If there are at least dst->size() values remaining in the underlying file,
  // this will always return *n == dst->size(). In other words, this does not
  // ever result in a "short read".
  virtual Status PrepareBatch(size_t *n) = 0;

  // Copy values into the prepared column block.
  // Any indirected values (eg strings) are copied into the dst block's
  // arena.
  // This does _not_ advance the position in the underlying file. Multiple
  // calls to Scan() will re-read the same values.
  virtual Status Scan(ColumnBlock *dst) = 0;

  // Finish processing the current batch, advancing the iterators
  // such that the next call to PrepareBatch() will start where the previous
  // batch left off.
  virtual Status FinishBatch() = 0;

  virtual const IOStatistics &io_statistics() const = 0;
};

// ColumnIterator that fills the ColumnBlock with the specified value.
// It is used by the CFileSet to handle the case of a column present
// in the projection schema but not in the base data.
//
// Example:
//    DefaultColumnValueIterator iter;
//    iter.Scan(&column_block);
class DefaultColumnValueIterator : public ColumnIterator {
 public:
  DefaultColumnValueIterator(DataType type, const void *value)
    : type_(type), value_(value) {
  }

  Status SeekToOrdinal(rowid_t ord_idx);

  bool seeked() const { return true; }

  rowid_t GetCurrentOrdinal() const { return ordinal_; }

  Status PrepareBatch(size_t *n);
  Status Scan(ColumnBlock *dst);
  Status FinishBatch();

  const IOStatistics &io_statistics() const { return io_stats_; }

 private:
  const DataType type_;
  const void *value_;

  size_t batch_;
  rowid_t ordinal_;
  IOStatistics io_stats_;
};


class CFileIterator : public ColumnIterator {
 public:
  CFileIterator(const CFileReader *reader,
                const BlockPointer *posidx_root,
                const BlockPointer *validx_root);

  // Seek to the first entry in the file. This works for both
  // ordinal-indexed and value-indexed files.
  Status SeekToFirst();

  // Seek to the given ordinal entry in the file.
  // Entry 0 is the first entry written to the file.
  // If provided seek point is past the end of the file,
  // then returns a NotFound Status.
  // TODO: do we ever want to be able to seek to the end of the file?
  Status SeekToOrdinal(rowid_t ord_idx);

  // Seek the index to the given row_key, or to the index entry immediately
  // before it. Then (if the index is sparse) seek the data block to the
  // value matching value or to the value immediately after it.
  //
  // Sets *exact_match to indicate whether the seek found the exact
  // key requested.
  //
  // If this iterator was constructed without no value index,
  // then this will return a NotSupported status.
  Status SeekAtOrAfter(const EncodedKey &encoded_key,
                       bool *exact_match);

  // Return true if this reader is currently seeked.
  // If the iterator is not seeked, it is an error to call any functions except
  // for seek (including GetCurrentOrdinal).
  bool seeked() const { return seeked_; }

  // Get the ordinal index that the iterator is currently pointed to.
  //
  // Prior to calling PrepareBatch(), this returns the position after the last
  // seek. PrepareBatch() and Scan() do not change the position returned by this
  // function. FinishBatch() advances the ordinal to the position of the next
  // block to be prepared.
  rowid_t GetCurrentOrdinal() const;

  // Prepare to read up to *n into the given column block.
  // On return sets *n to the number of prepared rows, which is always
  // <= the requested value.
  //
  // This assumes that dst->size() >= *n on input.
  //
  // If there are at least dst->size() values remaining in the underlying file,
  // this will always return *n == dst->size(). In other words, this does not
  // ever result in a "short read".
  Status PrepareBatch(size_t *n);

  // Copy values into the prepared column block.
  // Any indirected values (eg strings) are copied into the dst block's
  // arena.
  // This does _not_ advance the position in the underlying file. Multiple
  // calls to Scan() will re-read the same values.
  Status Scan(ColumnBlock *dst);

  // Finish processing the current batch, advancing the iterators
  // such that the next call to PrepareBatch() will start where the previous
  // batch left off.
  Status FinishBatch();

  // Return true if the next call to PrepareBatch will return at least one row.
  bool HasNext() const;

  // Convenience method to prepare a batch, scan it, and finish it.
  Status CopyNextValues(size_t *n, ColumnBlock *dst);

  const IOStatistics &io_statistics() const {
    return io_stats_;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(CFileIterator);

  struct PreparedBlock {
    BlockPointer dblk_ptr_;
    BlockCacheHandle dblk_data_;
    gscoped_ptr<BlockDecoder> dblk_;

    // The rowid of the first row in this block.
    rowid_t first_row_idx() const {
      return dblk_->GetFirstRowId();
    }

    // The index of the seeked position, relative to the start of the block.
    // In case of null bitmap present, dblk_->GetCurrentIndex() is not aligned
    // with the row number, since null values are not written to the data block.
    // check CFileIterator::SeekToPositionInBlock()
    uint32_t idx_in_block_;

    // When the block is first read, it is seeked to the proper position
    // and rewind_idx_ is set to that offset in the block. needs_rewind_
    // is initially false, but after any values are read from the block,
    // it becomes true. This indicates that dblk_ is pointed at a later
    // position in the block, and should be rewound if a second call to
    // Scan() is made.
    // rewind_idx is relative to the first entry in the block (i.e. not a rowid)
    bool needs_rewind_;
    uint32_t rewind_idx_;

    // Total number of rows in the block (nulls + not nulls)
    uint32_t num_rows_in_block_;

    // Null bitmap and bitmap (RLE) decoder
    Slice rle_bitmap;
    RleDecoder<bool> rle_decoder_;

    rowid_t last_row_idx() const {
      return first_row_idx() + num_rows_in_block_ - 1;
    }

    string ToString() const;
  };

  // Seek the given PreparedBlock to the given index within it.
  void SeekToPositionInBlock(PreparedBlock *pb, uint32_t idx_in_block);

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

  // True if PrepareBatch() has been called more recently than FinishBatch().
  bool prepared_;

  // RowID of the current prepared batch, if prepared_ is true.
  // Otherwise, the RowID of the next batch that will be prepared.
  rowid_t last_prepare_idx_;

  // Number of rows in the current batch, if prepared_ is true.
  // Otherwise, 0.
  uint32_t last_prepare_count_;

  IOStatistics io_stats_;

  // a temporary buffer for encoding
  faststring tmp_buf_;
};

} // namespace cfile
} // namespace kudu

#endif
