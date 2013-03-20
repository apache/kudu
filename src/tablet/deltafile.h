// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_DELTAFILE_H
#define KUDU_TABLET_DELTAFILE_H

#include <boost/noncopyable.hpp>
#include <boost/ptr_container/ptr_deque.hpp>
#include <tr1/memory>
#include <vector>

#include "cfile/block_cache.h"
#include "cfile/string_plain_block.h"
#include "cfile/cfile.h"
#include "cfile/index_btree.h"
#include "common/columnblock.h"
#include "common/schema.h"
#include "gutil/gscoped_ptr.h"
#include "tablet/deltamemstore.h"
#include "tablet/layer-interfaces.h"

namespace kudu {

class WritableFile;
class RandomAccessFile;
class Env;

namespace cfile {
class CFileReader;
}

namespace tablet {

using std::tr1::shared_ptr;

class DeltaFileIterator;


class DeltaFileWriter : boost::noncopyable {
public:
  // Construct a new delta file writer.
  // The writer takes ownership over the file and will Close it
  // in Finish().
  explicit DeltaFileWriter(const Schema &schema,
                           const shared_ptr<WritableFile> &file);

  Status Start();

  // Finish writing the file, including closing the underlying WritableFile
  // object (even if someone else has a reference to the same WritableFile).
  Status Finish();

  Status AppendDelta(uint32_t row_idx, const RowChangeList &delta);

private:
  const Schema schema_;

  gscoped_ptr<cfile::Writer> writer_;

  // Buffer used as a temporary for storing the serialized form
  // of the deltas
  faststring tmp_buf_;

  #ifndef NDEBUG
  // The index of the previously written row.
  // This is used in debug mode to make sure that rows are appended
  // in order.
  uint32_t last_row_idx_;
  #endif
};


class DeltaFileReader : public DeltaTrackerInterface, boost::noncopyable {
public:
  // Open the Delta File at the given path.
  static Status Open(Env *env, const string &path,
                     const Schema &schema,
                     gscoped_ptr<DeltaFileReader> *reader);

  virtual DeltaIteratorInterface *NewDeltaIterator(const Schema &projection);

  const Schema &schema() const {
    return schema_;
  }

private:
  friend class DeltaFileIterator;

  const shared_ptr<cfile::CFileReader> &cfile_reader() const {
    return reader_;
  }

  DeltaFileReader(cfile::CFileReader *cf_reader,
                  const Schema &schema);

  Status Init();

  shared_ptr<cfile::CFileReader> reader_;
  const Schema schema_;
};


// Iterator over the deltas contained in a delta file.
//
// See DeltaIteratorInterface for details.
class DeltaFileIterator : boost::noncopyable, public DeltaIteratorInterface {
public:
  Status Init();

  Status SeekToOrdinal(uint32_t idx);
  Status PrepareToApply(RowBlock *dst);
  Status ApplyUpdates(RowBlock *dst, size_t col_to_apply);
private:
  friend class DeltaFileReader;

  // PrepareToApply() will read forward all blocks from the deltafile
  // which overlap with the block being prepared, enqueueing them onto
  // the 'delta_blocks_' deque. The prepared blocks are then used to
  // actually apply deltas in ApplyUpdates().
  struct PreparedDeltaBlock {
    // The pointer from which this block was read. This is only used for
    // logging, etc.
    cfile::BlockPointer block_ptr_;

    // Handle to the block in the block cache, so it doesn't get freed
    // from underneath us.
    cfile::BlockCacheHandle block_;

    // The cached block decoder, to avoid having to re-parse the block header
    // on every ApplyUpdates() call
    gscoped_ptr<cfile::StringPlainBlockDecoder> decoder_;

    // The first row index for which there is an update in this delta block.
    uint32_t first_updated_idx_;

    // The last row index for which there is an update in this delta block.
    uint32_t last_updated_idx_;

    // Within this block, the index of the update which is the first one that
    // needs to be consulted. This allows deltas to be skipped at the beginning
    // of the block when the row block starts towards the end of the delta block.
    // For example:
    // <-- delta block ---->
    //                   <--- prepared row block --->
    // Here, we can skip a bunch of deltas at the beginning of the delta block
    // which we know don't apply to the prepared row block.
    uint32_t prepared_block_start_idx_;

    // Return a string description of this prepared block, for logging.
    string ToString() const;
  };


  DeltaFileIterator(DeltaFileReader *dfr, const Schema &projection);


  // Determine the row index of the first update in the block currently
  // pointed to by index_iter_.
  Status GetFirstRowIndexInCurrentBlock(uint32_t *idx);

  // Determine the last updated row index contained in the given decoded block.
  static Status GetLastRowIndexInDecodedBlock(
    const cfile::StringPlainBlockDecoder &dec, uint32_t *idx);

  // Read the current block of data from the current position in the file
  // onto the end of the delta_blocks_ queue.
  Status ReadCurrentBlockOntoQueue();

  static Status DecodeUpdatedIndexFromSlice(const Slice &s, uint32_t *idx);
  Status ApplyEncodedDelta(const Slice &s, size_t col_idx, 
                           uint32_t start_row, ColumnBlock *dst,
                           bool *done) const;


  DeltaFileReader *dfr_;
  shared_ptr<cfile::CFileReader> cfile_reader_;
  const Schema projection_;
  vector<size_t> projection_indexes_;

  gscoped_ptr<cfile::IndexTreeIterator> index_iter_;

  uint32_t cur_idx_;
  uint32_t prepared_idx_;
  RowBlock *prepared_block_;
  bool exhausted_;

  // After PrepareToApply(), the set of delta blocks in the delta file
  // which correspond to prepared_block_.
  boost::ptr_deque<PreparedDeltaBlock> delta_blocks_;
};

} // namespace tablet
} // namespace kudu

#endif
