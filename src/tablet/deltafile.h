// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_DELTAFILE_H
#define KUDU_TABLET_DELTAFILE_H

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
#include "gutil/macros.h"
#include "tablet/deltamemstore.h"
#include "tablet/delta_key.h"

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
class DeltaKey;

class DeltaFileWriter {
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

  // Append a given delta to the file. This must be called in ascending order
  // of (key, txid).
  Status AppendDelta(const DeltaKey &key, const RowChangeList &delta);

private:
  const Schema schema_;

  DISALLOW_COPY_AND_ASSIGN(DeltaFileWriter);

  gscoped_ptr<cfile::Writer> writer_;

  // Buffer used as a temporary for storing the serialized form
  // of the deltas
  faststring tmp_buf_;

  #ifndef NDEBUG
  // The index of the previously written row.
  // This is used in debug mode to make sure that rows are appended
  // in order.
  DeltaKey last_key_;
  bool has_appended_;
  #endif
};


class DeltaFileReader : public DeltaStore {
public:
  // Open the Delta File at the given path.
  static Status Open(Env *env, const string &path,
                     const Schema &schema,
                     gscoped_ptr<DeltaFileReader> *reader);

  // See DeltaStore::NewDeltaIterator(...)
  virtual DeltaIterator *NewDeltaIterator(const Schema &projection,
                                          const MvccSnapshot &snap) const;

  // See DeltaStore::CheckRowDeleted
  virtual Status CheckRowDeleted(rowid_t row_idx, bool *deleted) const;

  const Schema &schema() const {
    return schema_;
  }

  const string path() const { return path_; }

private:
  friend class DeltaFileIterator;

  DISALLOW_COPY_AND_ASSIGN(DeltaFileReader);

  const shared_ptr<cfile::CFileReader> &cfile_reader() const {
    return reader_;
  }

  DeltaFileReader(cfile::CFileReader *cf_reader,
                  const string &path,
                  const Schema &schema);

  Status Init();

  shared_ptr<cfile::CFileReader> reader_;
  const Schema schema_;

  // The path of the file being read (should be used only for debugging)
  const string path_;
};


// Iterator over the deltas contained in a delta file.
//
// See DeltaIterator for details.
class DeltaFileIterator : public DeltaIterator {
public:
  Status Init();

  Status SeekToOrdinal(rowid_t idx);
  Status PrepareBatch(size_t nrows);
  Status ApplyUpdates(size_t col_to_apply, ColumnBlock *dst);
  Status ApplyDeletes(SelectionVector *sel_vec);
  Status CollectMutations(vector<Mutation *> *dst, Arena *arena);
  string ToString() const;

private:
  friend class DeltaFileReader;
  friend struct ApplyingVisitor;
  friend struct CollectingVisitor;
  friend struct DeletingVisitor;

  DISALLOW_COPY_AND_ASSIGN(DeltaFileIterator);

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
    rowid_t first_updated_idx_;

    // The last row index for which there is an update in this delta block.
    rowid_t last_updated_idx_;

    // Within this block, the index of the update which is the first one that
    // needs to be consulted. This allows deltas to be skipped at the beginning
    // of the block when the row block starts towards the end of the delta block.
    // For example:
    // <-- delta block ---->
    //                   <--- prepared row block --->
    // Here, we can skip a bunch of deltas at the beginning of the delta block
    // which we know don't apply to the prepared row block.
    rowid_t prepared_block_start_idx_;

    // Return a string description of this prepared block, for logging.
    string ToString() const;
  };


  DeltaFileIterator(const DeltaFileReader *dfr, const Schema &projection,
                    const MvccSnapshot &snap);


  // Determine the row index of the first update in the block currently
  // pointed to by index_iter_.
  Status GetFirstRowIndexInCurrentBlock(rowid_t *idx);

  // Determine the last updated row index contained in the given decoded block.
  static Status GetLastRowIndexInDecodedBlock(
    const cfile::StringPlainBlockDecoder &dec, rowid_t *idx);

  // Read the current block of data from the current position in the file
  // onto the end of the delta_blocks_ queue.
  Status ReadCurrentBlockOntoQueue();

  // Visit all mutations in the currently prepared row range with the specified
  // visitor class.
  template<class Visitor>
  Status VisitMutations(Visitor *visitor);

  // Log a FATAL error message about a bad delta.
  void FatalUnexpectedDelta(const DeltaKey &key, const Slice &deltas, const string &msg);

  const DeltaFileReader *dfr_;
  shared_ptr<cfile::CFileReader> cfile_reader_;
  const Schema projection_;
  vector<size_t> projection_indexes_;

  // The MVCC state which determines which deltas should be applied.
  const MvccSnapshot mvcc_snap_;

  gscoped_ptr<cfile::IndexTreeIterator> index_iter_;

  // TODO: add better comments here.
  rowid_t prepared_idx_;
  uint32_t prepared_count_;
  bool prepared_;
  bool exhausted_;

  // After PrepareToApply(), the set of delta blocks in the delta file
  // which correspond to prepared_block_.
  boost::ptr_deque<PreparedDeltaBlock> delta_blocks_;

  // Temporary buffer used in seeking.
  faststring tmp_buf_;
};

} // namespace tablet
} // namespace kudu

#endif
