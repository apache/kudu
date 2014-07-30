// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_DELTAFILE_H
#define KUDU_TABLET_DELTAFILE_H

#include <boost/ptr_container/ptr_deque.hpp>
#include <tr1/memory>
#include <string>
#include <vector>

#include "kudu/cfile/block_cache.h"
#include "kudu/cfile/string_plain_block.h"
#include "kudu/cfile/cfile.h"
#include "kudu/cfile/index_btree.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/tablet/deltamemstore.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/tablet.pb.h"

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
class DeltaCompactionInput;
template<DeltaType Type>
struct ApplyingVisitor;
template<DeltaType Type>
struct DeletingVisitor;

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
  // of (key, timestamp) for REDOS and ascending order of key, descending order
  // of timestamp for UNDOS.
  template<DeltaType Type>
  Status AppendDelta(const DeltaKey &key, const RowChangeList &delta);

  Status WriteDeltaStats(const DeltaStats& stats);

  const Schema& schema() const { return schema_; }

 private:
  const Schema schema_;

  Status WriteSchema();

  Status DoAppendDelta(const DeltaKey &key, const RowChangeList &delta);

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

  DISALLOW_COPY_AND_ASSIGN(DeltaFileWriter);
};

class DeltaFileReader : public DeltaStore,
                        public std::tr1::enable_shared_from_this<DeltaFileReader> {
 public:
  static const char * const kSchemaMetaEntryName;
  static const char * const kDeltaStatsEntryName;

  // Open the Delta File at the given path.
  static Status Open(Env *env,
                     const string &path,
                     const BlockId& block_id,
                     std::tr1::shared_ptr<DeltaFileReader>* reader_out,
                     DeltaType delta_type);

  static Status Open(const string& path,
                     const shared_ptr<RandomAccessFile> &file,
                     uint64_t file_size,
                     const BlockId& block_id,
                     std::tr1::shared_ptr<DeltaFileReader>* reader_out,
                     DeltaType delta_type);

  // See DeltaStore::NewDeltaIterator(...)
  Status NewDeltaIterator(const Schema *projection,
                          const MvccSnapshot &snap,
                          DeltaIterator** iterator) const OVERRIDE;

  // See DeltaStore::CheckRowDeleted
  virtual Status CheckRowDeleted(rowid_t row_idx, bool *deleted) const OVERRIDE;

  virtual const Schema &schema() const OVERRIDE {
    return schema_;
  }

  const string& path() const { return path_; }

  const BlockId& block_id() const { return block_id_; }

  virtual const DeltaStats& delta_stats() const OVERRIDE { return *delta_stats_; }

  virtual std::string ToString() const OVERRIDE {
    return path();
  }

 private:
  friend class DeltaFileIterator;
  friend class DeltaCompactionInput;

  DISALLOW_COPY_AND_ASSIGN(DeltaFileReader);

  const shared_ptr<cfile::CFileReader> &cfile_reader() const {
    return reader_;
  }

  DeltaFileReader(const BlockId& block_id,
                  cfile::CFileReader *cf_reader,
                  const string &path,
                  DeltaType delta_type);

  Status Init();

  Status ReadSchema();

  Status ReadDeltaStats();

  shared_ptr<cfile::CFileReader> reader_;
  gscoped_ptr<DeltaStats> delta_stats_;
  Schema schema_;

  // The path of the file being read (should be used only for debugging)
  const string path_;

  const BlockId block_id_;

  // The type of this delta, i.e. UNDO or REDO.
  const DeltaType delta_type_;
};

// Iterator over the deltas contained in a delta file.
//
// See DeltaIterator for details.
class DeltaFileIterator : public DeltaIterator {
 public:
  Status Init() OVERRIDE;

  Status SeekToOrdinal(rowid_t idx) OVERRIDE;
  Status PrepareBatch(size_t nrows) OVERRIDE;
  Status ApplyUpdates(size_t col_to_apply, ColumnBlock *dst) OVERRIDE;
  Status ApplyDeletes(SelectionVector *sel_vec) OVERRIDE;
  Status CollectMutations(vector<Mutation *> *dst, Arena *arena) OVERRIDE;
  Status FilterColumnsAndAppend(const metadata::ColumnIndexes& col_indexes,
                                vector<DeltaKeyAndUpdate>* out,
                                Arena* arena) OVERRIDE;
  string ToString() const OVERRIDE;
  virtual bool HasNext() OVERRIDE;

 private:
  friend class DeltaFileReader;
  friend struct ApplyingVisitor<REDO>;
  friend struct ApplyingVisitor<UNDO>;
  friend struct CollectingVisitor;
  friend struct DeletingVisitor<REDO>;
  friend struct DeletingVisitor<UNDO>;
  friend struct FilterAndAppendVisitor;

  DISALLOW_COPY_AND_ASSIGN(DeltaFileIterator);

  // PrepareBatch() will read forward all blocks from the deltafile
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


  // The passed 'projection' and 'dfr' must remain valid for the lifetime
  // of the iterator.
  DeltaFileIterator(const std::tr1::shared_ptr<const DeltaFileReader>& dfr,
                    const Schema *projection,
                    const MvccSnapshot &snap,
                    DeltaType delta_type);


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

  std::tr1::shared_ptr<const DeltaFileReader> dfr_;
  shared_ptr<cfile::CFileReader> cfile_reader_;

  // Mapping from projected column index back to memrowset column index.
  DeltaProjector projector_;

  // The MVCC state which determines which deltas should be applied.
  const MvccSnapshot mvcc_snap_;

  gscoped_ptr<cfile::IndexTreeIterator> index_iter_;

  // TODO: add better comments here.
  rowid_t prepared_idx_;
  uint32_t prepared_count_;
  bool prepared_;
  bool exhausted_;

  // After PrepareBatch(), the set of delta blocks in the delta file
  // which correspond to prepared_block_.
  boost::ptr_deque<PreparedDeltaBlock> delta_blocks_;

  // Temporary buffer used in seeking.
  faststring tmp_buf_;

  // Temporary buffer used for RowChangeList projection.
  faststring delta_buf_;

  // The type of this delta iterator, i.e. UNDO or REDO.
  const DeltaType delta_type_;
};


} // namespace tablet
} // namespace kudu

#endif
