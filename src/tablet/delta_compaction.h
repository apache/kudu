// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_DELTA_COMPACTION_H
#define KUDU_TABLET_DELTA_COMPACTION_H

#include <string>
#include <vector>
#include <utility>
#include <tr1/unordered_map>

#include "cfile/cfile.h"
#include "tablet/deltafile.h"

namespace kudu {
namespace tablet {

class DeltaMemStore;
class DeltaKey;

class DeltaCompactionInput {
 public:

  // Creates a compaction input from a DeltaFileReader.
  // 'projection' is the compaction output schema.
  static Status Open(const std::tr1::shared_ptr<DeltaFileReader>& reader,
                     const Schema* projection,
                     gscoped_ptr<DeltaCompactionInput> *input);

  static Status Open(const DeltaMemStore &dms,
                     const Schema* projection,
                     gscoped_ptr<DeltaCompactionInput> *input);

  // Create a merging iterator from multiple DeltaCompactionInputs
  // that will combine the deltas in the correct order -- first by rows,
  // then by the transaction ids -- such that they can be sequentially
  // appended to an on-disk delta-file.
  static DeltaCompactionInput *Merge(const Schema& projection,
                                     const vector<shared_ptr<DeltaCompactionInput> > &inputs);

  virtual Status Init() = 0;

  // Fetch the next block of deltas into memory for processing.
  virtual Status PrepareBlock(vector<DeltaKeyAndUpdate> *block) = 0;

  // Finish processing the current block (if any) and position to fetch the next
  // block of deltas.
  virtual Status FinishBlock() = 0;

  virtual bool HasMoreBlocks() = 0;

  virtual const Schema& schema() const = 0;

  virtual const DeltaStats& stats() const = 0;

  // Returns the projector from the schema used when the deltas were
  // added to this input's underlying store to the current schema.
  virtual const DeltaProjector* delta_projector() const = 0;

  virtual ~DeltaCompactionInput() {}
};

// Inspired by DiskRowSetWriter, but handles rewriting only specified
// columns of a single rowset. Data blocks for unchanged columns as well
// as the index and bloom filter blocks are kept the same as in the
// original rowset. This class is not thread-safe: the caller must
// synchronize access to tablet_metadata.
//
// TODO: we probably shouldn't create a new RowSetMetadata right away
class RowSetColumnUpdater {
 public:
  // Create a new column updater. The given 'tablet_metadata' and
  // 'input_rowset_metadata' must remain valid until Open(), as they are
  // used to create the new rowset and copy the ids of the existing data
  // blocks; 'col_indexes' must be sorted.
  RowSetColumnUpdater(metadata::TabletMetadata* tablet_metadata,
                      const shared_ptr<metadata::RowSetMetadata> &input_rowset_metadata,
                      const metadata::ColumnIndexes& col_indexes);

  ~RowSetColumnUpdater();

  // Set 'output_rowset_meta' to a new RowSetMetadata, create new column
  // blocks for columns specified in the constructor, and start writers
  // for the columns being rewritten.
  Status Open(shared_ptr<metadata::RowSetMetadata>* output_rowset_meta);

  // Writes the specified columns from RowBlock into the newly opened RowSet.
  // Rows must be appended in an ascending order.
  //
  // See major_delta_compaction-test.cc (TestRowSetColumnUpdater) for
  // sample use.
  Status AppendColumnsFromRowBlock(const RowBlock& row_block);

  Status Finish();

  string ColumnNamesToString() const;

  const metadata::ColumnIndexes& column_indexes() const {
    return column_indexes_;
  }

  const Schema& base_schema() const {
    return base_schema_;
  }

  const Schema& partial_schema() const {
    return partial_schema_;
  }

  const shared_ptr<metadata::RowSetMetadata> &input_rowset_meta() const {
    return input_rowset_meta_;
  }

  size_t old_to_new(size_t old_idx) {
    return old_to_new_[old_idx];
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(RowSetColumnUpdater);

  metadata::TabletMetadata* tablet_meta_;
  shared_ptr<metadata::RowSetMetadata> input_rowset_meta_;
  const metadata::ColumnIndexes column_indexes_;
  const Schema base_schema_;
  std::tr1::unordered_map<size_t, cfile::Writer*> column_writers_;
  bool finished_;

  Schema partial_schema_;
  std::tr1::unordered_map<size_t, size_t> old_to_new_;
};

// Handles major delta compaction: applying deltas to specific columns
// of a DiskRowSet, writing out an updated DiskRowSet without re-writing the
// unchanged columns (see RowSetColumnUpdater), and writing out a new
// deltafile which does not contain the deltas applied to the specific rows.
//
// TODO: creation and instantion of this class together with RowSetColumnUpdater
// is somewhat awkward, could be improved.
class MajorDeltaCompaction {
 public:
  // Creates a new major delta compaction request. The given 'rsu' must not
  // have been opened and must remain valid for the lifetime of this object;
  // 'delta_iter' must not be initialized.
  MajorDeltaCompaction(const shared_ptr<DeltaIterator>& delta_iter,
                       RowSetColumnUpdater* rsu);

  // Executes a compaction request specified in this class, setting
  // 'output' to the newly created rowset, 'block_id' to the newly created
  // delta file block, and deltas_written to the number of deltas written out
  // during the compaction.
  Status Compact(shared_ptr<metadata::RowSetMetadata> *output,
                 BlockId* block_id, size_t* deltas_written);

 private:
  // Helper for Compact() method; 'dfw' must remain valid for duration of
  // of this method's run. This method will call Start() and Stop() on dfw.
  // See also: Compact()
  Status FlushRowSetAndDeltas(DeltaFileWriter* dfw, size_t* deltas_written);

  shared_ptr<DeltaIterator> delta_iter_;
  RowSetColumnUpdater* rsu_;
  size_t nrows_;

  enum State {
    kInitialized = 1,
    kFinished = 2,
  };
  State state_;
};


// Populate "lines" with a humanly readable string representing each
// delta read from the "input" iterator
Status DebugDumpDeltaCompactionInput(DeltaCompactionInput *input, vector<string> *lines,
                                     const Schema &schema);


// Iterates through the specified compaction input and flushes them to the "out"
// delta writer
Status FlushDeltaCompactionInput(DeltaCompactionInput *input, DeltaFileWriter *out);

} // namespace tablet
} // namespace kudu

#endif
