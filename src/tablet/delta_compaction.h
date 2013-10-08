// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_DELTA_COMPACTION_H
#define KUDU_TABLET_DELTA_COMPACTION_H

#include <string>
#include <vector>
#include <utility>

#include "cfile/cfile.h"
#include "tablet/deltafile.h"

namespace kudu {
namespace tablet {

class DeltaMemStore;
class DeltaKey;
struct DeltaCompactionInputCell;

class DeltaCompactionInput {
 public:

  // Creates a compaction input from a DeltaFileReader. The
  // DeltaFileReader reference must be valid for the lifetime the
  // DeltaCompactionInput. The projection is the compaction output schema.
  static Status Open(const DeltaFileReader &reader,
                     const Schema& projection,
                     gscoped_ptr<DeltaCompactionInput> *input);

  static Status Open(const DeltaMemStore &dms,
                     const Schema& projection,
                     gscoped_ptr<DeltaCompactionInput> *input);

  // Create a merging iterator from multiple DeltaCompactionInputs
  // that will combine the deltas in the correct order -- first by rows,
  // then by the transaction ids -- such that they can be sequentially
  // appended to an on-disk delta-file.
  static DeltaCompactionInput *Merge(const Schema& projection,
                                     const vector<shared_ptr<DeltaCompactionInput> > &inputs);

  virtual Status Init() = 0;

  // Fetch the next block of deltas into memory for processing.
  virtual Status PrepareBlock(vector<DeltaCompactionInputCell> *block) = 0;

  // Finish processing the current block (if any) and position to fetch the next
  // block of deltas.
  virtual Status FinishBlock() = 0;

  virtual bool HasMoreBlocks() = 0;

  virtual const Schema& schema() const = 0;

  virtual ~DeltaCompactionInput() {}
};

struct DeltaCompactionInputCell {
  DeltaKey key;
  Slice cell;
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
                      const metadata::RowSetMetadata* input_rowset_metadata,
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

 private:
  DISALLOW_COPY_AND_ASSIGN(RowSetColumnUpdater);

  metadata::TabletMetadata* tablet_meta_;
  const metadata::RowSetMetadata* const input_rowset_meta_;
  const metadata::ColumnIndexes column_indexes_;
  std::tr1::unordered_map<size_t, cfile::Writer*> column_writers_;

  bool finished_;
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
