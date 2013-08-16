// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_DELTA_COMPACTION_H
#define KUDU_TABLET_DELTA_COMPACTION_H

#include "tablet/deltafile.h"

namespace kudu {

namespace tablet {

class DeltaKey;
struct DeltaCompactionInputCell;

class DeltaCompactionInput {
 public:

  // Creates a compaction input from a DeltaFileReader. The
  // DeltaFileReader reference must be valid for the lifetime the
  // DeltaCompactionInput.
  static Status Open(const DeltaFileReader &reader,
                     gscoped_ptr<DeltaCompactionInput> *input);

  // Create a merging iterator from multiple DeltaCompactionInputs
  // that will combine the deltas in the correct order -- first by rows,
  // then by the transaction ids -- such that they can be sequentially
  // appended to an on-disk delta-file.
  static DeltaCompactionInput *Merge(const vector<shared_ptr<DeltaCompactionInput> > &inputs);

  virtual Status Init() = 0;

  // Fetch the next block of deltas into memory for processing.
  virtual Status PrepareBlock(vector<DeltaCompactionInputCell> *block) = 0;

  // Finish processing the current block (if any) and position to fetch the next
  // block of deltas.
  virtual Status FinishBlock() = 0;

  virtual bool HasMoreBlocks() = 0;

  virtual ~DeltaCompactionInput() {}
};

struct DeltaCompactionInputCell {
  DeltaKey key;
  Slice cell;
};


// Populate "lines" with a humanly readable string representing each
// delta read from the "input" iterator
Status DebugDumpDeltaCompactionInput(DeltaCompactionInput *input, vector<string> *lines,
                                     const Schema &schema);

} // namespace tablet
} // namespace kudu

#endif
