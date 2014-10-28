// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TABLET_DELTA_COMPACTION_H
#define KUDU_TABLET_DELTA_COMPACTION_H

#include <string>
#include <vector>
#include <utility>
#include <tr1/unordered_map>

#include "kudu/cfile/cfile_writer.h"
#include "kudu/tablet/deltafile.h"

namespace kudu {

namespace metadata {
class RowSetMetadata;
} // namespace metadata

namespace tablet {

class CFileSet;
class DeltaMemStore;
class DeltaKey;
class MultiColumnWriter;

// Handles major delta compaction: applying deltas to specific columns
// of a DiskRowSet, writing out an updated DiskRowSet without re-writing the
// unchanged columns (see RowSetColumnUpdater), and writing out a new
// deltafile which does not contain the deltas applied to the specific rows.
class MajorDeltaCompaction {
 public:
  // Creates a new major delta compaction. The given 'base_data' should already
  // be open and must remain valid for the lifetime of this object.
  // 'delta_iter' must not be initialized.
  // col_indexes determines which columns of 'base_schema' should be compacted.
  //
  // TODO: is base_schema supposed to be the same as base_data->schema()? how about
  // in an ALTER scenario?
  MajorDeltaCompaction(FsManager* fs_manager,
                       const Schema& base_schema,
                       CFileSet* base_data,
                       const shared_ptr<DeltaIterator>& delta_iter,
                       const std::vector<std::tr1::shared_ptr<DeltaStore> >& included_stores,
                       const ColumnIndexes& col_indexes);
  ~MajorDeltaCompaction();

  // Executes the compaction.
  // This has no effect on the metadata of the tablet, etc.
  Status Compact();

  // After a compaction is successful, prepares a metadata update which:
  // 1) swaps out the old columns for the new ones
  // 2) removes the compacted deltas
  // 3) adds the new REDO delta which contains any uncompacted deltas
  Status CreateMetadataUpdate(RowSetMetadataUpdate* update);

  // Apply the changes to the given delta tracker.
  Status UpdateDeltaTracker(DeltaTracker* tracker);

 private:
  std::string ColumnNamesToString() const;

  Status OpenNewColumns();
  Status OpenNewDeltaBlock();
  Status FlushRowSetAndDeltas();

  FsManager* const fs_manager_;

  // TODO: doc me
  const Schema base_schema_;

  // The computed partial schema which includes only the columns being
  // compacted.
  Schema partial_schema_;

  // The column indexes to compact (relative to the base schema)
  const ColumnIndexes column_indexes_;

  // Mapping from base schema index to partial schema index.
  std::tr1::unordered_map<size_t, size_t> old_to_new_;

  // Inputs:
  //-----------------

  // The base data into which deltas are being compacted.
  CFileSet* const base_data_;

  // The DeltaStores from which deltas are being read.
  const SharedDeltaStoreVector included_stores_;

  // The merged view of the deltas from included_stores_.
  const shared_ptr<DeltaIterator> delta_iter_;

  // Outputs:
  gscoped_ptr<MultiColumnWriter> col_writer_;
  gscoped_ptr<DeltaFileWriter> delta_writer_;
  BlockId new_delta_block_;

  size_t nrows_;
  size_t deltas_written_;

  enum State {
    kInitialized = 1,
    kFinished = 2,
  };
  State state_;
};

} // namespace tablet
} // namespace kudu

#endif
