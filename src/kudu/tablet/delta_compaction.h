// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#ifndef KUDU_TABLET_DELTA_COMPACTION_H
#define KUDU_TABLET_DELTA_COMPACTION_H

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/fs/block_id.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/tablet/compaction.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/util/status.h"

namespace kudu {

class FsManager;

namespace tablet {

class CFileSet;
class DeltaFileWriter;
class DeltaTracker;
class MultiColumnWriter;
class RowSetMetadataUpdate;

// Handles major delta compaction: applying deltas to specific columns
// of a DiskRowSet, writing out an updated DiskRowSet without re-writing the
// unchanged columns (see RowSetColumnUpdater), and writing out a new
// deltafile which does not contain the deltas applied to the specific rows.
class MajorDeltaCompaction {
 public:
  // Creates a new major delta compaction. The given 'base_data' should already
  // be open and must remain valid for the lifetime of this object.
  // 'delta_iter' must not be initialized.
  // 'col_ids' determines which columns of 'base_schema' should be compacted.
  //
  // TODO: is base_schema supposed to be the same as base_data->schema()? how about
  // in an ALTER scenario?
  MajorDeltaCompaction(
      FsManager* fs_manager, const Schema& base_schema, CFileSet* base_data,
      std::unique_ptr<DeltaIterator> delta_iter,
      std::vector<std::shared_ptr<DeltaStore> > included_stores,
      std::vector<ColumnId> col_ids,
      HistoryGcOpts history_gc_opts,
      std::string tablet_id);
  ~MajorDeltaCompaction();

  // Executes the compaction.
  // This has no effect on the metadata of the tablet, etc.
  Status Compact();

  // After a compaction is successful, prepares a metadata update which:
  // 1) swaps out the old columns for the new ones
  // 2) removes the compacted deltas
  // 3) adds the new REDO delta which contains any uncompacted deltas
  void CreateMetadataUpdate(RowSetMetadataUpdate* update);

  // Apply the changes to the given delta tracker.
  Status UpdateDeltaTracker(DeltaTracker* tracker);

 private:
  std::string ColumnNamesToString() const;

  // Opens a writer for the base data.
  Status OpenBaseDataWriter();

  // Opens a writer for the REDO delta file, won't be called if we don't need to write
  // back REDO delta mutations.
  Status OpenRedoDeltaFileWriter();

  // Opens a writer for the UNDO delta file, won't be called if we don't need to write
  // back UNDO delta mutations.
  Status OpenUndoDeltaFileWriter();

  // Reads the current base data, applies the deltas, and then writes the new base data.
  // A new delta file is written if not all columns were selected for compaction and some
  // deltas need to be written back into a delta file.
  Status FlushRowSetAndDeltas();

  FsManager* const fs_manager_;

  // TODO: doc me
  const Schema base_schema_;

  // The computed partial schema which includes only the columns being
  // compacted.
  Schema partial_schema_;

  // The column ids to compact.
  const std::vector<ColumnId> column_ids_;

  const HistoryGcOpts history_gc_opts_;

  // Inputs:
  //-----------------

  // The base data into which deltas are being compacted.
  CFileSet* const base_data_;

  // The DeltaStores from which deltas are being read.
  const SharedDeltaStoreVector included_stores_;

  // The merged view of the deltas from included_stores_.
  const std::unique_ptr<DeltaIterator> delta_iter_;

  // The ID of the tablet being compacted.
  const std::string tablet_id_;

  // Outputs:
  gscoped_ptr<MultiColumnWriter> base_data_writer_;
  // The following two may not be initialized if we don't need to write a delta file.
  gscoped_ptr<DeltaFileWriter> new_redo_delta_writer_;
  BlockId new_redo_delta_block_;

  gscoped_ptr<DeltaFileWriter> new_undo_delta_writer_;
  BlockId new_undo_delta_block_;

  size_t redo_delta_mutations_written_;
  size_t undo_delta_mutations_written_;

  enum State {
    kInitialized = 1,
    kFinished = 2,
  };
  State state_;
};

} // namespace tablet
} // namespace kudu

#endif
