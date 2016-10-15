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
//
// A DiskRowSet is a horizontal slice of a Kudu tablet.
// Each DiskRowSet contains data for a a disjoint set of keys.
// See src/kudu/tablet/README for a detailed description.

#ifndef KUDU_TABLET_DISKROWSET_H_
#define KUDU_TABLET_DISKROWSET_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/rowid.h"
#include "kudu/common/schema.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/delta_tracker.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet_mem_trackers.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/util/bloom_filter.h"
#include "kudu/util/faststring.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {

class MonoTime;
class RowBlock;
class RowChangeList;
class RowwiseIterator;
class Timestamp;

namespace cfile {
class BloomFileWriter;
class CFileWriter;
}

namespace consensus {
class OpId;
}

namespace log {
class LogAnchorRegistry;
}

namespace tablet {

class CFileSet;
class CompactionInput;
class DeltaFileWriter;
class DeltaStats;
class HistoryGcOpts;
class MultiColumnWriter;
class Mutation;
class MvccSnapshot;
class OperationResultPB;

class DiskRowSetWriter {
 public:
  // TODO: document ownership of rowset_metadata
  DiskRowSetWriter(RowSetMetadata* rowset_metadata, const Schema* schema,
                   BloomFilterSizing bloom_sizing);

  ~DiskRowSetWriter();

  Status Open();

  // The block is written to all column writers as well as the bloom filter,
  // if configured.
  // Rows must be appended in ascending order.
  Status AppendBlock(const RowBlock &block);

  // Closes the CFiles and their underlying writable blocks.
  // If no rows were written, returns Status::Aborted().
  Status Finish();

  // Closes the CFiles, releasing the underlying blocks to 'closer'.
  // If no rows were written, returns Status::Aborted().
  Status FinishAndReleaseBlocks(fs::ScopedWritableBlockCloser* closer);

  // The base DiskRowSetWriter never rolls. This method is necessary for tests
  // which are templatized on the writer type.
  Status RollIfNecessary() { return Status::OK(); }

  rowid_t written_count() const {
    CHECK(finished_);
    return written_count_;
  }

  // Return the total number of bytes written so far to this DiskRowSet.
  // Additional bytes may be written by "Finish()", but this should provide
  // a reasonable estimate for the total data size.
  size_t written_size() const;

  const Schema& schema() const { return *schema_; }

 private:
  DISALLOW_COPY_AND_ASSIGN(DiskRowSetWriter);

  Status InitBloomFileWriter();

  // Initializes the index writer required for compound keys
  // this index is written to a new file instead of embedded in the col_* files
  Status InitAdHocIndexWriter();

  // Return the cfile::Writer responsible for writing the key index.
  // (the ad-hoc writer for composite keys, otherwise the key column writer)
  cfile::CFileWriter *key_index_writer();

  RowSetMetadata *rowset_metadata_;
  const Schema* const schema_;

  BloomFilterSizing bloom_sizing_;

  bool finished_;
  rowid_t written_count_;
  gscoped_ptr<MultiColumnWriter> col_writer_;
  gscoped_ptr<cfile::BloomFileWriter> bloom_writer_;
  gscoped_ptr<cfile::CFileWriter> ad_hoc_index_writer_;

  // The last encoded key written.
  faststring last_encoded_key_;
};


// Wrapper around DiskRowSetWriter which "rolls" to a new DiskRowSet after
// a certain amount of data has been written. Each output rowset is suffixed
// with ".N" where N starts at 0 and increases as new rowsets are generated.
//
// See AppendBlock(...) for important usage information.
class RollingDiskRowSetWriter {
 public:
  // Create a new rolling writer. The given 'tablet_metadata' must stay valid
  // for the lifetime of this writer, and is used to construct the new rowsets
  // that this RollingDiskRowSetWriter creates.
  RollingDiskRowSetWriter(TabletMetadata* tablet_metadata, const Schema& schema,
                          BloomFilterSizing bloom_sizing,
                          size_t target_rowset_size);
  ~RollingDiskRowSetWriter();

  Status Open();

  // The block is written to all column writers as well as the bloom filter,
  // if configured.
  // Rows must be appended in ascending order.
  //
  // NOTE: data must be appended in a particular order: for each set of rows
  // you must append deltas using the APIs below *before* appending the block
  // of rows that they correspond to. This ensures that the output delta files
  // and data files are aligned.
  Status AppendBlock(const RowBlock &block);

  // Appends a sequence of REDO deltas for the same row to the current
  // redo delta file. 'row_idx_in_next_block' is the positional index after
  // the last written block. The 'row_idx_in_drs' out parameter will be set
  // with the row index from the start of the DiskRowSet currently being written.
  Status AppendRedoDeltas(rowid_t row_idx_in_next_block,
                          Mutation* redo_deltas,
                          rowid_t* row_idx_in_drs);

  // Appends a sequence of UNDO deltas for the same row to the current
  // undo delta file. 'row_idx_in_next_block' is the positional index after
  // the last written block. The 'row_idx_in_drs' out parameter will be set
  // with the row index from the start of the DiskRowSet currently being written.
  Status AppendUndoDeltas(rowid_t row_idx_in_next_block,
                          Mutation* undo_deltas,
                          rowid_t* row_idx_in_drs);

  // Try to roll the output, if we've passed the configured threshold. This will
  // only roll if called immediately after an AppendBlock() call. The implementation
  // of AppendBlock() doesn't call it automatically, because it doesn't know if there
  // is any more data to be appended. It is safe to call this in other circumstances --
  // it will be ignored if it is not a good time to roll.
  Status RollIfNecessary();

  Status Finish();

  int64_t written_count() const { return written_count_; }

  const Schema &schema() const { return schema_; }

  // Return the set of rowset paths that were written by this writer.
  // This must only be called after Finish() returns an OK result.
  void GetWrittenRowSetMetadata(RowSetMetadataVector* metas) const;

  uint64_t written_size() const { return written_size_; }

 private:
  Status RollWriter();

  // Close the current DRS and delta writers, releasing their finished blocks
  // into block_closer_.
  Status FinishCurrentWriter();

  template<DeltaType Type>
  Status AppendDeltas(rowid_t row_idx_in_block,
                      Mutation* delta_head,
                      rowid_t* row_idx,
                      DeltaFileWriter* writer,
                      DeltaStats* delta_stats);

  enum State {
    kInitialized,
    kStarted,
    kFinished
  };
  State state_;

  TabletMetadata* tablet_metadata_;
  const Schema schema_;
  std::shared_ptr<RowSetMetadata> cur_drs_metadata_;
  const BloomFilterSizing bloom_sizing_;
  const size_t target_rowset_size_;

  gscoped_ptr<DiskRowSetWriter> cur_writer_;

  // A delta writer to store the undos for each DRS
  gscoped_ptr<DeltaFileWriter> cur_undo_writer_;
  gscoped_ptr<DeltaStats> cur_undo_delta_stats;
  // a delta writer to store the redos for each DRS
  gscoped_ptr<DeltaFileWriter> cur_redo_writer_;
  gscoped_ptr<DeltaStats> cur_redo_delta_stats;
  BlockId cur_undo_ds_block_id_;
  BlockId cur_redo_ds_block_id_;

  uint64_t row_idx_in_cur_drs_;

  // True when we are allowed to roll. We can only roll when the delta writers
  // and data writers are aligned (i.e. just after we've appended a new block of data).
  bool can_roll_;

  // RowSetMetadata objects for diskrowsets which have been successfully
  // written out.
  RowSetMetadataVector written_drs_metas_;

  int64_t written_count_;
  uint64_t written_size_;

  // Syncs and closes all outstanding blocks when the rolling writer is
  // destroyed.
  fs::ScopedWritableBlockCloser block_closer_;

  DISALLOW_COPY_AND_ASSIGN(RollingDiskRowSetWriter);
};

////////////////////////////////////////////////////////////
// DiskRowSet
////////////////////////////////////////////////////////////

class MajorDeltaCompaction;

class DiskRowSet : public RowSet {
 public:
  static const char *kMinKeyMetaEntryName;
  static const char *kMaxKeyMetaEntryName;

  // Open a rowset from disk.
  // If successful, sets *rowset to the newly open rowset
  static Status Open(const std::shared_ptr<RowSetMetadata>& rowset_metadata,
                     log::LogAnchorRegistry* log_anchor_registry,
                     const TabletMemTrackers& mem_trackers,
                     std::shared_ptr<DiskRowSet> *rowset);

  ////////////////////////////////////////////////////////////
  // "Management" functions
  ////////////////////////////////////////////////////////////

  // Flush all accumulated delta data to disk.
  Status FlushDeltas() OVERRIDE;

  // Perform delta store minor compaction.
  // This compacts the delta files down to a single one.
  // If there is already only a single delta file, this does nothing.
  Status MinorCompactDeltaStores() OVERRIDE;

  ////////////////////////////////////////////////////////////
  // RowSet implementation
  ////////////////////////////////////////////////////////////

  ////////////////////
  // Updates
  ////////////////////

  // Update the given row.
  // 'key' should be the key portion of the row -- i.e a contiguous
  // encoding of the key columns.
  Status MutateRow(Timestamp timestamp,
                   const RowSetKeyProbe &probe,
                   const RowChangeList &update,
                   const consensus::OpId& op_id,
                   ProbeStats* stats,
                   OperationResultPB* result) OVERRIDE;

  Status CheckRowPresent(const RowSetKeyProbe &probe,
                         bool *present,
                         ProbeStats* stats) const OVERRIDE;

  ////////////////////
  // Read functions.
  ////////////////////
  virtual Status NewRowIterator(const Schema *projection,
                                const MvccSnapshot &mvcc_snap,
                                OrderMode order,
                                gscoped_ptr<RowwiseIterator>* out) const OVERRIDE;

  virtual Status NewCompactionInput(const Schema* projection,
                                    const MvccSnapshot &snap,
                                    gscoped_ptr<CompactionInput>* out) const OVERRIDE;

  // Count the number of rows in this rowset.
  Status CountRows(rowid_t *count) const OVERRIDE;

  // See RowSet::GetBounds(...)
  virtual Status GetBounds(std::string* min_encoded_key,
                           std::string* max_encoded_key) const OVERRIDE;

  // Estimate the on-disk size of this rowset's cfile set, including bloomfiles
  // and the ad hoc index.
  uint64_t BaseDataOnDiskSize() const;

  // Estimate the size on-disk of the data in this rowset's cfile set.
  uint64_t BaseDataOnDiskSizeNoMetadata() const;

  // Estimate the size on-disk of this rowset's REDO deltas.
  uint64_t RedoDeltaOnDiskSize() const;

  uint64_t OnDiskSize() const OVERRIDE;

  uint64_t OnDiskDataSizeNoUndos() const OVERRIDE;

  size_t DeltaMemStoreSize() const OVERRIDE;

  bool DeltaMemStoreEmpty() const OVERRIDE;

  int64_t MinUnflushedLogIndex() const OVERRIDE;

  size_t CountDeltaStores() const;

  double DeltaStoresCompactionPerfImprovementScore(DeltaCompactionType type) const OVERRIDE;

  Status EstimateBytesInPotentiallyAncientUndoDeltas(Timestamp ancient_history_mark,
                                                     int64_t* bytes) OVERRIDE;

  Status InitUndoDeltas(Timestamp ancient_history_mark,
                        MonoTime deadline,
                        int64_t* delta_blocks_initialized,
                        int64_t* bytes_in_ancient_undos) OVERRIDE;

  Status DeleteAncientUndoDeltas(Timestamp ancient_history_mark,
                                 int64_t* blocks_deleted, int64_t* bytes_deleted) OVERRIDE;

  // Major compacts all the delta files for all the columns.
  Status MajorCompactDeltaStores(HistoryGcOpts history_gc_opts);

  std::mutex *compact_flush_lock() OVERRIDE {
    return &compact_flush_lock_;
  }

  DeltaTracker *delta_tracker() {
    return DCHECK_NOTNULL(delta_tracker_.get());
  }

  std::shared_ptr<RowSetMetadata> metadata() OVERRIDE {
    return rowset_metadata_;
  }

  std::string ToString() const OVERRIDE {
    return rowset_metadata_->ToString();
  }

  std::string LogPrefix() const {
    return strings::Substitute("T $0 P $1: $2: ",
        rowset_metadata_->tablet_metadata()->tablet_id(),
        rowset_metadata_->fs_manager()->uuid(),
        ToString());
  }

  virtual Status DebugDump(std::vector<std::string> *lines = NULL) OVERRIDE;

 private:
  FRIEND_TEST(TestRowSet, TestRowSetUpdate);
  FRIEND_TEST(TestRowSet, TestDMSFlush);
  FRIEND_TEST(TestCompaction, TestOneToOne);
  FRIEND_TEST(TabletHistoryGcTest, TestMajorDeltaCompactionOnSubsetOfColumns);

  friend class CompactionInput;
  friend class Tablet;

  DiskRowSet(std::shared_ptr<RowSetMetadata> rowset_metadata,
             log::LogAnchorRegistry* log_anchor_registry,
             TabletMemTrackers mem_trackers);

  Status Open();

  // Create a new major delta compaction object to compact the specified columns.
  Status NewMajorDeltaCompaction(const std::vector<ColumnId>& col_ids,
                                 HistoryGcOpts history_gc_opts,
                                 gscoped_ptr<MajorDeltaCompaction>* out) const;

  // Major compacts all the delta files for the specified columns.
  Status MajorCompactDeltaStoresWithColumnIds(const std::vector<ColumnId>& col_ids,
                                              HistoryGcOpts history_gc_opts);

  std::shared_ptr<RowSetMetadata> rowset_metadata_;

  bool open_;

  log::LogAnchorRegistry* log_anchor_registry_;

  TabletMemTrackers mem_trackers_;

  // Base data for this rowset.
  mutable rw_spinlock component_lock_;
  std::shared_ptr<CFileSet> base_data_;
  gscoped_ptr<DeltaTracker> delta_tracker_;

  // Lock governing this rowset's inclusion in a compact/flush. If locked,
  // no other compactor will attempt to include this rowset.
  std::mutex compact_flush_lock_;

  DISALLOW_COPY_AND_ASSIGN(DiskRowSet);
};

} // namespace tablet
} // namespace kudu

#endif // KUDU_TABLET_DISKROWSET_H_
