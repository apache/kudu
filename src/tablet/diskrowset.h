// Copyright (c) 2012, Cloudera, inc.
//
// A DiskRowSet is a horizontal slice of a Kudu tablet.
// Each DiskRowSet contains data for a a disjoint set of keys.
// See src/tablet/README for a detailed description.

#ifndef KUDU_TABLET_LAYER_H
#define KUDU_TABLET_LAYER_H

#include <boost/thread/shared_mutex.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include "cfile/cfile.h"
#include "cfile/cfile_reader.h"
#include "common/row.h"
#include "common/rowblock.h"
#include "common/row_changelist.h"
#include "common/schema.h"
#include "gutil/macros.h"
#include "tablet/deltamemstore.h"
#include "tablet/delta_tracker.h"
#include "tablet/cfile_set.h"
#include "tablet/rowset.h"
#include "util/bloom_filter.h"
#include "util/memory/arena.h"

namespace kudu {

namespace cfile {
class BloomFileWriter;
}

namespace tablet {

using std::string;
using kudu::cfile::BloomFileWriter;
using kudu::cfile::CFileIterator;
using kudu::cfile::CFileReader;

class MutationResultPB;

class DiskRowSetWriter {
 public:
  // TODO: document ownership of rowset_metadata
  DiskRowSetWriter(metadata::RowSetMetadata *rowset_metadata,
                   const BloomFilterSizing &bloom_sizing) :
    rowset_metadata_(rowset_metadata),
    bloom_sizing_(bloom_sizing),
    finished_(false),
    written_count_(0)
  {}

  ~DiskRowSetWriter();

  Status Open();

  // The block is written to all column writers as well as the bloom filter,
  // if configured.
  // Rows must be appended in ascending order.
  Status AppendBlock(const RowBlock &block);

  Status Finish();

  rowid_t written_count() const {
    CHECK(finished_);
    return written_count_;
  }

  // Return the total number of bytes written so far to this DiskRowSet.
  // Additional bytes may be written by "Finish()", but this should provide
  // a reasonable estimate for the total data size.
  size_t written_size() const;

  const Schema& schema() const { return rowset_metadata_->schema(); }

 private:
  DISALLOW_COPY_AND_ASSIGN(DiskRowSetWriter);

  Status InitBloomFileWriter();

  // Initializes the index writer required for compound keys
  // this index is written to a new file instead of embedded in the col_* files
  Status InitAdHocIndexWriter();

  // Return the cfile::Writer responsible for writing the key index.
  // (the ad-hoc writer for composite keys, otherwise the key column writer)
  cfile::Writer *key_index_writer();

  metadata::RowSetMetadata *rowset_metadata_;
  BloomFilterSizing bloom_sizing_;

  bool finished_;
  rowid_t written_count_;
  std::vector<cfile::Writer *> cfile_writers_;
  gscoped_ptr<BloomFileWriter> bloom_writer_;
  gscoped_ptr<cfile::Writer> ad_hoc_index_writer_;

  // The last encoded key written.
  faststring last_encoded_key_;
};


// Wrapper around DiskRowSetWriter which "rolls" to a new DiskRowSet after
// a certain amount of data has been written. Each output rowset is suffixed
// with ".N" where N starts at 0 and increases as new rowsets are generated.
class RollingDiskRowSetWriter {
 public:
  // Create a new rolling writer. The given 'tablet_metadata' must stay valid
  // for the lifetime of this writer, and is used to construct the new rowsets
  // that this RollingDiskRowSetWriter creates.
  RollingDiskRowSetWriter(metadata::TabletMetadata* tablet_metadata,
                          const Schema &schema,
                          const BloomFilterSizing &bloom_sizing,
                          size_t target_rowset_size);
  ~RollingDiskRowSetWriter();

  Status Open();

  // The block is written to all column writers as well as the bloom filter,
  // if configured.
  // Rows must be appended in ascending order.
  Status AppendBlock(const RowBlock &block);

  Status Finish();

  int64_t written_count() const { return written_count_; }

  const Schema &schema() const { return schema_; }

  // Return the set of rowset paths that were written by this writer.
  // This must only be called after Finish() returns an OK result.
  void GetWrittenMetadata(metadata::RowSetMetadataVector* metas) const;

 private:
  Status RollWriter();
  Status FinishCurrentWriter();

  enum State {
    kInitialized,
    kStarted,
    kFinished
  };
  State state_;

  metadata::TabletMetadata* tablet_metadata_;
  const Schema schema_;
  shared_ptr<metadata::RowSetMetadata> cur_metadata_;
  const BloomFilterSizing bloom_sizing_;
  const size_t target_rowset_size_;

  gscoped_ptr<DiskRowSetWriter> cur_writer_;

  // The index for the next output.
  int output_index_;

  // RowSetMetadata objects for rowsets which have been successfully
  // written out.
  metadata::RowSetMetadataVector written_metas_;

  int64_t written_count_;

  DISALLOW_COPY_AND_ASSIGN(RollingDiskRowSetWriter);
};

////////////////////////////////////////////////////////////
// DiskRowSet
////////////////////////////////////////////////////////////

class MajorDeltaCompaction;
class RowSetColumnUpdater;

class DiskRowSet : public RowSet {
 public:
  static const char *kMinKeyMetaEntryName;
  static const char *kMaxKeyMetaEntryName;

  // Open a rowset from disk.
  // If successful, sets *rowset to the newly open rowset
  static Status Open(const shared_ptr<metadata::RowSetMetadata>& rowset_metadata,
                     shared_ptr<DiskRowSet> *rowset);

  ////////////////////////////////////////////////////////////
  // "Management" functions
  ////////////////////////////////////////////////////////////

  // Flush all accumulated delta data to disk.
  //
  // TODO right now delta flushing is not automated, but when we get to it we
  // need to take into account that if we never flush deltas that receive no
  // updates then log segments will never be garbage collected. This because
  // log GC works by comparing the last delta ids of the row sets in two
  // different superblocks, and if the last deltas of any row set have the same
  // id, then log GC will assume that there is in-mem state in common.
  // A possible solution is to add a last_durable_dms_id to RowSetDataPB and
  // increase it (and increase the id of the current DeltaMemStore) when
  // flushing even if we didn't actually create a delta file.
  Status FlushDeltas();

  // Perform delta store minor compaction.
  // This compacts the delta files down to a single one.
  // If there is already only a single delta file, this does nothing.
  Status MinorCompactDeltaStores();

  ////////////////////////////////////////////////////////////
  // RowSet implementation
  ////////////////////////////////////////////////////////////

  ////////////////////
  // Updates
  ////////////////////

  // Update the given row.
  // 'key' should be the key portion of the row -- i.e a contiguous
  // encoding of the key columns.
  Status MutateRow(txid_t txid,
                   const RowSetKeyProbe &probe,
                   const RowChangeList &update,
                   ProbeStats* stats,
                   MutationResultPB* result);

  Status CheckRowPresent(const RowSetKeyProbe &probe, bool *present, ProbeStats* stats) const;

  ////////////////////
  // Read functions.
  ////////////////////
  RowwiseIterator *NewRowIterator(const Schema *projection,
                                  const MvccSnapshot &snap) const OVERRIDE;

  virtual CompactionInput *NewCompactionInput(const Schema* projection,
                                              const MvccSnapshot &snap) const OVERRIDE;

  // Count the number of rows in this rowset.
  Status CountRows(rowid_t *count) const;

  // See RowSet::GetBounds(...)
  virtual Status GetBounds(Slice *min_encoded_key,
                           Slice *max_encoded_key) const;

  // Estimate the number of bytes on-disk
  uint64_t EstimateOnDiskSize() const;

  size_t DeltaMemStoreSize() const;

  size_t CountDeltaStores() const;

  Status AlterSchema(const Schema& schema);

  boost::mutex *compact_flush_lock() {
    return &compact_flush_lock_;
  }

  DeltaTracker *delta_tracker() {
    return DCHECK_NOTNULL(delta_tracker_.get());
  }

  shared_ptr<metadata::RowSetMetadata> metadata() {
    return rowset_metadata_;
  }

  const Schema& schema() const {
    return rowset_metadata_->schema();
  }

  string ToString() const {
    return rowset_metadata_->ToString();
  }

  virtual Status DebugDump(vector<string> *out = NULL);

 private:
  FRIEND_TEST(TestRowSet, TestRowSetUpdate);
  FRIEND_TEST(TestRowSet, TestDMSFlush);
  FRIEND_TEST(TestCompaction, TestOneToOne);
  DISALLOW_COPY_AND_ASSIGN(DiskRowSet);
  friend class CompactionInput;
  friend class Tablet;

  explicit DiskRowSet(const shared_ptr<metadata::RowSetMetadata>& rowset_metadata);

  Status Open();

  // Create a new major delta compaction object, with 'updater', and a
  // merge of all of deltafiles in this object's delta tracker as
  // input. Sets 'delta_store_id' to the correct delta store id to use
  // for the delta file created by this delta compaction
  // 'updater' must remain valid for the lifetime of the returned
  // NewMajorDeltaCompaction() object.
  MajorDeltaCompaction* NewMajorDeltaCompaction(RowSetColumnUpdater* updater,
                                                int64_t* delta_store_id) const;

  // Points this object's delta memstore to delta memstore in
  // 'rs'.
  void SetDMSFrom(DiskRowSet* rs);

  shared_ptr<metadata::RowSetMetadata> rowset_metadata_;

  bool open_;

  // Base data for this rowset.
  // This vector contains one entry for each column.
  shared_ptr<CFileSet> base_data_;
  shared_ptr<DeltaTracker> delta_tracker_;

  // Lock governing this rowset's inclusion in a compact/flush. If locked,
  // no other compactor will attempt to include this rowset.
  boost::mutex compact_flush_lock_;
};

} // namespace tablet
} // namespace kudu

#endif
