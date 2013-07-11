// Copyright (c) 2012, Cloudera, inc.
//
// A DiskRowSet is a horizontal slice of a Kudu tablet.
// Each DiskRowSet contains data for a a disjoint set of keys.
// See src/tablet/README for a detailed description.

#ifndef KUDU_TABLET_LAYER_H
#define KUDU_TABLET_LAYER_H

#include <boost/ptr_container/ptr_vector.hpp>
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

using boost::ptr_vector;
using std::string;
using kudu::cfile::BloomFileWriter;
using kudu::cfile::CFileIterator;
using kudu::cfile::CFileReader;

class DiskRowSetWriter {
 public:
  DiskRowSetWriter(metadata::RowSetMetadata *rowset_metadata,
                   const Schema &schema,
                   const BloomFilterSizing &bloom_sizing) :
    rowset_metadata_(rowset_metadata),
    schema_(schema),
    bloom_sizing_(bloom_sizing),
    finished_(false),
    written_count_(0)
  {}

  Status Open();

  // Append a new row into the rowset.
  // This is inefficient and should only be used by tests. Real code should use
  // AppendBlock() instead.
  Status WriteRow(const Slice &row);

  // The block is written to all column writers as well as the bloom filter,
  // if configured.
  // Rows must be appended in ascending order.
  Status AppendBlock(const RowBlock &block);

  Status Finish();

  rowid_t written_count() const {
    CHECK(finished_);
    return written_count_;
  }


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
  const Schema schema_;
  BloomFilterSizing bloom_sizing_;

  bool finished_;
  rowid_t written_count_;
  ptr_vector<cfile::Writer> cfile_writers_;
  gscoped_ptr<BloomFileWriter> bloom_writer_;
  gscoped_ptr<cfile::Writer> ad_hoc_index_writer_;

  // The last encoded key written.
  faststring last_encoded_key_;
};

////////////////////////////////////////////////////////////
// DiskRowSet
////////////////////////////////////////////////////////////

class DiskRowSet : public RowSet {
 public:
  static const char *kMinKeyMetaEntryName;
  static const char *kMaxKeyMetaEntryName;

  // Open a rowset from disk.
  // If successful, sets *rowset to the newly open rowset
  static Status Open(const shared_ptr<metadata::RowSetMetadata>& rowset_metadata,
                     const Schema &schema,
                     shared_ptr<DiskRowSet> *rowset);

  ////////////////////////////////////////////////////////////
  // "Management" functions
  ////////////////////////////////////////////////////////////

  // Flush all accumulated delta data to disk.
  Status FlushDeltas();

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
                   const RowChangeList &update);

  Status CheckRowPresent(const RowSetKeyProbe &probe, bool *present) const;

  ////////////////////
  // Read functions.
  ////////////////////
  RowwiseIterator *NewRowIterator(const Schema &projection,
                                  const MvccSnapshot &snap) const;

  CompactionInput *NewCompactionInput(const MvccSnapshot &snap) const;

  // Count the number of rows in this rowset.
  Status CountRows(rowid_t *count) const;

  // See RowSet::GetBounds(...)
  virtual Status GetBounds(Slice *min_encoded_key,
                           Slice *max_encoded_key) const;

  // Estimate the number of bytes on-disk
  uint64_t EstimateOnDiskSize() const;

  boost::mutex *compact_flush_lock() {
    return &compact_flush_lock_;
  }

  shared_ptr<metadata::RowSetMetadata> metadata() {
    return rowset_metadata_;
  }

  const Schema &schema() const {
    return schema_;
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
  friend class Tablet;
  friend class CompactionInput;

  // TODO: should 'schema' be stored with the rowset? quite likely
  // so that we can support cheap alter table.
  DiskRowSet(const shared_ptr<metadata::RowSetMetadata>& rowset_metadata, const Schema &schema);

  Status Open();

  shared_ptr<metadata::RowSetMetadata> rowset_metadata_;
  const Schema schema_;

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
