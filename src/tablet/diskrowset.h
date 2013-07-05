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

class Env;

namespace cfile {
class BloomFileWriter;
}

namespace tablet {

using std::string;
using kudu::cfile::BloomFileWriter;
using kudu::cfile::CFileIterator;
using kudu::cfile::CFileReader;

class DiskRowSetWriter {
 public:
  DiskRowSetWriter(Env *env,
              const Schema &schema,
              const string &rowset_dir,
              const BloomFilterSizing &bloom_sizing) :
    env_(env),
    schema_(schema),
    dir_(rowset_dir),
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

  const Schema &schema() const { return schema_; }

 private:
  DISALLOW_COPY_AND_ASSIGN(DiskRowSetWriter);

  Status InitBloomFileWriter();

  // Initializes the index writer required for compound keys
  // this index is written to a new file instead of embedded in the col_* files
  Status InitAdHocIndexWriter();

  // Return the cfile::Writer responsible for writing the key index.
  // (the ad-hoc writer for composite keys, otherwise the key column writer)
  cfile::Writer *key_index_writer();

  Env *env_;
  const Schema schema_;
  const string dir_;
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
  RollingDiskRowSetWriter(Env *env,
                          const Schema &schema,
                          const string &base_path,
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
  void GetWrittenPaths(std::vector<std::string> *paths) const;

 private:
  Status RollWriter();
  Status FinishCurrentWriter();

  enum State {
    kInitialized,
    kStarted,
    kFinished
  };
  State state_;

  Env * const env_;
  const Schema schema_;
  const std::string base_path_;
  std::string cur_path_;
  const BloomFilterSizing bloom_sizing_;
  const size_t target_rowset_size_;

  gscoped_ptr<DiskRowSetWriter> cur_writer_;

  // The index for the next output.
  int output_index_;

  // DiskRowSet paths which have been successfully written out.
  std::vector<std::string> written_paths_;

  int64_t written_count_;

  DISALLOW_COPY_AND_ASSIGN(RollingDiskRowSetWriter);
};

////////////////////////////////////////////////////////////
// DiskRowSet
////////////////////////////////////////////////////////////

class DiskRowSet : public RowSet {
 public:
  static const char *kDeltaPrefix;
  static const char *kColumnPrefix;
  static const char *kBloomFileName;
  static const char *kAdHocIdxFileName;
  static const char *kTmpRowSetSuffix;

  static const char *kMinKeyMetaEntryName;
  static const char *kMaxKeyMetaEntryName;

  // Open a rowset from disk.
  // If successful, sets *rowset to the newly open rowset
  static Status Open(Env *env,
                     const Schema &schema,
                     const string &rowset_dir,
                     shared_ptr<DiskRowSet> *rowset);

  ////////////////////////////////////////////////////////////
  // "Management" functions
  ////////////////////////////////////////////////////////////

  // Flush all accumulated delta data to disk.
  Status FlushDeltas();

  // Delete the rowset directory.
  Status Delete();

  // Rename the directory where this rowset is stored.
  Status RenameRowSetDir(const string &new_dir);


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

  const Schema &schema() const {
    return schema_;
  }

  string ToString() const {
    return dir_;
  }

  virtual Status DebugDump(vector<string> *out = NULL);

  static string GetColumnPath(const string &dir, int col_idx);
  static string GetDeltaPath(const string &dir, int delta_idx);
  static string GetBloomPath(const string &dir);
  static string GetAdHocIndexPath(const string &dir);

 private:
  FRIEND_TEST(TestRowSet, TestRowSetUpdate);
  FRIEND_TEST(TestRowSet, TestDMSFlush);
  FRIEND_TEST(TestCompaction, TestOneToOne);
  DISALLOW_COPY_AND_ASSIGN(DiskRowSet);
  friend class Tablet;
  friend class CompactionInput;

  // TODO: should 'schema' be stored with the rowset? quite likely
  // so that we can support cheap alter table.
  DiskRowSet(Env *env,
        const Schema &schema,
        const string &rowset_dir);

  Status Open();

  Env *env_;
  const Schema schema_;
  string dir_;

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
