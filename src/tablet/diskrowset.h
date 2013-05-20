// Copyright (c) 2012, Cloudera, inc.
//
// A RowSet is a horizontal slice of a Kudu tablet.
// Each RowSet contains data for a a disjoint set of keys.
// See src/tablet/README for a detailed description.

#ifndef KUDU_TABLET_LAYER_H
#define KUDU_TABLET_LAYER_H

#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <gtest/gtest.h>
#include <string>
#include <memory>

#include "cfile/cfile.h"
#include "cfile/cfile_reader.h"
#include "common/row.h"
#include "common/rowblock.h"
#include "common/row_changelist.h"
#include "common/schema.h"
#include "tablet/deltafile.h"
#include "tablet/deltamemstore.h"
#include "tablet/delta_tracker.h"
#include "tablet/cfile_set.h"
#include "util/bloom_filter.h"
#include "util/memory/arena.h"

namespace kudu {

class Env;

namespace cfile {
class BloomFileWriter;
}

namespace tablet {

using boost::ptr_vector;
using std::string;
using kudu::cfile::BloomFileWriter;
using kudu::cfile::CFileIterator;
using kudu::cfile::CFileReader;

class RowSetWriter : boost::noncopyable {
public:
  RowSetWriter(Env *env,
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

  Status InitBloomFileWriter();

  Env *env_;
  const Schema schema_;
  const string dir_;
  BloomFilterSizing bloom_sizing_;

  bool finished_;
  rowid_t written_count_;
  ptr_vector<cfile::Writer> cfile_writers_;
  gscoped_ptr<BloomFileWriter> bloom_writer_;

  faststring tmp_buf_;
};

////////////////////////////////////////////////////////////
// RowSet
////////////////////////////////////////////////////////////

class RowSet : public RowSetInterface, boost::noncopyable {
public:
  static const char *kDeltaPrefix;
  static const char *kColumnPrefix;
  static const char *kBloomFileName;
  static const char *kTmpRowSetSuffix;

  // Open a rowset from disk.
  // If successful, sets *rowset to the newly open rowset
  static Status Open(Env *env,
                     const Schema &schema,
                     const string &rowset_dir,
                     shared_ptr<RowSet> *rowset);

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
  // RowSetInterface implementation
  ////////////////////////////////////////////////////////////

  ////////////////////
  // Updates
  ////////////////////
  Status UpdateRow(txid_t txid,
                   const void *key,
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

  static string GetColumnPath(const string &dir, int col_idx);
  static string GetDeltaPath(const string &dir, int delta_idx);
  static string GetBloomPath(const string &dir);

private:
  FRIEND_TEST(TestRowSet, TestRowSetUpdate);
  FRIEND_TEST(TestRowSet, TestDMSFlush);
  FRIEND_TEST(TestCompaction, TestOneToOne);
  friend class Tablet;
  friend class CompactionInput;

  // TODO: should 'schema' be stored with the rowset? quite likely
  // so that we can support cheap alter table.
  RowSet(Env *env,
        const Schema &schema,
        const string &rowset_dir);

  Status Open();

  void set_delta_tracker(const shared_ptr<DeltaTracker> &dt) {
    delta_tracker_ = dt;
  }

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



// RowSet which is used during the middle of a flush or compaction.
// It consists of a set of one or more input rowsets, and a single
// output rowset. All mutations are duplicated to the appropriate input
// rowset as well as the output rowset. All reads are directed to the
// union of the input rowsets.
//
// See compaction.txt for a little more detail on how this is used.
class DuplicatingRowSet : public RowSetInterface, boost::noncopyable {
public:
  DuplicatingRowSet(const vector<shared_ptr<RowSetInterface> > &old_rowsets,
                   const shared_ptr<RowSetInterface> &new_rowset);


  Status UpdateRow(txid_t txid, const void *key, const RowChangeList &update);

  Status CheckRowPresent(const RowSetKeyProbe &key, bool *present) const;

  RowwiseIterator *NewRowIterator(const Schema &projection,
                                  const MvccSnapshot &snap) const;

  CompactionInput *NewCompactionInput(const MvccSnapshot &snap) const;

  Status CountRows(rowid_t *count) const;

  uint64_t EstimateOnDiskSize() const;

  string ToString() const;

  Status Delete();

  // A flush-in-progress rowset should never be selected for compaction.
  boost::mutex *compact_flush_lock() {
    return &always_locked_;
  }

  ~DuplicatingRowSet();

  const Schema &schema() const {
    return new_rowset_->schema();
  }

private:
  friend class Tablet;

  vector<shared_ptr<RowSetInterface> > old_rowsets_;
  shared_ptr<RowSetInterface> new_rowset_;

  boost::mutex always_locked_;
};


} // namespace tablet
} // namespace kudu

#endif
