// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_ROWSET_H
#define KUDU_TABLET_ROWSET_H

#include <string>
#include <vector>

#include "cfile/cfile_reader.h"
#include "cfile/cfile_util.h"
#include "common/iterator.h"
#include "common/rowid.h"
#include "common/schema.h"
#include "gutil/macros.h"
#include "server/metadata.h"
#include "tablet/mvcc.h"
#include "util/bloom_filter.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {

class RowChangeList;

namespace metadata {
class RowSetMetadata;
}

namespace tablet {

class CompactionInput;
class MvccSnapshot;
class RowSetKeyProbe;

class RowSet {
 public:
  // Check if a given row key is present in this rowset.
  // Sets *present and returns Status::OK, unless an error
  // occurs.
  //
  // If the row was once present in this rowset, but no longer present
  // due to a DELETE, then this should set *present = false, as if
  // it were never there.
  virtual Status CheckRowPresent(const RowSetKeyProbe &probe, bool *present) const = 0;

  // Update/delete a row in this rowset.
  //
  // If the row does not exist in this rowset, returns
  // Status::NotFound().
  virtual Status MutateRow(txid_t txid,
                           const RowSetKeyProbe &probe,
                           const RowChangeList &update) = 0;

  // Return a new RowIterator for this rowset, with the given projection.
  // The iterator will return rows/updates which were committed as of the time of
  // 'snap'.
  // The returned iterator is not Initted.
  virtual RowwiseIterator *NewRowIterator(const Schema &projection,
                                          const MvccSnapshot &snap) const = 0;

  // Create the input to be used for a compaction.
  virtual CompactionInput *NewCompactionInput(const MvccSnapshot &snap) const = 0;

  // Count the number of rows in this rowset.
  virtual Status CountRows(rowid_t *count) const = 0;

  // Return the bounds for this RowSet. 'min_encoded_key' and 'max_encoded_key'
  // are set to the first and last encoded keys for this RowSet. The storage
  // for these slices is part of the RowSet and only guaranteed to stay valid
  // until the RowSet is destroyed.
  //
  // In the case that the rowset is still mutable (eg MemRowSet), this may
  // return Status::NotImplemented.
  virtual Status GetBounds(Slice *min_encoded_key,
                           Slice *max_encoded_key) const = 0;

  // Return a displayable string for this rowset.
  virtual string ToString() const = 0;

  // Dump the full contents of this rowset, for debugging.
  // This is very verbose so only useful within unit tests.
  virtual Status DebugDump(vector<string> *lines = NULL) = 0;

  // Estimate the number of bytes on-disk
  virtual uint64_t EstimateOnDiskSize() const = 0;

  // Return the lock used for including this DiskRowSet in a compaction.
  // This prevents multiple compactions and flushes from trying to include
  // the same rowset.
  virtual boost::mutex *compact_flush_lock() = 0;

  // Return the schema for data in this rowset.
  virtual const Schema &schema() const = 0;

  // Returns the metadata associated with this rowset.
  virtual shared_ptr<metadata::RowSetMetadata> metadata() = 0;

  virtual ~RowSet() {}
};

// Used often enough, may as well typedef it.
typedef vector<shared_ptr<RowSet> > RowSetVector;

// Structure which caches an encoded and hashed key, suitable
// for probing against rowsets.
class RowSetKeyProbe {
 public:
  // schema: the schema containing the key
  // raw_key: a pointer to the key portion of a row in memory
  // to probe for.
  //
  // NOTE: raw_key is not copied and must be valid for the liftime
  // of this object.
  RowSetKeyProbe(const ConstContiguousRow& row_key)
      : row_key_(row_key) {
    cfile::EncodeKey(row_key, &encoded_key_);
    bloom_probe_ = BloomKeyProbe(Slice(encoded_key_));
  }

  const ConstContiguousRow& row_key() const { return row_key_; }

  // Pointer to the key which has been encoded to be contiguous
  // and lexicographically comparable
  const Slice encoded_key() const { return Slice(encoded_key_); }

  // Return the cached structure used to query bloom filters.
  const BloomKeyProbe &bloom_probe() const { return bloom_probe_; }

  const Schema &schema() const { return row_key_.schema(); }

  const cfile::CFileKeyProbe ToCFileKeyProbe() const {
    return cfile::CFileKeyProbe(row_key_.row_data(),
                                encoded_key_,
                                schema().num_key_columns());
  }

 private:
  const ConstContiguousRow& row_key_;
  faststring encoded_key_;
  BloomKeyProbe bloom_probe_;
};


// RowSet which is used during the middle of a flush or compaction.
// It consists of a set of one or more input rowsets, and a single
// output rowset. All mutations are duplicated to the appropriate input
// rowset as well as the output rowset. All reads are directed to the
// union of the input rowsets.
//
// See compaction.txt for a little more detail on how this is used.
class DuplicatingRowSet : public RowSet {
 public:
  DuplicatingRowSet(const vector<shared_ptr<RowSet> > &old_rowsets,
                   const shared_ptr<RowSet> &new_rowset);


  Status MutateRow(txid_t txid, const RowSetKeyProbe &probe, const RowChangeList &update);

  Status CheckRowPresent(const RowSetKeyProbe &probe, bool *present) const;

  RowwiseIterator *NewRowIterator(const Schema &projection,
                                  const MvccSnapshot &snap) const;

  CompactionInput *NewCompactionInput(const MvccSnapshot &snap) const;

  Status CountRows(rowid_t *count) const;

  virtual Status GetBounds(Slice *min_encoded_key,
                           Slice *max_encoded_key) const;

  uint64_t EstimateOnDiskSize() const;

  string ToString() const;

  virtual Status DebugDump(vector<string> *lines = NULL);

  shared_ptr<metadata::RowSetMetadata> metadata();

  // A flush-in-progress rowset should never be selected for compaction.
  boost::mutex *compact_flush_lock() {
    return &always_locked_;
  }

  ~DuplicatingRowSet();

  const Schema &schema() const {
    return schema_;
  }

 private:
  friend class Tablet;

  DISALLOW_COPY_AND_ASSIGN(DuplicatingRowSet);

  vector<shared_ptr<RowSet> > old_rowsets_;
  shared_ptr<RowSet> new_rowset_;

  const Schema &schema_;
  const Schema key_schema_;

  boost::mutex always_locked_;
};

} // namespace tablet
} // namespace kudu

#endif
