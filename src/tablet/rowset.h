// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_ROWSET_H
#define KUDU_TABLET_ROWSET_H

#include "common/iterator.h"
#include "common/rowid.h"
#include "common/schema.h"
#include "util/bloom_filter.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {

class RowChangeList;

namespace tablet {

class CompactionInput;
class MvccSnapshot;
class RowSetKeyProbe;

class RowSet {
public:
  // Check if a given row key is present in this rowset.
  // Sets *present and returns Status::OK, unless an error
  // occurs.
  virtual Status CheckRowPresent(const RowSetKeyProbe &probe, bool *present) const = 0;

  // Update a row in this rowset.
  //
  // If the row does not exist in this rowset, returns
  // Status::NotFound().
  virtual Status UpdateRow(txid_t txid,
                           const void *key,
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

  // Return a displayable string for this rowset.
  virtual string ToString() const = 0;

  // Delete the underlying storage for this rowset.
  virtual Status Delete() = 0;

  // Estimate the number of bytes on-disk
  virtual uint64_t EstimateOnDiskSize() const = 0;

  // Return the lock used for including this DiskRowSet in a compaction.
  // This prevents multiple compactions and flushes from trying to include
  // the same rowset.
  virtual boost::mutex *compact_flush_lock() = 0;

  // Return the schema for data in this rowset.
  virtual const Schema &schema() const = 0;

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
  RowSetKeyProbe(const Schema &schema, const void *raw_key)
    : raw_key_(raw_key)
  {
    ConstContiguousRow row_slice(schema, raw_key);
    schema.EncodeComparableKey(row_slice, &encoded_key_);
    bloom_probe_ = BloomKeyProbe(Slice(encoded_key_));
  }

  // Pointer to the raw pointer for the key in memory.
  const void *raw_key() const { return raw_key_; }

  // Pointer to the key which has been encoded to be contiguous
  // and lexicographically comparable
  const Slice encoded_key() const { return Slice(encoded_key_); }

  // Return the cached structure used to query bloom filters.
  const BloomKeyProbe &bloom_probe() const { return bloom_probe_; }

private:
  const void *raw_key_;
  faststring encoded_key_;
  BloomKeyProbe bloom_probe_;
};


} // namespace tablet
} // namespace kudu

#endif
