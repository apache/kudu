// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_LAYER_INTERFACES_H
#define KUDU_TABLET_LAYER_INTERFACES_H

#include <tr1/memory>

#include "common/iterator.h"
#include "common/row.h"
#include "common/rowid.h"
#include "common/row_changelist.h"
#include "util/bloom_filter.h"
#include "util/status.h"
#include "tablet/mvcc.h"

namespace kudu {

class ColumnBlock;

namespace tablet {

using std::tr1::shared_ptr;

class DeltaIteratorInterface;
class Mutation;

// Structure which caches an encoded and hashed key, suitable
// for probing against layers.
class LayerKeyProbe {
public:

  // schema: the schema containing the key
  // raw_key: a pointer to the key portion of a row in memory
  // to probe for.
  //
  // NOTE: raw_key is not copied and must be valid for the liftime
  // of this object.
  LayerKeyProbe(const Schema &schema, const void *raw_key) :
    raw_key_(raw_key) {

    Slice raw_slice(reinterpret_cast<const uint8_t *>(raw_key),
                    schema.key_byte_size());
    schema.EncodeComparableKey(raw_slice, &encoded_key_);
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

class LayerInterface {
public:
  // Check if a given row key is present in this layer.
  // Sets *present and returns Status::OK, unless an error
  // occurs.
  virtual Status CheckRowPresent(const LayerKeyProbe &probe, bool *present) const = 0;

  // Update a row in this layer.
  //
  // If the row does not exist in this layer, returns
  // Status::NotFound().
  virtual Status UpdateRow(txid_t txid,
                           const void *key,
                           const RowChangeList &update) = 0;

  // Return a new RowIterator for this layer, with the given projection.
  // The iterator will return rows/updates which were committed as of the time of
  // 'snap'.
  // The returned iterator is not Initted.
  virtual RowwiseIterator *NewRowIterator(const Schema &projection,
                                          const MvccSnapshot &snap) const = 0;

  // Count the number of rows in this layer.
  virtual Status CountRows(rowid_t *count) const = 0;

  // Return a displayable string for this layer.
  virtual string ToString() const = 0;

  // Delete the underlying storage for this layer.
  virtual Status Delete() = 0;

  // Estimate the number of bytes on-disk
  virtual uint64_t EstimateOnDiskSize() const = 0;

  // Return the lock used for including this Layer in a compaction.
  // This prevents multiple compactions and flushes from trying to include
  // the same layer.
  virtual boost::mutex *compact_flush_lock() = 0;

  virtual ~LayerInterface() {}
};

// Used often enough, may as well typedef it.
typedef vector<shared_ptr<LayerInterface> > LayerVector;

// Interface for the pieces of the system that track deltas/updates.
// This is implemented by DeltaMemStore and by DeltaTracker, which reads
// on-disk delta files.
class DeltaTrackerInterface {
public:

  // Create a DeltaIteratorInterface for the given projection.
  //
  // The projection corresponds to whatever scan is currently ongoing.
  // All RowBlocks passed to this DeltaIterator must have this same schema.
  //
  // 'snapshot' is the MVCC state which determines which transactions
  // should be considered committed (and thus applied by the iterator).
  virtual DeltaIteratorInterface *NewDeltaIterator(const Schema &projection_,
                                                   const MvccSnapshot &snapshot) = 0;

  virtual ~DeltaTrackerInterface() {}
};


// Iterator over deltas.
// For each layer, this iterator is constructed alongside the base data iterator,
// and used to apply any updates which haven't been yet compacted into the base
// (i.e. those edits in the DeltaMemStore or in delta files)
//
// Typically this is used as follows:
//
//   Open iterator, seek to particular point in file
//   RowBlock rowblock;
//   foreach RowBlock in base data {
//     clear row block
//     CHECK_OK(iter->PrepareBatch(rowblock.size()));
//     ... read column 0 from base data into row block ...
//     CHECK_OK(iter->ApplyUpdates(0, rowblock.column(0))
//     ... check predicates for column ...
//     ... read another column from base data...
//     CHECK_OK(iter->ApplyUpdates(1, rowblock.column(1)))
//     ...
//  }
class DeltaIteratorInterface {
public:
  // Initialize the iterator. This must be called once before any other
  // call.  
  virtual Status Init() = 0;

  // Seek to a particular ordinal position in the delta data. This cancels any prepared
  // block, and must be called at least once prior to PrepareToApply.
  virtual Status SeekToOrdinal(rowid_t idx) = 0;

  // Prepare to apply deltas to a block of rows. This takes a consistent snapshot
  // of all updates to the next 'nrows' rows, so that subsequent calls to
  // ApplyUpdates() will not cause any "tearing"/non-atomicity.
  //
  // Each time this is called, the iterator is advanced by the full length
  // of the previously prepared block.
  virtual Status PrepareBatch(size_t nrows) = 0;

  // Apply the snapshotted updates to one of the columns.
  // 'dst' must be the same length as was previously passed to PrepareToApply()
  virtual Status ApplyUpdates(size_t col_to_apply, ColumnBlock *dst) = 0;

  // Collect the mutations associated with each row in the current prepared batch.
  //
  // Each entry in the vector will be treated as a singly linked list of Mutation
  // objects. If there are no mutations for that row, the entry will be unmodified.
  // If there are mutations, they will be appended at the tail of the linked list
  // (i.e in ascending txid order)
  //
  // The Mutation objects will be allocated out of the provided Arena, which must be non-NULL.
  virtual Status CollectMutations(vector<Mutation *> *dst, Arena *arena) = 0;

  // Return a string representation suitable for debug printouts.
  virtual string ToString() const = 0;

  virtual ~DeltaIteratorInterface() {}
};



} // namespace tablet
} // namespace kudu

#endif
