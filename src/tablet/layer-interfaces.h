// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_LAYER_INTERFACES_H
#define KUDU_TABLET_LAYER_INTERFACES_H

#include <tr1/memory>

#include "common/iterator.h"
#include "common/row.h"
#include "tablet/rowdelta.h"
#include "util/bloom_filter.h"
#include "util/status.h"

namespace kudu {

class ColumnBlock;

namespace tablet {

using std::tr1::shared_ptr;

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
  virtual Status UpdateRow(const void *key,
                           const RowDelta &update) = 0;

  // Return a new RowIterator for this layer, with the given projection.
  // NB: the returned iterator is not yet Initted.
  // TODO: make this consistent with above.
  virtual RowIteratorInterface *NewRowIterator(const Schema &projection) const = 0;

  // Count the number of rows in this layer.
  virtual Status CountRows(size_t *count) const = 0;

  // Return a displayable string for this layer.
  virtual string ToString() const = 0;

  // Delete the underlying storage for this layer.
  virtual Status Delete() = 0;

  // Estimate the number of bytes on-disk
  virtual size_t EstimateOnDiskSize() const = 0;

  // Return the lock used for including this Layer in a compaction.
  // This prevents multiple compactions and flushes from trying to include
  // the same layer.
  virtual boost::mutex *compact_flush_lock() = 0;

  virtual ~LayerInterface() {}
};

// Used often enough, may as well typedef it.
typedef vector<shared_ptr<LayerInterface> > LayerVector;


class DeltaTrackerInterface {
public:

  // Apply updates for a given column to a batch of rows.
  // TODO: would be better to take in a projection schema here, maybe?
  // Or provide functions for each (column-wise scanning vs early materialization?)
  //
  // The target buffer 'dst' is assumed to have a length at least
  // as large as row_stride * nrows.
  virtual Status ApplyUpdates(size_t col_idx, uint32_t start_row,
                              ColumnBlock *dst) const = 0;

  virtual ~DeltaTrackerInterface() {}
};


} // namespace tablet
} // namespace kudu

#endif
