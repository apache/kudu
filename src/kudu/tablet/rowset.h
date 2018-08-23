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
#ifndef KUDU_TABLET_ROWSET_H
#define KUDU_TABLET_ROWSET_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/row.h"
#include "kudu/common/rowid.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/util/bloom_filter.h"
#include "kudu/util/status.h"
// IWYU pragma: no_include "kudu/util/monotime.h"

namespace kudu {

class MonoTime; // IWYU pragma: keep
class RowChangeList;
class RowwiseIterator;
class Schema;
class Slice;

namespace consensus {
class OpId;
}

namespace fs {
struct IOContext;
}

namespace tablet {

class CompactionInput;
class OperationResultPB;
class RowSetKeyProbe;
class RowSetMetadata;
struct ProbeStats;

// Encapsulates all options passed to row-based Iterators.
struct RowIteratorOptions {
  RowIteratorOptions();

  // The projection to use in the iteration.
  //
  // Defaults to nullptr.
  const Schema* projection;

  // Transactions not committed in this snapshot will be ignored in the iteration.
  //
  // Defaults to a snapshot that includes all transactions.
  MvccSnapshot snap_to_include;

  // Transactions committed in this snapshot will be ignored in the iteration.
  // This is stored in a boost::optional so that iterators can ignore it
  // entirely if it is unset (the common case).
  //
  // Defaults to none.
  boost::optional<MvccSnapshot> snap_to_exclude;

  // Whether iteration should be ordered by primary key. Only relevant to those
  // iterators that deal with primary key order.
  //
  // Defaults to UNORDERED.
  OrderMode order;

  // Context of IO.
  //
  // Defaults to nullptr.
  const fs::IOContext* io_context;

  // Whether iteration should include rows whose last mutation was a DELETE.
  //
  // Defaults to false.
  bool include_deleted_rows;
};

class RowSet {
 public:
  enum DeltaCompactionType {
    MAJOR_DELTA_COMPACTION,
    MINOR_DELTA_COMPACTION
  };

  // Check if a given row key is present in this rowset.
  // Sets *present and returns Status::OK, unless an error
  // occurs.
  //
  // If the row was once present in this rowset, but no longer present
  // due to a DELETE, then this should set *present = false, as if
  // it were never there.
  virtual Status CheckRowPresent(const RowSetKeyProbe &probe, const fs::IOContext* io_context,
                                 bool *present, ProbeStats* stats) const = 0;

  // Update/delete a row in this rowset.
  // The 'update_schema' is the client schema used to encode the 'update' RowChangeList.
  //
  // If the row does not exist in this rowset, returns
  // Status::NotFound().
  virtual Status MutateRow(Timestamp timestamp,
                           const RowSetKeyProbe &probe,
                           const RowChangeList &update,
                           const consensus::OpId& op_id,
                           const fs::IOContext* io_context,
                           ProbeStats* stats,
                           OperationResultPB* result) = 0;

  // Return a new RowIterator for this rowset, with the given options.
  //
  // Pointers in 'opts' must remain valid for the lifetime of the iterator.
  //
  // The iterator will return rows/updates which were committed as of the time of
  // the snapshot in 'opts'.
  //
  // The returned iterator is not Initted.
  virtual Status NewRowIterator(const RowIteratorOptions& opts,
                                gscoped_ptr<RowwiseIterator>* out) const = 0;

  // Create the input to be used for a compaction.
  //
  // The provided 'projection' is for the compaction output. Each row
  // will be projected into this Schema.
  virtual Status NewCompactionInput(const Schema* projection,
                                    const MvccSnapshot &snap,
                                    const fs::IOContext* io_context,
                                    gscoped_ptr<CompactionInput>* out) const = 0;

  // Count the number of rows in this rowset.
  virtual Status CountRows(const fs::IOContext* io_context, rowid_t *count) const = 0;

  // Return the bounds for this RowSet. 'min_encoded_key' and 'max_encoded_key'
  // are set to the first and last encoded keys for this RowSet.
  //
  // In the case that the rowset is still mutable (eg MemRowSet), this may
  // return Status::NotImplemented.
  virtual Status GetBounds(std::string* min_encoded_key,
                           std::string* max_encoded_key) const = 0;

  // Return a displayable string for this rowset.
  virtual std::string ToString() const = 0;

  // Dump the full contents of this rowset, for debugging.
  // This is very verbose so only useful within unit tests.
  virtual Status DebugDump(std::vector<std::string> *lines = NULL) = 0;

  // Return the size of this rowset on disk, in bytes.
  virtual uint64_t OnDiskSize() const = 0;

  // Return the size of this rowset's base data on disk, in bytes.
  // Excludes bloomfiles and the ad hoc index.
  virtual uint64_t OnDiskBaseDataSize() const = 0;

  // Return the size, in bytes, of this rowset's base data and REDO deltas.
  // Does not include bloomfiles, the ad hoc index, or UNDO deltas.
  virtual uint64_t OnDiskBaseDataSizeWithRedos() const = 0;

  // Return the lock used for including this DiskRowSet in a compaction.
  // This prevents multiple compactions and flushes from trying to include
  // the same rowset.
  virtual std::mutex *compact_flush_lock() = 0;

  // Returns the metadata associated with this rowset.
  virtual std::shared_ptr<RowSetMetadata> metadata() = 0;

  // Get the size of the delta's MemStore
  virtual size_t DeltaMemStoreSize() const = 0;

  virtual bool DeltaMemStoreEmpty() const = 0;

  // Get the minimum log index corresponding to unflushed data in this row set.
  virtual int64_t MinUnflushedLogIndex() const = 0;

  // Get the performance improvement that running a minor or major delta compaction would give.
  // The returned score ranges between 0 and 1 inclusively.
  virtual double DeltaStoresCompactionPerfImprovementScore(DeltaCompactionType type) const = 0;

  // Flush the DMS if there's one
  virtual Status FlushDeltas(const fs::IOContext* io_context) = 0;

  // Compact delta stores if more than one.
  virtual Status MinorCompactDeltaStores(const fs::IOContext* io_context) = 0;

  // Estimate the number of bytes in ancient undo delta stores. This may be an
  // overestimate. The argument 'ancient_history_mark' must be valid (it may
  // not be equal to Timestamp::kInvalidTimestamp).
  virtual Status EstimateBytesInPotentiallyAncientUndoDeltas(Timestamp ancient_history_mark,
                                                             int64_t* bytes) = 0;

  // Initialize undo delta blocks until the given 'deadline' is passed, or
  // until all undo delta blocks with a max timestamp older than
  // 'ancient_history_mark' have been initialized.
  //
  // Invoking this method may also improve the estimate given by
  // EstimateBytesInPotentiallyAncientUndoDeltas().
  //
  // If this method returns OK, it returns the number of blocks actually
  // initialized in the out-param 'delta_blocks_initialized' and the number of
  // bytes that can be freed from disk in 'bytes_in_ancient_undos'.
  //
  // If 'ancient_history_mark' is set to Timestamp::kInvalidTimestamp then the
  // 'max_timestamp' of the blocks being initialized is ignored and no
  // age-based short-circuiting takes place.
  // If 'deadline' is not Initialized() then no deadline is enforced.
  //
  // The out-parameters, 'delta_blocks_initialized' and 'bytes_in_ancient_undos',
  // may be passed in as nullptr.
  virtual Status InitUndoDeltas(Timestamp ancient_history_mark,
                                MonoTime deadline,
                                const fs::IOContext* io_context,
                                int64_t* delta_blocks_initialized,
                                int64_t* bytes_in_ancient_undos) = 0;

  // Delete all initialized undo delta blocks with a max timestamp earlier than
  // the specified 'ancient_history_mark'.
  //
  // Note: This method does not flush updates to the rowset metadata. If this
  // method returns OK, the caller is responsible for persisting changes to the
  // rowset metadata by explicity flushing it.
  //
  // Note also: Blocks are not actually deleted until the rowset metadata is
  // flushed, because that invokes tablet metadata flush, which iterates over
  // and deletes the blocks present in the metadata orphans list.
  //
  // If this method returns OK, it also returns the number of delta blocks
  // deleted and the number of bytes deleted in the out-params 'blocks_deleted'
  // and 'bytes_deleted', respectively.
  //
  // The out-parameters, 'blocks_deleted' and 'bytes_deleted', may be passed in
  // as nullptr.
  virtual Status DeleteAncientUndoDeltas(Timestamp ancient_history_mark,
                                         const fs::IOContext* io_context,
                                         int64_t* blocks_deleted,
                                         int64_t* bytes_deleted) = 0;

  virtual ~RowSet() {}

  // Return true if this RowSet is available for compaction, based on
  // the current state of the compact_flush_lock. This should only be
  // used under the Tablet's compaction selection lock, or else the
  // lock status may change at any point.
  virtual bool IsAvailableForCompaction() {
    // Try to obtain the lock. If we don't succeed, it means the rowset
    // was already locked for compaction by some other compactor thread,
    // or it is a RowSet type which can't be used as a compaction input.
    //
    // We can be sure that our check here will remain true until after
    // the compaction selection has finished because only one thread
    // makes compaction selection at a time on a given Tablet due to
    // Tablet::compact_select_lock_.
    std::unique_lock<std::mutex> try_lock(*compact_flush_lock(), std::try_to_lock);
    return try_lock.owns_lock() && !has_been_compacted();
  }

  // Checked while validating that a rowset is available for compaction.
  virtual bool has_been_compacted() const = 0;

  // Set after a compaction has completed to indicate that the rowset has been
  // removed from the rowset tree and is thus longer available for compaction.
  virtual void set_has_been_compacted() = 0;
};

// Used often enough, may as well typedef it.
typedef std::vector<std::shared_ptr<RowSet> > RowSetVector;
// Structure which caches an encoded and hashed key, suitable
// for probing against rowsets.
class RowSetKeyProbe {
 public:
  // row_key: a reference to the key portion of a row in memory
  // to probe for.
  //
  // NOTE: row_key is not copied and must be valid for the lifetime
  // of this object.
  explicit RowSetKeyProbe(ConstContiguousRow row_key)
      : row_key_(row_key) {
    encoded_key_ = EncodedKey::FromContiguousRow(row_key_);
    bloom_probe_ = BloomKeyProbe(encoded_key_slice());
  }

  // RowSetKeyProbes are usually allocated on the stack, which means that we
  // must copy it if we require it later (e.g. Table::Mutate()).
  //
  // Still, the ConstContiguousRow row_key_ remains a reference to the data
  // underlying the original RowsetKeyProbe and is not copied.
  explicit RowSetKeyProbe(const RowSetKeyProbe& probe)
  : row_key_(probe.row_key_) {
    encoded_key_ = EncodedKey::FromContiguousRow(row_key_);
    bloom_probe_ = BloomKeyProbe(encoded_key_slice());
  }

  const ConstContiguousRow& row_key() const { return row_key_; }

  // Pointer to the key which has been encoded to be contiguous
  // and lexicographically comparable
  const Slice &encoded_key_slice() const { return encoded_key_->encoded_key(); }

  // Return the cached structure used to query bloom filters.
  const BloomKeyProbe &bloom_probe() const { return bloom_probe_; }

  // The schema containing the key.
  const Schema* schema() const { return row_key_.schema(); }

  const EncodedKey &encoded_key() const {
    return *encoded_key_;
  }

 private:
  const ConstContiguousRow row_key_;
  gscoped_ptr<EncodedKey> encoded_key_;
  BloomKeyProbe bloom_probe_;
};

// Statistics collected during row operations, counting how many times
// various structures had to be consulted to perform the operation.
//
// These eventually propagate into tablet-scoped metrics, and when we
// have RPC tracing capability, we could also stringify them into the
// trace to understand why an RPC may have been slow.
struct ProbeStats {
  ProbeStats()
    : blooms_consulted(0),
      keys_consulted(0),
      deltas_consulted(0),
      mrs_consulted(0) {
  }

  // Incremented for each bloom filter consulted.
  int blooms_consulted;

  // Incremented for each key cfile consulted.
  int keys_consulted;

  // Incremented for each delta file consulted.
  int deltas_consulted;

  // Incremented for each MemRowSet consulted.
  int mrs_consulted;
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
  DuplicatingRowSet(RowSetVector old_rowsets, RowSetVector new_rowsets);

  virtual Status MutateRow(Timestamp timestamp,
                           const RowSetKeyProbe &probe,
                           const RowChangeList &update,
                           const consensus::OpId& op_id,
                           const fs::IOContext* io_context,
                           ProbeStats* stats,
                           OperationResultPB* result) OVERRIDE;

  Status CheckRowPresent(const RowSetKeyProbe &probe, const fs::IOContext* io_context,
                         bool *present, ProbeStats* stats) const OVERRIDE;

  virtual Status NewRowIterator(const RowIteratorOptions& opts,
                                gscoped_ptr<RowwiseIterator>* out) const OVERRIDE;

  virtual Status NewCompactionInput(const Schema* projection,
                                    const MvccSnapshot &snap,
                                    const fs::IOContext* io_context,
                                    gscoped_ptr<CompactionInput>* out) const OVERRIDE;

  Status CountRows(const fs::IOContext* io_context, rowid_t *count) const OVERRIDE;

  virtual Status GetBounds(std::string* min_encoded_key,
                           std::string* max_encoded_key) const OVERRIDE;

  // Return the total size on-disk of this rowset, in bytes.
  uint64_t OnDiskSize() const OVERRIDE;

  // Return the total size on-disk of this rowset's data (i.e. excludes metadata), in bytes.
  uint64_t OnDiskBaseDataSize() const OVERRIDE;

  // Return the size, in bytes, of this rowset's data, not including UNDOs.
  uint64_t OnDiskBaseDataSizeWithRedos() const OVERRIDE;

  std::string ToString() const OVERRIDE;

  virtual Status DebugDump(std::vector<std::string> *lines = NULL) OVERRIDE;

  std::shared_ptr<RowSetMetadata> metadata() OVERRIDE;

  // A flush-in-progress rowset should never be selected for compaction.
  std::mutex *compact_flush_lock() OVERRIDE {
    LOG(FATAL) << "Cannot be compacted";
    return NULL;
  }

  virtual bool IsAvailableForCompaction() OVERRIDE {
    return false;
  }

  virtual bool has_been_compacted() const OVERRIDE {
    return false;
  }

  virtual void set_has_been_compacted() OVERRIDE {
    LOG(FATAL) << "Cannot be compacted";
  }

  ~DuplicatingRowSet();

  size_t DeltaMemStoreSize() const OVERRIDE { return 0; }

  bool DeltaMemStoreEmpty() const OVERRIDE { return true; }

  double DeltaStoresCompactionPerfImprovementScore(DeltaCompactionType type) const OVERRIDE {
    return 0;
  }

  int64_t MinUnflushedLogIndex() const OVERRIDE { return -1; }

  Status FlushDeltas(const fs::IOContext* /*io_context*/) OVERRIDE {
    // It's important that DuplicatingRowSet does not FlushDeltas. This prevents
    // a bug where we might end up with out-of-order deltas. See the long
    // comment in Tablet::Flush(...)
    return Status::OK();
  }

  Status EstimateBytesInPotentiallyAncientUndoDeltas(Timestamp /*ancient_history_mark*/,
                                                     int64_t* bytes) OVERRIDE {
    DCHECK(bytes);
    *bytes = 0;
    return Status::OK();
  }

  Status InitUndoDeltas(Timestamp /*ancient_history_mark*/,
                        MonoTime /*deadline*/,
                        const fs::IOContext* /*io_context*/,
                        int64_t* delta_blocks_initialized,
                        int64_t* bytes_in_ancient_undos) OVERRIDE {
    if (delta_blocks_initialized) *delta_blocks_initialized = 0;
    if (bytes_in_ancient_undos) *bytes_in_ancient_undos = 0;
    return Status::OK();
  }

  Status DeleteAncientUndoDeltas(Timestamp /*ancient_history_mark*/,
                                 const fs::IOContext* /*io_context*/,
                                 int64_t* blocks_deleted, int64_t* bytes_deleted) OVERRIDE {
    if (blocks_deleted) *blocks_deleted = 0;
    if (bytes_deleted) *bytes_deleted = 0;
    return Status::OK();
  }

  Status MinorCompactDeltaStores(
      const fs::IOContext* /*io_context*/) OVERRIDE { return Status::OK(); }

 private:
  friend class Tablet;

  DISALLOW_COPY_AND_ASSIGN(DuplicatingRowSet);

  RowSetVector old_rowsets_;
  RowSetVector new_rowsets_;
};


} // namespace tablet
} // namespace kudu

#endif
