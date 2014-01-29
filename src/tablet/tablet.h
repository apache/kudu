// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_H
#define KUDU_TABLET_TABLET_H

#include <string>
#include <vector>

#include "common/iterator.h"
#include "common/schema.h"
#include "common/predicate_encoder.h"
#include "gutil/atomicops.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "server/metadata.h"
#include "tablet/mvcc.h"
#include "tablet/lock_manager.h"
#include "tablet/rowset.h"
#include "util/locks.h"
#include "util/status.h"
#include "util/slice.h"

namespace kudu {

class MetricContext;
class RowChangeList;
class UnionIterator;

namespace consensus {
class Consensus;
}

namespace log {
class OpIdAnchorRegistry;
}

namespace server {
class Clock;
}

namespace tablet {

using std::string;
using std::tr1::shared_ptr;

class AlterSchemaTransactionContext;
class CompactionPolicy;
class MemRowSet;
class MvccSnapshot;
class PreparedRowWrite;
class RowSetsInCompaction;
class RowSetTree;
struct TabletMetrics;
class WriteTransactionContext;

class Tablet {
 public:
  class CompactionFaultHooks;
  class FlushCompactCommonHooks;
  class FlushFaultHooks;
  class Iterator;

  // Create a new tablet.
  //
  // If 'parent_metrics_context' is non-NULL, then this tablet will store
  // metrics in a sub-context of this context. Otherwise, no metrics are collected.
  Tablet(gscoped_ptr<metadata::TabletMetadata> metadata,
         const scoped_refptr<server::Clock>& clock,
         const MetricContext* parent_metric_context,
         log::OpIdAnchorRegistry* opid_anchor_registry);

  ~Tablet();

  // Open the tablet.
  Status Open();

  // TODO update tests so that we can remove Insert() and Mutate()
  // and use only InsertUnlocked() and MutateUnlocked().

  // Creates a PreparedRowWrite with write_type() INSERT, acquires the row lock
  // for the row and creates a probe for later use. 'row_write' is set to the
  // PreparedRowWrite if this method returns OK.
  //
  // TODO when we get to remove the locked versions of Insert/Mutate we
  // can make the PreparedRowWrite own the row and can revert to passing just
  // the raw row data, but right now we need to pass the built ConstContinuousRow
  // as there are cases where row is passed as a reference (old API).
  Status CreatePreparedInsert(const WriteTransactionContext* tx_ctx,
                              const ConstContiguousRow* row,
                              gscoped_ptr<PreparedRowWrite>* row_write);

  // Insert a new row into the tablet.
  //
  // The provided 'data' slice should have length equivalent to this
  // tablet's Schema.byte_size().
  //
  // After insert, the row and any referred-to memory (eg for strings)
  // have been copied into internal memory, and thus the provided memory
  // buffer may safely be re-used or freed.
  //
  // Returns Status::AlreadyPresent() if an entry with the same key is already
  // present in the tablet.
  // Returns Status::OK unless allocation fails.
  Status InsertForTesting(WriteTransactionContext *tx_ctx, const ConstContiguousRow& row);

  // A version of Insert that does not acquire locks and instead assumes that
  // they were already acquired. Requires that handles for the relevant locks
  // and Mvcc transaction are present in the transaction context.
  Status InsertUnlocked(WriteTransactionContext *tx_ctx,
                        const PreparedRowWrite* insert);

  // Creates a PreparedRowWrite with write_type() MUTATE, acquires the row lock
  // for the row and creates a probe for later use. 'row_write' is set to the
  // PreparedRowWrite if this method returns OK.
  //
  // TODO when we get to remove the locked versions of Insert/Mutate we
  // can make the PreparedRowWrite own the row and can revert to passing just
  // the raw row data, but right now we need to pass the built ConstContinuousRow
  // as there are cases where row is passed as a reference (old API).
  Status CreatePreparedMutate(const WriteTransactionContext* tx_ctx,
                              const ConstContiguousRow* row_key,
                              const RowChangeList& changelist,
                              gscoped_ptr<PreparedRowWrite>* row_write);

  // Update a row in this tablet.
  // The specified schema is the full user schema necessary to decode
  // the update RowChangeList.
  //
  // If the row does not exist in this tablet, returns
  // Status::NotFound().
  Status MutateRowForTesting(WriteTransactionContext *tx_ctx,
                             const ConstContiguousRow& row_key,
                             const Schema& update_schema,
                             const RowChangeList& update);

  // A version of MutateRow that does not acquire locks and instead assumes
  // they were already acquired. Requires that handles for the relevant locks
  // and Mvcc transaction are present in the transaction context.
  Status MutateRowUnlocked(WriteTransactionContext *tx_ctx,
                           const PreparedRowWrite* mutate);

  // Create a new row iterator which yields the rows as of the current MVCC
  // state of this tablet.
  // The returned iterator is not initialized.
  Status NewRowIterator(const Schema &projection,
                        gscoped_ptr<RowwiseIterator> *iter) const;

  // Create a new row iterator for some historical snapshot.
  Status NewRowIterator(const Schema &projection,
                        const MvccSnapshot &snap,
                        gscoped_ptr<RowwiseIterator> *iter) const;

  // TODO: Document me.
  // Apparently, this method only flushes the MemRowSet.
  // To flush the DeltaMemStores, call something else.
  Status Flush();

  // Prepares the transaction context for the alter schema operation.
  // An error will be returned if the specified schema is invalid (e.g.
  // key mismatch, or missing IDs)
  // The "tablet lock" (component_lock_) will be taken in exclusive mode to
  // prevent concurrent operations (e.g. Insert, MutateRow, ...)
  Status CreatePreparedAlterSchema(AlterSchemaTransactionContext *tx_ctx,
                                   const Schema* schema);

  // Apply the Schema of the specified transaction.
  // This operation will trigger a flush on the current MemRowSet and on all the DeltaMemStores.
  //
  // The component lock acquired in exclusive mode by CreatePreparedAlterSchema()
  // will be released when the schema is replaced in every RowSet.
  Status AlterSchema(AlterSchemaTransactionContext* tx_ctx);

  // Flags to change the behavior of compaction.
  enum CompactFlag {
    COMPACT_NO_FLAGS = 0,

    // Force the compaction to include all rowsets, regardless of the
    // configured compaction policy. This is currently only used in
    // tests.
    FORCE_COMPACT_ALL = 1 << 0
  };
  typedef int CompactFlags;

  Status Compact(CompactFlags flags);

  size_t MemRowSetSize() const;

  // Estimate the total on-disk size of this tablet, in bytes.
  size_t EstimateOnDiskSize() const;

  // Get the total size of all the DMS
  size_t DeltaMemStoresSize() const;

  // Flush only the biggest DMS
  Status FlushBiggestDMS();

  // Finds the RowSet which has the most separate delta files and
  // issues a minor delta compaction.
  Status MinorCompactWorstDeltas();

  // Return the current number of rowsets in the tablet.
  size_t num_rowsets() const;

  // Attempt to count the total number of rows in the tablet.
  // This is not super-efficient since it must iterate over the
  // memrowset in the current implementation.
  Status CountRows(uint64_t *count) const;


  // Verbosely dump this entire tablet to the logs. This is only
  // really useful when debugging unit tests failures where the tablet
  // has a very small number of rows.
  Status DebugDump(vector<string> *lines = NULL);

  // Return the current schema of the metadata. Note that this returns
  // a copy, so should not be used in a performance-critical section.
  Schema schema() const { return schema_; }

  // Return a pointer to the current Schema. Callers must only use this
  // while protected by the component lock to prevent concurrent schema change,
  // which could invalidate the Schema object.
  const Schema* schema_ptr() const {
    DCHECK(component_lock_.is_locked());
    return &schema_;
  }

  // Returns a reference to the key projection of the tablet schema.
  // The schema keys are immutable.
  const Schema& key_schema() const { return key_schema_; }

  // Return the MVCC manager for this tablet.
  MvccManager* mvcc_manager() { return &mvcc_; }

  // Return the Lock Manager for this tablet
  LockManager* lock_manager() { return &lock_manager_; }

  // Returns the component lock for this tablet
  rw_semaphore* component_lock() { return &component_lock_; }

  const metadata::TabletMetadata *metadata() const { return metadata_.get(); }
  metadata::TabletMetadata *metadata() { return metadata_.get(); }

  void SetConsensus(consensus::Consensus* consensus);

  void SetCompactionHooksForTests(const shared_ptr<CompactionFaultHooks> &hooks);
  void SetFlushHooksForTests(const shared_ptr<FlushFaultHooks> &hooks);
  void SetFlushCompactCommonHooksForTests(const shared_ptr<FlushCompactCommonHooks> &hooks);

  int32_t CurrentMrsIdForTests() const;

  int32_t TotalMissedDeltasMutationsForTests() const { return total_missed_deltas_mutations_; }

  // Runs a major delta major compaction on columns at specified
  // indexes in 'input_rowset'; 'column_indexes' must be sorted.
  // NOTE: RowSet must presently be a DiskRowSet. (Perhaps the API should be
  // a shared_ptr API for now?)
  //
  // TODO: Handle MVCC to support MemRowSet and handle deltas in DeltaMemStore
  Status DoMajorDeltaCompaction(const metadata::ColumnIndexes& column_indexes,
                                shared_ptr<RowSet> input_rowset);

  // Method used by tests to retrieve all rowsets of this table. This
  // will be removed once code for selecting the appropriate RowSet is
  // finished and delta files is finished is part of Tablet class.
  void GetRowSetsForTests(vector<shared_ptr<RowSet> >* out);

  const std::string& tablet_id() const { return metadata_->oid(); }

  // Return the metrics for this tablet.
  // May be NULL in unit tests, etc.
  TabletMetrics* metrics() { return metrics_.get(); }

  // Return handle to the metric context of this tablet. For unit tests.
  const MetricContext* GetMetricContextForTests() const { return metric_context_.get(); }

 private:
  friend class Iterator;

  // Capture a set of iterators which, together, reflect all of the data in the tablet.
  //
  // These iterators are not true snapshot iterators, but they are safe against
  // concurrent modification. They will include all data that was present at the time
  // of creation, and potentially newer data.
  //
  // The returned iterators are not Init()ed.
  // 'projection' must remain valid and unchanged for the lifetime of the returned iterators.
  Status CaptureConsistentIterators(const Schema *projection,
                                    const MvccSnapshot &snap,
                                    const ScanSpec *spec,
                                    vector<shared_ptr<RowwiseIterator> > *iters) const;

  Status PickRowSetsToCompact(RowSetsInCompaction *picked,
                              CompactFlags flags) const;

  Status DoCompactionOrFlush(const Schema& schema,
                             const RowSetsInCompaction &input,
                             int64_t mrs_being_flushed);

  Status FlushMetadata(const RowSetVector& to_remove,
                       const metadata::RowSetMetadataVector& to_add,
                       int64_t mrs_being_flushed);

  // Swap out a set of rowsets, atomically replacing them with the new rowset
  // under the lock.
  void AtomicSwapRowSets(const RowSetVector &old_rowsets,
                         const RowSetVector &new_rowsets,
                         MvccSnapshot *snap_under_lock);

  // Same as the above, but without taking the lock. This should only be used
  // in cases where the lock is already held.
  void AtomicSwapRowSetsUnlocked(const RowSetVector &old_rowsets,
                                 const RowSetVector &new_rowsets,
                                 MvccSnapshot *snap_under_lock);

  // Delete the underlying storage for the input layers in a compaction.
  Status DeleteCompactionInputs(const RowSetsInCompaction &input);

  // Create a new MemRowSet with the specified 'schema' and replace the current one.
  // The 'old_ms' pointer will be set to the current MemRowSet set before the replacement.
  // If the MemRowSet is not empty it will be added to the 'compaction' input
  // and the MemRowSet compaction lock will be taken to prevent the inclusion
  // in any concurrent compactions.
  Status ReplaceMemRowSetUnlocked(const Schema& schema,
                                  RowSetsInCompaction *compaction,
                                  shared_ptr<MemRowSet> *old_ms);

  // TODO: Document me.
  Status Flush(const RowSetsInCompaction& input,
               const shared_ptr<MemRowSet>& old_ms,
               const Schema& schema);

  BloomFilterSizing bloom_sizing() const;

  // Convert the specified read client schema (without IDs) to a server schema (with IDs)
  // This method is used by NewRowIterator().
  Status GetMappedReadProjection(const Schema& projection,
                                 Schema *mapped_projection) const;

  Schema schema_;
  Schema key_schema_;
  gscoped_ptr<metadata::TabletMetadata> metadata_;
  shared_ptr<MemRowSet> memrowset_;
  shared_ptr<RowSetTree> rowsets_;

  gscoped_ptr<MetricContext> metric_context_;
  gscoped_ptr<TabletMetrics> metrics_;

  consensus::Consensus* consensus_;
  log::OpIdAnchorRegistry* opid_anchor_registry_;

  Atomic32 next_mrs_id_;

  // A pointer to the server's clock.
  scoped_refptr<server::Clock> clock_;

  MvccManager mvcc_;
  LockManager lock_manager_;

  Atomic32 total_missed_deltas_mutations_;

  gscoped_ptr<CompactionPolicy> compaction_policy_;

  // Lock protecting write access to the components of the tablet (memrowset and rowsets).
  // Shared mode:
  // - Inserters, updaters take this in shared mode during their mutation.
  // - Readers take this in shared mode while capturing their iterators.
  // Exclusive mode:
  // - Flushers take this lock in order to lock out concurrent updates when swapping in
  //   a new memrowset.
  //
  // NOTE: callers should avoid taking this lock for a long time, even in shared mode.
  // This is because the lock has some concept of fairness -- if, while a long reader
  // is active, a writer comes along, then all future short readers will be blocked.
  //
  // TODO: this could probably done more efficiently with a single atomic swap of a list
  // and an RCU-style quiesce phase, but not worth it for now.
  mutable rw_semaphore component_lock_;

  // Lock protecting the selection of rowsets for compaction.
  // Only one thread may run the compaction selection algorithm at a time
  // so that they don't both try to select the same rowset. Before taking
  // this lock, you should also hold component_lock_ in read mode so that
  // no other thread could perform a swap underneath.
  mutable boost::mutex compact_select_lock_;

  // Lock protecting the schema access
  // Shared mode:
  //  - DoCompactionOrFlush takes the lock to access the schema to ensure that
  //    the newly added rowsets have the latest one.
  //  - schema returns a copy of the current schema and takes the lock
  // Exclusive mode:
  //  - AlterSchema take this lock to prevent reading the half set schema
  mutable percpu_rwlock schema_lock_;

  bool open_;

  // Fault hooks. In production code, these will always be NULL.
  shared_ptr<CompactionFaultHooks> compaction_hooks_;
  shared_ptr<FlushFaultHooks> flush_hooks_;
  shared_ptr<FlushCompactCommonHooks> common_hooks_;

  DISALLOW_COPY_AND_ASSIGN(Tablet);
};


// Hooks used in test code to inject faults or other code into interesting
// parts of the compaction code.
class Tablet::CompactionFaultHooks {
 public:
  virtual Status PostSelectIterators() { return Status::OK(); }
  virtual ~CompactionFaultHooks() {}
};

class Tablet::FlushCompactCommonHooks {
 public:
  virtual Status PostTakeMvccSnapshot() { return Status::OK(); }
  virtual Status PostWriteSnapshot() { return Status::OK(); }
  virtual Status PostSwapInDuplicatingRowSet() { return Status::OK(); }
  virtual Status PostReupdateMissedDeltas() { return Status::OK(); }
  virtual Status PostSwapNewRowSet() { return Status::OK(); }
  virtual ~FlushCompactCommonHooks() {}
};

// Hooks used in test code to inject faults or other code into interesting
// parts of the Flush() code.
class Tablet::FlushFaultHooks {
 public:
  virtual Status PostSwapNewMemRowSet() { return Status::OK(); }
  virtual ~FlushFaultHooks() {}
};

class Tablet::Iterator : public RowwiseIterator {
 public:
  virtual ~Iterator();

  virtual Status Init(ScanSpec *spec);

  virtual Status PrepareBatch(size_t *nrows);

  virtual bool HasNext() const;

  virtual Status MaterializeBlock(RowBlock *dst);

  virtual Status FinishBatch();

  string ToString() const;

  const Schema &schema() const {
    return projection_;
  }

 private:
  friend class Tablet;

  DISALLOW_COPY_AND_ASSIGN(Iterator);

  Iterator(const Tablet *tablet,
           const Schema &projection,
           const MvccSnapshot &snap);

  const Tablet *tablet_;
  Schema projection_;
  const MvccSnapshot snap_;
  gscoped_ptr<UnionIterator> iter_;
  RangePredicateEncoder encoder_;
};

} // namespace tablet
} // namespace kudu

#endif
