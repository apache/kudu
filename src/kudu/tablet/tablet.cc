// Copyright (c) 2012, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include <algorithm>
#include <boost/assign/list_of.hpp>
#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <iterator>
#include <limits>
#include <ostream>
#include <tr1/memory>
#include <tr1/unordered_set>
#include <vector>

#include "kudu/cfile/cfile_writer.h"
#include "kudu/common/iterator.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/tablet/compaction.h"
#include "kudu/tablet/compaction_policy.h"
#include "kudu/tablet/delta_compaction.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/maintenance_manager.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/rowset_info.h"
#include "kudu/tablet/rowset_tree.h"
#include "kudu/tablet/row_op.h"
#include "kudu/tablet/svg_dump.h"
#include "kudu/tablet/transactions/alter_schema_transaction.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/util/bloom_filter.h"
#include "kudu/util/env.h"
#include "kudu/util/locks.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/trace.h"
#include "kudu/util/url-coding.h"
#include "kudu/util/stopwatch.h"

DEFINE_bool(tablet_do_dup_key_checks, true,
            "Whether to check primary keys for duplicate on insertion. "
            "Use at your own risk!");

DEFINE_int32(tablet_compaction_budget_mb, 128,
             "Budget for a single compaction, if the 'budget' compaction "
             "algorithm is selected");

DEFINE_int32(flush_threshold_mb, 64,
             "Size at which MemRowSet flushes are triggered. "
             "A MRS can still flush below this threshold if it if hasn't flushed in a while");

METRIC_DEFINE_gauge_uint64(memrowset_size, kudu::MetricUnit::kBytes,
                           "Size of this tablet's memrowset");

namespace kudu {
namespace tablet {

using kudu::MaintenanceManager;
using consensus::OpId;
using consensus::MaximumOpId;
using log::LogAnchorRegistry;
using std::string;
using std::set;
using std::vector;
using std::tr1::shared_ptr;
using std::tr1::unordered_set;
using strings::Substitute;
using base::subtle::Barrier_AtomicIncrement;

static const int64_t kNoMrsFlushed = -1;
static const char* const kTmpSuffix = ".tmp";

static CompactionPolicy *CreateCompactionPolicy() {
  return new BudgetedCompactionPolicy(FLAGS_tablet_compaction_budget_mb);
}

TabletComponents::TabletComponents(const shared_ptr<MemRowSet>& mrs,
                                   const shared_ptr<RowSetTree>& rs_tree)
  : memrowset(mrs),
    rowsets(rs_tree) {
}

Tablet::Tablet(const scoped_refptr<TabletMetadata>& metadata,
               const scoped_refptr<server::Clock>& clock,
               const MetricContext* parent_metric_context,
               LogAnchorRegistry* log_anchor_registry)
  : schema_(new Schema(metadata->schema())),
    key_schema_(schema_->CreateKeyProjection()),
    metadata_(metadata),
    log_anchor_registry_(log_anchor_registry),
    mem_tracker_(MemTracker::CreateTracker(-1,
                                           Substitute("tablet-$0", tablet_id()),
                                           NULL)),
    rowsets_(new RowSetTree()),
    next_mrs_id_(0),
    clock_(clock),
    mvcc_(clock),
    rowsets_flush_sem_(1),
    open_(false) {
  CHECK(schema_->has_column_ids());
  compaction_policy_.reset(CreateCompactionPolicy());

  if (parent_metric_context) {
    metric_context_.reset(new MetricContext(*parent_metric_context,
                                            Substitute("tablet.tablet-$0", tablet_id())));
    metrics_.reset(new TabletMetrics(*metric_context_));
    METRIC_memrowset_size.InstantiateFunctionGauge(
        *metric_context_, boost::bind(&Tablet::MemRowSetSize, this));
  }
}

Tablet::~Tablet() {
  // We need to clear the maintenance ops manually here, so that the operation
  // callbacks can't trigger while we're in the process of tearing down the rest
  // of the tablet fields.
  UnregisterMaintenanceOps();
}

Status Tablet::Open() {
  boost::lock_guard<rw_spinlock> lock(component_lock_);
  CHECK(!open_) << "already open";
  CHECK(schema_->has_column_ids());
  // TODO: track a state_ variable, ensure tablet is open, etc.

  next_mrs_id_ = metadata_->last_durable_mrs_id() + 1;

  RowSetVector rowsets_opened;

  // open the tablet row-sets
  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& rowset_meta, metadata_->rowsets()) {
    shared_ptr<DiskRowSet> rowset;
    Status s = DiskRowSet::Open(rowset_meta, log_anchor_registry_, &rowset, mem_tracker_);
    if (!s.ok()) {
      LOG(ERROR) << "Failed to open rowset " << rowset_meta->ToString() << ": "
                 << s.ToString();
      return s;
    }

    rowsets_opened.push_back(rowset);
  }

  shared_ptr<RowSetTree> new_rowset_tree(new RowSetTree());
  CHECK_OK(new_rowset_tree->Reset(rowsets_opened));
  // now that the current state is loaded, create the new MemRowSet with the next id
  shared_ptr<MemRowSet> new_mrs(new MemRowSet(next_mrs_id_++, *schema_.get(),
                                              log_anchor_registry_,
                                              mem_tracker_));
  components_ = new TabletComponents(new_mrs, new_rowset_tree);

  open_ = true;
  return Status::OK();
}

Status Tablet::GetMappedReadProjection(const Schema& projection,
                                       Schema *mapped_projection) const {
  shared_ptr<Schema> cur_schema(schema());
  return cur_schema->GetMappedReadProjection(projection, mapped_projection);
}

BloomFilterSizing Tablet::bloom_sizing() const {
  // TODO: make this configurable
  return BloomFilterSizing::BySizeAndFPRate(64*1024, 0.01f);
}

Status Tablet::NewRowIterator(const Schema &projection,
                              gscoped_ptr<RowwiseIterator> *iter) const {
  // Yield current rows.
  MvccSnapshot snap(mvcc_);
  return NewRowIterator(projection, snap, iter);
}


Status Tablet::NewRowIterator(const Schema &projection,
                              const MvccSnapshot &snap,
                              gscoped_ptr<RowwiseIterator> *iter) const {
  if (metrics_) {
    metrics_->scans_started->Increment();
  }
  VLOG(2) << "Created new Iterator under snap: " << snap.ToString();
  iter->reset(new Iterator(this, projection, snap));
  return Status::OK();
}

Status Tablet::DecodeWriteOperations(const Schema* client_schema,
                                     WriteTransactionState* tx_state) {
  DCHECK_EQ(tx_state->row_ops().size(), 0);

  // Acquire the schema lock in shared mode, so that the schema doesn't
  // change while this transaction is in-flight.
  tx_state->AcquireSchemaLock(&schema_lock_);

  // The Schema needs to be held constant while any transactions are between
  // PREPARE and APPLY stages
  TRACE("PREPARE: Decoding operations");
  vector<DecodedRowOperation> ops;

  // Decode the ops
  RowOperationsPBDecoder dec(&tx_state->request()->row_operations(),
                             client_schema,
                             schema_.get(),
                             tx_state->arena());
  RETURN_NOT_OK(dec.DecodeOperations(&ops));

  // Create RowOp objects for each
  vector<RowOp*> row_ops;
  ops.reserve(ops.size());
  BOOST_FOREACH(const DecodedRowOperation& op, ops) {
    row_ops.push_back(new RowOp(op));
  }

  tx_state->mutable_row_ops()->swap(row_ops);
  tx_state->set_schema_at_decode_time(schema_.get());

  return Status::OK();
}

Status Tablet::AcquireRowLocks(WriteTransactionState* tx_state) {
  TRACE("PREPARE: Acquiring locks for $0 operations", tx_state->row_ops().size());

  BOOST_FOREACH(RowOp* op, *tx_state->mutable_row_ops()) {
    RETURN_NOT_OK(AcquireLockForOp(tx_state, op));
  }
  TRACE("PREPARE: locks acquired");
  return Status::OK();
}

Status Tablet::CheckRowInTablet(const tablet::RowSetKeyProbe& probe) const {
  if (!probe.encoded_key().InRange(Slice(metadata_->start_key()),
                                   Slice(metadata_->end_key()))) {
    return Status::NotFound(
        Substitute("Row not within tablet range. Tablet start key: '$0', end key: '$1'."
                   "Probe key: '$2'",
                   schema_->DebugEncodedRowKey(metadata_->start_key(), Schema::START_KEY),
                   schema_->DebugEncodedRowKey(metadata_->end_key(), Schema::END_KEY),
                   schema_->DebugEncodedRowKey(probe.encoded_key().encoded_key(),
                                               Schema::START_KEY)));
  }
  return Status::OK();
}

Status Tablet::AcquireLockForOp(WriteTransactionState* tx_state, RowOp* op) {
  ConstContiguousRow row_key(&key_schema_, op->decoded_op.row_data);
  op->key_probe.reset(new tablet::RowSetKeyProbe(row_key));
  RETURN_NOT_OK(CheckRowInTablet(*op->key_probe));

  ScopedRowLock row_lock(&lock_manager_,
                         tx_state,
                         op->key_probe->encoded_key_slice(),
                         LockManager::LOCK_EXCLUSIVE);
  op->row_lock = row_lock.Pass();
  return Status::OK();
}

void Tablet::StartTransaction(WriteTransactionState* tx_state) {
  boost::shared_lock<rw_spinlock> lock(component_lock_);

  gscoped_ptr<ScopedTransaction> mvcc_tx;
  if (tx_state->external_consistency_mode() == COMMIT_WAIT) {
    mvcc_tx.reset(new ScopedTransaction(&mvcc_, ScopedTransaction::NOW_LATEST));
  } else {
    mvcc_tx.reset(new ScopedTransaction(&mvcc_, ScopedTransaction::NOW));
  }
  tx_state->set_mvcc_tx(mvcc_tx.Pass());
  tx_state->set_tablet_components(components_);
}

Status Tablet::InsertUnlocked(WriteTransactionState *tx_state,
                              RowOp* insert) {
  const TabletComponents* comps = DCHECK_NOTNULL(tx_state->tablet_components());

  CHECK(open_) << "must Open() first!";
  // make sure that the WriteTransactionState has the component lock and that
  // there the RowOp has the row lock.
  DCHECK(insert->has_row_lock()) << "RowOp must hold the row lock.";
  DCHECK_EQ(tx_state->schema_at_decode_time(), schema_.get()) << "Raced against schema change";
  DCHECK(tx_state->op_id().IsInitialized()) << "TransactionState OpId needed for anchoring";

  ProbeStats stats;

  // Submit the stats before returning from this function
  ProbeStatsSubmitter submitter(stats, metrics_.get());

  // First, ensure that it is a unique key by checking all the open RowSets.
  if (FLAGS_tablet_do_dup_key_checks) {
    vector<RowSet *> to_check;
    comps->rowsets->FindRowSetsWithKeyInRange(insert->key_probe->encoded_key_slice(),
                                              &to_check);

    BOOST_FOREACH(const RowSet *rowset, to_check) {
      bool present = false;
      RETURN_NOT_OK(rowset->CheckRowPresent(*insert->key_probe, &present, &stats));
      if (PREDICT_FALSE(present)) {
        Status s = Status::AlreadyPresent("key already present");
        if (metrics_) {
          metrics_->insertions_failed_dup_key->Increment();
        }
        insert->SetFailed(s);
        return s;
      }
    }
  }

  Timestamp ts = tx_state->timestamp();
  ConstContiguousRow row(schema_.get(), insert->decoded_op.row_data);

  // TODO: the Insert() call below will re-encode the key, which is a
  // waste. Should pass through the KeyProbe structure perhaps.

  // Now try to insert into memrowset. The memrowset itself will return
  // AlreadyPresent if it has already been inserted there.
  Status s = comps->memrowset->Insert(ts, row, tx_state->op_id());
  if (PREDICT_TRUE(s.ok())) {
    insert->SetInsertSucceeded(comps->memrowset->mrs_id());
  } else {
    if (s.IsAlreadyPresent() && metrics_) {
      metrics_->insertions_failed_dup_key->Increment();
    }
    insert->SetFailed(s);
  }
  return s;
}

Status Tablet::MutateRowUnlocked(WriteTransactionState *tx_state,
                                 RowOp* mutate) {
  DCHECK(tx_state != NULL) << "you must have a WriteTransactionState";
  DCHECK(tx_state->op_id().IsInitialized()) << "TransactionState OpId needed for anchoring";
  DCHECK_EQ(tx_state->schema_at_decode_time(), schema_.get());

  gscoped_ptr<OperationResultPB> result(new OperationResultPB());

  const TabletComponents* comps = DCHECK_NOTNULL(tx_state->tablet_components());

  // Validate the update.
  RowChangeListDecoder rcl_decoder(schema_.get(), mutate->decoded_op.changelist);
  Status s = rcl_decoder.Init();
  if (rcl_decoder.is_reinsert()) {
    // REINSERT mutations are the byproduct of an INSERT on top of a ghost
    // row, not something the user is allowed to specify on their own.
    s = Status::InvalidArgument("User may not specify REINSERT mutations");
  }
  if (!s.ok()) {
    mutate->SetFailed(s);
    return s;
  }

  Timestamp ts = tx_state->timestamp();

  ProbeStats stats;
  // Submit the stats before returning from this function
  ProbeStatsSubmitter submitter(stats, metrics_.get());

  // First try to update in memrowset.
  s = comps->memrowset->MutateRow(ts,
                            *mutate->key_probe,
                            mutate->decoded_op.changelist,
                            tx_state->op_id(),
                            &stats,
                            result.get());
  if (s.ok()) {
    mutate->SetMutateSucceeded(result.Pass());
    return s;
  }
  if (!s.IsNotFound()) {
    mutate->SetFailed(s);
    return s;
  }

  // Next, check the disk rowsets.

  // TODO: could iterate the rowsets in a smart order
  // based on recent statistics - eg if a rowset is getting
  // updated frequently, pick that one first.
  vector<RowSet *> to_check;
  comps->rowsets->FindRowSetsWithKeyInRange(mutate->key_probe->encoded_key_slice(),
                                            &to_check);
  BOOST_FOREACH(RowSet *rs, to_check) {
    s = rs->MutateRow(ts,
                      *mutate->key_probe,
                      mutate->decoded_op.changelist,
                      tx_state->op_id(),
                      &stats,
                      result.get());
    if (s.ok()) {
      mutate->SetMutateSucceeded(result.Pass());
      return s;
    }
    if (!s.IsNotFound()) {
      mutate->SetFailed(s);
      return s;
    }
  }

  s = Status::NotFound("key not found");
  mutate->SetFailed(s);
  return s;
}

void Tablet::ApplyRowOperations(WriteTransactionState* tx_state) {
  BOOST_FOREACH(RowOp* row_op, *tx_state->mutable_row_ops()) {
    ApplyRowOperation(tx_state, row_op);
  }
}

void Tablet::ApplyRowOperation(WriteTransactionState* tx_state,
                               RowOp* row_op) {
  switch (row_op->decoded_op.type) {
    case RowOperationsPB::INSERT:
      ignore_result(InsertUnlocked(tx_state, row_op));
      return;

    case RowOperationsPB::UPDATE:
    case RowOperationsPB::DELETE:
      ignore_result(MutateRowUnlocked(tx_state, row_op));
      return;

    default:
      LOG(FATAL) << RowOperationsPB::Type_Name(row_op->decoded_op.type);
  }
}

void Tablet::ModifyRowSetTree(const RowSetTree& old_tree,
                              const RowSetVector& rowsets_to_remove,
                              const RowSetVector& rowsets_to_add,
                              RowSetTree* new_tree) {
  RowSetVector post_swap;

  // O(n^2) diff algorithm to collect the set of rowsets excluding
  // the rowsets that were included in the compaction
  int num_removed = 0;

  BOOST_FOREACH(const shared_ptr<RowSet> &rs, old_tree.all_rowsets()) {
    // Determine if it should be removed
    bool should_remove = false;
    BOOST_FOREACH(const shared_ptr<RowSet> &to_remove, rowsets_to_remove) {
      if (to_remove == rs) {
        should_remove = true;
        num_removed++;
        break;
      }
    }
    if (!should_remove) {
      post_swap.push_back(rs);
    }
  }

  CHECK_EQ(num_removed, rowsets_to_remove.size());

  // Then push the new rowsets on the end of the new list
  std::copy(rowsets_to_add.begin(),
            rowsets_to_add.end(),
            std::back_inserter(post_swap));

  CHECK_OK(new_tree->Reset(post_swap));
}

void Tablet::AtomicSwapRowSets(const RowSetVector &old_rowsets,
                               const RowSetVector &new_rowsets) {
  boost::lock_guard<rw_spinlock> lock(component_lock_);
  AtomicSwapRowSetsUnlocked(old_rowsets, new_rowsets);
}

void Tablet::AtomicSwapRowSetsUnlocked(const RowSetVector &to_remove,
                                       const RowSetVector &to_add) {
  DCHECK(component_lock_.is_locked());

  shared_ptr<RowSetTree> new_tree(new RowSetTree());
  ModifyRowSetTree(*components_->rowsets,
                   to_remove, to_add, new_tree.get());

  components_ = new TabletComponents(components_->memrowset, new_tree);
}

Status Tablet::DoMajorDeltaCompaction(const ColumnIndexes& column_indexes,
                                      shared_ptr<RowSet> input_rs) {
  return down_cast<DiskRowSet*>(input_rs.get())->MajorCompactDeltaStores(column_indexes);
}

Status Tablet::DeleteCompactionInputs(const RowSetsInCompaction &input) {
  Status ret;
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, input.rowsets()) {
    // Skip MemRowSet & DuplicatingRowSets which don't have metadata.
    RowSetMetadata* rsm = rs->metadata().get();
    if (rsm == NULL) {
      continue;
    }

    // Collect all blocks to delete.
    //
    // By this point the rowset is orphaned, so it's safe to iterate like
    // this without acquiring any locks.
    vector<BlockId> to_delete;
    if (!rsm->adhoc_index_block().IsNull()) {
      to_delete.push_back(rsm->adhoc_index_block());
    }
    if (!rsm->bloom_block().IsNull()) {
      to_delete.push_back(rsm->bloom_block());
    }
    BOOST_FOREACH(const BlockId& b, rsm->redo_delta_blocks()) {
      to_delete.push_back(b);
    }
    BOOST_FOREACH(const BlockId& b, rsm->undo_delta_blocks()) {
      to_delete.push_back(b);
    }
    for (int i = 0; i < rsm->schema().num_columns(); i++) {
      to_delete.push_back(rsm->column_data_block(i));
    }

    // Delete them. Return the first failure.
    Status s = metadata_->fs_manager()->DeleteBlocks(to_delete);
    if (PREDICT_TRUE(s.ok())) {
      VLOG(1) << "Deleted " << to_delete.size() << " orphaned blocks "
              << "after compacting " << rs->ToString();
    } else {
      LOG(WARNING) << "Failed to delete orphaned blocks after compacting "
                   << rs->ToString() << ": " << s.ToString();
      if (ret.ok()) {
        ret = s;
      }
    }
  }

  // Return the first failure.
  return ret;
}

Status Tablet::Flush() {
  boost::lock_guard<Semaphore> lock(rowsets_flush_sem_);
  return FlushUnlocked();
}

Status Tablet::FlushUnlocked() {
  RowSetsInCompaction input;
  shared_ptr<MemRowSet> old_mrs;
  shared_ptr<Schema> old_schema;
  {
    // Create a new MRS with the latest schema.
    boost::lock_guard<rw_spinlock> lock(component_lock_);
    old_schema = schema_;
    RETURN_NOT_OK(ReplaceMemRowSetUnlocked(*old_schema.get(), &input, &old_mrs));
  }

  // Wait for any in-flight transactions to finish against the old MRS
  // before we flush it.
  mvcc_.WaitForAllInFlightToCommit();

  // Note: "input" should only contain old_mrs.
  return FlushInternal(input, old_mrs, *old_schema.get());
}

Status Tablet::ReplaceMemRowSetUnlocked(const Schema& schema,
                                        RowSetsInCompaction *compaction,
                                        shared_ptr<MemRowSet> *old_ms) {
  *old_ms = components_->memrowset;
  // Mark the memrowset rowset as locked, so compactions won't consider it
  // for inclusion in any concurrent compactions.
  shared_ptr<boost::mutex::scoped_try_lock> ms_lock(
    new boost::mutex::scoped_try_lock(*((*old_ms)->compact_flush_lock())));
  CHECK(ms_lock->owns_lock());

  // Add to compaction.
  compaction->AddRowSet(*old_ms, ms_lock);

  shared_ptr<MemRowSet> new_mrs(new MemRowSet(next_mrs_id_++, schema, log_anchor_registry_,
                                mem_tracker_));
  shared_ptr<RowSetTree> new_rst(new RowSetTree());
  ModifyRowSetTree(*components_->rowsets,
                   RowSetVector(), // remove nothing
                   boost::assign::list_of(*old_ms), // add the old MRS
                   new_rst.get());

  // Swap it in
  components_ = new TabletComponents(new_mrs, new_rst);
  return Status::OK();
}

Status Tablet::FlushInternal(const RowSetsInCompaction& input,
                             const shared_ptr<MemRowSet>& old_ms,
                             const Schema& schema) {
  CHECK(open_);

  // Step 1. Freeze the old memrowset by blocking readers and swapping
  // it in as a new rowset, replacing it with an empty one.
  //
  // At this point, we have already swapped in a new empty rowset, and
  // any new inserts are going into that one. 'old_ms' is effectively
  // frozen -- no new inserts should arrive after this point.
  //
  // NOTE: updates and deletes may still arrive into 'old_ms' at this point.
  //
  // TODO(perf): there's a memrowset.Freeze() call which we might be able to
  // use to improve iteration performance during the flush. The old design
  // used this, but not certain whether it's still doable with the new design.

  uint64_t start_insert_count = old_ms->debug_insert_count();
  int64_t mrs_being_flushed = old_ms->mrs_id();

  if (flush_hooks_) {
    RETURN_NOT_OK_PREPEND(flush_hooks_->PostSwapNewMemRowSet(),
                          "PostSwapNewMemRowSet hook failed");
  }

  LOG(INFO) << "Flush: entering stage 1 (old memrowset already frozen for inserts)";
  input.DumpToLog();
  LOG(INFO) << "Memstore in-memory size: " << old_ms->memory_footprint() << " bytes";

  RETURN_NOT_OK(DoCompactionOrFlush(schema, input, mrs_being_flushed));

  // Sanity check that no insertions happened during our flush.
  CHECK_EQ(start_insert_count, old_ms->debug_insert_count())
    << "Sanity check failed: insertions continued in memrowset "
    << "after flush was triggered! Aborting to prevent dataloss.";

  return Status::OK();
}

Status Tablet::CreatePreparedAlterSchema(AlterSchemaTransactionState *tx_state,
                                         const Schema* schema) {
  if (!key_schema_.KeyEquals(*schema)) {
    return Status::InvalidArgument("Schema keys cannot be altered",
                                   schema->CreateKeyProjection().ToString());
  }

  if (!schema->has_column_ids()) {
    // this probably means that the request is not from the Master
    return Status::InvalidArgument("Missing Column IDs");
  }

  // Alter schema must run when no reads/writes are in progress.
  // However, compactions and flushes can continue to run in parallel
  // with the schema change,
  tx_state->AcquireSchemaLock(&schema_lock_);

  tx_state->set_schema(schema);
  return Status::OK();
}

Status Tablet::AlterSchema(AlterSchemaTransactionState *tx_state) {
  DCHECK(key_schema_.KeyEquals(*DCHECK_NOTNULL(tx_state->schema()))) <<
    "Schema keys cannot be altered";

  // Prevent any concurrent flushes. Otherwise, we run into issues where
  // we have an MRS in the rowset tree, and we can't alter its schema
  // in-place.
  boost::lock_guard<Semaphore> lock(rowsets_flush_sem_);

  RowSetsInCompaction input;
  shared_ptr<MemRowSet> old_ms;
  {
    // If the current version >= new version, there is nothing to do.
    bool same_schema = schema_->Equals(*tx_state->schema());
    if (metadata_->schema_version() >= tx_state->schema_version()) {
      LOG(INFO) << "Already running schema version " << metadata_->schema_version()
                << " got alter request for version " << tx_state->schema_version();
      return Status::OK();
    }

    LOG(INFO) << "Alter schema from " << schema_->ToString()
              << " version " << metadata_->schema_version()
              << " to " << tx_state->schema()->ToString()
              << " version " << tx_state->schema_version();
    DCHECK(schema_lock_.is_locked());
    schema_.reset(new Schema(*tx_state->schema()));
    metadata_->SetSchema(*schema_, tx_state->schema_version());
    if (tx_state->has_new_table_name()) {
      metadata_->SetTableName(tx_state->new_table_name());
    }

    // If the current schema and the new one are equal, there is nothing to do.
    if (same_schema) {
      return metadata_->Flush();
    }

    // Update the DiskRowSet/DeltaTracker
    // TODO: This triggers a flush of the DeltaMemStores...
    //       The flush should be just a message (async)...
    //       with the current code the only way we can do a flush ouside this big lock
    //       is to get the list of DeltaMemStores out from the AlterSchema method...
    BOOST_FOREACH(const shared_ptr<RowSet>& rs, components_->rowsets->all_rowsets()) {
      RETURN_NOT_OK(rs->AlterSchema(*schema_.get()));
    }
  }


  // Replace the MemRowSet
  {
    boost::lock_guard<rw_spinlock> lock(component_lock_);
    RETURN_NOT_OK(ReplaceMemRowSetUnlocked(*schema_.get(), &input, &old_ms));
  }

  tx_state->ReleaseSchemaLock();

  // Flush the old MemRowSet
  return FlushInternal(input, old_ms, *tx_state->schema());
}

void Tablet::SetCompactionHooksForTests(
  const shared_ptr<Tablet::CompactionFaultHooks> &hooks) {
  compaction_hooks_ = hooks;
}

void Tablet::SetFlushHooksForTests(
  const shared_ptr<Tablet::FlushFaultHooks> &hooks) {
  flush_hooks_ = hooks;
}

void Tablet::SetFlushCompactCommonHooksForTests(
  const shared_ptr<Tablet::FlushCompactCommonHooks> &hooks) {
  common_hooks_ = hooks;
}

int32_t Tablet::CurrentMrsIdForTests() const {
  boost::shared_lock<rw_spinlock> lock(component_lock_);
  return components_->memrowset->mrs_id();
}

enum {
  kFlushDueToTimeMs = 120 * 1000
};

// Common method for MRS and DMS flush. Sets the performance improvement based on the anchored ram
// if it's over the threshold, else it will set it based on how long it has been since the last
// flush.
static void SetPerfImprovementForFlush(MaintenanceOpStats* stats,
                                       double elapsed_ms, bool is_empty) {
  if (stats->ram_anchored > FLAGS_flush_threshold_mb * 1024 * 1024) {
    // If we're over the user-specified flush threshold, then consider the perf
    // improvement to be 1 for every extra MB.  This produces perf_improvement results
    // which are much higher than any compaction would produce, and means that, when
    // there is an MRS over threshold, a flush will almost always be selected instead of
    // a compaction.  That's not necessarily a good thing, but in the absense of better
    // heuristics, it will do for now.
    int extra_mb = stats->ram_anchored / 1024 / 1024;
    stats->perf_improvement = extra_mb;
  } else if (!is_empty && elapsed_ms > kFlushDueToTimeMs) {
    // Even if we aren't over the threshold, consider flushing if we haven't flushed
    // in a long time. But, don't give it a large perf_improvement score. We should
    // only do this if we really don't have much else to do.
    double extra_millis = elapsed_ms - 60 * 1000;
    stats->perf_improvement = std::min(600000.0 / extra_millis, 0.05);
  }
}

// Maintenance op for MRS flush.
//
class FlushMRSOp : public MaintenanceOp {
 public:
  explicit FlushMRSOp(Tablet* tablet)
    : MaintenanceOp(StringPrintf("FlushMRSOp(%s)", tablet->tablet_id().c_str())),
      tablet_(tablet) {
    time_since_flush_.start();
  }

  virtual void UpdateStats(MaintenanceOpStats* stats) OVERRIDE {
    boost::lock_guard<simple_spinlock> l(lock_);

    {
      boost::unique_lock<Semaphore> lock(tablet_->rowsets_flush_sem_,
                                         boost::defer_lock);
      stats->runnable = lock.try_lock();
    }
    stats->ram_anchored = tablet_->MemRowSetSize();
    // TODO: add a field to MemRowSet storing how old a timestamp it contains
    stats->ts_anchored_secs = 0;
    // TODO: use workload statistics here to find out how "hot" the tablet has
    // been in the last 5 minutes.
    SetPerfImprovementForFlush(stats,
                               time_since_flush_.elapsed().wall_millis(),
                               tablet_->MemRowSetEmpty());
  }

  virtual bool Prepare() OVERRIDE {
    // Try to acquire the rowsets_flush_sem_.  If we can't, the Prepare step
    // fails.  This also implies that only one instance of FlushMRSOp can be
    // running at once.
    return tablet_->rowsets_flush_sem_.try_lock();
  }

  virtual void Perform() OVERRIDE {
    CHECK(!tablet_->rowsets_flush_sem_.try_lock());

    tablet_->FlushUnlocked();

    {
      boost::lock_guard<simple_spinlock> l(lock_);
      time_since_flush_.start();
    }
    tablet_->rowsets_flush_sem_.unlock();
  }

  virtual Histogram* DurationHistogram() OVERRIDE {
    return tablet_->metrics()->flush_mrs_duration;
  }

  virtual AtomicGauge<uint32_t>* RunningGauge() OVERRIDE {
    return tablet_->metrics()->flush_mrs_running;
  }

 private:


  // Lock protecting time_since_flush_
  mutable simple_spinlock lock_;
  Stopwatch time_since_flush_;

  Tablet *const tablet_;
};

// MaintenanceOp for rowset compaction.
//
// This periodically invokes the tablet's CompactionPolicy to select a compaction.  The
// compaction policy's "quality" is used as a proxy for the performance improvement which
// is exposed back to the maintenance manager. As compactions become more fruitful (i.e.
// more overlapping rowsets), the perf_improvement score goes up, increasing priority
// with which a compaction on this tablet will be selected by the maintenance manager.
class CompactRowSetsOp : public MaintenanceOp {
 public:
  explicit CompactRowSetsOp(Tablet* tablet)
    : MaintenanceOp(Substitute("CompactRowSetsOp($0)", tablet->tablet_id())),
      tablet_(tablet) {
    since_last_stats_update_.start();
  }

  virtual void UpdateStats(MaintenanceOpStats* stats) OVERRIDE {
    boost::lock_guard<simple_spinlock> l(lock_);
    // If we've computed stats recently, no need to recompute.
    // TODO: it would be nice if we could invalidate this on any Flush as well
    // as after a compaction.
    if (since_last_stats_update_.elapsed().wall_millis() < kRefreshIntervalMs) {
      *stats = prev_stats_;
      return;
    }

    tablet_->UpdateCompactionStats(&prev_stats_);
    *stats = prev_stats_;
    since_last_stats_update_.start();
  }

  virtual bool Prepare() OVERRIDE {
    boost::lock_guard<simple_spinlock> l(lock_);
    // Resetting stats ensures that, while this compaction is running, if we
    // are asked for more stats, we won't end up calling Compact() twice
    // and accidentally scheduling a much less fruitful compaction.
    prev_stats_.Clear();
    return true;
  }

  virtual void Perform() OVERRIDE {
    WARN_NOT_OK(tablet_->Compact(Tablet::COMPACT_NO_FLAGS),
                Substitute("Compaction failed on $0", tablet_->tablet_id()));
  }

  virtual Histogram* DurationHistogram() OVERRIDE {
    return tablet_->metrics()->compact_rs_duration;
  }

  virtual AtomicGauge<uint32_t>* RunningGauge() OVERRIDE {
    return tablet_->metrics()->compact_rs_running;
  }

 private:
  enum {
    kRefreshIntervalMs = 5000
  };

  mutable simple_spinlock lock_;
  Stopwatch since_last_stats_update_;
  MaintenanceOpStats prev_stats_;

  Tablet * const tablet_;
};

class FlushDeltaMemStoresOp : public MaintenanceOp {
 public:
  explicit FlushDeltaMemStoresOp(Tablet* tablet)
    : MaintenanceOp(StringPrintf("FlushDeltaMemStoresOp(%s)",
                                 tablet->tablet_id().c_str())),
      tablet_(tablet) {
    time_since_flush_.start();
  }

  virtual void UpdateStats(MaintenanceOpStats* stats) OVERRIDE {
    boost::lock_guard<simple_spinlock> l(lock_);
    size_t dms_size = tablet_->DeltaMemStoresSize();
    stats->ram_anchored = dms_size;
    stats->runnable = true;
    stats->ts_anchored_secs = 0;

    SetPerfImprovementForFlush(stats,
                               time_since_flush_.elapsed().wall_millis(),
                               tablet_->DeltaMemRowSetEmpty());

  }

  virtual bool Prepare() OVERRIDE {
    return true;
  }

  virtual void Perform() OVERRIDE {
    WARN_NOT_OK(tablet_->FlushBiggestDMS(),
                Substitute("Failed to flush biggest DMS on $0", tablet_->tablet_id()));
    {
      boost::lock_guard<simple_spinlock> l(lock_);
      time_since_flush_.start();
    }
  }

  virtual Histogram* DurationHistogram() OVERRIDE {
    return tablet_->metrics()->flush_dms_duration;
  }

  virtual AtomicGauge<uint32_t>* RunningGauge() OVERRIDE {
    return tablet_->metrics()->flush_dms_running;
  }

 private:
  Tablet *const tablet_;
  mutable simple_spinlock lock_;
  Stopwatch time_since_flush_;
};

Status Tablet::PickRowSetsToCompact(RowSetsInCompaction *picked,
                                    CompactFlags flags) const {
  // Grab a local reference to the current RowSetTree. This is to avoid
  // holding the component_lock_ for too long. See the comment on component_lock_
  // in tablet.h for details on why that would be bad.
  shared_ptr<RowSetTree> rowsets_copy;
  {
    boost::shared_lock<rw_spinlock> lock(component_lock_);
    rowsets_copy = components_->rowsets;
  }

  boost::lock_guard<boost::mutex> compact_lock(compact_select_lock_);
  CHECK_EQ(picked->num_rowsets(), 0);

  unordered_set<RowSet*> picked_set;

  if (flags & FORCE_COMPACT_ALL) {
    // Compact all rowsets, regardless of policy.
    BOOST_FOREACH(const shared_ptr<RowSet>& rs, rowsets_copy->all_rowsets()) {
      if (rs->IsAvailableForCompaction()) {
        picked_set.insert(rs.get());
      }
    }
  } else {
    // Let the policy decide which rowsets to compact.
    double quality = 0;
    RETURN_NOT_OK(compaction_policy_->PickRowSets(*rowsets_copy, &picked_set, &quality, NULL));
    VLOG(2) << "Compaction quality: " << quality;
  }

  boost::shared_lock<rw_spinlock> lock(component_lock_);
  BOOST_FOREACH(const shared_ptr<RowSet>& rs, components_->rowsets->all_rowsets()) {
    if (picked_set.erase(rs.get()) == 0) {
      // Not picked.
      continue;
    }

    // Grab the compact_flush_lock: this prevents any other concurrent
    // compaction from selecting this same rowset, and also ensures that
    // we don't select a rowset which is currently in the middle of being
    // flushed.
    shared_ptr<boost::mutex::scoped_try_lock> lock(
      new boost::mutex::scoped_try_lock(*rs->compact_flush_lock()));
    CHECK(lock->owns_lock()) << rs->ToString() << " appeared available for "
      "compaction when inputs were selected, but was unable to lock its "
      "compact_flush_lock to prepare for compaction.";

    // Push the lock on our scoped list, so we unlock when done.
    picked->AddRowSet(rs, lock);
  }

  // When we iterated through the current rowsets, we should have found all of the
  // rowsets that we picked. If we didn't, that implies that some other thread swapped
  // them out while we were making our selection decision -- that's not possible
  // since we only picked rowsets that were marked as available for compaction.
  if (!picked_set.empty()) {
    BOOST_FOREACH(const RowSet* not_found, picked_set) {
      LOG(ERROR) << "Rowset selected for compaction but not available anymore: "
                 << not_found->ToString();
    }
    LOG(FATAL) << "Was unable to find all rowsets selected for compaction";
  }
  return Status::OK();
}

void Tablet::GetRowSetsForTests(RowSetVector* out) {
  shared_ptr<RowSetTree> rowsets_copy;
  {
    boost::shared_lock<rw_spinlock> lock(component_lock_);
    rowsets_copy = components_->rowsets;
  }
  BOOST_FOREACH(const shared_ptr<RowSet>& rs, rowsets_copy->all_rowsets()) {
    out->push_back(rs);
  }
}

bool Tablet::IsTabletFileName(const std::string& fname) {
  if (HasSuffixString(fname, kTmpSuffix)) {
    LOG(WARNING) << "Ignoring tmp file in master block dir: " << fname;
    return false;
  }

  if (HasPrefixString(fname, ".")) {
    // Hidden file or ./..
    VLOG(1) << "Ignoring hidden file in master block dir: " << fname;
    return false;
  }

  return true;
}

void Tablet::RegisterMaintenanceOps(MaintenanceManager* maint_mgr) {
  gscoped_ptr<MaintenanceOp> mrs_flush_op(new FlushMRSOp(this));
  maint_mgr->RegisterOp(mrs_flush_op.get());
  maintenance_ops_.push_back(mrs_flush_op.release());

  gscoped_ptr<MaintenanceOp> rs_compact_op(new CompactRowSetsOp(this));
  maint_mgr->RegisterOp(rs_compact_op.get());
  maintenance_ops_.push_back(rs_compact_op.release());

  gscoped_ptr<MaintenanceOp> dms_flush_op(new FlushDeltaMemStoresOp(this));
  maint_mgr->RegisterOp(dms_flush_op.get());
  maintenance_ops_.push_back(dms_flush_op.release());
}

void Tablet::UnregisterMaintenanceOps() {
  BOOST_FOREACH(MaintenanceOp* op, maintenance_ops_) {
    op->Unregister();
  }
  STLDeleteElements(&maintenance_ops_);
}

Status Tablet::FlushMetadata(const RowSetVector& to_remove,
                             const RowSetMetadataVector& to_add,
                             int64_t mrs_being_flushed) {
  RowSetMetadataIds to_remove_meta;
  BOOST_FOREACH(const shared_ptr<RowSet>& rowset, to_remove) {
    // Skip MemRowSet & DuplicatingRowSets which don't have metadata.
    if (rowset->metadata().get() == NULL) {
      continue;
    }
    to_remove_meta.insert(rowset->metadata()->id());
  }

  // If we're flushing an mrs update the latest durable one in the metadata
  if (mrs_being_flushed != kNoMrsFlushed) {
    return metadata_->UpdateAndFlush(to_remove_meta, to_add, mrs_being_flushed);
  }
  return metadata_->UpdateAndFlush(to_remove_meta, to_add);
}

Status Tablet::DoCompactionOrFlush(const Schema& schema,
        const RowSetsInCompaction &input, int64_t mrs_being_flushed) {
  const char *op_name =
        (mrs_being_flushed == kNoMrsFlushed) ?  "Compaction" : "Flush";

  LOG(INFO) << op_name << ": entering phase 1 (flushing snapshot)";

  MvccSnapshot flush_snap(mvcc_);

  VLOG(1) << "Flushing with MVCC snapshot: " << flush_snap.ToString();

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostTakeMvccSnapshot(),
                          "PostTakeMvccSnapshot hook failed");
  }

  shared_ptr<CompactionInput> merge;
  RETURN_NOT_OK(input.CreateCompactionInput(flush_snap, &schema, &merge));

  RollingDiskRowSetWriter drsw(metadata_.get(), merge->schema(), bloom_sizing(),
                               compaction_policy_->target_rowset_size());
  RETURN_NOT_OK_PREPEND(drsw.Open(), "Failed to open DiskRowSet for flush");
  RETURN_NOT_OK_PREPEND(FlushCompactionInput(merge.get(), flush_snap, &drsw),
                        "Flush to disk failed");
  RETURN_NOT_OK_PREPEND(drsw.Finish(), "Failed to finish DRS writer");

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostWriteSnapshot(),
                          "PostWriteSnapshot hook failed");
  }

  // Though unlikely, it's possible that all of the input rows were actually
  // GCed in this compaction. In that case, we don't actually want to reopen.
  bool gced_all_input = drsw.written_count() == 0;
  if (gced_all_input) {
    LOG(INFO) << op_name << " resulted in no output rows (all input rows "
              << "were GCed!)  Removing all input rowsets.";
    AtomicSwapRowSets(input.rowsets(), RowSetVector());

    // Write out the new Tablet Metadata
    RETURN_NOT_OK_PREPEND(FlushMetadata(input.rowsets(),
                                        RowSetMetadataVector(),
                                        mrs_being_flushed),
                          "Failed to flush new tablet metadata");

    // Remove old rowsets.
    // TODO: Consensus catch-up may want to reserve the compaction inputs.
    WARN_NOT_OK(DeleteCompactionInputs(input),
        Substitute("Unable to remove $0 inputs. Will GC later.", op_name));

    return Status::OK();
  }

  // The RollingDiskRowSet writer wrote out one or more RowSets as the
  // output. Open these into 'new_rowsets'.
  vector<shared_ptr<RowSet> > new_disk_rowsets;
  RowSetMetadataVector new_drs_metas;
  drsw.GetWrittenRowSetMetadata(&new_drs_metas);

  if (metrics_.get()) metrics_->bytes_flushed->IncrementBy(drsw.written_size());
  CHECK(!new_drs_metas.empty());
  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& meta, new_drs_metas) {
    shared_ptr<DiskRowSet> new_rowset;
    Status s = DiskRowSet::Open(meta, log_anchor_registry_, &new_rowset, mem_tracker_);
    if (!s.ok()) {
      LOG(WARNING) << "Unable to open snapshot " << op_name << " results "
                   << meta->ToString() << ": " << s.ToString();
      return s;
    }
    new_disk_rowsets.push_back(new_rowset);
  }

  // Setup for Phase 2: Start duplicating any new updates into the new on-disk
  // rowsets.
  //
  // During Phase 1, we may have missed some updates which came into the input
  // rowsets while we were writing. So, we can't immediately start reading from
  // the on-disk rowsets alone. Starting here, we continue to read from the
  // original rowset(s), but mirror updates to both the input and the output
  // data.
  //
  // It's crucial that, during the rest of the compaction, we do not allow the
  // output rowsets to flush their deltas to disk. This is to avoid the following
  // bug:
  // - during phase 1, timestamp 1 updates a flushed row. This is only reflected in the
  //   input rowset. (ie it is a "missed delta")
  // - during phase 2, timestamp 2 updates the same row. This is reflected in both the
  //   input and output, because of the DuplicatingRowSet.
  // - now suppose the output rowset were allowed to flush deltas. This would create the
  //   first DeltaFile for the output rowset, with only timestamp 2.
  // - Now we run the "ReupdateMissedDeltas", and copy over the first transaction to the output
  //   DMS, which later flushes.
  // The end result would be that redos[0] has timestamp 2, and redos[1] has timestamp 1.
  // This breaks an invariant that the redo files are time-ordered, and would we would probably
  // reapply the deltas in the wrong order on the read path.
  //
  // The way that we avoid this case is that DuplicatingRowSet's FlushDeltas method is a
  // no-op.
  LOG(INFO) << op_name << ": entering phase 2 (starting to duplicate updates "
            << "in new rowsets)";
  shared_ptr<DuplicatingRowSet> inprogress_rowset(
    new DuplicatingRowSet(input.rowsets(), new_disk_rowsets));
  shared_ptr<Schema> schema2;

  // The next step is to swap in the DuplicatingRowSet, and at the same time, determine an
  // MVCC snapshot which includes all of the transactions that saw a pre-DuplicatingRowSet
  // version of components_.
  MvccSnapshot non_duplicated_txns_snap;
  vector<Timestamp> in_flight_during_swap;
  {
    // Taking component_lock_ in write mode ensures that no new transactions
    // can start (or snapshot components_) during this block.
    boost::lock_guard<rw_spinlock> lock(component_lock_);
    AtomicSwapRowSetsUnlocked(input.rowsets(), boost::assign::list_of(inprogress_rowset));
    schema2 = schema_;

    // NOTE: transactions may *commit* in between these two lines.
    // We need to make sure all such transactions end up in the
    // in-flight list, the 'non_duplicated_txns_snap' snapshot, or both. Thus it's
    // crucial that these next two lines are in this order!
    mvcc_.GetInFlightTransactionTimestamps(&in_flight_during_swap);
    non_duplicated_txns_snap = MvccSnapshot(mvcc_);
  }

  // All transactions committed in 'non_duplicated_txns_snap' saw the pre-swap components_.
  // Additionally, any transactions that were in-flight during the above block by definition
  // _started_ before the swap. Hence the in-flights also need to get included in
  // non_duplicated_txns_snap. To do so, we wait for those in-flights to commit, and then
  // manually include them into our snapshot.
  if (VLOG_IS_ON(1) && !in_flight_during_swap.empty()) {
    VLOG(1) << "Waiting for " << in_flight_during_swap.size() << " in-flight txns to commit "
            << "before finishing compaction...";
    BOOST_FOREACH(const Timestamp& ts, in_flight_during_swap) {
      VLOG(1) << "  " << ts.value();
    }
  }

  // This wait is a little bit conservative - technically we only need to wait for
  // those transactions in 'in_flight_during_swap', but MVCC doesn't implement the
  // ability to wait for a specific set. So instead we wait for all current in-flights --
  // a bit more than we need, but still correct.
  mvcc_.WaitForAllInFlightToCommit();

  // Then we want to consider all those transactions that were in-flight when we did the
  // swap as committed in 'non_duplicated_txns_snap'.
  non_duplicated_txns_snap.AddCommittedTimestamps(in_flight_during_swap);

  // Ensure that the latest schema is set to the new RowSets
  BOOST_FOREACH(const shared_ptr<RowSet>& rs, new_disk_rowsets) {
    RETURN_NOT_OK_PREPEND(rs->AlterSchema(*schema2.get()),
                          "Failed to set current schema on latest RS");
  }

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostSwapInDuplicatingRowSet(),
                          "PostSwapInDuplicatingRowSet hook failed");
  }

  // Phase 2. Here we re-scan the compaction input, copying those missed updates into the
  // new rowset's DeltaTracker.
  LOG(INFO) << op_name << " Phase 2: carrying over any updates which arrived during Phase 1";
  LOG(INFO) << "Phase 2 snapshot: " << non_duplicated_txns_snap.ToString();
  RETURN_NOT_OK_PREPEND(
      input.CreateCompactionInput(non_duplicated_txns_snap, schema2.get(), &merge),
          Substitute("Failed to create $0 inputs", op_name).c_str());

  // Update the output rowsets with the deltas that came in in phase 1, before we swapped
  // in the DuplicatingRowSets. This will perform a flush of the updated DeltaTrackers
  // in the end so that the data that is reported in the log as belonging to the input
  // rowsets is flushed.
  RETURN_NOT_OK_PREPEND(ReupdateMissedDeltas(metadata_->oid(),
                                             merge.get(),
                                             flush_snap,
                                             non_duplicated_txns_snap,
                                             new_disk_rowsets),
        Substitute("Failed to re-update deltas missed during $0 phase 1",
                     op_name).c_str());

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostReupdateMissedDeltas(),
                          "PostReupdateMissedDeltas hook failed");
  }

  // ------------------------------
  // Flush was successful.

  // Replace the compacted rowsets with the new on-disk rowsets.
  AtomicSwapRowSets(boost::assign::list_of(inprogress_rowset), new_disk_rowsets);

  // Write out the new Tablet Metadata
  RETURN_NOT_OK_PREPEND(FlushMetadata(input.rowsets(), new_drs_metas, mrs_being_flushed),
                        "Failed to flush new tablet metadata");

  // Remove old rowsets
  WARN_NOT_OK(DeleteCompactionInputs(input),
              Substitute("Unable to remove $0 inputs. Will GC later.",
                           op_name).c_str());

  LOG(INFO) << op_name << " successful on " << drsw.written_count()
            << " rows " << "(" << drsw.written_size() << " bytes)";

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostSwapNewRowSet(),
                          "PostSwapNewRowSet hook failed");
  }

  return Status::OK();
}

Status Tablet::Compact(CompactFlags flags) {
  CHECK(open_);

  RowSetsInCompaction input;
  // Step 1. Capture the rowsets to be merged
  RETURN_NOT_OK_PREPEND(PickRowSetsToCompact(&input, flags),
                        "Failed to pick rowsets to compact");
  if (input.num_rowsets() < 2) {
    VLOG(1) << "Not enough rowsets to run compaction! Aborting...";
    return Status::OK();
  }
  LOG(INFO) << "Compaction: stage 1 complete, picked "
            << input.num_rowsets() << " rowsets to compact";
  if (compaction_hooks_) {
    RETURN_NOT_OK_PREPEND(compaction_hooks_->PostSelectIterators(),
                          "PostSelectIterators hook failed");
  }

  input.DumpToLog();

  shared_ptr<Schema> cur_schema(schema());
  return DoCompactionOrFlush(*cur_schema.get(), input, kNoMrsFlushed);
}

void Tablet::UpdateCompactionStats(MaintenanceOpStats* stats) {

  // TODO: use workload statistics here to find out how "hot" the tablet has
  // been in the last 5 minutes, and somehow scale the compaction quality
  // based on that, so we favor hot tablets.
  double quality = 0;
  unordered_set<RowSet*> picked_set_ignored;

  shared_ptr<RowSetTree> rowsets_copy;
  {
    boost::shared_lock<rw_spinlock> lock(component_lock_);
    rowsets_copy = components_->rowsets;
  }

  {
    boost::lock_guard<boost::mutex> compact_lock(compact_select_lock_);
    WARN_NOT_OK(compaction_policy_->PickRowSets(*rowsets_copy, &picked_set_ignored, &quality, NULL),
                Substitute("Couldn't determine compaction quality for $0", tablet_id()));
  }

  VLOG(1) << "Best compaction for " << tablet_id() << ": " << quality;

  stats->runnable = quality >= 0;
  stats->perf_improvement = quality;
}


Status Tablet::DebugDump(vector<string> *lines) {
  boost::shared_lock<rw_spinlock> lock(component_lock_);

  LOG_STRING(INFO, lines) << "Dumping tablet:";
  LOG_STRING(INFO, lines) << "---------------------------";

  LOG_STRING(INFO, lines) << "MRS " << components_->memrowset->ToString() << ":";
  RETURN_NOT_OK(components_->memrowset->DebugDump(lines));

  BOOST_FOREACH(const shared_ptr<RowSet> &rs, components_->rowsets->all_rowsets()) {
    LOG_STRING(INFO, lines) << "RowSet " << rs->ToString() << ":";
    RETURN_NOT_OK(rs->DebugDump(lines));
  }

  return Status::OK();
}

Status Tablet::CaptureConsistentIterators(
  const Schema *projection,
  const MvccSnapshot &snap,
  const ScanSpec *spec,
  vector<shared_ptr<RowwiseIterator> > *iters) const {
  boost::shared_lock<rw_spinlock> lock(component_lock_);

  // Construct all the iterators locally first, so that if we fail
  // in the middle, we don't modify the output arguments.
  vector<shared_ptr<RowwiseIterator> > ret;

  // Grab the memrowset iterator.
  shared_ptr<RowwiseIterator> ms_iter(
    components_->memrowset->NewRowIterator(projection, snap));
  ret.push_back(ms_iter);

  // We can only use this optimization if there is a single encoded predicate
  // TODO : should we even support multiple predicates on the key, given they're
  // currently ANDed together? This should be the job for a separate query
  // optimizer.
  if (spec != NULL && spec->lower_bound_key() && spec->upper_bound_key()) {
    // TODO : support open-ended intervals
    vector<RowSet *> interval_sets;
    components_->rowsets->FindRowSetsIntersectingInterval(spec->lower_bound_key()->encoded_key(),
                                                          spec->upper_bound_key()->encoded_key(),
                                                          &interval_sets);
    BOOST_FOREACH(const RowSet *rs, interval_sets) {
      shared_ptr<RowwiseIterator> row_it(rs->NewRowIterator(projection, snap));
      ret.push_back(row_it);
    }
    ret.swap(*iters);
    return Status::OK();
  }

  // If there are no encoded predicates or they represent an open-ended range, then
  // fall back to grabbing all rowset iterators
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, components_->rowsets->all_rowsets()) {
    shared_ptr<RowwiseIterator> row_it(rs->NewRowIterator(projection, snap));
    ret.push_back(row_it);
  }

  // Swap results into the parameters.
  ret.swap(*iters);
  return Status::OK();
}

Status Tablet::CountRows(uint64_t *count) const {
  // First grab a consistent view of the components of the tablet.
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  // Now sum up the counts.
  *count = comps->memrowset->entry_count();
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, comps->rowsets->all_rowsets()) {
    rowid_t l_count;
    RETURN_NOT_OK(rowset->CountRows(&l_count));
    *count += l_count;
  }

  return Status::OK();
}

size_t Tablet::MemRowSetSize() const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  if (comps) {
    return comps->memrowset->memory_footprint();
  }
  return 0;
}

bool Tablet::MemRowSetEmpty() const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  return comps->memrowset->empty();
}

size_t Tablet::EstimateOnDiskSize() const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  size_t ret = 0;
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, comps->rowsets->all_rowsets()) {
    ret += rowset->EstimateOnDiskSize();
  }

  return ret;
}

size_t Tablet::DeltaMemStoresSize() const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  size_t ret = 0;
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, comps->rowsets->all_rowsets()) {
    ret += rowset->DeltaMemStoreSize();
  }

  return ret;
}

bool Tablet::DeltaMemRowSetEmpty() const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, comps->rowsets->all_rowsets()) {
    if (!rowset->DeltaMemStoreEmpty()) {
      return false;
    }
  }

  return true;
}

Status Tablet::FlushBiggestDMS() {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  int64_t max_size = -1;
  shared_ptr<RowSet> biggest_drs;
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, comps->rowsets->all_rowsets()) {
    int64_t current = rowset->DeltaMemStoreSize();
    if (current > max_size) {
      max_size = current;
      biggest_drs = rowset;
    }
  }
  return max_size > 0 ? biggest_drs->FlushDeltas() : Status::OK();
}

Status Tablet::MinorCompactWorstDeltas() {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  int worst_delta_count = -1;
  shared_ptr<RowSet> worst_rs;
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, comps->rowsets->all_rowsets()) {
    int count = rowset->CountDeltaStores();
    if (count > worst_delta_count) {
      worst_rs = rowset;
      worst_delta_count = count;
    }
  }

  if (worst_delta_count > 1) {
    RETURN_NOT_OK_PREPEND(worst_rs->MinorCompactDeltaStores(),
                          "Failed minor delta compaction on " + worst_rs->ToString());
  }
  return Status::OK();
}

size_t Tablet::num_rowsets() const {
  boost::shared_lock<rw_spinlock> lock(component_lock_);
  return components_->rowsets->all_rowsets().size();
}

void Tablet::PrintRSLayout(ostream* o) {
  shared_ptr<RowSetTree> rowsets_copy;
  {
    boost::shared_lock<rw_spinlock> lock(component_lock_);
    rowsets_copy = components_->rowsets;
  }
  // Run the compaction policy in order to get its log and highlight those
  // rowsets which would be compacted next.
  vector<string> log;
  unordered_set<RowSet*> picked;
  double quality;
  Status s = compaction_policy_->PickRowSets(*rowsets_copy, &picked, &quality, &log);
  if (!s.ok()) {
    *o << "<b>Error:</b> " << EscapeForHtmlToString(s.ToString());
    return;
  }

  if (!picked.empty()) {
    *o << "<p>";
    *o << "Highlighted rowsets indicate those that would be compacted next if a "
       << "compaction were to run on this tablet.";
    *o << "</p>";
  }

  vector<RowSetInfo> min, max;
  RowSetInfo::CollectOrdered(*rowsets_copy, &min, &max);
  DumpCompactionSVG(min, picked, o, false);

  *o << "<h2>Compaction policy log</h2>" << std::endl;

  *o << "<pre>" << std::endl;
  BOOST_FOREACH(const string& s, log) {
    *o << EscapeForHtmlToString(s) << std::endl;
  }
  *o << "</pre>" << std::endl;
}

Tablet::Iterator::Iterator(const Tablet *tablet,
                           const Schema &projection,
                           const MvccSnapshot &snap)
    : tablet_(tablet),
      projection_(projection),
      snap_(snap),
      encoder_(&tablet_->key_schema()) {
}

Tablet::Iterator::~Iterator() {}

Status Tablet::Iterator::Init(ScanSpec *spec) {
  DCHECK(iter_.get() == NULL);

  RETURN_NOT_OK(tablet_->GetMappedReadProjection(projection_, &projection_));

  vector<shared_ptr<RowwiseIterator> > iters;
  if (spec != NULL) {
    VLOG(3) << "Before encoding range preds: " << spec->ToString();
    encoder_.EncodeRangePredicates(spec, true);
    VLOG(3) << "After encoding range preds: " << spec->ToString();
  }

  RETURN_NOT_OK(tablet_->CaptureConsistentIterators(
      &projection_, snap_, spec, &iters));
  iter_.reset(new UnionIterator(iters));
  RETURN_NOT_OK(iter_->Init(spec));
  return Status::OK();
}

bool Tablet::Iterator::HasNext() const {
  DCHECK(iter_.get() != NULL) << "Not initialized!";
  return iter_->HasNext();
}

Status Tablet::Iterator::NextBlock(RowBlock *dst) {
  DCHECK(iter_.get() != NULL) << "Not initialized!";
  return iter_->NextBlock(dst);
}

string Tablet::Iterator::ToString() const {
  string s;
  s.append("tablet iterator: ");
  if (iter_.get() == NULL) {
    s.append("NULL");
  } else {
    s.append(iter_->ToString());
  }
  return s;
}

void Tablet::Iterator::GetIteratorStats(vector<IteratorStats>* stats) const {
  iter_->GetIteratorStats(stats);
}

} // namespace tablet
} // namespace kudu
