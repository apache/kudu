// Copyright (c) 2012, Cloudera, inc.
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

#include "cfile/cfile.h"
#include "common/iterator.h"
#include "common/row_changelist.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "consensus/consensus.pb.h"
#include "consensus/opid_anchor_registry.h"
#include "gutil/atomicops.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "tablet/compaction.h"
#include "tablet/compaction_policy.h"
#include "tablet/delta_compaction.h"
#include "tablet/diskrowset.h"
#include "tablet/maintenance_manager.h"
#include "tablet/tablet.h"
#include "tablet/tablet_metrics.h"
#include "tablet/rowset_info.h"
#include "tablet/rowset_tree.h"
#include "tablet/svg_dump.h"
#include "tablet/transactions/alter_schema_transaction.h"
#include "tablet/transactions/write_transaction.h"
#include "tablet/transactions/write_util.h"
#include "util/bloom_filter.h"
#include "util/env.h"
#include "util/locks.h"
#include "util/mem_tracker.h"
#include "util/metrics.h"

DEFINE_bool(tablet_do_dup_key_checks, true,
            "Whether to check primary keys for duplicate on insertion. "
            "Use at your own risk!");

DEFINE_string(tablet_compaction_policy, "budget",
              "Which compaction policy to use. Valid options are currently "
              "'size' or 'budget'");

DEFINE_int32(tablet_compaction_budget_mb, 128,
             "Budget for a single compaction, if the 'budget' compaction "
             "algorithm is selected");

DEFINE_int32(flush_threshold_mb, 64, "Minimum memrowset size to flush");


METRIC_DEFINE_gauge_uint64(memrowset_size, kudu::MetricUnit::kBytes,
                           "Size of this tablet's memrowset");

namespace kudu {
namespace tablet {

using kudu::MaintenanceManager;
using consensus::OpId;
using log::MaximumOpId;
using log::OpIdAnchorRegistry;
using metadata::RowSetMetadata;
using metadata::RowSetMetadataIds;
using metadata::RowSetMetadataVector;
using metadata::TabletMetadata;
using metadata::ColumnIndexes;
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
  if (FLAGS_tablet_compaction_policy == "size") {
    return new SizeRatioCompactionPolicy();
  } else if (FLAGS_tablet_compaction_policy == "budget") {
    return new BudgetedCompactionPolicy(FLAGS_tablet_compaction_budget_mb);
  } else {
    LOG(FATAL) << "Unknown compaction policy: " << FLAGS_tablet_compaction_policy;
  }
  return NULL;
}

TabletComponents::TabletComponents(const shared_ptr<MemRowSet>& mrs,
                                   const shared_ptr<RowSetTree>& rs_tree)
  : memrowset(mrs),
    rowsets(rs_tree) {
}

Tablet::Tablet(const scoped_refptr<TabletMetadata>& metadata,
               const scoped_refptr<server::Clock>& clock,
               const MetricContext* parent_metric_context,
               OpIdAnchorRegistry* opid_anchor_registry)
  : schema_(new Schema(metadata->schema())),
    key_schema_(schema_->CreateKeyProjection()),
    metadata_(metadata),
    opid_anchor_registry_(opid_anchor_registry),
    mem_tracker_(MemTracker::CreateTracker(-1,
                                           Substitute("tablet-$0", tablet_id()),
                                           NULL)),
    rowsets_(new RowSetTree()),
    next_mrs_id_(0),
    clock_(clock),
    mvcc_(clock),
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
    Status s = DiskRowSet::Open(rowset_meta, opid_anchor_registry_, &rowset, mem_tracker_);
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
                                              opid_anchor_registry_,
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

Status Tablet::CheckRowInTablet(const tablet::RowSetKeyProbe& probe) const {
  if (!probe.encoded_key().InRange(Slice(metadata_->start_key()),
                                   Slice(metadata_->end_key()))) {
    return Status::NotFound(
        Substitute("Row not within tablet range. Tablet start key: '$0', end key: '$1'."
                   "Probe key: '$2'",
                   schema_->DebugEncodedRowKey(metadata_->start_key()),
                   schema_->DebugEncodedRowKey(metadata_->end_key()),
                   schema_->DebugEncodedRowKey(probe.encoded_key().encoded_key().ToString())));
  }
  return Status::OK();
}

Status Tablet::CreatePreparedInsert(const WriteTransactionState* tx_state,
                                    const ConstContiguousRow* row,
                                    gscoped_ptr<PreparedRowWrite>* row_write) {
  gscoped_ptr<tablet::RowSetKeyProbe> probe(new tablet::RowSetKeyProbe(*row));

  RETURN_NOT_OK(CheckRowInTablet(*probe));

  ScopedRowLock row_lock(&lock_manager_,
                         tx_state,
                         probe->encoded_key_slice(),
                         LockManager::LOCK_EXCLUSIVE);
  row_write->reset(new PreparedRowWrite(row, probe.Pass(), row_lock.Pass()));

  // when we have a more advanced lock manager, acquiring the row lock might fail
  // but for now always return OK.
  return Status::OK();
}

void Tablet::StartTransaction(WriteTransactionState* tx_state) {
  boost::shared_lock<rw_spinlock> lock(component_lock_);

  gscoped_ptr<ScopedTransaction> mvcc_tx;
  if (tx_state->external_consistency_mode() == COMMIT_WAIT) {
    mvcc_tx.reset(new ScopedTransaction(&mvcc_, true));
  } else {
    mvcc_tx.reset(new ScopedTransaction(&mvcc_));
  }
  tx_state->set_mvcc_tx(mvcc_tx.Pass());
  tx_state->set_tablet_components(components_);
}

Status Tablet::InsertForTesting(WriteTransactionState *tx_state,
                                const ConstContiguousRow& row) {
  CHECK(open_) << "must Open() first!";
  DCHECK(tx_state) << "you must have a transaction context";

  DCHECK_KEY_PROJECTION_SCHEMA_EQ(key_schema_, row.schema());

  // Convert the client row to a server row (with IDs)
  // TODO: We have now three places where we do the projection (RPC, Tablet, Bootstrap)
  //       One is the RPC side, the other is this method.
  DCHECK(!row.schema().has_column_ids());
  RowProjector row_projector(&row.schema(), schema_.get());
  if (!row_projector.is_identity()) {
    RETURN_NOT_OK(schema_->VerifyProjectionCompatibility(row.schema()));
    RETURN_NOT_OK(row_projector.Init());
  }
  const ConstContiguousRow* proj_row = ProjectRowForInsert(tx_state, schema_.get(),
                                                           row_projector, row.row_data());

  gscoped_ptr<PreparedRowWrite> row_write;
  RETURN_NOT_OK(CreatePreparedInsert(tx_state, proj_row, &row_write));
  tx_state->add_prepared_row(row_write.Pass());

  // Create a "fake" OpId and set it in the TransactionState for anchoring.
  tx_state->mutable_op_id()->CopyFrom(MaximumOpId());

  StartTransaction(tx_state);

  Status s = InsertUnlocked(tx_state, tx_state->rows()[0]);
  tx_state->commit();
  return s;
}

Status Tablet::InsertUnlocked(WriteTransactionState *tx_state,
                              const PreparedRowWrite* insert) {
  const TabletComponents* comps = DCHECK_NOTNULL(tx_state->tablet_components());

  CHECK(open_) << "must Open() first!";
  // make sure that the WriteTransactionState has the component lock and that
  // there the PreparedRowWrite has the row lock.
  DCHECK(insert->has_row_lock()) << "PreparedRowWrite must hold the row lock.";
  DCHECK_KEY_PROJECTION_SCHEMA_EQ(key_schema_, insert->row()->schema());
  DCHECK(tx_state->op_id().IsInitialized()) << "TransactionState OpId needed for anchoring";

  ProbeStats stats;

  // Submit the stats before returning from this function
  ProbeStatsSubmitter submitter(stats, metrics_.get());

  // First, ensure that it is a unique key by checking all the open RowSets.
  if (FLAGS_tablet_do_dup_key_checks) {
    vector<RowSet *> to_check;
    comps->rowsets->FindRowSetsWithKeyInRange(insert->probe()->encoded_key_slice(), &to_check);

    BOOST_FOREACH(const RowSet *rowset, to_check) {
      bool present = false;
      RETURN_NOT_OK(rowset->CheckRowPresent(*insert->probe(), &present, &stats));
      if (PREDICT_FALSE(present)) {
        Status s = Status::AlreadyPresent("key already present");
        if (metrics_) {
          metrics_->insertions_failed_dup_key->Increment();
        }
        tx_state->AddFailedOperation(s);
        return s;
      }
    }
  }

  Timestamp ts = tx_state->timestamp();

  // TODO: the Insert() call below will re-encode the key, which is a
  // waste. Should pass through the KeyProbe structure perhaps.

  // Now try to insert into memrowset. The memrowset itself will return
  // AlreadyPresent if it has already been inserted there.
  Status s = comps->memrowset->Insert(ts, *insert->row(), tx_state->op_id());
  if (PREDICT_TRUE(s.ok())) {
    RETURN_NOT_OK(tx_state->AddInsert(ts, comps->memrowset->mrs_id()));
  } else {
    if (s.IsAlreadyPresent() && metrics_) {
      metrics_->insertions_failed_dup_key->Increment();
    }
    tx_state->AddFailedOperation(s);
  }
  return s;
}

Status Tablet::CreatePreparedMutate(const WriteTransactionState* tx_state,
                                    const ConstContiguousRow* row_key,
                                    const RowChangeList& changelist,
                                    gscoped_ptr<PreparedRowWrite>* row_write) {
  gscoped_ptr<tablet::RowSetKeyProbe> probe(new tablet::RowSetKeyProbe(*row_key));

  RETURN_NOT_OK(CheckRowInTablet(*probe));

  ScopedRowLock row_lock(&lock_manager_,
                         tx_state,
                         probe->encoded_key_slice(),
                         LockManager::LOCK_EXCLUSIVE);
  row_write->reset(new PreparedRowWrite(row_key, changelist, probe.Pass(), row_lock.Pass()));

  // when we have a more advanced lock manager, acquiring the row lock might fail
  // but for now always return OK.
  return Status::OK();
}

Status Tablet::MutateRowForTesting(WriteTransactionState *tx_state,
                                   const ConstContiguousRow& row_key,
                                   const Schema& update_schema,
                                   const RowChangeList& update) {
  // TODO: use 'probe' when calling UpdateRow on each rowset.
  DCHECK_SCHEMA_EQ(key_schema_, row_key.schema());
  DCHECK_KEY_PROJECTION_SCHEMA_EQ(key_schema_, update_schema);
  DCHECK(tx_state) << "you must have a transaction context";
  CHECK(tx_state->rows().empty()) << "WriteTransactionState must have no PreparedRowWrites.";

  // Convert the client RowChangeList to a server RowChangeList (with IDs)
  // TODO: We have now three places where we do the projection (RPC, Tablet, Bootstrap)
  //       One is the RPC side, the other is this method that should be renamed MutateForTesting()
  DCHECK(!update_schema.has_column_ids());
  DeltaProjector delta_projector(&update_schema, schema_.get());
  if (!delta_projector.is_identity()) {
    RETURN_NOT_OK(schema_->VerifyProjectionCompatibility(update_schema));
    RETURN_NOT_OK(update_schema.GetProjectionMapping(*schema_.get(), &delta_projector));
  }

  RowChangeList changelist = ProjectMutation(tx_state, delta_projector, update);

  gscoped_ptr<PreparedRowWrite> row_write;
  RETURN_NOT_OK(CreatePreparedMutate(tx_state, &row_key, changelist, &row_write));
  tx_state->add_prepared_row(row_write.Pass());

  StartTransaction(tx_state);

  // Create a "fake" OpId and set it in the TransactionState for anchoring.
  tx_state->mutable_op_id()->CopyFrom(MaximumOpId());

  Status s = MutateRowUnlocked(tx_state, tx_state->rows()[0]);
  tx_state->commit();
  return s;
}

Status Tablet::MutateRowUnlocked(WriteTransactionState *tx_state,
                                 const PreparedRowWrite* mutate) {
  DCHECK(tx_state != NULL) << "you must have a WriteTransactionState";
  DCHECK(tx_state->op_id().IsInitialized()) << "TransactionState OpId needed for anchoring";

  gscoped_ptr<OperationResultPB> result(new OperationResultPB());

  const TabletComponents* comps = DCHECK_NOTNULL(tx_state->tablet_components());

  // Validate the update.
  RowChangeListDecoder rcl_decoder(*schema_.get(), mutate->changelist());
  Status s = rcl_decoder.Init();
  if (rcl_decoder.is_reinsert()) {
    // REINSERT mutations are the byproduct of an INSERT on top of a ghost
    // row, not something the user is allowed to specify on their own.
    s = Status::InvalidArgument("User may not specify REINSERT mutations");
  }
  if (!s.ok()) {
    tx_state->AddFailedOperation(s);
    return s;
  }


  Timestamp ts = tx_state->timestamp();

  ProbeStats stats;
  // Submit the stats before returning from this function
  ProbeStatsSubmitter submitter(stats, metrics_.get());

  // First try to update in memrowset.
  s = comps->memrowset->MutateRow(ts,
                            *mutate->probe(),
                            mutate->changelist(),
                            tx_state->op_id(),
                            &stats,
                            result.get());
  if (s.ok()) {
    RETURN_NOT_OK(tx_state->AddMutation(ts, result.Pass()));
    return s;
  }
  if (!s.IsNotFound()) {
    tx_state->AddFailedOperation(s);
    return s;
  }

  // Next, check the disk rowsets.

  // TODO: could iterate the rowsets in a smart order
  // based on recent statistics - eg if a rowset is getting
  // updated frequently, pick that one first.
  vector<RowSet *> to_check;
  comps->rowsets->FindRowSetsWithKeyInRange(mutate->probe()->encoded_key_slice(), &to_check);
  BOOST_FOREACH(RowSet *rs, to_check) {
    s = rs->MutateRow(ts, *mutate->probe(), mutate->changelist(), tx_state->op_id(),
                      &stats, result.get());
    if (s.ok()) {
      RETURN_NOT_OK(tx_state->AddMutation(ts, result.Pass()));
      return s;
    }
    if (!s.IsNotFound()) {
      tx_state->AddFailedOperation(s);
      return s;
    }
  }

  s = Status::NotFound("key not found");
  tx_state->AddFailedOperation(s);
  return s;
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
  //BOOST_FOREACH(const shared_ptr<RowSet> &l_input, input.rowsets()) {
  //  LOG(INFO) << "Removing compaction input rowset " << l_input->ToString();
  //}
  return Status::OK();
}

Status Tablet::Flush() {
  boost::lock_guard<boost::mutex> lock(rowsets_flush_lock_);
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

  shared_ptr<MemRowSet> new_mrs(new MemRowSet(next_mrs_id_++, schema, opid_anchor_registry_,
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
  tx_state->set_schema(schema);
  return Status::OK();
}

Status Tablet::AlterSchema(AlterSchemaTransactionState *tx_state) {
  DCHECK(key_schema_.KeyEquals(*DCHECK_NOTNULL(tx_state->schema()))) <<
    "Schema keys cannot be altered";

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

  // TODO: release a schema lock?

  // Flush the old MemRowSet
  boost::lock_guard<boost::mutex> lock(rowsets_flush_lock_);
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

class FlushRowSetsOp : public MaintenanceOp {
 public:
  explicit FlushRowSetsOp(Tablet* tablet)
    : MaintenanceOp(StringPrintf("FlushRowSetsOp(%s)", tablet->tablet_id().c_str())),
      tablet_(tablet)
  { }

  virtual void UpdateStats(MaintenanceOpStats* stats) OVERRIDE {
    {
      boost::mutex::scoped_try_lock guard(tablet_->rowsets_flush_lock_);
      stats->runnable = guard.owns_lock();
    }
    stats->ram_anchored = tablet_->MemRowSetSize();
    // TODO: add a field to MemRowSet storing how old a timestamp it contains
    stats->ts_anchored_secs = 0;
    // TODO: use workload statistics here to find out how "hot" the tablet has
    // been in the last 5 minutes.
    if (stats->ram_anchored > FLAGS_flush_threshold_mb * 1024 * 1024) {
      int extra_mb = stats->ram_anchored / 1024 / 1024;
      stats->perf_improvement = extra_mb;
    } else {
      stats->perf_improvement = 0;
    }
  }

  virtual bool Prepare() OVERRIDE {
    // Try to acquire the rowsets_flush_lock_.  If we can't, the Prepare step
    // fails.  This also implies that only one instance of FlushRowSetsOp can be
    // running at once.
    return tablet_->rowsets_flush_lock_.try_lock();
  }

  virtual void Perform() OVERRIDE {
    tablet_->FlushUnlocked();
    return tablet_->rowsets_flush_lock_.unlock();
  }

 private:
  Tablet *const tablet_;
};

class CompactRowSetsOp : public MaintenanceOp {
 public:
  explicit CompactRowSetsOp(Tablet* tablet)
    : MaintenanceOp(StringPrintf("CompactRowSetsOp(%s)", tablet->tablet_id().c_str())),
      last_(MonoTime::Now(MonoTime::FINE)),
      tablet_(tablet),
      compact_running_(0)
  { }

  virtual void UpdateStats(MaintenanceOpStats* stats) OVERRIDE {
    stats->runnable = true;
    stats->ram_anchored = 0;
    stats->ts_anchored_secs = 0;

    // TODO: use workload statistics here to find out how "hot" the tablet has
    // been in the last 5 minutes.  For now, we just set perf_improvement to 0
    // if the tablet has been compacted in the last 5 seconds.
    MonoTime now(MonoTime::Now(MonoTime::FINE));
    MonoDelta delta(now.GetDeltaSince(last()));
    int64_t deltaMs = delta.ToMilliseconds();
    if (deltaMs < 5000) {
      stats->perf_improvement = 0;
    } else {
      stats->perf_improvement = deltaMs / 10;
    }

    // Reduce perf_improvement stat if there is another rowset compaction
    // already running on this tablet.
    stats->perf_improvement /= (compact_running_ + 1);
  }

  virtual bool Prepare() OVERRIDE {
    compact_running_ = running();
    return true;
  }

  virtual void Perform() OVERRIDE {
    tablet_->Compact(Tablet::COMPACT_NO_FLAGS);
    set_last(MonoTime::Now(MonoTime::FINE));
  }

 private:
  MonoTime last() const {
    boost::lock_guard<simple_spinlock> l(lock_);
    return last_;
  }

  void set_last(const MonoTime& last) {
    boost::lock_guard<simple_spinlock> l(lock_);
    last_ = last;
  }

  mutable simple_spinlock lock_;
  MonoTime last_;
  Tablet *const tablet_;
  uint32_t compact_running_;
};

class FlushDeltaMemStoresOp : public MaintenanceOp {
 public:
  explicit FlushDeltaMemStoresOp(Tablet* tablet)
    : MaintenanceOp(StringPrintf("FlushDeltaMemStoresOp(%s)",
                                 tablet->tablet_id().c_str())),
      tablet_(tablet)
  { }

  virtual void UpdateStats(MaintenanceOpStats* stats) OVERRIDE {
    size_t dms_size = tablet_->DeltaMemStoresSize();
    uint64_t threshold = FLAGS_flush_threshold_mb * 1024LLU * 1024LLU;
    if (dms_size < threshold) {
      stats->perf_improvement = 0;
    } else {
      stats->perf_improvement = (2.0f * dms_size) / threshold;
    }
    stats->ram_anchored = dms_size;
    stats->ts_anchored_secs = 0;
  }

  virtual bool Prepare() OVERRIDE {
    return true;
  }

  virtual void Perform() OVERRIDE {
    tablet_->FlushBiggestDMS();
  }

 private:
  Tablet *const tablet_;
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
    RETURN_NOT_OK(compaction_policy_->PickRowSets(*rowsets_copy, &picked_set));
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
  gscoped_ptr<MaintenanceOp> mrs_flush_op(new FlushRowSetsOp(this));
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
    // Skip MemRowSet & DuplicatingRowSets which don't have metadata
    if (rowset->metadata().get() == NULL) continue;
    to_remove_meta.insert(rowset->metadata()->id());
  }

  // If we're flushing an mrs update the latest durable one in the metadata
  if (mrs_being_flushed != kNoMrsFlushed) {
    return metadata_->UpdateAndFlush(to_remove_meta, to_add, mrs_being_flushed, NULL);
  }
  return metadata_->UpdateAndFlush(to_remove_meta, to_add, NULL);
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

    // Remove old rowsets.
    // TODO: Consensus catch-up may want to reserve the compaction inputs.
    WARN_NOT_OK(DeleteCompactionInputs(input),
        Substitute("Unable to remove $0 inputs. Will GC later.", op_name));

    // Write out the new Tablet Metadata
    return FlushMetadata(input.rowsets(), RowSetMetadataVector(), mrs_being_flushed);
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
    Status s = DiskRowSet::Open(meta, opid_anchor_registry_, &new_rowset, mem_tracker_);
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
  if (spec != NULL && spec->encoded_ranges().size() == 1) {
    const EncodedKeyRange &range = *(spec->encoded_ranges()[0]);
    // TODO : support open-ended intervals
    if (range.has_lower_bound() && range.has_upper_bound()) {
      vector<RowSet *> interval_sets;
      components_->rowsets->FindRowSetsIntersectingInterval(range.lower_bound().encoded_key(),
                                                range.upper_bound().encoded_key(),
                                                &interval_sets);
      BOOST_FOREACH(const RowSet *rs, interval_sets) {
        shared_ptr<RowwiseIterator> row_it(rs->NewRowIterator(projection, snap));
        ret.push_back(row_it);
      }
      ret.swap(*iters);
      return Status::OK();
    }
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

  return comps->memrowset->memory_footprint();
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

void Tablet::PrintRSLayout(ostream* o, bool header) {
  shared_ptr<RowSetTree> rowsets_copy;
  {
    boost::shared_lock<rw_spinlock> lock(component_lock_);
    rowsets_copy = components_->rowsets;
  }
  // Simulate doing a compaction with no rowsets chosen to simply display
  // the current layout
  vector<RowSetInfo> min, max;
  RowSetInfo::CollectOrdered(*rowsets_copy, &min, &max);
  unordered_set<RowSet*> picked;
  DumpCompactionSVG(min, picked, o, header);
}

Tablet::Iterator::Iterator(const Tablet *tablet,
                           const Schema &projection,
                           const MvccSnapshot &snap)
    : tablet_(tablet),
      projection_(projection),
      snap_(snap),
      encoder_(tablet_->key_schema()) {
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
