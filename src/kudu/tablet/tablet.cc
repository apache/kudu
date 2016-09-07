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

#include "kudu/tablet/tablet.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <unordered_set>
#include <utility>
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
#include "kudu/server/hybrid_clock.h"
#include "kudu/tablet/compaction.h"
#include "kudu/tablet/compaction_policy.h"
#include "kudu/tablet/delta_compaction.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/row_op.h"
#include "kudu/tablet/rowset_info.h"
#include "kudu/tablet/rowset_tree.h"
#include "kudu/tablet/svg_dump.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet_mm_ops.h"
#include "kudu/tablet/transactions/alter_schema_transaction.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/util/bloom_filter.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/env.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/trace.h"
#include "kudu/util/url-coding.h"

DEFINE_int32(tablet_compaction_budget_mb, 128,
             "Budget for a single compaction");
TAG_FLAG(tablet_compaction_budget_mb, experimental);

DEFINE_int32(tablet_bloom_block_size, 4096,
             "Block size of the bloom filters used for tablet keys.");
TAG_FLAG(tablet_bloom_block_size, advanced);

DEFINE_double(tablet_bloom_target_fp_rate, 0.0001f,
              "Target false-positive rate (between 0 and 1) to size tablet key bloom filters. "
              "A lower false positive rate may reduce the number of disk seeks required "
              "in heavy insert workloads, at the expense of more space and RAM "
              "required for bloom filters.");
TAG_FLAG(tablet_bloom_target_fp_rate, advanced);


DEFINE_double(fault_crash_before_flush_tablet_meta_after_compaction, 0.0,
              "Fraction of the time, during compaction, to crash before flushing metadata");
TAG_FLAG(fault_crash_before_flush_tablet_meta_after_compaction, unsafe);

DEFINE_double(fault_crash_before_flush_tablet_meta_after_flush_mrs, 0.0,
              "Fraction of the time, while flushing an MRS, to crash before flushing metadata");
TAG_FLAG(fault_crash_before_flush_tablet_meta_after_flush_mrs, unsafe);

DEFINE_int64(tablet_throttler_rpc_per_sec, 0,
             "Maximum write RPC rate (op/s) allowed for a tablet, write RPC exceeding this "
             "limit will be throttled. 0 means no limit.");
TAG_FLAG(tablet_throttler_rpc_per_sec, experimental);

DEFINE_int64(tablet_throttler_bytes_per_sec, 0,
             "Maximum write RPC IO rate (byte/s) allowed for a tablet, write RPC exceeding "
             "this limit will be throttled. 0 means no limit.");
TAG_FLAG(tablet_throttler_bytes_per_sec, experimental);

DEFINE_double(tablet_throttler_burst_factor, 1.0f,
             "Burst factor for write RPC throttling. The maximum rate the throttler "
             "allows within a token refill period (100ms) equals burst factor multiply "
             "base rate.");
TAG_FLAG(tablet_throttler_burst_factor, experimental);

DEFINE_int32(tablet_history_max_age_sec, 15 * 60,
             "Number of seconds to retain tablet history. Reads initiated at a "
             "snapshot that is older than this age will be rejected. "
             "To disable history removal, set to -1.");
TAG_FLAG(tablet_history_max_age_sec, advanced);

METRIC_DEFINE_entity(tablet);
METRIC_DEFINE_gauge_size(tablet, memrowset_size, "MemRowSet Memory Usage",
                         kudu::MetricUnit::kBytes,
                         "Size of this tablet's memrowset");
METRIC_DEFINE_gauge_size(tablet, on_disk_size, "Tablet Size On Disk",
                         kudu::MetricUnit::kBytes,
                         "Size of this tablet on disk.");

using base::subtle::Barrier_AtomicIncrement;
using kudu::MaintenanceManager;
using kudu::consensus::OpId;
using kudu::consensus::MaximumOpId;
using kudu::log::LogAnchorRegistry;
using kudu::server::HybridClock;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

static CompactionPolicy *CreateCompactionPolicy() {
  return new BudgetedCompactionPolicy(FLAGS_tablet_compaction_budget_mb);
}

////////////////////////////////////////////////////////////
// TabletComponents
////////////////////////////////////////////////////////////

TabletComponents::TabletComponents(shared_ptr<MemRowSet> mrs,
                                   shared_ptr<RowSetTree> rs_tree)
    : memrowset(std::move(mrs)), rowsets(std::move(rs_tree)) {}

////////////////////////////////////////////////////////////
// Tablet
////////////////////////////////////////////////////////////

const char* Tablet::kDMSMemTrackerId = "DeltaMemStores";

Tablet::Tablet(const scoped_refptr<TabletMetadata>& metadata,
               const scoped_refptr<server::Clock>& clock,
               const shared_ptr<MemTracker>& parent_mem_tracker,
               MetricRegistry* metric_registry,
               const scoped_refptr<LogAnchorRegistry>& log_anchor_registry)
  : key_schema_(metadata->schema().CreateKeyProjection()),
    metadata_(metadata),
    log_anchor_registry_(log_anchor_registry),
    mem_tracker_(MemTracker::CreateTracker(
        -1, Substitute("tablet-$0", tablet_id()),
                       parent_mem_tracker)),
    dms_mem_tracker_(MemTracker::CreateTracker(
        -1, kDMSMemTrackerId, mem_tracker_)),
    next_mrs_id_(0),
    clock_(clock),
    mvcc_(clock),
    rowsets_flush_sem_(1),
    state_(kInitialized) {
      CHECK(schema()->has_column_ids());
  compaction_policy_.reset(CreateCompactionPolicy());

  if (metric_registry) {
    MetricEntity::AttributeMap attrs;
    // TODO(KUDU-745): table_id is apparently not set in the metadata.
    attrs["table_id"] = metadata_->table_id();
    attrs["table_name"] = metadata_->table_name();
    attrs["partition"] = metadata_->partition_schema().PartitionDebugString(metadata_->partition(),
                                                                            *schema());
    metric_entity_ = METRIC_ENTITY_tablet.Instantiate(metric_registry, tablet_id(), attrs);
    metrics_.reset(new TabletMetrics(metric_entity_));
    METRIC_memrowset_size.InstantiateFunctionGauge(
      metric_entity_, Bind(&Tablet::MemRowSetSize, Unretained(this)))
      ->AutoDetach(&metric_detacher_);
    METRIC_on_disk_size.InstantiateFunctionGauge(
      metric_entity_, Bind(&Tablet::EstimateOnDiskSize, Unretained(this)))
      ->AutoDetach(&metric_detacher_);
  }

  if (FLAGS_tablet_throttler_rpc_per_sec > 0 || FLAGS_tablet_throttler_bytes_per_sec > 0) {
    throttler_.reset(new Throttler(MonoTime::Now(),
                                   FLAGS_tablet_throttler_rpc_per_sec,
                                   FLAGS_tablet_throttler_bytes_per_sec,
                                   FLAGS_tablet_throttler_burst_factor));
  }
}

Tablet::~Tablet() {
  Shutdown();
}

Status Tablet::Open() {
  TRACE_EVENT0("tablet", "Tablet::Open");
  std::lock_guard<rw_spinlock> lock(component_lock_);
  CHECK_EQ(state_, kInitialized) << "already open";
  CHECK(schema()->has_column_ids());

  next_mrs_id_ = metadata_->last_durable_mrs_id() + 1;

  RowSetVector rowsets_opened;

  // open the tablet row-sets
  for (const shared_ptr<RowSetMetadata>& rowset_meta : metadata_->rowsets()) {
    shared_ptr<DiskRowSet> rowset;
    Status s = DiskRowSet::Open(rowset_meta, log_anchor_registry_.get(), &rowset, mem_tracker_);
    if (!s.ok()) {
      LOG_WITH_PREFIX(ERROR) << "Failed to open rowset " << rowset_meta->ToString() << ": "
                             << s.ToString();
      return s;
    }

    rowsets_opened.push_back(rowset);
  }

  shared_ptr<RowSetTree> new_rowset_tree(new RowSetTree());
  CHECK_OK(new_rowset_tree->Reset(rowsets_opened));
  // now that the current state is loaded, create the new MemRowSet with the next id
  shared_ptr<MemRowSet> new_mrs(new MemRowSet(next_mrs_id_++, *schema(),
                                              log_anchor_registry_.get(),
                                              mem_tracker_));
  components_ = new TabletComponents(new_mrs, new_rowset_tree);

  state_ = kBootstrapping;
  return Status::OK();
}

void Tablet::MarkFinishedBootstrapping() {
  CHECK_EQ(state_, kBootstrapping);
  state_ = kOpen;
}

void Tablet::Shutdown() {
  UnregisterMaintenanceOps();

  std::lock_guard<rw_spinlock> lock(component_lock_);
  components_ = nullptr;
  state_ = kShutdown;

  // In the case of deleting a tablet, we still keep the metadata around after
  // ShutDown(), and need to flush the metadata to indicate that the tablet is deleted.
  // During that flush, we don't want metadata to call back into the Tablet, so we
  // have to unregister the pre-flush callback.
  metadata_->SetPreFlushCallback(Bind(DoNothingStatusClosure));
}

Status Tablet::GetMappedReadProjection(const Schema& projection,
                                       Schema *mapped_projection) const {
  const Schema* cur_schema = schema();
  return cur_schema->GetMappedReadProjection(projection, mapped_projection);
}

BloomFilterSizing Tablet::bloom_sizing() const {
  return BloomFilterSizing::BySizeAndFPRate(FLAGS_tablet_bloom_block_size,
                                            FLAGS_tablet_bloom_target_fp_rate);
}

Status Tablet::NewRowIterator(const Schema &projection,
                              gscoped_ptr<RowwiseIterator> *iter) const {
  // Yield current rows.
  MvccSnapshot snap(mvcc_);
  return NewRowIterator(projection, snap, Tablet::UNORDERED, iter);
}


Status Tablet::NewRowIterator(const Schema &projection,
                              const MvccSnapshot &snap,
                              const OrderMode order,
                              gscoped_ptr<RowwiseIterator> *iter) const {
  CHECK_EQ(state_, kOpen);
  if (metrics_) {
    metrics_->scans_started->Increment();
  }
  VLOG_WITH_PREFIX(2) << "Created new Iterator under snap: " << snap.ToString();
  iter->reset(new Iterator(this, projection, snap, order));
  return Status::OK();
}

Status Tablet::DecodeWriteOperations(const Schema* client_schema,
                                     WriteTransactionState* tx_state) {
  TRACE_EVENT0("tablet", "Tablet::DecodeWriteOperations");

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
                             schema(),
                             tx_state->arena());
  RETURN_NOT_OK(dec.DecodeOperations(&ops));
  TRACE_COUNTER_INCREMENT("num_ops", ops.size());

  // Create RowOp objects for each
  vector<RowOp*> row_ops;
  row_ops.reserve(ops.size());
  for (const DecodedRowOperation& op : ops) {
    row_ops.push_back(new RowOp(op));
  }

  // Important to set the schema before the ops -- we need the
  // schema in order to stringify the ops.
  tx_state->set_schema_at_decode_time(schema());
  tx_state->swap_row_ops(&row_ops);

  return Status::OK();
}

Status Tablet::AcquireRowLocks(WriteTransactionState* tx_state) {
  TRACE_EVENT1("tablet", "Tablet::AcquireRowLocks",
               "num_locks", tx_state->row_ops().size());
  TRACE("PREPARE: Acquiring locks for $0 operations", tx_state->row_ops().size());
  for (RowOp* op : tx_state->row_ops()) {
    RETURN_NOT_OK(AcquireLockForOp(tx_state, op));
  }
  TRACE("PREPARE: locks acquired");
  return Status::OK();
}

Status Tablet::CheckRowInTablet(const ConstContiguousRow& row) const {
  bool contains_row;
  RETURN_NOT_OK(metadata_->partition_schema().PartitionContainsRow(metadata_->partition(),
                                                                   row,
                                                                   &contains_row));

  if (PREDICT_FALSE(!contains_row)) {
    return Status::NotFound(
        Substitute("Row not in tablet partition. Partition: '$0', row: '$1'.",
                   metadata_->partition_schema().PartitionDebugString(metadata_->partition(),
                                                                      *schema()),
                   metadata_->partition_schema().RowDebugString(row)));
  }
  return Status::OK();
}

Status Tablet::AcquireLockForOp(WriteTransactionState* tx_state, RowOp* op) {
  ConstContiguousRow row_key(&key_schema_, op->decoded_op.row_data);
  op->key_probe.reset(new tablet::RowSetKeyProbe(row_key));
  RETURN_NOT_OK(CheckRowInTablet(row_key));

  op->row_lock = ScopedRowLock(&lock_manager_,
                               tx_state,
                               op->key_probe->encoded_key_slice(),
                               LockManager::LOCK_EXCLUSIVE);
  return Status::OK();
}

void Tablet::StartTransaction(WriteTransactionState* tx_state) {
  gscoped_ptr<ScopedTransaction> mvcc_tx;

  // If the state already has a timestamp then we're replaying a transaction that occurred
  // before a crash or at another node...
  if (tx_state->has_timestamp()) {
    mvcc_tx.reset(new ScopedTransaction(&mvcc_, tx_state->timestamp()));

  // ... otherwise this is a new transaction and we must assign a new timestamp. We either
  // assign a timestamp in the future, if the consistency mode is COMMIT_WAIT, or we assign
  // one in the present if the consistency mode is any other one.
  } else if (tx_state->external_consistency_mode() == COMMIT_WAIT) {
    mvcc_tx.reset(new ScopedTransaction(&mvcc_, ScopedTransaction::NOW_LATEST));
  } else {
    mvcc_tx.reset(new ScopedTransaction(&mvcc_, ScopedTransaction::NOW));
  }
  tx_state->SetMvccTxAndTimestamp(std::move(mvcc_tx));
}

Status Tablet::InsertOrUpsertUnlocked(WriteTransactionState *tx_state,
                                      RowOp* op,
                                      ProbeStats* stats) {
  const bool is_upsert = op->decoded_op.type == RowOperationsPB::UPSERT;
  const TabletComponents* comps = DCHECK_NOTNULL(tx_state->tablet_components());

  // First, ensure that it is a unique key by checking all the open RowSets.
  vector<RowSet *> to_check = FindRowSetsToCheck(op, comps);
  for (RowSet *rowset : to_check) {
    bool present = false;
    RETURN_NOT_OK(rowset->CheckRowPresent(*op->key_probe, &present, stats));
    if (present) {
      if (is_upsert) {
        return ApplyUpsertAsUpdate(tx_state, op, rowset, stats);
      }
      Status s = Status::AlreadyPresent("key already present");
      if (metrics_) {
        metrics_->insertions_failed_dup_key->Increment();
      }
      op->SetFailed(s);
      return s;
    }
  }

  Timestamp ts = tx_state->timestamp();
  ConstContiguousRow row(schema(), op->decoded_op.row_data);

  // TODO: the Insert() call below will re-encode the key, which is a
  // waste. Should pass through the KeyProbe structure perhaps.

  // Now try to op into memrowset. The memrowset itself will return
  // AlreadyPresent if it has already been oped there.
  Status s = comps->memrowset->Insert(ts, row, tx_state->op_id());
  if (s.ok()) {
    op->SetInsertSucceeded(comps->memrowset->mrs_id());
  } else {
    if (s.IsAlreadyPresent()) {
      if (is_upsert) {
        return ApplyUpsertAsUpdate(tx_state, op, comps->memrowset.get(), stats);
      }
      if (metrics_) {
        metrics_->insertions_failed_dup_key->Increment();
      }
    }
    op->SetFailed(s);
  }
  return s;
}

Status Tablet::ApplyUpsertAsUpdate(WriteTransactionState* tx_state,
                                   RowOp* upsert,
                                   RowSet* rowset,
                                   ProbeStats* stats) {
  const auto* schema = this->schema();
  ConstContiguousRow row(schema, upsert->decoded_op.row_data);
  faststring buf;
  RowChangeListEncoder enc(&buf);
  for (int i = 0; i < schema->num_columns(); i++) {
    if (schema->is_key_column(i)) continue;

    // If the user didn't explicitly set this column in the UPSERT, then we should
    // not turn it into an UPDATE. This prevents the UPSERT from updating
    // values back to their defaults when unset.
    if (!BitmapTest(upsert->decoded_op.isset_bitmap, i)) continue;
    const auto& c = schema->column(i);
    const void* val = c.is_nullable() ? row.nullable_cell_ptr(i) : row.cell_ptr(i);
    enc.AddColumnUpdate(c, schema->column_id(i), val);
  }

  // If the UPSERT just included the primary key columns, and the rest
  // were unset (eg because the table only _has_ primary keys, or because
  // the rest are intended to be set to their defaults), we need to
  // avoid doing anything.
  gscoped_ptr<OperationResultPB> result(new OperationResultPB());
  if (enc.is_empty()) {
    upsert->SetMutateSucceeded(std::move(result));
    return Status::OK();
  }

  RowChangeList rcl = enc.as_changelist();

  Status s = rowset->MutateRow(tx_state->timestamp(),
                               *upsert->key_probe,
                               rcl,
                               tx_state->op_id(),
                               stats,
                               result.get());
  CHECK(!s.IsNotFound());
  if (s.ok()) {
    upsert->SetMutateSucceeded(std::move(result));
  } else {
    upsert->SetFailed(s);
  }
  return s;
}

vector<RowSet*> Tablet::FindRowSetsToCheck(RowOp* op,
                                           const TabletComponents* comps) {
  vector<RowSet*> to_check;
  if (PREDICT_TRUE(!op->orig_result_from_log_)) {
    // TODO: could iterate the rowsets in a smart order
    // based on recent statistics - eg if a rowset is getting
    // updated frequently, pick that one first.
    comps->rowsets->FindRowSetsWithKeyInRange(op->key_probe->encoded_key_slice(),
                                              &to_check);
#ifndef NDEBUG
    // The order in which the rowset tree returns its results doesn't have semantic
    // relevance. We've had bugs in the past (eg KUDU-1341) which were obscured by
    // relying on the order of rowsets here. So, in debug builds, we shuffle the
    // order to encourage finding such bugs more easily.
    std::random_shuffle(to_check.begin(), to_check.end());
#endif
    return to_check;
  }

  // If we are replaying an operation during bootstrap, then we already have a
  // COMMIT message which tells us specifically which memory store to apply it to.
  for (const auto& store : op->orig_result_from_log_->mutated_stores()) {
    if (store.has_mrs_id()) {
      to_check.push_back(comps->memrowset.get());
    } else {
      DCHECK(store.has_rs_id());
      RowSet* drs = comps->rowsets->drs_by_id(store.rs_id());
      if (PREDICT_TRUE(drs)) {
        to_check.push_back(drs);
      }

      // If for some reason we didn't find any stores that the COMMIT message indicated,
      // then 'to_check' will be empty at this point. That will result in a NotFound()
      // status below, which the bootstrap code catches and propagates as a tablet
      // corruption.
    }
  }
  return to_check;
}

Status Tablet::MutateRowUnlocked(WriteTransactionState *tx_state,
                                 RowOp* mutate,
                                 ProbeStats* stats) {
  gscoped_ptr<OperationResultPB> result(new OperationResultPB());

  const TabletComponents* comps = DCHECK_NOTNULL(tx_state->tablet_components());

  // Validate the update.
  RowChangeListDecoder rcl_decoder(mutate->decoded_op.changelist);
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

  // First try to update in memrowset.
  s = comps->memrowset->MutateRow(ts,
                            *mutate->key_probe,
                            mutate->decoded_op.changelist,
                            tx_state->op_id(),
                            stats,
                            result.get());
  if (s.ok()) {
    mutate->SetMutateSucceeded(std::move(result));
    return s;
  }
  if (!s.IsNotFound()) {
    mutate->SetFailed(s);
    return s;
  }

  // Next, check the disk rowsets.

  vector<RowSet *> to_check = FindRowSetsToCheck(mutate, comps);
  for (RowSet *rs : to_check) {
    s = rs->MutateRow(ts,
                      *mutate->key_probe,
                      mutate->decoded_op.changelist,
                      tx_state->op_id(),
                      stats,
                      result.get());
    if (s.ok()) {
      mutate->SetMutateSucceeded(std::move(result));
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

void Tablet::StartApplying(WriteTransactionState* tx_state) {
  shared_lock<rw_spinlock> l(component_lock_);
  tx_state->StartApplying();
  tx_state->set_tablet_components(components_);
}

void Tablet::ApplyRowOperations(WriteTransactionState* tx_state) {
  // Allocate the ProbeStats objects from the transaction's arena, so
  // they're all contiguous and we don't need to do any central allocation.
  int num_ops = tx_state->row_ops().size();
  ProbeStats* stats_array = static_cast<ProbeStats*>(
      tx_state->arena()->AllocateBytesAligned(sizeof(ProbeStats) * num_ops,
                                              alignof(ProbeStats)));

  StartApplying(tx_state);
  int i = 0;
  for (RowOp* row_op : tx_state->row_ops()) {
    ProbeStats* stats = &stats_array[i++];
    // Manually run the constructor to clear the stats to 0 before collecting
    // them.
    new (stats) ProbeStats();
    ApplyRowOperation(tx_state, row_op, stats);
  }

  if (metrics_) {
    metrics_->AddProbeStats(stats_array, num_ops, tx_state->arena());
  }
}

void Tablet::ApplyRowOperation(WriteTransactionState* tx_state,
                               RowOp* row_op,
                               ProbeStats* stats) {
  CHECK(state_ == kOpen || state_ == kBootstrapping);
  DCHECK(row_op->has_row_lock()) << "RowOp must hold the row lock.";
  DCHECK(tx_state != nullptr) << "must have a WriteTransactionState";
  DCHECK(tx_state->op_id().IsInitialized()) << "TransactionState OpId needed for anchoring";
  DCHECK_EQ(tx_state->schema_at_decode_time(), schema());

  switch (row_op->decoded_op.type) {
    case RowOperationsPB::INSERT:
    case RowOperationsPB::UPSERT:
      ignore_result(InsertOrUpsertUnlocked(tx_state, row_op, stats));
      return;

    case RowOperationsPB::UPDATE:
    case RowOperationsPB::DELETE:
      ignore_result(MutateRowUnlocked(tx_state, row_op, stats));
      return;

    default:
      LOG_WITH_PREFIX(FATAL) << RowOperationsPB::Type_Name(row_op->decoded_op.type);
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

  for (const shared_ptr<RowSet> &rs : old_tree.all_rowsets()) {
    // Determine if it should be removed
    bool should_remove = false;
    for (const shared_ptr<RowSet> &to_remove : rowsets_to_remove) {
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
  std::lock_guard<rw_spinlock> lock(component_lock_);
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

Status Tablet::DoMajorDeltaCompaction(const vector<ColumnId>& col_ids,
                                      shared_ptr<RowSet> input_rs) {
  CHECK_EQ(state_, kOpen);
  Status s = down_cast<DiskRowSet*>(input_rs.get())
      ->MajorCompactDeltaStoresWithColumnIds(col_ids, GetHistoryGcOpts());
  return s;
}

bool Tablet::GetTabletAncientHistoryMark(Timestamp* ancient_history_mark) const {
  // We currently only support history GC through a fully-instantiated tablet
  // when using the HybridClock, since we can calculate the age of a mutation.
  if (!clock_->HasPhysicalComponent() || FLAGS_tablet_history_max_age_sec < 0) {
    return false;
  }
  Timestamp now = clock_->Now();
  uint64_t now_micros = HybridClock::GetPhysicalValueMicros(now);
  uint64_t max_age_micros = FLAGS_tablet_history_max_age_sec * 1000000ULL;
  // Ensure that the AHM calculation doesn't underflow when
  // '--tablet_history_max_age_sec' is set to a very high value.
  if (max_age_micros <= now_micros) {
    *ancient_history_mark =
        HybridClock::TimestampFromMicrosecondsAndLogicalValue(
            now_micros - max_age_micros,
            HybridClock::GetLogicalValue(now));
  } else {
    *ancient_history_mark = Timestamp(0);
  }
  return true;
}

HistoryGcOpts Tablet::GetHistoryGcOpts() const {
  Timestamp ancient_history_mark;
  if (GetTabletAncientHistoryMark(&ancient_history_mark)) {
    return HistoryGcOpts::Enabled(ancient_history_mark);
  }
  return HistoryGcOpts::Disabled();
}

Status Tablet::Flush() {
  TRACE_EVENT1("tablet", "Tablet::Flush", "id", tablet_id());
  std::lock_guard<Semaphore> lock(rowsets_flush_sem_);
  return FlushUnlocked();
}

Status Tablet::FlushUnlocked() {
  TRACE_EVENT0("tablet", "Tablet::FlushUnlocked");
  RowSetsInCompaction input;
  shared_ptr<MemRowSet> old_mrs;
  {
    // Create a new MRS with the latest schema.
    std::lock_guard<rw_spinlock> lock(component_lock_);
    RETURN_NOT_OK(ReplaceMemRowSetUnlocked(&input, &old_mrs));
  }

  // Wait for any in-flight transactions to finish against the old MRS
  // before we flush it.
  mvcc_.WaitForApplyingTransactionsToCommit();

  // Note: "input" should only contain old_mrs.
  return FlushInternal(input, old_mrs);
}

Status Tablet::ReplaceMemRowSetUnlocked(RowSetsInCompaction *compaction,
                                        shared_ptr<MemRowSet> *old_ms) {
  *old_ms = components_->memrowset;
  // Mark the memrowset rowset as locked, so compactions won't consider it
  // for inclusion in any concurrent compactions.
  std::unique_lock<std::mutex> ms_lock(*(*old_ms)->compact_flush_lock(), std::try_to_lock);
  CHECK(ms_lock.owns_lock());

  // Add to compaction.
  compaction->AddRowSet(*old_ms, std::move(ms_lock));

  shared_ptr<MemRowSet> new_mrs(new MemRowSet(next_mrs_id_++, *schema(), log_anchor_registry_.get(),
                                mem_tracker_));
  shared_ptr<RowSetTree> new_rst(new RowSetTree());
  ModifyRowSetTree(*components_->rowsets,
                   RowSetVector(), // remove nothing
                   { *old_ms }, // add the old MRS
                   new_rst.get());

  // Swap it in
  components_ = new TabletComponents(new_mrs, new_rst);
  return Status::OK();
}

Status Tablet::FlushInternal(const RowSetsInCompaction& input,
                             const shared_ptr<MemRowSet>& old_ms) {
  CHECK(state_ == kOpen || state_ == kBootstrapping);

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

  if (old_ms->empty()) {
    // If we're flushing an empty RowSet, we can short circuit here rather than
    // waiting until the check at the end of DoCompactionAndFlush(). This avoids
    // the need to create cfiles and write their headers only to later delete
    // them.
    LOG(INFO) << "MemRowSet was empty: no flush needed.";
    return HandleEmptyCompactionOrFlush(input.rowsets(), mrs_being_flushed);
  }

  if (flush_hooks_) {
    RETURN_NOT_OK_PREPEND(flush_hooks_->PostSwapNewMemRowSet(),
                          "PostSwapNewMemRowSet hook failed");
  }

  LOG_WITH_PREFIX(INFO) << "Flush: entering stage 1 (old memrowset already frozen for inserts)";
  input.DumpToLog();
  LOG_WITH_PREFIX(INFO) << "Memstore in-memory size: " << old_ms->memory_footprint() << " bytes";

  RETURN_NOT_OK(DoMergeCompactionOrFlush(input, mrs_being_flushed));

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
  std::lock_guard<Semaphore> lock(rowsets_flush_sem_);

  // If the current version >= new version, there is nothing to do.
  bool same_schema = schema()->Equals(*tx_state->schema());
  if (metadata_->schema_version() >= tx_state->schema_version()) {
    LOG_WITH_PREFIX(INFO) << "Already running schema version " << metadata_->schema_version()
                          << " got alter request for version " << tx_state->schema_version();
    return Status::OK();
  }

  LOG_WITH_PREFIX(INFO) << "Alter schema from " << schema()->ToString()
                        << " version " << metadata_->schema_version()
                        << " to " << tx_state->schema()->ToString()
                        << " version " << tx_state->schema_version();
  DCHECK(schema_lock_.is_locked());
  metadata_->SetSchema(*tx_state->schema(), tx_state->schema_version());
  if (tx_state->has_new_table_name()) {
    metadata_->SetTableName(tx_state->new_table_name());
    if (metric_entity_) {
      metric_entity_->SetAttribute("table_name", tx_state->new_table_name());
    }
  }

  // If the current schema and the new one are equal, there is nothing to do.
  if (same_schema) {
    return metadata_->Flush();
  }

  return FlushUnlocked();
}

Status Tablet::RewindSchemaForBootstrap(const Schema& new_schema,
                                        int64_t schema_version) {
  CHECK_EQ(state_, kBootstrapping);

  // We know that the MRS should be empty at this point, because we
  // rewind the schema before replaying any operations. So, we just
  // swap in a new one with the correct schema, rather than attempting
  // to flush.
  LOG_WITH_PREFIX(INFO) << "Rewinding schema during bootstrap to " << new_schema.ToString();

  metadata_->SetSchema(new_schema, schema_version);
  {
    std::lock_guard<rw_spinlock> lock(component_lock_);

    shared_ptr<MemRowSet> old_mrs = components_->memrowset;
    shared_ptr<RowSetTree> old_rowsets = components_->rowsets;
    CHECK(old_mrs->empty());
    shared_ptr<MemRowSet> new_mrs(new MemRowSet(old_mrs->mrs_id(), new_schema,
                                                log_anchor_registry_.get(), mem_tracker_));
    components_ = new TabletComponents(new_mrs, old_rowsets);
  }
  return Status::OK();
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
  shared_lock<rw_spinlock> l(component_lock_);
  return components_->memrowset->mrs_id();
}

bool Tablet::ShouldThrottleAllow(int64_t bytes) {
  if (!throttler_) {
    return true;
  }
  return throttler_->Take(MonoTime::Now(), 1, bytes);
}

Status Tablet::PickRowSetsToCompact(RowSetsInCompaction *picked,
                                    CompactFlags flags) const {
  CHECK_EQ(state_, kOpen);
  // Grab a local reference to the current RowSetTree. This is to avoid
  // holding the component_lock_ for too long. See the comment on component_lock_
  // in tablet.h for details on why that would be bad.
  shared_ptr<RowSetTree> rowsets_copy;
  {
    shared_lock<rw_spinlock> l(component_lock_);
    rowsets_copy = components_->rowsets;
  }

  std::lock_guard<std::mutex> compact_lock(compact_select_lock_);
  CHECK_EQ(picked->num_rowsets(), 0);

  unordered_set<RowSet*> picked_set;

  if (flags & FORCE_COMPACT_ALL) {
    // Compact all rowsets, regardless of policy.
    for (const shared_ptr<RowSet>& rs : rowsets_copy->all_rowsets()) {
      if (rs->IsAvailableForCompaction()) {
        picked_set.insert(rs.get());
      }
    }
  } else {
    // Let the policy decide which rowsets to compact.
    double quality = 0;
    RETURN_NOT_OK(compaction_policy_->PickRowSets(*rowsets_copy, &picked_set, &quality, NULL));
    VLOG_WITH_PREFIX(2) << "Compaction quality: " << quality;
  }

  shared_lock<rw_spinlock> l(component_lock_);
  for (const shared_ptr<RowSet>& rs : components_->rowsets->all_rowsets()) {
    if (picked_set.erase(rs.get()) == 0) {
      // Not picked.
      continue;
    }

    // Grab the compact_flush_lock: this prevents any other concurrent
    // compaction from selecting this same rowset, and also ensures that
    // we don't select a rowset which is currently in the middle of being
    // flushed.
    std::unique_lock<std::mutex> lock(*rs->compact_flush_lock(), std::try_to_lock);
    CHECK(lock.owns_lock()) << rs->ToString() << " appeared available for "
      "compaction when inputs were selected, but was unable to lock its "
      "compact_flush_lock to prepare for compaction.";

    // Push the lock on our scoped list, so we unlock when done.
    picked->AddRowSet(rs, std::move(lock));
  }

  // When we iterated through the current rowsets, we should have found all of the
  // rowsets that we picked. If we didn't, that implies that some other thread swapped
  // them out while we were making our selection decision -- that's not possible
  // since we only picked rowsets that were marked as available for compaction.
  if (!picked_set.empty()) {
    for (const RowSet* not_found : picked_set) {
      LOG_WITH_PREFIX(ERROR) << "Rowset selected for compaction but not available anymore: "
                             << not_found->ToString();
    }
    LOG_WITH_PREFIX(FATAL) << "Was unable to find all rowsets selected for compaction";
  }
  return Status::OK();
}

void Tablet::GetRowSetsForTests(RowSetVector* out) {
  shared_ptr<RowSetTree> rowsets_copy;
  {
    shared_lock<rw_spinlock> l(component_lock_);
    rowsets_copy = components_->rowsets;
  }
  for (const shared_ptr<RowSet>& rs : rowsets_copy->all_rowsets()) {
    out->push_back(rs);
  }
}

void Tablet::RegisterMaintenanceOps(MaintenanceManager* maint_mgr) {
  CHECK_EQ(state_, kOpen);
  DCHECK(maintenance_ops_.empty());

  gscoped_ptr<MaintenanceOp> rs_compact_op(new CompactRowSetsOp(this));
  maint_mgr->RegisterOp(rs_compact_op.get());
  maintenance_ops_.push_back(rs_compact_op.release());

  gscoped_ptr<MaintenanceOp> minor_delta_compact_op(new MinorDeltaCompactionOp(this));
  maint_mgr->RegisterOp(minor_delta_compact_op.get());
  maintenance_ops_.push_back(minor_delta_compact_op.release());

  gscoped_ptr<MaintenanceOp> major_delta_compact_op(new MajorDeltaCompactionOp(this));
  maint_mgr->RegisterOp(major_delta_compact_op.get());
  maintenance_ops_.push_back(major_delta_compact_op.release());
}

void Tablet::UnregisterMaintenanceOps() {
  // First cancel all of the operations, so that while we're waiting for one
  // operation to finish in Unregister(), a different one can't get re-scheduled.
  for (MaintenanceOp* op : maintenance_ops_) {
    op->CancelAndDisable();
  }

  for (MaintenanceOp* op : maintenance_ops_) {
    op->Unregister();
  }
  STLDeleteElements(&maintenance_ops_);
}

Status Tablet::FlushMetadata(const RowSetVector& to_remove,
                             const RowSetMetadataVector& to_add,
                             int64_t mrs_being_flushed) {
  RowSetMetadataIds to_remove_meta;
  for (const shared_ptr<RowSet>& rowset : to_remove) {
    // Skip MemRowSet & DuplicatingRowSets which don't have metadata.
    if (rowset->metadata().get() == nullptr) {
      continue;
    }
    to_remove_meta.insert(rowset->metadata()->id());
  }

  return metadata_->UpdateAndFlush(to_remove_meta, to_add, mrs_being_flushed);
}

Status Tablet::DoMergeCompactionOrFlush(const RowSetsInCompaction &input,
                                        int64_t mrs_being_flushed) {
  const char *op_name =
        (mrs_being_flushed == TabletMetadata::kNoMrsFlushed) ? "Compaction" : "Flush";
  TRACE_EVENT2("tablet", "Tablet::DoMergeCompactionOrFlush",
               "tablet_id", tablet_id(),
               "op", op_name);

  MvccSnapshot flush_snap(mvcc_);
  LOG_WITH_PREFIX(INFO) << op_name << ": entering phase 1 (flushing snapshot). Phase 1 snapshot: "
                        << flush_snap.ToString();

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostTakeMvccSnapshot(),
                          "PostTakeMvccSnapshot hook failed");
  }

  shared_ptr<CompactionInput> merge;
  RETURN_NOT_OK(input.CreateCompactionInput(flush_snap, schema(), &merge));

  RollingDiskRowSetWriter drsw(metadata_.get(), merge->schema(), bloom_sizing(),
                               compaction_policy_->target_rowset_size());
  RETURN_NOT_OK_PREPEND(drsw.Open(), "Failed to open DiskRowSet for flush");

  HistoryGcOpts history_gc_opts = GetHistoryGcOpts();
  RETURN_NOT_OK_PREPEND(FlushCompactionInput(merge.get(), flush_snap, history_gc_opts, &drsw),
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
    LOG_WITH_PREFIX(INFO) << op_name << " resulted in no output rows (all input rows "
                          << "were GCed!)  Removing all input rowsets.";
    return HandleEmptyCompactionOrFlush(input.rowsets(), mrs_being_flushed);
  }

  // The RollingDiskRowSet writer wrote out one or more RowSets as the
  // output. Open these into 'new_rowsets'.
  vector<shared_ptr<RowSet> > new_disk_rowsets;
  RowSetMetadataVector new_drs_metas;
  drsw.GetWrittenRowSetMetadata(&new_drs_metas);

  if (metrics_.get()) metrics_->bytes_flushed->IncrementBy(drsw.written_size());
  CHECK(!new_drs_metas.empty());
  {
    TRACE_EVENT0("tablet", "Opening compaction results");
    for (const shared_ptr<RowSetMetadata>& meta : new_drs_metas) {
      shared_ptr<DiskRowSet> new_rowset;
      Status s = DiskRowSet::Open(meta, log_anchor_registry_.get(), &new_rowset, mem_tracker_);
      if (!s.ok()) {
        LOG_WITH_PREFIX(WARNING) << "Unable to open snapshot " << op_name << " results "
                                 << meta->ToString() << ": " << s.ToString();
        return s;
      }
      new_disk_rowsets.push_back(new_rowset);
    }
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
  // This breaks an invariant that the redo files are time-ordered, and we would probably
  // reapply the deltas in the wrong order on the read path.
  //
  // The way that we avoid this case is that DuplicatingRowSet's FlushDeltas method is a
  // no-op.
  LOG_WITH_PREFIX(INFO) << op_name << ": entering phase 2 (starting to duplicate updates "
                        << "in new rowsets)";
  shared_ptr<DuplicatingRowSet> inprogress_rowset(
    new DuplicatingRowSet(input.rowsets(), new_disk_rowsets));

  // The next step is to swap in the DuplicatingRowSet, and at the same time, determine an
  // MVCC snapshot which includes all of the transactions that saw a pre-DuplicatingRowSet
  // version of components_.
  MvccSnapshot non_duplicated_txns_snap;
  vector<Timestamp> applying_during_swap;
  {
    TRACE_EVENT0("tablet", "Swapping DuplicatingRowSet");
    // Taking component_lock_ in write mode ensures that no new transactions
    // can StartApplying() (or snapshot components_) during this block.
    std::lock_guard<rw_spinlock> lock(component_lock_);
    AtomicSwapRowSetsUnlocked(input.rowsets(), { inprogress_rowset });

    // NOTE: transactions may *commit* in between these two lines.
    // We need to make sure all such transactions end up in the
    // 'applying_during_swap' list, the 'non_duplicated_txns_snap' snapshot,
    // or both. Thus it's crucial that these next two lines are in this order!
    mvcc_.GetApplyingTransactionsTimestamps(&applying_during_swap);
    non_duplicated_txns_snap = MvccSnapshot(mvcc_);
  }

  // All transactions committed in 'non_duplicated_txns_snap' saw the pre-swap components_.
  // Additionally, any transactions that were APPLYING during the above block by definition
  // _started_ doing so before the swap. Hence those transactions also need to get included in
  // non_duplicated_txns_snap. To do so, we wait for them to commit, and then
  // manually include them into our snapshot.
  if (VLOG_IS_ON(1) && !applying_during_swap.empty()) {
    VLOG_WITH_PREFIX(1) << "Waiting for " << applying_during_swap.size()
                        << " mid-APPLY txns to commit before finishing compaction...";
    for (const Timestamp& ts : applying_during_swap) {
      VLOG_WITH_PREFIX(1) << "  " << ts.value();
    }
  }

  // This wait is a little bit conservative - technically we only need to wait for
  // those transactions in 'applying_during_swap', but MVCC doesn't implement the
  // ability to wait for a specific set. So instead we wait for all currently applying --
  // a bit more than we need, but still correct.
  mvcc_.WaitForApplyingTransactionsToCommit();

  // Then we want to consider all those transactions that were in-flight when we did the
  // swap as committed in 'non_duplicated_txns_snap'.
  non_duplicated_txns_snap.AddCommittedTimestamps(applying_during_swap);

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostSwapInDuplicatingRowSet(),
                          "PostSwapInDuplicatingRowSet hook failed");
  }

  // Phase 2. Here we re-scan the compaction input, copying those missed updates into the
  // new rowset's DeltaTracker.
  LOG_WITH_PREFIX(INFO) << op_name
                        << " Phase 2: carrying over any updates which arrived during Phase 1";
  LOG_WITH_PREFIX(INFO) << "Phase 2 snapshot: " << non_duplicated_txns_snap.ToString();
  RETURN_NOT_OK_PREPEND(
      input.CreateCompactionInput(non_duplicated_txns_snap, schema(), &merge),
          Substitute("Failed to create $0 inputs", op_name).c_str());

  // Update the output rowsets with the deltas that came in in phase 1, before we swapped
  // in the DuplicatingRowSets. This will perform a flush of the updated DeltaTrackers
  // in the end so that the data that is reported in the log as belonging to the input
  // rowsets is flushed.
  RETURN_NOT_OK_PREPEND(ReupdateMissedDeltas(metadata_->tablet_id(),
                                             merge.get(),
                                             history_gc_opts,
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

  // Run fault points used by some integration tests.
  if (input.num_rowsets() > 1) {
    MAYBE_FAULT(FLAGS_fault_crash_before_flush_tablet_meta_after_compaction);
  } else if (input.num_rowsets() == 1 &&
             input.rowsets()[0]->EstimateOnDiskSize() == 0) {
    MAYBE_FAULT(FLAGS_fault_crash_before_flush_tablet_meta_after_flush_mrs);
  }

  // Write out the new Tablet Metadata and remove old rowsets.
  RETURN_NOT_OK_PREPEND(FlushMetadata(input.rowsets(), new_drs_metas, mrs_being_flushed),
                        "Failed to flush new tablet metadata");

  // Replace the compacted rowsets with the new on-disk rowsets, making them visible now that
  // their metadata was written to disk.
  AtomicSwapRowSets({ inprogress_rowset }, new_disk_rowsets);

  LOG_WITH_PREFIX(INFO) << op_name << " successful on " << drsw.written_count()
                        << " rows " << "(" << drsw.written_size() << " bytes)";

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostSwapNewRowSet(),
                          "PostSwapNewRowSet hook failed");
  }

  return Status::OK();
}

Status Tablet::HandleEmptyCompactionOrFlush(const RowSetVector& rowsets,
                                            int mrs_being_flushed) {
  // Write out the new Tablet Metadata and remove old rowsets.
  RETURN_NOT_OK_PREPEND(FlushMetadata(rowsets,
                                      RowSetMetadataVector(),
                                      mrs_being_flushed),
                        "Failed to flush new tablet metadata");

  AtomicSwapRowSets(rowsets, RowSetVector());
  return Status::OK();
}

Status Tablet::Compact(CompactFlags flags) {
  CHECK_EQ(state_, kOpen);

  RowSetsInCompaction input;
  // Step 1. Capture the rowsets to be merged
  RETURN_NOT_OK_PREPEND(PickRowSetsToCompact(&input, flags),
                        "Failed to pick rowsets to compact");
  LOG_WITH_PREFIX(INFO) << "Compaction: stage 1 complete, picked "
                        << input.num_rowsets() << " rowsets to compact";
  if (compaction_hooks_) {
    RETURN_NOT_OK_PREPEND(compaction_hooks_->PostSelectIterators(),
                          "PostSelectIterators hook failed");
  }

  input.DumpToLog();

  return DoMergeCompactionOrFlush(input,
                                  TabletMetadata::kNoMrsFlushed);
}

void Tablet::UpdateCompactionStats(MaintenanceOpStats* stats) {

  // TODO: use workload statistics here to find out how "hot" the tablet has
  // been in the last 5 minutes, and somehow scale the compaction quality
  // based on that, so we favor hot tablets.
  double quality = 0;
  unordered_set<RowSet*> picked_set_ignored;

  shared_ptr<RowSetTree> rowsets_copy;
  {
    shared_lock<rw_spinlock> l(component_lock_);
    rowsets_copy = components_->rowsets;
  }

  {
    std::lock_guard<std::mutex> compact_lock(compact_select_lock_);
    WARN_NOT_OK(compaction_policy_->PickRowSets(*rowsets_copy, &picked_set_ignored, &quality, NULL),
                Substitute("Couldn't determine compaction quality for $0", tablet_id()));
  }

  VLOG_WITH_PREFIX(1) << "Best compaction for " << tablet_id() << ": " << quality;

  stats->set_runnable(quality >= 0);
  stats->set_perf_improvement(quality);
}


Status Tablet::DebugDump(vector<string> *lines) {
  shared_lock<rw_spinlock> l(component_lock_);

  LOG_STRING(INFO, lines) << "Dumping tablet:";
  LOG_STRING(INFO, lines) << "---------------------------";

  LOG_STRING(INFO, lines) << "MRS " << components_->memrowset->ToString() << ":";
  RETURN_NOT_OK(components_->memrowset->DebugDump(lines));

  for (const shared_ptr<RowSet> &rs : components_->rowsets->all_rowsets()) {
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
  shared_lock<rw_spinlock> l(component_lock_);

  // Construct all the iterators locally first, so that if we fail
  // in the middle, we don't modify the output arguments.
  vector<shared_ptr<RowwiseIterator> > ret;

  // Grab the memrowset iterator.
  gscoped_ptr<RowwiseIterator> ms_iter;
  RETURN_NOT_OK(components_->memrowset->NewRowIterator(projection, snap, &ms_iter));
  ret.push_back(shared_ptr<RowwiseIterator>(ms_iter.release()));

  // Cull row-sets in the case of key-range queries.
  if (spec != nullptr && spec->lower_bound_key() && spec->exclusive_upper_bound_key()) {
    // TODO : support open-ended intervals
    // TODO: the upper bound key is exclusive, but the RowSetTree function takes
    // an inclusive interval. So, we might end up fetching one more rowset than
    // necessary.
    vector<RowSet *> interval_sets;
    components_->rowsets->FindRowSetsIntersectingInterval(
        spec->lower_bound_key()->encoded_key(),
        spec->exclusive_upper_bound_key()->encoded_key(),
        &interval_sets);
    for (const RowSet *rs : interval_sets) {
      gscoped_ptr<RowwiseIterator> row_it;
      RETURN_NOT_OK_PREPEND(rs->NewRowIterator(projection, snap, &row_it),
                            Substitute("Could not create iterator for rowset $0",
                                       rs->ToString()));
      ret.push_back(shared_ptr<RowwiseIterator>(row_it.release()));
    }
    ret.swap(*iters);
    return Status::OK();
  }

  // If there are no encoded predicates or they represent an open-ended range, then
  // fall back to grabbing all rowset iterators
  for (const shared_ptr<RowSet> &rs : components_->rowsets->all_rowsets()) {
    gscoped_ptr<RowwiseIterator> row_it;
    RETURN_NOT_OK_PREPEND(rs->NewRowIterator(projection, snap, &row_it),
                          Substitute("Could not create iterator for rowset $0",
                                     rs->ToString()));
    ret.push_back(shared_ptr<RowwiseIterator>(row_it.release()));
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
  for (const shared_ptr<RowSet> &rowset : comps->rowsets->all_rowsets()) {
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

size_t Tablet::MemRowSetLogRetentionSize(const MaxIdxToSegmentMap& max_idx_to_segment_size) const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  return GetLogRetentionSizeForIndex(comps->memrowset->MinUnflushedLogIndex(),
                                     max_idx_to_segment_size);
}

size_t Tablet::EstimateOnDiskSize() const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  if (!comps) return 0;

  size_t ret = 0;
  for (const shared_ptr<RowSet> &rowset : comps->rowsets->all_rowsets()) {
    ret += rowset->EstimateOnDiskSize();
  }

  return ret;
}

size_t Tablet::DeltaMemStoresSize() const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  size_t ret = 0;
  for (const shared_ptr<RowSet> &rowset : comps->rowsets->all_rowsets()) {
    ret += rowset->DeltaMemStoreSize();
  }

  return ret;
}

bool Tablet::DeltaMemRowSetEmpty() const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  for (const shared_ptr<RowSet> &rowset : comps->rowsets->all_rowsets()) {
    if (!rowset->DeltaMemStoreEmpty()) {
      return false;
    }
  }

  return true;
}

void Tablet::GetInfoForBestDMSToFlush(const MaxIdxToSegmentMap& max_idx_to_segment_size,
                                      int64_t* mem_size, int64_t* retention_size) const {
  shared_ptr<RowSet> rowset = FindBestDMSToFlush(max_idx_to_segment_size);

  if (rowset) {
    *retention_size = GetLogRetentionSizeForIndex(rowset->MinUnflushedLogIndex(),
                                            max_idx_to_segment_size);
    *mem_size = rowset->DeltaMemStoreSize();
  } else {
    *retention_size = 0;
    *mem_size = 0;
  }
}

Status Tablet::FlushDMSWithHighestRetention(const MaxIdxToSegmentMap&
                                            max_idx_to_segment_size) const {
  shared_ptr<RowSet> rowset = FindBestDMSToFlush(max_idx_to_segment_size);
  if (rowset) {
    return rowset->FlushDeltas();
  }
  return Status::OK();
}

shared_ptr<RowSet> Tablet::FindBestDMSToFlush(const MaxIdxToSegmentMap&
                                              max_idx_to_segment_size) const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);
  int64_t mem_size = 0;
  int64_t retention_size = 0;
  shared_ptr<RowSet> best_dms;
  for (const shared_ptr<RowSet> &rowset : comps->rowsets->all_rowsets()) {
    if (rowset->DeltaMemStoreEmpty()) {
      continue;
    }
    int64_t size = GetLogRetentionSizeForIndex(rowset->MinUnflushedLogIndex(),
                                               max_idx_to_segment_size);
    if ((size > retention_size) ||
        (size == retention_size &&
         (rowset->DeltaMemStoreSize() > mem_size))) {
      mem_size = rowset->DeltaMemStoreSize();
      retention_size = size;
      best_dms = rowset;
    }
  }
  return best_dms;
}

int64_t Tablet::GetLogRetentionSizeForIndex(int64_t min_log_index,
                                            const MaxIdxToSegmentMap& max_idx_to_segment_size) {
  if (max_idx_to_segment_size.size() == 0 || min_log_index == -1) {
    return 0;
  }
  int64_t total_size = 0;
  for (const MaxIdxToSegmentMap::value_type& entry : max_idx_to_segment_size) {
    if (min_log_index > entry.first) {
      continue; // We're not in this segment, probably someone else is retaining it.
    }
    total_size += entry.second;
  }
  return total_size;
}

Status Tablet::FlushBiggestDMS() {
  CHECK_EQ(state_, kOpen);
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  int64_t max_size = -1;
  shared_ptr<RowSet> biggest_drs;
  for (const shared_ptr<RowSet> &rowset : comps->rowsets->all_rowsets()) {
    int64_t current = rowset->DeltaMemStoreSize();
    if (current > max_size) {
      max_size = current;
      biggest_drs = rowset;
    }
  }
  return max_size > 0 ? biggest_drs->FlushDeltas() : Status::OK();
}

Status Tablet::CompactWorstDeltas(RowSet::DeltaCompactionType type) {
  CHECK_EQ(state_, kOpen);
  shared_ptr<RowSet> rs;

  // We're required to grab the rowset's compact_flush_lock under the compact_select_lock_.
  std::unique_lock<std::mutex> lock;
  double perf_improv;
  {
    // We only want to keep the selection lock during the time we look at rowsets to compact.
    // The returned rowset is guaranteed to be available to lock since locking must be done
    // under this lock.
    std::lock_guard<std::mutex> compact_lock(compact_select_lock_);
    perf_improv = GetPerfImprovementForBestDeltaCompactUnlocked(type, &rs);
    if (!rs) {
      return Status::OK();
    }
    lock = std::unique_lock<std::mutex>(*rs->compact_flush_lock(), std::try_to_lock);
    CHECK(lock.owns_lock());
  }

  // We just released compact_select_lock_ so other compactions can select and run, but the
  // rowset is ours.
  DCHECK(perf_improv != 0);
  if (type == RowSet::MINOR_DELTA_COMPACTION) {
    RETURN_NOT_OK_PREPEND(rs->MinorCompactDeltaStores(),
                          "Failed minor delta compaction on " + rs->ToString());
  } else if (type == RowSet::MAJOR_DELTA_COMPACTION) {
    RETURN_NOT_OK_PREPEND(down_cast<DiskRowSet*>(rs.get())->MajorCompactDeltaStores(
        GetHistoryGcOpts()), "Failed major delta compaction on " + rs->ToString());
  }
  return Status::OK();
}

double Tablet::GetPerfImprovementForBestDeltaCompact(RowSet::DeltaCompactionType type,
                                                     shared_ptr<RowSet>* rs) const {
  std::lock_guard<std::mutex> compact_lock(compact_select_lock_);
  return GetPerfImprovementForBestDeltaCompactUnlocked(type, rs);
}

double Tablet::GetPerfImprovementForBestDeltaCompactUnlocked(RowSet::DeltaCompactionType type,
                                                             shared_ptr<RowSet>* rs) const {
  std::unique_lock<std::mutex> cs_lock(compact_select_lock_, std::try_to_lock);
  DCHECK(!cs_lock.owns_lock());
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);
  double worst_delta_perf = 0;
  shared_ptr<RowSet> worst_rs;
  for (const shared_ptr<RowSet> &rowset : comps->rowsets->all_rowsets()) {
    if (!rowset->IsAvailableForCompaction()) {
      continue;
    }
    double perf_improv = rowset->DeltaStoresCompactionPerfImprovementScore(type);
    if (perf_improv > worst_delta_perf) {
      worst_rs = rowset;
      worst_delta_perf = perf_improv;
    }
  }
  if (rs && worst_delta_perf > 0) {
    *rs = worst_rs;
  }
  return worst_delta_perf;
}

size_t Tablet::num_rowsets() const {
  shared_lock<rw_spinlock> l(component_lock_);
  return components_->rowsets->all_rowsets().size();
}

void Tablet::PrintRSLayout(ostream* o) {
  shared_ptr<RowSetTree> rowsets_copy;
  {
    shared_lock<rw_spinlock> l(component_lock_);
    rowsets_copy = components_->rowsets;
  }
  std::lock_guard<std::mutex> compact_lock(compact_select_lock_);
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
  for (const string& s : log) {
    *o << EscapeForHtmlToString(s) << std::endl;
  }
  *o << "</pre>" << std::endl;
}

string Tablet::LogPrefix() const {
  return Substitute("T $0 ", tablet_id());
}

////////////////////////////////////////////////////////////
// Tablet::Iterator
////////////////////////////////////////////////////////////

Tablet::Iterator::Iterator(const Tablet* tablet, const Schema& projection,
                           MvccSnapshot snap, const OrderMode order)
    : tablet_(tablet),
      projection_(projection),
      snap_(std::move(snap)),
      order_(order) {}

Tablet::Iterator::~Iterator() {}

Status Tablet::Iterator::Init(ScanSpec *spec) {
  DCHECK(iter_.get() == nullptr);

  RETURN_NOT_OK(tablet_->GetMappedReadProjection(projection_, &projection_));

  vector<shared_ptr<RowwiseIterator>> iters;

  RETURN_NOT_OK(tablet_->CaptureConsistentIterators(&projection_, snap_, spec, &iters));

  switch (order_) {
    case ORDERED:
      iter_.reset(new MergeIterator(projection_, iters));
      break;
    case UNORDERED:
    default:
      iter_.reset(new UnionIterator(iters));
      break;
  }

  RETURN_NOT_OK(iter_->Init(spec));
  return Status::OK();
}

bool Tablet::Iterator::HasNext() const {
  DCHECK(iter_.get() != nullptr) << "Not initialized!";
  return iter_->HasNext();
}

Status Tablet::Iterator::NextBlock(RowBlock *dst) {
  DCHECK(iter_.get() != nullptr) << "Not initialized!";
  return iter_->NextBlock(dst);
}

string Tablet::Iterator::ToString() const {
  string s;
  s.append("tablet iterator: ");
  if (iter_.get() == nullptr) {
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
