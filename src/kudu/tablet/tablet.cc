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
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/generic_iterators.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partition.h"
#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/rowid.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/io_context.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/threading/thread_collision_warner.h"
#include "kudu/tablet/compaction.h"
#include "kudu/tablet/compaction_policy.h"
#include "kudu/tablet/delta_tracker.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/memrowset.h"
#include "kudu/tablet/row_op.h"
#include "kudu/tablet/rowset_info.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/rowset_tree.h"
#include "kudu/tablet/svg_dump.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet_mm_ops.h"
#include "kudu/tablet/transactions/alter_schema_transaction.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/bloom_filter.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/faststring.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/slice.h"
#include "kudu/util/status_callback.h"
#include "kudu/util/throttler.h"
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

DEFINE_int32(max_cell_size_bytes, 64 * 1024,
             "The maximum size of any individual cell in a table. Attempting to store "
             "string or binary columns with a size greater than this will result "
             "in errors.");
TAG_FLAG(max_cell_size_bytes, unsafe);

// Large encoded keys cause problems because we store the min/max encoded key in the
// CFile footer for the composite key column. The footer has a max length of 64K, so
// the default here comfortably fits two of them with room for other metadata.
DEFINE_int32(max_encoded_key_size_bytes, 16 * 1024,
             "The maximum size of a row's encoded composite primary key. This length is "
             "approximately the sum of the sizes of the component columns, though it can "
             "be larger in cases where the components contain embedded NULL bytes. "
             "Attempting to insert a row with a larger encoded composite key will "
             "result in an error.");
TAG_FLAG(max_encoded_key_size_bytes, unsafe);

DEFINE_bool(enable_undo_delta_block_gc, true,
    "Whether to enable undo delta block garbage collection. "
    "This only affects the undo delta block deletion background task, and "
    "doesn't control whether compactions delete ancient history. "
    "To change what is considered ancient history use --tablet_history_max_age_sec");
TAG_FLAG(enable_undo_delta_block_gc, evolving);

METRIC_DEFINE_entity(tablet);
METRIC_DEFINE_gauge_size(tablet, memrowset_size, "MemRowSet Memory Usage",
                         kudu::MetricUnit::kBytes,
                         "Size of this tablet's memrowset");
METRIC_DEFINE_gauge_size(tablet, on_disk_data_size, "Tablet Data Size On Disk",
                         kudu::MetricUnit::kBytes,
                         "Space used by this tablet's data blocks.");
METRIC_DEFINE_gauge_size(tablet, num_rowsets_on_disk, "Tablet Number of Rowsets on Disk",
                         kudu::MetricUnit::kUnits,
                         "Number of diskrowsets in this tablet");

using kudu::MaintenanceManager;
using kudu::clock::HybridClock;
using kudu::fs::IOContext;
using kudu::log::LogAnchorRegistry;
using std::ostream;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

class RowBlock;
struct IteratorStats;

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

Tablet::Tablet(scoped_refptr<TabletMetadata> metadata,
               scoped_refptr<clock::Clock> clock,
               shared_ptr<MemTracker> parent_mem_tracker,
               MetricRegistry* metric_registry,
               scoped_refptr<LogAnchorRegistry> log_anchor_registry)
  : key_schema_(metadata->schema().CreateKeyProjection()),
    metadata_(std::move(metadata)),
    log_anchor_registry_(std::move(log_anchor_registry)),
    mem_trackers_(tablet_id(), std::move(parent_mem_tracker)),
    next_mrs_id_(0),
    clock_(std::move(clock)),
    rowsets_flush_sem_(1),
    state_(kInitialized) {
      CHECK(schema()->has_column_ids());
  compaction_policy_.reset(CreateCompactionPolicy());

  if (metric_registry) {
    MetricEntity::AttributeMap attrs;
    attrs["table_id"] = metadata_->table_id();
    attrs["table_name"] = metadata_->table_name();
    attrs["partition"] = metadata_->partition_schema().PartitionDebugString(metadata_->partition(),
                                                                            *schema());
    metric_entity_ = METRIC_ENTITY_tablet.Instantiate(metric_registry, tablet_id(), attrs);
    metrics_.reset(new TabletMetrics(metric_entity_));
    METRIC_memrowset_size.InstantiateFunctionGauge(
      metric_entity_, Bind(&Tablet::MemRowSetSize, Unretained(this)))
      ->AutoDetach(&metric_detacher_);
    METRIC_on_disk_data_size.InstantiateFunctionGauge(
      metric_entity_, Bind(&Tablet::OnDiskDataSize, Unretained(this)))
      ->AutoDetach(&metric_detacher_);
    METRIC_num_rowsets_on_disk.InstantiateFunctionGauge(
      metric_entity_, Bind(&Tablet::num_rowsets, Unretained(this)))
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

// Returns an error if the Tablet has been stopped, i.e. is 'kStopped' or
// 'kShutdown', and otherwise checks that 'expected_state' matches 'state_'.
#define RETURN_IF_STOPPED_OR_CHECK_STATE(expected_state) do { \
  std::lock_guard<simple_spinlock> l(state_lock_); \
  RETURN_NOT_OK(CheckHasNotBeenStoppedUnlocked()); \
  CHECK_EQ(expected_state, state_); \
} while (0);

Status Tablet::Open() {
  TRACE_EVENT0("tablet", "Tablet::Open");
  RETURN_IF_STOPPED_OR_CHECK_STATE(kInitialized);

  std::lock_guard<rw_spinlock> lock(component_lock_);
  CHECK(schema()->has_column_ids());

  next_mrs_id_ = metadata_->last_durable_mrs_id() + 1;

  RowSetVector rowsets_opened;

  fs::IOContext io_context({ tablet_id() });
  // open the tablet row-sets
  for (const shared_ptr<RowSetMetadata>& rowset_meta : metadata_->rowsets()) {
    shared_ptr<DiskRowSet> rowset;
    Status s = DiskRowSet::Open(rowset_meta,
                                log_anchor_registry_.get(),
                                mem_trackers_,
                                &io_context,
                                &rowset);
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
  shared_ptr<MemRowSet> new_mrs;
  RETURN_NOT_OK(MemRowSet::Create(next_mrs_id_++, *schema(),
                                  log_anchor_registry_.get(),
                                  mem_trackers_.tablet_tracker,
                                  &new_mrs));
  components_ = new TabletComponents(new_mrs, new_rowset_tree);

  {
    std::lock_guard<simple_spinlock> l(state_lock_);
    if (state_ != kInitialized) {
      DCHECK(state_ == kStopped || state_ == kShutdown);
      return Status::IllegalState("Expected the Tablet to be initialized");
    }
    set_state_unlocked(kBootstrapping);
  }
  return Status::OK();
}

void Tablet::Stop() {
  {
    std::lock_guard<simple_spinlock> l(state_lock_);
    if (state_ == kStopped || state_ == kShutdown) {
      return;
    }
    set_state_unlocked(kStopped);
  }

  // Close MVCC so Applying transactions will not complete and will not be
  // waited on. This prevents further snapshotting of the tablet.
  mvcc_.Close();

  // Stop tablet ops from being scheduled by the maintenance manager.
  CancelMaintenanceOps();
}

Status Tablet::MarkFinishedBootstrapping() {
  std::lock_guard<simple_spinlock> l(state_lock_);
  if (state_ != kBootstrapping) {
    DCHECK(state_ == kStopped || state_ == kShutdown);
    return Status::IllegalState("The tablet has been stopped");
  }
  set_state_unlocked(kOpen);
  return Status::OK();
}

void Tablet::Shutdown() {
  Stop();
  UnregisterMaintenanceOps();

  std::lock_guard<rw_spinlock> lock(component_lock_);
  components_ = nullptr;
  {
    std::lock_guard<simple_spinlock> l(state_lock_);
    set_state_unlocked(kShutdown);
  }
  if (metric_entity_) {
    metric_entity_->Unpublish();
  }

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

BloomFilterSizing Tablet::DefaultBloomSizing() {
  return BloomFilterSizing::BySizeAndFPRate(FLAGS_tablet_bloom_block_size,
                                            FLAGS_tablet_bloom_target_fp_rate);
}

Status Tablet::NewRowIterator(const Schema &projection,
                              gscoped_ptr<RowwiseIterator> *iter) const {
  // Yield current rows.
  MvccSnapshot snap(mvcc_);
  return NewRowIterator(projection, snap, UNORDERED, iter);
}


Status Tablet::NewRowIterator(const Schema &projection,
                              const MvccSnapshot &snap,
                              const OrderMode order,
                              gscoped_ptr<RowwiseIterator> *iter) const {
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
  if (metrics_) {
    metrics_->scans_started->Increment();
  }
  IOContext io_context({ tablet_id() });
  VLOG_WITH_PREFIX(2) << "Created new Iterator under snap: " << snap.ToString();
  iter->reset(new Iterator(this, projection, snap, order, std::move(io_context)));
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

  // Important to set the schema before the ops -- we need the
  // schema in order to stringify the ops.
  tx_state->set_schema_at_decode_time(schema());
  tx_state->SetRowOps(std::move(ops));

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
                   metadata_->partition_schema().PartitionKeyDebugString(row)));
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

void Tablet::AssignTimestampAndStartTransactionForTests(WriteTransactionState* tx_state) {
  CHECK(!tx_state->has_timestamp());
  // Don't support COMMIT_WAIT for tests that don't boot a tablet server.
  CHECK_NE(tx_state->external_consistency_mode(), COMMIT_WAIT);

  // Make sure timestamp assignment and transaction start are atomic, for tests.
  //
  // This is to make sure that when test txns advance safe time later, we don't have
  // any txn in-flight between getting a timestamp and being started. Otherwise we
  // might run the risk of assigning a timestamp to txn1, and have another txn
  // get a timestamp/start/advance safe time before txn1 starts making txn1's timestamp
  // invalid on start.
  {
    std::lock_guard<simple_spinlock> l(test_start_txn_lock_);
    tx_state->set_timestamp(clock_->Now());
    StartTransaction(tx_state);
  }
}

void Tablet::StartTransaction(WriteTransactionState* tx_state) {
  gscoped_ptr<ScopedTransaction> mvcc_tx;
  DCHECK(tx_state->has_timestamp());
  mvcc_tx.reset(new ScopedTransaction(&mvcc_, tx_state->timestamp()));
  tx_state->SetMvccTx(std::move(mvcc_tx));
}

bool Tablet::ValidateOpOrMarkFailed(RowOp* op) const {
  if (op->validated) return true;

  Status s = ValidateOp(*op);
  if (PREDICT_FALSE(!s.ok())) {
    // TODO(todd): add a metric tracking the number of invalid ops.
    op->SetFailed(s);
    return false;
  }
  op->validated = true;
  return true;
}

Status Tablet::ValidateOp(const RowOp& op) const {
  switch (op.decoded_op.type) {
    case RowOperationsPB::INSERT:
    case RowOperationsPB::UPSERT:
      return ValidateInsertOrUpsertUnlocked(op);

    case RowOperationsPB::UPDATE:
    case RowOperationsPB::DELETE:
      return ValidateMutateUnlocked(op);

    default:
      LOG_WITH_PREFIX(FATAL) << RowOperationsPB::Type_Name(op.decoded_op.type);
  }
  abort(); // unreachable
}

Status Tablet::ValidateInsertOrUpsertUnlocked(const RowOp& op) const {
  // Check that no individual cell is larger than the specified max.
  ConstContiguousRow row(schema(), op.decoded_op.row_data);
  for (int i = 0; i < schema()->num_columns(); i++) {
    if (!BitmapTest(op.decoded_op.isset_bitmap, i)) continue;
    const auto& col = schema()->column(i);
    if (col.type_info()->physical_type() != BINARY) continue;
    const auto& cell = row.cell(i);
    if (cell.is_nullable() && cell.is_null()) continue;
    Slice s;
    memcpy(&s, cell.ptr(), sizeof(s));
    if (PREDICT_FALSE(s.size() > FLAGS_max_cell_size_bytes)) {
      return Status::InvalidArgument(Substitute(
          "value too large for column '$0' ($1 bytes, maximum is $2 bytes)",
          col.name(), s.size(), FLAGS_max_cell_size_bytes));
    }
  }
  // Check that the encoded key is not longer than the maximum.
  auto enc_key_size = op.key_probe->encoded_key_slice().size();
  if (PREDICT_FALSE(enc_key_size > FLAGS_max_encoded_key_size_bytes)) {
    return Status::InvalidArgument(Substitute(
        "encoded primary key too large ($0 bytes, maximum is $1 bytes)",
        enc_key_size, FLAGS_max_encoded_key_size_bytes));
  }
  return Status::OK();
}

Status Tablet::ValidateMutateUnlocked(const RowOp& op) const {
  RowChangeListDecoder rcl_decoder(op.decoded_op.changelist);
  RETURN_NOT_OK(rcl_decoder.Init());
  if (rcl_decoder.is_reinsert()) {
    // REINSERT mutations are the byproduct of an INSERT on top of a ghost
    // row, not something the user is allowed to specify on their own.
    return Status::InvalidArgument("User may not specify REINSERT mutations");
  }

  if (rcl_decoder.is_delete()) {
    // Don't validate the composite key length on delete. This is important to allow users
    // to delete a row if a row with a too-large key was inserted on a previous version
    // that had no limits.
    return Status::OK();
  }

  // For updates, just check the new cell values themselves, and not the row key,
  // following the same logic.
  while (rcl_decoder.HasNext()) {
    RowChangeListDecoder::DecodedUpdate cell_update;
    RETURN_NOT_OK(rcl_decoder.DecodeNext(&cell_update));
    if (cell_update.null) continue;
    Slice s = cell_update.raw_value;
    if (PREDICT_FALSE(s.size() > FLAGS_max_cell_size_bytes)) {
      const auto& col = schema()->column_by_id(cell_update.col_id);
      return Status::InvalidArgument(Substitute(
          "value too large for column '$0' ($1 bytes, maximum is $2 bytes)",
          col.name(), s.size(), FLAGS_max_cell_size_bytes));

    }
  }
  return Status::OK();
}

Status Tablet::InsertOrUpsertUnlocked(const IOContext* io_context,
                                      WriteTransactionState *tx_state,
                                      RowOp* op,
                                      ProbeStats* stats) {
  DCHECK(op->checked_present);
  DCHECK(op->validated);

  const bool is_upsert = op->decoded_op.type == RowOperationsPB::UPSERT;
  const TabletComponents* comps = DCHECK_NOTNULL(tx_state->tablet_components());

  if (op->present_in_rowset) {
    if (is_upsert) {
      return ApplyUpsertAsUpdate(io_context, tx_state, op, op->present_in_rowset, stats);
    }
    Status s = Status::AlreadyPresent("key already present");
    if (metrics_) {
      metrics_->insertions_failed_dup_key->Increment();
    }
    op->SetFailed(s);
    return s;
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
        return ApplyUpsertAsUpdate(io_context, tx_state, op, comps->memrowset.get(), stats);
      }
      if (metrics_) {
        metrics_->insertions_failed_dup_key->Increment();
      }
    }
    op->SetFailed(s);
  }
  return s;
}

Status Tablet::ApplyUpsertAsUpdate(const IOContext* io_context,
                                   WriteTransactionState* tx_state,
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
                               io_context,
                               stats,
                               result.get());
  CHECK(!s.IsNotFound());
  if (s.ok()) {
    if (metrics_) {
      metrics_->upserts_as_updates->Increment();
    }
    upsert->SetMutateSucceeded(std::move(result));
  } else {
    upsert->SetFailed(s);
  }
  return s;
}

vector<RowSet*> Tablet::FindRowSetsToCheck(const RowOp* op,
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

Status Tablet::MutateRowUnlocked(const IOContext* io_context,
                                 WriteTransactionState *tx_state,
                                 RowOp* mutate,
                                 ProbeStats* stats) {
  DCHECK(mutate->checked_present);
  DCHECK(mutate->validated);

  gscoped_ptr<OperationResultPB> result(new OperationResultPB());
  const TabletComponents* comps = DCHECK_NOTNULL(tx_state->tablet_components());
  Timestamp ts = tx_state->timestamp();

  // If we found the row in any existing RowSet, mutate it there. Otherwise
  // attempt to mutate in the MRS.
  RowSet* rs_to_attempt = mutate->present_in_rowset ?
      mutate->present_in_rowset : comps->memrowset.get();
  Status s = rs_to_attempt->MutateRow(ts,
                                      *mutate->key_probe,
                                      mutate->decoded_op.changelist,
                                      tx_state->op_id(),
                                      io_context,
                                      stats,
                                      result.get());
  if (PREDICT_TRUE(s.ok())) {
    mutate->SetMutateSucceeded(std::move(result));
  } else {
    if (s.IsNotFound()) {
      // Replace internal error messages with one more suitable for users.
      s = Status::NotFound("key not found");
    }
    mutate->SetFailed(s);
  }
  return s;
}

void Tablet::StartApplying(WriteTransactionState* tx_state) {
  shared_lock<rw_spinlock> l(component_lock_);
  tx_state->StartApplying();
  tx_state->set_tablet_components(components_);
}

Status Tablet::BulkCheckPresence(const IOContext* io_context, WriteTransactionState* tx_state) {
  int num_ops = tx_state->row_ops().size();

  // TODO(todd) determine why we sometimes get empty writes!
  if (PREDICT_FALSE(num_ops == 0)) return Status::OK();

  // The compiler seems to be bad at hoisting this load out of the loops,
  // so load it up top.
  RowOp* const * row_ops_base = tx_state->row_ops().data();

  // Run all of the ops through the RowSetTree.
  vector<pair<Slice, int>> keys_and_indexes;
  keys_and_indexes.reserve(num_ops);
  for (int i = 0; i < num_ops; i++) {
    RowOp* op = row_ops_base[i];
    // If the op already failed in validation, or if we've got the original result
    // filled in already during replay, then we don't need to consult the RowSetTree.
    if (op->has_result() || op->orig_result_from_log_) continue;
    keys_and_indexes.emplace_back(op->key_probe->encoded_key_slice(), i);
  }

  // Sort the query points by their probe keys, retaining the equivalent indexes.
  //
  // It's important to do a stable-sort here so that the 'unique' call
  // below retains only the _first_ op the user specified, instead of
  // an arbitrary one.
  //
  // TODO(todd): benchmark stable_sort vs using sort() and falling back to
  // comparing 'a.second' when a.first == b.first. Some microbenchmarks
  // seem to indicate stable_sort is actually faster.
  // TODO(todd): could also consider weaving in a check in the loop above to
  // see if the incoming batch is already totally-ordered and in that case
  // skip this sort and std::unique call.
  std::stable_sort(keys_and_indexes.begin(), keys_and_indexes.end(),
                   [](const pair<Slice, int>& a,
                      const pair<Slice, int>& b) {
                     return a.first.compare(b.first) < 0;
                   });
  // If the batch has more than one operation for the same row, then we can't
  // use the up-front presence optimization on those operations, since the
  // first operation may change the result of the later presence-checks.
  keys_and_indexes.erase(std::unique(
      keys_and_indexes.begin(), keys_and_indexes.end(),
      [](const pair<Slice, int>& a,
         const pair<Slice, int>& b) {
        return a.first == b.first;
      }), keys_and_indexes.end());

  // Unzip the keys into a separate array (since the RowSetTree API just wants a vector of
  // Slices)
  vector<Slice> keys(keys_and_indexes.size());
  for (int i = 0; i < keys.size(); i++) {
    keys[i] = keys_and_indexes[i].first;
  }

  // Actually perform the presence checks. We use the "bulk query" functionality
  // provided by RowSetTree::ForEachRowSetContainingKeys(), which yields results
  // via a callback, with grouping guarantees that callbacks for the same RowSet
  // will be grouped together with increasing query keys.
  //
  // We want to process each such "group" (set of subsequent calls for the same
  // RowSet) one at a time. So, the callback itself aggregates results into
  // 'pending_group' and then calls 'ProcessPendingGroup' when the next group
  // begins.
  vector<pair<RowSet*, int>> pending_group;
  Status s;
  const auto& ProcessPendingGroup = [&]() {
    if (pending_group.empty() || !s.ok()) return;
    // Check invariant of the batch RowSetTree query: within each output group
    // we should have fully-sorted keys.
    DCHECK(std::is_sorted(pending_group.begin(), pending_group.end(),
                          [&](const pair<RowSet*, int>& a,
                              const pair<RowSet*, int>& b) {
                            auto s_a = keys[a.second];
                            auto s_b = keys[b.second];
                            return s_a.compare(s_b) < 0;
                          }));
    RowSet* rs = pending_group[0].first;
    for (auto it = pending_group.begin();
         it != pending_group.end();
         ++it) {
      DCHECK_EQ(it->first, rs) << "All results within a group should be for the same RowSet";
      int op_idx = keys_and_indexes[it->second].second;
      RowOp* op = row_ops_base[op_idx];
      if (op->present_in_rowset) {
        // Already found this op present somewhere.
        continue;
      }

      bool present = false;
      s = rs->CheckRowPresent(*op->key_probe, io_context,
                              &present, tx_state->mutable_op_stats(op_idx));
      if (PREDICT_FALSE(!s.ok())) {
        LOG(WARNING) << Substitute("Tablet $0 failed to check row presence for op $1: $2",
            tablet_id(), op->ToString(key_schema_), s.ToString());
        return;
      }
      if (present) {
        op->present_in_rowset = rs;
      }
    }
    pending_group.clear();
  };

  const TabletComponents* comps = DCHECK_NOTNULL(tx_state->tablet_components());
  comps->rowsets->ForEachRowSetContainingKeys(
      keys,
      [&](RowSet* rs, int i) {
        if (!pending_group.empty() && rs != pending_group.back().first) {
          ProcessPendingGroup();
        }
        pending_group.emplace_back(rs, i);
      });
  // Process the last group.
  ProcessPendingGroup();
  RETURN_NOT_OK_PREPEND(s, "Error while checking presence of rows");

  // Mark all of the ops as having been checked.
  // TODO(todd): this could potentially be weaved into the std::unique() call up
  // above to avoid some cache misses.
  for (auto& p : keys_and_indexes) {
    row_ops_base[p.second]->checked_present = true;
  }
  return Status::OK();
}

bool Tablet::HasBeenStopped() const {
  std::lock_guard<simple_spinlock> l(state_lock_);
  return state_ == kStopped || state_ == kShutdown;
}

Status Tablet::CheckHasNotBeenStoppedUnlocked() const {
  DCHECK(state_lock_.is_locked());
  if (PREDICT_FALSE(state_ == kStopped || state_ == kShutdown)) {
    return Status::IllegalState("Tablet has been stopped");
  }
  return Status::OK();
}

Status Tablet::ApplyRowOperations(WriteTransactionState* tx_state) {
  int num_ops = tx_state->row_ops().size();

  StartApplying(tx_state);

  // Validate all of the ops.
  for (RowOp* op : tx_state->row_ops()) {
    ValidateOpOrMarkFailed(op);
  }

  IOContext io_context({ tablet_id() });
  RETURN_NOT_OK(BulkCheckPresence(&io_context, tx_state));

  // Actually apply the ops.
  for (int op_idx = 0; op_idx < num_ops; op_idx++) {
    RowOp* row_op = tx_state->row_ops()[op_idx];
    if (row_op->has_result()) continue;

    RETURN_NOT_OK(ApplyRowOperation(&io_context, tx_state, row_op,
                                    tx_state->mutable_op_stats(op_idx)));
    DCHECK(row_op->has_result());
  }

  if (metrics_ && num_ops > 0) {
    metrics_->AddProbeStats(tx_state->mutable_op_stats(0), num_ops, tx_state->arena());
  }
  return Status::OK();
}

Status Tablet::ApplyRowOperation(const IOContext* io_context,
                                 WriteTransactionState* tx_state,
                                 RowOp* row_op,
                                 ProbeStats* stats) {
  {
    std::lock_guard<simple_spinlock> l(state_lock_);
    RETURN_NOT_OK_PREPEND(CheckHasNotBeenStoppedUnlocked(),
        Substitute("Apply of $0 exited early", tx_state->ToString()));
    CHECK(state_ == kOpen || state_ == kBootstrapping);
  }
  DCHECK(row_op->has_row_lock()) << "RowOp must hold the row lock.";
  DCHECK(tx_state != nullptr) << "must have a WriteTransactionState";
  DCHECK(tx_state->op_id().IsInitialized()) << "TransactionState OpId needed for anchoring";
  DCHECK_EQ(tx_state->schema_at_decode_time(), schema());

  if (!ValidateOpOrMarkFailed(row_op)) {
    return Status::OK();
  }

  // If we were unable to check rowset presence in batch (e.g. because we are processing
  // a batch which contains some duplicate keys) we need to do so now.
  if (PREDICT_FALSE(!row_op->checked_present)) {
    vector<RowSet *> to_check = FindRowSetsToCheck(row_op, tx_state->tablet_components());
    for (RowSet *rowset : to_check) {
      bool present = false;
      RETURN_NOT_OK_PREPEND(rowset->CheckRowPresent(*row_op->key_probe, io_context,
                                                    &present, stats),
          "Failed to check if row is present");
      if (present) {
        row_op->present_in_rowset = rowset;
        break;
      }
    }
    row_op->checked_present = true;
  }

  Status s;
  switch (row_op->decoded_op.type) {
    case RowOperationsPB::INSERT:
    case RowOperationsPB::UPSERT:
      s = InsertOrUpsertUnlocked(io_context, tx_state, row_op, stats);
      if (s.IsAlreadyPresent()) {
        return Status::OK();
      }
      return s;

    case RowOperationsPB::UPDATE:
    case RowOperationsPB::DELETE:
      s = MutateRowUnlocked(io_context, tx_state, row_op, stats);
      if (s.IsNotFound()) {
        return Status::OK();
      }
      return s;

    default:
      LOG_WITH_PREFIX(FATAL) << RowOperationsPB::Type_Name(row_op->decoded_op.type);
  }
  return Status::OK();
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
                                      const shared_ptr<RowSet>& input_rs) {
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
  Status s = down_cast<DiskRowSet*>(input_rs.get())
      ->MajorCompactDeltaStoresWithColumnIds(col_ids, nullptr, GetHistoryGcOpts());
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
  RETURN_NOT_OK(CheckHasNotBeenStopped());
  RowSetsInCompaction input;
  shared_ptr<MemRowSet> old_mrs;
  {
    // Create a new MRS with the latest schema.
    std::lock_guard<rw_spinlock> lock(component_lock_);
    RETURN_NOT_OK(ReplaceMemRowSetUnlocked(&input, &old_mrs));
  }

  // Wait for any in-flight transactions to finish against the old MRS
  // before we flush it.
  //
  // This may fail if the tablet has been stopped.
  RETURN_NOT_OK(mvcc_.WaitForApplyingTransactionsToCommit());

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

  shared_ptr<MemRowSet> new_mrs;
  RETURN_NOT_OK(MemRowSet::Create(next_mrs_id_++, *schema(),
                                  log_anchor_registry_.get(),
                                  mem_trackers_.tablet_tracker,
                                  &new_mrs));
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
  {
    std::lock_guard<simple_spinlock> l(state_lock_);
    RETURN_NOT_OK(CheckHasNotBeenStoppedUnlocked());
    CHECK(state_ == kOpen || state_ == kBootstrapping);
  }

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
    LOG_WITH_PREFIX(INFO) << "MemRowSet was empty: no flush needed.";
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
  DCHECK(key_schema_.KeyTypeEquals(*DCHECK_NOTNULL(tx_state->schema()))) <<
    "Schema keys cannot be altered(except name)";

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
  RETURN_IF_STOPPED_OR_CHECK_STATE(kBootstrapping);

  // We know that the MRS should be empty at this point, because we
  // rewind the schema before replaying any operations. So, we just
  // swap in a new one with the correct schema, rather than attempting
  // to flush.
  VLOG_WITH_PREFIX(1) << "Rewinding schema during bootstrap to " << new_schema.ToString();

  metadata_->SetSchema(new_schema, schema_version);
  {
    std::lock_guard<rw_spinlock> lock(component_lock_);

    shared_ptr<MemRowSet> old_mrs = components_->memrowset;
    shared_ptr<RowSetTree> old_rowsets = components_->rowsets;
    CHECK(old_mrs->empty());
    shared_ptr<MemRowSet> new_mrs;
    RETURN_NOT_OK(MemRowSet::Create(old_mrs->mrs_id(), new_schema,
                                    log_anchor_registry_.get(),
                                    mem_trackers_.tablet_tracker,
                                    &new_mrs));
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
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
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

  // When we iterated through the current rowsets, we should have found all of
  // the rowsets that we picked. If we didn't, that implies that some other
  // thread swapped them out while we were making our selection decision --
  // that's not possible since we only picked rowsets that were marked as
  // available for compaction.
  if (!picked_set.empty()) {
    for (const RowSet* not_found : picked_set) {
      LOG_WITH_PREFIX(ERROR) << "Rowset selected for compaction but not available anymore: "
                             << not_found->ToString();
    }
    const char* msg = "Was unable to find all rowsets selected for compaction";
    LOG_WITH_PREFIX(DFATAL) << msg;
    return Status::RuntimeError(msg);
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
  // This method must be externally synchronized to not coincide with other
  // calls to it or to UnregisterMaintenanceOps.
  DFAKE_SCOPED_LOCK(maintenance_registration_fake_lock_);
  {
    std::lock_guard<simple_spinlock> l(state_lock_);
    if (state_ == kStopped || state_ == kShutdown) {
      LOG(WARNING) << "Could not register maintenance ops";
      return;
    }
    CHECK_EQ(kOpen, state_);
    DCHECK(maintenance_ops_.empty());
  }

  vector<MaintenanceOp*> maintenance_ops;
  gscoped_ptr<MaintenanceOp> rs_compact_op(new CompactRowSetsOp(this));
  maint_mgr->RegisterOp(rs_compact_op.get());
  maintenance_ops.push_back(rs_compact_op.release());

  gscoped_ptr<MaintenanceOp> minor_delta_compact_op(new MinorDeltaCompactionOp(this));
  maint_mgr->RegisterOp(minor_delta_compact_op.get());
  maintenance_ops.push_back(minor_delta_compact_op.release());

  gscoped_ptr<MaintenanceOp> major_delta_compact_op(new MajorDeltaCompactionOp(this));
  maint_mgr->RegisterOp(major_delta_compact_op.get());
  maintenance_ops.push_back(major_delta_compact_op.release());

  if (FLAGS_enable_undo_delta_block_gc) {
    gscoped_ptr<MaintenanceOp> undo_delta_block_gc_op(new UndoDeltaBlockGCOp(this));
    maint_mgr->RegisterOp(undo_delta_block_gc_op.get());
    maintenance_ops.push_back(undo_delta_block_gc_op.release());
  }

  std::lock_guard<simple_spinlock> l(state_lock_);
  maintenance_ops_.swap(maintenance_ops);
}

void Tablet::UnregisterMaintenanceOps() {
  // This method must be externally synchronized to not coincide with other
  // calls to it or to RegisterMaintenanceOps.
  DFAKE_SCOPED_LOCK(maintenance_registration_fake_lock_);

  // First cancel all of the operations, so that while we're waiting for one
  // operation to finish in Unregister(), a different one can't get re-scheduled.
  CancelMaintenanceOps();

  // We don't lock here because unregistering ops may take a long time.
  // 'maintenance_registration_fake_lock_' is sufficient to ensure nothing else
  // is updating 'maintenance_ops_'.
  for (MaintenanceOp* op : maintenance_ops_) {
    op->Unregister();
  }

  // Finally, delete the ops under lock.
  std::lock_guard<simple_spinlock> l(state_lock_);
  STLDeleteElements(&maintenance_ops_);
}

void Tablet::CancelMaintenanceOps() {
  std::lock_guard<simple_spinlock> l(state_lock_);
  for (MaintenanceOp* op : maintenance_ops_) {
    op->CancelAndDisable();
  }
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

  const IOContext io_context({ tablet_id() });

  MvccSnapshot flush_snap(mvcc_);
  LOG_WITH_PREFIX(INFO) << op_name << ": entering phase 1 (flushing snapshot). Phase 1 snapshot: "
                        << flush_snap.ToString();

  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostTakeMvccSnapshot(),
                          "PostTakeMvccSnapshot hook failed");
  }

  shared_ptr<CompactionInput> merge;
  RETURN_NOT_OK(input.CreateCompactionInput(flush_snap, schema(), &io_context, &merge));

  RollingDiskRowSetWriter drsw(metadata_.get(), merge->schema(), DefaultBloomSizing(),
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

  // Though unlikely, it's possible that no rows were written because all of
  // the input rows were GCed in this compaction. In that case, we don't
  // actually want to reopen.
  if (drsw.rows_written_count() == 0) {
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
      Status s = DiskRowSet::Open(meta,
                                  log_anchor_registry_.get(),
                                  mem_trackers_,
                                  &io_context,
                                  &new_rowset);
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
  RETURN_NOT_OK(mvcc_.WaitForApplyingTransactionsToCommit());

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
      input.CreateCompactionInput(non_duplicated_txns_snap, schema(), &io_context, &merge),
          Substitute("Failed to create $0 inputs", op_name).c_str());

  // Update the output rowsets with the deltas that came in in phase 1, before we swapped
  // in the DuplicatingRowSets. This will perform a flush of the updated DeltaTrackers
  // in the end so that the data that is reported in the log as belonging to the input
  // rowsets is flushed.
  RETURN_NOT_OK_PREPEND(ReupdateMissedDeltas(&io_context,
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
      input.rowsets()[0]->OnDiskBaseDataSizeWithRedos() == 0) {
    MAYBE_FAULT(FLAGS_fault_crash_before_flush_tablet_meta_after_flush_mrs);
  }

  // Write out the new Tablet Metadata and remove old rowsets.
  RETURN_NOT_OK_PREPEND(FlushMetadata(input.rowsets(), new_drs_metas, mrs_being_flushed),
                        "Failed to flush new tablet metadata");

  // Now that we've completed the operation, mark any rowsets that have been
  // compacted, preventing them from being considered for future compactions.
  for (const auto& rs : input.rowsets()) {
    rs->set_has_been_compacted();
  }

  // Replace the compacted rowsets with the new on-disk rowsets, making them visible now that
  // their metadata was written to disk.
  AtomicSwapRowSets({ inprogress_rowset }, new_disk_rowsets);

  LOG_WITH_PREFIX(INFO) << Substitute("$0 successful on $1 rows ($2 rowsets, $3 bytes)",
                                      op_name,
                                      drsw.rows_written_count(),
                                      drsw.drs_written_count(),
                                      drsw.written_size());

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
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);

  RowSetsInCompaction input;
  // Step 1. Capture the rowsets to be merged
  RETURN_NOT_OK_PREPEND(PickRowSetsToCompact(&input, flags),
                        "Failed to pick rowsets to compact");
  LOG_WITH_PREFIX(INFO) << "Compaction: stage 1 complete, picked "
                        << input.num_rowsets() << " rowsets to compact or flush";
  if (compaction_hooks_) {
    RETURN_NOT_OK_PREPEND(compaction_hooks_->PostSelectIterators(),
                          "PostSelectIterators hook failed");
  }

  input.DumpToLog();

  return DoMergeCompactionOrFlush(input, TabletMetadata::kNoMrsFlushed);
}

void Tablet::UpdateCompactionStats(MaintenanceOpStats* stats) {

  if (mvcc_.GetCleanTimestamp() == Timestamp::kInitialTimestamp) {
    KLOG_EVERY_N_SECS(WARNING, 30) << LogPrefix() <<  "Can't schedule compaction. Clean time has "
                                   << "not been advanced past its initial value.";
    stats->set_runnable(false);
    return;
  }

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
    const Schema* projection,
    const MvccSnapshot& snap,
    const ScanSpec* spec,
    OrderMode order,
    const IOContext* io_context,
    vector<shared_ptr<RowwiseIterator>>* iters) const {

  shared_lock<rw_spinlock> l(component_lock_);
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);

  // Construct all the iterators locally first, so that if we fail
  // in the middle, we don't modify the output arguments.
  vector<shared_ptr<RowwiseIterator>> ret;

  RowIteratorOptions opts;
  opts.projection = projection;
  opts.snap_to_include = snap;
  opts.order = order;

  // Grab the memrowset iterator.
  gscoped_ptr<RowwiseIterator> ms_iter;
  RETURN_NOT_OK(components_->memrowset->NewRowIterator(opts, &ms_iter));
  ret.emplace_back(ms_iter.release());

  opts.io_context = io_context;

  // Cull row-sets in the case of key-range queries.
  if (spec != nullptr && spec->lower_bound_key() && spec->exclusive_upper_bound_key()) {
    // TODO(todd): support open-ended intervals
    // TODO(todd): the upper bound key is exclusive, but the RowSetTree
    // function takes an inclusive interval. So, we might end up fetching one
    // more rowset than necessary.
    vector<RowSet *> interval_sets;
    components_->rowsets->FindRowSetsIntersectingInterval(
        spec->lower_bound_key()->encoded_key(),
        spec->exclusive_upper_bound_key()->encoded_key(),
        &interval_sets);
    for (const RowSet *rs : interval_sets) {
      gscoped_ptr<RowwiseIterator> row_it;
      RETURN_NOT_OK_PREPEND(rs->NewRowIterator(opts, &row_it),
                            Substitute("Could not create iterator for rowset $0",
                                       rs->ToString()));
      ret.emplace_back(row_it.release());
    }
    ret.swap(*iters);
    return Status::OK();
  }

  // If there are no encoded predicates or they represent an open-ended range, then
  // fall back to grabbing all rowset iterators
  for (const shared_ptr<RowSet> &rs : components_->rowsets->all_rowsets()) {
    gscoped_ptr<RowwiseIterator> row_it;
    RETURN_NOT_OK_PREPEND(rs->NewRowIterator(opts, &row_it),
                          Substitute("Could not create iterator for rowset $0",
                                     rs->ToString()));
    ret.emplace_back(row_it.release());
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
  IOContext io_context({ tablet_id() });
  *count = comps->memrowset->entry_count();
  for (const shared_ptr<RowSet> &rowset : comps->rowsets->all_rowsets()) {
    rowid_t l_count;
    RETURN_NOT_OK(rowset->CountRows(&io_context, &l_count));
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

size_t Tablet::MemRowSetLogReplaySize(const ReplaySizeMap& replay_size_map) const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  return GetReplaySizeForIndex(comps->memrowset->MinUnflushedLogIndex(), replay_size_map);
}

size_t Tablet::OnDiskSize() const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  if (!comps) return 0;

  size_t ret = metadata()->on_disk_size();
  for (const shared_ptr<RowSet> &rowset : comps->rowsets->all_rowsets()) {
    ret += rowset->OnDiskSize();
  }

  return ret;
}

size_t Tablet::OnDiskDataSize() const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  if (!comps) return 0;

  size_t ret = 0;
  for (const shared_ptr<RowSet> &rowset : comps->rowsets->all_rowsets()) {
    ret += rowset->OnDiskBaseDataSize();
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

void Tablet::GetInfoForBestDMSToFlush(const ReplaySizeMap& replay_size_map,
                                      int64_t* mem_size, int64_t* replay_size) const {
  shared_ptr<RowSet> rowset = FindBestDMSToFlush(replay_size_map);

  if (rowset) {
    *replay_size = GetReplaySizeForIndex(rowset->MinUnflushedLogIndex(),
                                         replay_size_map);
    *mem_size = rowset->DeltaMemStoreSize();
  } else {
    *replay_size = 0;
    *mem_size = 0;
  }
}

Status Tablet::FlushBestDMS(const ReplaySizeMap &replay_size_map) const {
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
  shared_ptr<RowSet> rowset = FindBestDMSToFlush(replay_size_map);
  if (rowset) {
    IOContext io_context({ tablet_id() });
    return rowset->FlushDeltas(&io_context);
  }
  return Status::OK();
}

shared_ptr<RowSet> Tablet::FindBestDMSToFlush(const ReplaySizeMap& replay_size_map) const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);
  int64_t mem_size = 0;
  double max_score = 0;
  double mem_weight = 0;
  // If system is under memory pressure, we use the percentage of the hard limit consumed
  // as mem_weight, so the tighter memory, the higher weight. Otherwise just left the
  // mem_weight to 0.
  process_memory::UnderMemoryPressure(&mem_weight);

  shared_ptr<RowSet> best_dms;
  for (const shared_ptr<RowSet> &rowset : comps->rowsets->all_rowsets()) {
    if (rowset->DeltaMemStoreEmpty()) {
      continue;
    }
    int64_t size = GetReplaySizeForIndex(rowset->MinUnflushedLogIndex(),
                                         replay_size_map);
    int64_t mem = rowset->DeltaMemStoreSize();
    double score = mem * mem_weight + size * (100 - mem_weight);

    if ((score > max_score) ||
        (score > max_score - 1 && mem > mem_size)) {
      max_score = score;
      mem_size = mem;
      best_dms = rowset;
    }
  }
  return best_dms;
}

int64_t Tablet::GetReplaySizeForIndex(int64_t min_log_index,
                                      const ReplaySizeMap& size_map) {
  // If min_log_index is -1, that indicates that there is no anchor held
  // for the tablet, and therefore no logs would need to be replayed.
  if (size_map.empty() || min_log_index == -1) {
    return 0;
  }

  const auto& it = size_map.lower_bound(min_log_index);
  if (it == size_map.end()) {
    return 0;
  }
  return it->second;
}

Status Tablet::FlushBiggestDMS() {
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
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
  return max_size > 0 ? biggest_drs->FlushDeltas(nullptr) : Status::OK();
}

Status Tablet::FlushAllDMSForTests() {
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);
  for (const auto& rowset : comps->rowsets->all_rowsets()) {
    RETURN_NOT_OK(rowset->FlushDeltas(nullptr));
  }
  return Status::OK();
}

Status Tablet::MajorCompactAllDeltaStoresForTests() {
  LOG_WITH_PREFIX(INFO) << "Major compacting all delta stores, for tests";
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);
  IOContext io_context({ tablet_id() });
  for (const auto& rs : comps->rowsets->all_rowsets()) {
    if (!rs->IsAvailableForCompaction()) continue;
    DiskRowSet* drs = down_cast<DiskRowSet*>(rs.get());
    RETURN_NOT_OK(drs->delta_tracker()->InitAllDeltaStoresForTests(DeltaTracker::REDOS_ONLY));
    RETURN_NOT_OK_PREPEND(drs->MajorCompactDeltaStores(&io_context, GetHistoryGcOpts()),
                          "Failed major delta compaction on " + rs->ToString());
  }
  return Status::OK();
}

Status Tablet::CompactWorstDeltas(RowSet::DeltaCompactionType type) {
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
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
  IOContext io_context({ tablet_id() });
  if (type == RowSet::MINOR_DELTA_COMPACTION) {
    RETURN_NOT_OK_PREPEND(rs->MinorCompactDeltaStores(&io_context),
                          "Failed minor delta compaction on " + rs->ToString());
  } else if (type == RowSet::MAJOR_DELTA_COMPACTION) {
    RETURN_NOT_OK_PREPEND(
        down_cast<DiskRowSet*>(rs.get())->MajorCompactDeltaStores(&io_context, GetHistoryGcOpts()),
        "Failed major delta compaction on " + rs->ToString());
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

Status Tablet::EstimateBytesInPotentiallyAncientUndoDeltas(int64_t* bytes) {
  DCHECK(bytes);

  Timestamp ancient_history_mark;
  if (!Tablet::GetTabletAncientHistoryMark(&ancient_history_mark)) {
    VLOG_WITH_PREFIX(1) << "Cannot get ancient history mark. "
                           "The clock is likely not a hybrid clock";
    return Status::OK();
  }

  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  int64_t tablet_bytes = 0;
  for (const auto& rowset : comps->rowsets->all_rowsets()) {
    int64_t rowset_bytes;
    RETURN_NOT_OK(rowset->EstimateBytesInPotentiallyAncientUndoDeltas(ancient_history_mark,
                                                                      &rowset_bytes));
    tablet_bytes += rowset_bytes;
  }

  metrics_->undo_delta_block_estimated_retained_bytes->set_value(tablet_bytes);
  *bytes = tablet_bytes;
  return Status::OK();
}

Status Tablet::InitAncientUndoDeltas(MonoDelta time_budget, int64_t* bytes_in_ancient_undos) {
  MonoTime tablet_init_start = MonoTime::Now();

  IOContext io_context({ tablet_id() });
  Timestamp ancient_history_mark;
  if (!Tablet::GetTabletAncientHistoryMark(&ancient_history_mark)) {
    VLOG_WITH_PREFIX(1) << "Cannot get ancient history mark. "
                           "The clock is likely not a hybrid clock";
    return Status::OK();
  }

  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  RowSetVector rowsets = comps->rowsets->all_rowsets();

  // Estimate the size of the ancient undos in each rowset so that we can
  // initialize them greedily.
  vector<pair<size_t, int64_t>> rowset_ancient_undos_est_sizes; // index, bytes
  rowset_ancient_undos_est_sizes.reserve(rowsets.size());
  for (size_t i = 0; i < rowsets.size(); i++) {
    const auto& rowset = rowsets[i];
    int64_t bytes;
    RETURN_NOT_OK(rowset->EstimateBytesInPotentiallyAncientUndoDeltas(ancient_history_mark,
                                                                      &bytes));
    rowset_ancient_undos_est_sizes.emplace_back(i, bytes);
  }

  // Sort the rowsets in descending size order to optimize for the worst offenders.
  std::sort(rowset_ancient_undos_est_sizes.begin(), rowset_ancient_undos_est_sizes.end(),
            [&](const pair<size_t, int64_t>& a, const pair<size_t, int64_t>& b) {
              return a.second > b.second; // Descending order.
            });

  // Begin timeout / deadline countdown here in case the above takes some time.
  MonoTime deadline = time_budget.Initialized() ? MonoTime::Now() + time_budget : MonoTime();

  // Initialize the rowsets largest-first.
  int64_t tablet_bytes_in_ancient_undos = 0;
  for (const auto& rs_est_size : rowset_ancient_undos_est_sizes) {
    size_t index = rs_est_size.first;
    const auto& rowset = rowsets[index];
    int64_t rowset_blocks_initialized;
    int64_t rowset_bytes_in_ancient_undos;
    RETURN_NOT_OK(rowset->InitUndoDeltas(ancient_history_mark, deadline, &io_context,
                                         &rowset_blocks_initialized,
                                         &rowset_bytes_in_ancient_undos));
    tablet_bytes_in_ancient_undos += rowset_bytes_in_ancient_undos;
  }

  MonoDelta tablet_init_duration = MonoTime::Now() - tablet_init_start;
  metrics_->undo_delta_block_gc_init_duration->Increment(
      tablet_init_duration.ToMilliseconds());

  VLOG_WITH_PREFIX(2) << Substitute("Bytes in ancient undos: $0. Init duration: $1",
                                    HumanReadableNumBytes::ToString(tablet_bytes_in_ancient_undos),
                                    tablet_init_duration.ToString());

  if (bytes_in_ancient_undos) *bytes_in_ancient_undos = tablet_bytes_in_ancient_undos;
  return Status::OK();
}

Status Tablet::DeleteAncientUndoDeltas(int64_t* blocks_deleted, int64_t* bytes_deleted) {
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
  MonoTime tablet_delete_start = MonoTime::Now();

  Timestamp ancient_history_mark;
  if (!Tablet::GetTabletAncientHistoryMark(&ancient_history_mark)) return Status::OK();

  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  // We need to hold the compact_flush_lock for each rowset we GC undos from.
  RowSetVector rowsets_to_gc_undos;
  vector<std::unique_lock<std::mutex>> rowset_locks;
  {
    // We hold the selection lock so other threads will not attempt to select the
    // same rowsets for compaction while we delete old undos.
    std::lock_guard<std::mutex> compact_lock(compact_select_lock_);
    for (const auto& rowset : comps->rowsets->all_rowsets()) {
      if (!rowset->IsAvailableForCompaction()) {
        continue;
      }
      std::unique_lock<std::mutex> lock(*rowset->compact_flush_lock(), std::try_to_lock);
      CHECK(lock.owns_lock()) << rowset->ToString() << " unable to lock compact_flush_lock";
      rowsets_to_gc_undos.push_back(rowset);
      rowset_locks.push_back(std::move(lock));
    }
  }

  int64_t tablet_blocks_deleted = 0;
  int64_t tablet_bytes_deleted = 0;
  fs::IOContext io_context({ tablet_id() });
  for (const auto& rowset : rowsets_to_gc_undos) {
    int64_t rowset_blocks_deleted;
    int64_t rowset_bytes_deleted;
    RETURN_NOT_OK(rowset->DeleteAncientUndoDeltas(ancient_history_mark, &io_context,
                                                  &rowset_blocks_deleted, &rowset_bytes_deleted));
    tablet_blocks_deleted += rowset_blocks_deleted;
    tablet_bytes_deleted += rowset_bytes_deleted;
  }
  // We flush the tablet metadata at the end because we don't flush per-RowSet
  // for performance reasons.
  if (tablet_blocks_deleted > 0) {
    RETURN_NOT_OK(metadata_->Flush());
  }

  MonoDelta tablet_delete_duration = MonoTime::Now() - tablet_delete_start;
  metrics_->undo_delta_block_gc_bytes_deleted->IncrementBy(tablet_bytes_deleted);
  metrics_->undo_delta_block_gc_delete_duration->Increment(
      tablet_delete_duration.ToMilliseconds());

  if (blocks_deleted) *blocks_deleted = tablet_blocks_deleted;
  if (bytes_deleted) *bytes_deleted = tablet_bytes_deleted;
  return Status::OK();
}

int64_t Tablet::CountUndoDeltasForTests() const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);
  int64_t sum = 0;
  for (const auto& rowset : comps->rowsets->all_rowsets()) {
    shared_ptr<RowSetMetadata> metadata = rowset->metadata();
    if (metadata) {
      sum += metadata->undo_delta_blocks().size();
    }
  }
  return sum;
}

int64_t Tablet::CountRedoDeltasForTests() const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);
  int64_t sum = 0;
  for (const auto& rowset : comps->rowsets->all_rowsets()) {
    shared_ptr<RowSetMetadata> metadata = rowset->metadata();
    if (metadata) {
      sum += metadata->redo_delta_blocks().size();
    }
  }
  return sum;
}

size_t Tablet::num_rowsets() const {
  shared_lock<rw_spinlock> l(component_lock_);
  return components_ ? components_->rowsets->all_rowsets().size() : 0;
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
  return Substitute("T $0 P $1: ", tablet_id(), metadata_->fs_manager()->uuid());
}

////////////////////////////////////////////////////////////
// Tablet::Iterator
////////////////////////////////////////////////////////////

Tablet::Iterator::Iterator(const Tablet* tablet, const Schema& projection,
                           MvccSnapshot snap, OrderMode order,
                           IOContext io_context)
    : tablet_(tablet),
      projection_(projection),
      snap_(std::move(snap)),
      order_(order),
      io_context_(std::move(io_context)) {}

Tablet::Iterator::~Iterator() {}

Status Tablet::Iterator::Init(ScanSpec *spec) {
  RETURN_NOT_OK(tablet_->CheckHasNotBeenStopped());
  DCHECK(iter_.get() == nullptr);

  RETURN_NOT_OK(tablet_->GetMappedReadProjection(projection_, &projection_));

  vector<shared_ptr<RowwiseIterator>> iters;
  RETURN_NOT_OK(tablet_->CaptureConsistentIterators(&projection_, snap_, spec, order_,
                                                    &io_context_, &iters));
  TRACE_COUNTER_INCREMENT("rowset_iterators", iters.size());

  switch (order_) {
    case ORDERED:
      iter_.reset(new MergeIterator(projection_, std::move(iters)));
      break;
    case UNORDERED:
    default:
      iter_.reset(new UnionIterator(std::move(iters)));
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
