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
#include <ctime>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <random>
#include <shared_mutex>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/arena.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/generic_iterators.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partition.h"
#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/rowid.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/io_context.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/threading/thread_collision_warner.h"
#include "kudu/tablet/compaction.h"
#include "kudu/tablet/compaction_policy.h"
#include "kudu/tablet/delta_tracker.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/memrowset.h"
#include "kudu/tablet/ops/alter_schema_op.h"
#include "kudu/tablet/ops/participant_op.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/row_op.h"
#include "kudu/tablet/rowset_info.h"
#include "kudu/tablet/rowset_tree.h"
#include "kudu/tablet/svg_dump.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet_mm_ops.h"
#include "kudu/tablet/txn_metadata.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/bloom_filter.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/faststring.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/slice.h"
#include "kudu/util/status_callback.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/throttler.h"
#include "kudu/util/trace.h"
#include "kudu/util/url-coding.h"

DEFINE_bool(prevent_kudu_2233_corruption, true,
            "Whether or not to prevent KUDU-2233 corruptions. Used for testing only!");
TAG_FLAG(prevent_kudu_2233_corruption, unsafe);

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

DEFINE_int32(tablet_history_max_age_sec, 60 * 60 * 24 * 7,
             "Number of seconds to retain tablet history, including history "
             "required to perform diff scans and incremental backups. Reads "
             "initiated at a snapshot that is older than this age will be "
             "rejected. To disable history removal, set to -1.");
TAG_FLAG(tablet_history_max_age_sec, advanced);
TAG_FLAG(tablet_history_max_age_sec, runtime);
TAG_FLAG(tablet_history_max_age_sec, stable);

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

DEFINE_int32(workload_stats_rate_collection_min_interval_ms, 60 * 1000,
             "The minimal interval in milliseconds at which we collect read/write rates.");
TAG_FLAG(workload_stats_rate_collection_min_interval_ms, experimental);
TAG_FLAG(workload_stats_rate_collection_min_interval_ms, runtime);

DEFINE_int32(workload_stats_metric_collection_interval_ms, 5 * 60 * 1000,
             "The interval in milliseconds at which we collect workload metrics.");
TAG_FLAG(workload_stats_metric_collection_interval_ms, experimental);
TAG_FLAG(workload_stats_metric_collection_interval_ms, runtime);

DEFINE_double(workload_score_upper_bound, 1.0, "Upper bound for workload score.");
TAG_FLAG(workload_score_upper_bound, experimental);
TAG_FLAG(workload_score_upper_bound, runtime);

DEFINE_int32(scans_started_per_sec_for_hot_tablets, 1,
    "Minimum read rate for tablets to be considered 'hot' (scans/sec). If a tablet's "
    "read rate exceeds this value, flush/compaction ops for it will be assigned the highest "
    "possible workload score, which is defined by --workload_score_upper_bound.");
TAG_FLAG(scans_started_per_sec_for_hot_tablets, experimental);
TAG_FLAG(scans_started_per_sec_for_hot_tablets, runtime);

DEFINE_int32(rows_writed_per_sec_for_hot_tablets, 1000,
    "Minimum write rate for tablets to be considered 'hot' (rows/sec). If a tablet's "
    "write rate exceeds this value, compaction ops for it will be assigned the highest "
    "possible workload score, which is defined by --workload_score_upper_bound.");
TAG_FLAG(rows_writed_per_sec_for_hot_tablets, experimental);
TAG_FLAG(rows_writed_per_sec_for_hot_tablets, runtime);

DEFINE_double(rowset_compaction_ancient_delta_max_ratio, 0.2,
              "The ratio of data in ancient UNDO deltas to the total amount "
              "of data in all deltas across rowsets picked for rowset merge "
              "compaction used as a threshold to determine whether to run "
              "the operation when --rowset_compaction_ancient_delta_max_ratio "
              "is set to 'true'. If the ratio is greater than the threshold "
              "defined by this flag, CompactRowSetsOp operations are postponed "
              "until UndoDeltaBlockGCOp purges enough of ancient UNDO deltas.");
TAG_FLAG(rowset_compaction_ancient_delta_max_ratio, advanced);
TAG_FLAG(rowset_compaction_ancient_delta_max_ratio, runtime);

DEFINE_bool(rowset_compaction_ancient_delta_threshold_enabled, true,
            "Whether to check the ratio of data in ancient UNDO deltas against "
            "the threshold set by --rowset_compaction_ancient_delta_max_ratio "
            "before running rowset merge compaction. If the ratio of ancient "
            "data in UNDO deltas is greater than the threshold, postpone "
            "running CompactRowSetsOp until UndoDeltaBlockGCOp purges ancient "
            "data and the ratio drops below the threshold (NOTE: regardless of "
            "the setting, the effective "
            "value of this flag becomes 'false' if "
            "--enable_undo_delta_block_gc is set to 'false')");
TAG_FLAG(rowset_compaction_ancient_delta_threshold_enabled, advanced);
TAG_FLAG(rowset_compaction_ancient_delta_threshold_enabled, runtime);

DEFINE_bool(enable_gc_deleted_rowsets_without_live_row_count, false,
            "Whether to enable 'DeletedRowsetGCOp' for ancient, fully deleted "
            "rowsets without live row count stats. This is used to release "
            "the storage space of ancient, fully deleted rowsets generated "
            "by Kudu clusters that do not have live row count stats. "
            "If live row count feature is already supported in your kudu "
            "cluster, just ignore this flag.");
TAG_FLAG(enable_gc_deleted_rowsets_without_live_row_count, advanced);

DECLARE_bool(enable_undo_delta_block_gc);
DECLARE_uint32(rowset_compaction_estimate_min_deltas_size_mb);

METRIC_DEFINE_entity(tablet);
METRIC_DEFINE_gauge_size(tablet, memrowset_size, "MemRowSet Memory Usage",
                         kudu::MetricUnit::kBytes,
                         "Size of this tablet's memrowset",
                         kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_size(tablet, on_disk_data_size, "Tablet Data Size On Disk",
                         kudu::MetricUnit::kBytes,
                         "Space used by this tablet's data blocks.",
                         kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_size(tablet, num_rowsets_on_disk, "Tablet Number of Rowsets on Disk",
                         kudu::MetricUnit::kUnits,
                         "Number of diskrowsets in this tablet",
                         kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_uint64(tablet, last_read_elapsed_time, "Seconds Since Last Read",
                           kudu::MetricUnit::kSeconds,
                           "The elapsed time, in seconds, since the last read operation on this "
                           "tablet, or since this Tablet object was created on current tserver if "
                           "it hasn't been read since then.",
                           kudu::MetricLevel::kDebug);
METRIC_DEFINE_gauge_uint64(tablet, last_write_elapsed_time, "Seconds Since Last Write",
                           kudu::MetricUnit::kSeconds,
                           "The elapsed time, in seconds, since the last write operation on this "
                           "tablet, or since this Tablet object was created on current tserver if "
                           "it hasn't been written to since then.",
                           kudu::MetricLevel::kDebug);

using kudu::MaintenanceManager;
using kudu::clock::HybridClock;
using kudu::consensus::OpId;
using kudu::fs::IOContext;
using kudu::log::LogAnchorRegistry;
using kudu::log::MinLogIndexAnchorer;
using std::endl;
using std::make_shared;
using std::make_unique;
using std::nullopt;
using std::optional;
using std::ostream;
using std::pair;
using std::shared_lock;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace {

bool ValidateAncientDeltaMaxRatio(const char* flag, double val) {
  constexpr double kMinVal = 0.0;
  constexpr double kMaxVal = 1.0;
  if (val < kMinVal || val > kMaxVal) {
    LOG(ERROR) << Substitute(
        "$0: invalid value for --$1 flag, should be between $2 and $3",
        val, flag, kMinVal, kMaxVal);
    return false;
  }
  return true;
}
DEFINE_validator(rowset_compaction_ancient_delta_max_ratio,
                 &ValidateAncientDeltaMaxRatio);

bool ValidateRowsetCompactionGuard() {
  if (FLAGS_rowset_compaction_ancient_delta_threshold_enabled &&
      !FLAGS_enable_undo_delta_block_gc) {
    LOG(WARNING) << Substitute(
        "rowset compaction ancient ratio threshold is enabled "
        "but UNDO delta block GC is disabled: check current settings of "
        "--rowset_compaction_ancient_delta_threshold_enabled and "
        "--enable_undo_delta_block_gc flags");
  }
  return true;
}
GROUP_FLAG_VALIDATOR(rowset_compaction, &ValidateRowsetCompactionGuard);

} // anonymous namespace

namespace kudu {

struct IteratorStats;

namespace tablet {

class RowSetMetadata;

////////////////////////////////////////////////////////////
// TabletComponents
////////////////////////////////////////////////////////////

TabletComponents::TabletComponents(shared_ptr<MemRowSet> mrs,
                                   std::vector<std::shared_ptr<MemRowSet>> txn_mrss,
                                   shared_ptr<RowSetTree> rs_tree)
    : memrowset(std::move(mrs)),
      txn_memrowsets(std::move(txn_mrss)),
      rowsets(std::move(rs_tree)) {}

////////////////////////////////////////////////////////////
// Tablet
////////////////////////////////////////////////////////////

Tablet::Tablet(scoped_refptr<TabletMetadata> metadata,
               clock::Clock* clock,
               shared_ptr<MemTracker> parent_mem_tracker,
               MetricRegistry* metric_registry,
               scoped_refptr<LogAnchorRegistry> log_anchor_registry)
    : key_schema_(metadata->schema()->CreateKeyProjection()),
      metadata_(std::move(metadata)),
      log_anchor_registry_(std::move(log_anchor_registry)),
      mem_trackers_(tablet_id(), std::move(parent_mem_tracker)),
      next_mrs_id_(0),
      auto_incrementing_counter_(0),
      clock_(clock),
      txn_participant_(metadata_),
      rowsets_flush_sem_(1),
      state_(kInitialized),
      last_read_time_(MonoTime::Now()),
      last_write_time_(last_read_time_),
      last_update_workload_stats_time_(last_read_time_),
      last_scans_started_(0),
      last_rows_mutated_(0),
      last_read_score_(0.0),
      last_write_score_(0.0) {
  CHECK(schema()->has_column_ids());

  if (metric_registry) {
    MetricEntity::AttributeMap attrs;
    attrs["table_id"] = metadata_->table_id();
    attrs["table_name"] = metadata_->table_name();
    attrs["partition"] = metadata_->partition_schema().PartitionDebugString(metadata_->partition(),
                                                                            *schema());
    metric_entity_ = METRIC_ENTITY_tablet.Instantiate(metric_registry, tablet_id(), attrs);
    metrics_.reset(new TabletMetrics(metric_entity_));
    METRIC_memrowset_size.InstantiateFunctionGauge(
        metric_entity_, [this]() { return this->MemRowSetSize(); })
        ->AutoDetach(&metric_detacher_);
    METRIC_on_disk_data_size.InstantiateFunctionGauge(
        metric_entity_, [this]() { return this->OnDiskDataSize(); })
        ->AutoDetach(&metric_detacher_);
    METRIC_num_rowsets_on_disk.InstantiateFunctionGauge(
        metric_entity_, [this]() { return this->num_rowsets(); })
        ->AutoDetach(&metric_detacher_);
    METRIC_last_read_elapsed_time.InstantiateFunctionGauge(
        metric_entity_, [this]() { return this->LastReadElapsedSeconds(); },
        MergeType::kMin)
        ->AutoDetach(&metric_detacher_);
    METRIC_last_write_elapsed_time.InstantiateFunctionGauge(
        metric_entity_, [this]() { return this->LastWriteElapsedSeconds(); },
        MergeType::kMin)
        ->AutoDetach(&metric_detacher_);
  }

  compaction_policy_.reset(new BudgetedCompactionPolicy(
      FLAGS_tablet_compaction_budget_mb, metrics_.get()));

  if (FLAGS_tablet_throttler_rpc_per_sec > 0 || FLAGS_tablet_throttler_bytes_per_sec > 0) {
    throttler_.reset(new Throttler(FLAGS_tablet_throttler_rpc_per_sec,
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
  State _local_state; \
  RETURN_NOT_OK(CheckHasNotBeenStopped(&_local_state)); \
  CHECK_EQ(expected_state, _local_state); \
} while (0)

Status Tablet::Open(const unordered_set<int64_t>& in_flight_txn_ids,
                    const unordered_set<int64_t>& txn_ids_with_mrs) {
  TRACE_EVENT0("tablet", "Tablet::Open");
  RETURN_IF_STOPPED_OR_CHECK_STATE(kInitialized);

  CHECK(schema()->has_column_ids());

  next_mrs_id_ = metadata_->last_durable_mrs_id() + 1;

  // If we persisted the state of any transaction IDs before shutting down,
  // initialize those that were in-flight here as kOpen. If there were any ops
  // applied that didn't get persisted to the tablet metadata, the bootstrap
  // process will replay those ops.
  for (const auto& txn_id : in_flight_txn_ids) {
    txn_participant_.CreateOpenTransaction(txn_id, log_anchor_registry_.get());
  }

  fs::IOContext io_context({ tablet_id() });
  // open the tablet row-sets
  RowSetVector rowsets_opened;
  rowsets_opened.reserve(metadata_->rowsets().size());
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
    rowsets_opened.emplace_back(std::move(rowset));
  }

  // Update the auto incrementing counter of the tablet from the data directories
  if (schema()->has_auto_incrementing()) {
    Status s = UpdateAutoIncrementingCounter(rowsets_opened);
    if (!s.ok()) {
      LOG_WITH_PREFIX(ERROR) << "Failed to update auto incrementing counter" << s.ToString();
      return s;
    }
  }

  {
    auto new_rowset_tree(make_shared<RowSetTree>());
    RETURN_NOT_OK(new_rowset_tree->Reset(rowsets_opened));

    // Now that the current state is loaded, create the new MemRowSet with the next id.
    shared_ptr<MemRowSet> new_mrs;
    const SchemaPtr schema_ptr = schema();
    RETURN_NOT_OK(MemRowSet::Create(next_mrs_id_++, *schema_ptr,
                                    log_anchor_registry_.get(),
                                    mem_trackers_.tablet_tracker,
                                    &new_mrs));

    // Create MRSs for any in-flight transactions there might be.
    // NOTE: we may also have to create MRSs for committed transactions; that
    // will happen upon bootstrapping.
    std::unordered_map<int64_t, scoped_refptr<TxnRowSets>> uncommitted_rs_by_txn_id;
    const auto txn_meta_by_id = metadata_->GetTxnMetadata();
    for (const auto& txn_id : txn_ids_with_mrs) {
      shared_ptr<MemRowSet> txn_mrs;
      // NOTE: we are able to FindOrDie() on these IDs because
      // 'txn_ids_with_mrs' is a subset of the transaction IDs known by the
      // metadata.
      RETURN_NOT_OK(MemRowSet::Create(0, *schema_ptr, txn_id, FindOrDie(txn_meta_by_id, txn_id),
                                      log_anchor_registry_.get(),
                                      mem_trackers_.tablet_tracker,
                                      &txn_mrs));
      EmplaceOrDie(&uncommitted_rs_by_txn_id, txn_id, new TxnRowSets(std::move(txn_mrs)));
    }
    std::lock_guard<rw_spinlock> lock(component_lock_);
    components_.reset(new TabletComponents(
        std::move(new_mrs), {}, std::move(new_rowset_tree)));
    uncommitted_rowsets_by_txn_id_ = std::move(uncommitted_rs_by_txn_id);
  }

  // Compute the initial average rowset height.
  UpdateAverageRowsetHeight();

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

Status Tablet::UpdateAutoIncrementingCounter(const RowSetVector& rowsets_opened) {
  LOG_TIMING(INFO, "fetching auto increment counter") {
    for (const shared_ptr<RowSet>& rowset: rowsets_opened) {
      RowIteratorOptions opts;
      opts.projection = schema().get();
      opts.include_deleted_rows = false;
      // TODO(achennaka): Materialize only the auto incrementing column for better performance
      unique_ptr<RowwiseIterator> iter;
      RETURN_NOT_OK(rowset->NewRowIterator(opts, &iter));
      RETURN_NOT_OK(iter->Init(nullptr));
      // The default size of 32K should be a good start as it would be increased if needed.
      RowBlockMemory mem;
      RowBlock block(&iter->schema(), 512, &mem);
      while (iter->HasNext()) {
        mem.Reset();
        RETURN_NOT_OK(iter->NextBlock(&block));
        const size_t nrows = block.nrows();
        for (size_t i = 0; i < nrows; ++i) {
          if (!block.selection_vector()->IsRowSelected(i)) {
            continue;
          }
          int64 counter = *reinterpret_cast<const int64 *>(block.row(i).cell_ptr(
              schema()->auto_incrementing_col_idx()));
          if (counter > auto_incrementing_counter_) {
            auto_incrementing_counter_ = counter;
          }
        }
      }
    }
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

  // Close MVCC so Applying ops will not complete and will not be waited on.
  // This prevents further snapshotting of the tablet.
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
  metadata_->SetPreFlushCallback(&DoNothingStatusClosure);
}

Status Tablet::GetMappedReadProjection(const Schema& projection,
                                       Schema *mapped_projection) const {
  const SchemaPtr cur_schema = schema();
  return cur_schema->GetMappedReadProjection(projection, mapped_projection);
}

BloomFilterSizing Tablet::DefaultBloomSizing() {
  return BloomFilterSizing::BySizeAndFPRate(FLAGS_tablet_bloom_block_size,
                                            FLAGS_tablet_bloom_target_fp_rate);
}

void Tablet::SplitKeyRange(const EncodedKey* start_key,
                           const EncodedKey* stop_key,
                           const std::vector<ColumnId>& column_ids,
                           uint64 target_chunk_size,
                           std::vector<KeyRange>* key_range_info) {
  shared_ptr<RowSetTree> rowsets_copy;
  {
    shared_lock<rw_spinlock> l(component_lock_);
    rowsets_copy = components_->rowsets;
  }

  Slice start, stop;
  if (start_key != nullptr) {
    start = start_key->encoded_key();
  }
  if (stop_key != nullptr) {
    stop = stop_key->encoded_key();
  }
  RowSetInfo::SplitKeyRange(*rowsets_copy, start, stop,
                            column_ids, target_chunk_size, key_range_info);
}

Status Tablet::NewRowIterator(const Schema& projection,
                              unique_ptr<RowwiseIterator>* iter) const {
  RowIteratorOptions opts;
  // Yield current rows.
  opts.snap_to_include = MvccSnapshot(mvcc_);
  opts.projection = &projection;
  return NewRowIterator(std::move(opts), iter);
}

Status Tablet::NewOrderedRowIterator(const Schema& projection,
                                     unique_ptr<RowwiseIterator>* iter) const {
  RowIteratorOptions opts;
  // Yield current rows.
  opts.snap_to_include = MvccSnapshot(mvcc_);
  opts.projection = &projection;
  opts.order = ORDERED;
  return NewRowIterator(std::move(opts), iter);
}

Status Tablet::NewRowIterator(RowIteratorOptions opts,
                              unique_ptr<RowwiseIterator>* iter) const {
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
  if (metrics_) {
    metrics_->scans_started->Increment();
  }

  VLOG_WITH_PREFIX(2) << "Created new Iterator for snapshot range: ("
                      << (opts.snap_to_exclude ? opts.snap_to_exclude->ToString() : "-Inf")
                      << ", " << opts.snap_to_include.ToString() << ")";
  iter->reset(new Iterator(this, std::move(opts)));
  return Status::OK();
}

Status Tablet::DecodeWriteOperations(const Schema* client_schema,
                                     WriteOpState* op_state) {
  TRACE_EVENT0("tablet", "Tablet::DecodeWriteOperations");

  DCHECK(op_state->row_ops().empty());

  // Acquire the schema lock in shared mode, so that the schema doesn't change
  // while this op is in-flight.
  op_state->AcquireSchemaLock(&schema_lock_);

  // The Schema needs to be held constant while any ops are between PREPARE and
  // APPLY stages
  TRACE("Decoding operations");
  vector<DecodedRowOperation> ops;

  SchemaPtr schema_ptr = schema();
  // Decode the ops
  RowOperationsPBDecoder dec(&op_state->request()->row_operations(),
                             client_schema,
                             schema_ptr.get(),
                             op_state->arena());
  RETURN_NOT_OK(dec.DecodeOperations<DecoderMode::WRITE_OPS>(&ops, &auto_incrementing_counter_));
  TRACE_COUNTER_INCREMENT("num_ops", ops.size());

  // Important to set the schema before the ops -- we need the
  // schema in order to stringify the ops.
  op_state->set_schema_at_decode_time(schema_ptr);
  op_state->SetRowOps(std::move(ops));

  return Status::OK();
}

Status Tablet::AcquireRowLocks(WriteOpState* op_state) {
  TRACE_EVENT1("tablet", "Tablet::AcquireRowLocks",
               "num_locks", op_state->row_ops().size());
  TRACE("Acquiring locks for $0 operations", op_state->row_ops().size());

  for (RowOp* op : op_state->row_ops()) {
    if (op->has_result()) continue;

    ConstContiguousRow row_key(&key_schema_, op->decoded_op.row_data);
    Arena* arena = op_state->arena();
    op->key_probe = arena->NewObject<RowSetKeyProbe>(row_key, arena);
    if (PREDICT_FALSE(!ValidateOpOrMarkFailed(op))) {
      continue;
    }
    RETURN_NOT_OK(CheckRowInTablet(row_key));
  }

  op_state->AcquireRowLocks(&lock_manager_);

  TRACE("Row locks acquired");
  return Status::OK();
}

Status Tablet::AcquirePartitionLock(WriteOpState* op_state,
                                    LockManager::LockWaitMode wait_mode) {
  return op_state->AcquirePartitionLock(&lock_manager_, wait_mode);
}

Status Tablet::AcquireTxnLock(int64_t txn_id, WriteOpState* op_state) {
  auto txn = txn_participant_.GetTransaction(txn_id);
  if (!txn) {
    // While we might not have an in-flight transaction, we still might have a
    // finished transaction that has metadata.
    if (metadata_->HasTxnMetadata(txn_id)) {
      // TODO(awong): IllegalState or Aborted seem more intuitive, but clients
      // currently retry on both of those errors, assuming the error comes from
      // Raft followers complaining about the lack of leadership.
      return Status::InvalidArgument(Substitute("txn $0 is not open", txn_id));
    }
    return Status::NotFound(Substitute("txn $0 not found on tablet $1", txn_id, tablet_id()));
  }
  return op_state->AcquireTxnLockCheckOpen(std::move(txn));
}

Status Tablet::CheckRowInTablet(const ConstContiguousRow& row) const {
  const auto& ps = metadata_->partition_schema();
  if (PREDICT_FALSE(!ps.PartitionContainsRow(metadata_->partition(), row))) {
    return Status::NotFound(
        Substitute("Row not in tablet partition. Partition: '$0', row: '$1'.",
                   ps.PartitionDebugString(metadata_->partition(), *schema().get()),
                   ps.PartitionKeyDebugString(row)));
  }
  return Status::OK();
}

void Tablet::AssignTimestampAndStartOpForTests(WriteOpState* op_state) {
  CHECK(!op_state->has_timestamp());
  // Don't support COMMIT_WAIT for tests that don't boot a tablet server.
  CHECK_NE(op_state->external_consistency_mode(), COMMIT_WAIT);

  // Make sure timestamp assignment and op start are atomic, for tests.
  //
  // This is to make sure that when test ops advance safe time later, we don't have
  // any op in-flight between getting a timestamp and being started. Otherwise we
  // might run the risk of assigning a timestamp to op1, and have another op
  // get a timestamp/start/advance safe time before op1 starts making op1's timestamp
  // invalid on start.
  {
    std::lock_guard<simple_spinlock> l(test_start_op_lock_);
    op_state->set_timestamp(clock_->Now());
    StartOp(op_state);
  }
}

void Tablet::StartOp(WriteOpState* op_state) {
  DCHECK(op_state->has_timestamp());
  op_state->SetMvccOp(make_unique<ScopedOp>(&mvcc_, op_state->timestamp()));
}

void Tablet::StartOp(ParticipantOpState* op_state) {
  if (op_state->request()->op().type() == tserver::ParticipantOpPB::BEGIN_COMMIT) {
    DCHECK(op_state->has_timestamp());
    op_state->SetMvccOp(make_unique<ScopedOp>(&mvcc_, op_state->timestamp()));
  }
}

bool Tablet::ValidateOpOrMarkFailed(RowOp* op) {
  if (op->valid) return true;
  if (PREDICT_FALSE(op->has_result())) {
    DCHECK(op->result->has_failed_status());
    return false;
  }

  Status s = ValidateOp(*op);
  if (PREDICT_FALSE(!s.ok())) {
    // TODO(todd): add a metric tracking the number of invalid ops.
    op->SetFailed(s);
    return false;
  }
  op->valid = true;
  return true;
}

Status Tablet::ValidateOp(const RowOp& op) {
  switch (op.decoded_op.type) {
    case RowOperationsPB::INSERT:
    case RowOperationsPB::INSERT_IGNORE:
    case RowOperationsPB::UPSERT:
    case RowOperationsPB::UPSERT_IGNORE:
      return ValidateInsertOrUpsertUnlocked(op);

    case RowOperationsPB::UPDATE:
    case RowOperationsPB::UPDATE_IGNORE:
    case RowOperationsPB::DELETE:
    case RowOperationsPB::DELETE_IGNORE:
      return ValidateMutateUnlocked(op);

    default:
      LOG(FATAL) << RowOperationsPB::Type_Name(op.decoded_op.type);
  }
  __builtin_unreachable();
}

Status Tablet::ValidateInsertOrUpsertUnlocked(const RowOp& op) {
  // Check that the encoded key is not longer than the maximum.
  auto enc_key_size = op.key_probe->encoded_key_slice().size();
  if (PREDICT_FALSE(enc_key_size > FLAGS_max_encoded_key_size_bytes)) {
    return Status::InvalidArgument(Substitute(
        "encoded primary key too large ($0 bytes, maximum is $1 bytes)",
        enc_key_size, FLAGS_max_encoded_key_size_bytes));
  }
  return Status::OK();
}

Status Tablet::ValidateMutateUnlocked(const RowOp& op) {
  RowChangeListDecoder rcl_decoder(op.decoded_op.changelist);
  RETURN_NOT_OK(rcl_decoder.Init());
  if (PREDICT_FALSE(rcl_decoder.is_reinsert())) {
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

  DCHECK(rcl_decoder.is_update());
  return Status::OK();
}

Status Tablet::InsertOrUpsertUnlocked(const IOContext* io_context,
                                      WriteOpState *op_state,
                                      RowOp* op,
                                      ProbeStats* stats) {
  DCHECK(op->checked_present);
  DCHECK(op->valid);

  RowOperationsPB_Type op_type = op->decoded_op.type;
  const TabletComponents* comps = DCHECK_NOTNULL(op_state->tablet_components());

  if (op->present_in_rowset) {
    switch (op_type) {
      case RowOperationsPB::UPSERT:
      case RowOperationsPB::UPSERT_IGNORE:
        return ApplyUpsertAsUpdate(io_context, op_state, op, op->present_in_rowset, stats);
      case RowOperationsPB::INSERT_IGNORE:
        op->SetErrorIgnored();
        return Status::OK();
      case RowOperationsPB::INSERT: {
        Status s = Status::AlreadyPresent("key already present");
        if (metrics_) {
          metrics_->insertions_failed_dup_key->Increment();
        }
        op->SetFailed(s);
        return s;
      }
      default:
        LOG(FATAL) << "Unknown operation type: " << op_type;
    }
  }

  DCHECK(!op->present_in_rowset);
  Timestamp ts = op_state->timestamp();
  ConstContiguousRow row(schema().get(), op->decoded_op.row_data);

  // TODO(todd): the Insert() call below will re-encode the key, which is a
  // waste. Should pass through the KeyProbe structure perhaps.

  const auto& txn_id = op_state->txn_id();
  if (txn_id) {
    // Only inserts are supported -- this is guaranteed in the prepare phase.
    DCHECK(op_type == RowOperationsPB::INSERT ||
           op_type == RowOperationsPB::INSERT_IGNORE);
    // The previous presence checks only checked disk rowsets and committed txn
    // memrowsets. Before inserting into this transaction's MRS, ensure the
    // row doesn't already exist in the main MRS.
    bool present_in_main_mrs = false;
    RETURN_NOT_OK(comps->memrowset->CheckRowPresent(*op->key_probe, io_context,
                                                    &present_in_main_mrs, stats));
    if (present_in_main_mrs) {
      if (op_type == RowOperationsPB::INSERT_IGNORE) {
        op->SetErrorIgnored();
        return Status::OK();
      }
      Status s = Status::AlreadyPresent("key already present");
      if (metrics_) {
        metrics_->insertions_failed_dup_key->Increment();
      }
      op->SetFailed(s);
      return s;
    }
    const auto* txn_rowsets = DCHECK_NOTNULL(op_state->txn_rowsets());
    Status s = txn_rowsets->memrowset->Insert(ts, row, op_state->op_id());
    // TODO(awong): once we support transactional updates, update this to check
    // for AlreadyPresent statuses.
    if (PREDICT_TRUE(s.ok())) {
      op->SetInsertSucceeded(*txn_id, txn_rowsets->memrowset->mrs_id());
    } else if (s.IsAlreadyPresent() && op_type == RowOperationsPB::INSERT_IGNORE) {
      op->SetErrorIgnored();
      return Status::OK();
    } else {
      op->SetFailed(s);
    }
    return s;
  }

  // Now try to op into memrowset. The memrowset itself will return
  // AlreadyPresent if it has already been inserted there.
  Status s = comps->memrowset->Insert(ts, row, op_state->op_id());
  if (s.ok()) {
    op->SetInsertSucceeded(comps->memrowset->mrs_id());
  } else {
    if (s.IsAlreadyPresent()) {
      switch (op_type) {
        case RowOperationsPB::UPSERT:
        case RowOperationsPB::UPSERT_IGNORE:
          return ApplyUpsertAsUpdate(io_context, op_state, op, comps->memrowset.get(), stats);
        case RowOperationsPB::INSERT_IGNORE:
          op->SetErrorIgnored();
          return Status::OK();
        case RowOperationsPB::INSERT:
          if (metrics_) {
            metrics_->insertions_failed_dup_key->Increment();
          }
          break;
        default:
          LOG(FATAL) << "Unknown operation type: " << op_type;
      }
    }
    op->SetFailed(s);
  }
  return s;
}

Status Tablet::ApplyUpsertAsUpdate(const IOContext* io_context,
                                   WriteOpState* op_state,
                                   RowOp* upsert,
                                   RowSet* rowset,
                                   ProbeStats* stats) {
  const auto op_type = upsert->decoded_op.type;
  const auto* schema = this->schema().get();
  ConstContiguousRow row(schema, upsert->decoded_op.row_data);
  faststring buf;
  RowChangeListEncoder enc(&buf);
  for (int i = schema->num_key_columns(); i < schema->num_columns(); i++) {
    // If the user didn't explicitly set this column in the UPSERT, then we should
    // not turn it into an UPDATE. This prevents the UPSERT from updating
    // values back to their defaults when unset.
    if (!BitmapTest(upsert->decoded_op.isset_bitmap, i)) continue;
    const auto& c = schema->column(i);
    if (c.is_immutable()) {
      if (op_type == RowOperationsPB::UPSERT) {
        Status s = Status::Immutable("UPDATE not allowed for immutable column", c.ToString());
        upsert->SetFailed(s);
        return s;
      }
      DCHECK_EQ(op_type, RowOperationsPB::UPSERT_IGNORE);
      // Only set upsert->error_ignored flag instead of calling SetErrorIgnored() to avoid setting
      // upsert->result which can be set only once. Then the upsert operation can be continued to
      // mutate the other cells even if the current cell has been skipped, the upsert->result can be
      // set normally in the next steps.
      upsert->error_ignored = true;
      continue;
    }

    const void* val = c.is_nullable() ? row.nullable_cell_ptr(i) : row.cell_ptr(i);
    enc.AddColumnUpdate(c, schema->column_id(i), val);
  }

  // If the UPSERT just included the primary key columns, and the rest
  // were unset (eg because the table only _has_ primary keys, or because
  // the rest are intended to be set to their defaults), we need to
  // avoid doing anything.
  auto* result = google::protobuf::Arena::CreateMessage<OperationResultPB>(
      op_state->pb_arena());
  if (enc.is_empty()) {
    upsert->SetMutateSucceeded(result);
    return Status::OK();
  }

  RowChangeList rcl = enc.as_changelist();

  Status s = rowset->MutateRow(op_state->timestamp(),
                               *upsert->key_probe,
                               rcl,
                               op_state->op_id(),
                               io_context,
                               stats,
                               result);
  CHECK(!s.IsNotFound());
  if (s.ok()) {
    if (metrics_) {
      metrics_->upserts_as_updates->Increment();
    }
    upsert->SetMutateSucceeded(result);
  } else {
    upsert->SetFailed(s);
  }
  return s;
}

vector<RowSet*> Tablet::FindRowSetsToCheck(const RowOp* op,
                                           const TabletComponents* comps) {
  vector<RowSet*> to_check;
  for (const auto& txn_mrs : comps->txn_memrowsets) {
    to_check.emplace_back(txn_mrs.get());
    uint64_t rows;
    txn_mrs->CountLiveRows(&rows);
  }
  if (PREDICT_TRUE(!op->orig_result_from_log)) {
    // TODO(yingchun): could iterate the rowsets in a smart order
    // based on recent statistics - eg if a rowset is getting
    // updated frequently, pick that one first.
    comps->rowsets->FindRowSetsWithKeyInRange(op->key_probe->encoded_key_slice(),
                                              &to_check);
#ifndef NDEBUG
    // The order in which the rowset tree returns its results doesn't have semantic
    // relevance. We've had bugs in the past (eg KUDU-1341) which were obscured by
    // relying on the order of rowsets here. So, in debug builds, we shuffle the
    // order to encourage finding such bugs more easily.
    std::random_device rdev;
    std::mt19937 gen(rdev());
    std::shuffle(to_check.begin(), to_check.end(), gen);
#endif
    return to_check;
  }

  // If we are replaying an operation during bootstrap, then we already have a
  // COMMIT message which tells us specifically which memory store to apply it to.
  for (const auto& store : op->orig_result_from_log->mutated_stores()) {
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
                                 WriteOpState *op_state,
                                 RowOp* op,
                                 ProbeStats* stats) {
  DCHECK(op->checked_present);
  DCHECK(op->valid);

  auto* result = google::protobuf::Arena::CreateMessage<OperationResultPB>(
      op_state->pb_arena());
  const TabletComponents* comps = DCHECK_NOTNULL(op_state->tablet_components());
  Timestamp ts = op_state->timestamp();

  // If we found the row in any existing RowSet, mutate it there. Otherwise
  // attempt to mutate in the MRS.
  RowSet* rs_to_attempt = op->present_in_rowset ?
      op->present_in_rowset : comps->memrowset.get();
  Status s = rs_to_attempt->MutateRow(ts,
                                      *op->key_probe,
                                      op->decoded_op.changelist,
                                      op_state->op_id(),
                                      io_context,
                                      stats,
                                      result);
  if (PREDICT_TRUE(s.ok())) {
    op->SetMutateSucceeded(result);
  } else {
    if (s.IsNotFound()) {
      RowOperationsPB_Type op_type = op->decoded_op.type;
      switch (op_type) {
        case RowOperationsPB::UPDATE_IGNORE:
        case RowOperationsPB::DELETE_IGNORE:
          s = Status::OK();
          op->SetErrorIgnored();
          break;
        case RowOperationsPB::UPDATE:
        case RowOperationsPB::DELETE:
          // Replace internal error messages with one more suitable for users.
          s = Status::NotFound("key not found");
          op->SetFailed(s);
          break;
        default:
          LOG(FATAL) << "Unknown operation type: " << op_type;
      }
    } else {
      op->SetFailed(s);
    }
  }
  return s;
}

void Tablet::StartApplying(WriteOpState* op_state) {
  shared_lock<rw_spinlock> l(component_lock_);

  const auto txn_id = op_state->txn_id();
  if (txn_id) {
    auto txn_rowsets = FindPtrOrNull(uncommitted_rowsets_by_txn_id_, *txn_id);
    // The provisional rowset for this transaction should have been created
    // when applying the BEGIN_TXN op.
    op_state->set_txn_rowsets(txn_rowsets);
  }
  op_state->StartApplying();
  op_state->set_tablet_components(components_);
}

void Tablet::StartApplying(ParticipantOpState* op_state) {
  const auto& op_type = op_state->request()->op().type();
  if (op_type == tserver::ParticipantOpPB::FINALIZE_COMMIT) {
    // NOTE: we may not have an MVCC op if we are bootstrapping and did not
    // replay a BEGIN_COMMIT op.
    if (op_state->txn()->commit_op()) {
      op_state->txn()->commit_op()->StartApplying();
    }
  }
}

void Tablet::CreateTxnRowSets(int64_t txn_id, scoped_refptr<TxnMetadata> txn_meta) {
  shared_ptr<MemRowSet> new_mrs;
  const SchemaPtr schema_ptr = schema();
  CHECK_OK(MemRowSet::Create(0, *schema_ptr, txn_id, std::move(txn_meta),
                             log_anchor_registry_.get(),
                             mem_trackers_.tablet_tracker,
                             &new_mrs));
  scoped_refptr<TxnRowSets> rowsets(new TxnRowSets(std::move(new_mrs)));
  {
    std::lock_guard<rw_spinlock> l(component_lock_);
    // TODO(awong): can we ever get here?
    if (ContainsKey(uncommitted_rowsets_by_txn_id_, txn_id)) {
      return;
    }
    // We are guaranteed to succeed here because this is only ever called by
    // the BEGIN_TXN op, which is only applied once per transaction
    // participant.
    EmplaceOrDie(&uncommitted_rowsets_by_txn_id_, txn_id, std::move(rowsets));
  }
}

Status Tablet::BulkCheckPresence(const IOContext* io_context, WriteOpState* op_state) {
  int num_ops = op_state->row_ops().size();

  // TODO(todd) determine why we sometimes get empty writes!
  if (PREDICT_FALSE(num_ops == 0)) return Status::OK();

  // The compiler seems to be bad at hoisting this load out of the loops,
  // so load it up top.
  RowOp* const * row_ops_base = op_state->row_ops().data();

  // Run all of the ops through the RowSetTree.
  vector<pair<Slice, int>> keys_and_indexes;
  keys_and_indexes.reserve(num_ops);
  for (int i = 0; i < num_ops; i++) {
    RowOp* op = row_ops_base[i];
    // If the op already failed in validation, or if we've got the original result
    // filled in already during replay, then we don't need to consult the RowSetTree.
    if (op->has_result() || op->orig_result_from_log) continue;
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
                     return a.first < b.first;
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

  // Check the presence in the committed transaction memrowsets.
  // NOTE: we don't have to check whether the row is in the main memrowset; if
  // it does exist there, we'll discover that when we try to insert into it.
  const TabletComponents* comps = DCHECK_NOTNULL(op_state->tablet_components());
  for (auto& key_and_index : keys_and_indexes) {
    int idx = key_and_index.second;
    RowOp* op = row_ops_base[idx];
    if (op->present_in_rowset) {
      continue;
    }
    bool present = false;
    for (const auto& mrs : comps->txn_memrowsets) {
      RETURN_NOT_OK_PREPEND(mrs->CheckRowPresent(*op->key_probe, io_context, &present,
                            op_state->mutable_op_stats(idx)),
                            Substitute("Tablet $0 failed to check row presence for op $1",
                                       tablet_id(), op->ToString(key_schema_)));
      if (present) {
        op->present_in_rowset = mrs.get();
        break;
      }
    }
  }

  // Perform the presence checks on the other rowsets. We use the "bulk query"
  // functionality provided by RowSetTree::ForEachRowSetContainingKeys(), which
  // yields results via a callback, with grouping guarantees that callbacks for
  // the same RowSet will be grouped together with increasing query keys.
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
                            return keys[a.second] < keys[b.second];
                          }));
    RowSet* rs = pending_group[0].first;
    for (auto it = pending_group.begin(); it != pending_group.end(); ++it) {
      DCHECK_EQ(it->first, rs) << "All results within a group should be for the same RowSet";
      int op_idx = keys_and_indexes[it->second].second;
      RowOp* op = row_ops_base[op_idx];
      if (op->present_in_rowset) {
        // Already found this op present somewhere.
        continue;
      }

      bool present = false;
      s = rs->CheckRowPresent(*op->key_probe, io_context,
                              &present, op_state->mutable_op_stats(op_idx));
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
  State s = state_.load();
  return s == kStopped || s == kShutdown;
}

Status Tablet::CheckHasNotBeenStopped(State* cur_state) const {
  State s = state_.load();
  if (PREDICT_FALSE(s == kStopped || s == kShutdown)) {
    return Status::IllegalState("Tablet has been stopped");
  }
  if (cur_state) {
    *cur_state = s;
  }
  return Status::OK();
}

void Tablet::BeginTransaction(Txn* txn,
                              const OpId& op_id) {
  unique_ptr<MinLogIndexAnchorer> anchor(new MinLogIndexAnchorer(log_anchor_registry_.get(),
        Substitute("BEGIN_TXN-$0-$1", txn->txn_id(), txn)));
  anchor->AnchorIfMinimum(op_id.index());
  metadata_->AddTxnMetadata(txn->txn_id(), std::move(anchor));
  const auto& txn_id = txn->txn_id();
  CreateTxnRowSets(txn_id, FindOrDie(metadata_->GetTxnMetadata(), txn_id));
  txn->BeginTransaction();
}

void Tablet::BeginCommit(Txn* txn, Timestamp mvcc_op_ts, const OpId& op_id) {
  unique_ptr<MinLogIndexAnchorer> anchor(new MinLogIndexAnchorer(log_anchor_registry_.get(),
        Substitute("BEGIN_COMMIT-$0-$1", txn->txn_id(), txn)));
  anchor->AnchorIfMinimum(op_id.index());
  metadata_->BeginCommitTransaction(txn->txn_id(), mvcc_op_ts, std::move(anchor));
  txn->BeginCommit(op_id);
}

void Tablet::CommitTransaction(Txn* txn, Timestamp commit_ts, const OpId& op_id) {
  unique_ptr<MinLogIndexAnchorer> anchor(new MinLogIndexAnchorer(log_anchor_registry_.get(),
        Substitute("FINALIZE_COMMIT-$0-$1", txn->txn_id(), txn)));
  anchor->AnchorIfMinimum(op_id.index());

  const auto& txn_id = txn->txn_id();
  metadata_->AddCommitTimestamp(txn_id, commit_ts, std::move(anchor));
  CommitTxnRowSets(txn_id);
  txn->FinalizeCommit(commit_ts.value());
}

void Tablet::CommitTxnRowSets(int64_t txn_id) {
  std::lock_guard<rw_spinlock> lock(component_lock_);
  auto txn_rowsets = EraseKeyReturnValuePtr(&uncommitted_rowsets_by_txn_id_, txn_id);
  CHECK(txn_rowsets);
  auto committed_mrss = components_->txn_memrowsets;
  committed_mrss.emplace_back(txn_rowsets->memrowset);
  components_ = new TabletComponents(components_->memrowset,
                                     std::move(committed_mrss),
                                     components_->rowsets);
}

void Tablet::AbortTransaction(Txn* txn,  const OpId& op_id) {
  unique_ptr<MinLogIndexAnchorer> anchor(new MinLogIndexAnchorer(log_anchor_registry_.get(),
        Substitute("ABORT_TXN-$0-$1", txn->txn_id(), txn)));
  anchor->AnchorIfMinimum(op_id.index());
  const auto& txn_id = txn->txn_id();
  metadata_->AbortTransaction(txn_id, std::move(anchor));
  {
    std::lock_guard<rw_spinlock> lock(component_lock_);
    uncommitted_rowsets_by_txn_id_.erase(txn_id);
  }
  txn->AbortTransaction();
}

Status Tablet::ApplyRowOperations(WriteOpState* op_state) {
  const size_t num_ops = op_state->row_ops().size();
  StartApplying(op_state);

  TRACE("starting BulkCheckPresence");
  IOContext io_context({ tablet_id() });
  RETURN_NOT_OK(BulkCheckPresence(&io_context, op_state));
  TRACE("finished BulkCheckPresence");

  TRACE("starting ApplyRowOperation cycle");
  // Actually apply the ops.
  for (size_t op_idx = 0; op_idx < num_ops; ++op_idx) {
    RowOp* row_op = op_state->row_ops()[op_idx];
    if (row_op->has_result()) {
      continue;
    }
    RETURN_NOT_OK(ApplyRowOperation(&io_context, op_state, row_op,
                                    op_state->mutable_op_stats(op_idx)));
    DCHECK(row_op->has_result());
  }
  TRACE("finished ApplyRowOperation cycle");

  {
    std::lock_guard<rw_spinlock> l(last_rw_time_lock_);
    last_write_time_ = MonoTime::Now();
  }

  if (metrics_ && PREDICT_TRUE(num_ops > 0)) {
    metrics_->AddProbeStats(op_state->mutable_op_stats(0), num_ops, op_state->arena());
  }
  return Status::OK();
}

Status Tablet::ApplyRowOperation(const IOContext* io_context,
                                 WriteOpState* op_state,
                                 RowOp* row_op,
                                 ProbeStats* stats) {
  if (!ValidateOpOrMarkFailed(row_op)) {
    return Status::OK();
  }

  {
    State s;
    RETURN_NOT_OK_PREPEND(CheckHasNotBeenStopped(&s),
        Substitute("Apply of $0 exited early", op_state->ToString()));
    CHECK(s == kOpen || s == kBootstrapping);
  }
  DCHECK(op_state != nullptr) << "must have a WriteOpState";
  DCHECK(op_state->op_id().IsInitialized()) << "OpState OpId needed for anchoring";
  DCHECK_EQ(op_state->schema_at_decode_time(), schema().get());

  // If we were unable to check rowset presence in batch (e.g. because we are processing
  // a batch which contains some duplicate keys) we need to do so now.
  if (PREDICT_FALSE(!row_op->checked_present)) {
    vector<RowSet *> to_check = FindRowSetsToCheck(row_op,
                                                   op_state->tablet_components());
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
    case RowOperationsPB::INSERT_IGNORE:
    case RowOperationsPB::UPSERT:
    case RowOperationsPB::UPSERT_IGNORE:
      s = InsertOrUpsertUnlocked(io_context, op_state, row_op, stats);
      if (s.IsAlreadyPresent() || s.IsImmutable()) {
        return Status::OK();
      }
      return s;

    case RowOperationsPB::UPDATE:
    case RowOperationsPB::UPDATE_IGNORE:
    case RowOperationsPB::DELETE:
    case RowOperationsPB::DELETE_IGNORE:
      s = MutateRowUnlocked(io_context, op_state, row_op, stats);
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

void Tablet::AtomicSwapRowSets(const RowSetVector &to_remove,
                               const RowSetVector &to_add) {
  std::lock_guard<rw_spinlock> lock(component_lock_);
  AtomicSwapRowSetsUnlocked(to_remove, to_add);
}

void Tablet::AtomicSwapRowSetsUnlocked(const RowSetVector &to_remove,
                                       const RowSetVector &to_add) {
  DCHECK(component_lock_.is_locked());

  auto new_tree(make_shared<RowSetTree>());
  ModifyRowSetTree(*components_->rowsets, to_remove, to_add, new_tree.get());

  components_ = new TabletComponents(components_->memrowset,
                                     components_->txn_memrowsets,
                                     std::move(new_tree));
}

Status Tablet::DoMajorDeltaCompaction(const vector<ColumnId>& col_ids,
                                      const shared_ptr<RowSet>& input_rs,
                                      const IOContext* io_context) {
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
  Status s = down_cast<DiskRowSet*>(input_rs.get())
      ->MajorCompactDeltaStoresWithColumnIds(col_ids, io_context, GetHistoryGcOpts());
  return s;
}

bool Tablet::GetTabletAncientHistoryMark(Timestamp* ancient_history_mark) const {
  int32_t tablet_history_max_age_sec = FLAGS_tablet_history_max_age_sec;
  const auto& extra_config = metadata_->extra_config();
  if (extra_config && extra_config->has_history_max_age_sec()) {
    // Override the global configuration with the configuration of the table
    tablet_history_max_age_sec = extra_config->history_max_age_sec();
  }
  // We currently only support history GC through a fully-instantiated tablet
  // when using the HybridClock, since we can calculate the age of a mutation.
  if (!clock_->HasPhysicalComponent() || tablet_history_max_age_sec < 0) {
    return false;
  }
  Timestamp now = clock_->Now();
  uint64_t now_micros = HybridClock::GetPhysicalValueMicros(now);
  uint64_t max_age_micros = tablet_history_max_age_sec * 1000000ULL;
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
  RowSetsInCompactionOrFlush input;
  vector<shared_ptr<MemRowSet>> old_mrss;
  {
    // Create a new MRS with the latest schema.
    std::lock_guard<rw_spinlock> lock(component_lock_);
    RETURN_NOT_OK(ReplaceMemRowSetsUnlocked(&input, &old_mrss));
    DCHECK_GE(old_mrss.size(), 1);
  }

  // Wait for any in-flight ops to finish against the old MRS before we flush
  // it.
  //
  // This may fail if the tablet has been stopped.
  RETURN_NOT_OK(mvcc_.WaitForApplyingOpsToApply());

  {
    State s;
    RETURN_NOT_OK(CheckHasNotBeenStopped(&s));
    CHECK(s == kOpen || s == kBootstrapping);
  }

  // Freeze the old memrowset by blocking readers and swapping it in as a new
  // rowset, replacing it with an empty one.
  //
  // At this point, we have already swapped in a new empty rowset, and any new
  // inserts are going into that one. 'old_ms' is effectively frozen -- no new
  // inserts should arrive after this point.
  //
  // NOTE: updates and deletes may still arrive into 'old_ms' at this point.
  //
  // TODO(perf): there's a memrowset.Freeze() call which we might be able to
  // use to improve iteration performance during the flush. The old design
  // used this, but not certain whether it's still doable with the new design.

  uint64_t start_insert_count = 0;
  // Keep track of the main MRS.
  int64_t main_mrs_id = -1;
  vector<TxnInfoBeingFlushed> txns_being_flushed;
  for (const auto& old_mrs : old_mrss) {
    start_insert_count += old_mrs->debug_insert_count();
    if (old_mrs->txn_id()) {
      txns_being_flushed.emplace_back(*old_mrs->txn_id());
    } else {
      DCHECK_EQ(-1, main_mrs_id);
      main_mrs_id = old_mrs->mrs_id();
    }
  }
  DCHECK_NE(-1, main_mrs_id);

  if (old_mrss.size() == 1 && old_mrss[0]->empty()) {
    // If we're flushing an empty RowSet, we can short circuit here rather than
    // waiting until the check at the end of DoCompactionAndFlush(). This avoids
    // the need to create cfiles and write their headers only to later delete
    // them.
    LOG_WITH_PREFIX(INFO) << "MemRowSet was empty: no flush needed.";
    return HandleEmptyCompactionOrFlush(input.rowsets(), main_mrs_id, txns_being_flushed);
  }

  if (flush_hooks_) {
    RETURN_NOT_OK_PREPEND(flush_hooks_->PostSwapNewMemRowSet(),
                          "PostSwapNewMemRowSet hook failed");
  }

  if (VLOG_IS_ON(1)) {
    size_t memory_footprint = 0;
    for (const auto& old_mrs : old_mrss) {
      memory_footprint += old_mrs->memory_footprint();
    }
    VLOG_WITH_PREFIX(1) << Substitute("Flush: entering stage 1 (old memrowset "
                                      "already frozen for inserts). Memstore "
                                      "in-memory size: $0 bytes",
                                      memory_footprint);
  }

  RETURN_NOT_OK(DoMergeCompactionOrFlush(input, main_mrs_id, txns_being_flushed));

  uint64_t end_insert_count = 0;
  for (const auto& old_mrs : old_mrss) {
    end_insert_count += old_mrs->debug_insert_count();
  }
  // Sanity check that no insertions happened during our flush.
  CHECK_EQ(start_insert_count, end_insert_count)
      << "Sanity check failed: insertions continued in memrowset "
      << "after flush was triggered! Aborting to prevent data loss.";

  return Status::OK();
}

Status Tablet::ReplaceMemRowSetsUnlocked(RowSetsInCompactionOrFlush* new_mrss,
                                         vector<shared_ptr<MemRowSet>>* old_mrss) {
  DCHECK(old_mrss->empty());
  old_mrss->emplace_back(components_->memrowset);
  for (const auto& committed_mrs : components_->txn_memrowsets) {
    old_mrss->emplace_back(committed_mrs);
  }
  // Mark the memrowsets as locked, so compactions won't consider it
  // for inclusion in any concurrent compactions.
  for (auto& mrs : *old_mrss) {
    std::unique_lock<std::mutex> ms_lock(*mrs->compact_flush_lock(), std::try_to_lock);
    CHECK(ms_lock.owns_lock());
    new_mrss->AddRowSet(mrs, std::move(ms_lock));
  }

  shared_ptr<MemRowSet> new_mrs;
  const SchemaPtr schema_ptr = schema();
  RETURN_NOT_OK(MemRowSet::Create(next_mrs_id_++, *schema_ptr,
                                  log_anchor_registry_.get(),
                                  mem_trackers_.tablet_tracker,
                                  &new_mrs));
  auto new_rst(make_shared<RowSetTree>());
  ModifyRowSetTree(*components_->rowsets,
                   RowSetVector(), // remove nothing
                   RowSetVector(old_mrss->begin(), old_mrss->end()), // add the old MRSs
                   new_rst.get());
  // Swap it in
  components_.reset(new TabletComponents(std::move(new_mrs), {}, std::move(new_rst)));
  return Status::OK();
}

Status Tablet::CreatePreparedAlterSchema(AlterSchemaOpState* op_state,
                                         const SchemaPtr& schema) {

  if (!schema->has_column_ids()) {
    // this probably means that the request is not from the Master
    return Status::InvalidArgument("Missing Column IDs");
  }

  // Alter schema must run when no reads/writes are in progress.
  // However, compactions and flushes can continue to run in parallel
  // with the schema change,
  op_state->AcquireSchemaLock(&schema_lock_);

  op_state->set_schema(schema);
  return Status::OK();
}

Status Tablet::AlterSchema(AlterSchemaOpState* op_state) {
  DCHECK(key_schema_.KeyTypeEquals(*DCHECK_NOTNULL(op_state->schema().get())))
      << "Schema keys cannot be altered(except name)";

  // Prevent any concurrent flushes. Otherwise, we run into issues where
  // we have an MRS in the rowset tree, and we can't alter its schema
  // in-place.
  std::lock_guard<Semaphore> lock(rowsets_flush_sem_);

  // If the current version >= new version, there is nothing to do.
  const bool same_schema = (*schema() == *op_state->schema());
  if (metadata_->schema_version() >= op_state->schema_version()) {
    if (!op_state->force()) {
      const string msg =
          Substitute("Skipping requested alter to schema version $0, tablet already "
                     "version $1", op_state->schema_version(), metadata_->schema_version());
      LOG_WITH_PREFIX(INFO) << msg;
      op_state->SetError(Status::InvalidArgument(msg));
      return Status::OK();
    }

    DCHECK(op_state->force());
    if (!same_schema) {
      const string msg =
          Substitute("Skipping requested alter to schema version $0, the same schema is needed, "
                     "but local $1 vs requested $2",
                     op_state->schema_version(),
                     schema()->ToString(),
                     op_state->schema()->ToString());
      LOG_WITH_PREFIX(INFO) << msg;
      op_state->SetError(Status::InvalidArgument(msg));
      return Status::OK();
    }
  }

  LOG_WITH_PREFIX(INFO) << "Alter schema from " << schema()->ToString()
                        << " version " << metadata_->schema_version()
                        << " to " << op_state->schema()->ToString()
                        << " version " << op_state->schema_version();
  DCHECK(schema_lock_.is_locked());
  metadata_->SetSchema(op_state->schema(), op_state->schema_version());
  if (op_state->has_new_table_name()) {
    metadata_->SetTableName(op_state->new_table_name());
    if (metric_entity_) {
      metric_entity_->SetAttribute("table_name", op_state->new_table_name());
    }
  }
  if (op_state->has_new_extra_config()) {
    metadata_->SetExtraConfig(op_state->new_extra_config());
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

  SchemaPtr schema = std::make_shared<Schema>(new_schema);
  metadata_->SetSchema(schema, schema_version);
  {
    std::lock_guard<rw_spinlock> lock(component_lock_);

    shared_ptr<MemRowSet> old_mrs = components_->memrowset;
    shared_ptr<RowSetTree> old_rowsets = components_->rowsets;
    CHECK(old_mrs->empty());
    shared_ptr<MemRowSet> new_mrs;
    RETURN_NOT_OK(MemRowSet::Create(old_mrs->mrs_id(), *schema,
                                    log_anchor_registry_.get(),
                                    mem_trackers_.tablet_tracker,
                                    &new_mrs));
    components_ = new TabletComponents(new_mrs, components_->txn_memrowsets, old_rowsets);
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
  return throttler_->Take(1, bytes);
}

Status Tablet::PickRowSetsToCompact(RowSetsInCompactionOrFlush *picked,
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

  unordered_set<const RowSet*> picked_set;

  if (flags & FORCE_COMPACT_ALL) {
    // Compact all rowsets, regardless of policy.
    for (const shared_ptr<RowSet>& rs : rowsets_copy->all_rowsets()) {
      if (rs->IsAvailableForCompaction()) {
        picked_set.insert(rs.get());
      }
    }
  } else {
    // Let the policy decide which rowsets to compact.
    double quality = 0.0;
    RETURN_NOT_OK(compaction_policy_->PickRowSets(*rowsets_copy,
                                                  &picked_set,
                                                  &quality,
                                                  /*log=*/nullptr));
    VLOG_WITH_PREFIX(2) << "Compaction quality: " << quality;
  }

  shared_lock<rw_spinlock> l(component_lock_);
  for (const shared_ptr<RowSet>& rs : components_->rowsets->all_rowsets()) {
    if (picked_set.erase(rs.get()) == 0) {
      // Not picked.
      continue;
    }

    // For every rowset we pick, we have to take its compact_flush_lock. TSAN
    // disallows taking more than 64 locks in a single thread[1], so for large
    // compactions this can cause TSAN CHECK failures. To work around, limit the
    // number of rowsets picked in TSAN to 32.
    // [1]: https://github.com/google/sanitizers/issues/950
    // TODO(wdberkeley): Experiment with a compact_flush lock table instead of
    // a per-rowset compact_flush lock.
    #if defined(THREAD_SANITIZER)
      constexpr auto kMaxPickedUnderTsan = 32;
      if (picked->num_rowsets() > kMaxPickedUnderTsan) {
        LOG(WARNING) << Substitute("Limiting compaction to $0 rowsets under TSAN",
                                   kMaxPickedUnderTsan);
        // Clear 'picked_set' to indicate there's no more rowsets we expect
        // to lock.
        picked_set.clear();
        break;
      }
    #endif

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

bool Tablet::compaction_enabled() const {
  const auto& extra_config = metadata_->extra_config();
  if (extra_config && extra_config->has_disable_compaction()) {
    return !extra_config->disable_compaction();
  }
  return true;
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

  vector<unique_ptr<MaintenanceOp>> maintenance_ops;
  maintenance_ops.emplace_back(new CompactRowSetsOp(this));
  maint_mgr->RegisterOp(maintenance_ops.back().get());

  maintenance_ops.emplace_back(new MinorDeltaCompactionOp(this));
  maint_mgr->RegisterOp(maintenance_ops.back().get());

  maintenance_ops.emplace_back(new MajorDeltaCompactionOp(this));
  maint_mgr->RegisterOp(maintenance_ops.back().get());

  maintenance_ops.emplace_back(new UndoDeltaBlockGCOp(this));
  maint_mgr->RegisterOp(maintenance_ops.back().get());

  // The deleted rowset GC operation relies on live rowset counting. If this
  // tablet doesn't support such counting, do not register the op.
  if (metadata_->supports_live_row_count()
      || FLAGS_enable_gc_deleted_rowsets_without_live_row_count) {
    maintenance_ops.emplace_back(new DeletedRowsetGCOp(this));
    maint_mgr->RegisterOp(maintenance_ops.back().get());
  }

  std::lock_guard<simple_spinlock> l(state_lock_);
  maintenance_ops_ = std::move(maintenance_ops);
}

void Tablet::UnregisterMaintenanceOps() {
  // This method must be externally synchronized to not coincide with other
  // calls to it or to RegisterMaintenanceOps.
  DFAKE_SCOPED_LOCK(maintenance_registration_fake_lock_);

  // First cancel all of the operations, so that while we're waiting for one
  // operation to finish in Unregister(), a different one can't get re-scheduled.
  CancelMaintenanceOps();

  decltype(maintenance_ops_) ops;
  {
    std::lock_guard<simple_spinlock> l(state_lock_);
    ops = std::move(maintenance_ops_);
    maintenance_ops_.clear();
  }

  // We don't lock here because unregistering ops may take a long time.
  // 'maintenance_registration_fake_lock_' is sufficient to ensure nothing else
  // is updating 'maintenance_ops_'.
  for (auto& op : ops) {
    op->Unregister();
  }
}

void Tablet::CancelMaintenanceOps() {
  std::lock_guard<simple_spinlock> l(state_lock_);
  for (auto& op : maintenance_ops_) {
    op->CancelAndDisable();
  }
}

Status Tablet::FlushMetadata(const RowSetVector& to_remove,
                             const RowSetMetadataVector& to_add,
                             int64_t mrs_being_flushed,
                             const vector<TxnInfoBeingFlushed>& txns_being_flushed) {
  RowSetMetadataIds to_remove_meta;
  for (const shared_ptr<RowSet>& rowset : to_remove) {
    // Skip MemRowSet & DuplicatingRowSets which don't have metadata.
    if (rowset->metadata().get() == nullptr) {
      continue;
    }
    to_remove_meta.insert(rowset->metadata()->id());
  }

  return metadata_->UpdateAndFlush(to_remove_meta, to_add, mrs_being_flushed,
                                   txns_being_flushed);
}

// Computes on-disk size of all the deltas in provided rowsets.
size_t Tablet::GetAllDeltasSizeOnDisk(const RowSetsInCompactionOrFlush &input) {
  size_t deltas_on_disk_size = 0;
  for (const auto& rs : input.rowsets()) {
    DiskRowSetSpace drss;
    DiskRowSet* drs = down_cast<DiskRowSet*>(rs.get());
    drs->GetDiskRowSetSpaceUsage(&drss);
    deltas_on_disk_size += drss.redo_deltas_size + drss.undo_deltas_size;
  }

  return deltas_on_disk_size;
}

Status Tablet::DoMergeCompactionOrFlush(const RowSetsInCompactionOrFlush &input,
                                        int64_t mrs_being_flushed,
                                        const vector<TxnInfoBeingFlushed>& txns_being_flushed) {
  const char *op_name =
        (mrs_being_flushed == TabletMetadata::kNoMrsFlushed) ? "Compaction" : "Flush";
  TRACE_EVENT2("tablet", "Tablet::DoMergeCompactionOrFlush",
               "tablet_id", tablet_id(),
               "op", op_name);

  // Save the stats on the total on-disk size of all deltas in selected rowsets.
  size_t deltas_on_disk_size = 0;
  if (mrs_being_flushed == TabletMetadata::kNoMrsFlushed) {
    deltas_on_disk_size = GetAllDeltasSizeOnDisk(input);
  }

  const auto& tid = tablet_id();
  const IOContext io_context({ tid });

  shared_ptr<CompactionOrFlushInput> merge;
  const SchemaPtr schema_ptr = schema();
  MvccSnapshot flush_snap(mvcc_);
  VLOG_WITH_PREFIX(1) << Substitute("$0: entering phase 1 (flushing snapshot). "
                                    "Phase 1 snapshot: $1",
                                    op_name, flush_snap.ToString());

  // Fault injection hook for testing and debugging purpose only.
  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostTakeMvccSnapshot(),
                          "PostTakeMvccSnapshot hook failed");
  }

  // Create input of rowsets by iterating through all rowsets and for each rowset:
  //   - For compaction ops, create input that contains initialized base,
  //     relevant REDO and UNDO delta iterators to be used read from persistent storage.
  //   - For Flush ops, create iterator for in-memory tree holding data updates.
  RETURN_NOT_OK(input.CreateCompactionOrFlushInput(flush_snap,
                                                   schema_ptr.get(),
                                                   &io_context,
                                                   &merge));

  // Initializing a DRS writer, to be used later for writing REDO, UNDO deltas, delta stats, etc.
  RollingDiskRowSetWriter drsw(metadata_.get(), merge->schema(), DefaultBloomSizing(),
                               compaction_policy_->target_rowset_size());
  RETURN_NOT_OK_PREPEND(drsw.Open(), "Failed to open DiskRowSet for flush");

  // Get tablet history, to be used later for AHM validation checks.
  HistoryGcOpts history_gc_opts = GetHistoryGcOpts();

  // Apply REDO and UNDO deltas to the rows, merge histories of rows with 'ghost' entries.
  RETURN_NOT_OK_PREPEND(
      FlushCompactionInput(
          tid, metadata_->fs_manager()->block_manager()->error_manager(),
          merge.get(), flush_snap, history_gc_opts, &drsw),
      "Flush to disk failed");
  RETURN_NOT_OK_PREPEND(drsw.Finish(), "Failed to finish DRS writer");

  // Fault injection hook for testing and debugging purpose only.
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
    return HandleEmptyCompactionOrFlush(input.rowsets(), mrs_being_flushed,
                                        txns_being_flushed);
  }

  // The RollingDiskRowSet writer wrote out one or more RowSets as the
  // output. Open these into 'new_rowsets'.
  RowSetMetadataVector new_drs_metas;
  drsw.GetWrittenRowSetMetadata(&new_drs_metas);
  CHECK(!new_drs_metas.empty());

  if (metrics_) {
    metrics_->bytes_flushed->IncrementBy(drsw.written_size());
  }

  // Open all the rowsets (that were processed in this stage) from disk and
  // store the pointers to each of rowset inside new_disk_rowsets.
  vector<shared_ptr<RowSet>> new_disk_rowsets;
  new_disk_rowsets.reserve(new_drs_metas.size());
  {
    TRACE_EVENT0("tablet", "Opening compaction results");
    for (const shared_ptr<RowSetMetadata>& meta : new_drs_metas) {
      // TODO(awong): it'd be nice to plumb delta stats from the rowset writer
      // into the new deltafile readers opened here.
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
      new_disk_rowsets.emplace_back(std::move(new_rowset));
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
  // - Now we run the "ReupdateMissedDeltas", and copy over the first op to the
  //   output DMS, which later flushes.
  // The end result would be that redos[0] has timestamp 2, and redos[1] has timestamp 1.
  // This breaks an invariant that the redo files are time-ordered, and we would probably
  // reapply the deltas in the wrong order on the read path.
  //
  // The way that we avoid this case is that DuplicatingRowSet's FlushDeltas method is a
  // no-op.
  VLOG_WITH_PREFIX(1) << Substitute("$0: entering phase 2 (starting to "
                                    "duplicate updates in new rowsets)",
                                    op_name);
  shared_ptr<DuplicatingRowSet> inprogress_rowset(
      make_shared<DuplicatingRowSet>(input.rowsets(), new_disk_rowsets));

  // The next step is to swap in the DuplicatingRowSet, and at the same time,
  // determine an MVCC snapshot which includes all of the ops that saw a
  // pre-DuplicatingRowSet version of components_.
  MvccSnapshot non_duplicated_ops_snap;
  vector<Timestamp> applying_during_swap;
  {
    TRACE_EVENT0("tablet", "Swapping DuplicatingRowSet");
    // Taking component_lock_ in write mode ensures that no new ops can
    // StartApplying() (or snapshot components_) during this block.
    std::lock_guard<rw_spinlock> lock(component_lock_);
    AtomicSwapRowSetsUnlocked(input.rowsets(), { inprogress_rowset });

    // NOTE: ops may *commit* in between these two lines.
    // We need to make sure all such ops end up in the 'applying_during_swap'
    // list, the 'non_duplicated_ops_snap' snapshot, or both. Thus it's crucial
    // that these next two lines are in this order!
    mvcc_.GetApplyingOpsTimestamps(&applying_during_swap);
    non_duplicated_ops_snap = MvccSnapshot(mvcc_);
  }

  // All ops committed in 'non_duplicated_ops_snap' saw the pre-swap
  // components_. Additionally, any ops that were APPLYING during the above
  // block by definition _started_ doing so before the swap. Hence those ops
  // also need to get included in non_duplicated_ops_snap. To do so, we wait
  // for them to commit, and then manually include them into our snapshot.
  if (VLOG_IS_ON(1) && !applying_during_swap.empty()) {
    VLOG_WITH_PREFIX(1) << "Waiting for " << applying_during_swap.size()
                        << " mid-APPLY ops to commit before finishing compaction...";
    for (const Timestamp& ts : applying_during_swap) {
      VLOG_WITH_PREFIX(1) << "  " << ts.value();
    }
  }

  // This wait is a little bit conservative - technically we only need to wait
  // for those ops in 'applying_during_swap', but MVCC doesn't implement the
  // ability to wait for a specific set. So instead we wait for all currently
  // applying -- a bit more than we need, but still correct.
  RETURN_NOT_OK(mvcc_.WaitForApplyingOpsToApply());

  // Then we want to consider all those ops that were in-flight when we did the
  // swap as committed in 'non_duplicated_ops_snap'.
  non_duplicated_ops_snap.AddAppliedTimestamps(applying_during_swap);

  // Fault injection hook for testing and debugging purpose only.
  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostSwapInDuplicatingRowSet(),
                          "PostSwapInDuplicatingRowSet hook failed");
  }

  // Store the stats on the max memory used for compaction phase 1.
  const size_t peak_mem_usage_ph1 = merge->memory_footprint();

  // Phase 2. Here we re-scan the compaction input, copying those missed updates into the
  // new rowset's DeltaTracker.
  VLOG_WITH_PREFIX(1) << Substitute("$0: Phase 2: carrying over any updates "
                                    "which arrived during Phase 1. Snapshot: $1",
                                    op_name, non_duplicated_ops_snap.ToString());
  const SchemaPtr schema_ptr2 = schema();
  RETURN_NOT_OK_PREPEND(input.CreateCompactionOrFlushInput(non_duplicated_ops_snap,
                                                           schema_ptr2.get(),
                                                           &io_context,
                                                           &merge),
                        Substitute("Failed to create $0 inputs", op_name).c_str());

  // Update the output rowsets with the deltas that came in in phase 1, before we swapped
  // in the DuplicatingRowSets. This will perform a flush of the updated DeltaTrackers
  // in the end so that the data that is reported in the log as belonging to the input
  // rowsets is flushed.
  RETURN_NOT_OK_PREPEND(ReupdateMissedDeltas(&io_context,
                                             merge.get(),
                                             history_gc_opts,
                                             flush_snap,
                                             non_duplicated_ops_snap,
                                             new_disk_rowsets),
        Substitute("Failed to re-update deltas missed during $0 phase 1",
                     op_name).c_str());

  // Fault injection hook for testing and debugging purpose only.
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
  RETURN_NOT_OK_PREPEND(FlushMetadata(input.rowsets(), new_drs_metas, mrs_being_flushed,
                                      txns_being_flushed),
                        "Failed to flush new tablet metadata");

  // Now that we've completed the operation, mark any rowsets that have been
  // compacted, preventing them from being considered for future compactions.
  for (const auto& rs : input.rowsets()) {
    rs->set_has_been_compacted();
  }

  // Replace the compacted rowsets with the new on-disk rowsets, making them visible now that
  // their metadata was written to disk.
  AtomicSwapRowSets({ inprogress_rowset }, new_disk_rowsets);
  UpdateAverageRowsetHeight();

  const size_t peak_mem_usage = std::max(peak_mem_usage_ph1,
                                         merge->memory_footprint());
  // For rowset merge compactions, update the stats on the max peak memory used
  // and ratio of the amount of memory used to the size of all deltas on disk.
  if (deltas_on_disk_size > 0) {
    // Update the peak memory usage metric.
    metrics_->compact_rs_mem_usage->Increment(peak_mem_usage);

    // Update the ratio of the peak memory usage to the size of deltas on disk.
    // To keep the stats relevant for larger rowsets, filter out rowsets with
    // relatively small amount of data in deltas. Update the memory-to-disk size
    // ratio metric only when the on-disk size of deltas crosses the configured
    // threshold.
    const int64_t min_deltas_size_bytes =
        FLAGS_rowset_compaction_estimate_min_deltas_size_mb * 1024 * 1024;
    if (deltas_on_disk_size > min_deltas_size_bytes) {
      // Round up the ratio. Since the ratio is used to estimate the amount of
      // memory needed to perform merge rowset compaction based on the amount of
      // data stored in rowsets' deltas, it's safer to provide an upper rather
      // than a lower bound estimate.
      metrics_->compact_rs_mem_usage_to_deltas_size_ratio->Increment(
          (peak_mem_usage + deltas_on_disk_size - 1) / deltas_on_disk_size);
    }
  }

  const auto rows_written = drsw.rows_written_count();
  const auto drs_written = drsw.drs_written_count();
  const auto bytes_written = drsw.written_size();
  TRACE_COUNTER_INCREMENT("rows_written", rows_written);
  TRACE_COUNTER_INCREMENT("drs_written", drs_written);
  TRACE_COUNTER_INCREMENT("bytes_written", bytes_written);
  TRACE_COUNTER_INCREMENT("peak_mem_usage", peak_mem_usage);
  VLOG_WITH_PREFIX(1) << Substitute("$0 successful on $1 rows ($2 rowsets, $3 bytes)",
                                    op_name,
                                    rows_written,
                                    drs_written,
                                    bytes_written);

  // Fault injection hook for testing and debugging purpose only.
  if (common_hooks_) {
    RETURN_NOT_OK_PREPEND(common_hooks_->PostSwapNewRowSet(),
                          "PostSwapNewRowSet hook failed");
  }

  return Status::OK();
}

Status Tablet::HandleEmptyCompactionOrFlush(const RowSetVector& rowsets,
                                            int mrs_being_flushed,
                                            const vector<TxnInfoBeingFlushed>& txns_being_flushed) {
  // Write out the new Tablet Metadata and remove old rowsets.
  RETURN_NOT_OK_PREPEND(FlushMetadata(rowsets,
                                      RowSetMetadataVector(),
                                      mrs_being_flushed,
                                      txns_being_flushed),
                        "Failed to flush new tablet metadata");

  AtomicSwapRowSets(rowsets, RowSetVector());
  UpdateAverageRowsetHeight();
  return Status::OK();
}

void Tablet::UpdateAverageRowsetHeight() {
  if (!metrics_) {
    return;
  }
  // TODO(wdberkeley): We should be able to cache the computation of the CDF
  // and average height and efficiently recompute it instead of doing it from
  // scratch.
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);
  std::lock_guard<std::mutex> l(compact_select_lock_);
  double rowset_total_height;
  double rowset_total_width;
  RowSetInfo::ComputeCdfAndCollectOrdered(*comps->rowsets,
                                          /*is_on_memory_budget=*/nullopt,
                                          &rowset_total_height,
                                          &rowset_total_width,
                                          /*info_by_min_key=*/nullptr,
                                          /*info_by_max_key=*/nullptr);
  metrics_->average_diskrowset_height->set_value(rowset_total_height, rowset_total_width);
}

Status Tablet::Compact(CompactFlags flags) {
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);

  RowSetsInCompactionOrFlush input;
  // Step 1. Capture the rowsets to be merged
  RETURN_NOT_OK_PREPEND(PickRowSetsToCompact(&input, flags),
                        "Failed to pick rowsets to compact");
  const auto num_input_rowsets = input.num_rowsets();
  TRACE_COUNTER_INCREMENT("num_input_rowsets", num_input_rowsets);
  VLOG_WITH_PREFIX(1) << Substitute("Compaction: stage 1 complete, picked $0 "
                                    "rowsets to compact or flush",
                                    num_input_rowsets);
  if (compaction_hooks_) {
    RETURN_NOT_OK_PREPEND(compaction_hooks_->PostSelectIterators(),
                          "PostSelectIterators hook failed");
  }

  if (VLOG_IS_ON(1)) {
    input.DumpToLog();
  }

  return DoMergeCompactionOrFlush(input, TabletMetadata::kNoMrsFlushed, {});
}

void Tablet::UpdateCompactionStats(MaintenanceOpStats* stats) {

  if (mvcc_.GetCleanTimestamp() == Timestamp::kInitialTimestamp &&
      PREDICT_TRUE(FLAGS_prevent_kudu_2233_corruption)) {
    KLOG_EVERY_N_SECS(WARNING, 30) << LogPrefix() <<  "Can't schedule compaction. Clean time has "
                                   << "not been advanced past its initial value.";
    stats->set_runnable(false);
    return;
  }

  double quality = 0;
  unordered_set<const RowSet*> picked;

  shared_ptr<RowSetTree> rowsets_copy;
  {
    shared_lock<rw_spinlock> l(component_lock_);
    rowsets_copy = components_->rowsets;
  }

  {
    std::lock_guard<std::mutex> compact_lock(compact_select_lock_);
    WARN_NOT_OK(compaction_policy_->PickRowSets(*rowsets_copy, &picked, &quality, nullptr),
                Substitute("Couldn't determine compaction quality for $0", tablet_id()));
  }

  // An estimate for the total amount of data stored in the UNDO deltas across
  // all the rowsets selected for this rowset compaction.
  uint64_t undos_total_size = 0;

  // An estimate for the amount of data stored in ancient UNDO deltas across
  // all the rowsets picked for this rowset compaction.
  int64_t ancient_undos_total_size = 0;

  for (const auto* rs : picked) {
    const auto* drs = down_cast<const DiskRowSet*>(rs);
    const auto& dt = drs->delta_tracker();

    // Using RowSet::UNDERESTIMATE as the estimate type in this context: it's
    // necessary to be 100% sure the delta is ancient to apply the criterion
    // based on the --rowset_compaction_ancient_delta_max_ratio threshold.
    // Otherwise, the rowset merge compaction task might skip over rowsets that
    // contain mostly recent deltas until they indeed become ancient and GC-ed
    // by the UNDO delta GC maintenance task.
    int64_t size = 0;
    {
      Timestamp ancient_history_mark;
      if (Tablet::GetTabletAncientHistoryMark(&ancient_history_mark)) {
        WARN_NOT_OK(dt.EstimateBytesInPotentiallyAncientUndoDeltas(
            ancient_history_mark, RowSet::UNDERESTIMATE, &size),
            "could not estimate size of ancient UNDO deltas");
      }
    }
    ancient_undos_total_size += size;
    undos_total_size += dt.UndoDeltaOnDiskSize();
  }

  // Whether there is too much of data accumulated in ancient UNDO deltas.
  bool much_of_ancient_data = false;
  if (FLAGS_rowset_compaction_ancient_delta_threshold_enabled) {
    // Check if too much of the UNDO data in the selected rowsets is ancient.
    // If so, wait while the UNDO delta GC maintenance task does its job, if
    // the latter is enabled. Don't waste too much of IO, memory, and CPU cycles
    // working with the data that will be discarded later on.
    //
    // TODO(aserbin): instead of this workaroud, update CompactRowSetsOp
    //                maintenance operation to avoid reading in, working with,
    //                and discarding of ancient deltas; right now it's done
    //                only in the very end before persisting the result
    const auto ancient_undos_threshold = static_cast<int64_t>(
        FLAGS_rowset_compaction_ancient_delta_max_ratio *
        static_cast<double>(undos_total_size));
    if (ancient_undos_threshold < ancient_undos_total_size) {
      much_of_ancient_data = true;
      LOG_WITH_PREFIX(INFO) << Substitute(
          "compaction isn't runnable because of too much data in "
          "ancient UNDO deltas: $0 out of $1 total bytes",
          ancient_undos_total_size, undos_total_size);
    }
    VLOG_WITH_PREFIX(2) << Substitute(
        "UNDO deltas estimated size: $0 ancient; $1 total",
        ancient_undos_total_size, undos_total_size);
  }
  VLOG_WITH_PREFIX(1) << Substitute("compaction quality: $0", quality);

  stats->set_runnable(!much_of_ancient_data && quality >= 0);
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
    const RowIteratorOptions& opts,
    const ScanSpec* spec,
    vector<IterWithBounds>* iters) const {

  shared_lock<rw_spinlock> l(component_lock_);
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);

  // Construct all the iterators locally first, so that if we fail
  // in the middle, we don't modify the output arguments.
  vector<IterWithBounds> ret;

  // Grab the memrowset iterator.
  {
    unique_ptr<RowwiseIterator> ms_iter;
    RETURN_NOT_OK(components_->memrowset->NewRowIterator(opts, &ms_iter));
    IterWithBounds mrs_iwb;
    mrs_iwb.iter = std::move(ms_iter);
    ret.emplace_back(std::move(mrs_iwb));
  }

  // Capture any iterators for memrowsets whose inserts were added as a part of
  // committed transactions.
  for (const auto& txn_mrs : components_->txn_memrowsets) {
    unique_ptr<RowwiseIterator> txn_ms_iter;
    RETURN_NOT_OK(txn_mrs->NewRowIterator(opts, &txn_ms_iter));
    IterWithBounds txn_mrs_iwb;
    txn_mrs_iwb.iter = std::move(txn_ms_iter);
    ret.emplace_back(std::move(txn_mrs_iwb));
  }

  // Cull row-sets in the case of key-range queries.
  if (spec != nullptr && (spec->lower_bound_key() || spec->exclusive_upper_bound_key())) {
    optional<Slice> lower_bound = spec->lower_bound_key() ?
        optional<Slice>(spec->lower_bound_key()->encoded_key()) : nullopt;
    optional<Slice> upper_bound = spec->exclusive_upper_bound_key() ?
        optional<Slice>(spec->exclusive_upper_bound_key()->encoded_key()) : nullopt;
    vector<RowSet*> interval_sets;
    components_->rowsets->FindRowSetsIntersectingInterval(lower_bound, upper_bound, &interval_sets);
    for (const auto* rs : interval_sets) {
      IterWithBounds iwb;
      RETURN_NOT_OK_PREPEND(rs->NewRowIteratorWithBounds(opts, &iwb),
                            Substitute("Could not create iterator for rowset $0",
                                       rs->ToString()));
      ret.emplace_back(std::move(iwb));
    }
    *iters = std::move(ret);
    return Status::OK();
  }

  // If there are no encoded predicates of the primary keys, then
  // fall back to grabbing all rowset iterators.
  for (const shared_ptr<RowSet>& rs : components_->rowsets->all_rowsets()) {
    IterWithBounds iwb;
    RETURN_NOT_OK_PREPEND(rs->NewRowIteratorWithBounds(opts, &iwb),
                          Substitute("Could not create iterator for rowset $0",
                                     rs->ToString()));
    ret.emplace_back(std::move(iwb));
  }

  // Swap results into the parameters.
  *iters = std::move(ret);
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

Status Tablet::CountLiveRows(uint64_t* count) const {
  if (!metadata_->supports_live_row_count()) {
    return Status::NotSupported("This tablet doesn't support live row counting");
  }

  scoped_refptr<TabletComponents> comps;
  GetComponentsOrNull(&comps);
  if (!comps) {
    return Status::RuntimeError("The tablet has been shut down");
  }

  uint64_t ret = 0;
  uint64_t tmp = 0;
  RETURN_NOT_OK(comps->memrowset->CountLiveRows(&ret));
  for (const auto& mrs : comps->txn_memrowsets) {
    RETURN_NOT_OK(mrs->CountLiveRows(&tmp));
    ret += tmp;
  }
  for (const shared_ptr<RowSet>& rowset : comps->rowsets->all_rowsets()) {
    RETURN_NOT_OK(rowset->CountLiveRows(&tmp));
    ret += tmp;
  }
  *count = ret;
  return Status::OK();
}

size_t Tablet::MemRowSetSize() const {
  scoped_refptr<TabletComponents> comps;
  GetComponentsOrNull(&comps);

  size_t ret = 0;
  if (comps) {
    for (const auto& mrs : comps->txn_memrowsets) {
      ret += mrs->memory_footprint();
    }
    ret += comps->memrowset->memory_footprint();
  }
  return ret;
}

bool Tablet::MemRowSetEmpty() const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  const auto& txn_mrss = comps->txn_memrowsets;
  return comps->memrowset->empty() && std::all_of(txn_mrss.begin(), txn_mrss.end(),
      [] (const shared_ptr<MemRowSet>& mrs) { return mrs->empty(); });
}

size_t Tablet::MemRowSetLogReplaySize(const ReplaySizeMap& replay_size_map) const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);
  auto min_index = comps->memrowset->MinUnflushedLogIndex();
  for (const auto& mrs : comps->txn_memrowsets) {
    const auto& mrs_min_index = mrs->MinUnflushedLogIndex();
    // If the current min isn't valid, set it.
    if (min_index == -1) {
      min_index = mrs_min_index;
      continue;
    }
    // If the transaction MRS's min is valid and lower than the current, valid
    // min, set it.
    if (mrs_min_index != -1) {
      min_index = std::min(mrs_min_index, min_index);
    }
  }

  return GetReplaySizeForIndex(min_index, replay_size_map);
}

size_t Tablet::OnDiskSize() const {
  scoped_refptr<TabletComponents> comps;
  GetComponentsOrNull(&comps);

  if (!comps) return 0;

  size_t ret = metadata()->on_disk_size();
  for (const shared_ptr<RowSet> &rowset : comps->rowsets->all_rowsets()) {
    ret += rowset->OnDiskSize();
  }

  return ret;
}

size_t Tablet::OnDiskDataSize() const {
  scoped_refptr<TabletComponents> comps;
  GetComponentsOrNull(&comps);

  if (!comps) return 0;

  size_t ret = 0;
  for (const shared_ptr<RowSet> &rowset : comps->rowsets->all_rowsets()) {
    ret += rowset->OnDiskBaseDataSize();
  }
  return ret;
}

uint64_t Tablet::LastReadElapsedSeconds() const {
  shared_lock<rw_spinlock> l(last_rw_time_lock_);
  DCHECK(last_read_time_.Initialized());
  return static_cast<uint64_t>((MonoTime::Now() - last_read_time_).ToSeconds());
}

void Tablet::UpdateLastReadTime() {
  std::lock_guard<rw_spinlock> l(last_rw_time_lock_);
  last_read_time_ = MonoTime::Now();
}

uint64_t Tablet::LastWriteElapsedSeconds() const {
  shared_lock<rw_spinlock> l(last_rw_time_lock_);
  DCHECK(last_write_time_.Initialized());
  return static_cast<uint64_t>((MonoTime::Now() - last_write_time_).ToSeconds());
}

double Tablet::CollectAndUpdateWorkloadStats(MaintenanceOp::PerfImprovementOpType type) {
  DCHECK(last_update_workload_stats_time_.Initialized());
  double workload_score = 0;
  MonoDelta elapse = MonoTime::Now() - last_update_workload_stats_time_;
  if (metrics_) {
    int64_t scans_started = metrics_->scans_started->value();
    int64_t rows_mutated = metrics_->rows_inserted->value() +
                           metrics_->rows_upserted->value() +
                           metrics_->rows_updated->value() +
                           metrics_->rows_deleted->value();
    if (elapse.ToMilliseconds() > FLAGS_workload_stats_rate_collection_min_interval_ms) {
      double last_read_rate =
          static_cast<double>(scans_started - last_scans_started_) / elapse.ToSeconds();
      last_read_score_ =
          std::min(1.0, last_read_rate / FLAGS_scans_started_per_sec_for_hot_tablets) *
          FLAGS_workload_score_upper_bound;
      double last_write_rate =
          static_cast<double>(rows_mutated - last_rows_mutated_) / elapse.ToSeconds();
      last_write_score_ =
          std::min(1.0, last_write_rate / FLAGS_rows_writed_per_sec_for_hot_tablets) *
          FLAGS_workload_score_upper_bound;
    }
    if (elapse.ToMilliseconds() > FLAGS_workload_stats_metric_collection_interval_ms) {
      last_update_workload_stats_time_ = MonoTime::Now();
      last_scans_started_ = metrics_->scans_started->value();
      last_rows_mutated_ = rows_mutated;
    }
  }
  if (type == MaintenanceOp::FLUSH_OP) {
    // Flush ops are already scored based on how hot the tablet is
    // for writes, so we'll only adjust the workload score based on
    // how hot the tablet is for reads.
    workload_score = last_read_score_;
  } else if (type == MaintenanceOp::COMPACT_OP) {
    // Since compactions may improve both read and write performance, increase
    // the workload score based on the read and write rate to the tablet.
    workload_score = std::min(FLAGS_workload_score_upper_bound,
                              last_read_score_ + last_write_score_);
  }
  return workload_score;
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

Status Tablet::FlushBestDMS(const ReplaySizeMap &replay_size_map) const {
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
  shared_ptr<RowSet> rowset = FindBestDMSToFlush(replay_size_map);
  if (rowset) {
    IOContext io_context({ tablet_id() });
    return rowset->FlushDeltas(&io_context);
  }
  return Status::OK();
}

shared_ptr<RowSet> Tablet::FindBestDMSToFlush(const ReplaySizeMap& replay_size_map,
                                              int64_t* mem_size, int64_t* replay_size,
                                              MonoTime* earliest_dms_time) const {
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);
  int64_t max_dms_size = 0;
  double max_score = 0;
  double mem_weight = 0;
  int64_t dms_replay_size = 0;
  MonoTime earliest_creation_time = MonoTime::Max();
  // If system is under memory pressure, we use the percentage of the hard limit consumed
  // as mem_weight, so the tighter memory, the higher weight. Otherwise just left the
  // mem_weight to 0.
  process_memory::UnderMemoryPressure(&mem_weight);

  shared_ptr<RowSet> best_dms;
  for (const shared_ptr<RowSet>& rowset : comps->rowsets->all_rowsets()) {
    size_t dms_size_bytes;
    MonoTime creation_time;
    if (!rowset->DeltaMemStoreInfo(&dms_size_bytes, &creation_time)) {
      continue;
    }
    earliest_creation_time = std::min(earliest_creation_time, creation_time);
    int64_t replay_size_bytes = GetReplaySizeForIndex(rowset->MinUnflushedLogIndex(),
                                                      replay_size_map);
    // To facilitate memory-based flushing when under memory pressure, we
    // define a score that's part memory and part WAL retention bytes.
    double score = dms_size_bytes * mem_weight + replay_size_bytes * (100 - mem_weight);
    if ((score > max_score) ||
        // If the score is close to the max, as a tie-breaker, just look at the
        // DMS size.
        (score > max_score - 1 && dms_size_bytes > max_dms_size)) {
      max_score = score;
      max_dms_size = dms_size_bytes;
      dms_replay_size = replay_size_bytes;
      best_dms = rowset;
    }
  }
  if (earliest_dms_time) {
    *earliest_dms_time = earliest_creation_time;
  }
  if (mem_size) {
    *mem_size = max_dms_size;
  }
  if (replay_size) {
    *replay_size = dms_replay_size;
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

Status Tablet::FlushBiggestDMSForTests() {
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  size_t max_size = 0;
  shared_ptr<RowSet> biggest_drs;
  for (const auto& rowset : comps->rowsets->all_rowsets()) {
    size_t current = rowset->DeltaMemStoreSize();
    if (current > max_size) {
      max_size = current;
      biggest_drs = rowset;
    }
  }
  return biggest_drs ? biggest_drs->FlushDeltas(nullptr) : Status::OK();
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
    RETURN_NOT_OK(drs->mutable_delta_tracker()->InitAllDeltaStoresForTests(
        DeltaTracker::REDOS_ONLY));
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
#ifndef NDEBUG
  std::unique_lock<std::mutex> cs_lock(compact_select_lock_, std::try_to_lock);
  CHECK(!cs_lock.owns_lock());
#endif
  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);
  double worst_delta_perf = 0;
  shared_ptr<RowSet> worst_rs;
  for (const auto& rowset : comps->rowsets->all_rowsets()) {
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
    *rs = std::move(worst_rs);
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
    // Since the estimate is for "potentially ancient" deltas, the deltas
    // with no information on their age should be included into the result:
    // Row::OVERESTIMATE provides the desired type of estimate in this case.
    RETURN_NOT_OK(rowset->EstimateBytesInPotentiallyAncientUndoDeltas(
        ancient_history_mark, RowSet::OVERESTIMATE, &rowset_bytes));
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
  // initialize them greedily. Using the RowSet::OVERESTIMATE estimate type here
  // as in Tablet::EstimateBytesInPotentiallyAncientUndoDeltas() above
  // for the same reason.
  vector<pair<size_t, int64_t>> rowset_ancient_undos_est_sizes; // index, bytes
  rowset_ancient_undos_est_sizes.reserve(rowsets.size());
  for (size_t i = 0; i < rowsets.size(); i++) {
    const auto& rowset = rowsets[i];
    int64_t bytes;
    RETURN_NOT_OK(rowset->EstimateBytesInPotentiallyAncientUndoDeltas(
      ancient_history_mark, RowSet::OVERESTIMATE, &bytes));
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

Status Tablet::GetBytesInAncientDeletedRowsets(int64_t* bytes_in_ancient_deleted_rowsets) {
  Timestamp ancient_history_mark;
  if (!Tablet::GetTabletAncientHistoryMark(&ancient_history_mark)) {
    VLOG_WITH_PREFIX(1) << "Cannot get ancient history mark. "
                           "The clock is likely not a hybrid clock";
    *bytes_in_ancient_deleted_rowsets = 0;
    return Status::OK();
  }

  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);
  int64_t bytes = 0;
  {
    std::lock_guard<std::mutex> csl(compact_select_lock_);
    for (const auto& rowset : comps->rowsets->all_rowsets()) {
      if (!rowset->IsAvailableForCompaction()) {
        continue;
      }
      bool deleted_and_ancient = false;
      RETURN_NOT_OK(rowset->IsDeletedAndFullyAncient(ancient_history_mark, &deleted_and_ancient));
      if (deleted_and_ancient) {
        bytes += rowset->OnDiskSize();
      }
    }
  }
  metrics_->deleted_rowset_estimated_retained_bytes->set_value(bytes);
  *bytes_in_ancient_deleted_rowsets = bytes;
  return Status::OK();
}

Status Tablet::DeleteAncientDeletedRowsets() {
  RETURN_IF_STOPPED_OR_CHECK_STATE(kOpen);
  const MonoTime start_time = MonoTime::Now();
  Timestamp ancient_history_mark;
  if (!Tablet::GetTabletAncientHistoryMark(&ancient_history_mark)) {
    VLOG_WITH_PREFIX(1) << "Cannot get ancient history mark. "
                           "The clock is likely not a hybrid clock";
    return Status::OK();
  }

  scoped_refptr<TabletComponents> comps;
  GetComponents(&comps);

  // We'll take our the rowsets' locks to ensure we don't GC the rowsets while
  // they're being compacted.
  RowSetVector to_delete;
  vector<std::unique_lock<std::mutex>> rowset_locks;
  int64_t bytes_deleted = 0;
  {
    std::lock_guard<std::mutex> csl(compact_select_lock_);
    for (const auto& rowset : comps->rowsets->all_rowsets()) {
      // Check if this rowset has been locked by a compaction. If so, we
      // shouldn't attempt to delete it.
      if (!rowset->IsAvailableForCompaction()) {
        continue;
      }
      bool deleted_and_empty = false;
      RETURN_NOT_OK(rowset->IsDeletedAndFullyAncient(ancient_history_mark, &deleted_and_empty));
      if (deleted_and_empty) {
        // If we intend on deleting the rowset, take its lock so concurrent
        // compactions don't try to select it for compactions.
        std::unique_lock<std::mutex> l(*rowset->compact_flush_lock(), std::try_to_lock);
        CHECK(l.owns_lock());
        to_delete.emplace_back(rowset);
        rowset_locks.emplace_back(std::move(l));
        bytes_deleted += rowset->OnDiskSize();
      }
    }
  }
  if (to_delete.empty()) {
    return Status::OK();
  }
  RETURN_NOT_OK(HandleEmptyCompactionOrFlush(
      to_delete, TabletMetadata::kNoMrsFlushed, {}));
  metrics_->deleted_rowset_gc_bytes_deleted->IncrementBy(bytes_deleted);
  metrics_->deleted_rowset_gc_duration->Increment((MonoTime::Now() - start_time).ToMilliseconds());
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
  DCHECK(o);
  auto& out = *o;

  shared_ptr<RowSetTree> rowsets_copy;
  {
    shared_lock<rw_spinlock> l(component_lock_);
    rowsets_copy = components_->rowsets;
  }
  std::lock_guard<std::mutex> compact_lock(compact_select_lock_);
  // Run the compaction policy in order to get its log and highlight those
  // rowsets which would be compacted next.
  vector<string> log;
  unordered_set<const RowSet*> picked;
  double quality;
  Status s = compaction_policy_->PickRowSets(*rowsets_copy, &picked, &quality, &log);
  if (!s.ok()) {
    out << "<b>Error:</b> " << EscapeForHtmlToString(s.ToString());
    return;
  }

  if (!picked.empty()) {
    out << "<p>";
    out << "Highlighted rowsets indicate those that would be compacted next if a "
        << "compaction were to run on this tablet.";
    out << "</p>";
  }

  double rowset_total_height;
  double rowset_total_width;
  vector<RowSetInfo> min;
  vector<RowSetInfo> max;
  RowSetInfo::ComputeCdfAndCollectOrdered(*rowsets_copy,
                                          /*is_on_memory_budget=*/nullopt,
                                          &rowset_total_height,
                                          &rowset_total_width,
                                          &min,
                                          &max);
  double average_rowset_height = rowset_total_width > 0
                               ? rowset_total_height / rowset_total_width
                               : 0.0;
  DumpCompactionSVG(min, picked, o, /*print_xml_header=*/false);

  // Compaction policy ignores rowsets unavailable for compaction. This is good,
  // except it causes the SVG to be potentially missing rowsets. It's hard to
  // take these presently-compacting rowsets into account because we are racing
  // against the compaction finishing, and at the end of the compaction the
  // rowsets might no longer exist (merge compaction) or their bounds may have
  // changed (major delta compaction). So, let's just disclose how many of these
  // rowsets there are.
  int num_rowsets_unavailable_for_compaction = std::count_if(
      rowsets_copy->all_rowsets().begin(),
      rowsets_copy->all_rowsets().end(),
      [](const shared_ptr<RowSet>& rowset) {
        // The first condition excludes the memrowset.
        return rowset->metadata() && !rowset->IsAvailableForCompaction();
      });
  out << Substitute("<div><p>In addition to the rowsets pictured and listed, "
                    "there are $0 rowset(s) currently undergoing compactions."
                    "</p></div>",
                    num_rowsets_unavailable_for_compaction)
      << endl;

  // Compute some summary statistics for the tablet's rowsets.
  const auto num_rowsets = min.size();
  if (num_rowsets > 0) {
    vector<int64_t> rowset_sizes;
    rowset_sizes.reserve(num_rowsets);
    for (const auto& rsi : min) {
      rowset_sizes.push_back(rsi.size_bytes());
    }
    out << "<table class=\"table tablet-striped table-hover\">" << endl;
    // Compute the stats quick'n'dirty by sorting and looking at approximately
    // the right spot.
    // TODO(wdberkeley): Could use an O(n) quickselect-based algorithm.
    // TODO(wdberkeley): A bona fide box-and-whisker plot would be nice.
    // d3.js can make really nice ones: https://bl.ocks.org/mbostock/4061502.
    std::sort(rowset_sizes.begin(), rowset_sizes.end());
    const auto size_bytes_min = rowset_sizes[0];
    const auto size_bytes_first_quartile = rowset_sizes[num_rowsets / 4];
    const auto size_bytes_median = rowset_sizes[num_rowsets / 2];
    const auto size_bytes_third_quartile = rowset_sizes[3 * num_rowsets / 4];
    const auto size_bytes_max = rowset_sizes[num_rowsets - 1];
    out << Substitute("<thead><tr>"
                      "  <th>Statistic</th>"
                      "  <th>Approximate Value</th>"
                      "<tr></thead>"
                      "<tbody>"
                      "  <tr><td>Count</td><td>$0</td></tr>"
                      "  <tr><td>Min</td><td>$1</td></tr>"
                      "  <tr><td>First quartile</td><td>$2</td></tr>"
                      "  <tr><td>Median</td><td>$3</td></tr>"
                      "  <tr><td>Third quartile</td><td>$4</td></tr>"
                      "  <tr><td>Max</td><td>$5</td></tr>"
                      "  <tr><td>Avg. Height</td><td>$6</td></tr>"
                      "<tbody>",
                      num_rowsets,
                      HumanReadableNumBytes::ToString(size_bytes_min),
                      HumanReadableNumBytes::ToString(size_bytes_first_quartile),
                      HumanReadableNumBytes::ToString(size_bytes_median),
                      HumanReadableNumBytes::ToString(size_bytes_third_quartile),
                      HumanReadableNumBytes::ToString(size_bytes_max),
                      average_rowset_height);
    out << "</table>" << endl;
  }

  // TODO(wdberkeley): Should we even display this? It's one line per rowset
  // and doesn't contain any useful information except each rowset's size.
  out << "<h2>Compaction policy log</h2>" << endl;

  out << "<pre>" << std::endl;
  for (const string& s : log) {
    out << EscapeForHtmlToString(s) << endl;
  }
  out << "</pre>" << endl;
}

string Tablet::LogPrefix() const {
  return Substitute("T $0 P $1: ", tablet_id(), metadata_->fs_manager()->uuid());
}

////////////////////////////////////////////////////////////
// Tablet::Iterator
////////////////////////////////////////////////////////////

Tablet::Iterator::Iterator(const Tablet* tablet,
                           RowIteratorOptions opts)
    : tablet_(tablet),
      io_context_({ tablet->tablet_id() }),
      projection_(*CHECK_NOTNULL(opts.projection)),
      opts_(std::move(opts)) {
  opts_.io_context = &io_context_;
  opts_.projection = &projection_;
}

Tablet::Iterator::~Iterator() {}

Status Tablet::Iterator::Init(ScanSpec *spec) {
  RETURN_NOT_OK(tablet_->CheckHasNotBeenStopped());
  DCHECK(iter_.get() == nullptr);

  RETURN_NOT_OK(tablet_->GetMappedReadProjection(projection_, &projection_));

  vector<IterWithBounds> iters;
  RETURN_NOT_OK(tablet_->CaptureConsistentIterators(opts_, spec, &iters));
  TRACE_COUNTER_INCREMENT("rowset_iterators", iters.size());

  switch (opts_.order) {
    case ORDERED:
      iter_ = NewMergeIterator(MergeIteratorOptions(opts_.include_deleted_rows), std::move(iters));
      break;
    case UNORDERED:
    default:
      iter_ = NewUnionIterator(std::move(iters));
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

const Schema& Tablet::Iterator::schema() const {
  return *opts_.projection;
}

void Tablet::Iterator::GetIteratorStats(vector<IteratorStats>* stats) const {
  iter_->GetIteratorStats(stats);
}

} // namespace tablet
} // namespace kudu
