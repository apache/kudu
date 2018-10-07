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

#include "kudu/tablet/tablet_mm_ops.h"

#include <mutex>
#include <ostream>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

DEFINE_int32(undo_delta_block_gc_init_budget_millis, 1000,
    "The maximum number of milliseconds we will spend initializing "
    "UNDO delta blocks per invocation of UndoDeltaBlockGCOp. Existing delta "
    "blocks must be initialized once per process startup to determine "
    "when they can be deleted.");
TAG_FLAG(undo_delta_block_gc_init_budget_millis, evolving);
TAG_FLAG(undo_delta_block_gc_init_budget_millis, advanced);

DEFINE_bool(enable_major_delta_compaction, true,
    "Whether to enable major delta compaction. Disabling major delta "
    "compaction may worsen performance and increase disk space usage for "
    "workloads involving updates and deletes.");
TAG_FLAG(enable_major_delta_compaction, runtime);
TAG_FLAG(enable_major_delta_compaction, unsafe);

DEFINE_bool(enable_minor_delta_compaction, true,
    "Whether to enable minor delta compaction. Disabling minor delta "
    "compaction may worsen performance and increase disk space usage for "
    "workloads involving updates and deletes.");
TAG_FLAG(enable_minor_delta_compaction, runtime);
TAG_FLAG(enable_minor_delta_compaction, unsafe);

DEFINE_bool(enable_rowset_compaction, true,
    "Whether to enable rowset compaction. Disabling rowset compaction "
    "may worsen performance and increase disk space usage.");
TAG_FLAG(enable_rowset_compaction, runtime);
TAG_FLAG(enable_rowset_compaction, unsafe);

DEFINE_bool(enable_undo_delta_block_gc, true,
    "Whether to enable undo delta block garbage collection. Disabling undo "
    "delta block garbage collection may worsen performance and increase disk "
    "space usage for workloads involving updates and deletes. This only "
    "affects the undo delta block deletion background task, and doesn't "
    "control whether compactions delete ancient history. To change what is "
    "considered ancient history use --tablet_history_max_age_sec");
TAG_FLAG(enable_undo_delta_block_gc, runtime);
TAG_FLAG(enable_undo_delta_block_gc, unsafe);

using std::string;
using strings::Substitute;

namespace kudu {
namespace tablet {

TabletOpBase::TabletOpBase(string name, IOUsage io_usage, Tablet* tablet)
    : MaintenanceOp(std::move(name), io_usage),
      tablet_(tablet) {
}

string TabletOpBase::LogPrefix() const {
  return tablet_->LogPrefix();
}

////////////////////////////////////////////////////////////
// CompactRowSetsOp
////////////////////////////////////////////////////////////

CompactRowSetsOp::CompactRowSetsOp(Tablet* tablet)
  : TabletOpBase(Substitute("CompactRowSetsOp($0)", tablet->tablet_id()),
                 MaintenanceOp::HIGH_IO_USAGE, tablet),
    last_num_mrs_flushed_(0),
    last_num_rs_compacted_(0) {
}

void CompactRowSetsOp::UpdateStats(MaintenanceOpStats* stats) {
  if (PREDICT_FALSE(!FLAGS_enable_rowset_compaction)) {
    KLOG_EVERY_N_SECS(WARNING, 300)
        << "Rowset compaction is disabled (check --enable_rowset_compaction)";
    stats->set_runnable(false);
    return;
  }

  std::lock_guard<simple_spinlock> l(lock_);

  // Any operation that changes the on-disk row layout invalidates the
  // cached stats.
  TabletMetrics* metrics = tablet_->metrics();
  if (metrics) {
    uint64_t new_num_mrs_flushed = metrics->flush_mrs_duration->TotalCount();
    uint64_t new_num_rs_compacted = metrics->compact_rs_duration->TotalCount();
    if (prev_stats_.valid() &&
        new_num_mrs_flushed == last_num_mrs_flushed_ &&
        new_num_rs_compacted == last_num_rs_compacted_) {
      *stats = prev_stats_;
      return;
    } else {
      last_num_mrs_flushed_ = new_num_mrs_flushed;
      last_num_rs_compacted_ = new_num_rs_compacted;
    }
  }

  tablet_->UpdateCompactionStats(&prev_stats_);
  *stats = prev_stats_;
}

bool CompactRowSetsOp::Prepare() {
  std::lock_guard<simple_spinlock> l(lock_);
  // Invalidate the cached stats so that another section of the tablet can
  // be compacted concurrently.
  //
  // TODO: we should acquire the rowset compaction locks here. Otherwise, until
  // Compact() acquires them, the maintenance manager may compute the same
  // stats for this op and run it again, even though Perform() will end up
  // performing a much less fruitful compaction. See KUDU-790 for more details.
  prev_stats_.Clear();
  return true;
}

void CompactRowSetsOp::Perform() {
  WARN_NOT_OK(tablet_->Compact(Tablet::COMPACT_NO_FLAGS),
              Substitute("$0Compaction failed on $1",
                         LogPrefix(), tablet_->tablet_id()));
}

scoped_refptr<Histogram> CompactRowSetsOp::DurationHistogram() const {
  return tablet_->metrics()->compact_rs_duration;
}

scoped_refptr<AtomicGauge<uint32_t> > CompactRowSetsOp::RunningGauge() const {
  return tablet_->metrics()->compact_rs_running;
}

////////////////////////////////////////////////////////////
// MinorDeltaCompactionOp
////////////////////////////////////////////////////////////

MinorDeltaCompactionOp::MinorDeltaCompactionOp(Tablet* tablet)
  : TabletOpBase(Substitute("MinorDeltaCompactionOp($0)", tablet->tablet_id()),
                 MaintenanceOp::HIGH_IO_USAGE, tablet),
    last_num_mrs_flushed_(0),
    last_num_dms_flushed_(0),
    last_num_rs_compacted_(0),
    last_num_rs_minor_delta_compacted_(0) {
}

void MinorDeltaCompactionOp::UpdateStats(MaintenanceOpStats* stats) {
  if (PREDICT_FALSE(!FLAGS_enable_minor_delta_compaction)) {
    KLOG_EVERY_N_SECS(WARNING, 300)
        << "Minor delta compaction is disabled (check --enable_minor_delta_compaction)";
    stats->set_runnable(false);
    return;
  }

  std::lock_guard<simple_spinlock> l(lock_);

  // Any operation that changes the number of REDO files invalidates the
  // cached stats.
  TabletMetrics* metrics = tablet_->metrics();
  if (metrics) {
    uint64_t new_num_mrs_flushed = metrics->flush_mrs_duration->TotalCount();
    uint64_t new_num_dms_flushed = metrics->flush_dms_duration->TotalCount();
    uint64_t new_num_rs_compacted = metrics->compact_rs_duration->TotalCount();
    uint64_t new_num_rs_minor_delta_compacted =
        metrics->delta_minor_compact_rs_duration->TotalCount();
    if (prev_stats_.valid() &&
        new_num_mrs_flushed == last_num_mrs_flushed_ &&
        new_num_dms_flushed == last_num_dms_flushed_ &&
        new_num_rs_compacted == last_num_rs_compacted_ &&
        new_num_rs_minor_delta_compacted == last_num_rs_minor_delta_compacted_) {
      *stats = prev_stats_;
      return;
    } else {
      last_num_mrs_flushed_ = new_num_mrs_flushed;
      last_num_dms_flushed_ = new_num_dms_flushed;
      last_num_rs_compacted_ = new_num_rs_compacted;
      last_num_rs_minor_delta_compacted_ = new_num_rs_minor_delta_compacted;
    }
  }

  double perf_improv = tablet_->GetPerfImprovementForBestDeltaCompact(
      RowSet::MINOR_DELTA_COMPACTION, nullptr);
  prev_stats_.set_perf_improvement(perf_improv);
  prev_stats_.set_runnable(perf_improv > 0);
  *stats = prev_stats_;
}

bool MinorDeltaCompactionOp::Prepare() {
  std::lock_guard<simple_spinlock> l(lock_);
  // Invalidate the cached stats so that another rowset in the tablet can
  // be delta compacted concurrently.
  //
  // TODO: See CompactRowSetsOp::Prepare().
  prev_stats_.Clear();
  return true;
}

void MinorDeltaCompactionOp::Perform() {
  WARN_NOT_OK(tablet_->CompactWorstDeltas(RowSet::MINOR_DELTA_COMPACTION),
              Substitute("$0Minor delta compaction failed on $1",
                         LogPrefix(), tablet_->tablet_id()));
}

scoped_refptr<Histogram> MinorDeltaCompactionOp::DurationHistogram() const {
  return tablet_->metrics()->delta_minor_compact_rs_duration;
}

scoped_refptr<AtomicGauge<uint32_t> > MinorDeltaCompactionOp::RunningGauge() const {
  return tablet_->metrics()->delta_minor_compact_rs_running;
}

////////////////////////////////////////////////////////////
// MajorDeltaCompactionOp
////////////////////////////////////////////////////////////

MajorDeltaCompactionOp::MajorDeltaCompactionOp(Tablet* tablet)
  : TabletOpBase(Substitute("MajorDeltaCompactionOp($0)", tablet->tablet_id()),
                 MaintenanceOp::HIGH_IO_USAGE, tablet),
    last_num_mrs_flushed_(0),
    last_num_dms_flushed_(0),
    last_num_rs_compacted_(0),
    last_num_rs_minor_delta_compacted_(0),
    last_num_rs_major_delta_compacted_(0) {
}

void MajorDeltaCompactionOp::UpdateStats(MaintenanceOpStats* stats) {
  if (PREDICT_FALSE(!FLAGS_enable_major_delta_compaction)) {
    KLOG_EVERY_N_SECS(WARNING, 300)
        << "Major delta compaction is disabled (check --enable_major_delta_compaction)";
    stats->set_runnable(false);
    return;
  }

  std::lock_guard<simple_spinlock> l(lock_);

  // Any operation that changes the size of the on-disk data invalidates the
  // cached stats.
  TabletMetrics* metrics = tablet_->metrics();
  if (metrics) {
    int64_t new_num_mrs_flushed = metrics->flush_mrs_duration->TotalCount();
    int64_t new_num_dms_flushed = metrics->flush_dms_duration->TotalCount();
    int64_t new_num_rs_compacted = metrics->compact_rs_duration->TotalCount();
    int64_t new_num_rs_minor_delta_compacted =
        metrics->delta_minor_compact_rs_duration->TotalCount();
    int64_t new_num_rs_major_delta_compacted =
        metrics->delta_major_compact_rs_duration->TotalCount();
    if (prev_stats_.valid() &&
        new_num_mrs_flushed == last_num_mrs_flushed_ &&
        new_num_dms_flushed == last_num_dms_flushed_ &&
        new_num_rs_compacted == last_num_rs_compacted_ &&
        new_num_rs_minor_delta_compacted == last_num_rs_minor_delta_compacted_ &&
        new_num_rs_major_delta_compacted == last_num_rs_major_delta_compacted_) {
      *stats = prev_stats_;
      return;
    } else {
      last_num_mrs_flushed_ = new_num_mrs_flushed;
      last_num_dms_flushed_ = new_num_dms_flushed;
      last_num_rs_compacted_ = new_num_rs_compacted;
      last_num_rs_minor_delta_compacted_ = new_num_rs_minor_delta_compacted;
      last_num_rs_major_delta_compacted_ = new_num_rs_major_delta_compacted;
    }
  }

  double perf_improv = tablet_->GetPerfImprovementForBestDeltaCompact(
      RowSet::MAJOR_DELTA_COMPACTION, nullptr);
  prev_stats_.set_perf_improvement(perf_improv);
  prev_stats_.set_runnable(perf_improv > 0);
  *stats = prev_stats_;
}

bool MajorDeltaCompactionOp::Prepare() {
  std::lock_guard<simple_spinlock> l(lock_);
  // Invalidate the cached stats so that another rowset in the tablet can
  // be delta compacted concurrently.
  //
  // TODO: See CompactRowSetsOp::Prepare().
  prev_stats_.Clear();
  return true;
}

void MajorDeltaCompactionOp::Perform() {
  WARN_NOT_OK(tablet_->CompactWorstDeltas(RowSet::MAJOR_DELTA_COMPACTION),
              Substitute("$0Major delta compaction failed on $1",
                         LogPrefix(), tablet_->tablet_id()));
}

scoped_refptr<Histogram> MajorDeltaCompactionOp::DurationHistogram() const {
  return tablet_->metrics()->delta_major_compact_rs_duration;
}

scoped_refptr<AtomicGauge<uint32_t> > MajorDeltaCompactionOp::RunningGauge() const {
  return tablet_->metrics()->delta_major_compact_rs_running;
}

////////////////////////////////////////////////////////////
// UndoDeltaBlockGCOp
////////////////////////////////////////////////////////////

UndoDeltaBlockGCOp::UndoDeltaBlockGCOp(Tablet* tablet)
  : TabletOpBase(Substitute("UndoDeltaBlockGCOp($0)", tablet->tablet_id()),
                 MaintenanceOp::HIGH_IO_USAGE, tablet) {
}

void UndoDeltaBlockGCOp::UpdateStats(MaintenanceOpStats* stats) {
  if (PREDICT_FALSE(!FLAGS_enable_undo_delta_block_gc)) {
    KLOG_EVERY_N_SECS(WARNING, 300)
        << "Undo delta block GC is disabled (check --enable_undo_delta_block_gc)";
    stats->set_runnable(false);
    return;
  }

  int64_t max_estimated_retained_bytes = 0;
  WARN_NOT_OK(tablet_->EstimateBytesInPotentiallyAncientUndoDeltas(&max_estimated_retained_bytes),
              "Unable to count bytes in potentially ancient undo deltas");
  stats->set_data_retained_bytes(max_estimated_retained_bytes);
  stats->set_runnable(max_estimated_retained_bytes > 0);
}

bool UndoDeltaBlockGCOp::Prepare() {
  // Nothing for us to do.
  return true;
}

void UndoDeltaBlockGCOp::Perform() {
  MonoDelta time_budget = MonoDelta::FromMilliseconds(FLAGS_undo_delta_block_gc_init_budget_millis);
  int64_t bytes_in_ancient_undos = 0;
  Status s = tablet_->InitAncientUndoDeltas(time_budget, &bytes_in_ancient_undos);
  if (PREDICT_FALSE(!s.ok())) {
    LOG_WITH_PREFIX(WARNING) << s.ToString();
    return;
  }

  // Return early if it turns out that we have nothing to GC.
  if (bytes_in_ancient_undos == 0) return;

  CHECK_OK_PREPEND(tablet_->DeleteAncientUndoDeltas(),
                   Substitute("$0GC of undo delta blocks failed", LogPrefix()));
}

scoped_refptr<Histogram> UndoDeltaBlockGCOp::DurationHistogram() const {
  return tablet_->metrics()->undo_delta_block_gc_perform_duration;
}

scoped_refptr<AtomicGauge<uint32_t>> UndoDeltaBlockGCOp::RunningGauge() const {
  return tablet_->metrics()->undo_delta_block_gc_running;
}

std::string UndoDeltaBlockGCOp::LogPrefix() const {
  return tablet_->LogPrefix();
}

} // namespace tablet
} // namespace kudu
