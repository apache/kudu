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

#include "kudu/tablet/tablet_replica_mm_ops.h"

#include <map>
#include <mutex>
#include <ostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"

DEFINE_bool(enable_flush_memrowset, true,
    "Whether to enable memrowset flush. Disabling memrowset flush prevents "
    "the tablet server from flushing writes to diskrowsets, resulting in "
    "increasing memory and WAL disk space usage.");
TAG_FLAG(enable_flush_memrowset, runtime);
TAG_FLAG(enable_flush_memrowset, unsafe);

DEFINE_bool(enable_flush_deltamemstores, true,
    "Whether to enable deltamemstore flush. Disabling deltamemstore flush "
    "prevents the tablet server from flushing updates to deltafiles, resulting "
    "in increasing memory and WAL disk space usage for workloads involving "
    "updates and deletes.");
TAG_FLAG(enable_flush_deltamemstores, runtime);
TAG_FLAG(enable_flush_deltamemstores, unsafe);

DEFINE_bool(enable_log_gc, true,
    "Whether to enable write-ahead log garbage collection. Disabling WAL "
    "garbage collection will cause the tablet server to stop reclaiming space "
    "from the WAL, leading to increasing WAL disk space usage.");
TAG_FLAG(enable_log_gc, runtime);
TAG_FLAG(enable_log_gc, unsafe);

DEFINE_int32(flush_threshold_mb, 1024,
             "Size at which MemRowSet flushes are triggered. "
             "A MRS can still flush below this threshold if it if hasn't flushed in a while, "
             "or if the server-wide memory limit has been reached.");
TAG_FLAG(flush_threshold_mb, experimental);
TAG_FLAG(flush_threshold_mb, runtime);

DEFINE_int32(flush_threshold_secs, 2 * 60,
             "Number of seconds after which a non-empty MemRowSet will become flushable "
             "even if it is not large.");
TAG_FLAG(flush_threshold_secs, experimental);
TAG_FLAG(flush_threshold_secs, runtime);


METRIC_DEFINE_gauge_uint32(tablet, log_gc_running,
                           "Log GCs Running",
                           kudu::MetricUnit::kOperations,
                           "Number of log GC operations currently running.");
METRIC_DEFINE_histogram(tablet, log_gc_duration,
                        "Log GC Duration",
                        kudu::MetricUnit::kMilliseconds,
                        "Time spent garbage collecting the logs.", 60000LU, 1);

namespace kudu {
namespace tablet {

using std::map;
using strings::Substitute;

// Upper bound for how long it takes to reach "full perf improvement" in time-based flushing.
const double kFlushUpperBoundMs = 60 * 60 * 1000;

//
// FlushOpPerfImprovementPolicy.
//

void FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(MaintenanceOpStats* stats,
                                                              double elapsed_ms) {
  double anchored_mb = static_cast<double>(stats->ram_anchored()) / (1024 * 1024);
  const double threshold_mb = FLAGS_flush_threshold_mb;
  if (anchored_mb > threshold_mb) {
    // If we're over the user-specified flush threshold, then consider the perf
    // improvement to be 1 for every extra MB.  This produces perf_improvement results
    // which are much higher than most compactions would produce, and means that, when
    // there is an MRS over threshold, a flush will almost always be selected instead of
    // a compaction.  That's not necessarily a good thing, but in the absence of better
    // heuristics, it will do for now.
    double extra_mb = anchored_mb - threshold_mb;
    DCHECK_GE(extra_mb, 0);
    stats->set_perf_improvement(extra_mb);
  } else if (elapsed_ms > FLAGS_flush_threshold_secs * 1000) {
    // Even if we aren't over the threshold, consider flushing if we haven't flushed
    // in a long time. But, don't give it a large perf_improvement score. We should
    // only do this if we really don't have much else to do, and if we've already waited a bit.
    // The following will give an improvement that's between 0.0 and 1.0, gradually growing
    // as 'elapsed_ms' approaches 'kFlushUpperBoundMs'.
    double perf = elapsed_ms / kFlushUpperBoundMs;
    if (perf > 1.0) {
      perf = 1.0;
    }
    stats->set_perf_improvement(perf);
  }
}

//
// FlushMRSOp.
//

void FlushMRSOp::UpdateStats(MaintenanceOpStats* stats) {
  if (PREDICT_FALSE(!FLAGS_enable_flush_memrowset)) {
    KLOG_EVERY_N_SECS(WARNING, 300)
        << "Memrowset flush is disabled (check --enable_flush_memrowset)";
    stats->set_runnable(false);
    return;
  }

  std::lock_guard<simple_spinlock> l(lock_);

  map<int64_t, int64_t> replay_size_map;
  if (tablet_replica_->tablet()->MemRowSetEmpty() ||
      !tablet_replica_->GetReplaySizeMap(&replay_size_map).ok()) {
    return;
  }

  {
    std::unique_lock<Semaphore> lock(tablet_replica_->tablet()->rowsets_flush_sem_,
                                     std::defer_lock);
    stats->set_runnable(lock.try_lock());
  }

  stats->set_ram_anchored(tablet_replica_->tablet()->MemRowSetSize());
  stats->set_logs_retained_bytes(
      tablet_replica_->tablet()->MemRowSetLogReplaySize(replay_size_map));

  // TODO(todd): use workload statistics here to find out how "hot" the tablet has
  // been in the last 5 minutes.
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(
      stats,
      time_since_flush_.elapsed().wall_millis());
}

bool FlushMRSOp::Prepare() {
  // Try to acquire the rowsets_flush_sem_.  If we can't, the Prepare step
  // fails.  This also implies that only one instance of FlushMRSOp can be
  // running at once.
  return tablet_replica_->tablet()->rowsets_flush_sem_.try_lock();
}

void FlushMRSOp::Perform() {
  Tablet* tablet = tablet_replica_->tablet();
  CHECK(!tablet->rowsets_flush_sem_.try_lock());
  SCOPED_CLEANUP({
    tablet->rowsets_flush_sem_.unlock();
  });

  Status s = tablet->FlushUnlocked();
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << tablet->LogPrefix() << "failed to flush MRS: " << s.ToString();
    CHECK(tablet->HasBeenStopped()) << "Unrecoverable flush failure caused by error: "
                                    << s.ToString();
    return;
  }

  {
    std::lock_guard<simple_spinlock> l(lock_);
    time_since_flush_.start();
  }
}

scoped_refptr<Histogram> FlushMRSOp::DurationHistogram() const {
  return tablet_replica_->tablet()->metrics()->flush_mrs_duration;
}

scoped_refptr<AtomicGauge<uint32_t> > FlushMRSOp::RunningGauge() const {
  return tablet_replica_->tablet()->metrics()->flush_mrs_running;
}

//
// FlushDeltaMemStoresOp.
//

void FlushDeltaMemStoresOp::UpdateStats(MaintenanceOpStats* stats) {
  if (PREDICT_FALSE(!FLAGS_enable_flush_deltamemstores)) {
    KLOG_EVERY_N_SECS(WARNING, 300)
        << "Deltamemstore flush is disabled (check --enable_flush_deltamemstores)";
    stats->set_runnable(false);
    return;
  }

  std::lock_guard<simple_spinlock> l(lock_);
  int64_t dms_size;
  int64_t retention_size;
  map<int64_t, int64_t> max_idx_to_replay_size;
  if (tablet_replica_->tablet()->DeltaMemRowSetEmpty() ||
      !tablet_replica_->GetReplaySizeMap(&max_idx_to_replay_size).ok()) {
    return;
  }
  tablet_replica_->tablet()->GetInfoForBestDMSToFlush(max_idx_to_replay_size,
                                                   &dms_size, &retention_size);

  stats->set_ram_anchored(dms_size);
  stats->set_runnable(true);
  stats->set_logs_retained_bytes(retention_size);

  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(
      stats,
      time_since_flush_.elapsed().wall_millis());
}

void FlushDeltaMemStoresOp::Perform() {
  map<int64_t, int64_t> max_idx_to_replay_size;
  if (!tablet_replica_->GetReplaySizeMap(&max_idx_to_replay_size).ok()) {
    LOG(WARNING) << "Won't flush deltas since tablet shutting down: "
                 << tablet_replica_->tablet_id();
    return;
  }
  Tablet* tablet = tablet_replica_->tablet();
  Status s = tablet->FlushBestDMS(max_idx_to_replay_size);
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << tablet->LogPrefix() << "failed to flush DMS: " << s.ToString();
    CHECK(tablet->HasBeenStopped()) << "Unrecoverable flush failure caused by error: "
                                    << s.ToString();
    return;
  }
  {
    std::lock_guard<simple_spinlock> l(lock_);
    time_since_flush_.start();
  }
}

scoped_refptr<Histogram> FlushDeltaMemStoresOp::DurationHistogram() const {
  return tablet_replica_->tablet()->metrics()->flush_dms_duration;
}

scoped_refptr<AtomicGauge<uint32_t> > FlushDeltaMemStoresOp::RunningGauge() const {
  return tablet_replica_->tablet()->metrics()->flush_dms_running;
}

//
// LogGCOp.
//

LogGCOp::LogGCOp(TabletReplica* tablet_replica)
    : MaintenanceOp(StringPrintf("LogGCOp(%s)", tablet_replica->tablet()->tablet_id().c_str()),
                    MaintenanceOp::LOW_IO_USAGE),
      tablet_replica_(tablet_replica),
      log_gc_duration_(METRIC_log_gc_duration.Instantiate(
                           tablet_replica->tablet()->GetMetricEntity())),
      log_gc_running_(METRIC_log_gc_running.Instantiate(
                          tablet_replica->tablet()->GetMetricEntity(), 0)),
      sem_(1) {}

void LogGCOp::UpdateStats(MaintenanceOpStats* stats) {
  if (PREDICT_FALSE(!FLAGS_enable_log_gc)) {
    KLOG_EVERY_N_SECS(WARNING, 300)
        << "Log GC is disabled (check --enable_log_gc)";
    stats->set_runnable(false);
    return;
  }

  int64_t retention_size;

  if (!tablet_replica_->GetGCableDataSize(&retention_size).ok()) {
    return;
  }

  stats->set_logs_retained_bytes(retention_size);
  stats->set_runnable(sem_.GetValue() == 1);
}

bool LogGCOp::Prepare() {
  return sem_.try_lock();
}

void LogGCOp::Perform() {
  CHECK(!sem_.try_lock());

  tablet_replica_->RunLogGC();

  sem_.unlock();
}

scoped_refptr<Histogram> LogGCOp::DurationHistogram() const {
  return log_gc_duration_;
}

scoped_refptr<AtomicGauge<uint32_t> > LogGCOp::RunningGauge() const {
  return log_gc_running_;
}

}  // namespace tablet
}  // namespace kudu
