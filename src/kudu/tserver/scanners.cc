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
#include "kudu/tserver/scanners.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <numeric>
#include <ostream>
#include <type_traits>

#include <gflags/gflags.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/iterator.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/hash/string_hash.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tserver/scanner_metrics.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/thread.h"

DEFINE_int32(scanner_ttl_ms, 60000,
             "Number of milliseconds of inactivity allowed for a scanner "
             "before it may be expired");
TAG_FLAG(scanner_ttl_ms, advanced);
TAG_FLAG(scanner_ttl_ms, runtime);

DEFINE_int32(scanner_gc_check_interval_us, 5 * 1000L * 1000L, // 5 seconds
             "Number of microseconds in the interval at which we remove expired scanners");
TAG_FLAG(scanner_gc_check_interval_us, hidden);

DEFINE_int32(completed_scan_history_count, 10,
             "Number of latest scans to keep history for. Determines how many historical "
             "latest scans will be shown on the tablet server's scans dashboard.");
TAG_FLAG(completed_scan_history_count, experimental);

DEFINE_bool(show_slow_scans, false,
            "Whether to show slow scans on the /scans page of web or record it in the log. "
            "Please note that once set to true, full table scans may occur, which may affect "
            "the normal Kudu service unexpectedly.");
TAG_FLAG(show_slow_scans, advanced);
TAG_FLAG(show_slow_scans, runtime);

DEFINE_int32(slow_scanner_threshold_ms, 60 * 1000L, // 1 minute
             "Number of milliseconds for the threshold of slow scan.");
TAG_FLAG(slow_scanner_threshold_ms, advanced);
TAG_FLAG(slow_scanner_threshold_ms, runtime);

DEFINE_int32(slow_scan_history_count, 10,
             "Number of slow scans to keep history for. Determines how many historical "
             "slow scans will be shown on the tablet server's scans dashboard. The "
             "threshold for a slow scan is defined with --slow_scanner_threshold_ms.");
TAG_FLAG(slow_scan_history_count, experimental);

DECLARE_int32(rpc_default_keepalive_time_ms);

METRIC_DEFINE_gauge_size(server, active_scanners,
                         "Active Scanners",
                         kudu::MetricUnit::kScanners,
                         "Number of scanners that are currently active",
                         kudu::MetricLevel::kInfo);

using kudu::rpc::RemoteUser;
using kudu::tablet::TabletReplica;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace {

// Issue a warning if --rpc_default_keepalive_time_ms and --scanner_ttl_ms flags
// are set improperly. That's just a warning, not an error: the client should be
// able to re-establish the connection even if it's been closed by the server.
// However, it takes extra roundtrips and adds extra latency to the scan time.
bool ValidateScannerAndRpcConnectionIdleTimes() {
  if (FLAGS_rpc_default_keepalive_time_ms >= 0 &&
      FLAGS_rpc_default_keepalive_time_ms < FLAGS_scanner_ttl_ms) {
    LOG(WARNING) << Substitute(
        "--rpc_default_keepalive_time_ms (currently $0) should be set at least "
        "as high as --scanner_ttl_ms (currently $1) to prevent servers closing "
        "idle client connections to not-yet-expired scanners",
        FLAGS_rpc_default_keepalive_time_ms, FLAGS_scanner_ttl_ms);
  }
  return true;
}

GROUP_FLAG_VALIDATOR(idle_intervals, ValidateScannerAndRpcConnectionIdleTimes);
} // anonymous namespace

namespace kudu {
namespace tserver {

ScannerManager::ScannerManager(const scoped_refptr<MetricEntity>& metric_entity)
    : shutdown_(false),
      shutdown_cv_(&shutdown_lock_),
      completed_scans_offset_(0),
      slow_scans_offset_(0) {
  if (metric_entity) {
    metrics_.reset(new ScannerMetrics(metric_entity));
    METRIC_active_scanners.InstantiateFunctionGauge(
        metric_entity, [this]() { return this->CountActiveScanners(); })
        ->AutoDetach(&metric_detacher_);
  }
  for (size_t i = 0; i < kNumScannerMapStripes; i++) {
    scanner_maps_.push_back(new ScannerMapStripe());
  }

  if (FLAGS_completed_scan_history_count > 0) {
    completed_scans_.reserve(FLAGS_completed_scan_history_count);
  }

  if (FLAGS_slow_scan_history_count > 0) {
    slow_scans_.reserve(FLAGS_slow_scan_history_count);
  }
}

ScannerManager::~ScannerManager() {
  {
    MutexLock l(shutdown_lock_);
    shutdown_ = true;
    shutdown_cv_.Broadcast();
  }
  if (removal_thread_.get() != nullptr) {
    CHECK_OK(ThreadJoiner(removal_thread_.get()).Join());
  }
  STLDeleteElements(&scanner_maps_);
}

Status ScannerManager::StartCollectAndRemovalThread() {
  RETURN_NOT_OK(Thread::Create("scanners", "collect_and_removal_thread",
                               [this]() { this->RunCollectAndRemovalThread(); },
                               &removal_thread_));
  return Status::OK();
}

void ScannerManager::RunCollectAndRemovalThread() {
  while (true) {
    // Loop until we are shutdown.
    {
      MutexLock l(shutdown_lock_);
      if (shutdown_) {
        return;
      }
      shutdown_cv_.WaitFor(MonoDelta::FromMicroseconds(FLAGS_scanner_gc_check_interval_us));
    }

    if (FLAGS_show_slow_scans) {
      // Control the collection of slow scans to avoid full scanning affecting normal Kudu
      // service without perception.
      CollectSlowScanners();
    }
    RemoveExpiredScanners();
  }
}

ScannerManager::ScannerMapStripe& ScannerManager::GetStripeByScannerId(const string& scanner_id) {
  size_t slot = HashStringThoroughly(scanner_id.data(), scanner_id.size()) % kNumScannerMapStripes;
  return *scanner_maps_[slot];
}

void ScannerManager::NewScanner(const scoped_refptr<TabletReplica>& tablet_replica,
                                const RemoteUser& remote_user,
                                uint64_t row_format_flags,
                                SharedScanner* scanner) {
  // Keep trying to generate a unique ID until we get one.
  bool success = false;
  while (!success) {
    string id = oid_generator_.Next();
    scanner->reset(new Scanner(id,
                               tablet_replica,
                               remote_user,
                               metrics_.get(),
                               row_format_flags));

    ScannerMapStripe& stripe = GetStripeByScannerId(id);
    std::lock_guard<RWMutex> l(stripe.lock_);
    success = InsertIfNotPresent(&stripe.scanners_by_id_, id, *scanner);
  }
}

Status ScannerManager::LookupScanner(const string& scanner_id,
                                     const string& username,
                                     TabletServerErrorPB::Code* error_code,
                                     SharedScanner* scanner) {
  SharedScanner ret;
  ScannerMapStripe& stripe = GetStripeByScannerId(scanner_id);
  shared_lock<RWMutex> l(stripe.lock_);
  bool found_scanner = FindCopy(stripe.scanners_by_id_, scanner_id, &ret);
  if (!found_scanner) {
    *error_code = TabletServerErrorPB::SCANNER_EXPIRED;
    return Status::NotFound(Substitute("Scanner $0 not found (it may have expired)",
                                       scanner_id));
  }
  if (username != ret->remote_user().username()) {
    *error_code = TabletServerErrorPB::NOT_AUTHORIZED;
    return Status::NotAuthorized(Substitute("User $0 doesn't own scanner $1",
                                            username, scanner_id));
  }
  *scanner = std::move(ret);
  return Status::OK();
}

bool ScannerManager::UnregisterScanner(const string& scanner_id) {
  SharedScanDescriptor descriptor;
  ScannerMapStripe& stripe = GetStripeByScannerId(scanner_id);
  {
    std::lock_guard<RWMutex> l(stripe.lock_);
    auto it = stripe.scanners_by_id_.find(scanner_id);
    if (it == stripe.scanners_by_id_.end()) {
      return false;
    }

    bool is_initted = it->second->is_initted();
    if (is_initted) {
      descriptor = it->second->Descriptor();
      descriptor->state = it->second->iter()->HasNext() ? ScanState::kFailed : ScanState::kComplete;
    }
    stripe.scanners_by_id_.erase(it);
    if (!is_initted) {
      return true;
    }
  }

  {
    std::lock_guard<percpu_rwlock> l(completed_scans_lock_);
    RecordCompletedScanUnlocked(descriptor);
  }

  {
    const MonoTime start_time = descriptor->start_time;
    if (start_time + MonoDelta::FromMilliseconds(FLAGS_slow_scanner_threshold_ms)
        < MonoTime::Now()) {
      std::lock_guard<percpu_rwlock> l(slow_scans_lock_);
      RecordSlowScanUnlocked(descriptor);
    }
  }
  return true;
}

size_t ScannerManager::CountActiveScanners() const {
  size_t total = 0;
  for (const ScannerMapStripe* e : scanner_maps_) {
    shared_lock<RWMutex> l(e->lock_);
    total += e->scanners_by_id_.size();
  }
  return total;
}

void ScannerManager::ListScanners(std::vector<SharedScanner>* scanners) const {
  for (const ScannerMapStripe* stripe : scanner_maps_) {
    shared_lock<RWMutex> l(stripe->lock_);
    for (const auto& se : stripe->scanners_by_id_) {
      scanners->push_back(se.second);
    }
  }
}

vector<SharedScanDescriptor> ScannerManager::ListScans() const {
  unordered_map<string, SharedScanDescriptor> scans;
  for (const ScannerMapStripe* stripe : scanner_maps_) {
    shared_lock<RWMutex> l(stripe->lock_);
    for (const auto& se : stripe->scanners_by_id_) {
      if (se.second->is_initted()) {
        SharedScanDescriptor desc = se.second->Descriptor();
        desc->state = ScanState::kActive;
        EmplaceOrDie(&scans, se.first, std::move(desc));
      }
    }
  }

  {
    kudu::shared_lock<rw_spinlock> l(completed_scans_lock_.get_lock());
    // A scanner in 'scans' may have completed between the above loop and here.
    // As we'd rather have the finalized descriptor of the completed scan,
    // update over the old descriptor in this case.
    for (const auto& scan : completed_scans_) {
      InsertOrUpdate(&scans, scan->scanner_id, scan);
    }
  }

  vector<SharedScanDescriptor> ret;
  ret.reserve(scans.size());
  AppendValuesFromMap(scans, &ret);

  // Sort oldest to newest, so that the ordering is consistent across calls.
  std::sort(ret.begin(), ret.end(), [] (const SharedScanDescriptor& a,
                                        const SharedScanDescriptor& b) {
      return a->start_time > b->start_time;
  });

  return ret;
}

vector<SharedScanDescriptor> ScannerManager::ListSlowScans() const {
  vector<SharedScanDescriptor> ret;
  if (!FLAGS_show_slow_scans) {
    LOG(INFO) << "Slow scans show is disabled. Set --show_slow_scans to enable it.";
    return ret;
  }

  // Get all the scans first.
  unordered_map<string, SharedScanDescriptor> scans;
  {
    kudu::shared_lock<rw_spinlock> l(slow_scans_lock_.get_lock());
    for (const auto& scan : slow_scans_) {
      InsertOrUpdate(&scans, scan->scanner_id, scan);
    }
  }

  ret.reserve(scans.size());
  AppendValuesFromMap(scans, &ret);

  // Sort oldest to newest, so that the ordering is consistent across calls.
  std::sort(ret.begin(), ret.end(), [] (const SharedScanDescriptor& a,
                                        const SharedScanDescriptor& b) {
      return a->start_time > b->start_time;
  });

  return ret;
}

void ScannerManager::CollectSlowScanners() {
  const MonoTime now = MonoTime::Now();

  vector<SharedScanDescriptor> descriptors;
  int32_t slow_scanner_threshold = FLAGS_slow_scanner_threshold_ms;
  for (ScannerMapStripe* stripe : scanner_maps_) {
    std::lock_guard<RWMutex> l(stripe->lock_);
    for (auto it = stripe->scanners_by_id_.begin(); it != stripe->scanners_by_id_.end(); ++it) {
      const SharedScanner& scanner = it->second;
      if (!scanner->is_initted()) {
        // Ignore uninitialized scans.
        continue;
      }
      const MonoTime start_time = scanner->start_time();
      if (start_time + MonoDelta::FromMilliseconds(slow_scanner_threshold) >= now) {
        continue;
      }

      MonoDelta delta_time = now - start_time -
          MonoDelta::FromMilliseconds(slow_scanner_threshold);
      LOG(INFO) << Substitute(
          "Slow scanner id: $0, of tablet $1, "
          "exceed the time threshold $2 ms for $3 ms.",
          it->first,
          scanner->tablet_id(),
          slow_scanner_threshold,
          delta_time.ToMilliseconds());
      descriptors.emplace_back(scanner->Descriptor());
    }
  }

  std::lock_guard<percpu_rwlock> l(slow_scans_lock_);
  for (auto& descriptor : descriptors) {
    if (std::find(slow_scans_.begin(), slow_scans_.end(), descriptor) == slow_scans_.end()) {
      RecordSlowScanUnlocked(descriptor);
    }
  }
}

void ScannerManager::RemoveExpiredScanners() {
  MonoDelta scanner_ttl = MonoDelta::FromMilliseconds(FLAGS_scanner_ttl_ms);
  const MonoTime now = MonoTime::Now();

  vector<SharedScanDescriptor> descriptors;
  for (ScannerMapStripe* stripe : scanner_maps_) {
    std::lock_guard<RWMutex> l(stripe->lock_);
    for (auto it = stripe->scanners_by_id_.begin(); it != stripe->scanners_by_id_.end();) {
      const SharedScanner& scanner = it->second;
      MonoDelta idle_time = scanner->TimeSinceLastAccess(now);
      if (idle_time <= scanner_ttl) {
        ++it;
        continue;
      }

      // The scanner has expired because of inactivity.
      LOG(INFO) << Substitute(
          "Expiring scanner id: $0, of tablet $1, "
          "after $2 ms of inactivity, which is > TTL ($3 ms).",
          it->first,
          scanner->tablet_id(),
          idle_time.ToMilliseconds(),
          scanner_ttl.ToMilliseconds());
      if (scanner->is_initted()) {
        descriptors.emplace_back(scanner->Descriptor());
      }
      it = stripe->scanners_by_id_.erase(it);
      if (metrics_) {
        metrics_->scanners_expired->Increment();
      }
    }
  }

  std::lock_guard<percpu_rwlock> l(completed_scans_lock_);
  for (auto& descriptor : descriptors) {
    descriptor->last_access_time = now;
    descriptor->state = ScanState::kExpired;
    RecordCompletedScanUnlocked(descriptor);
  }
}

void ScannerManager::CircularUpdateRecordInFifo(vector<SharedScanDescriptor>& scans_vec,
                                                size_t& scans_offset,
                                                const SharedScanDescriptor& descriptor) {
  if (scans_vec.capacity() == 0) {
    return;
  }

  if (scans_vec.size() == scans_vec.capacity()) {
    scans_vec[scans_offset++] = descriptor;
    if (scans_offset == scans_vec.capacity()) {
      scans_offset = 0;
    }
  } else {
    scans_vec.emplace_back(descriptor);
  }
}

void ScannerManager::RecordCompletedScanUnlocked(const SharedScanDescriptor& descriptor) {
  return CircularUpdateRecordInFifo(completed_scans_, completed_scans_offset_, descriptor);
}

void ScannerManager::RecordSlowScanUnlocked(const SharedScanDescriptor& descriptor) {
  return CircularUpdateRecordInFifo(slow_scans_, slow_scans_offset_, descriptor);
}

const std::string Scanner::kNullTabletId = "null tablet";

Scanner::Scanner(string id, const scoped_refptr<TabletReplica>& tablet_replica,
                 RemoteUser remote_user, ScannerMetrics* metrics,
                 uint64_t row_format_flags)
    : id_(std::move(id)),
      tablet_replica_(tablet_replica),
      remote_user_(std::move(remote_user)),
      start_time_(MonoTime::Now()),
      row_format_flags_(row_format_flags),
      metrics_(metrics),
      arena_(256),
      last_access_time_(start_time_),
      call_seq_id_(0),
      num_rows_returned_(0) {
  if (tablet_replica_) {
    auto tablet = tablet_replica->shared_tablet();
    if (tablet && tablet->metrics()) {
      tablet->metrics()->tablet_active_scanners->Increment();
    }
  }
}

Scanner::~Scanner() {
  if (tablet_replica_) {
    auto tablet = tablet_replica_->shared_tablet();
    if (tablet && tablet->metrics()) {
      tablet->metrics()->tablet_active_scanners->IncrementBy(-1);
    }
  }
  if (metrics_) {
    metrics_->SubmitScannerDuration(start_time_);
  }
}

void Scanner::AddTimings(const CpuTimes& elapsed) {
  std::unique_lock<RWMutex> l(cpu_times_lock_);
  cpu_times_.Add(elapsed);
}

void Scanner::Init(unique_ptr<RowwiseIterator> iter,
                   unique_ptr<ScanSpec> spec,
                   unique_ptr<Schema> client_projection) {
  lock_.AssertAcquired();
  CHECK(!iter_) << "Already initialized";
  iter_ = std::move(iter);
  spec_ = std::move(spec);
  client_projection_schema_ = std::move(client_projection);
  initted_.store(true, std::memory_order_release);
}

const ScanSpec& Scanner::spec() const {
  return *spec_;
}

IteratorStats Scanner::UpdateStatsAndGetDelta() {
  // Here we have to dig into the per-column iterator stats, sum them up, and then
  // subtract out the total that we already reported in a previous scan.
  lock_.AssertAcquired();
  vector<IteratorStats> stats_by_col;
  iter_->GetIteratorStats(&stats_by_col);
  IteratorStats total_stats = std::accumulate(stats_by_col.begin(),
                                              stats_by_col.end(),
                                              IteratorStats());
  IteratorStats delta_stats = total_stats - already_reported_stats_;
  already_reported_stats_ = total_stats;
  return delta_stats;
}

SharedScanDescriptor Scanner::Descriptor() const {
  // Ignore non-initialized scans. The initializing state is transient, and
  // handling it correctly is complicated. Since the scanner is initialized we
  // can assume iter_, spec_, and client_projection_schema_ are valid
  // pointers.
  CHECK(is_initted());

  SharedScanDescriptor descriptor(new ScanDescriptor);
  descriptor->tablet_id = tablet_id();
  descriptor->scanner_id = id();
  descriptor->remote_user = remote_user();
  descriptor->start_time = start_time_;

  for (const auto& column : client_projection_schema_->columns()) {
    descriptor->projected_columns.emplace_back(column.name());
  }

  const auto& tablet_metadata = tablet_replica_->tablet_metadata();
  descriptor->table_name = tablet_metadata->table_name();
  SchemaPtr schema_ptr = tablet_metadata->schema();
  if (spec().lower_bound_key()) {
    descriptor->predicates.emplace_back(
        Substitute("PRIMARY KEY >= $0", KUDU_REDACT(
            spec().lower_bound_key()->Stringify(*schema_ptr))));
  }
  if (spec().exclusive_upper_bound_key()) {
    descriptor->predicates.emplace_back(
        Substitute("PRIMARY KEY < $0", KUDU_REDACT(
            spec().exclusive_upper_bound_key()->Stringify(*schema_ptr))));
  }

  for (const auto& predicate : spec().predicates()) {
    descriptor->predicates.emplace_back(predicate.second.ToString());
  }

  vector<IteratorStats> iterator_stats;
  iter_->GetIteratorStats(&iterator_stats);

  DCHECK_EQ(iterator_stats.size(), iter_->schema().num_columns());
  for (int col_idx = 0; col_idx < iterator_stats.size(); col_idx++) {
    descriptor->iterator_stats.emplace_back(iter_->schema().column(col_idx).name(),
                                            iterator_stats[col_idx]);
  }

  descriptor->last_call_seq_id = ANNOTATE_UNPROTECTED_READ(call_seq_id_);
  descriptor->last_access_time = last_access_time_.load(std::memory_order_relaxed);
  descriptor->cpu_times = cpu_times();

  return descriptor;
}

CpuTimes Scanner::cpu_times() const {
  shared_lock<RWMutex> l(cpu_times_lock_);
  return cpu_times_;
}

} // namespace tserver
} // namespace kudu
