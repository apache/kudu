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
#include <memory>
#include <mutex>
#include <ostream>

#include <gflags/gflags.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/iterator.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/hash/string_hash.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tserver/scanner_metrics.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

DEFINE_int32(scanner_ttl_ms, 60000,
             "Number of milliseconds of inactivity allowed for a scanner"
             "before it may be expired");
TAG_FLAG(scanner_ttl_ms, advanced);
DEFINE_int32(scanner_gc_check_interval_us, 5 * 1000L *1000L, // 5 seconds
             "Number of microseconds in the interval at which we remove expired scanners");
TAG_FLAG(scanner_gc_check_interval_us, hidden);

DEFINE_int32(scan_history_count, 20,
             "Number of completed scans to keep history for. Determines how many historical "
             "scans will be shown on the tablet server's scans dashboard.");
TAG_FLAG(scan_history_count, experimental);

METRIC_DEFINE_gauge_size(server, active_scanners,
                         "Active Scanners",
                         kudu::MetricUnit::kScanners,
                         "Number of scanners that are currently active");

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

using rpc::RemoteUser;
using tablet::TabletReplica;

namespace tserver {

ScannerManager::ScannerManager(const scoped_refptr<MetricEntity>& metric_entity)
    : shutdown_(false),
      shutdown_cv_(&shutdown_lock_),
      completed_scans_offset_(0) {
  if (metric_entity) {
    metrics_.reset(new ScannerMetrics(metric_entity));
    METRIC_active_scanners.InstantiateFunctionGauge(
        metric_entity, Bind(&ScannerManager::CountActiveScanners,
                               Unretained(this)))
      ->AutoDetach(&metric_detacher_);
  }
  for (size_t i = 0; i < kNumScannerMapStripes; i++) {
    scanner_maps_.push_back(new ScannerMapStripe());
  }

  if (FLAGS_scan_history_count > 0) {
    completed_scans_.reserve(FLAGS_scan_history_count);
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

Status ScannerManager::StartRemovalThread() {
  RETURN_NOT_OK(Thread::Create("scanners", "removal_thread",
                               &ScannerManager::RunRemovalThread, this,
                               &removal_thread_));
  return Status::OK();
}

void ScannerManager::RunRemovalThread() {
  while (true) {
    // Loop until we are shutdown.
    {
      MutexLock l(shutdown_lock_);
      if (shutdown_) {
        return;
      }
      shutdown_cv_.WaitFor(MonoDelta::FromMicroseconds(FLAGS_scanner_gc_check_interval_us));
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
  ScanDescriptor descriptor;
  ScannerMapStripe& stripe = GetStripeByScannerId(scanner_id);
  {
    std::lock_guard<RWMutex> l(stripe.lock_);
    auto it = stripe.scanners_by_id_.find(scanner_id);
    if (it == stripe.scanners_by_id_.end()) {
      return false;
    }

    bool is_initialized = it->second->IsInitialized();
    if (is_initialized) {
      descriptor = it->second->descriptor();
      descriptor.state = it->second->iter()->HasNext() ? ScanState::kFailed : ScanState::kComplete;
    }
    stripe.scanners_by_id_.erase(it);
    if (!is_initialized) {
      return true;
    }
  }

  std::lock_guard<RWMutex> l(completed_scans_lock_);
  RecordCompletedScanUnlocked(std::move(descriptor));
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

vector<ScanDescriptor> ScannerManager::ListScans() const {
  vector<ScanDescriptor> scans;
  for (const ScannerMapStripe* stripe : scanner_maps_) {
    shared_lock<RWMutex> l(stripe->lock_);
    for (const auto& se : stripe->scanners_by_id_) {
      if (se.second->IsInitialized()) {
        scans.emplace_back(se.second->descriptor());
        scans.back().state = ScanState::kActive;
      }
    }
  }

  {
    shared_lock<RWMutex> l(completed_scans_lock_);
    scans.insert(scans.end(), completed_scans_.begin(), completed_scans_.end());
  }

  // TODO(dan): It's possible for a descriptor to be included twice in the
  // result set if its scanner is concurrently removed from the scanner map.

  // Sort oldest to newest, so that the ordering is consistent across calls.
  std::sort(scans.begin(), scans.end(), [] (const ScanDescriptor& a, const ScanDescriptor& b) {
      return a.start_time > b.start_time;
  });

  return scans;
}

void ScannerManager::RemoveExpiredScanners() {
  MonoDelta scanner_ttl = MonoDelta::FromMilliseconds(FLAGS_scanner_ttl_ms);
  const MonoTime now = MonoTime::Now();

  vector<ScanDescriptor> descriptors;
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
      if (scanner->IsInitialized()) {
        descriptors.emplace_back(scanner->descriptor());
      }
      it = stripe->scanners_by_id_.erase(it);
      if (metrics_) {
        metrics_->scanners_expired->Increment();
      }
    }
  }

  std::lock_guard<RWMutex> l(completed_scans_lock_);
  for (auto& descriptor : descriptors) {
    descriptor.last_access_time = now;
    descriptor.state = ScanState::kExpired;
    RecordCompletedScanUnlocked(std::move(descriptor));
  }
}

void ScannerManager::RecordCompletedScanUnlocked(ScanDescriptor descriptor) {
  if (completed_scans_.capacity() == 0) {
    return;
  }
  if (completed_scans_.size() == completed_scans_.capacity()) {
    completed_scans_[completed_scans_offset_++] = std::move(descriptor);
    if (completed_scans_offset_ == completed_scans_.capacity()) {
      completed_scans_offset_ = 0;
    }
  } else {
    completed_scans_.emplace_back(std::move(descriptor));
  }
}

const std::string Scanner::kNullTabletId = "null tablet";

Scanner::Scanner(string id, const scoped_refptr<TabletReplica>& tablet_replica,
                 RemoteUser remote_user, ScannerMetrics* metrics,
                 uint64_t row_format_flags)
    : id_(std::move(id)),
      tablet_replica_(tablet_replica),
      remote_user_(std::move(remote_user)),
      call_seq_id_(0),
      start_time_(MonoTime::Now()),
      metrics_(metrics),
      arena_(256),
      row_format_flags_(row_format_flags),
      num_rows_returned_(0) {
  if (tablet_replica_) {
    auto tablet = tablet_replica->shared_tablet();
    if (tablet && tablet->metrics()) {
      tablet->metrics()->tablet_active_scanners->Increment();
    }
  }
  UpdateAccessTime();
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

void Scanner::UpdateAccessTime() {
  std::lock_guard<simple_spinlock> l(lock_);
  last_access_time_ = MonoTime::Now();
}

void Scanner::Init(unique_ptr<RowwiseIterator> iter,
                   gscoped_ptr<ScanSpec> spec) {
  std::lock_guard<simple_spinlock> l(lock_);
  CHECK(!iter_) << "Already initialized";
  iter_ = std::move(iter);
  spec_.reset(spec.release());
}

const ScanSpec& Scanner::spec() const {
  return *spec_;
}

void Scanner::GetIteratorStats(vector<IteratorStats>* stats) const {
  iter_->GetIteratorStats(stats);
}

ScanDescriptor Scanner::descriptor() const {
  // Ignore non-initialized scans. The initializing state is transient, and
  // handling it correctly is complicated. Since the scanner is initialized we
  // can assume iter(), spec(), and client_projection_schema() return valid
  // pointers.
  CHECK(IsInitialized());

  ScanDescriptor descriptor;
  descriptor.tablet_id = tablet_id();
  descriptor.scanner_id = id();
  descriptor.remote_user = remote_user();
  descriptor.start_time = start_time_;

  for (const auto& column : client_projection_schema()->columns()) {
    descriptor.projected_columns.emplace_back(column.name());
  }

  const auto& tablet_metadata = tablet_replica_->tablet_metadata();
  descriptor.table_name = tablet_metadata->table_name();
  if (spec().lower_bound_key()) {
    descriptor.predicates.emplace_back(
        Substitute("PRIMARY KEY >= $0", KUDU_REDACT(
            spec().lower_bound_key()->Stringify(tablet_metadata->schema()))));
  }
  if (spec().exclusive_upper_bound_key()) {
    descriptor.predicates.emplace_back(
        Substitute("PRIMARY KEY < $0", KUDU_REDACT(
            spec().exclusive_upper_bound_key()->Stringify(tablet_metadata->schema()))));
  }

  for (const auto& predicate : spec().predicates()) {
    descriptor.predicates.emplace_back(predicate.second.ToString());
  }

  vector<IteratorStats> iterator_stats;
  GetIteratorStats(&iterator_stats);

  DCHECK_EQ(iterator_stats.size(), iter()->schema().num_columns());
  for (int col_idx = 0; col_idx < iterator_stats.size(); col_idx++) {
    descriptor.iterator_stats.emplace_back(iter()->schema().column(col_idx).name(),
                                           iterator_stats[col_idx]);
  }

  {
    std::lock_guard<simple_spinlock> l(lock_);
    descriptor.last_call_seq_id = call_seq_id_;
    descriptor.last_access_time = last_access_time_;
  }

  return descriptor;
}

} // namespace tserver
} // namespace kudu
