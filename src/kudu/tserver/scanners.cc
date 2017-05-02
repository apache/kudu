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

#include <mutex>

#include <gflags/gflags.h>

#include "kudu/common/iterator.h"
#include "kudu/common/scan_spec.h"
#include "kudu/gutil/hash/string_hash.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tserver/scanner_metrics.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/metrics.h"
#include "kudu/util/thread.h"

DEFINE_int32(scanner_ttl_ms, 60000,
             "Number of milliseconds of inactivity allowed for a scanner"
             "before it may be expired");
TAG_FLAG(scanner_ttl_ms, advanced);
DEFINE_int32(scanner_gc_check_interval_us, 5 * 1000L *1000L, // 5 seconds
             "Number of microseconds in the interval at which we remove expired scanners");
TAG_FLAG(scanner_gc_check_interval_us, hidden);

// TODO: would be better to scope this at a tablet level instead of
// server level.
METRIC_DEFINE_gauge_size(server, active_scanners,
                         "Active Scanners",
                         kudu::MetricUnit::kScanners,
                         "Number of scanners that are currently active");

using strings::Substitute;

namespace kudu {

using tablet::TabletPeer;

namespace tserver {

ScannerManager::ScannerManager(const scoped_refptr<MetricEntity>& metric_entity)
    : shutdown_(false),
      shutdown_cv_(&shutdown_lock_) {
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
      shutdown_cv_.TimedWait(MonoDelta::FromMicroseconds(FLAGS_scanner_gc_check_interval_us));
    }
    RemoveExpiredScanners();
  }
}

ScannerManager::ScannerMapStripe& ScannerManager::GetStripeByScannerId(const string& scanner_id) {
  size_t slot = HashStringThoroughly(scanner_id.data(), scanner_id.size()) % kNumScannerMapStripes;
  return *scanner_maps_[slot];
}

void ScannerManager::NewScanner(const scoped_refptr<TabletPeer>& tablet_peer,
                                const std::string& requestor_string,
                                uint64 row_format_flags,
                                SharedScanner* scanner) {
  // Keep trying to generate a unique ID until we get one.
  bool success = false;
  while (!success) {
    // TODO(KUDU-1918): are these UUIDs predictable? If so, we should
    // probably generate random numbers instead, since we can safely
    // just retry until we avoid a collision. Alternatively we could
    // verify that the requestor userid does not change mid-scan.
    string id = oid_generator_.Next();
    scanner->reset(new Scanner(id,
                               tablet_peer,
                               requestor_string,
                               metrics_.get(),
                               row_format_flags));

    ScannerMapStripe& stripe = GetStripeByScannerId(id);
    std::lock_guard<RWMutex> l(stripe.lock_);
    success = InsertIfNotPresent(&stripe.scanners_by_id_, id, *scanner);
  }
}

bool ScannerManager::LookupScanner(const string& scanner_id, SharedScanner* scanner) {
  ScannerMapStripe& stripe = GetStripeByScannerId(scanner_id);
  shared_lock<RWMutex> l(stripe.lock_);
  return FindCopy(stripe.scanners_by_id_, scanner_id, scanner);
}

bool ScannerManager::UnregisterScanner(const string& scanner_id) {
  ScannerMapStripe& stripe = GetStripeByScannerId(scanner_id);
  std::lock_guard<RWMutex> l(stripe.lock_);
  return stripe.scanners_by_id_.erase(scanner_id) > 0;
}

size_t ScannerManager::CountActiveScanners() const {
  size_t total = 0;
  for (const ScannerMapStripe* e : scanner_maps_) {
    shared_lock<RWMutex> l(e->lock_);
    total += e->scanners_by_id_.size();
  }
  return total;
}

void ScannerManager::ListScanners(std::vector<SharedScanner>* scanners) {
  for (const ScannerMapStripe* stripe : scanner_maps_) {
    shared_lock<RWMutex> l(stripe->lock_);
    for (const ScannerMapEntry& se : stripe->scanners_by_id_) {
      scanners->push_back(se.second);
    }
  }
}

void ScannerManager::RemoveExpiredScanners() {
  MonoDelta scanner_ttl = MonoDelta::FromMilliseconds(FLAGS_scanner_ttl_ms);
  const MonoTime now = MonoTime::Now();

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
      it = stripe->scanners_by_id_.erase(it);
      if (metrics_) {
        metrics_->scanners_expired->Increment();
      }
    }
  }
}

const std::string Scanner::kNullTabletId = "null tablet";

Scanner::Scanner(string id, const scoped_refptr<TabletPeer>& tablet_peer,
                 string requestor_string, ScannerMetrics* metrics,
                 uint64_t row_format_flags)
    : id_(std::move(id)),
      tablet_peer_(tablet_peer),
      requestor_string_(std::move(requestor_string)),
      call_seq_id_(0),
      start_time_(MonoTime::Now()),
      metrics_(metrics),
      arena_(1024, 1024 * 1024),
      row_format_flags_(row_format_flags) {
  UpdateAccessTime();
}

Scanner::~Scanner() {
  if (metrics_) {
    metrics_->SubmitScannerDuration(start_time_);
  }
}

void Scanner::UpdateAccessTime() {
  std::lock_guard<simple_spinlock> l(lock_);
  last_access_time_ = MonoTime::Now();
}

void Scanner::Init(gscoped_ptr<RowwiseIterator> iter,
                   gscoped_ptr<ScanSpec> spec) {
  std::lock_guard<simple_spinlock> l(lock_);
  CHECK(!iter_) << "Already initialized";
  iter_.reset(iter.release());
  spec_.reset(spec.release());
}

const ScanSpec& Scanner::spec() const {
  return *spec_;
}

void Scanner::GetIteratorStats(vector<IteratorStats>* stats) const {
  iter_->GetIteratorStats(stats);
}


} // namespace tserver
} // namespace kudu
