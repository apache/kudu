// Copyright(c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.
#include "kudu/tserver/scanners.h"

#include <boost/bind.hpp>
#include <boost/thread/locks.hpp>
#include <gflags/gflags.h>
#include <tr1/memory>

#include "kudu/common/iterator.h"
#include "kudu/common/scan_spec.h"
#include "kudu/gutil/map-util.h"
#include "kudu/tserver/scanner_metrics.h"
#include "kudu/util/thread.h"
#include "kudu/util/metrics.h"

DEFINE_int32(tablet_server_scanner_ttl_millis, 60000,
             "Number of milliseconds of inactivity allowed for a scanner"
             "before it may be expired");

METRIC_DEFINE_gauge_uint64(active_scanners, kudu::MetricUnit::kScanners,
                           "Number of scanners that are currently active");

namespace kudu {
namespace tserver {

// The interval at which we remove expired scanners.
static const uint64_t kRemovalThreadIntervalUs = 5000000;

ScannerManager::ScannerManager(MetricContext* parent_metric_context)
  : scanner_ttl_(MonoDelta::FromMilliseconds(
                   FLAGS_tablet_server_scanner_ttl_millis)),
    shutdown_(false) {
  if (parent_metric_context) {
    metric_context_.reset(new MetricContext(*parent_metric_context,
                                            "tserver.scanners"));
    metrics_.reset(new ScannerMetrics(*metric_context_));
    METRIC_active_scanners.InstantiateFunctionGauge(
        *metric_context_, boost::bind(&ScannerManager::CountActiveScanners, this));
  }
}

ScannerManager::~ScannerManager() {
  {
    boost::lock_guard<boost::mutex> l(shutdown_lock_);
    shutdown_ = true;
    shutdown_cv_.notify_all();
  }
  if (removal_thread_.get() != NULL) {
    CHECK_OK(ThreadJoiner(removal_thread_.get()).Join());
  }
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
      boost::unique_lock<boost::mutex> l(shutdown_lock_);
      if (shutdown_) {
        return;
      }
      boost::system_time wtime = boost::get_system_time() +
          boost::posix_time::microseconds(kRemovalThreadIntervalUs);
      shutdown_cv_.timed_wait(l, wtime);
    }
    RemoveExpiredScanners();
  }
}

void ScannerManager::NewScanner(const std::string& tablet_id,
                                const std::string& requestor_string,
                                SharedScanner* scanner) {
  // Keep trying to generate a unique ID until we get one.
  bool success = false;
  while (!success) {
    // TODO(security): are these UUIDs predictable? If so, we should
    // probably generate random numbers instead, since we can safely
    // just retry until we avoid a collission.
    string id = oid_generator_.Next();
    scanner->reset(new Scanner(id, tablet_id, requestor_string, metrics_.get()));

    boost::lock_guard<boost::shared_mutex> l(lock_);
    success = InsertIfNotPresent(&scanners_by_id_, id, *scanner);
  }
}

bool ScannerManager::LookupScanner(const string& scanner_id, SharedScanner* scanner) {
  boost::shared_lock<boost::shared_mutex> l(lock_);
  return FindCopy(scanners_by_id_, scanner_id, scanner);
}

bool ScannerManager::UnregisterScanner(const string& scanner_id) {
  boost::lock_guard<boost::shared_mutex> l(lock_);
  return scanners_by_id_.erase(scanner_id) > 0;
}

size_t ScannerManager::CountActiveScanners() const {
  boost::shared_lock<boost::shared_mutex> l(lock_);
  return scanners_by_id_.size();
}

void ScannerManager::ListScanners(std::vector<SharedScanner>* scanners) {
  boost::shared_lock<boost::shared_mutex> l(lock_);
  BOOST_FOREACH(const ScannerMapEntry& e, scanners_by_id_) {
    scanners->push_back(e.second);
  }
}

void ScannerManager::RemoveExpiredScanners() {
  boost::lock_guard<boost::shared_mutex> l(lock_);
  for (ScannerMap::iterator it = scanners_by_id_.begin();
       it != scanners_by_id_.end(); ) {
    SharedScanner& scanner = it->second;
    MonoDelta time_live =
        scanner->TimeSinceLastAccess(MonoTime::Now(MonoTime::COARSE));
    if (time_live.MoreThan(scanner_ttl_)) {
      // TODO: once we have a metric for the number of scanners expired, make this a
      // VLOG(1).
      LOG(INFO) << "Expiring scanner id: " << it->first << ", after "
                << time_live.ToMicroseconds() << " us of inactivity, which is > TTL ("
                << scanner_ttl_.ToMicroseconds() << " us).";
      it = scanners_by_id_.erase(it);
    } else {
      ++it;
    }
  }
}

Scanner::Scanner(const string& id,
                 const string& tablet_id,
                 const string& requestor_string,
                 ScannerMetrics* metrics)
    : id_(id),
      tablet_id_(tablet_id),
      requestor_string_(requestor_string),
      start_time_(MonoTime::Now(MonoTime::COARSE)),
      metrics_(metrics),
      arena_(1024, 1024 * 1024) {
  UpdateAccessTime();
}

Scanner::~Scanner() {
  if (metrics_) {
    metrics_->SubmitScannerDuration(start_time_);
  }
}

void Scanner::UpdateAccessTime() {
  boost::lock_guard<simple_spinlock> l(lock_);
  last_access_time_ = MonoTime::Now(MonoTime::COARSE);
}

void Scanner::Init(gscoped_ptr<RowwiseIterator> iter,
                   gscoped_ptr<ScanSpec> spec) {
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
