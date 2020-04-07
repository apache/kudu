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
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/common/iterator_stats.h"
#include "kudu/common/scan_spec.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/auto_release_pool.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/stopwatch.h"

namespace kudu {

class RowwiseIterator;
class Schema;
class Status;
class Thread;

namespace tserver {

class Scanner;

enum class ScanState;
struct ScanDescriptor;
struct ScannerMetrics;

typedef std::shared_ptr<Scanner> SharedScanner;

// Manages the live scanners within a Tablet Server.
//
// When a scanner is created by a client, it is assigned a unique scanner ID.
// The client may then use this ID to fetch more rows from the scanner
// or close it.
//
// Since scanners keep resources on the server, the manager periodically
// removes any scanners which have not been accessed since a configurable TTL.
class ScannerManager {
 public:
  explicit ScannerManager(const scoped_refptr<MetricEntity>& metric_entity);
  ~ScannerManager();

  // Starts the expired scanner removal thread.
  Status StartRemovalThread();

  // Create a new scanner with a unique ID, inserting it into the map. Further
  // lookups for the scanner must provide the username associated with
  // 'remote_user'.
  void NewScanner(const scoped_refptr<tablet::TabletReplica>& tablet_replica,
                  const rpc::RemoteUser& remote_user,
                  uint64_t row_format_flags,
                  SharedScanner* scanner);

  // Lookup the given scanner by its ID with the provided username, setting an
  // appropriate error code.
  // Returns NotFound if the scanner doesn't exist, or NotAuthorized if the
  // scanner wasn't created by 'username'.
  Status LookupScanner(const std::string& scanner_id,
                       const std::string& username,
                       TabletServerErrorPB::Code* error_code,
                       SharedScanner* scanner);

  // Unregister the given scanner by its ID.
  // Returns true if unregistered successfully.
  bool UnregisterScanner(const std::string& scanner_id);

  // Return the number of scanners currently active.
  // Note this method will not return accurate value
  // if under concurrent modifications.
  size_t CountActiveScanners() const;

  // List all active scanners.
  // Note this method will not return a consistent view
  // of all active scanners if under concurrent modifications.
  void ListScanners(std::vector<SharedScanner>* scanners) const;

  // List active and recently completed scans.
  std::vector<ScanDescriptor> ListScans() const;

  // Iterate through scanners and remove any which are past their TTL.
  void RemoveExpiredScanners();

 private:
  FRIEND_TEST(ScannerTest, TestExpire);

  enum {
    kNumScannerMapStripes = 32
  };

  typedef std::unordered_map<std::string, SharedScanner> ScannerMap;

  struct ScannerMapStripe {
    // Lock protecting the scanner map.
    mutable RWMutex lock_;
    // Map of the currently active scanners.
    ScannerMap scanners_by_id_;
  };

  // Periodically call RemoveExpiredScanners().
  void RunRemovalThread();

  ScannerMapStripe& GetStripeByScannerId(const std::string& scanner_id);

  // Adds the scan descriptor to the completed scans FIFO.
  void RecordCompletedScanUnlocked(ScanDescriptor descriptor);

  // (Optional) scanner metrics for this instance.
  std::unique_ptr<ScannerMetrics> metrics_;

  // If true, removal thread should shut itself down. Protected
  // by 'shutdown_lock_' and 'shutdown_cv_'.
  bool shutdown_;
  mutable Mutex shutdown_lock_;
  ConditionVariable shutdown_cv_;

  std::vector<ScannerMapStripe*> scanner_maps_;

  // completed_scans_ is a FIFO ring buffer of completed scans.
  mutable RWMutex completed_scans_lock_;
  std::vector<ScanDescriptor> completed_scans_;
  size_t completed_scans_offset_;

  // Generator for scanner IDs.
  ObjectIdGenerator oid_generator_;

  // Thread to remove expired scanners.
  scoped_refptr<kudu::Thread> removal_thread_;

  FunctionGaugeDetacher metric_detacher_;

  DISALLOW_COPY_AND_ASSIGN(ScannerManager);
};

// RAII wrapper to unregister a scanner upon scope exit.
class ScopedUnregisterScanner {
 public:
  ScopedUnregisterScanner(ScannerManager* mgr, std::string id)
      : mgr_(mgr), id_(std::move(id)), cancelled_(false) {}

  ~ScopedUnregisterScanner() {
    if (!cancelled_) {
      mgr_->UnregisterScanner(id_);
    }
  }

  // Do not unregister the scanner when the scope is exited.
  void Cancel() {
    cancelled_ = true;
  }

 private:
  ScannerManager* const mgr_;
  const std::string id_;
  bool cancelled_;
};

// An open scanner on the server side.
//
// NOTE: unless otherwise specified, all methods of this class require that the
// caller has acquired the access lock using Scanner::LockForAccess(). It's assumed
// that any RPC related to a scanner will acquire that lock, so that only a single
// RPC thread works on a given scanner at a time.
class Scanner {
 public:
  class AccessLock {
   public:
    AccessLock(AccessLock&& l) noexcept
        : s_(l.s_),
          lock_(std::move(l.lock_)) {
    }
    ~AccessLock() {
      if (lock_.owns_lock()) {
        Unlock();
      }
    }
    void Unlock() {
      s_->last_access_time_.store(MonoTime::Now(), std::memory_order_relaxed);
      lock_.unlock();
    }
    bool owns_lock() {
      return lock_.owns_lock();
    }

   private:
    friend class Scanner;
    explicit AccessLock(Scanner* s)
        : s_(DCHECK_NOTNULL(s)),
          lock_(s->lock_) {
    }
    AccessLock(Scanner* s, std::try_to_lock_t try_lock)
        : s_(DCHECK_NOTNULL(s)),
          lock_(s->lock_, try_lock) {
    }

    Scanner* const s_;
    std::unique_lock<Mutex> lock_;
  };

  Scanner(std::string id,
          const scoped_refptr<tablet::TabletReplica>& tablet_replica,
          rpc::RemoteUser remote_user, ScannerMetrics* metrics,
          uint64_t row_format_flags);
  ~Scanner();

  // Lock this scanner for the purposes of an RPC.
  //
  // While the lock is held, the TimeSinceLastAccess() method will return 0, indicating
  // that a call is actively being processed. Upon destruction of the returned Lock
  // object, the last-access time will be set to the current time and the internal lock
  // released.
  AccessLock LockForAccess() WARN_UNUSED_RESULT {
    return AccessLock(this);
  }

  // Try to lock the scanner, but do not wait in the case that the scanner
  // is already locked by another thread.
  //
  // Check result.owns_lock() to see if the lock was successful.
  AccessLock TryLockForAccess() WARN_UNUSED_RESULT {
    return AccessLock(this, std::try_to_lock);
  }

  // Mark the scanner as initialized. This indicates that it successfully
  // created an iterator, passed validation, etc, and will allow it to
  // show up in the scanner dashboard.
  void Init(std::unique_ptr<RowwiseIterator> iter,
            std::unique_ptr<ScanSpec> spec,
            std::unique_ptr<Schema> client_projection);

  RowwiseIterator* iter() {
    lock_.AssertAcquired();
    return DCHECK_NOTNULL(iter_.get());
  }

  const RowwiseIterator* iter() const {
    lock_.AssertAcquired();
    return DCHECK_NOTNULL(iter_.get());
  }

  // Add the timings in 'elapsed' to the total timings for this scanner.
  void AddTimings(const CpuTimes& elapsed);

  // Return the auto-release pool which will be freed when this scanner
  // closes. This can be used as a storage area for the ScanSpec and any
  // associated data (eg storage for its predicates).
  AutoReleasePool* autorelease_pool() {
    return &autorelease_pool_;
  }

  Arena* arena() {
    lock_.AssertAcquired();
    return &arena_;
  }

  const std::string& id() const { return id_; }

  // Return the ScanSpec associated with this Scanner.
  const ScanSpec& spec() const;

  const std::string& tablet_id() const {
    // scanners-test passes a null tablet_replica.
    return tablet_replica_ ? tablet_replica_->tablet_id() : kNullTabletId;
  }

  const scoped_refptr<tablet::TabletReplica>& tablet_replica() const { return tablet_replica_; }

  const rpc::RemoteUser& remote_user() const { return remote_user_; }

  // Returns the current call sequence ID of the scanner.
  uint32_t call_seq_id() const {
    lock_.AssertAcquired();
    return call_seq_id_;
  }

  // Increments the call sequence ID.
  void IncrementCallSeqId() {
    lock_.AssertAcquired();
    call_seq_id_++;
  }

  // Return the delta from the last time this scan was updated to 'now'.
  MonoDelta TimeSinceLastAccess(const MonoTime& now) const {
    std::unique_lock<Mutex> l(lock_, std::try_to_lock);
    if (l.owns_lock()) {
      return now - last_access_time_;
    }
    return MonoDelta::FromMilliseconds(0);
  }

  // Returns the time this scan was started.
  const MonoTime& start_time() const { return start_time_; }


  // Returns client's projection schema.
  //
  // This may differ from the schema used by the iterator, which must contain all columns
  // used as predicates).
  const Schema* client_projection_schema() const {
    lock_.AssertAcquired();
    return DCHECK_NOTNULL(client_projection_schema_.get());
  }

  // Update the stats from the underlying scanner and return a delta since the
  // previous call to this method.
  IteratorStats UpdateStatsAndGetDelta();

  uint64_t row_format_flags() const {
    lock_.AssertAcquired();
    return row_format_flags_;
  }

  void add_num_rows_returned(int64_t num_rows_added) {
    lock_.AssertAcquired();
    num_rows_returned_ += num_rows_added;
    DCHECK_LE(num_rows_added, num_rows_returned_);
  }

  int64_t num_rows_returned() const {
    lock_.AssertAcquired();
    return num_rows_returned_;
  }

  bool has_fulfilled_limit() const {
    lock_.AssertAcquired();
    return spec_ && spec_->has_limit() && num_rows_returned_ >= spec_->limit();
  }

  // Return a descriptor of the current state of this scan.
  // Does not require the AccessLock.
  //
  // REQUIRES: is_initted() must be true.
  ScanDescriptor Descriptor() const;

  // Returns the amount of CPU time accounted to this scanner.
  // Does not require the AccessLock.
  CpuTimes cpu_times() const;

 private:
  friend class ScannerManager;

  static const std::string kNullTabletId;

  // Return true if the scanner has been initialized (i.e has an iterator).
  // Once a Scanner is initialized, it is safe to assume that iter() and spec()
  // return non-NULL for the lifetime of the Scanner object.
  bool is_initted() const {
    return initted_.load(std::memory_order_acquire);
  }

  // The unique ID of this scanner.
  const std::string id_;

  // Tablet associated with the scanner.
  const scoped_refptr<tablet::TabletReplica> tablet_replica_;

  // The remote user making the request. Populated from the RemoteUser of the
  // first request.
  const rpc::RemoteUser remote_user_;

  // The time the scanner was started.
  const MonoTime start_time_;

  // The row format flags the client passed, if any.
  const uint64_t row_format_flags_;

  // (Optional) scanner metrics struct, for recording scanner's duration.
  ScannerMetrics* metrics_;

  AutoReleasePool autorelease_pool_;

  // Arena used for allocations which must last as long as the scanner
  // itself. This is _not_ used for row data, which is scoped to a single RPC
  // response.
  Arena arena_;

  // Protects access to this scanner by a single RPC at a time.
  mutable Mutex lock_;

  std::atomic<bool> initted_ { false };

  // The spec used by 'iter_'
  // Assumed to be set once initted_ is true.
  std::unique_ptr<ScanSpec> spec_;

  // Assumed to be set once initted_ is true.
  std::unique_ptr<RowwiseIterator> iter_;

  // Stores the request's projection schema, if it differs from the
  // schema used by the iterator.
  // Assumed to be set once initted_ is true.
  std::unique_ptr<Schema> client_projection_schema_;

  // The last time that the scanner was accessed.
  // Only modified under lock_ but can be read outside.
  std::atomic<MonoTime> last_access_time_;

  // The current call sequence ID.
  // Only modified under lock_ but can be read outside.
  uint32_t call_seq_id_;

  // A summary of the statistics already reported to the metrics system
  // for this scanner. This allows us to report the metrics incrementally
  // as the scanner proceeds.
  // Protected by lock_.
  IteratorStats already_reported_stats_;

  // The number of rows that have been serialized and sent over the wire by
  // this scanner.
  int64_t num_rows_returned_;

  // The cumulative amounts of wall, user cpu, and system cpu time spent on
  // this scanner, in seconds.
  mutable RWMutex cpu_times_lock_;
  CpuTimes cpu_times_;

  DISALLOW_COPY_AND_ASSIGN(Scanner);
};

enum class ScanState {
  // The scan is actively running.
  kActive,
  // The scan is complete.
  kComplete,
  // The scan failed.
  kFailed,
  // The scan timed out due to inactivity.
  kExpired,
};

// ScanDescriptor holds information about a scan. The ScanDescriptor can outlive
// the associated scanner without holding open any of the scanner's resources.
struct ScanDescriptor {
  // The tablet ID.
  std::string tablet_id;
  // The scanner ID.
  std::string scanner_id;

  // The user that made the first request.
  rpc::RemoteUser remote_user;

  // The table name.
  std::string table_name;
  // The selected columns.
  std::vector<std::string> projected_columns;
  // The scan predicates. Holds both the primary key and column predicates.
  std::vector<std::string> predicates;

  // The per-column scan stats, paired with the column name.
  std::vector<std::pair<std::string, IteratorStats>> iterator_stats;

  ScanState state;

  MonoTime start_time;
  MonoTime last_access_time;
  uint32_t last_call_seq_id;

  // The cumulative amounts of wall, user cpu, and system cpu time spent on
  // this scanner, in seconds.
  CpuTimes cpu_times;
};

// RAII wrapper to update a scanner with timing information upon scope exit.
class ScopedAddScannerTiming {
 public:
  // 'scanner' must outlive the scoped object.
  // object pointed to by 'cpu_times' will contain the cpu timing information of the scanner upon
  // scope exit
  explicit ScopedAddScannerTiming(Scanner* scanner, CpuTimes* cpu_times)
      : stopped_(false),
        scanner_(scanner),
        cpu_times_(cpu_times) {
    sw_.start();
  }

  ~ScopedAddScannerTiming() {
    if (!stopped_) {
      Stop();
    }
  }

  // Stop the timing and update the scanner.
  void Stop() {
    stopped_ = true;
    sw_.stop();
    scanner_->AddTimings(sw_.elapsed());
    *cpu_times_ = scanner_->cpu_times();
  }

  bool stopped_;
  Scanner* scanner_;
  CpuTimes* cpu_times_;
  Stopwatch sw_;

  DISALLOW_COPY_AND_ASSIGN(ScopedAddScannerTiming);
};

} // namespace tserver
} // namespace kudu

