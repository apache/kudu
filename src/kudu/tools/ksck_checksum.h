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

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/util/atomic.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

class Schema;

namespace tools {

class KsckCluster;
class KsckTable;
class KsckTablet;
class KsckTabletServer;

// Options for checksum scans.
struct KsckChecksumOptions {
  // A checksum with this special timestamp will use a timestamp selected by
  // one of tablet servers performing the snapshot scan.
  static constexpr uint64_t kCurrentTimestamp = 0;

  KsckChecksumOptions();

  KsckChecksumOptions(std::vector<std::string> table_filters,
                      std::vector<std::string> tablet_id_filters);

  KsckChecksumOptions(MonoDelta timeout,
                      int scan_concurrency,
                      bool use_snapshot,
                      uint64_t snapshot_timestamp);

  KsckChecksumOptions(MonoDelta timeout,
                      int scan_concurrency,
                      bool use_snapshot,
                      uint64_t snapshot_timestamp,
                      std::vector<std::string> table_filters,
                      std::vector<std::string> tablet_id_filters);

  // The maximum total time to wait for results to come back from all replicas.
  MonoDelta timeout;

  // The maximum number of concurrent checksum scans to run per tablet server.
  int scan_concurrency;

  // Whether to use a snapshot checksum scanner.
  bool use_snapshot;

  // The snapshot timestamp to use for snapshot checksum scans.
  uint64_t snapshot_timestamp;

  // Filters for the table names and tablet ids whose contents should be
  // checksummed.
  std::vector<std::string> table_filters;
  std::vector<std::string> tablet_id_filters;
};

// Interface for reporting progress on checksumming a single
// replica.
class KsckChecksumProgressCallbacks {
 public:
  virtual ~KsckChecksumProgressCallbacks() {}

  // Report incremental progress from the server side.
  // 'delta_disk_bytes_summed' only counts data read from DiskRowSets on the
  // server side and does not count MRS bytes, etc.
  virtual void Progress(int64_t delta_rows_summed, int64_t delta_disk_bytes_summed) = 0;

  // The scan of the current tablet is complete.
  virtual void Finished(const Status& status, uint64_t checksum) = 0;
};

// Class to act as a collector of scan results.
// Provides thread-safe accessors to update and read a hash table of results.
class ChecksumResultReporter : public RefCountedThreadSafe<ChecksumResultReporter> {
 public:
  typedef std::pair<Status, uint64_t> ResultPair;
  typedef std::unordered_map<std::string, ResultPair> ReplicaResultMap;
  typedef std::unordered_map<std::string, ReplicaResultMap> TabletResultMap;

  // Initialize reporter with the number of replicas being queried.
  explicit ChecksumResultReporter(int num_tablet_replicas);

  void ReportProgress(int64_t delta_rows, int64_t delta_bytes);

  // Write an entry to the result map indicating a response from the remote.
  void ReportResult(const std::string& tablet_id,
                    const std::string& replica_uuid,
                    const Status& status,
                    uint64_t checksum);

  // Blocks until either the number of results plus errors reported equals
  // num_tablet_replicas (from the constructor), or until the timeout expires,
  // whichever comes first. Progress messages are printed to 'out'.
  // Returns false if the timeout expired before all responses came in.
  // Otherwise, returns true.
  bool WaitFor(const MonoDelta& timeout, std::ostream* out) const;

  // Returns true iff all replicas have reported in.
  bool AllReported() const { return responses_.count() == 0; }

  // Get reported results.
  TabletResultMap checksums() const {
    std::lock_guard<simple_spinlock> guard(lock_);
    return checksums_;
  }

 private:
  friend class RefCountedThreadSafe<ChecksumResultReporter>;
  ~ChecksumResultReporter() {}

  const int expected_count_;
  CountDownLatch responses_;

  mutable simple_spinlock lock_; // Protects 'checksums_'.
  // checksums_ is an unordered_map of { tablet_id : { replica_uuid : checksum } }.
  TabletResultMap checksums_;

  AtomicInt<int64_t> rows_summed_;
  AtomicInt<int64_t> disk_bytes_summed_;
};

// Queue of tablet replicas for an individual tablet server.
typedef std::shared_ptr<BlockingQueue<std::pair<Schema, std::string>>> SharedTabletQueue;

// A set of callbacks which records the result of a tablet replica's checksum,
// and then checks if the tablet server has any more tablets to checksum. If so,
// a new async checksum scan is started.
class TabletServerChecksumCallbacks : public KsckChecksumProgressCallbacks {
 public:
  TabletServerChecksumCallbacks(
      scoped_refptr<ChecksumResultReporter> reporter,
      std::shared_ptr<KsckTabletServer> tablet_server,
      SharedTabletQueue queue,
      std::string tablet_id,
      KsckChecksumOptions options);

  void Progress(int64_t rows_summed, int64_t disk_bytes_summed) override;

  void Finished(const Status& status, uint64_t checksum) override;

 private:
  ~TabletServerChecksumCallbacks() = default;

  const scoped_refptr<ChecksumResultReporter> reporter_;
  const std::shared_ptr<KsckTabletServer> tablet_server_;
  const SharedTabletQueue queue_;
  const KsckChecksumOptions options_;

  std::string tablet_id_;
};

// A class for performing checksum scans against a Kudu cluster.
class KsckChecksummer {
 public:
   // 'cluster' must remain valid as long as this instance is alive.
  explicit KsckChecksummer(KsckCluster* cluster);

  // Checksum the data in the Kudu cluster according to the options provided in
  // 'opts'. Results will be populated in the 'checksum_results'. If non-null,
  // progress updates will be written to 'out_for_progress_updates'.
  // NOTE: Even if this method returns a bad Status, 'checksum_results' will be
  // populated with whatever checksum results were received.
  Status ChecksumData(const KsckChecksumOptions& opts,
                      KsckChecksumResults* checksum_results,
                      std::ostream* out_for_progress_updates);

 private:
  typedef std::unordered_map<std::shared_ptr<KsckTablet>,
                             std::shared_ptr<KsckTable>> TabletTableMap;

  // Builds a mapping from tablet-to-be-checksummed to its table, which is
  // used to create checksum callbacks. This mapping is returned in
  // 'tablet_table_map' and the total number of replicas to be checksummed is
  // returned in 'num_replicas'.
  Status BuildTabletTableMap(const KsckChecksumOptions& opts,
                             TabletTableMap* tablet_table_map,
                             int* num_replicas) const;

  // Collates the results of checksums into 'table_checksum_map', with the
  // total number of results returned as 'num_results'.
  // NOTE: Even if this function returns a bad Status, 'table_checksum_map'
  // and 'num_results' will still be populated using whatever results are
  // available.
  Status CollateChecksumResults(
      const ChecksumResultReporter::TabletResultMap& checksums,
      KsckTableChecksumMap* table_checksum_map,
      int* num_results) const;

  KsckCluster* cluster_;

  DISALLOW_COPY_AND_ASSIGN(KsckChecksummer);
};
} // namespace tools
} // namespace kudu
