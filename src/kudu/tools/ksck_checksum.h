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
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/gutil/macros.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/make_shared.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace kudu {

namespace rpc {
class PeriodicTimer;
class Messenger;
} // namespace rpc

namespace tools {

class KsckCluster;
class KsckTablet;
class KsckTabletServer;

// Options for checksum scans.
struct KsckChecksumOptions {
  // A checksum with this snapshot timestamp will choose a timestamp for each
  // tablet, from one of the tablet servers hosting a replica, at the
  // time the checksums are started.
  static constexpr uint64_t kCurrentTimestamp = 0;

  KsckChecksumOptions();

  KsckChecksumOptions(std::vector<std::string> table_filters,
                      std::vector<std::string> tablet_id_filters);

  KsckChecksumOptions(MonoDelta timeout,
                      MonoDelta idle_timeout,
                      int scan_concurrency,
                      bool use_snapshot,
                      uint64_t snapshot_timestamp);

  KsckChecksumOptions(MonoDelta timeout,
                      MonoDelta idle_timeout,
                      int scan_concurrency,
                      bool use_snapshot,
                      uint64_t snapshot_timestamp,
                      std::vector<std::string> table_filters,
                      std::vector<std::string> tablet_id_filters);

  // The maximum total time to wait for results to come back from all replicas.
  MonoDelta timeout;

  // The maximum amount of time to wait for progress to be made. Progress
  // means at least one additional row is checksummed.
  MonoDelta idle_timeout;

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

typedef std::pair<Status, uint64_t> ReplicaChecksumResult;
typedef std::unordered_map<std::string, ReplicaChecksumResult> TabletChecksumResult;
typedef std::unordered_map<std::string, TabletChecksumResult> TabletChecksumResultsMap;

// A convenience struct containing info needed to checksum a particular tablet.
struct TabletChecksumInfo {
  TabletChecksumInfo(std::shared_ptr<KsckTablet> tablet, Schema schema)
    : tablet(std::move(tablet)),
      schema(std::move(schema)) {}

  // The tablet to be checksummed.
  std::shared_ptr<KsckTablet> tablet;

  // The schema of the tablet's table.
  Schema schema;
};

typedef std::unordered_map<std::string, TabletChecksumInfo> TabletInfoMap;

// Map (tablet server UUID -> number of open slots available for checksum scans).
typedef std::unordered_map<std::string, int> TabletServerChecksumScanSlotsMap;

typedef std::vector<std::shared_ptr<KsckTabletServer>> TabletServerList;

// Class to coordinate a checksum process. Checksums are started on all replicas
// of a tablet at once while respecting per-tablet-server checksum scan
// concurrency limits.
class KsckChecksumManager : public std::enable_shared_from_this<KsckChecksumManager>,
                            public enable_make_shared<KsckChecksumManager> {
 public:
  // Return in 'manager' a new KsckChecksumManager created from the given
  // parameters. All replicas of tablet in 'tablet_infos' must be on tablet
  // servers in 'tservers'. Because its ownership is shared with callbacks that
  // are part of the checksum process, a KsckChecksumManager should always be
  // wrapped in a shared_ptr.
  // If 'messenger' is non-null, it will be used by the instance; otherwise, a
  // new messenger will be constructed.
  static Status New(KsckChecksumOptions opts,
                    TabletInfoMap tablet_infos,
                    TabletServerList tservers,
                    std::shared_ptr<rpc::Messenger> messenger,
                    std::shared_ptr<KsckChecksumManager>* manager);

  // Reports an increase in the number of rows and bytes from disk processed
  // by checksums to this KsckChecksumManager. This information is used in
  // progress messages.
  void ReportProgress(int64_t delta_rows, int64_t delta_bytes);

  // Reports the result of checksumming all the replicas of tablet to this
  // KsckChecksumManager.
  void ReportResult(const std::string& tablet_id,
                    const std::string& replica_uuid,
                    const Status& status,
                    uint64_t checksum);

  // The possible final outcomes of a checksum process.
  enum class Outcome {
    // All replicas finished their checksums (either successfully or not).
    FINISHED,
    // The checksum process timed out.
    TIMED_OUT,
    // The checksum process went too long without making progress.
    IDLE_TIMED_OUT,
  };

  // Blocks until the number of replica results and errors reported equals
  // the number of replicas that need to be processed, until the this instnce's
  // timeout expires, or until the checksum process makes no progress for longer
  // than this instance's idle timeout, whichever comes first. Progress messages
  // are printed to 'out' if it is non-null.
  Outcome WaitFor(std::ostream* out);

  // Run the checksum process asynchronously.
  // The caller should wait for results with WaitFor().
  Status RunChecksumsAsync();

  // Get a snapshot of results reported so far.
  TabletChecksumResultsMap checksums() const {
    std::lock_guard<simple_spinlock> guard(lock_);
    return checksums_;
  }

 protected:
  KsckChecksumManager(int num_replicas,
                      KsckChecksumOptions opts,
                      TabletInfoMap tablet_infos,
                      TabletServerList tservers,
                      std::shared_ptr<rpc::Messenger> messenger);

 private:
  // Perform post-construction initialization that may fail.
  Status Init();

  // Shutdown this manager.
  void Shutdown();

  // Start as many tablet checksums as possible, given the per-tablet-server
  // concurrency limits on checksum scans.
  // Since this uses a brute force method, it is fairly expensive, and therefore
  // we run it on a threadpool instead of in the callback, which is run from a
  // reactor thread.
  void StartTabletChecksums();

  // Are there enough checksum scan slots available on the tablet servers
  // hosting replicas of 'tablet' to start a checksum scan on all of them?
  // If so, return true and reserve the slots. Else, return false.
  bool ReserveSlotsToChecksumUnlocked(const std::shared_ptr<KsckTablet>& tablet);

  // Begin the checksum on the tablet named in 'tablet_info'.
  void BeginTabletChecksum(const TabletChecksumInfo& tablet_info);

  // Initialize 'ts_open_slots_map_'.
  void InitializeTsSlotsMap();

  // Release a checksum scan slot on each tserver in 'tserver_uuids'.
  void ReleaseTsSlotsUnlocked(const std::vector<std::string>& ts_uuids);

  // Are there any open slots at all?
  bool HasOpenTsSlotsUnlocked() const;

  // Returns a summary of checksum scan slot usage across tablet servers.
  // This is useful as debug info to check how saturated the tablet servers are
  // with checksum scans.
  std::string OpenTsSlotSummaryString() const;

  // The options for the checksum process.
  const KsckChecksumOptions opts_;

  // A map of information about tablets to be checksummed. As tablet checksums
  // are started, entries are removed from this map.
  TabletInfoMap tablet_infos_;

  // Tracks the open slots for each tablet server that hosts a replica of the
  // tablets in 'tablet_infos_'.
  TabletServerChecksumScanSlotsMap ts_slots_open_map_;

  // checksums_ is an unordered_map of { tablet_id : { replica_uuid : checksum } }.
  TabletChecksumResultsMap checksums_;

  // Protects 'tablet_infos_', 'ts_slots_map_', and 'checksums_'.
  mutable simple_spinlock lock_;

  // The list of tablet servers that checksum scans will be run on. Every
  // replica of tablet to be checksummed must be located on one of these
  // tablet servers.
  const TabletServerList tservers_;

  const int expected_replica_count_;
  CountDownLatch responses_;

  // Used for the 'timestamp_update_timer_' periodic timer.
  std::shared_ptr<rpc::Messenger> messenger_;

  // A timer used to periodically refresh the timestamps of the tablet servers
  // in 'tablet_servers_', so that snapshot timestamps don't fall behind the
  // ancient history mark.
  std::shared_ptr<rpc::PeriodicTimer> timestamp_update_timer_;

  // A threadpool for running tasks that find additional tablets that can
  // be checksummed based on available slots on tablet servers.
  std::unique_ptr<ThreadPool> find_tablets_to_checksum_pool_;

  std::atomic<int64_t> rows_summed_;
  std::atomic<int64_t> disk_bytes_summed_;

  DISALLOW_COPY_AND_ASSIGN(KsckChecksumManager);
};

// A class for performing checksums on a Kudu cluster.
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
  // Builds a map of tablets to-be-checksummed, given the options in 'opts' and
  // the cluster 'cluster'. The resulting tablets are populated in 'tablet_infos'
  // and the total number of replicas to be checksummed is set in 'num_replica'.
  Status BuildTabletInfoMap(const KsckChecksumOptions& opts,
                            TabletInfoMap* tablet_infos,
                            int* num_replicas) const;

  // Collates the results of checksums that are reported in 'checksums' into
  // 'table_checksum_map', with the total number of results returned as
  // 'num_results'.
  // NOTE: Even if this function returns a bad Status, 'table_checksum_map'
  // and 'num_results' will still be populated using whatever results are
  // available.
  Status CollateChecksumResults(
      const TabletChecksumResultsMap& checksums,
      KsckTableChecksumMap* table_checksum_map,
      int* num_results) const;

  KsckCluster* cluster_;

  DISALLOW_COPY_AND_ASSIGN(KsckChecksummer);
};
} // namespace tools
} // namespace kudu
