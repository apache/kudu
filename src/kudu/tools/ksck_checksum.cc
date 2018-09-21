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

#include "kudu/tools/ksck_checksum.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/tool_action_common.h"

using std::endl;
using std::ostream;
using std::shared_ptr;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

DEFINE_int32(checksum_timeout_sec, 3600,
             "Maximum total seconds to wait for a checksum scan to complete "
             "before timing out.");
DEFINE_int32(checksum_scan_concurrency, 4,
             "Number of concurrent checksum scans to execute per tablet server.");
DEFINE_bool(checksum_snapshot, true, "Should the checksum scanner use a snapshot scan?");
DEFINE_uint64(checksum_snapshot_timestamp,
              kudu::tools::KsckChecksumOptions::kCurrentTimestamp,
              "Timestamp to use for snapshot checksum scans. Defaults to 0, which "
              "uses the current timestamp of a tablet server involved in the scan.");

namespace kudu {
namespace tools {

KsckChecksumOptions::KsckChecksumOptions()
    : KsckChecksumOptions({}, {}) {}

KsckChecksumOptions::KsckChecksumOptions(vector<string> table_filters,
                                         vector<string> tablet_id_filters)
    : KsckChecksumOptions(MonoDelta::FromSeconds(FLAGS_checksum_timeout_sec),
                          FLAGS_checksum_scan_concurrency,
                          FLAGS_checksum_snapshot,
                          FLAGS_checksum_snapshot_timestamp,
                          std::move(table_filters),
                          std::move(tablet_id_filters)) {}

KsckChecksumOptions::KsckChecksumOptions(MonoDelta timeout,
                                         int scan_concurrency,
                                         bool use_snapshot,
                                         uint64_t snapshot_timestamp)
    : KsckChecksumOptions(timeout,
                          scan_concurrency,
                          use_snapshot,
                          snapshot_timestamp,
                          {},
                          {}) {}

KsckChecksumOptions::KsckChecksumOptions(MonoDelta timeout,
                                         int scan_concurrency,
                                         bool use_snapshot,
                                         uint64_t snapshot_timestamp,
                                         vector<string> table_filters,
                                         vector<string> tablet_id_filters)
    : timeout(timeout),
      scan_concurrency(scan_concurrency),
      use_snapshot(use_snapshot),
      snapshot_timestamp(snapshot_timestamp),
      table_filters(std::move(table_filters)),
      tablet_id_filters(std::move(tablet_id_filters)) {}

ChecksumResultReporter::ChecksumResultReporter(int num_tablet_replicas)
    : expected_count_(num_tablet_replicas),
      responses_(num_tablet_replicas),
      rows_summed_(0),
      disk_bytes_summed_(0) {}

void ChecksumResultReporter::ReportProgress(int64_t delta_rows, int64_t delta_bytes) {
  rows_summed_.IncrementBy(delta_rows);
  disk_bytes_summed_.IncrementBy(delta_bytes);
}

// Write an entry to the result map indicating a response from the remote.
void ChecksumResultReporter::ReportResult(const string& tablet_id,
                                          const string& replica_uuid,
                                          const Status& status,
                                          uint64_t checksum) {
  std::lock_guard<simple_spinlock> guard(lock_);
  unordered_map<string, ResultPair>& replica_results =
      LookupOrInsert(&checksums_, tablet_id, unordered_map<string, ResultPair>());
  InsertOrDie(&replica_results, replica_uuid, ResultPair(status, checksum));
  responses_.CountDown();
}

// Blocks until either the number of results plus errors reported equals
// num_tablet_replicas (from the constructor), or until the timeout expires,
// whichever comes first. Progress messages are printed to 'out'.
// Returns false if the timeout expired before all responses came in.
// Otherwise, returns true.
// Print progress updates to 'out' if it is non-null.
bool ChecksumResultReporter::WaitFor(const MonoDelta& timeout, std::ostream* out) const {
  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + timeout;

  bool done = false;
  while (!done) {
    MonoTime now = MonoTime::Now();
    auto rem_ms = (deadline - now).ToMilliseconds();
    if (rem_ms <= 0) return false;

    constexpr int64_t max_wait_ms = 5000;
    done = responses_.WaitFor(
        MonoDelta::FromMilliseconds(std::min(rem_ms, max_wait_ms)));
    if (out) {
      string status = done ? "finished in" : "running for";
      int run_time_sec = (MonoTime::Now() - start).ToSeconds();
      (*out) << Substitute("Checksum $0 $1s: $2/$3 replicas remaining "
                           "($4 from disk, $5 rows summed)",
                           status,
                           run_time_sec,
                           responses_.count(),
                           expected_count_,
                           HumanReadableNumBytes::ToString(disk_bytes_summed_.Load()),
                           HumanReadableInt::ToString(rows_summed_.Load()))
             << endl;
    }
  }
  return true;
}

TabletServerChecksumCallbacks::TabletServerChecksumCallbacks(
    scoped_refptr<ChecksumResultReporter> reporter,
    shared_ptr<KsckTabletServer> tablet_server,
    SharedTabletQueue queue,
    string tablet_id,
    KsckChecksumOptions options)
    : reporter_(std::move(reporter)),
      tablet_server_(std::move(tablet_server)),
      queue_(std::move(queue)),
      options_(options),
      tablet_id_(std::move(tablet_id)) {}

void TabletServerChecksumCallbacks::Progress(int64_t rows_summed, int64_t disk_bytes_summed) {
  reporter_->ReportProgress(rows_summed, disk_bytes_summed);
}

void TabletServerChecksumCallbacks::Finished(const Status& status, uint64_t checksum) {
  reporter_->ReportResult(tablet_id_, tablet_server_->uuid(), status, checksum);

  std::pair<Schema, string> table_tablet;
  if (queue_->BlockingGet(&table_tablet)) {
    const Schema& table_schema = table_tablet.first;
    tablet_id_ = table_tablet.second;
    tablet_server_->RunTabletChecksumScanAsync(tablet_id_, table_schema, options_, this);
  } else {
    delete this;
  }
}

KsckChecksummer::KsckChecksummer(KsckCluster* cluster)
    : cluster_(CHECK_NOTNULL(cluster)) {}

Status KsckChecksummer::BuildTabletTableMap(
    const KsckChecksumOptions& opts,
    KsckChecksummer::TabletTableMap* tablet_table_map,
    int* num_replicas) const {
  CHECK(tablet_table_map);
  CHECK(num_replicas);

  TabletTableMap tablet_table_map_tmp;
  int num_tables = 0;
  int num_tablets = 0;
  int num_replicas_tmp = 0;
  for (const shared_ptr<KsckTable>& table : cluster_->tables()) {
    VLOG(1) << "Table: " << table->name();
    if (!MatchesAnyPattern(opts.table_filters, table->name())) continue;
    num_tables += 1;
    num_tablets += table->tablets().size();
    for (const shared_ptr<KsckTablet>& tablet : table->tablets()) {
      VLOG(1) << "Tablet: " << tablet->id();
      if (!MatchesAnyPattern(opts.tablet_id_filters, tablet->id())) continue;
      InsertOrDie(&tablet_table_map_tmp, tablet, table);
      num_replicas_tmp += tablet->replicas().size();
    }
  }

  if (num_tables == 0) {
    string msg = "No table found.";
    if (!opts.table_filters.empty()) {
      msg += " Filter: table_filters=" + JoinStrings(opts.table_filters, ",");
    }
    return Status::NotFound(msg);
  }

  if (num_tablets > 0 && num_replicas_tmp == 0) {
    // Warn if the table has tablets, but no replicas. The table may have no
    // tablets if all range partitions have been dropped.
    string msg = "No tablet replicas found.";
    if (!opts.table_filters.empty() || !opts.tablet_id_filters.empty()) {
      msg += " Filter:";
      if (!opts.table_filters.empty()) {
        msg += " table_filters=" + JoinStrings(opts.table_filters, ",");
      }
      if (!opts.tablet_id_filters.empty()) {
        msg += " tablet_id_filters=" + JoinStrings(opts.tablet_id_filters, ",");
      }
    }
    return Status::NotFound(msg);
  }

  *tablet_table_map = std::move(tablet_table_map_tmp);
  *num_replicas = num_replicas_tmp;
  return Status::OK();
}

Status KsckChecksummer::CollateChecksumResults(
    const ChecksumResultReporter::TabletResultMap& checksums,
    KsckTableChecksumMap* table_checksum_map,
    int* num_results) const {
  CHECK(table_checksum_map);
  CHECK(num_results);

  table_checksum_map->clear();
  *num_results = 0;
  int num_errors = 0;
  int num_mismatches = 0;
  for (const auto& table : cluster_->tables()) {
    KsckTableChecksum table_checksum;
    for (const auto& tablet : table->tablets()) {
      if (ContainsKey(checksums, tablet->id())) {
        KsckTabletChecksum tablet_checksum;
        tablet_checksum.tablet_id = tablet->id();
        bool seen_first_replica = false;
        uint64_t first_checksum = 0;

        for (const auto& r : FindOrDie(checksums, tablet->id())) {
          KsckReplicaChecksum replica_checksum;
          const auto& replica_uuid = r.first;
          const auto& ts = FindOrDie(cluster_->tablet_servers(), replica_uuid);
          replica_checksum.ts_uuid = ts->uuid();
          replica_checksum.ts_address = ts->address();

          const ChecksumResultReporter::ResultPair& result = r.second;
          const Status& status = result.first;
          replica_checksum.checksum = result.second;
          replica_checksum.status = status;
          if (!status.ok()) {
            num_errors++;
          } else if (!seen_first_replica) {
            seen_first_replica = true;
            first_checksum = replica_checksum.checksum;
          } else if (replica_checksum.checksum != first_checksum) {
            num_mismatches++;
            tablet_checksum.mismatch = true;
          }
          (*num_results)++;
          EmplaceOrDie(&tablet_checksum.replica_checksums,
                       replica_checksum.ts_uuid,
                       std::move(replica_checksum));
        }
        EmplaceOrDie(&table_checksum,
                     tablet_checksum.tablet_id,
                     std::move(tablet_checksum));
      }
    }
    EmplaceOrDie(table_checksum_map, table->name(), std::move(table_checksum));
  }

  if (num_mismatches != 0) {
    return Status::Corruption(Substitute("$0 checksum mismatches were detected.",
                                         num_mismatches));
  }
  if (num_errors != 0) {
    return Status::Aborted(Substitute("$0 errors were detected", num_errors));
  }
  return Status::OK();
}

Status KsckChecksummer::ChecksumData(const KsckChecksumOptions& opts,
                                     KsckChecksumResults* checksum_results,
                                     ostream* out_for_progress_updates) {
  CHECK(checksum_results);

  // Make a copy of 'opts' because we may need to assign a snapshot timestamp
  // if one was not provided.
  KsckChecksumOptions options = opts;

  // Clear the contents of 'checksum_results' because we always overwrite it
  // with whatever results are obtained (and with nothing if there's no results).
  checksum_results->snapshot_timestamp = boost::none;
  checksum_results->tables.clear();

  TabletTableMap tablet_table_map;
  int num_replicas;
  RETURN_NOT_OK(BuildTabletTableMap(options, &tablet_table_map, &num_replicas));

  // Map of tablet servers to tablet queue.
  typedef unordered_map<shared_ptr<KsckTabletServer>, SharedTabletQueue> TabletServerQueueMap;

  TabletServerQueueMap tablet_server_queues;
  scoped_refptr<ChecksumResultReporter> reporter(
      new ChecksumResultReporter(num_replicas));

  // Create a queue of checksum callbacks grouped by the tablet server.
  for (const auto& entry : tablet_table_map) {
    const shared_ptr<KsckTablet>& tablet = entry.first;
    const shared_ptr<KsckTable>& table = entry.second;
    for (const shared_ptr<KsckTabletReplica>& replica : tablet->replicas()) {
      const shared_ptr<KsckTabletServer>& ts =
          FindOrDie(cluster_->tablet_servers(), replica->ts_uuid());

      const SharedTabletQueue& queue =
          LookupOrInsertNewSharedPtr(&tablet_server_queues, ts, num_replicas);
      CHECK_EQ(QUEUE_SUCCESS, queue->Put(make_pair(table->schema(), tablet->id())));
    }
  }

  // Set the snapshot timestamp. If the sentinel value 'kCurrentTimestamp' was
  // provided, the snapshot timestamp is set to the current timestamp of some
  // healthy tablet server.
  if (options.use_snapshot &&
      options.snapshot_timestamp == KsckChecksumOptions::kCurrentTimestamp) {
    for (const auto& ts : tablet_server_queues) {
      if (ts.first->is_healthy()) {
        options.snapshot_timestamp = ts.first->current_timestamp();
        break;
      }
    }
    if (options.snapshot_timestamp == KsckChecksumOptions::kCurrentTimestamp) {
      return Status::ServiceUnavailable(
          "No tablet servers were available to fetch the current timestamp");
    }
    checksum_results->snapshot_timestamp = options.snapshot_timestamp;
  }

  // Kick off checksum scans in parallel. For each tablet server, we start
  // 'options.scan_concurrency' scans. Each callback then initiates one
  // additional scan when it returns if the queue for that TS is not empty.
  for (const auto& entry : tablet_server_queues) {
    const shared_ptr<KsckTabletServer>& tablet_server = entry.first;
    const SharedTabletQueue& queue = entry.second;
    queue->Shutdown(); // Ensures that BlockingGet() will not block.
    for (int i = 0; i < options.scan_concurrency; i++) {
      std::pair<Schema, std::string> table_tablet;
      if (queue->BlockingGet(&table_tablet)) {
        const Schema& table_schema = table_tablet.first;
        const std::string& tablet_id = table_tablet.second;
        auto* cbs = new TabletServerChecksumCallbacks(
            reporter, tablet_server, queue, tablet_id, options);
        // 'cbs' deletes itself when complete.
        tablet_server->RunTabletChecksumScanAsync(tablet_id, table_schema, options, cbs);
      }
    }
  }

  bool timed_out = !reporter->WaitFor(options.timeout, out_for_progress_updates);

  // Even if we timed out, collate the checksum results that we did get.
  KsckTableChecksumMap checksum_table_map;
  int num_results;
  const Status s = CollateChecksumResults(reporter->checksums(),
                                          &checksum_table_map,
                                          &num_results);
  checksum_results->tables = std::move(checksum_table_map);

  if (timed_out) {
    return Status::TimedOut(Substitute("Checksum scan did not complete within the timeout of $0: "
                                       "Received results for $1 out of $2 expected replicas",
                                       options.timeout.ToString(), num_results,
                                       num_replicas));
  }
  CHECK_EQ(num_results, num_replicas)
      << Substitute("Unexpected error: only got $0 out of $1 replica results",
                    num_results, num_replicas);
  return s;
}

} // namespace tools
} // namespace kudu
