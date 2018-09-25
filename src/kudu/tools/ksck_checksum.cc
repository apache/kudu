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
#include <functional>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/periodic.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

using std::endl;
using std::ostream;
using std::shared_ptr;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

DEFINE_int32(checksum_idle_timeout_sec, 60 * 10,
             "Maximum total seconds to wait without making any progress in a "
             "checksum scan before timing out due to idleness.");
DEFINE_int32(checksum_timeout_sec, 24 * 60 * 60,
             "Maximum total seconds to wait for a checksum scan to complete "
             "before timing out.");
DEFINE_int32(checksum_scan_concurrency, 4,
             "Number of concurrent checksum scans to execute per tablet server.");
DEFINE_int32(max_progress_report_wait_ms, 5000,
             "Maximum number of milliseconds to wait between progress reports. "
             "Used to speed up tests.");
TAG_FLAG(max_progress_report_wait_ms, hidden);
DEFINE_int32(timestamp_update_period_ms, 60000,
             "Number of milliseconds to wait between updating the current "
             "timestamps used for checksum scans. This would only need to be "
             "changed if checksumming replicas on servers with a very low "
             "value of -tablet_history_max_age_sec.");
TAG_FLAG(timestamp_update_period_ms, advanced);
DEFINE_int32(wait_before_setting_snapshot_timestamp_ms, 0,
             "Number of milliseconds to wait before assigning a timestamp and "
             "starting a checksum scan. For tests only.");
TAG_FLAG(wait_before_setting_snapshot_timestamp_ms, hidden);
TAG_FLAG(wait_before_setting_snapshot_timestamp_ms, unsafe);
DEFINE_bool(checksum_snapshot, true, "Should the checksum scanner use a snapshot scan?");
DEFINE_uint64(checksum_snapshot_timestamp,
              kudu::tools::KsckChecksumOptions::kCurrentTimestamp,
              "Timestamp to use for snapshot checksum scans. Defaults to 0, "
              "which will cause each tablet to use a recent timestamp chosen "
              "when all the checksums for its replicas begin.");

namespace kudu {
namespace tools {

namespace {
const string LogPrefix(const string& tablet_id,
                       const string& replica_uuid = "") {
  if (replica_uuid.empty()) {
    return Substitute("T $0: ", tablet_id);
  }
  return Substitute("T $0 P $1: ", tablet_id, replica_uuid);
}

// Count the replica in 'tablet_infos' and check that the every replica belongs
// to a tablet server listed in 'tservers'.
Status CountReplicasAndCheckTabletServersAreConsistent(
    const TabletInfoMap& tablet_infos,
    const TabletServerList& tservers,
    int* num_replicas) {
  CHECK(num_replicas);
  *num_replicas = 0;
  std::set<string> tserver_uuid_set;
  for (const auto& tserver : tservers) {
    InsertIfNotPresent(&tserver_uuid_set, tserver->uuid());
  }
  for (const auto& entry : tablet_infos) {
    const auto& tablet = entry.second.tablet;
    for (const auto& replica : tablet->replicas()) {
      (*num_replicas)++;
      if (!ContainsKey(tserver_uuid_set, replica->ts_uuid())) {
        return Status::InvalidArgument(Substitute(
            "tablet server $0 hosting a replica of tablet $1 not found in "
            "list of tablet servers",
            replica->ts_uuid(),
            tablet->id()));
      }
    }
  }
  return Status::OK();
}
} // anonymous namespace

KsckChecksumOptions::KsckChecksumOptions()
    : KsckChecksumOptions({}, {}) {}

KsckChecksumOptions::KsckChecksumOptions(vector<string> table_filters,
                                         vector<string> tablet_id_filters)
    : KsckChecksumOptions(MonoDelta::FromSeconds(FLAGS_checksum_timeout_sec),
                          MonoDelta::FromSeconds(FLAGS_checksum_idle_timeout_sec),
                          FLAGS_checksum_scan_concurrency,
                          FLAGS_checksum_snapshot,
                          FLAGS_checksum_snapshot_timestamp,
                          std::move(table_filters),
                          std::move(tablet_id_filters)) {}

KsckChecksumOptions::KsckChecksumOptions(MonoDelta timeout,
                                         MonoDelta idle_timeout,
                                         int scan_concurrency,
                                         bool use_snapshot,
                                         uint64_t snapshot_timestamp)
    : KsckChecksumOptions(timeout,
                          idle_timeout,
                          scan_concurrency,
                          use_snapshot,
                          snapshot_timestamp,
                          {},
                          {}) {}

KsckChecksumOptions::KsckChecksumOptions(MonoDelta timeout,
                                         MonoDelta idle_timeout,
                                         int scan_concurrency,
                                         bool use_snapshot,
                                         uint64_t snapshot_timestamp,
                                         vector<string> table_filters,
                                         vector<string> tablet_id_filters)
    : timeout(timeout),
      idle_timeout(idle_timeout),
      scan_concurrency(scan_concurrency),
      use_snapshot(use_snapshot),
      snapshot_timestamp(snapshot_timestamp),
      table_filters(std::move(table_filters)),
      tablet_id_filters(std::move(tablet_id_filters)) {}

void KsckChecksumManager::InitializeTsSlotsMap() {
  for (const auto& tserver : tservers_) {
    EmplaceIfNotPresent(&ts_slots_open_map_,
                        tserver->uuid(),
                        opts_.scan_concurrency);
  }
}

void KsckChecksumManager::ReleaseTsSlotsUnlocked(const vector<string>& ts_uuids) {
  for (const auto& uuid : ts_uuids) {
    auto& slots_open = FindOrDie(ts_slots_open_map_, uuid);
    DCHECK_GE(slots_open, 0);
    DCHECK_LT(slots_open, opts_.scan_concurrency);
    slots_open++;
  }
}

bool KsckChecksumManager::HasOpenTsSlotsUnlocked() const {
  for (const auto& entry : ts_slots_open_map_) {
    DCHECK_GE(entry.second, 0);
    DCHECK_LE(entry.second, opts_.scan_concurrency);
    if (entry.second > 0) {
      return true;
    }
  }
  return false;;
}

string KsckChecksumManager::OpenTsSlotSummaryString() const {
  std::lock_guard<simple_spinlock> lock(lock_);
  string summary = "Summary of Open TS Slots";
  for (const auto& entry : ts_slots_open_map_) {
    summary.append(Substitute("\n$0 : $1 / $2",
                              entry.first,
                              entry.second,
                              opts_.scan_concurrency));
  }
  return summary;
}

Status KsckChecksumManager::New(KsckChecksumOptions opts,
                                TabletInfoMap tablet_infos,
                                TabletServerList tservers,
                                shared_ptr<rpc::Messenger> messenger,
                                shared_ptr<KsckChecksumManager>* manager) {
  CHECK(manager);
  int num_replicas;
  RETURN_NOT_OK(CountReplicasAndCheckTabletServersAreConsistent(
      tablet_infos,
      tservers,
      &num_replicas));
  auto manager_tmp = KsckChecksumManager::make_shared(num_replicas,
                                                      std::move(opts),
                                                      std::move(tablet_infos),
                                                      std::move(tservers),
                                                      std::move(messenger));
  RETURN_NOT_OK(manager_tmp->Init());
  *manager = std::move(manager_tmp);
  return Status::OK();
}


KsckChecksumManager::KsckChecksumManager(
    int num_replicas,
    KsckChecksumOptions opts,
    TabletInfoMap tablet_infos,
    TabletServerList tservers,
    shared_ptr<rpc::Messenger> messenger)
    : opts_(std::move(opts)),
      tablet_infos_(std::move(tablet_infos)),
      tservers_(std::move(tservers)),
      expected_replica_count_(num_replicas),
      responses_(num_replicas),
      messenger_(std::move(messenger)),
      rows_summed_(0),
      disk_bytes_summed_(0) {
  InitializeTsSlotsMap();
}

Status KsckChecksumManager::Init() {
  ThreadPoolBuilder builder("find_tablets_to_checksum");
  builder.set_max_threads(1);
  return builder.Build(&find_tablets_to_checksum_pool_);
}

void KsckChecksumManager::Shutdown() {
  find_tablets_to_checksum_pool_->Shutdown();
  timestamp_update_timer_->Stop();
}

void KsckChecksumManager::ReportProgress(int64_t delta_rows, int64_t delta_bytes) {
  DCHECK_GE(delta_rows, 0);
  DCHECK_GE(delta_bytes, 0);
  rows_summed_ += delta_rows;
  disk_bytes_summed_ += delta_bytes;
}

void KsckChecksumManager::ReportResult(const string& tablet_id,
                                       const string& replica_uuid,
                                       const Status& status,
                                       uint64_t checksum) {
  VLOG(1) << LogPrefix(tablet_id, replica_uuid)
          << "Checksum finished. Status: " << status.ToString();

  {
    std::lock_guard<simple_spinlock> guard(lock_);
    auto& tablet_result = LookupOrEmplace(&checksums_,
                                          tablet_id,
                                          TabletChecksumResult());
    EmplaceOrDie(&tablet_result, replica_uuid, std::make_pair(status, checksum));
    ReleaseTsSlotsUnlocked({ replica_uuid });
  }

  responses_.CountDown();
  WARN_NOT_OK(find_tablets_to_checksum_pool_->SubmitFunc(
      std::bind(&KsckChecksumManager::StartTabletChecksums, this)),
      "failed to submit task to start additional tablet checksums");
}

KsckChecksumManager::Outcome KsckChecksumManager::WaitFor(std::ostream* out) {
  SCOPED_CLEANUP({ Shutdown(); });

  MonoTime start = MonoTime::Now();
  const MonoTime deadline = start + opts_.timeout;

  int64_t rows_summed_prev = 0;
  MonoTime progress_deadline = start + opts_.idle_timeout;

  bool done = false;
  while (!done) {
    MonoTime now = MonoTime::Now();
    int rem_ms = (deadline - now).ToMilliseconds();
    if (rem_ms <= 0) {
      return Outcome::TIMED_OUT;
    }

    done = responses_.WaitFor(MonoDelta::FromMilliseconds(
        std::min(rem_ms, FLAGS_max_progress_report_wait_ms)));

    // Checked the rows summed vs the previous value to see if any progress has
    // been made. Also load the disk bytes summed, so there's less chance the
    // two are out-of-sync when printed later.
    int64_t rows_summed = rows_summed_;
    int64_t disk_bytes_summed = disk_bytes_summed_;
    if (rows_summed == rows_summed_prev) {
      if (now > progress_deadline) {
        return Outcome::IDLE_TIMED_OUT;
      }
    } else {
      progress_deadline = now + opts_.idle_timeout;
    }
    rows_summed_prev = rows_summed;

    if (out) {
      string status = done ? "finished in" : "running for";
      int run_time_sec = (MonoTime::Now() - start).ToSeconds();
      (*out) << Substitute("Checksum $0 $1s: $2/$3 replicas remaining "
                           "($4 from disk, $5 rows summed)",
                           status,
                           run_time_sec,
                           responses_.count(),
                           expected_replica_count_,
                           HumanReadableNumBytes::ToString(disk_bytes_summed),
                           HumanReadableInt::ToString(rows_summed))
             << endl;
    }
    VLOG(1) << OpenTsSlotSummaryString() << endl;
  }
  return Outcome::FINISHED;
}

bool KsckChecksumManager::ReserveSlotsToChecksumUnlocked(
    const shared_ptr<KsckTablet>& tablet) {
  DCHECK(lock_.is_locked());
  vector<int*> slot_counts_to_decrement;
  for (const auto& replica : tablet->replicas()) {
    auto* slots_open = FindOrNull(ts_slots_open_map_, replica->ts_uuid());
    DCHECK(slots_open);
    DCHECK_GE(*slots_open, 0);
    DCHECK_LE(*slots_open, opts_.scan_concurrency);
    if (*slots_open == 0) {
      return false;
    }
    slot_counts_to_decrement.push_back(slots_open);
  }
  for (auto* slots_open : slot_counts_to_decrement) {
    (*slots_open)--;
  }
  return true;
}

Status KsckChecksumManager::RunChecksumsAsync() {
  if (!messenger_) {
    rpc::MessengerBuilder builder("timestamp update");
    RETURN_NOT_OK(builder.Build(&messenger_));
  }
  timestamp_update_timer_ = rpc::PeriodicTimer::Create(
      messenger_,
      [&]() {
        VLOG(1) << "Updating timestamps";
        for (auto& ts : tservers_) {
          ts->FetchCurrentTimestampAsync();
        }
      },
      MonoDelta::FromMilliseconds(FLAGS_timestamp_update_period_ms));
  timestamp_update_timer_->Start();
  StartTabletChecksums();
  return Status::OK();
}

void KsckChecksumManager::BeginTabletChecksum(const TabletChecksumInfo& tablet_info) {
  VLOG(1) << LogPrefix(tablet_info.tablet->id()) << "Starting checksum";
  std::unordered_set<string> replica_uuids;
  for (const auto& replica : tablet_info.tablet->replicas()) {
    InsertOrDie(&replica_uuids, replica->ts_uuid());
  }

  TabletServerList tablet_servers;
  for (const auto& ts : tservers_) {
    if (ContainsKey(replica_uuids, ts->uuid())) {
      tablet_servers.push_back(ts);
    }
  }

  MAYBE_INJECT_FIXED_LATENCY(FLAGS_wait_before_setting_snapshot_timestamp_ms);

  // Use the current timestamp of a peer if the user did not specify a timestamp.
  uint64_t timestamp_for_tablet = opts_.snapshot_timestamp;
  if (opts_.use_snapshot &&
      opts_.snapshot_timestamp == KsckChecksumOptions::kCurrentTimestamp) {
    for (const auto& ts : tablet_servers) {
      if (ts->is_healthy()) {
        timestamp_for_tablet = ts->current_timestamp();
        break;
      }
    }
    // If we couldn't get a timestamp from any peer because they are unhealthy,
    // short circuit the checksum for the tablet with an error.
    if (timestamp_for_tablet == KsckChecksumOptions::kCurrentTimestamp) {
      for (const auto& ts : tablet_servers) {
        ReportResult(
            tablet_info.tablet->id(),
            ts->uuid(),
            Status::Aborted("no healthy peer was available to provide a timestamp"),
            0);
      }
      return;
    }
  }

  VLOG(1) << LogPrefix(tablet_info.tablet->id()) << "Using timestamp "
          << timestamp_for_tablet;

  for (const auto& ts: tablet_servers) {
    // Copy options and set timestamp for each replica checksum.
    KsckChecksumOptions options = opts_;
    options.snapshot_timestamp = timestamp_for_tablet;
    ts->RunTabletChecksumScanAsync(tablet_info.tablet->id(),
                                   tablet_info.schema,
                                   options,
                                   shared_from_this());
  }
}

void KsckChecksumManager::StartTabletChecksums() {
  vector<TabletChecksumInfo> requests_to_process;
  {
    // To find all tablets that we can start checksums on, we check every
    // one. This means checking 'ts_open_slots_map_' once for every replica,
    // so it's pretty expensive. But, compared to checksumming multi-gigabyte
    // replicas, and in particular the benefit of greater parallelism in
    // checksumming such replicas, it seems like it's worth it.
    std::lock_guard<simple_spinlock> guard(lock_);
    // Short-circuit if there's no slots available.
    if (!HasOpenTsSlotsUnlocked()) {
      VLOG(1) << "No slots open. Short-circuiting search.";
      return;
    }
    for (const auto& entry : tablet_infos_) {
      const auto& request = entry.second;
      if (ReserveSlotsToChecksumUnlocked(request.tablet)) {
        requests_to_process.push_back(request);
      }
    }
    for (const auto& request : requests_to_process) {
      tablet_infos_.erase(request.tablet->id());
    }
    VLOG(1) << Substitute("Starting checksums on $0 tablet(s)",
                          requests_to_process.size());
  }
  for (const auto& request : requests_to_process) {
    BeginTabletChecksum(request);
  }
}

KsckChecksummer::KsckChecksummer(KsckCluster* cluster)
    : cluster_(CHECK_NOTNULL(cluster)) {}

Status KsckChecksummer::BuildTabletInfoMap(
    const KsckChecksumOptions& opts,
    TabletInfoMap* tablet_infos,
    int* num_replicas) const {
  CHECK(tablet_infos);
  CHECK(num_replicas);

  TabletInfoMap tablet_infos_tmp;
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
      EmplaceOrDie(&tablet_infos_tmp,
                   tablet->id(),
                   TabletChecksumInfo(tablet, table->schema()));
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

  *tablet_infos = std::move(tablet_infos_tmp);
  *num_replicas = num_replicas_tmp;
  return Status::OK();
}

Status KsckChecksummer::CollateChecksumResults(
    const TabletChecksumResultsMap& checksums,
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

          const ReplicaChecksumResult& result = r.second;
          const Status& status = result.first;
          replica_checksum.checksum = result.second;
          replica_checksum.status = status;
          if (!status.ok()) {
            num_errors++;
          } else if (!seen_first_replica) {
            seen_first_replica = true;
            first_checksum = replica_checksum.checksum;
          } else if (replica_checksum.checksum != first_checksum &&
                     !tablet_checksum.mismatch) {
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
    if (table_checksum.empty()) {
      continue;
    }
    EmplaceOrDie(table_checksum_map, table->name(), std::move(table_checksum));
  }

  if (num_mismatches != 0) {
    return Status::Corruption(Substitute("$0 tablet(s) had checksum mismatches",
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

  // Clear the contents of 'checksum_results' because we always overwrite it
  // with whatever results are obtained (and with nothing if there's no results).
  checksum_results->snapshot_timestamp = boost::none;
  checksum_results->tables.clear();

  TabletInfoMap tablet_infos;
  int num_replicas;
  RETURN_NOT_OK(BuildTabletInfoMap(opts, &tablet_infos, &num_replicas));

  TabletServerList tablet_servers;
  for (const auto& entry : cluster_->tablet_servers()) {
    tablet_servers.push_back(entry.second);
  }

  // Set the snapshot timestamp. If the sentinel value 'kCurrentTimestamp' was
  // provided, the snapshot timestamp is set to the current timestamp of some
  // healthy tablet server, and it may be updated for each tablet, as it is
  // checksummed.
  if (opts.use_snapshot) {
    if (opts.snapshot_timestamp == KsckChecksumOptions::kCurrentTimestamp) {
      // The timestamps are actually set for each tablet when the tablet is
      // checksummed, but let's do a sanity check that some tablet server is
      // available to provide timestamps.
      bool exists_healthy_ts = false;
      for (const auto& ts : tablet_servers) {
        if (ts->is_healthy()) {
          exists_healthy_ts = true;
          break;
        }
      }
      if (!exists_healthy_ts) {
        return Status::ServiceUnavailable(
            "no tablet servers are available");
      }
    } else {
      // The results only include the snapshot timestamp when it applies to
      // every tablet.
      checksum_results->snapshot_timestamp = opts.snapshot_timestamp;
    }
  }

  shared_ptr<KsckChecksumManager> manager;
  RETURN_NOT_OK(KsckChecksumManager::New(opts,
                                         tablet_infos,
                                         tablet_servers,
                                         cluster_->messenger(),
                                         &manager));
  RETURN_NOT_OK(manager->RunChecksumsAsync());

  auto final_status = manager->WaitFor(out_for_progress_updates);

  // Even if we timed out, collate the checksum results that we did get.
  KsckTableChecksumMap checksum_table_map;
  int num_results;
  const Status s = CollateChecksumResults(manager->checksums(),
                                          &checksum_table_map,
                                          &num_results);
  checksum_results->tables = std::move(checksum_table_map);

  switch (final_status) {
    case KsckChecksumManager::Outcome::TIMED_OUT:
      return Status::TimedOut(Substitute("Checksum scan did not complete "
                                         "within the timeout of $0: Received "
                                         "results for $1 out of $2 expected "
                                         "replicas",
                                         opts.timeout.ToString(),
                                         num_results,
                                         num_replicas));
    case KsckChecksumManager::Outcome::IDLE_TIMED_OUT:
      return Status::TimedOut(Substitute("Checksum scan did not make progress "
                                         "within the idle timeout of $0: Received "
                                         "results for $1 out of $2 expected "
                                         "replicas",
                                         opts.idle_timeout.ToString(),
                                         num_results,
                                         num_replicas));
    case KsckChecksumManager::Outcome::FINISHED:
      CHECK_EQ(num_results, num_replicas)
        << Substitute("Unexpected error: only got $0 out of $1 replica results",
                      num_results, num_replicas);
      return s;
  }
}

} // namespace tools
} // namespace kudu
