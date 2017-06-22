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

#include "kudu/tools/ksck.h"

#include <algorithm>
#include <glog/logging.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>

#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/tools/color.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/atomic.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/threadpool.h"

DEFINE_int32(checksum_timeout_sec, 3600,
             "Maximum total seconds to wait for a checksum scan to complete "
             "before timing out.");
DEFINE_int32(checksum_scan_concurrency, 4,
             "Number of concurrent checksum scans to execute per tablet server.");
DEFINE_bool(checksum_snapshot, true, "Should the checksum scanner use a snapshot scan");
DEFINE_uint64(checksum_snapshot_timestamp,
              kudu::tools::ChecksumOptions::kCurrentTimestamp,
              "timestamp to use for snapshot checksum scans, defaults to 0, which "
              "uses the current timestamp of a tablet server involved in the scan");

DEFINE_int32(fetch_replica_info_concurrency, 20,
             "Number of concurrent tablet servers to fetch replica info from.");

DEFINE_bool(consensus, true,
            "Whether to check the consensus state from each tablet against the master.");

namespace kudu {
namespace tools {

using std::cout;
using std::endl;
using std::left;
using std::ostream;
using std::right;
using std::setw;
using std::shared_ptr;
using std::string;
using std::stringstream;
using std::unordered_map;
using strings::Substitute;

// The stream to write output to. If this is NULL, defaults to cout.
// This is used by tests to capture output.
ostream* g_err_stream = NULL;

// Print an informational message to cout.
static ostream& Out() {
  return (g_err_stream ? *g_err_stream : cout);
}

// Print a warning message to cout.
static ostream& Warn() {
  return Out() << Color(AnsiCode::YELLOW, "WARNING: ");
}

// Print an error message to cout.
static ostream& Error() {
  return Out() << Color(AnsiCode::RED, "WARNING: ");
}

namespace {
// Return true if 'str' matches any of the patterns in 'patterns', or if
// 'patterns' is empty.
bool MatchesAnyPattern(const vector<string>& patterns, const string& str) {
  // Consider no filter a wildcard.
  if (patterns.empty()) return true;

  for (const auto& p : patterns) {
    if (MatchPattern(str, p)) return true;
  }
  return false;
}
} // anonymous namespace

ChecksumOptions::ChecksumOptions()
    : timeout(MonoDelta::FromSeconds(FLAGS_checksum_timeout_sec)),
      scan_concurrency(FLAGS_checksum_scan_concurrency),
      use_snapshot(FLAGS_checksum_snapshot),
      snapshot_timestamp(FLAGS_checksum_snapshot_timestamp) {
}

ChecksumOptions::ChecksumOptions(MonoDelta timeout, int scan_concurrency,
                                 bool use_snapshot, uint64_t snapshot_timestamp)
    : timeout(timeout),
      scan_concurrency(scan_concurrency),
      use_snapshot(use_snapshot),
      snapshot_timestamp(snapshot_timestamp) {}

const uint64_t ChecksumOptions::kCurrentTimestamp = 0;

tablet::TabletStatePB KsckTabletServer::ReplicaState(const std::string& tablet_id) const {
  CHECK_EQ(state_, kFetched);
  if (!ContainsKey(tablet_status_map_, tablet_id)) {
    return tablet::UNKNOWN;
  }
  return tablet_status_map_.at(tablet_id).state();
}

KsckCluster::~KsckCluster() {
}

Status KsckCluster::FetchTableAndTabletInfo() {
  RETURN_NOT_OK(master_->Connect());
  RETURN_NOT_OK(RetrieveTablesList());
  RETURN_NOT_OK(RetrieveTabletServers());
  for (const shared_ptr<KsckTable>& table : tables()) {
    RETURN_NOT_OK(RetrieveTabletsList(table));
  }
  return Status::OK();
}

// Gets the list of tablet servers from the Master.
Status KsckCluster::RetrieveTabletServers() {
  return master_->RetrieveTabletServers(&tablet_servers_);
}

// Gets the list of tables from the Master.
Status KsckCluster::RetrieveTablesList() {
  return master_->RetrieveTablesList(&tables_);
}

Status KsckCluster::RetrieveTabletsList(const shared_ptr<KsckTable>& table) {
  return master_->RetrieveTabletsList(table);
}

Status Ksck::CheckMasterRunning() {
  VLOG(1) << "Connecting to the Master";
  Status s = cluster_->master()->Connect();
  if (s.ok()) {
    Out() << "Connected to the Master" << endl;
  }
  return s;
}

Status Ksck::FetchTableAndTabletInfo() {
  return cluster_->FetchTableAndTabletInfo();
}

Status Ksck::FetchInfoFromTabletServers() {
  VLOG(1) << "Getting the Tablet Servers list";
  int servers_count = cluster_->tablet_servers().size();
  VLOG(1) << Substitute("List of $0 Tablet Servers retrieved", servers_count);

  if (servers_count == 0) {
    return Status::NotFound("No tablet servers found");
  }

  gscoped_ptr<ThreadPool> pool;
  RETURN_NOT_OK(ThreadPoolBuilder("ksck-fetch")
                .set_max_threads(FLAGS_fetch_replica_info_concurrency)
                .Build(&pool));

  AtomicInt<int32_t> bad_servers(0);
  VLOG(1) << "Fetching info from all the Tablet Servers";
  for (const KsckMaster::TSMap::value_type& entry : cluster_->tablet_servers()) {
    CHECK_OK(pool->SubmitFunc([&]() {
          Status s = ConnectToTabletServer(entry.second);
          if (!s.ok()) {
            bad_servers.Increment();
          }
        }));
  }
  pool->Wait();

  if (bad_servers.Load() == 0) {
    Out() << Substitute("Fetched info from all $0 Tablet Servers", servers_count) << endl;
    return Status::OK();
  } else {
    Warn() << Substitute("Fetched info from $0 Tablet Servers, $1 weren't reachable",
                         servers_count - bad_servers.Load(), bad_servers.Load()) << endl;
    return Status::NetworkError("Not all Tablet Servers are reachable");
  }
}

Status Ksck::ConnectToTabletServer(const shared_ptr<KsckTabletServer>& ts) {
  VLOG(1) << "Going to connect to Tablet Server: " << ts->uuid();
  Status s = ts->FetchInfo();
  if (s.ok()) {
    VLOG(1) << "Connected to Tablet Server: " << ts->uuid();
    if (FLAGS_consensus) {
      Status t = ts->FetchConsensusState();
      if (!t.ok()) {
        Warn() << Substitute("Errors gathering consensus info for Tablet Server $0: $1",
                             ts->ToString(), t.ToString()) << endl;
      }
    }
  } else {
    Warn() << Substitute("Unable to connect to Tablet Server $0: $1",
                         ts->ToString(), s.ToString()) << endl;
  }
  return s;
}

Status Ksck::CheckTablesConsistency() {
  int tables_checked = 0;
  int bad_tables_count = 0;
  for (const shared_ptr<KsckTable> &table : cluster_->tables()) {
    if (!MatchesAnyPattern(table_filters_, table->name())) {
      VLOG(1) << "Skipping table " << table->name();
      continue;
    }
    tables_checked++;
    if (!VerifyTable(table)) {
      bad_tables_count++;
    }
    Out() << endl;
  }

  if (tables_checked == 0) {
    Out() << "The cluster doesn't have any matching tables" << endl;
    return Status::OK();
  }

  if (bad_tables_count == 0) {
    Out() << Substitute("The metadata for $0 table(s) is HEALTHY", tables_checked) << endl;
    return Status::OK();
  } else {
    Warn() << Substitute("$0 out of $1 table(s) are not in a healthy state",
                         bad_tables_count, tables_checked) << endl;
    return Status::Corruption(Substitute("$0 table(s) are bad", bad_tables_count));
  }
}

// Class to act as a collector of scan results.
// Provides thread-safe accessors to update and read a hash table of results.
class ChecksumResultReporter : public RefCountedThreadSafe<ChecksumResultReporter> {
 public:
  typedef std::pair<Status, uint64_t> ResultPair;
  typedef std::unordered_map<std::string, ResultPair> ReplicaResultMap;
  typedef std::unordered_map<std::string, ReplicaResultMap> TabletResultMap;

  // Initialize reporter with the number of replicas being queried.
  explicit ChecksumResultReporter(int num_tablet_replicas)
      : expected_count_(num_tablet_replicas),
        responses_(num_tablet_replicas),
        rows_summed_(0),
        disk_bytes_summed_(0) {
  }

  void ReportProgress(int64_t delta_rows, int64_t delta_bytes) {
    rows_summed_.IncrementBy(delta_rows);
    disk_bytes_summed_.IncrementBy(delta_bytes);
  }

  // Write an entry to the result map indicating a response from the remote.
  void ReportResult(const std::string& tablet_id,
                    const std::string& replica_uuid,
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
  // whichever comes first.
  // Returns false if the timeout expired before all responses came in.
  // Otherwise, returns true.
  bool WaitFor(const MonoDelta& timeout) const {
    MonoTime start = MonoTime::Now();
    MonoTime deadline = start + timeout;

    bool done = false;
    while (!done) {
      MonoTime now = MonoTime::Now();
      int rem_ms = (deadline - now).ToMilliseconds();
      if (rem_ms <= 0) return false;

      done = responses_.WaitFor(MonoDelta::FromMilliseconds(std::min(rem_ms, 5000)));
      string status = done ? "finished in " : "running for ";
      int run_time_sec = (MonoTime::Now() - start).ToSeconds();
      Out() << "Checksum " << status << run_time_sec << "s: "
             << responses_.count() << "/" << expected_count_ << " replicas remaining ("
             << HumanReadableNumBytes::ToString(disk_bytes_summed_.Load()) << " from disk, "
             << HumanReadableInt::ToString(rows_summed_.Load()) << " rows summed)"
             << endl;
    }
    return true;
  }

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

  // Report either a success or error response.
  void HandleResponse(const std::string& tablet_id, const std::string& replica_uuid,
                      const Status& status, uint64_t checksum);

  const int expected_count_;
  CountDownLatch responses_;

  mutable simple_spinlock lock_; // Protects 'checksums_'.
  // checksums_ is an unordered_map of { tablet_id : { replica_uuid : checksum } }.
  TabletResultMap checksums_;

  AtomicInt<int64_t> rows_summed_;
  AtomicInt<int64_t> disk_bytes_summed_;
};

// Queue of tablet replicas for an individual tablet server.
typedef shared_ptr<BlockingQueue<std::pair<Schema, std::string> > > SharedTabletQueue;

// A set of callbacks which records the result of a tablet replica's checksum,
// and then checks if the tablet server has any more tablets to checksum. If so,
// a new async checksum scan is started.
class TabletServerChecksumCallbacks : public ChecksumProgressCallbacks {
 public:
  TabletServerChecksumCallbacks(
    scoped_refptr<ChecksumResultReporter> reporter,
    shared_ptr<KsckTabletServer> tablet_server,
    SharedTabletQueue queue,
    std::string tablet_id,
    ChecksumOptions options) :
      reporter_(std::move(reporter)),
      tablet_server_(std::move(tablet_server)),
      queue_(std::move(queue)),
      options_(options),
      tablet_id_(std::move(tablet_id)) {
  }

  void Progress(int64_t rows_summed, int64_t disk_bytes_summed) override {
    reporter_->ReportProgress(rows_summed, disk_bytes_summed);
  }

  void Finished(const Status& status, uint64_t checksum) override {
    reporter_->ReportResult(tablet_id_, tablet_server_->uuid(), status, checksum);

    std::pair<Schema, std::string> table_tablet;
    if (queue_->BlockingGet(&table_tablet)) {
      const Schema& table_schema = table_tablet.first;
      tablet_id_ = table_tablet.second;
      tablet_server_->RunTabletChecksumScanAsync(tablet_id_, table_schema, options_, this);
    } else {
      delete this;
    }
  }

 private:
  const scoped_refptr<ChecksumResultReporter> reporter_;
  const shared_ptr<KsckTabletServer> tablet_server_;
  const SharedTabletQueue queue_;
  const ChecksumOptions options_;

  std::string tablet_id_;
};

Status Ksck::ChecksumData(const ChecksumOptions& opts) {
  // Copy options so that local modifications can be made and passed on.
  ChecksumOptions options = opts;

  typedef unordered_map<shared_ptr<KsckTablet>, shared_ptr<KsckTable>> TabletTableMap;
  TabletTableMap tablet_table_map;

  int num_tables = 0;
  int num_tablets = 0;
  int num_tablet_replicas = 0;
  for (const shared_ptr<KsckTable>& table : cluster_->tables()) {
    VLOG(1) << "Table: " << table->name();
    if (!MatchesAnyPattern(table_filters_, table->name())) continue;
    num_tables += 1;
    num_tablets += table->tablets().size();
    for (const shared_ptr<KsckTablet>& tablet : table->tablets()) {
      VLOG(1) << "Tablet: " << tablet->id();
      if (!MatchesAnyPattern(tablet_id_filters_, tablet->id())) continue;
      InsertOrDie(&tablet_table_map, tablet, table);
      num_tablet_replicas += tablet->replicas().size();
    }
  }

  if (num_tables == 0) {
    string msg = "No table found.";
    if (!table_filters_.empty()) {
      msg += " Filter: table_filters=" + JoinStrings(table_filters_, ",");
    }
    return Status::NotFound(msg);
  }

  if (num_tablets > 0 && num_tablet_replicas == 0) {
    // Warn if the table has tablets, but no replicas. The table may have no
    // tablets if all range partitions have been dropped.
    string msg = "No tablet replicas found.";
    if (!table_filters_.empty() || !tablet_id_filters_.empty()) {
      msg += " Filter: ";
      if (!table_filters_.empty()) {
        msg += "table_filters=" + JoinStrings(table_filters_, ",");
      }
      if (!tablet_id_filters_.empty()) {
        msg += "tablet_id_filters=" + JoinStrings(tablet_id_filters_, ",");
      }
    }
    return Status::NotFound(msg);
  }

  // Map of tablet servers to tablet queue.
  typedef unordered_map<shared_ptr<KsckTabletServer>, SharedTabletQueue> TabletServerQueueMap;

  TabletServerQueueMap tablet_server_queues;
  scoped_refptr<ChecksumResultReporter> reporter(new ChecksumResultReporter(num_tablet_replicas));

  // Create a queue of checksum callbacks grouped by the tablet server.
  for (const TabletTableMap::value_type& entry : tablet_table_map) {
    const shared_ptr<KsckTablet>& tablet = entry.first;
    const shared_ptr<KsckTable>& table = entry.second;
    for (const shared_ptr<KsckTabletReplica>& replica : tablet->replicas()) {
      const shared_ptr<KsckTabletServer>& ts =
          FindOrDie(cluster_->tablet_servers(), replica->ts_uuid());

      const SharedTabletQueue& queue =
          LookupOrInsertNewSharedPtr(&tablet_server_queues, ts, num_tablet_replicas);
      CHECK_EQ(QUEUE_SUCCESS, queue->Put(make_pair(table->schema(), tablet->id())));
    }
  }

  if (options.use_snapshot && options.snapshot_timestamp == ChecksumOptions::kCurrentTimestamp) {
    // Set the snapshot timestamp to the current timestamp of the first healthy tablet server
    // we can find.
    for (const auto& ts : tablet_server_queues) {
      if (ts.first->is_healthy()) {
        options.snapshot_timestamp = ts.first->current_timestamp();
        break;
      }
    }
    if (options.snapshot_timestamp == ChecksumOptions::kCurrentTimestamp) {
      return Status::ServiceUnavailable(
          "No tablet servers were available to fetch the current timestamp");
    }
    Out() << "Using snapshot timestamp: " << options.snapshot_timestamp << endl;
  }

  // Kick off checksum scans in parallel. For each tablet server, we start
  // scan_concurrency scans. Each callback then initiates one additional
  // scan when it returns if the queue for that TS is not empty.
  for (const TabletServerQueueMap::value_type& entry : tablet_server_queues) {
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

  bool timed_out = !reporter->WaitFor(options.timeout);

  // Even if we timed out, print the checksum results that we did get.
  ChecksumResultReporter::TabletResultMap checksums = reporter->checksums();

  int num_errors = 0;
  int num_mismatches = 0;
  int num_results = 0;
  for (const shared_ptr<KsckTable>& table : cluster_->tables()) {
    bool printed_table_name = false;
    for (const shared_ptr<KsckTablet>& tablet : table->tablets()) {
      if (ContainsKey(checksums, tablet->id())) {
        if (!printed_table_name) {
          printed_table_name = true;
          cout << "-----------------------" << endl;
          cout << table->name() << endl;
          cout << "-----------------------" << endl;
        }
        bool seen_first_replica = false;
        uint64_t first_checksum = 0;

        for (const ChecksumResultReporter::ReplicaResultMap::value_type& r :
                      FindOrDie(checksums, tablet->id())) {
          const string& replica_uuid = r.first;

          shared_ptr<KsckTabletServer> ts = FindOrDie(cluster_->tablet_servers(), replica_uuid);
          const ChecksumResultReporter::ResultPair& result = r.second;
          const Status& status = result.first;
          uint64_t checksum = result.second;
          string status_str = (status.ok()) ? Substitute("Checksum: $0", checksum)
                                            : Substitute("Error: $0", status.ToString());
          cout << Substitute("T $0 P $1 ($2): $3", tablet->id(), ts->uuid(), ts->address(),
                                                   status_str) << endl;
          if (!status.ok()) {
            num_errors++;
          } else if (!seen_first_replica) {
            seen_first_replica = true;
            first_checksum = checksum;
          } else if (checksum != first_checksum) {
            num_mismatches++;
            Error() << ">> Mismatch found in table " << table->name()
                    << " tablet " << tablet->id() << endl;
          }
          num_results++;
        }
      }
    }
    if (printed_table_name) cout << endl;
  }
  if (timed_out) {
    return Status::TimedOut(Substitute("Checksum scan did not complete within the timeout of $0: "
                                       "Received results for $1 out of $2 expected replicas",
                                       options.timeout.ToString(), num_results,
                                       num_tablet_replicas));
  }
  CHECK_EQ(num_results, num_tablet_replicas)
      << Substitute("Unexpected error: only got $0 out of $1 replica results",
                    num_results, num_tablet_replicas);

  if (num_mismatches != 0) {
    return Status::Corruption(Substitute("$0 checksum mismatches were detected.", num_mismatches));
  }
  if (num_errors != 0) {
    return Status::Aborted(Substitute("$0 errors were detected", num_errors));
  }

  return Status::OK();
}

bool Ksck::VerifyTable(const shared_ptr<KsckTable>& table) {
  const auto all_tablets = table->tablets();
  vector<shared_ptr<KsckTablet>> tablets;
  std::copy_if(all_tablets.begin(), all_tablets.end(), std::back_inserter(tablets),
                 [&](const shared_ptr<KsckTablet>& t) {
                   return MatchesAnyPattern(tablet_id_filters_, t->id());
                 });

  int table_num_replicas = table->num_replicas();
  VLOG(1) << Substitute("Verifying $0 tablet(s) for table $1 configured with num_replicas = $2",
                        tablets.size(), table->name(), table_num_replicas);

  map<CheckResult, int> result_counts;
  for (const auto& tablet : tablets) {
    auto tablet_result = VerifyTablet(tablet, table_num_replicas);
    result_counts[tablet_result]++;
  }
  if (result_counts[CheckResult::OK] == tablets.size()) {
    Out() << Substitute("Table $0 is $1 ($2 tablet(s) checked)",
                        table->name(),
                        Color(AnsiCode::GREEN, "HEALTHY"),
                        tablets.size()) << endl;
    return true;
  } else {
    if (result_counts[CheckResult::UNAVAILABLE] > 0) {
      Out() << Substitute("Table $0 has $1 $2 tablet(s)",
                          table->name(),
                          result_counts[CheckResult::UNAVAILABLE],
                          Color(AnsiCode::RED, "unavailable")) << endl;
    }
    if (result_counts[CheckResult::UNDER_REPLICATED] > 0) {
      Out() << Substitute("Table $0 has $1 $2 tablet(s)",
                          table->name(),
                          result_counts[CheckResult::UNDER_REPLICATED],
                          Color(AnsiCode::YELLOW, "under-replicated")) << endl;
    }
    return false;
  }
}

namespace {

// A struct consolidating the state of each replica, for easier analysis.
struct ReplicaInfo {
  KsckTabletReplica* replica;
  KsckTabletServer* ts = nullptr;
  tablet::TabletStatePB state = tablet::UNKNOWN;
  boost::optional<tablet::TabletStatusPB> status_pb;
  boost::optional<KsckConsensusState> consensus_state;
};

// Formats the peers known and unknown to 'config' using labels from 'peer_uuid_mapping'.
string format_peers(const map<string, char>& peer_uuid_mapping, const KsckConsensusState& config) {
  stringstream voters;
  int peer_width = 4;
  for (const auto &entry : peer_uuid_mapping) {
    if (!ContainsKey(config.peer_uuids, entry.first)) {
      voters << setw(peer_width) << left << "";
      continue;
    }
    if (config.leader_uuid && config.leader_uuid == entry.first) {
      voters << setw(peer_width) << left << Substitute("$0*", entry.second);
    } else {
      voters << setw(peer_width) << left << Substitute("$0", entry.second);
    }
  }
  return voters.str();
}

} // anonymous namespace

Ksck::CheckResult Ksck::VerifyTablet(const shared_ptr<KsckTablet>& tablet, int table_num_replicas) {
  const string tablet_str = Substitute("Tablet $0 of table '$1'",
                                 tablet->id(), tablet->table()->name());

  // Organize consensus info for the master.
  auto leader_it = std::find_if(tablet->replicas().cbegin(), tablet->replicas().cend(),
      [](const shared_ptr<KsckTabletReplica>& r) -> bool { return r->is_leader(); });
  boost::optional<string> leader_uuid;
  if (leader_it != tablet->replicas().cend()) {
    leader_uuid = (*leader_it)->ts_uuid();
  }
  vector<string> peer_uuids_from_master;
  for (const auto& replica : tablet->replicas()) {
    peer_uuids_from_master.push_back(replica->ts_uuid());
  }
  KsckConsensusState master_config(KsckConsensusConfigType::MASTER,
                                   boost::none, boost::none,
                                   leader_uuid, peer_uuids_from_master);

  vector<ReplicaInfo> replica_infos;
  for (const shared_ptr<KsckTabletReplica>& replica : tablet->replicas()) {
    replica_infos.emplace_back();
    auto* repl_info = &replica_infos.back();
    repl_info->replica = replica.get();
    VLOG(1) << Substitute("A replica of tablet $0 is on live tablet server $1",
                          tablet->id(), replica->ts_uuid());

    // Check for agreement on tablet assignment and state between the master
    // and the tablet server.
    auto ts = FindPointeeOrNull(cluster_->tablet_servers(), replica->ts_uuid());
    repl_info->ts = ts;
    if (ts && ts->is_healthy()) {
      repl_info->state = ts->ReplicaState(tablet->id());
      if (ContainsKey(ts->tablet_status_map(), tablet->id())) {
        repl_info->status_pb = ts->tablet_status_map().at(tablet->id());
      }

      // Organize consensus info for each replica.
      if (FLAGS_consensus) {
        std::pair<string, string> tablet_key = std::make_pair(ts->uuid(), tablet->id());
        if (!ContainsKey(ts->tablet_consensus_state_map(), tablet_key)) {
          continue;
        }
        const auto& cstate = FindOrDieNoPrint(ts->tablet_consensus_state_map(), tablet_key);
        const auto& config = cstate.has_pending_config() ?
                             cstate.pending_config() :
                             cstate.committed_config();
        const auto& peers = config.peers();
        vector<string> peer_uuids_from_replica;
        for (const auto& pb : peers) {
          peer_uuids_from_replica.push_back(pb.permanent_uuid());
        }
        boost::optional<int64_t> opid_index;
        if (config.has_opid_index()) {
          opid_index = config.opid_index();
        }
        boost::optional<string> repl_leader_uuid;
        if (cstate.has_leader_uuid()) {
          repl_leader_uuid = cstate.leader_uuid();
        }
        KsckConsensusConfigType config_type = cstate.has_pending_config() ?
                                              KsckConsensusConfigType::PENDING :
                                              KsckConsensusConfigType::COMMITTED;
        repl_info->consensus_state = KsckConsensusState(config_type,
                                                         cstate.current_term(),
                                                         opid_index,
                                                         repl_leader_uuid,
                                                         peer_uuids_from_replica);
      }
    }
  }

  // Summarize the states.
  int leaders_count = 0;
  int running_count = 0;
  for (const auto& r : replica_infos) {
    if (r.replica->is_leader()) {
      leaders_count++;
    }
    if (r.state == tablet::RUNNING) {
      running_count++;
    }
  }

  // Reconcile the master's and peers' consensus configs.
  int conflicting_states = 0;
  if (FLAGS_consensus) {
    for (const auto& r : replica_infos) {
      if (r.consensus_state && !r.consensus_state->Matches(master_config)) {
        conflicting_states++;
      }
    }
  }
  std::sort(replica_infos.begin(), replica_infos.end(),
            [](const ReplicaInfo& left, const ReplicaInfo& right) -> bool {
              if (!left.ts) return true;
              if (!right.ts) return false;
              return left.ts->uuid() < right.ts->uuid();
            });

  // Determine the overall health state of the tablet.
  CheckResult result = CheckResult::OK;
  int num_voters = replica_infos.size();
  int majority_size = consensus::MajoritySize(num_voters);
  if (running_count < majority_size) {
    Out() << Substitute("$0 is $1: $2 replica(s) not RUNNING",
                        tablet_str,
                        Color(AnsiCode::RED, "unavailable"),
                        num_voters - running_count) << endl;
    result = CheckResult::UNAVAILABLE;
  } else if (running_count < num_voters) {
    Out() << Substitute("$0 is $1: $2 replica(s) not RUNNING",
                        tablet_str,
                        Color(AnsiCode::YELLOW, "under-replicated"),
                        num_voters - running_count) << endl;
    result = CheckResult::UNDER_REPLICATED;
  } else if (check_replica_count_ && num_voters < table_num_replicas) {
    Out() << Substitute("$0 is $1: configuration has $2 replicas vs desired $3",
                        tablet_str,
                        Color(AnsiCode::YELLOW, "under-replicated"),
                        num_voters,
                        table_num_replicas) << endl;
    result = CheckResult::UNDER_REPLICATED;
  } else if (leaders_count != 1) {
    Out() << Substitute("$0 is $1: expected one LEADER replica",
                        tablet_str, Color(AnsiCode::RED, "unavailable")) << endl;
    result = CheckResult::UNAVAILABLE;
  } else if (conflicting_states > 0) {
    Out() << Substitute("$0 is $1: $0 replicas' active configs disagree with the master's",
                        tablet_str,
                        Color(AnsiCode::YELLOW, "conflicted"),
                        conflicting_states) << endl;
    result = CheckResult::CONSENSUS_MISMATCH;
  }

  // In the case that we found something wrong, dump info on all the replicas
  // to make it easy to debug.
  if (result != CheckResult::OK) {
    for (const ReplicaInfo& r : replica_infos) {
      string ts_str = r.ts ? r.ts->ToString() : r.replica->ts_uuid();
      const char* leader_str = r.replica->is_leader() ? " [LEADER]" : "";

      Out() << "  " << ts_str << ": ";
      if (!r.ts || !r.ts->is_healthy()) {
        Out() << Color(AnsiCode::YELLOW, "TS unavailable") << leader_str << endl;
        continue;
      }
      if (r.state == tablet::RUNNING) {
        Out() << Color(AnsiCode::GREEN, "RUNNING") << leader_str << endl;
        continue;
      }
      if (r.status_pb == boost::none) {
        Out() << Color(AnsiCode::YELLOW, "missing") << leader_str << endl;
        continue;
      }

      Out() << Color(AnsiCode::YELLOW, "bad state") << leader_str << endl;
      Out() << Substitute(
          "    State:       $0\n"
          "    Data state:  $1\n"
          "    Last status: $2\n",
          Color(AnsiCode::BLUE, tablet::TabletStatePB_Name(r.state)),
          Color(AnsiCode::BLUE, tablet::TabletDataState_Name(r.status_pb->tablet_data_state())),
          Color(AnsiCode::BLUE, r.status_pb->last_status()));
    }

    Out() << endl;
  }

  // If there are consensus conflicts, dump the consensus info too.
  if (conflicting_states > 0) {
    if (result != CheckResult::CONSENSUS_MISMATCH) {
      Out() << Substitute("$0 replicas' active configs differ from the master's.",
                          conflicting_states)
            << endl;
    }
    int i = 0;
    map<string, char> peer_uuid_mapping;
    // TODO(wdb): use a scheme that gives > 26 unique labels.
    const string labels = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    for (const auto& peer : master_config.peer_uuids) {
      if (InsertIfNotPresent(&peer_uuid_mapping, peer, labels[i])) {
        i = (i + 1) % labels.size();
      }
    }
    for (const ReplicaInfo& rs : replica_infos) {
      if (!rs.consensus_state) continue;
      for (const auto& peer : rs.consensus_state->peer_uuids) {
        if (InsertIfNotPresent(&peer_uuid_mapping, peer, labels[i])) {
          i = (i + 1) % labels.size();
        }
      }
    }

    Out() << "  All the peers reported by the master and tablet servers are:" << endl;
    for (const auto& entry : peer_uuid_mapping) {
      Out() << "  " << entry.second << " = " << entry.first << endl;
    }
    Out() << endl;
    Out() << "The consensus matrix is:" << endl;

    // Prepare the header and columns for PrintTable.
    const vector<string> headers{ "Config source", "Voters", "Current term",
                                  "Config index", "Committed?" };

    // Seed the columns with the master info.
    vector<string> sources{"master"};
    vector<string> voters{format_peers(peer_uuid_mapping, master_config)};
    vector<string> terms{""};
    vector<string> indexes{""};
    vector<string> committed{"Yes"};

    // Fill out the columns with info from the replicas.
    for (const auto& replica : replica_infos) {
      char label = FindOrDie(peer_uuid_mapping, replica.ts->uuid());
      sources.emplace_back(1, label);
      if (!replica.consensus_state) {
        voters.emplace_back("[config not available]");
        terms.emplace_back("");
        indexes.emplace_back("");
        committed.emplace_back("");
        continue;
      }
      voters.push_back(format_peers(peer_uuid_mapping, replica.consensus_state.get()));
      terms.push_back(replica.consensus_state->term ?
                      std::to_string(replica.consensus_state->term.get()) : "");
      indexes.push_back(replica.consensus_state->opid_index ?
                        std::to_string(replica.consensus_state->opid_index.get()) : "");
      committed.emplace_back(replica.consensus_state->type == KsckConsensusConfigType::PENDING ?
                          "No" : "Yes");
    }
    vector<vector<string>> columns{ sources, voters, terms, indexes, committed };
    PrintTable(headers, columns, Out());
  }

  return result;
}

} // namespace tools
} // namespace kudu
