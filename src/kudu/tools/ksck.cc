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
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <mutex>
#include <numeric>
#include <tuple>
#include <type_traits>
#include <vector>

#include <boost/optional.hpp> // IWYU pragma: keep
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/atomic.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/countdown_latch.h"
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
DEFINE_bool(verbose, false,
            "Output detailed information even if no inconsistency is found.");

using std::cout;
using std::endl;
using std::left;
using std::map;
using std::ostream;
using std::ostringstream;
using std::setw;
using std::shared_ptr;
using std::string;
using std::to_string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

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

// Return a formatted string version of 'config', mapping UUIDs to single-character
// labels using the mapping 'label_by_uuid'.
string format_replicas(const map<string, char>& label_by_uuid, const KsckConsensusState& config) {
  constexpr int kPeerWidth = 4;
  ostringstream result;
  // Sort the output by label for readability.
  std::set<std::pair<char, string>> labeled_replicas;
  for (const auto& entry : label_by_uuid) {
    labeled_replicas.emplace(entry.second, entry.first);
  }
  for (const auto &entry : labeled_replicas) {
    if (!ContainsKey(config.voter_uuids, entry.second) &&
        !ContainsKey(config.non_voter_uuids, entry.second)) {
      result << setw(kPeerWidth) << left << "";
      continue;
    }
    if (config.leader_uuid && config.leader_uuid == entry.second) {
      result << setw(kPeerWidth) << left << Substitute("$0*", entry.first);
    } else {
      if (ContainsKey(config.non_voter_uuids, entry.second)) {
        result << setw(kPeerWidth) << left << Substitute("$0~", entry.first);
      } else {
        result << setw(kPeerWidth) << left << Substitute("$0", entry.first);
      }
    }
  }
  return result.str();
}

void BuildKsckConsensusStateForConfigMember(const consensus::ConsensusStatePB& cstate,
                                            KsckConsensusState* ksck_cstate) {
  CHECK(ksck_cstate);
  ksck_cstate->term = cstate.current_term();
  ksck_cstate->type = cstate.has_pending_config() ?
                      KsckConsensusConfigType::PENDING :
                      KsckConsensusConfigType::COMMITTED;
  const auto& config = cstate.has_pending_config() ?
                       cstate.pending_config() :
                       cstate.committed_config();
  if (config.has_opid_index()) {
    ksck_cstate->opid_index = config.opid_index();
  }
  // Test for emptiness rather than mere presence, since Kudu nodes set
  // leader_uuid to "" explicitly when they do not know about a leader.
  if (!cstate.leader_uuid().empty()) {
    ksck_cstate->leader_uuid = cstate.leader_uuid();
  }
  const auto& peers = config.peers();
  for (const auto& pb : peers) {
    if (pb.member_type() == consensus::RaftPeerPB::NON_VOTER) {
      InsertOrDie(&ksck_cstate->non_voter_uuids, pb.permanent_uuid());
    } else {
      InsertOrDie(&ksck_cstate->voter_uuids, pb.permanent_uuid());
    }
  }
}

void AddToUuidLabelMapping(const std::set<string>& uuids,
                           map<string, char>* uuid_label_mapping) {
  CHECK(uuid_label_mapping);
  // TODO(wdberkeley): use a scheme that gives > 26 unique labels.
  const string labels = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  int i = uuid_label_mapping->size() % labels.size();
  for (const auto& uuid : uuids) {
    if (InsertIfNotPresent(uuid_label_mapping, uuid, labels[i])) {
      i = (i + 1) % labels.size();
    }
  }
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
  CHECK_EQ(state_, KsckFetchState::FETCHED);
  if (!ContainsKey(tablet_status_map_, tablet_id)) {
    return tablet::UNKNOWN;
  }
  return tablet_status_map_.at(tablet_id).state();
}

std::ostream& operator<<(std::ostream& lhs, KsckFetchState state) {
  switch (state) {
    case KsckFetchState::UNINITIALIZED:
      lhs << "UNINITIALIZED";
      break;
    case KsckFetchState::FETCH_FAILED:
      lhs << "FETCH_FAILED";
      break;
    case KsckFetchState::FETCHED:
      lhs << "FETCHED";
      break;
    default:
      LOG(FATAL) << "unknown KsckFetchState";
  }
  return lhs;
}

Ksck::Ksck(shared_ptr<KsckCluster> cluster, ostream* out)
    : cluster_(std::move(cluster)),
      out_(out == nullptr ? &std::cout : out) {
}

string Ksck::ServerHealthToString(ServerHealth sh) {
  switch (sh) {
    case ServerHealth::HEALTHY:
      return "HEALTHY";
    case ServerHealth::UNAVAILABLE:
      return "UNAVAILABLE";
    case ServerHealth::WRONG_SERVER_UUID:
      return "WRONG_SERVER_UUID";
    default:
      LOG(FATAL) << "Unknown ServerHealth";
  }
}

int Ksck::ServerHealthScore(ServerHealth sh) {
  switch (sh) {
    case ServerHealth::HEALTHY:
      return 0;
    case ServerHealth::UNAVAILABLE:
      return 1;
    case ServerHealth::WRONG_SERVER_UUID:
      return 2;
    default:
      LOG(FATAL) << "Unknown ServerHealth";
  }
}

Status Ksck::CheckMasterHealth() {
  int bad_masters = 0;
  vector<ServerHealthSummary> master_summaries;
  // There shouldn't be more than 5 masters, so we'll keep it simple and gather
  // info in sequence instead of spreading it across a threadpool.
  for (const KsckCluster::MasterList::value_type& master : cluster_->masters()) {
    Status s = master->FetchInfo();
    ServerHealthSummary sh;
    sh.uuid = master->uuid();
    sh.address = master->address();
    sh.health = s.ok() ? ServerHealth::HEALTHY : ServerHealth::UNAVAILABLE;
    master_summaries.emplace_back(std::move(sh));
    if (!s.ok()) {
      Warn() << Substitute("Unable to connect to master $0: $1",
                           master->ToString(), s.ToString()) << endl;
      bad_masters++;
    } else if (FLAGS_consensus) {
      if (!master->FetchConsensusState().ok()) {
        Warn() << Substitute("Errors gathering consensus info for master $0: $1",
                             master->ToString(), s.ToString()) << endl;
      }
    }
  }
  CHECK_OK(PrintServerHealthSummaries(ServerType::MASTER, std::move(master_summaries), Out()));
  int num_masters = cluster_->masters().size();
  if (bad_masters > 0) {
    Warn() << Substitute("Fetched info from $0 masters; $1 weren't reachable",
                         num_masters, bad_masters) << endl;
    return Status::NetworkError("failed to gather info from all masters");
  }
  Out() << Substitute("Fetched info from all $0 masters", num_masters) << endl;
  return Status::OK();
}

Status Ksck::CheckMasterConsensus() {
  if (!FLAGS_consensus) {
    return Status::OK();
  }
  // There's no "reference" cstate for masters, so pick an arbitrary master
  // cstate to compare with.
  bool missing_or_conflict = false;
  map<string, KsckConsensusState> master_cstates;
  for (const KsckCluster::MasterList::value_type& master : cluster_->masters()) {
    if (master->cstate()) {
      KsckConsensusState ksck_cstate;
      BuildKsckConsensusStateForConfigMember(*master->cstate(), &ksck_cstate);
      InsertOrDie(&master_cstates, master->uuid(), ksck_cstate);
    } else {
      missing_or_conflict = true;
    }
  }
  if (master_cstates.empty()) {
    return Status::NotFound("no master consensus state available");
  }
  const KsckConsensusState& base = master_cstates.begin()->second;
  for (const auto& entry : master_cstates) {
    if (!base.Matches(entry.second)) {
      missing_or_conflict = true;
      break;
    }
  }
  if (missing_or_conflict || FLAGS_verbose) {
    // We need to make a consensus matrix for the masters now.
    if (missing_or_conflict) {
      Warn() << "masters have consensus conflicts";
    }
    map<string, char> replica_labels;
    for (const KsckCluster::MasterList::value_type& master : cluster_->masters()) {
      AddToUuidLabelMapping({ master->uuid() }, &replica_labels);
    }
    // Master configs have no non-voters.
    for (const auto& entry : master_cstates) {
      AddToUuidLabelMapping(entry.second.voter_uuids, &replica_labels);
    }
    Out() << "  All reported masters are:" << endl;
    // Sort the output by label for readability.
    std::set<std::pair<char, string>> reported_masters;
    for (const auto& entry : replica_labels) {
      reported_masters.emplace(entry.second, entry.first);
    }
    for (const auto& entry : reported_masters) {
      Out() << "  " << entry.first << " = " << entry.second << endl;
    }
    DataTable cmatrix({ "Config source", "Replicas", "Current term",
                        "Config index", "Committed?"});
    for (const KsckCluster::MasterList::value_type& master : cluster_->masters()) {
      const string label(1, FindOrDie(replica_labels, master->uuid()));
      if (master->cstate()) {
        const auto& cstate = master->cstate();
        const string opid_index_str = cstate->committed_config().has_opid_index() ?
                                      std::to_string(cstate->committed_config().opid_index()) :
                                      "";
        cmatrix.AddRow({ label,
                         format_replicas(replica_labels,
                                         FindOrDie(master_cstates,
                                         master->uuid())),
                         std::to_string(cstate->current_term()),
                         opid_index_str,
                         "Yes" });
      } else {
        cmatrix.AddRow({ label, "[config not available]", "", "", "" });
      }
    }
    RETURN_NOT_OK(cmatrix.PrintTo(Out()));
  }
  if (missing_or_conflict) {
    return Status::Corruption("there are master consensus conflicts");
  }
  return Status::OK();
}

Status Ksck::CheckClusterRunning() {
  VLOG(1) << "Connecting to the leader master";
  Status s = cluster_->Connect();
  if (s.ok()) {
    Out() << "Connected to the leader master" << endl;
  }
  return s;
}

Status Ksck::FetchTableAndTabletInfo() {
  return cluster_->FetchTableAndTabletInfo();
}

Status Ksck::FetchInfoFromTabletServers() {
  VLOG(1) << "Fetching the list of tablet servers";
  int servers_count = cluster_->tablet_servers().size();
  VLOG(1) << Substitute("List of $0 tablet servers retrieved", servers_count);

  if (servers_count == 0) {
    return Status::NotFound("No tablet servers found");
  }

  gscoped_ptr<ThreadPool> pool;
  RETURN_NOT_OK(ThreadPoolBuilder("ksck-fetch")
                .set_max_threads(FLAGS_fetch_replica_info_concurrency)
                .Build(&pool));

  AtomicInt<int32_t> bad_servers(0);
  VLOG(1) << "Fetching info from all " << servers_count << " tablet servers";

  vector<ServerHealthSummary> tablet_server_summaries;
  simple_spinlock tablet_server_summaries_lock;

  for (const KsckCluster::TSMap::value_type& entry : cluster_->tablet_servers()) {
    CHECK_OK(pool->SubmitFunc([&]() {
          Status s = ConnectToTabletServer(entry.second);
          ServerHealthSummary summary;
          summary.uuid = entry.second->uuid();
          summary.address = entry.second->address();
          if (!s.ok()) {
            bad_servers.Increment();
            if (s.IsRemoteError()) {
              summary.health = ServerHealth::WRONG_SERVER_UUID;
            } else {
              summary.health = ServerHealth::UNAVAILABLE;
            }
          } else {
            summary.health = ServerHealth::HEALTHY;
          }

          std::lock_guard<simple_spinlock> lock(tablet_server_summaries_lock);
          tablet_server_summaries.push_back(std::move(summary));
        }));
  }
  pool->Wait();

  CHECK_OK(PrintServerHealthSummaries(ServerType::TABLET_SERVER,
                                      std::move(tablet_server_summaries),
                                      Out()));

  if (bad_servers.Load() == 0) {
    Out() << Substitute("Fetched info from all $0 tablet servers", servers_count) << endl;
    return Status::OK();
  }
  Warn() << Substitute("Fetched info from $0 tablet servers, $1 weren't reachable",
                       servers_count - bad_servers.Load(), bad_servers.Load()) << endl;
  return Status::NetworkError("Could not gather complete information from all tablet servers");
}

Status Ksck::ConnectToTabletServer(const shared_ptr<KsckTabletServer>& ts) {
  VLOG(1) << "Going to connect to tablet server: " << ts->uuid();
  Status s = ts->FetchInfo();
  if (!s.ok()) {
    Warn() << Substitute("Unable to connect to tablet server $0: $1",
                         ts->ToString(), s.ToString()) << endl;
    return s;
  }
  VLOG(1) << "Connected to tablet server: " << ts->uuid();
  if (FLAGS_consensus) {
    s = ts->FetchConsensusState();
    if (!s.ok()) {
      Warn() << Substitute("Errors gathering consensus info for tablet server $0: $1",
                           ts->ToString(), s.ToString()) << endl;
    }
  }
  return s;
}

Status Ksck::PrintServerHealthSummaries(ServerType type,
                                        vector<ServerHealthSummary> summaries,
                                        ostream& out) {
  // Sort by (health decreasing, uuid, address), so bad health appears
  // closest to the bottom of the output in a terminal.
  // The address is used in the sort for the unavailable master case, because
  // we do not know the uuid in that case.
  std::sort(summaries.begin(), summaries.end(),
            [](const ServerHealthSummary& left, const ServerHealthSummary& right) {
              return std::make_tuple(ServerHealthScore(left.health), left.uuid, left.address) <
                     std::make_tuple(ServerHealthScore(right.health), right.uuid, right.address);
            });
  out << ServerTypeToString(type) << " Summary" << endl;
  DataTable table({ "UUID", "Address", "Status"});
  for (const auto& s : summaries) {
    table.AddRow({ s.uuid, s.address, ServerHealthToString(s.health) });
  }
  return table.PrintTo(out);
}

Status Ksck::PrintTableSummaries(const vector<TableSummary>& table_summaries, ostream& out) {
  out << "Table Summary" << endl;
  DataTable table({ "Name", "Status", "Total Tablets",
                    "Healthy", "Recovering", "Under-replicated", "Unavailable"});
  for (const TableSummary& ts : table_summaries) {
    string status;
    switch (ts.TableStatus()) {
      case CheckResult::HEALTHY:
        status = "HEALTHY";
        break;
      case CheckResult::RECOVERING:
        status = "RECOVERING";
        break;
      case CheckResult::UNDER_REPLICATED:
        status = "UNDER-REPLICATED";
        break;
      default:
        status = "UNAVAILABLE";
        break;
    }
    table.AddRow({ ts.name, status, to_string(ts.TotalTablets()),
                   to_string(ts.healthy_tablets), to_string(ts.recovering_tablets),
                   to_string(ts.underreplicated_tablets),
                   to_string(ts.consensus_mismatch_tablets + ts.unavailable_tablets) });
  }
  return table.PrintTo(out);
}

Status Ksck::CheckTablesConsistency() {
  int bad_tables_count = 0;
  vector<TableSummary> table_summaries;
  for (const shared_ptr<KsckTable> &table : cluster_->tables()) {
    if (!MatchesAnyPattern(table_filters_, table->name())) {
      VLOG(1) << "Skipping table " << table->name();
      continue;
    }
    TableSummary ts;
    ts.name = table->name();
    if (!VerifyTable(table, &ts)) {
      bad_tables_count++;
    }
    table_summaries.emplace_back(std::move(ts));
    Out() << endl;
  }

  if (table_summaries.empty()) {
    Out() << "The cluster doesn't have any matching tables" << endl;
    return Status::OK();
  }

  // Show unhealthy tablets at the bottom so they're easier to see;
  // otherwise sort alphabetically.
  std::sort(table_summaries.begin(), table_summaries.end(),
            [](const TableSummary& left, const TableSummary& right) {
              return std::make_pair(left.TableStatus() != CheckResult::HEALTHY, left.name) <
                     std::make_pair(right.TableStatus() != CheckResult::HEALTHY, right.name);
            });
  CHECK_OK(PrintTableSummaries(table_summaries, Out()));

  if (bad_tables_count == 0) {
    Out() << Substitute("The metadata for $0 table(s) is HEALTHY", table_summaries.size()) << endl;
    return Status::OK();
  }
  return Status::Corruption(Substitute("$0 out of $1 table(s) are not healthy",
                                       bad_tables_count, table_summaries.size()));
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
  // whichever comes first. Progress messages are printed to 'out'.
  // Returns false if the timeout expired before all responses came in.
  // Otherwise, returns true.
  bool WaitFor(const MonoDelta& timeout, std::ostream* out) const {
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
      (*out) << "Checksum " << status << run_time_sec << "s: "
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
typedef shared_ptr<BlockingQueue<std::pair<Schema, std::string>>> SharedTabletQueue;

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

  bool timed_out = !reporter->WaitFor(options.timeout, out_);

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

bool Ksck::VerifyTable(const shared_ptr<KsckTable>& table, TableSummary* ts) {
  const auto all_tablets = table->tablets();
  vector<shared_ptr<KsckTablet>> tablets;
  std::copy_if(all_tablets.begin(), all_tablets.end(), std::back_inserter(tablets),
                 [&](const shared_ptr<KsckTablet>& t) {
                   return MatchesAnyPattern(tablet_id_filters_, t->id());
                 });

  int table_num_replicas = table->num_replicas();
  VLOG(1) << Substitute("Verifying $0 tablet(s) for table $1 configured with num_replicas = $2",
                        tablets.size(), table->name(), table_num_replicas);
  for (const auto& tablet : tablets) {
    auto tablet_result = VerifyTablet(tablet, table_num_replicas);
    switch (tablet_result) {
      case CheckResult::HEALTHY:
        ts->healthy_tablets++;
        break;
      case CheckResult::RECOVERING:
        ts->recovering_tablets++;
        break;
      case CheckResult::UNDER_REPLICATED:
        ts->underreplicated_tablets++;
        break;
      case CheckResult::CONSENSUS_MISMATCH:
        ts->consensus_mismatch_tablets++;
        break;
      case CheckResult::UNAVAILABLE:
        ts->unavailable_tablets++;
        break;
    }
  }
  if (ts->healthy_tablets == tablets.size()) {
    Out() << Substitute("Table $0 is $1 ($2 tablet(s) checked)",
                        table->name(),
                        Color(AnsiCode::GREEN, "HEALTHY"),
                        tablets.size()) << endl;
    return true;
  }
  if (ts->recovering_tablets > 0) {
    Out() << Substitute("Table $0 has $1 $2 tablet(s)",
                      table->name(),
                      ts->recovering_tablets,
                      Color(AnsiCode::YELLOW, "recovering")) << endl;
  }
  if (ts->underreplicated_tablets > 0) {
    Out() << Substitute("Table $0 has $1 $2 tablet(s)",
                        table->name(),
                        ts->underreplicated_tablets,
                        Color(AnsiCode::YELLOW, "under-replicated")) << endl;
  }
  if (ts->consensus_mismatch_tablets > 0) {
    Out() << Substitute("Table $0 has $1 tablet(s) $2",
                        table->name(),
                        ts->consensus_mismatch_tablets,
                        Color(AnsiCode::YELLOW, "with mismatched consensus")) << endl;
  }
  if (ts->unavailable_tablets > 0) {
    Out() << Substitute("Table $0 has $1 $2 tablet(s)",
                        table->name(),
                        ts->unavailable_tablets,
                        Color(AnsiCode::RED, "unavailable")) << endl;
  }
  return false;
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
  vector<string> voter_uuids_from_master;
  vector<string> non_voter_uuids_from_master;
  for (const auto& replica : tablet->replicas()) {
    if (replica->is_voter()) {
      voter_uuids_from_master.push_back(replica->ts_uuid());
    } else {
      non_voter_uuids_from_master.push_back(replica->ts_uuid());
    }
  }
  KsckConsensusState master_config(KsckConsensusConfigType::MASTER,
                                   boost::none,
                                   boost::none,
                                   leader_uuid,
                                   voter_uuids_from_master,
                                   non_voter_uuids_from_master);
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
        KsckConsensusState ksck_cstate;
        BuildKsckConsensusStateForConfigMember(cstate, &ksck_cstate);
        repl_info->consensus_state = std::move(ksck_cstate);
      }
    }
  }

  // Summarize the states.
  int leaders_count = 0;
  int running_voters_count = 0;
  int copying_replicas_count = 0;
  for (const auto& r : replica_infos) {
    if (r.replica->is_leader()) {
      leaders_count++;
    }
    if (r.state == tablet::RUNNING && r.replica->is_voter()) {
      running_voters_count++;
    } else if (r.status_pb && r.status_pb->tablet_data_state() == tablet::TABLET_DATA_COPYING) {
      copying_replicas_count++;
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
  CheckResult result = CheckResult::HEALTHY;
  int num_voters = std::accumulate(replica_infos.begin(), replica_infos.end(),
                                   0, [](int sum, const ReplicaInfo& info) {
      return sum + (info.replica->is_voter() ? 1 : 0);
    });
  int majority_size = consensus::MajoritySize(num_voters);
  if (copying_replicas_count > 0) {
    Out() << Substitute("$0 is $1: $2 on-going tablet copies",
                        tablet_str,
                        Color(AnsiCode::YELLOW, "recovering"),
                        copying_replicas_count) << endl;
    result = CheckResult::RECOVERING;
  } else if (running_voters_count < majority_size) {
    Out() << Substitute("$0 is $1: $2 replica(s) not RUNNING",
                        tablet_str,
                        Color(AnsiCode::RED, "unavailable"),
                        num_voters - running_voters_count) << endl;
    result = CheckResult::UNAVAILABLE;
  } else if (running_voters_count < num_voters) {
    Out() << Substitute("$0 is $1: $2 replica(s) not RUNNING",
                        tablet_str,
                        Color(AnsiCode::YELLOW, "under-replicated"),
                        num_voters - running_voters_count) << endl;
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
  // to make it easy to debug. Also, do that if verbose output is requested.
  if (result != CheckResult::HEALTHY || FLAGS_verbose) {
    for (const ReplicaInfo& r : replica_infos) {
      string ts_str = r.ts ? r.ts->ToString() : r.replica->ts_uuid();
      const char* spec_str = r.replica->is_leader()
          ? " [LEADER]" : (!r.replica->is_voter() ? " [NONVOTER]" : "");

      Out() << "  " << ts_str << ": ";
      if (!r.ts || !r.ts->is_healthy()) {
        Out() << Color(AnsiCode::YELLOW, "TS unavailable") << spec_str << endl;
        continue;
      }
      if (r.state == tablet::RUNNING) {
        Out() << Color(AnsiCode::GREEN, "RUNNING") << spec_str << endl;
        continue;
      }
      if (r.status_pb == boost::none) {
        Out() << Color(AnsiCode::YELLOW, "missing") << spec_str << endl;
        continue;
      }

      Out() << Color(AnsiCode::YELLOW, "not running") << spec_str << endl;
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

  // If there are consensus conflicts, dump the consensus info too. Do that also
  // if verbose output is requested.
  if (conflicting_states > 0 || FLAGS_verbose) {
    if (result != CheckResult::CONSENSUS_MISMATCH) {
      Out() << Substitute("$0 replicas' active configs differ from the master's.",
                          conflicting_states)
            << endl;
    }
    map<string, char> replica_uuid_mapping;
    AddToUuidLabelMapping(master_config.voter_uuids, &replica_uuid_mapping);
    AddToUuidLabelMapping(master_config.non_voter_uuids, &replica_uuid_mapping);
    for (const ReplicaInfo& rs : replica_infos) {
      if (!rs.consensus_state) continue;
      AddToUuidLabelMapping(rs.consensus_state->voter_uuids, &replica_uuid_mapping);
      AddToUuidLabelMapping(rs.consensus_state->non_voter_uuids, &replica_uuid_mapping);
    }

    Out() << "  All the peers reported by the master and tablet servers are:" << endl;
    for (const auto& entry : replica_uuid_mapping) {
      Out() << "  " << entry.second << " = " << entry.first << endl;
    }
    Out() << endl;
    Out() << "The consensus matrix is:" << endl;

    // Prepare the header and columns for PrintTable.
    DataTable table({});

    // Seed the columns with the master info.
    vector<string> sources{"master"};
    vector<string> replicas{format_replicas(replica_uuid_mapping, master_config)};
    vector<string> terms{""};
    vector<string> indexes{""};
    vector<string> committed{"Yes"};

    // Fill out the columns with info from the replicas.
    for (const auto& replica_info : replica_infos) {
      char label = FindOrDie(replica_uuid_mapping, replica_info.replica->ts_uuid());
      sources.emplace_back(1, label);
      if (!replica_info.consensus_state) {
        replicas.emplace_back("[config not available]");
        terms.emplace_back("");
        indexes.emplace_back("");
        committed.emplace_back("");
        continue;
      }
      replicas.push_back(format_replicas(replica_uuid_mapping, replica_info.consensus_state.get()));
      terms.push_back(replica_info.consensus_state->term ?
                      std::to_string(replica_info.consensus_state->term.get()) : "");
      indexes.push_back(replica_info.consensus_state->opid_index ?
                        std::to_string(replica_info.consensus_state->opid_index.get()) : "");
      committed.emplace_back(
          replica_info.consensus_state->type == KsckConsensusConfigType::PENDING ? "No" : "Yes");
    }
    table.AddColumn("Config source", std::move(sources));
    table.AddColumn("Replicas", std::move(replicas));
    table.AddColumn("Current term", std::move(terms));
    table.AddColumn("Config index", std::move(indexes));
    table.AddColumn("Committed?", std::move(committed));
    CHECK_OK(table.PrintTo(Out()));
  }

  return result;
}

} // namespace tools
} // namespace kudu
