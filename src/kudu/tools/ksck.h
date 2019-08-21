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
//
// Ksck, a tool to run a Kudu System Check.

#ifndef KUDU_TOOLS_KSCK_H
#define KUDU_TOOLS_KSCK_H

#include <atomic>
#include <cstdint>
#include <iosfwd>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/common/schema.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rebalance/cluster_status.h" // IWYU pragma: keep
#include "kudu/server/server_base.pb.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"  // IWYU pragma: keep
#include "kudu/tools/ksck_results.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace kudu {

class MonoDelta;

namespace rpc {
class Messenger;
} // namespace rpc

namespace tools {

class KsckChecksumManager;
class KsckTable;
struct KsckChecksumOptions;

// Representation of a tablet replica on a tablet server.
class KsckTabletReplica {
 public:
  KsckTabletReplica(std::string ts_uuid, bool is_leader, bool is_voter)
      : ts_uuid_(std::move(ts_uuid)),
        is_leader_(is_leader),
        is_voter_(is_voter) {
  }

  const std::string& ts_uuid() const {
    return ts_uuid_;
  }

  bool is_leader() const {
    return is_leader_;
  }

  bool is_voter() const {
    return is_voter_;
  }

 private:
  const std::string ts_uuid_;
  const bool is_leader_;
  const bool is_voter_;

  DISALLOW_COPY_AND_ASSIGN(KsckTabletReplica);
};

// Representation of a tablet belonging to a table. The tablet is composed of replicas.
class KsckTablet {
 public:
  KsckTablet(KsckTable* table, std::string id)
      : id_(std::move(id)),
        table_(table) {
  }

  const std::string& id() const {
    return id_;
  }

  const std::vector<std::shared_ptr<KsckTabletReplica>>& replicas() const {
    return replicas_;
  }

  void set_replicas(std::vector<std::shared_ptr<KsckTabletReplica>> replicas) {
    replicas_.swap(replicas);
  }

  KsckTable* table() {
    return table_;
  }

 private:
  const std::string id_;
  std::vector<std::shared_ptr<KsckTabletReplica>> replicas_;
  KsckTable* table_;
  DISALLOW_COPY_AND_ASSIGN(KsckTablet);
};

// Representation of a table. Composed of tablets.
class KsckTable {
 public:
  KsckTable(std::string id, std::string name, Schema schema, int num_replicas)
      : id_(std::move(id)),
        name_(std::move(name)),
        schema_(std::move(schema)),
        num_replicas_(num_replicas) {
  }

  const std::string& id() const {
    return id_;
  }

  const std::string& name() const {
    return name_;
  }

  const Schema& schema() const {
    return schema_;
  }

  int num_replicas() const {
    return num_replicas_;
  }

  void set_tablets(std::vector<std::shared_ptr<KsckTablet>> tablets) {
    tablets_ = std::move(tablets);
  }

  const std::vector<std::shared_ptr<KsckTablet>>& tablets() const {
    return tablets_;
  }

 private:
  const std::string id_;
  const std::string name_;
  const Schema schema_;
  const int num_replicas_;
  std::vector<std::shared_ptr<KsckTablet>> tablets_;
  DISALLOW_COPY_AND_ASSIGN(KsckTable);
};

// Enum representing the fetch status of a ksck master or tablet server.
enum class KsckFetchState {
  // Information has not yet been fetched.
  UNINITIALIZED,
  // The attempt to fetch information failed.
  FETCH_FAILED,
  // Information was fetched successfully.
  FETCHED,
};

// Required for logging in case of CHECK failures.
std::ostream& operator<<(std::ostream& lhs, KsckFetchState state);

// The following three classes must be extended in order to communicate with their respective
// components. The two main use cases envisioned for this are:
// - To be able to mock a cluster to more easily test the ksck checks.
// - To be able to communicate with a real Kudu cluster.

// Class that must be extended to represent a master.
class KsckMaster {
 public:
  explicit KsckMaster(std::string address) :
    address_(std::move(address)),
    uuid_(strings::Substitute("$0 ($1)", kDummyUuid, address_)) {}

  virtual ~KsckMaster() = default;

  virtual Status Init() = 0;

  // Connects to the master, checking if it is healthy and gathering basic info.
  virtual Status FetchInfo() = 0;

  // Connects to the master and populates the consensus map.
  virtual Status FetchConsensusState() = 0;

  // Retrieves "unusual" flags from the KsckMaster.
  // "Unusual" flags ares ones tagged hidden, experimental, or unsafe.
  virtual Status FetchUnusualFlags() = 0;

  // Since masters are provided by address, FetchInfo() must be called before
  // calling this method.
  virtual const std::string& uuid() const {
    CHECK_NE(KsckFetchState::UNINITIALIZED, state_);
    return uuid_;
  }

  virtual const std::string& address() const {
    return address_;
  }

  virtual const boost::optional<std::string>& version() const {
    CHECK_NE(KsckFetchState::UNINITIALIZED, state_);
    return version_;
  }

  virtual const boost::optional<consensus::ConsensusStatePB> cstate() const {
    CHECK_NE(KsckFetchState::UNINITIALIZED, state_);
    return cstate_;
  }

  virtual const boost::optional<server::GetFlagsResponsePB>& flags() const {
    CHECK_NE(KsckFetchState::UNINITIALIZED, flags_state_);
    return flags_;
  }

  std::string ToString() const {
    return strings::Substitute("$0 ($1)", uuid(), address());
  }

  bool is_healthy() const {
    CHECK_NE(KsckFetchState::UNINITIALIZED, state_);
    return state_ == KsckFetchState::FETCHED;
  }

  // Masters that haven't been fetched from or that were unavailable have a
  // dummy uuid.
  static constexpr const char* kDummyUuid = "<unknown>";

 protected:
  friend class KsckTest;

  const std::string address_;
  std::string uuid_;

  // state_ reflects whether the fetch of the core ksck info has been done, and
  // if it succeeded or failed.
  KsckFetchState state_ = KsckFetchState::UNINITIALIZED;

  // flags_state_ reflects whether the fetch of the non-critical flags info has
  // been done, and if it succeeded or failed.
  KsckFetchState flags_state_ = KsckFetchState::UNINITIALIZED;

  // May be none if fetching info from the master fails.
  boost::optional<std::string> version_;

  // May be none if consensus state fetch fails.
  boost::optional<consensus::ConsensusStatePB> cstate_;

  // May be none if flag fetch fails.
  boost::optional<server::GetFlagsResponsePB> flags_;

 private:
  DISALLOW_COPY_AND_ASSIGN(KsckMaster);
};

// Class that must be extended to represent a tablet server.
class KsckTabletServer {
 public:
  // Map from tablet id to tablet replicas.
  typedef std::unordered_map<std::string, tablet::TabletStatusPB> TabletStatusMap;

  // Map from (tserver id, tablet id) to tablet consensus information.
  typedef std::map
      <std::pair<std::string, std::string>, consensus::ConsensusStatePB> TabletConsensusStateMap;

  explicit KsckTabletServer(std::string uuid, std::string location = "")
      : uuid_(std::move(uuid)),
        location_(std::move(location)) {}

  virtual ~KsckTabletServer() { }

  // Connects to the configured tablet server and populates the fields of this class. 'health' must
  // not be nullptr.
  //
  // If Status is OK, 'health' will be HEALTHY
  // If the UUID is not what ksck expects, 'health' will be WRONG_SERVER_UUID
  // Otherwise 'health' will be UNAVAILABLE
  virtual Status FetchInfo(cluster_summary::ServerHealth* health) = 0;

  // Connects to the configured tablet server and populates the consensus map. 'health' must not be
  // nullptr.
  //
  // If Status is OK, 'health' will be HEALTHY
  // Otherwise 'health' will be UNAVAILABLE
  virtual Status FetchConsensusState(cluster_summary::ServerHealth* health) = 0;

  // Retrieves "unusual" flags from the KsckTabletServer.
  // "Unusual" flags ares ones tagged hidden, experimental, or unsafe.
  virtual Status FetchUnusualFlags() = 0;

  // Fetches and updates the current timestamp from the tablet server.
  virtual void FetchCurrentTimestampAsync() = 0;
  virtual Status FetchCurrentTimestamp() = 0;

  // Executes a checksum scan on a tablet and reports the result to 'manager'.
  virtual void RunTabletChecksumScanAsync(
                  const std::string& tablet_id,
                  const Schema& schema,
                  const KsckChecksumOptions& options,
                  std::shared_ptr<KsckChecksumManager> manager) = 0;

  virtual const std::string& uuid() const {
    return uuid_;
  }

  virtual const std::string& location() const {
    return location_;
  }

  std::string ToString() const {
    return strings::Substitute("$0 ($1)", uuid(), address());
  }

  virtual std::string address() const = 0;

  bool is_healthy() const {
    CHECK_NE(KsckFetchState::UNINITIALIZED, state_);
    return state_ == KsckFetchState::FETCHED;
  }

  // Gets the mapping of tablet id to tablet replica for this tablet server.
  const TabletStatusMap& tablet_status_map() const {
    CHECK_EQ(KsckFetchState::FETCHED, state_);
    return tablet_status_map_;
  }

  // Gets the mapping of tablet id to tablet consensus info for this tablet server.
  const TabletConsensusStateMap& tablet_consensus_state_map() const {
    CHECK_EQ(KsckFetchState::FETCHED, state_);
    return tablet_consensus_state_map_;
  }

  tablet::TabletStatePB ReplicaState(const std::string& tablet_id) const;

  virtual const boost::optional<std::string>& version() const {
    CHECK_NE(KsckFetchState::UNINITIALIZED, state_);
    return version_;
  }

  virtual const boost::optional<server::GetFlagsResponsePB>& flags() const {
    CHECK_NE(KsckFetchState::UNINITIALIZED, flags_state_);
    return flags_;
  }

  uint64_t current_timestamp() const {
    CHECK_EQ(KsckFetchState::FETCHED, state_);
    return timestamp_;
  }

 protected:
  friend class KsckTest;
  FRIEND_TEST(KsckTest, TestConsensusConflictExtraPeer);
  FRIEND_TEST(KsckTest, TestConsensusConflictDifferentLeader);
  FRIEND_TEST(KsckTest, TestConsensusConflictMissingPeer);
  FRIEND_TEST(KsckTest, TestMasterNotReportingTabletServerWithConsensusConflict);
  FRIEND_TEST(KsckTest, TestMismatchedAssignments);
  FRIEND_TEST(KsckTest, TestTabletCopying);

  // state_ reflects whether the fetch of the core info has been done, and if
  // it succeeded or failed.
  KsckFetchState state_ = KsckFetchState::UNINITIALIZED;

  // flags_state_ reflects whether the fetch of the non-critical flags info has
  // been done, and if it succeeded or failed.
  KsckFetchState flags_state_ = KsckFetchState::UNINITIALIZED;

  TabletStatusMap tablet_status_map_;
  TabletConsensusStateMap tablet_consensus_state_map_;

  // May be none if fetching info from the tablet server fails.
  boost::optional<std::string> version_;

  // May be none if flag fetch fails.
  boost::optional<server::GetFlagsResponsePB> flags_;
  std::atomic<uint64_t> timestamp_;
  const std::string uuid_;
  std::string location_;

 private:
  DISALLOW_COPY_AND_ASSIGN(KsckTabletServer);
};

// Class used to communicate with a cluster.
class KsckCluster {
 public:
  virtual ~KsckCluster() = default;

  // A list of masters.
  typedef std::vector<std::shared_ptr<KsckMaster>> MasterList;

  // Map of KsckTabletServer objects keyed by tablet server uuid.
  typedef std::map<std::string, std::shared_ptr<KsckTabletServer>> TSMap;

  // Fetches the lists of tables, tablets, and tablet servers from the master.
  Status FetchTableAndTabletInfo() {
    RETURN_NOT_OK(Connect());
    RETURN_NOT_OK(RetrieveTablesList());
    RETURN_NOT_OK(RetrieveTabletServers());
    RETURN_NOT_OK(RetrieveAllTablets());
    return Status::OK();
  }

  // Connects to the cluster (i.e. to the leader master).
  virtual Status Connect() = 0;

  // Fetches the list of tablet servers.
  virtual Status RetrieveTabletServers() = 0;

  // Fetches the list of tables.
  virtual Status RetrieveTablesList() = 0;

  // Fetches all tablets in the cluster.
  virtual Status RetrieveAllTablets() = 0;

  // Fetches the list of tablets for the given table.
  // The table's tablet list is modified only if this method returns OK.
  virtual Status RetrieveTabletsList(const std::shared_ptr<KsckTable>& table) = 0;

  const MasterList& masters() const {
    return masters_;
  }

  const TSMap& tablet_servers() const {
    return tablet_servers_;
  }

  const std::vector<std::shared_ptr<KsckTable>>& tables() const {
    return tables_;
  }

  // Returns a reference to the messenger used by this instance.
  // Returns nullptr if no messenger is used.
  virtual std::shared_ptr<rpc::Messenger> messenger() const {
    return nullptr;
  }

  // Setters for filtering the tables/tablets to be checked.
  // Equivalent to the same functions in class 'Ksck'.
  void set_table_filters(std::vector<std::string> table_names) {
    table_filters_ = std::move(table_names);
  }

  // See above.
  void set_tablet_id_filters(std::vector<std::string> tablet_ids) {
    tablet_id_filters_ = std::move(tablet_ids);
  }

  const std::vector<std::string>& table_filters() const {
    return table_filters_;
  }

  const std::vector<std::string>& tablet_id_filters() const {
    return tablet_id_filters_;
  }

  int filtered_tables_count() const {
    return filtered_tables_count_;
  }

  int filtered_tablets_count() const {
    return filtered_tablets_count_;
  }

 protected:
  KsckCluster() : filtered_tables_count_(0), filtered_tablets_count_(0) {}
  MasterList masters_;
  TSMap tablet_servers_;
  std::vector<std::shared_ptr<KsckTable>> tables_;
  gscoped_ptr<ThreadPool> pool_;

  std::vector<std::string> table_filters_;
  std::vector<std::string> tablet_id_filters_;

  // The count of tables/tablets filtered out.
  // Used to determine whether all tables/tablets have been filtered out.
  std::atomic<int> filtered_tables_count_;
  std::atomic<int> filtered_tablets_count_;

 private:
  DISALLOW_COPY_AND_ASSIGN(KsckCluster);
};

// Externally facing class to run checks against the provided cluster.
class Ksck {
 public:
  explicit Ksck(std::shared_ptr<KsckCluster> cluster,
                std::ostream* out = nullptr);

  ~Ksck() = default;

  // Set whether ksck should verify that each of the tablet's raft configurations
  // has the same number of replicas that is specified by the tablet metadata.
  // (default: true)
  void set_check_replica_count(bool check) {
    check_replica_count_ = check;
  }

  // Setters for filtering the tables/tablets to be checked.
  //
  // Filter strings are glob-style patterns. For example, 'Foo*' matches
  // all tables whose name begins with 'Foo'.
  //
  // If tables is not empty, checks only the named tables.
  // If tablets is not empty, checks only the specified tablet IDs.
  // If both are specified, takes the intersection.
  // If both are empty (unset), all tables and tablets are checked.
  void set_table_filters(std::vector<std::string> table_names) {
    table_filters_ = std::move(table_names);
  }

  // See above.
  void set_tablet_id_filters(std::vector<std::string> tablet_ids) {
    tablet_id_filters_ = std::move(tablet_ids);
  }

  void set_print_sections(const std::vector<std::string>& sections);

  const KsckResults& results() const {
    return results_;
  }

  // Check that all masters are healthy.
  Status CheckMasterHealth();

  // Check that the masters' consensus information is consistent.
  Status CheckMasterConsensus();

  // Check for "unusual" flags on masters.
  // "Unusual" flags are ones tagged hidden, experimental, or unsafe and set
  // to a non-default value.
  // Must first call CheckMasterHealth().
  Status CheckMasterUnusualFlags();

  // Verifies that it can connect to the cluster, i.e. that it can contact a
  // leader master.
  Status CheckClusterRunning();

  // Populates all the cluster table and tablet info from the master.
  // Must first call CheckClusterRunning().
  Status FetchTableAndTabletInfo();

  // Connects to all tablet servers, checks that they are alive, and fetches
  // their current status and tablet information.
  // Must first call FetchTableAndTabletInfo().
  Status FetchInfoFromTabletServers();

  // Check for "unusual" flags on tablet servers.
  // "Unusual" flags are ones tagged hidden, experimental, or unsafe and set
  // to a non-default value.
  // Must first call FetchInfoFromTabletServers().
  Status CheckTabletServerUnusualFlags();

  // Check for version inconsistencies among all servers.
  Status CheckServerVersions();

  // Verifies that all the tablets in all tables matching the filters have
  // enough replicas, and that each tablet's view of the tablet's consensus
  // matches every other tablet's and the master's.
  // Must first call FetchTableAndTabletInfo() and, if doing checks against
  // tablet servers (the default), must first call FetchInfoFromTabletServers().
  Status CheckTablesConsistency();

  // Verifies data checksums on all tablets by doing a scan of the database on each replica.
  // Must first call FetchTableAndTabletInfo().
  Status ChecksumData(const KsckChecksumOptions& opts);

  // Runs all the checks of ksck in the proper order, including checksum scans,
  // if enabled. Returns OK if and only if all checks succeed.
  Status Run();

  // Prints the results of ksck.
  Status PrintResults();

  // Performs all checks and prints the results.
  // Returns OK if and only if the ksck finds the cluster completely healthy,
  // and printing succeeds.
  Status RunAndPrintResults();

 private:
  friend class KsckTest;

  // Accumulate information about flags from a server into a FlagToServersMap and
  // a FlagTagsMap.
  // 'flags_to_server_map' and 'flag_tags_map' must not be null.
  void AddFlagsToFlagMaps(const server::GetFlagsResponsePB& flags,
                          const std::string& server_address,
                          KsckFlagToServersMap* flags_to_servers_map,
                          KsckFlagTagsMap* flag_tags_map);

  bool VerifyTable(const std::shared_ptr<KsckTable>& table);

  bool VerifyTableWithTimeout(const std::shared_ptr<KsckTable>& table,
                              const MonoDelta& timeout,
                              const MonoDelta& retry_interval);

  cluster_summary::HealthCheckResult VerifyTablet(
      const std::shared_ptr<KsckTablet>& tablet,
      int table_num_replicas);

  const std::shared_ptr<KsckCluster> cluster_;
  gscoped_ptr<ThreadPool> pool_;

  bool check_replica_count_ = true;
  std::vector<std::string> table_filters_;
  std::vector<std::string> tablet_id_filters_;

  std::ostream* const out_;

  // The output sections, could be a composite of PrintSections::Values
  int print_sections_flags_ = PrintSections::ALL_SECTIONS;

  KsckResults results_;

  DISALLOW_COPY_AND_ASSIGN(Ksck);
};

} // namespace tools
} // namespace kudu

#endif // KUDU_TOOLS_KSCK_H
