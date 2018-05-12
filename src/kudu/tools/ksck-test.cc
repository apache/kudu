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
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/common/schema.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(checksum_scan);
DECLARE_string(color);

namespace kudu {
namespace tools {

using std::ostringstream;
using std::shared_ptr;
using std::static_pointer_cast;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;
using tablet::TabletDataState;

class MockKsckMaster : public KsckMaster {
 public:
  explicit MockKsckMaster(const string& address, const string& uuid)
      : KsckMaster(address),
        fetch_info_status_(Status::OK()) {
    uuid_ = uuid;
  }

  Status Init() override {
    return Status::OK();
  }

  Status FetchInfo() override {
    if (fetch_info_status_.ok()) {
      state_ = KsckFetchState::FETCHED;
    } else {
      state_ = KsckFetchState::FETCH_FAILED;
    }
    return fetch_info_status_;
  }

  Status FetchConsensusState() override {
    return fetch_cstate_status_;
  }

  // Public because the unit tests mutate these variables directly.
  Status fetch_info_status_;
  Status fetch_cstate_status_;
  using KsckMaster::uuid_;
  using KsckMaster::cstate_;
};

class MockKsckTabletServer : public KsckTabletServer {
 public:
  explicit MockKsckTabletServer(const string& uuid)
      : KsckTabletServer(uuid),
        fetch_info_status_(Status::OK()),
        address_("<mock>") {
  }

  Status FetchInfo() override {
    timestamp_ = 12345;
    if (fetch_info_status_.ok()) {
      state_ = KsckFetchState::FETCHED;
    } else {
      state_ = KsckFetchState::FETCH_FAILED;
    }
    return fetch_info_status_;
  }

  Status FetchConsensusState() override {
    return Status::OK();
  }

  virtual void RunTabletChecksumScanAsync(
      const std::string& /*tablet_id*/,
      const Schema& /*schema*/,
      const ChecksumOptions& /*options*/,
      ChecksumProgressCallbacks* callbacks) OVERRIDE {
    callbacks->Progress(10, 20);
    callbacks->Finished(Status::OK(), 0);
  }

  virtual std::string address() const OVERRIDE {
    return address_;
  }

  // Public because the unit tests mutate this variable directly.
  Status fetch_info_status_;

 private:
  const string address_;
};

class MockKsckCluster : public KsckCluster {
 public:
  MockKsckCluster()
      : fetch_info_status_(Status::OK()) {
  }

  virtual Status Connect() override {
    return fetch_info_status_;
  }

  virtual Status RetrieveTabletServers() override {
    return Status::OK();
  }

  virtual Status RetrieveTablesList() override {
    return Status::OK();
  }

  virtual Status RetrieveTabletsList(const shared_ptr<KsckTable>& table) override {
    return Status::OK();
  }

  // Public because the unit tests mutate these variables directly.
  Status fetch_info_status_;
  using KsckCluster::masters_;
  using KsckCluster::tables_;
  using KsckCluster::tablet_servers_;
};

class KsckTest : public KuduTest {
 public:
  KsckTest()
      : cluster_(new MockKsckCluster()),
        ksck_(new Ksck(cluster_, &err_stream_)) {
    FLAGS_color = "never";

    // Set up the master consensus state.
    consensus::ConsensusStatePB cstate;
    cstate.set_current_term(0);
    cstate.set_leader_uuid("master-id-0");
    for (int i = 0; i < 3; i++) {
      auto* peer = cstate.mutable_committed_config()->add_peers();
      peer->set_member_type(consensus::RaftPeerPB::VOTER);
      peer->set_permanent_uuid(Substitute("master-id-$0", i));
    }

    for (int i = 0; i < 3; i++) {
      const string uuid = Substitute("master-id-$0", i);
      const string addr = Substitute("master-$0", i);
      shared_ptr<MockKsckMaster> master = std::make_shared<MockKsckMaster>(addr, uuid);
      master->cstate_ = cstate;
      cluster_->masters_.push_back(master);
    }

    KsckCluster::TSMap tablet_servers;
    for (int i = 0; i < 3; i++) {
      string name = Substitute("ts-id-$0", i);
      shared_ptr<MockKsckTabletServer> ts(new MockKsckTabletServer(name));
      InsertOrDie(&tablet_servers, ts->uuid(), ts);
    }
    cluster_->tablet_servers_.swap(tablet_servers);
  }

 protected:
  // Returns the expected summary for a table with the given tablet states.
  std::string ExpectedKsckTableSummary(const string& table_name,
                                       int replication_factor,
                                       int healthy_tablets,
                                       int recovering_tablets,
                                       int underreplicated_tablets,
                                       int consensus_mismatch_tablets,
                                       int unavailable_tablets) {
    KsckTableSummary table_summary;
    table_summary.name = table_name;
    table_summary.replication_factor = replication_factor;
    table_summary.healthy_tablets = healthy_tablets;
    table_summary.recovering_tablets = recovering_tablets;
    table_summary.underreplicated_tablets = underreplicated_tablets;
    table_summary.consensus_mismatch_tablets = consensus_mismatch_tablets;
    table_summary.unavailable_tablets = unavailable_tablets;
    std::ostringstream oss;
    PrintTableSummaries({ table_summary }, oss);
    return oss.str();
  }

  void CreateDefaultAssignmentPlan(int tablets_count) {
    SCOPED_CLEANUP({
        // This isn't necessary for correctness, but the tests were all
        // written to expect a reversed order and doing that here is more
        // convenient than rewriting many ASSERTs.
        std::reverse(assignment_plan_.begin(), assignment_plan_.end());
      })
    while (tablets_count > 0) {
      for (const auto& entry : cluster_->tablet_servers_) {
        if (tablets_count-- == 0) return;
        assignment_plan_.push_back(entry.second->uuid());
      }
    }
  }

  void CreateOneTableOneTablet() {
    CreateDefaultAssignmentPlan(1);

    auto table = CreateAndAddTable("test", 1);
    shared_ptr<KsckTablet> tablet(new KsckTablet(table.get(), "tablet-id-1"));
    CreateAndFillTablet(tablet, 1, true, true);
    table->set_tablets({ tablet });
  }

  void CreateOneSmallReplicatedTable(const string& table_name = "test",
                                     const string& tablet_id_prefix = "") {
    int num_replicas = 3;
    int num_tablets = 3;
    CreateDefaultAssignmentPlan(num_replicas * num_tablets);
    auto table = CreateAndAddTable(table_name, num_replicas);

    vector<shared_ptr<KsckTablet>> tablets;
    for (int i = 0; i < num_tablets; i++) {
      shared_ptr<KsckTablet> tablet(new KsckTablet(
          table.get(), Substitute("$0tablet-id-$1", tablet_id_prefix, i)));
      CreateAndFillTablet(tablet, num_replicas, true, true);
      tablets.push_back(tablet);
    }
    table->set_tablets(tablets);
  }

  void CreateOneSmallReplicatedTableWithTabletNotRunning() {
    int num_replicas = 3;
    int num_tablets = 3;
    CreateDefaultAssignmentPlan(num_replicas * num_tablets);
    auto table = CreateAndAddTable("test", num_replicas);

    vector<shared_ptr<KsckTablet>> tablets;
    for (int i = 0; i < num_tablets; i++) {
      shared_ptr<KsckTablet> tablet(new KsckTablet(
          table.get(), Substitute("tablet-id-$0", i)));
      CreateAndFillTablet(tablet, num_replicas, true, i != 0);
      tablets.push_back(tablet);
    }
    table->set_tablets(tablets);
  }

  void CreateOneOneTabletReplicatedBrokenTable() {
    // We're placing only two tablets, the 3rd goes nowhere.
    CreateDefaultAssignmentPlan(2);

    auto table = CreateAndAddTable("test", 3);

    shared_ptr<KsckTablet> tablet(new KsckTablet(table.get(), "tablet-id-1"));
    CreateAndFillTablet(tablet, 2, false, true);
    table->set_tablets({ tablet });
  }

  shared_ptr<KsckTable> CreateAndAddTable(const string& id_and_name, int num_replicas) {
    shared_ptr<KsckTable> table(new KsckTable(id_and_name, id_and_name,
                                              Schema(), num_replicas));
    cluster_->tables_.push_back(table);
    return table;
  }

  void CreateAndFillTablet(shared_ptr<KsckTablet>& tablet, int num_replicas,
                           bool has_leader, bool is_running) {
    {
      vector<shared_ptr<KsckTabletReplica>> replicas;
      if (has_leader) {
        CreateReplicaAndAdd(&replicas, tablet->id(), true, is_running);
        num_replicas--;
      }
      for (int i = 0; i < num_replicas; i++) {
        CreateReplicaAndAdd(&replicas, tablet->id(), false, is_running);
      }
      tablet->set_replicas(std::move(replicas));
    }

    // Set up the consensus state on each tablet server.
    consensus::ConsensusStatePB cstate;
    cstate.set_current_term(0);
    for (const auto& replica : tablet->replicas()) {
      if (replica->is_leader()) {
        cstate.set_leader_uuid(replica->ts_uuid());
      }
      auto* peer = cstate.mutable_committed_config()->add_peers();
      peer->set_member_type(consensus::RaftPeerPB::VOTER);
      peer->set_permanent_uuid(replica->ts_uuid());
    }
    for (const auto& replica : tablet->replicas()) {
      shared_ptr<MockKsckTabletServer> ts =
        static_pointer_cast<MockKsckTabletServer>(cluster_->tablet_servers_.at(replica->ts_uuid()));
      InsertOrDieNoPrint(&ts->tablet_consensus_state_map_,
                         std::make_pair(replica->ts_uuid(), tablet->id()),
                         cstate);
    }
  }

  void CreateReplicaAndAdd(vector<shared_ptr<KsckTabletReplica>>* replicas,
                           const string& tablet_id,
                           bool is_leader,
                           bool is_running) {
    shared_ptr<KsckTabletReplica> replica(
        new KsckTabletReplica(assignment_plan_.back(), is_leader, true));
    shared_ptr<MockKsckTabletServer> ts = static_pointer_cast<MockKsckTabletServer>(
            cluster_->tablet_servers_.at(assignment_plan_.back()));

    assignment_plan_.pop_back();
    replicas->push_back(replica);

    // Add the equivalent replica on the tablet server.
    tablet::TabletStatusPB pb;
    pb.set_tablet_id(tablet_id);
    pb.set_table_name("fake-table");
    pb.set_state(is_running ? tablet::RUNNING : tablet::FAILED);
    pb.set_tablet_data_state(TabletDataState::TABLET_DATA_UNKNOWN);
    InsertOrDie(&ts->tablet_status_map_, tablet_id, pb);
  }

  Status RunKsck() {
    auto c = MakeScopedCleanup([this]() {
        LOG(INFO) << "Ksck output:\n" << err_stream_.str();
      });
    return ksck_->RunAndPrintResults();
  }

  const string KsckResultsToJsonString() {
    ostringstream json_stream;
    ksck_->results().PrintJsonTo(PrintMode::JSON_COMPACT, json_stream);
    return json_stream.str();
  }

  shared_ptr<MockKsckCluster> cluster_;
  shared_ptr<Ksck> ksck_;
  // This is used as a stack. First the unit test is responsible to create a plan to follow, that
  // is the order in which each replica of each tablet will be assigned, starting from the end.
  // So if you have 2 tablets with num_replicas=3 and 3 tablet servers, then to distribute evenly
  // you should have a list that looks like ts1,ts2,ts3,ts3,ts2,ts1 so that the two LEADERS, which
  // are assigned first, end up on ts1 and ts3.
  vector<string> assignment_plan_;

  std::ostringstream err_stream_;
};

// Helpful macros for checking JSON fields vs. expected values.
// In all cases, the meaning of the parameters are as follows:
// 'reader' is the JsonReader that owns the parsed JSON data.
// 'value' is the rapidjson::Value* containing the field, or, if 'field'
// is nullptr, the field itself.
// 'field' is a const char* naming the field of 'value' to check.
// If it is null, the field value is extracted from 'value' directly.
// 'expected' is the expected value.
#define EXPECT_JSON_STRING_FIELD(reader, value, field, expected) do { \
  string actual; \
  ASSERT_OK((reader).ExtractString((value), (field), &actual)); \
  EXPECT_EQ((expected), actual); \
} while (0);

#define EXPECT_JSON_INT_FIELD(reader, value, field, expected) do { \
  int64_t actual; \
  ASSERT_OK((reader).ExtractInt64((value), (field), &actual)); \
  EXPECT_EQ((expected), actual); \
} while (0);

#define EXPECT_JSON_BOOL_FIELD(reader, value, field, expected) do { \
  bool actual; \
  ASSERT_OK((reader).ExtractBool((value), (field), &actual)); \
  EXPECT_EQ((expected), actual); \
} while (0);

#define EXPECT_JSON_FIELD_NOT_PRESENT(reader, value, field) do { \
  int64_t unused; \
  ASSERT_TRUE((reader).ExtractInt64((value), (field), &unused).IsNotFound()); \
} while (0);

// 'array' is a vector<const rapidjson::Value*> into which the array elements
// will be extracted.
// 'exp_size' is the expected size of the vector after extraction.
#define EXTRACT_ARRAY_CHECK_SIZE(reader, value, field, array, exp_size) do { \
  ASSERT_OK((reader).ExtractObjectArray((value), (field), &(array))); \
  ASSERT_EQ(exp_size, (array).size()); \
} while (0);

void CheckJsonVsServerHealthSummaries(const JsonReader& r,
                                      const string& key,
                                      const vector<KsckServerHealthSummary>& summaries) {
  if (summaries.empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), key.c_str());
    return;
  }
  vector<const rapidjson::Value*> health;
  EXTRACT_ARRAY_CHECK_SIZE(r, r.root(), key.c_str(), health, summaries.size());
  for (int i = 0; i < summaries.size(); i++) {
    const auto& summary = summaries[i];
    const auto* server = health[i];
    EXPECT_JSON_STRING_FIELD(r, server, "uuid", summary.uuid);
    EXPECT_JSON_STRING_FIELD(r, server, "address", summary.address);
    EXPECT_JSON_STRING_FIELD(r, server, "health", ServerHealthToString(summary.health));
    EXPECT_JSON_STRING_FIELD(r, server, "status", summary.status.ToString());
  }
}

const string KsckConsensusConfigTypeToString(KsckConsensusConfigType t) {
  switch (t) {
    case KsckConsensusConfigType::COMMITTED:
      return "COMMITTED";
    case KsckConsensusConfigType::PENDING:
      return "PENDING";
    case KsckConsensusConfigType::MASTER:
      return "MASTER";
    default:
      LOG(FATAL) << "unknown KsckConsensusConfigType";
  }
}

void CheckJsonVsConsensusState(const JsonReader& r,
                               const rapidjson::Value* cstate,
                               const string& ref_id,
                               const KsckConsensusState& ref_cstate) {
  EXPECT_JSON_STRING_FIELD(r, cstate, "type",
                           KsckConsensusConfigTypeToString(ref_cstate.type));
  if (ref_cstate.leader_uuid) {
    EXPECT_JSON_STRING_FIELD(r, cstate, "leader_uuid", ref_cstate.leader_uuid);
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, cstate, "leader_uuid");
  }
  if (ref_cstate.term) {
    EXPECT_JSON_INT_FIELD(r, cstate, "term", ref_cstate.term);
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, cstate, "term");
  }
  if (ref_cstate.opid_index) {
    EXPECT_JSON_INT_FIELD(r, cstate, "opid_index", ref_cstate.opid_index);
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, cstate, "opid_index");
  }
  // Check voters.
  if (ref_cstate.voter_uuids.empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, cstate, "voter_uuids");
  } else {
    const vector<string> ref_voter_uuids(ref_cstate.voter_uuids.begin(),
                                         ref_cstate.voter_uuids.end());
    vector<const rapidjson::Value*> voter_uuids;
    EXTRACT_ARRAY_CHECK_SIZE(r, cstate, "voter_uuids",
                             voter_uuids, ref_voter_uuids.size());
    for (int j = 0; j < voter_uuids.size(); j++) {
      EXPECT_JSON_STRING_FIELD(r, voter_uuids[j], nullptr, ref_voter_uuids[j]);
    }
  }
  // Check non-voters.
  if (ref_cstate.non_voter_uuids.empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, cstate, "non_voter_uuids");
  } else {
    const vector<string> ref_non_voter_uuids(ref_cstate.non_voter_uuids.begin(),
                                             ref_cstate.non_voter_uuids.end());
    vector<const rapidjson::Value*> non_voter_uuids;
    EXTRACT_ARRAY_CHECK_SIZE(r, cstate, "nonvoter_uuids",
                             non_voter_uuids, ref_non_voter_uuids.size());
    for (int j = 0; j < non_voter_uuids.size(); j++) {
      EXPECT_JSON_STRING_FIELD(r, non_voter_uuids[j], nullptr, ref_non_voter_uuids[j]);
    }
  }
}

void CheckJsonVsReplicaSummary(const JsonReader& r,
                               const rapidjson::Value* replica,
                               const KsckReplicaSummary& ref_replica) {
  EXPECT_JSON_STRING_FIELD(r, replica, "ts_uuid", ref_replica.ts_uuid);
  if (ref_replica.ts_address) {
    EXPECT_JSON_STRING_FIELD(r, replica, "ts_address", ref_replica.ts_address);
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, replica, "ts_address");
  }
  EXPECT_JSON_BOOL_FIELD(r, replica, "is_leader", ref_replica.is_leader);
  EXPECT_JSON_BOOL_FIELD(r, replica, "is_voter", ref_replica.is_voter);
  EXPECT_JSON_BOOL_FIELD(r, replica, "ts_healthy", ref_replica.ts_healthy);
  EXPECT_JSON_STRING_FIELD(r, replica, "state", tablet::TabletStatePB_Name(ref_replica.state));
  // The only thing ksck expects from the status_pb is the data state,
  // so it's all we check (even though the other info is nice to have).
  if (ref_replica.status_pb) {
    const rapidjson::Value* status_pb;
    ASSERT_OK(r.ExtractObject(replica, "status_pb", &status_pb));
    EXPECT_JSON_STRING_FIELD(
        r,
        status_pb,
        "tablet_data_state",
        tablet::TabletDataState_Name(ref_replica.status_pb->tablet_data_state()));
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, replica, "status_pb");
  }
  if (ref_replica.consensus_state) {
    const rapidjson::Value* cstate;
    ASSERT_OK(r.ExtractObject(replica, "consensus_state", &cstate));
    CheckJsonVsConsensusState(r, cstate, ref_replica.ts_uuid, *ref_replica.consensus_state);
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, replica, "consensus_state");
  }
}

void CheckJsonVsMasterConsensus(const JsonReader& r,
                                bool ref_conflict,
                                const KsckConsensusStateMap& ref_cstates) {
  if (ref_cstates.empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), "master_consensus_states");
    return;
  }
  EXPECT_JSON_BOOL_FIELD(r, r.root(), "master_consensus_conflict", ref_conflict);
  vector<const rapidjson::Value*> cstates;
  EXTRACT_ARRAY_CHECK_SIZE(r, r.root(), "master_consensus_states",
                           cstates, ref_cstates.size());
  int i = 0;
  for (const auto& entry : ref_cstates) {
    CheckJsonVsConsensusState(r, cstates[i++], entry.first, entry.second);
  }
}

void CheckJsonVsTableSummaries(const JsonReader& r,
                               const string& key,
                               const vector<KsckTableSummary>& ref_tables) {
  if (ref_tables.empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), key.c_str());
    return;
  }
  vector<const rapidjson::Value*> tables;
  EXTRACT_ARRAY_CHECK_SIZE(r, r.root(), key.c_str(), tables, ref_tables.size());
  for (int i = 0; i < ref_tables.size(); i++) {
    const auto& ref_table = ref_tables[i];
    const auto* table = tables[i];
    EXPECT_JSON_STRING_FIELD(r, table, "id", ref_table.id);
    EXPECT_JSON_STRING_FIELD(r, table, "name", ref_table.name);
    EXPECT_JSON_STRING_FIELD(r, table,
                             "health", KsckCheckResultToString(ref_table.TableStatus()));
    EXPECT_JSON_INT_FIELD(r, table,
                          "replication_factor", ref_table.replication_factor);
    EXPECT_JSON_INT_FIELD(r, table,
                          "total_tablets", ref_table.TotalTablets());
    EXPECT_JSON_INT_FIELD(r, table,
                          "healthy_tablets", ref_table.healthy_tablets);
    EXPECT_JSON_INT_FIELD(r, table,
                          "recovering_tablets", ref_table.recovering_tablets);
    EXPECT_JSON_INT_FIELD(r, table,
                          "underreplicated_tablets", ref_table.underreplicated_tablets);
    EXPECT_JSON_INT_FIELD(r, table,
                          "unavailable_tablets", ref_table.unavailable_tablets);
    EXPECT_JSON_INT_FIELD(r, table,
                          "consensus_mismatch_tablets", ref_table.consensus_mismatch_tablets);
  }
}

void CheckJsonVsTabletSummaries(const JsonReader& r,
                                const string& key,
                                const vector<KsckTabletSummary>& ref_tablets) {
  if (ref_tablets.empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), key.c_str());
    return;
  }
  vector<const rapidjson::Value*> tablets;
  EXTRACT_ARRAY_CHECK_SIZE(r, r.root(), key.c_str(), tablets, ref_tablets.size());
  for (int i = 0; i < ref_tablets.size(); i++) {
    const auto& ref_tablet = ref_tablets[i];
    const auto& tablet = tablets[i];
    EXPECT_JSON_STRING_FIELD(r, tablet, "id", ref_tablet.id);
    EXPECT_JSON_STRING_FIELD(r, tablet, "table_id", ref_tablet.table_id);
    EXPECT_JSON_STRING_FIELD(r, tablet, "table_name", ref_tablet.table_name);
    EXPECT_JSON_STRING_FIELD(r, tablet,
                             "health", KsckCheckResultToString(ref_tablet.result));
    EXPECT_JSON_STRING_FIELD(r, tablet, "status", ref_tablet.status);
    const rapidjson::Value* master_cstate;
    ASSERT_OK(r.ExtractObject(tablet, "master_cstate", &master_cstate));
    CheckJsonVsConsensusState(r, master_cstate, "master", ref_tablet.master_cstate);
    if (ref_tablet.replicas.empty()) {
      EXPECT_JSON_FIELD_NOT_PRESENT(r, tablet, "replicas");
      continue;
    }
    vector<const rapidjson::Value*> replicas;
    EXTRACT_ARRAY_CHECK_SIZE(r, tablet,
                             "replicas", replicas, ref_tablet.replicas.size());
    for (int j = 0; j < replicas.size(); j++) {
      const auto& ref_replica = ref_tablet.replicas[j];
      const auto* replica = replicas[j];
      CheckJsonVsReplicaSummary(r, replica, ref_replica);
    }
  }
}

void CheckJsonVsChecksumResults(const JsonReader& r,
                                const string& key,
                                const KsckChecksumResults& ref_checksum_results) {
  if (ref_checksum_results.tables.empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), key.c_str());
    return;
  }
  const rapidjson::Value* checksum_results;
  ASSERT_OK(r.ExtractObject(r.root(), key.c_str(), &checksum_results));
  if (ref_checksum_results.snapshot_timestamp) {
    EXPECT_JSON_INT_FIELD(r, checksum_results,
                          "snapshot_timestamp", *ref_checksum_results.snapshot_timestamp);
  } else {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, checksum_results, "snapshot_timestamp");
  }
  vector<const rapidjson::Value*> tables;
  EXTRACT_ARRAY_CHECK_SIZE(r, checksum_results, "tables",
                           tables, ref_checksum_results.tables.size());
  int i = 0;
  for (const auto& table_entry : ref_checksum_results.tables) {
    const auto& ref_table = table_entry.second;
    const auto* table = tables[i++];
    EXPECT_JSON_STRING_FIELD(r, table, "name", table_entry.first);
    vector<const rapidjson::Value*> tablets;
    EXTRACT_ARRAY_CHECK_SIZE(r, table, "tablets", tablets, ref_table.size());
    int j = 0;
    for (const auto& tablet_entry : ref_table) {
      const auto& ref_tablet = tablet_entry.second;
      const auto* tablet = tablets[j++];
      EXPECT_JSON_STRING_FIELD(r, tablet, "tablet_id", tablet_entry.first);
      EXPECT_JSON_BOOL_FIELD(r, tablet, "mismatch", ref_tablet.mismatch);
      vector<const rapidjson::Value*> checksums;
      EXTRACT_ARRAY_CHECK_SIZE(r, tablet, "replica_checksums",
                               checksums, ref_tablet.replica_checksums.size());
      int k = 0;
      for (const auto& replica_entry : ref_tablet.replica_checksums) {
        const auto& ref_replica = replica_entry.second;
        const auto* replica = checksums[k++];
        EXPECT_JSON_STRING_FIELD(r, replica, "ts_uuid", ref_replica.ts_uuid);
        EXPECT_JSON_STRING_FIELD(r, replica, "ts_address", ref_replica.ts_address);
        EXPECT_JSON_STRING_FIELD(r, replica, "status", ref_replica.status.ToString());
        // Checksum is a uint64_t and might plausibly be larger than int64_t's max,
        // so we're handling it special.
        int64_t signed_checksum;
        ASSERT_OK(r.ExtractInt64(replica, "checksum", &signed_checksum));
        ASSERT_EQ(ref_replica.checksum, static_cast<uint64_t>(signed_checksum));
      }
    }
  }
}

void CheckJsonVsErrors(const JsonReader& r,
                       const string& key,
                       const vector<Status>& ref_errors) {
  if (ref_errors.empty()) {
    EXPECT_JSON_FIELD_NOT_PRESENT(r, r.root(), key.c_str());
    return;
  }
  vector<const rapidjson::Value*> errors;
  EXTRACT_ARRAY_CHECK_SIZE(r, r.root(), "errors", errors, ref_errors.size());
  for (int i = 0; i < ref_errors.size(); i++) {
    EXPECT_JSON_STRING_FIELD(r, errors[i], nullptr, ref_errors[i].ToString());
  }
}

void CheckJsonStringVsKsckResults(const string& json, const KsckResults& results) {
  JsonReader r(json);
  ASSERT_OK(r.Init());

  CheckJsonVsServerHealthSummaries(r, "master_summaries", results.master_summaries);
  CheckJsonVsMasterConsensus(r,
                             results.master_consensus_conflict,
                             results.master_consensus_state_map);
  CheckJsonVsServerHealthSummaries(r, "tserver_summaries", results.tserver_summaries);
  CheckJsonVsTabletSummaries(r, "tablet_summaries", results.tablet_summaries);;
  CheckJsonVsTableSummaries(r, "table_summaries", results.table_summaries);;
  CheckJsonVsChecksumResults(r, "checksum_results", results.checksum_results);
  CheckJsonVsErrors(r, "errors", results.error_messages);
}

TEST_F(KsckTest, TestServersOk) {
  ASSERT_OK(RunKsck());
  const string err_string = err_stream_.str();
  // Master health.
  ASSERT_STR_CONTAINS(err_string,
    "Master Summary\n"
    "    UUID     | Address  | Status\n"
    "-------------+----------+---------\n"
    " master-id-0 | master-0 | HEALTHY\n"
    " master-id-1 | master-1 | HEALTHY\n"
    " master-id-2 | master-2 | HEALTHY\n");
  // Tablet server health.
  ASSERT_STR_CONTAINS(err_string,
    "Tablet Server Summary\n"
    "  UUID   | Address | Status\n"
    "---------+---------+---------\n"
    " ts-id-0 | <mock>  | HEALTHY\n"
    " ts-id-1 | <mock>  | HEALTHY\n"
    " ts-id-2 | <mock>  | HEALTHY\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestMasterUnavailable) {
  shared_ptr<MockKsckMaster> master =
      std::static_pointer_cast<MockKsckMaster>(cluster_->masters_.at(1));
  master->fetch_info_status_ = Status::NetworkError("gremlins");
  master->cstate_ = boost::none;
  ASSERT_TRUE(ksck_->CheckMasterHealth().IsNetworkError());
  ASSERT_TRUE(ksck_->CheckMasterConsensus().IsCorruption());
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
    "Master Summary\n"
    "    UUID     | Address  |   Status\n"
    "-------------+----------+-------------\n"
    " master-id-0 | master-0 | HEALTHY\n"
    " master-id-2 | master-2 | HEALTHY\n"
    " master-id-1 | master-1 | UNAVAILABLE\n");
  ASSERT_STR_CONTAINS(err_stream_.str(),
    "All reported replicas are:\n"
    "  A = master-id-0\n"
    "  B = master-id-1\n"
    "  C = master-id-2\n"
    "The consensus matrix is:\n"
    " Config source |        Replicas        | Current term | Config index | Committed?\n"
    "---------------+------------------------+--------------+--------------+------------\n"
    " A             | A*  B   C              | 0            |              | Yes\n"
    " B             | [config not available] |              |              | \n"
    " C             | A*  B   C              | 0            |              | Yes\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

// A wrong-master-uuid situation can happen if a master that is part of, e.g.,
// a 3-peer config fails permanently and is wiped and reborn on the same address
// with a new uuid.
TEST_F(KsckTest, TestWrongMasterUuid) {
  shared_ptr<MockKsckMaster> master =
      std::static_pointer_cast<MockKsckMaster>(cluster_->masters_.at(2));
  const string imposter_uuid = "master-id-imposter";
  master->uuid_ = imposter_uuid;
  master->cstate_->set_leader_uuid(imposter_uuid);
  auto* config = master->cstate_->mutable_committed_config();
  config->clear_peers();
  config->add_peers()->set_permanent_uuid(imposter_uuid);

  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_TRUE(ksck_->CheckMasterConsensus().IsCorruption());
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
    "Master Summary\n"
    "        UUID        | Address  | Status\n"
    "--------------------+----------+---------\n"
    " master-id-0        | master-0 | HEALTHY\n"
    " master-id-1        | master-1 | HEALTHY\n"
    " master-id-imposter | master-2 | HEALTHY\n");
  ASSERT_STR_CONTAINS(err_stream_.str(),
    "All reported replicas are:\n"
    "  A = master-id-0\n"
    "  B = master-id-1\n"
    "  C = master-id-imposter\n"
    "  D = master-id-2\n"
    "The consensus matrix is:\n"
    " Config source |     Replicas     | Current term | Config index | Committed?\n"
    "---------------+------------------+--------------+--------------+------------\n"
    " A             | A*  B       D    | 0            |              | Yes\n"
    " B             | A*  B       D    | 0            |              | Yes\n"
    " C             |         C*       | 0            |              | Yes\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestTwoLeaderMasters) {
  shared_ptr<MockKsckMaster> master =
      std::static_pointer_cast<MockKsckMaster>(cluster_->masters_.at(1));
  master->cstate_->set_leader_uuid(master->uuid_);

  ASSERT_OK(ksck_->CheckMasterHealth());
  ASSERT_TRUE(ksck_->CheckMasterConsensus().IsCorruption());
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
    "All reported replicas are:\n"
    "  A = master-id-0\n"
    "  B = master-id-1\n"
    "  C = master-id-2\n"
    "The consensus matrix is:\n"
    " Config source |   Replicas   | Current term | Config index | Committed?\n"
    "---------------+--------------+--------------+--------------+------------\n"
    " A             | A*  B   C    | 0            |              | Yes\n"
    " B             | A   B*  C    | 0            |              | Yes\n"
    " C             | A*  B   C    | 0            |              | Yes\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestLeaderMasterUnavailable) {
  Status error = Status::NetworkError("Network failure");
  cluster_->fetch_info_status_ = error;
  ASSERT_TRUE(ksck_->CheckClusterRunning().IsNetworkError());
  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestWrongUUIDTabletServer) {
  CreateOneTableOneTablet();

  Status error = Status::RemoteError("ID reported by tablet server "
                                     "doesn't match the expected ID");
  static_pointer_cast<MockKsckTabletServer>(cluster_->tablet_servers_["ts-id-1"])
    ->fetch_info_status_ = error;

  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  ASSERT_TRUE(ksck_->FetchInfoFromTabletServers().IsNetworkError());
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
    "Tablet Server Summary\n"
    "  UUID   | Address |      Status\n"
    "---------+---------+-------------------\n"
    " ts-id-0 | <mock>  | HEALTHY\n"
    " ts-id-2 | <mock>  | HEALTHY\n"
    " ts-id-1 | <mock>  | WRONG_SERVER_UUID\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestBadTabletServer) {
  CreateOneSmallReplicatedTable();

  // Mock a failure to connect to one of the tablet servers.
  Status error = Status::NetworkError("Network failure");
  static_pointer_cast<MockKsckTabletServer>(cluster_->tablet_servers_["ts-id-1"])
      ->fetch_info_status_ = error;

  ASSERT_OK(ksck_->CheckClusterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  Status s = ksck_->FetchInfoFromTabletServers();
  ASSERT_TRUE(s.IsNetworkError()) << "Status returned: " << s.ToString();

  s = ksck_->CheckTablesConsistency();
  EXPECT_EQ("Corruption: 1 out of 1 table(s) are not healthy", s.ToString());
  ASSERT_OK(ksck_->PrintResults());
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "Tablet Server Summary\n"
      "  UUID   | Address |   Status\n"
      "---------+---------+-------------\n"
      " ts-id-0 | <mock>  | HEALTHY\n"
      " ts-id-2 | <mock>  | HEALTHY\n"
      " ts-id-1 | <mock>  | UNAVAILABLE\n");
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "Error from <mock>: Network error: Network failure\n");
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "Tablet tablet-id-0 of table 'test' is under-replicated: 1 replica(s) not RUNNING\n"
      "  ts-id-0 (<mock>): RUNNING [LEADER]\n"
      "  ts-id-1 (<mock>): TS unavailable\n"
      "  ts-id-2 (<mock>): RUNNING\n");
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "Tablet tablet-id-1 of table 'test' is under-replicated: 1 replica(s) not RUNNING\n"
      "  ts-id-0 (<mock>): RUNNING [LEADER]\n"
      "  ts-id-1 (<mock>): TS unavailable\n"
      "  ts-id-2 (<mock>): RUNNING\n");
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "Tablet tablet-id-2 of table 'test' is under-replicated: 1 replica(s) not RUNNING\n"
      "  ts-id-0 (<mock>): RUNNING [LEADER]\n"
      "  ts-id-1 (<mock>): TS unavailable\n"
      "  ts-id-2 (<mock>): RUNNING\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestOneTableCheck) {
  CreateOneTableOneTablet();
  FLAGS_checksum_scan = true;
  ASSERT_OK(RunKsck());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "0/1 replicas remaining (20B from disk, 10 rows summed)");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestOneSmallReplicatedTable) {
  CreateOneSmallReplicatedTable();
  FLAGS_checksum_scan = true;
  ASSERT_OK(RunKsck());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "0/9 replicas remaining (180B from disk, 90 rows summed)");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

// Test filtering on a non-matching table pattern.
TEST_F(KsckTest, TestNonMatchingTableFilter) {
  CreateOneSmallReplicatedTable();
  ksck_->set_table_filters({"xyz"});
  FLAGS_checksum_scan = true;
  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  EXPECT_EQ("Not found: checksum scan error: No table found. Filter: table_filters=xyz",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "The cluster doesn't have any matching tables");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

// Test filtering with a matching table pattern.
TEST_F(KsckTest, TestMatchingTableFilter) {
  CreateOneSmallReplicatedTable();
  ksck_->set_table_filters({"te*"});
  FLAGS_checksum_scan = true;
  ASSERT_OK(RunKsck());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "0/9 replicas remaining (180B from disk, 90 rows summed)");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

// Test filtering on a non-matching tablet id pattern.
TEST_F(KsckTest, TestNonMatchingTabletIdFilter) {
  CreateOneSmallReplicatedTable();
  ksck_->set_tablet_id_filters({"xyz"});
  FLAGS_checksum_scan = true;
  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  EXPECT_EQ(
      "Not found: checksum scan error: No tablet replicas found. Filter: tablet_id_filters=xyz",
      error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "The cluster doesn't have any matching tablets");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

// Test filtering with a matching tablet ID pattern.
TEST_F(KsckTest, TestMatchingTabletIdFilter) {
  CreateOneSmallReplicatedTable();
  ksck_->set_tablet_id_filters({"*-id-2"});
  FLAGS_checksum_scan = true;
  ASSERT_OK(RunKsck());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "0/3 replicas remaining (60B from disk, 30 rows summed)");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestOneSmallReplicatedTableWithConsensusState) {
  CreateOneSmallReplicatedTable();
  ASSERT_OK(RunKsck());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedKsckTableSummary("test",
                                               /*replication_factor=*/ 3,
                                               /*healthy_tablets=*/ 3,
                                               /*recovering_tablets=*/ 0,
                                               /*underreplicated_tablets=*/ 0,
                                               /*consensus_mismatch_tablets=*/ 0,
                                               /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestConsensusConflictExtraPeer) {
  CreateOneSmallReplicatedTable();

  shared_ptr<KsckTabletServer> ts = FindOrDie(cluster_->tablet_servers_, "ts-id-0");
  auto& cstate = FindOrDieNoPrint(ts->tablet_consensus_state_map_,
                                  std::make_pair("ts-id-0", "tablet-id-0"));
  cstate.mutable_committed_config()->add_peers()->set_permanent_uuid("ts-id-fake");

  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "The consensus matrix is:\n"
      " Config source |     Replicas     | Current term | Config index | Committed?\n"
      "---------------+------------------+--------------+--------------+------------\n"
      " master        | A*  B   C        |              |              | Yes\n"
      " A             | A*  B   C   D    | 0            |              | Yes\n"
      " B             | A*  B   C        | 0            |              | Yes\n"
      " C             | A*  B   C        | 0            |              | Yes");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedKsckTableSummary("test",
                                               /*replication_factor=*/ 3,
                                               /*healthy_tablets=*/ 2,
                                               /*recovering_tablets=*/ 0,
                                               /*underreplicated_tablets=*/ 0,
                                               /*consensus_mismatch_tablets=*/ 1,
                                               /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestConsensusConflictMissingPeer) {
  CreateOneSmallReplicatedTable();

  shared_ptr<KsckTabletServer> ts = FindOrDie(cluster_->tablet_servers_, "ts-id-0");
  auto& cstate = FindOrDieNoPrint(ts->tablet_consensus_state_map_,
                                  std::make_pair("ts-id-0", "tablet-id-0"));
  cstate.mutable_committed_config()->mutable_peers()->RemoveLast();

  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "The consensus matrix is:\n"
      " Config source |   Replicas   | Current term | Config index | Committed?\n"
      "---------------+--------------+--------------+--------------+------------\n"
      " master        | A*  B   C    |              |              | Yes\n"
      " A             | A*  B        | 0            |              | Yes\n"
      " B             | A*  B   C    | 0            |              | Yes\n"
      " C             | A*  B   C    | 0            |              | Yes");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedKsckTableSummary("test",
                                               /*replication_factor=*/ 3,
                                               /*healthy_tablets=*/ 2,
                                               /*recovering_tablets=*/ 0,
                                               /*underreplicated_tablets=*/ 0,
                                               /*consensus_mismatch_tablets=*/ 1,
                                               /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestConsensusConflictDifferentLeader) {
  CreateOneSmallReplicatedTable();

  const shared_ptr<KsckTabletServer>& ts = FindOrDie(cluster_->tablet_servers_, "ts-id-0");
  auto& cstate = FindOrDieNoPrint(ts->tablet_consensus_state_map_,
                                  std::make_pair("ts-id-0", "tablet-id-0"));
  cstate.set_leader_uuid("ts-id-1");

  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "The consensus matrix is:\n"
      " Config source |   Replicas   | Current term | Config index | Committed?\n"
      "---------------+--------------+--------------+--------------+------------\n"
      " master        | A*  B   C    |              |              | Yes\n"
      " A             | A   B*  C    | 0            |              | Yes\n"
      " B             | A*  B   C    | 0            |              | Yes\n"
      " C             | A*  B   C    | 0            |              | Yes");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedKsckTableSummary("test",
                                               /*replication_factor=*/ 3,
                                               /*healthy_tablets=*/ 2,
                                               /*recovering_tablets=*/ 0,
                                               /*underreplicated_tablets=*/ 0,
                                               /*consensus_mismatch_tablets=*/ 1,
                                               /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestOneOneTabletBrokenTable) {
  CreateOneOneTabletReplicatedBrokenTable();
  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "Tablet tablet-id-1 of table 'test' is under-replicated: "
                      "configuration has 2 replicas vs desired 3");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedKsckTableSummary("test",
                                               /*replication_factor=*/ 3,
                                               /*healthy_tablets=*/ 0,
                                               /*recovering_tablets=*/ 0,
                                               /*underreplicated_tablets=*/ 1,
                                               /*consensus_mismatch_tablets=*/ 0,
                                               /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestMismatchedAssignments) {
  CreateOneSmallReplicatedTable();
  shared_ptr<MockKsckTabletServer> ts = static_pointer_cast<MockKsckTabletServer>(
      cluster_->tablet_servers_.at(Substitute("ts-id-$0", 0)));
  ASSERT_EQ(1, ts->tablet_status_map_.erase("tablet-id-2"));

  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "Tablet tablet-id-2 of table 'test' is under-replicated: "
                      "1 replica(s) not RUNNING\n"
                      "  ts-id-0 (<mock>): missing [LEADER]\n"
                      "  ts-id-1 (<mock>): RUNNING\n"
                      "  ts-id-2 (<mock>): RUNNING\n");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                     ExpectedKsckTableSummary("test",
                                              /*replication_factor=*/ 3,
                                              /*healthy_tablets=*/ 2,
                                              /*recovering_tablets=*/ 0,
                                              /*underreplicated_tablets=*/ 1,
                                              /*consensus_mismatch_tablets=*/ 0,
                                              /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestTabletNotRunning) {
  CreateOneSmallReplicatedTableWithTabletNotRunning();

  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "Tablet tablet-id-0 of table 'test' is unavailable: 3 replica(s) not RUNNING\n"
      "  ts-id-0 (<mock>): not running [LEADER]\n"
      "    State:       FAILED\n"
      "    Data state:  TABLET_DATA_UNKNOWN\n"
      "    Last status: \n"
      "  ts-id-1 (<mock>): not running\n"
      "    State:       FAILED\n"
      "    Data state:  TABLET_DATA_UNKNOWN\n"
      "    Last status: \n"
      "  ts-id-2 (<mock>): not running\n"
      "    State:       FAILED\n"
      "    Data state:  TABLET_DATA_UNKNOWN\n"
      "    Last status: \n");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedKsckTableSummary("test",
                                               /*replication_factor=*/ 3,
                                               /*healthy_tablets=*/ 2,
                                               /*recovering_tablets=*/ 0,
                                               /*underreplicated_tablets=*/ 0,
                                               /*consensus_mismatch_tablets=*/ 0,
                                               /*unavailable_tablets=*/ 1));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestTabletCopying) {
  CreateOneSmallReplicatedTableWithTabletNotRunning();
  CreateDefaultAssignmentPlan(1);

  // Mark one of the tablet replicas as copying.
  auto not_running_ts = static_pointer_cast<MockKsckTabletServer>(
          cluster_->tablet_servers_.at(assignment_plan_.back()));
  auto& pb = FindOrDie(not_running_ts->tablet_status_map_, "tablet-id-0");
  pb.set_tablet_data_state(TabletDataState::TABLET_DATA_COPYING);
  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedKsckTableSummary("test",
                                               /*replication_factor=*/ 3,
                                               /*healthy_tablets=*/ 2,
                                               /*recovering_tablets=*/ 1,
                                               /*underreplicated_tablets=*/ 0,
                                               /*consensus_mismatch_tablets=*/ 0,
                                               /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

// Test for a bug where we weren't properly handling a tserver not reported by the master.
TEST_F(KsckTest, TestMasterNotReportingTabletServer) {
  CreateOneSmallReplicatedTable();

  // Delete a tablet server from the master's list. This simulates a situation
  // where the master is starting and doesn't list all tablet servers yet, but
  // tablets from other tablet servers are listing a missing tablet server as a peer.
  EraseKeyReturnValuePtr(&cluster_->tablet_servers_, "ts-id-0");
  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedKsckTableSummary("test",
                                               /*replication_factor=*/ 3,
                                               /*healthy_tablets=*/ 0,
                                               /*recovering_tablets=*/ 0,
                                               /*underreplicated_tablets=*/ 3,
                                               /*consensus_mismatch_tablets=*/ 0,
                                               /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

// KUDU-2113: Test for a bug where we weren't properly handling a tserver not
// reported by the master when there was also a consensus conflict.
TEST_F(KsckTest, TestMasterNotReportingTabletServerWithConsensusConflict) {
  CreateOneSmallReplicatedTable();

  // Delete a tablet server from the cluster's list as in TestMasterNotReportingTabletServer.
  EraseKeyReturnValuePtr(&cluster_->tablet_servers_, "ts-id-0");

  // Now engineer a consensus conflict.
  const shared_ptr<KsckTabletServer>& ts = FindOrDie(cluster_->tablet_servers_, "ts-id-1");
  auto& cstate = FindOrDieNoPrint(ts->tablet_consensus_state_map_,
                                  std::make_pair("ts-id-1", "tablet-id-1"));
  cstate.set_leader_uuid("ts-id-1");

  ASSERT_TRUE(RunKsck().IsRuntimeError());
  const vector<Status>& error_messages = ksck_->results().error_messages;
  ASSERT_EQ(1, error_messages.size());
  ASSERT_EQ("Corruption: table consistency check error: 1 out of 1 table(s) are not healthy",
            error_messages[0].ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "The consensus matrix is:\n"
      " Config source |        Replicas        | Current term | Config index | Committed?\n"
      "---------------+------------------------+--------------+--------------+------------\n"
      " master        | A*  B   C              |              |              | Yes\n"
      " A             | [config not available] |              |              | \n"
      " B             | A   B*  C              | 0            |              | Yes\n"
      " C             | A*  B   C              | 0            |              | Yes");
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      ExpectedKsckTableSummary("test",
                                               /*replication_factor=*/ 3,
                                               /*healthy_tablets=*/ 0,
                                               /*recovering_tablets=*/ 0,
                                               /*underreplicated_tablets=*/ 3,
                                               /*consensus_mismatch_tablets=*/ 0,
                                               /*unavailable_tablets=*/ 0));

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestTableFiltersNoMatch) {
  CreateOneSmallReplicatedTable();

  ksck_->set_table_filters({ "fake-table" });

  // Every table we check is healthy ;).
  ASSERT_OK(RunKsck());
  ASSERT_STR_CONTAINS(err_stream_.str(), "The cluster doesn't have any matching tables");
  ASSERT_STR_NOT_CONTAINS(err_stream_.str(),
      "                | Total Count\n"
      "----------------+-------------\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestTableFilters) {
  CreateOneSmallReplicatedTable();
  CreateOneSmallReplicatedTable("other", "other-");

  ksck_->set_table_filters({ "test" });
  ASSERT_OK(RunKsck());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "                | Total Count\n"
      "----------------+-------------\n"
      " Masters        | 3\n"
      " Tablet Servers | 3\n"
      " Tables         | 1\n"
      " Tablets        | 3\n"
      " Replicas       | 9\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestTabletFiltersNoMatch) {
  CreateOneSmallReplicatedTable();

  ksck_->set_tablet_id_filters({ "tablet-id-fake" });

  // Every tablet we check is healthy ;).
  ASSERT_OK(RunKsck());
  ASSERT_STR_CONTAINS(err_stream_.str(), "The cluster doesn't have any matching tablets");
  ASSERT_STR_NOT_CONTAINS(err_stream_.str(),
      "                | Total Count\n"
      "----------------+-------------\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

TEST_F(KsckTest, TestTabletFilters) {
  CreateOneSmallReplicatedTable();

  ksck_->set_tablet_id_filters({ "tablet-id-0", "tablet-id-1" });
  ASSERT_OK(ksck_->RunAndPrintResults());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "                | Total Count\n"
      "----------------+-------------\n"
      " Masters        | 3\n"
      " Tablet Servers | 3\n"
      " Tables         | 1\n"
      " Tablets        | 2\n"
      " Replicas       | 6\n");

  CheckJsonStringVsKsckResults(KsckResultsToJsonString(), ksck_->results());
}

} // namespace tools
} // namespace kudu
