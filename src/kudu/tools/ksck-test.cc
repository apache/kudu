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

#include <iosfwd>
#include <memory>
#include <unordered_map>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/ksck.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/test_util.h"

DECLARE_string(color);
DECLARE_bool(consensus);

namespace kudu {
namespace tools {

using std::shared_ptr;
using std::static_pointer_cast;
using std::string;
using std::unordered_map;
using strings::Substitute;

// Import this symbol from ksck.cc so we can introspect the
// errors being written to stderr.
extern std::ostream* g_err_stream;

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
      state_ = kFetched;
    } else {
      state_ = kFetchFailed;
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

class MockKsckMaster : public KsckMaster {
 public:
  MockKsckMaster()
      : fetch_info_status_(Status::OK()) {
  }

  virtual Status Connect() OVERRIDE {
    return fetch_info_status_;
  }

  virtual Status RetrieveTabletServers(TSMap* tablet_servers) OVERRIDE {
    *tablet_servers = tablet_servers_;
    return Status::OK();
  }

  virtual Status RetrieveTablesList(vector<shared_ptr<KsckTable>>* tables) OVERRIDE {
    tables->assign(tables_.begin(), tables_.end());
    return Status::OK();
  }

  virtual Status RetrieveTabletsList(const shared_ptr<KsckTable>& table) OVERRIDE {
    return Status::OK();
  }

  // Public because the unit tests mutate these variables directly.
  Status fetch_info_status_;
  TSMap tablet_servers_;
  vector<shared_ptr<KsckTable>> tables_;
};

class KsckTest : public KuduTest {
 public:
  KsckTest()
      : master_(new MockKsckMaster()),
        cluster_(new KsckCluster(static_pointer_cast<KsckMaster>(master_))),
        ksck_(new Ksck(cluster_)) {
    FLAGS_color = "never";
    unordered_map<string, shared_ptr<KsckTabletServer>> tablet_servers;
    for (int i = 0; i < 3; i++) {
      string name = Substitute("ts-id-$0", i);
      shared_ptr<MockKsckTabletServer> ts(new MockKsckTabletServer(name));
      InsertOrDie(&tablet_servers, ts->uuid(), ts);
    }
    master_->tablet_servers_.swap(tablet_servers);

    g_err_stream = &err_stream_;
  }

  ~KsckTest() {
    g_err_stream = NULL;
  }

 protected:
  void CreateDefaultAssignmentPlan(int tablets_count) {
    while (tablets_count > 0) {
      for (const KsckMaster::TSMap::value_type& entry : master_->tablet_servers_) {
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

  void CreateOneSmallReplicatedTable() {
    int num_replicas = 3;
    int num_tablets = 3;
    CreateDefaultAssignmentPlan(num_replicas * num_tablets);
    auto table = CreateAndAddTable("test", num_replicas);

    vector<shared_ptr<KsckTablet>> tablets;
    for (int i = 0; i < num_tablets; i++) {
      shared_ptr<KsckTablet> tablet(new KsckTablet(
          table.get(), Substitute("tablet-id-$0", i)));
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

  shared_ptr<KsckTable> CreateAndAddTable(const string& name, int num_replicas) {
    shared_ptr<KsckTable> table(new KsckTable(name, Schema(), num_replicas));
    vector<shared_ptr<KsckTable>> tables = { table };
    master_->tables_.assign(tables.begin(), tables.end());
    return table;
  }

  void CreateAndFillTablet(shared_ptr<KsckTablet>& tablet, int num_replicas,
                           bool has_leader, bool is_running) {
    vector<shared_ptr<KsckTabletReplica>> replicas;
    if (has_leader) {
      CreateReplicaAndAdd(&replicas, tablet->id(), true, is_running);
      num_replicas--;
    }
    for (int i = 0; i < num_replicas; i++) {
      CreateReplicaAndAdd(&replicas, tablet->id(), false, is_running);
    }
    tablet->set_replicas(replicas);

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
        static_pointer_cast<MockKsckTabletServer>(master_->tablet_servers_.at(replica->ts_uuid()));
      InsertOrDieNoPrint(&ts->tablet_consensus_state_map_,
                         std::make_pair(replica->ts_uuid(), tablet->id()),
                         cstate);
    }
  }

  void CreateReplicaAndAdd(vector<shared_ptr<KsckTabletReplica>>* replicas,
                           const string& tablet_id,
                           bool is_leader,
                           bool is_running) {
    shared_ptr<KsckTabletReplica> replica(new KsckTabletReplica(assignment_plan_.back(),
                                                                is_leader));
    shared_ptr<MockKsckTabletServer> ts = static_pointer_cast<MockKsckTabletServer>(
            master_->tablet_servers_.at(assignment_plan_.back()));

    assignment_plan_.pop_back();
    replicas->push_back(replica);

    // Add the equivalent replica on the tablet server.
    tablet::TabletStatusPB pb;
    pb.set_tablet_id(tablet_id);
    pb.set_table_name("fake-table");
    pb.set_state(is_running ? tablet::RUNNING : tablet::FAILED);
    InsertOrDie(&ts->tablet_status_map_, tablet_id, std::move(pb));
  }

  Status RunKsck() {
    auto c = MakeScopedCleanup([this]() {
        LOG(INFO) << "Ksck output:\n" << err_stream_.str();
      });
    RETURN_NOT_OK(ksck_->CheckMasterRunning());
    RETURN_NOT_OK(ksck_->FetchTableAndTabletInfo());
    RETURN_NOT_OK(ksck_->FetchInfoFromTabletServers());
    RETURN_NOT_OK(ksck_->CheckTablesConsistency());
    return Status::OK();
  }


  shared_ptr<MockKsckMaster> master_;
  shared_ptr<KsckCluster> cluster_;
  shared_ptr<Ksck> ksck_;
  // This is used as a stack. First the unit test is responsible to create a plan to follow, that
  // is the order in which each replica of each tablet will be assigned, starting from the end.
  // So if you have 2 tablets with num_replicas=3 and 3 tablet servers, then to distribute evenly
  // you should have a list that looks like ts1,ts2,ts3,ts3,ts2,ts1 so that the two LEADERS, which
  // are assigned first, end up on ts1 and ts3.
  vector<string> assignment_plan_;

  std::ostringstream err_stream_;
};

TEST_F(KsckTest, TestMasterOk) {
  ASSERT_OK(ksck_->CheckMasterRunning());
}

TEST_F(KsckTest, TestMasterUnavailable) {
  Status error = Status::NetworkError("Network failure");
  master_->fetch_info_status_ = error;
  ASSERT_TRUE(ksck_->CheckMasterRunning().IsNetworkError());
}

TEST_F(KsckTest, TestTabletServersOk) {
  ASSERT_OK(RunKsck());
}

TEST_F(KsckTest, TestBadTabletServer) {
  CreateOneSmallReplicatedTable();

  // Mock a failure to connect to one of the tablet servers.
  Status error = Status::NetworkError("Network failure");
  static_pointer_cast<MockKsckTabletServer>(master_->tablet_servers_["ts-id-1"])
      ->fetch_info_status_ = error;

  ASSERT_OK(ksck_->CheckMasterRunning());
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  Status s = ksck_->FetchInfoFromTabletServers();
  ASSERT_TRUE(s.IsNetworkError()) << "Status returned: " << s.ToString();

  s = ksck_->CheckTablesConsistency();
  EXPECT_EQ("Corruption: 1 table(s) are bad", s.ToString());
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "WARNING: Unable to connect to Tablet Server "
      "ts-id-1 (<mock>): Network error: Network failure");
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
}

TEST_F(KsckTest, TestZeroTabletReplicasCheck) {
  ASSERT_OK(RunKsck());
}

TEST_F(KsckTest, TestZeroTableCheck) {
  ASSERT_OK(RunKsck());
}

TEST_F(KsckTest, TestOneTableCheck) {
  CreateOneTableOneTablet();
  ASSERT_OK(RunKsck());
  ASSERT_OK(ksck_->ChecksumData(ChecksumOptions()));
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "0/1 replicas remaining (20B from disk, 10 rows summed)");
}

TEST_F(KsckTest, TestOneSmallReplicatedTable) {
  CreateOneSmallReplicatedTable();
  ASSERT_OK(RunKsck());
  ASSERT_OK(ksck_->ChecksumData(ChecksumOptions()));
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "0/9 replicas remaining (180B from disk, 90 rows summed)");

  // Test filtering (a non-matching pattern)
  err_stream_.str("");
  ksck_->set_table_filters({"xyz"});
  ASSERT_OK(RunKsck());
  Status s = ksck_->ChecksumData(ChecksumOptions());
  EXPECT_EQ("Not found: No table found. Filter: table_filters=xyz", s.ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "The cluster doesn't have any matching tables");

  // Test filtering with a matching table pattern.
  err_stream_.str("");
  ksck_->set_table_filters({"te*"});
  ASSERT_OK(RunKsck());
  ASSERT_OK(ksck_->ChecksumData(ChecksumOptions()));
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "0/9 replicas remaining (180B from disk, 90 rows summed)");

  // Test filtering with a matching tablet ID pattern.
  err_stream_.str("");
  ksck_->set_table_filters({});
  ksck_->set_tablet_id_filters({"*-id-2"});
  ASSERT_OK(RunKsck());
  ASSERT_OK(ksck_->ChecksumData(ChecksumOptions()));
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "0/3 replicas remaining (60B from disk, 30 rows summed)");
}

TEST_F(KsckTest, TestOneSmallReplicatedTableWithConsensusState) {
  FLAGS_consensus = true;
  CreateOneSmallReplicatedTable();
  ASSERT_OK(RunKsck());
}

TEST_F(KsckTest, TestConsensusConflictExtraPeer) {
  FLAGS_consensus = true;
  CreateOneSmallReplicatedTable();

  shared_ptr<KsckTabletServer> ts = FindOrDie(master_->tablet_servers_, "ts-id-0");
  auto& cstate = FindOrDieNoPrint(ts->tablet_consensus_state_map_,
                                  std::make_pair("ts-id-0", "tablet-id-0"));
  cstate.mutable_committed_config()->add_peers()->set_permanent_uuid("ts-id-fake");

  Status s = RunKsck();
  ASSERT_EQ("Corruption: 1 table(s) are bad", s.ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "The consensus matrix is:\n"
      " Config source |      Voters      | Current term | Config index | Committed?\n"
      "---------------+------------------+--------------+--------------+------------\n"
      " master        | A*  B   C        |              |              | Yes\n"
      " A             | A*  B   C   D    | 0            |              | Yes\n"
      " B             | A*  B   C        | 0            |              | Yes\n"
      " C             | A*  B   C        | 0            |              | Yes");
}

TEST_F(KsckTest, TestConsensusConflictMissingPeer) {
  FLAGS_consensus = true;
  CreateOneSmallReplicatedTable();

  shared_ptr<KsckTabletServer> ts = FindOrDie(master_->tablet_servers_, "ts-id-0");
  auto& cstate = FindOrDieNoPrint(ts->tablet_consensus_state_map_,
                                  std::make_pair("ts-id-0", "tablet-id-0"));
  cstate.mutable_committed_config()->mutable_peers()->RemoveLast();

  Status s = RunKsck();
  ASSERT_EQ("Corruption: 1 table(s) are bad", s.ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "The consensus matrix is:\n"
      " Config source |    Voters    | Current term | Config index | Committed?\n"
      "---------------+--------------+--------------+--------------+------------\n"
      " master        | A*  B   C    |              |              | Yes\n"
      " A             | A*  B        | 0            |              | Yes\n"
      " B             | A*  B   C    | 0            |              | Yes\n"
      " C             | A*  B   C    | 0            |              | Yes");
}

TEST_F(KsckTest, TestConsensusConflictDifferentLeader) {
  FLAGS_consensus = true;
  CreateOneSmallReplicatedTable();

  const shared_ptr<KsckTabletServer>& ts = FindOrDie(master_->tablet_servers_, "ts-id-0");
  auto& cstate = FindOrDieNoPrint(ts->tablet_consensus_state_map_,
                                  std::make_pair("ts-id-0", "tablet-id-0"));
  cstate.set_leader_uuid("ts-id-1");

  Status s = RunKsck();
  ASSERT_EQ("Corruption: 1 table(s) are bad", s.ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
      "The consensus matrix is:\n"
      " Config source |    Voters    | Current term | Config index | Committed?\n"
      "---------------+--------------+--------------+--------------+------------\n"
      " master        | A*  B   C    |              |              | Yes\n"
      " A             | A   B*  C    | 0            |              | Yes\n"
      " B             | A*  B   C    | 0            |              | Yes\n"
      " C             | A*  B   C    | 0            |              | Yes");
}

TEST_F(KsckTest, TestOneOneTabletBrokenTable) {
  CreateOneOneTabletReplicatedBrokenTable();
  Status s = RunKsck();
  EXPECT_EQ("Corruption: 1 table(s) are bad", s.ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "Tablet tablet-id-1 of table 'test' is under-replicated: "
                      "configuration has 2 replicas vs desired 3");
}

TEST_F(KsckTest, TestMismatchedAssignments) {
  CreateOneSmallReplicatedTable();
  shared_ptr<MockKsckTabletServer> ts = static_pointer_cast<MockKsckTabletServer>(
      master_->tablet_servers_.at(Substitute("ts-id-$0", 0)));
  ASSERT_EQ(1, ts->tablet_status_map_.erase("tablet-id-2"));

  Status s = RunKsck();
  EXPECT_EQ("Corruption: 1 table(s) are bad", s.ToString());
  ASSERT_STR_CONTAINS(err_stream_.str(),
                      "Tablet tablet-id-2 of table 'test' is under-replicated: "
                      "1 replica(s) not RUNNING\n"
                      "  ts-id-0 (<mock>): missing [LEADER]\n"
                      "  ts-id-1 (<mock>): RUNNING\n"
                      "  ts-id-2 (<mock>): RUNNING\n");
}

TEST_F(KsckTest, TestTabletNotRunning) {
  CreateOneSmallReplicatedTableWithTabletNotRunning();

  Status s = RunKsck();
  EXPECT_EQ("Corruption: 1 table(s) are bad", s.ToString());
  ASSERT_STR_CONTAINS(
      err_stream_.str(),
      "Tablet tablet-id-0 of table 'test' is unavailable: 3 replica(s) not RUNNING\n"
      "  ts-id-0 (<mock>): bad state [LEADER]\n"
      "    State:       FAILED\n"
      "    Data state:  TABLET_DATA_UNKNOWN\n"
      "    Last status: \n"
      "  ts-id-1 (<mock>): bad state\n"
      "    State:       FAILED\n"
      "    Data state:  TABLET_DATA_UNKNOWN\n"
      "    Last status: \n"
      "  ts-id-2 (<mock>): bad state\n"
      "    State:       FAILED\n"
      "    Data state:  TABLET_DATA_UNKNOWN\n"
      "    Last status: \n");
}

} // namespace tools
} // namespace kudu
