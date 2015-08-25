// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Tests for the kudu-admin command-line tool.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/integration-tests/ts_itest-base.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace tools {

using itest::TabletServerMap;
using itest::TServerDetails;
using strings::Split;
using strings::Substitute;

static const char* const kAdminToolName = "kudu-admin";

class AdminCliTest : public tserver::TabletServerIntegrationTestBase {
 protected:
  // Figure out where the admin tool is.
  string GetAdminToolPath() const;
};

string AdminCliTest::GetAdminToolPath() const {
  string exe;
  CHECK_OK(Env::Default()->GetExecutablePath(&exe));
  string binroot = DirName(exe);
  string tool_path = JoinPathSegments(binroot, kAdminToolName);
  CHECK(Env::Default()->FileExists(tool_path)) << "kudu-admin tool not found at " << tool_path;
  return tool_path;
}

// Test kudu-admin config change while running a workload.
// 1. Instantiate external mini cluster with 3 TS.
// 2. Create table with 2 replicas.
// 3. Invoke kudu-admin CLI to invoke a config change.
// 4. Wait until the new server bootstraps.
// 5. Profit!
TEST_F(AdminCliTest, TestChangeConfig) {
  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 2;

  vector<string> flags;
  flags.push_back("--enable_leader_failure_detection=false");
  BuildAndStart(flags);

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(FLAGS_num_tablet_servers, tservers.size());

  TabletServerMap active_tablet_servers;
  TabletServerMap::const_iterator iter = tablet_replicas_.find(tablet_id_);
  TServerDetails* leader = iter->second;
  TServerDetails* follower = (++iter)->second;
  InsertOrDie(&active_tablet_servers, leader->uuid(), leader);
  InsertOrDie(&active_tablet_servers, follower->uuid(), follower);

  TServerDetails* new_node = NULL;
  BOOST_FOREACH(TServerDetails* ts, tservers) {
    if (!ContainsKey(active_tablet_servers, ts->uuid())) {
      new_node = ts;
      break;
    }
  }
  ASSERT_TRUE(new_node != NULL);

  // Elect the leader (still only a consensus config size of 2).
  ASSERT_OK(StartElection(leader, tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(30), active_tablet_servers,
                                  tablet_id_, 1));

  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.set_timeout_allowed(true);
  workload.set_write_timeout_millis(10000);
  workload.set_num_replicas(FLAGS_num_replicas);
  workload.set_num_write_threads(1);
  workload.set_write_batch_size(1);
  workload.Setup();
  workload.Start();

  // Wait until the Master knows about the leader tserver.
  TServerDetails* master_observed_leader;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &master_observed_leader));
  ASSERT_EQ(leader->uuid(), master_observed_leader->uuid());

  LOG(INFO) << "Adding tserver with uuid " << new_node->uuid() << " as VOTER...";
  string exe_path = GetAdminToolPath();
  string arg_str = Substitute("$0 -master_addresses $1 change_config $2 ADD_SERVER $3 VOTER",
                              exe_path,
                              cluster_->master()->bound_rpc_addr().ToString(),
                              tablet_id_, new_node->uuid());
  vector<string> args = Split(arg_str, " ");
  LOG(INFO) << "Invoking command: " << arg_str;
  {
    Subprocess proc(args[0], args);
    ASSERT_OK(proc.Start());
    int retcode;
    ASSERT_OK(proc.Wait(&retcode));
    ASSERT_EQ(0, retcode);
  }

  InsertOrDie(&active_tablet_servers, new_node->uuid(), new_node);
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(active_tablet_servers.size(),
                                                leader, tablet_id_,
                                                MonoDelta::FromSeconds(10)));

  workload.StopAndJoin();
  int num_batches = workload.batches_completed();

  LOG(INFO) << "Waiting for replicas to agree...";
  // Wait for all servers to replicate everything up through the last write op.
  // Since we don't batch, there should be at least # rows inserted log entries,
  // plus the initial leader's no-op, plus 1 for
  // the added replica for a total == #rows + 2.
  int min_log_index = num_batches + 2;
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(30),
                                  active_tablet_servers, tablet_id_,
                                  min_log_index));

  int rows_inserted = workload.rows_inserted();
  LOG(INFO) << "Number of rows inserted: " << rows_inserted;

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(kTableId, ClusterVerifier::AT_LEAST, rows_inserted));

  // Now remove the server once again.
  LOG(INFO) << "Removing tserver with uuid " << new_node->uuid() << " from the config...";
  arg_str = Substitute("$0 -master_addresses $1 change_config $2 REMOVE_SERVER $3",
                       exe_path,
                       cluster_->master()->bound_rpc_addr().ToString(),
                       tablet_id_, new_node->uuid());
  args = Split(arg_str, " ");
  LOG(INFO) << "Invoking command: " << arg_str;
  {
    Subprocess proc(args[0], args);
    ASSERT_OK(proc.Start());
    int retcode;
    ASSERT_OK(proc.Wait(&retcode));
    ASSERT_EQ(0, retcode);
  }

  ASSERT_EQ(1, active_tablet_servers.erase(new_node->uuid()));
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(active_tablet_servers.size(),
                                                leader, tablet_id_,
                                                MonoDelta::FromSeconds(10)));
}

} // namespace tools
} // namespace kudu
