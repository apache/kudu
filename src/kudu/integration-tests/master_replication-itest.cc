// Copyright (c) 2014, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <vector>

#include "kudu/client/client.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/mini_master.h"
#include "kudu/util/test_util.h"


namespace kudu {
namespace master {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduSchema;
using client::KuduTable;
using std::vector;

const char * const kTableId1 = "testMasterReplication-1";
const char * const kTableId2 = "testMasterReplication-2";

const int kNumTabletServerReplicas = 3;

class MasterReplicationTest : public KuduTest {
 public:
  MasterReplicationTest() {
    // Hard-coded ports for the masters. This is safe, as this unit test
    // runs under a resource lock (see CMakeLists.txt in this directory).
    // TODO we should have a generic method to obtain n free ports.
    opts_.master_rpc_ports = boost::assign::list_of(11010)(11011)(11012);

    opts_.num_masters = num_masters_ = opts_.master_rpc_ports.size();
    opts_.num_tablet_servers = kNumTabletServerReplicas;
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    cluster_.reset(new MiniCluster(env_.get(), opts_));
    ASSERT_STATUS_OK(cluster_->Start());
    ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(kNumTabletServerReplicas));
  }

  virtual void TearDown() OVERRIDE {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
    KuduTest::TearDown();
  }

  // Wait for the commit index to propagate (need to wait for the
  // consensus heartbeat). TODO: This is because we don't handle
  // orphaned replicates. We could do something fancier here to
  // probe the actual consensus state, but that would require a lot
  // of plumbing, and this is just temporary.
  void WaitForCommitsToPropagate() {
    usleep(1500 * 1000);
  }

  Status RestartCluster() {
    cluster_->Shutdown();
    RETURN_NOT_OK(cluster_->Start());
    return Status::OK();
  }

  Status CreateLeaderClient(shared_ptr<KuduClient>* out) {
    return KuduClientBuilder()
        .master_server_addr(cluster_->leader_mini_master()
                            ->bound_rpc_addr().ToString())
        .Build(out);
  }


  Status CreateTable(const shared_ptr<KuduClient>& client,
                     const std::string& table_name) {
    KuduSchema client_schema(boost::assign::list_of
                             (KuduColumnSchema("key", KuduColumnSchema::UINT32))
                             (KuduColumnSchema("int_val", KuduColumnSchema::UINT32))
                             (KuduColumnSchema("string_val", KuduColumnSchema::STRING))
                             , 1);
    return client->NewTableCreator()->table_name(table_name)
        .schema(&client_schema)
        .Create();
  }

  // Test promoting a follower at 'new_master_idx' to the role of
  // a leader, previously occupied by the node at 'orig_master_idx':
  //
  // 1) Verify that 'existing_table' exists on all master nodes.
  // 2) Verify that 'new_table' can be created and seen by all masters
  // and table servers.
  void TestPromoteMaster(int orig_master_idx, int new_master_idx,
                         const std::string& existing_table,
                         const std::string& new_table) {
    LOG(INFO) << "Previous configuration: leader at index " << orig_master_idx;
    LOG(INFO) << "New configuration: leader at index " << new_master_idx;
    cluster_->Shutdown();
    cluster_->set_leader_master_idx(new_master_idx);
    ASSERT_STATUS_OK(cluster_->Start());
    ASSERT_NO_FATAL_FAILURE(VerifyTableExists(existing_table));
    shared_ptr<KuduClient> leader_client;
    ASSERT_STATUS_OK(CreateLeaderClient(&leader_client));
    ASSERT_STATUS_OK(CreateTable(leader_client, new_table));

    ASSERT_TRUE(cluster_->leader_mini_master()->master()
                ->catalog_manager()->TableNameExists(new_table));

    WaitForCommitsToPropagate();

    ASSERT_STATUS_OK(RestartCluster());
    ASSERT_NO_FATAL_FAILURE(VerifyTableExists(new_table));
  }

  void VerifyTableExists(const std::string& table_id) {
    for (int i = 0; i < num_masters_; i++) {
      LOG(INFO) << "Verifying that " << table_id << " exists on Master " << i;
      ASSERT_TRUE(cluster_->mini_master(i)->master()
                  ->catalog_manager()->TableNameExists(table_id));
    }
  }

 protected:
  int num_masters_;
  MiniClusterOptions opts_;
  gscoped_ptr<MiniCluster> cluster_;
};

// Basic test. Verify that:
//
// 1) We can start multiple masters in a distributed configuration and
// that the clients and tablet servers can connect to the leader
// master.
//
// 2) We can create a table (using the standard client APIs) on the
// the leader and ensure that the appropriate table/tablet info is
// replicated to all of the
//
// 3) We can create another table and that the table info is visible
// on all of the masters after Bootstrap.
//
TEST_F(MasterReplicationTest, TestSysTablesReplication) {
  shared_ptr<KuduClient> leader_client;

  // Create the first table.
  ASSERT_STATUS_OK(CreateLeaderClient(&leader_client));
  ASSERT_STATUS_OK(CreateTable(leader_client, kTableId1));

  // Verify that it's created on the leader.
  ASSERT_TRUE(cluster_->leader_mini_master()->master()
              ->catalog_manager()->TableNameExists(kTableId1));
  WaitForCommitsToPropagate();

  // CatalogManager currently reads from copy on write objects that
  // are only updated on the leader master. As a result, we must
  // restart the follower masters (forcing bootstrap and a rebuild of
  // the in-memory objects) in order to see the changes that we've
  // made. See KUDU-500 for a TODO item to fix this and several
  // approaches that can be taken.
  ASSERT_STATUS_OK(RestartCluster());

  // Verify that after restarting the cluster and running bootstrap,
  // the first table is visible on all of the master nodes.
  ASSERT_NO_FATAL_FAILURE(VerifyTableExists(kTableId1));

  // Repeat the same for the second table.
  ASSERT_STATUS_OK(CreateTable(leader_client, kTableId2));
  WaitForCommitsToPropagate();
  ASSERT_STATUS_OK(RestartCluster());
  ASSERT_NO_FATAL_FAILURE(VerifyTableExists(kTableId2));
}

// Verify that we can:
//
// 1) Start a cluster, create a table, and replicate the table/tablet
// info to all of the followers.
//
// 2) Shut down the cluster, set a new node as the master leader, and
// pointing the TabletServers to the new master leader.

// 3) Verify that we can query existing tables/tablets, create new
// tables/tablets on the new leader, and that new changes to the
// SysTables are replicated to the newly configured master cluster.
TEST_F(MasterReplicationTest, TestManualPromotion) {
  shared_ptr<KuduClient> leader_client;

  // Create the first table.
  ASSERT_STATUS_OK(CreateLeaderClient(&leader_client));
  ASSERT_STATUS_OK(CreateTable(leader_client, kTableId1));

  // Verify that it's created on the leader.
  ASSERT_TRUE(cluster_->leader_mini_master()->master()
              ->catalog_manager()->TableNameExists(kTableId1));
  WaitForCommitsToPropagate();

  // Now for every possible master, verify that it can be promoted to
  // the role of a leader.
  int prev_leader_idx = cluster_->leader_master_idx();
  string prev_table_name = kTableId1;
  for (int i = 0; i < num_masters_; i++) {
    if (i == prev_leader_idx) {
      continue;
    }
    string new_table_name = strings::Substitute("$0-$1", kTableId1, i);
    ASSERT_NO_FATAL_FAILURE(TestPromoteMaster(prev_leader_idx, i,
                                              prev_table_name,
                                              new_table_name));
    prev_leader_idx = i;
    prev_table_name = new_table_name;
    // We need to sleep to make sure there are no pending transactions
    // when we restart the cluster in TestPromoteMaster.
    // This be remove once we're done with KUDU-255.
    sleep(2);
  }
}

} // namespace master
} // namespace kudu
