// Copyright (c) 2013, Cloudera, inc.

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <boost/foreach.hpp>
#include <boost/assign/list_of.hpp>

#include "client/client.h"
#include "common/wire_protocol-test-util.h"
#include "common/schema.h"
#include "integration-tests/mini_cluster.h"
#include "master/catalog_manager.h"
#include "master/mini_master.h"
#include "master/master.proxy.h"
#include "server/metadata.pb.h"
#include "tserver/tablet_server.h"
#include "tserver/mini_tablet_server.h"
#include "tserver/tablet_server-test-base.h"
#include "util/test_util.h"
#include "util/stopwatch.h"

DECLARE_int32(default_num_replicas);

namespace kudu {

namespace tserver {

using std::vector;
using std::tr1::shared_ptr;
using master::GetTableLocationsRequestPB;
using master::GetTableLocationsResponsePB;
using master::TableIdentifierPB;
using master::TabletLocationsPB;
using master::MiniMaster;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using tserver::TabletServer;
using client::KuduClientOptions;
using client::KuduClient;
using client::KuduTable;

static const int kMaxRetries = 20;

// Integration test for distributed consensus.
class DistConsensusTest : public TabletServerTest {
 public:

  struct ProxyDetails {
    master::TSInfoPB ts_info;
    gscoped_ptr<TabletServerServiceProxy> proxy;
  };

  virtual void SetUp() {
    KuduTest::SetUp();
    CreateCluster();
    CreateClient();
    WaitForAndGetQuorum();
  }

  void CreateCluster() {
    FLAGS_default_num_replicas = 3;
    cluster_.reset(new MiniCluster(env_.get(), test_dir_, 3));
    ASSERT_STATUS_OK(cluster_->Start());
    ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(3));
    CreateTestSchema(&schema_);
  }

  void CreateClient() {
    // Connect to the cluster.
    KuduClientOptions opts;
    opts.master_server_addr = cluster_->mini_master()->bound_rpc_addr().ToString();
    ASSERT_STATUS_OK(KuduClient::Create(opts, &client_));
    // Create a table with a single tablet, with three replicas
    ASSERT_STATUS_OK(client_->CreateTable(kTableId, schema_));
    ASSERT_STATUS_OK(client_->OpenTable(kTableId, &table_));
  }

  void CreateLeaderAndReplicaProxies(const TabletLocationsPB& locations) {
    leader_.reset();
    STLDeleteElements(&replicas_);
    BOOST_FOREACH(const TabletLocationsPB::ReplicaPB& replica_pb, locations.replicas()) {
      HostPort host_port;
      ASSERT_STATUS_OK(HostPortFromPB(replica_pb.ts_info().rpc_addresses(0), &host_port));
      vector<Sockaddr> addresses;
      host_port.ResolveAddresses(&addresses);
      gscoped_ptr<TabletServerServiceProxy> proxy;
      CreateClientProxy(addresses[0], &proxy);
      if (replica_pb.role() == QuorumPeerPB::LEADER) {
        ProxyDetails* leader = new ProxyDetails();
        leader->proxy.reset(proxy.release());
        leader->ts_info.CopyFrom(replica_pb.ts_info());
        leader_.reset(leader);
      } else if (replica_pb.role() == QuorumPeerPB::FOLLOWER) {
        ProxyDetails* replica = new ProxyDetails();
        replica->proxy.reset(proxy.release());
        replica->ts_info.CopyFrom(replica_pb.ts_info());
        replicas_.push_back(replica);
      }
    }
  }

  // Gets the the locations of the quorum and waits until 1 LEADER and 2 FOLLOWERS
  // are reported.
  void WaitForAndGetQuorum() {
    GetTableLocationsRequestPB req;
    TableIdentifierPB* id = req.mutable_table();
    id->set_table_name(kTableId);

    GetTableLocationsResponsePB resp;
    RpcController controller;

    CHECK_OK(client_->master_proxy()->GetTableLocations(req, &resp, &controller));
    ASSERT_EQ(resp.tablet_locations_size(), 1);
    tablet_id = resp.tablet_locations(0).tablet_id();

    TabletLocationsPB locations;
    int num_retries = 0;
    // make sure the three replicas are up and find the leader
    while (true) {
      if (num_retries >= kMaxRetries) {
        FAIL() << " Reached max. retries while looking up the quorum.";
      }
      // TODO add a way to wait for a tablet to be ready. Also to wait for it to
      // have a certain _active_ replication count.
      replicas_.clear();
      Status status = cluster_->WaitForReplicaCount(resp.tablet_locations(0).tablet_id(), 3,
                                                    &locations);
      if (status.IsTimedOut()) {
        LOG(WARNING)<< "Timeout waiting for all three replicas to be online, retrying...";
        num_retries++;
        continue;
      }

      ASSERT_STATUS_OK(status);
      CreateLeaderAndReplicaProxies(locations);

      if (leader_.get() == NULL || replicas_.size() < 2) {
        LOG(WARNING)<< "Couldn't find the leader and/or replicas. Locations: "
        << locations.ShortDebugString();
        sleep(1);
        num_retries++;
        continue;
      }
      break;
    }
    CreateSharedRegion();
  }

  void ScanReplica(TabletServerServiceProxy* replica_proxy,
                   vector<string>* results) {

    ScanRequestPB req;
    ScanResponsePB resp;
    RpcController rpc;

    NewScanRequestPB* scan = req.mutable_new_scan_request();
    scan->set_tablet_id(tablet_id);
    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, scan->mutable_projected_columns()));

    // Send the call
    {
      req.set_batch_size_bytes(0);
      SCOPED_TRACE(req.DebugString());
      ASSERT_STATUS_OK(replica_proxy->Scan(req, &resp, &rpc));
      SCOPED_TRACE(resp.DebugString());
      ASSERT_FALSE(resp.has_error());
    }

    if (!resp.has_more_results())
      return;

    // Drain all the rows from the scanner.
    ASSERT_NO_FATAL_FAILURE(DrainScannerToStrings(resp.scanner_id(),
                                                  schema_,
                                                  results,
                                                  replica_proxy));

    std::sort(results->begin(), results->end());
  }

  void AssertRowsExistInReplicas() {
    vector<string> leader_results;
    vector<string> replica_results;
    ScanReplica(leader_->proxy.get(), &leader_results);
    BOOST_FOREACH(ProxyDetails* replica, replicas_) {
      SCOPED_TRACE(strings::Substitute("Replica results did not match the leaders."
          "\nReplica: $0\nLeader:$1", replica->ts_info.ShortDebugString(),
          leader_->ts_info.ShortDebugString()));
      ScanReplica(replica->proxy.get(), &replica_results);
      ASSERT_EQ(leader_results.size(), replica_results.size());
      for (int i = 0; i < leader_results.size(); i++) {
        ASSERT_EQ(leader_results[i], replica_results[i]);
      }
      replica_results.clear();
    }
  }

  virtual void TearDown() {
    cluster_->Shutdown();
    STLDeleteElements(&replicas_);
  }

 protected:
  gscoped_ptr<MiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
  scoped_refptr<KuduTable> table_;
  gscoped_ptr<ProxyDetails> leader_;
  vector<ProxyDetails*> replicas_;

  QuorumPB quorum_;
  Schema schema_;
  string tablet_id;
};

// TODO allow the scan to define an operation id, fetch the last id
// from the leader and then use that id to make the replica wait
// until it is done. This will avoid the sleeps below.
TEST_F(DistConsensusTest, TestInsertAndMutateThroughConsensus) {

  if (AllowSlowTests()) {
    for (int i = 0; i < 100; i++) {
      InsertTestRowsRemote(0, i * 1000, 1000, 100, leader_->proxy.get(), tablet_id);
    }
    // sleep to let the request get committed to the replicas.
    usleep(500000);
  } else {
    InsertTestRowsRemote(0, 0, 1000, 100, leader_->proxy.get(), tablet_id);
    usleep(1000000);
  }
  AssertRowsExistInReplicas();
}

}  // namespace tserver
}  // namespace kudu

