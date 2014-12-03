// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/test_util.h"

namespace kudu {

const int kNumTabletServerReplicas = 3;

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduScanner;
using client::KuduSchema;
using client::KuduTable;
using std::tr1::shared_ptr;

const char * const kTableId1 = "testMasterFailover-1";

class MasterFailoverTest : public KuduTest {
 public:
  MasterFailoverTest() {
    opts_.master_rpc_ports = boost::assign::list_of(11010)(11011)(11012);
    opts_.num_masters = num_masters_ = opts_.master_rpc_ports.size();
    opts_.num_tablet_servers = kNumTabletServerReplicas;
    opts_.extra_master_flags.push_back("--leader_failure_max_missed_heartbeat_periods=1.0");
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    ASSERT_NO_FATAL_FAILURE(RestartCluster());
  }

  virtual void TearDown() OVERRIDE {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

  void RestartCluster() {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
    cluster_.reset(new ExternalMiniCluster(opts_));
    ASSERT_STATUS_OK(cluster_->Start());
    KuduClientBuilder builder;
    builder.default_admin_operation_timeout(MonoDelta::FromSeconds(5));
    builder.default_select_master_timeout(MonoDelta::FromSeconds(60));
    ASSERT_STATUS_OK(cluster_->CreateClient(builder, &client_));
  }

  Status CreateTable(const std::string& table_name) {
    KuduSchema client_schema(boost::assign::list_of
                             (KuduColumnSchema("key", KuduColumnSchema::UINT32))
                             (KuduColumnSchema("int_val", KuduColumnSchema::UINT32))
                             (KuduColumnSchema("string_val", KuduColumnSchema::STRING))
                             , 1);
    return client_->NewTableCreator()->table_name(table_name)
        .schema(&client_schema)
        .Create();
  }
 protected:
  int num_masters_;
  ExternalMiniClusterOptions opts_;
  gscoped_ptr<ExternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
};

// Test that any master can be paused without halting lookups during
// the pause.
TEST_F(MasterFailoverTest, TestPauseAnyMaster) {
  ASSERT_STATUS_OK(CreateTable(kTableId1));
  for (int i = 0; i < num_masters_; i++) {
    LOG(INFO) << "Pausing master: " << i;
    scoped_refptr<KuduTable> table;
    cluster_->master(i)->Pause();

    ASSERT_STATUS_OK(client_->OpenTable(kTableId1, &table));
    KuduScanner scanner(table.get());
    KuduSchema empty_projection(vector<KuduColumnSchema>(), 0);
    ASSERT_STATUS_OK(scanner.SetProjection(&empty_projection));
    ASSERT_STATUS_OK(scanner.Open());

    cluster_->master(i)->Resume();
  }
}

} // namespace kudu
