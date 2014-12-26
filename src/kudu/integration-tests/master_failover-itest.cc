// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/client-internal.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

namespace kudu {

// Note: this test needs to be in the client namespace in order for
// KuduClient::Data class methods to be visible via FRIEND_TEST macro.
namespace client {

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
  enum CreateTableMode {
    kWaitForCreate = 0,
    kNoWaitForCreate = 1
  };

  MasterFailoverTest() {
    opts_.master_rpc_ports = boost::assign::list_of(11010)(11011)(11012);
    opts_.num_masters = num_masters_ = opts_.master_rpc_ports.size();
    opts_.num_tablet_servers = kNumTabletServerReplicas;

    // Reduce various below as to make the detection of leader master
    // failures (specifically, failures as result of long pauses) more
    // rapid.

    // Set max missed heartbeats periods to 1.0 (down from 3.0).
    opts_.extra_master_flags.push_back("--leader_failure_max_missed_heartbeat_periods=1.0");

    // Set the TS->master heartbeat timeout to 2.5 seconds (down from 15 seconds).
    opts_.extra_tserver_flags.push_back("--heartbeat_rpc_timeout_ms=2500");
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

  Status CreateTable(const std::string& table_name, CreateTableMode mode) {
    KuduSchema client_schema(boost::assign::list_of
                             (KuduColumnSchema("key", KuduColumnSchema::UINT32))
                             (KuduColumnSchema("int_val", KuduColumnSchema::UINT32))
                             (KuduColumnSchema("string_val", KuduColumnSchema::STRING))
                             , 1);
    return client_->NewTableCreator()->table_name(table_name)
        .schema(&client_schema)
        .timeout(MonoDelta::FromSeconds(90))
        .wait(mode == kWaitForCreate)
        .Create();
  }

  // Test that we can get the table location information from the
  // master and then open scanners on the tablet server. This involves
  // sending RPCs to both the master and the tablet servers and
  // requires that the table and tablet exist both on the masters and
  // the tablet servers.
  Status OpenTableAndScanner(const std::string& table_name) {
    scoped_refptr<KuduTable> table;
    RETURN_NOT_OK_PREPEND(client_->OpenTable(table_name, &table),
                          "Unable to open table " + table_name);
    KuduScanner scanner(table.get());
    KuduSchema empty_projection(vector<KuduColumnSchema>(), 0);
    RETURN_NOT_OK_PREPEND(scanner.SetProjection(&empty_projection),
                          "Unable to open an empty projection on " + table_name);
    RETURN_NOT_OK_PREPEND(scanner.Open(),
                          "Unable to open scanner on " + table_name);
    return Status::OK();
  }

 protected:
  int num_masters_;
  ExternalMiniClusterOptions opts_;
  gscoped_ptr<ExternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
};

// Test that synchronous CreateTable (issue CreateTable call and then
// wait until the table has been created) works even when the original
// leader master has been paused.
TEST_F(MasterFailoverTest, TestCreateTableSync) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "This test can only be run in slow mode.";
    return;
  }

  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));

  LOG(INFO) << "Pausing leader master";
  cluster_->master(leader_idx)->Pause();
  ScopedResumeExternalDaemon resume_daemon(cluster_->master(leader_idx));

  string table_name = "testCreateTableSync";
  ASSERT_OK(CreateTable(table_name, kWaitForCreate));
  ASSERT_OK(OpenTableAndScanner(table_name));
}

// Test that we can issue a CreateTable call, pause the leader master
// immediately after, then verify that the table has been created on
// the newly elected leader master.
//
// TODO enable this test once flakiness issues are worked out and
// eliminated on test machines.
TEST_F(MasterFailoverTest, DISABLED_TestPauseAfterCreateTableIssued) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "This test can only be run in slow mode.";
    return;
  }

  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));

  string table_id = "testPauseAfterCreateTableIssued";
  LOG(INFO) << "Issuing CreateTable for " << table_id;
  ASSERT_OK(CreateTable(table_id, kNoWaitForCreate));

  LOG(INFO) << "Pausing leader master";
  cluster_->master(leader_idx)->Pause();
  ScopedResumeExternalDaemon resume_daemon(cluster_->master(leader_idx));

  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(MonoDelta::FromSeconds(90));
  ASSERT_OK(client_->data_->WaitForCreateTableToFinish(client_.get(),
                                                       table_id, deadline));

  ASSERT_OK(OpenTableAndScanner(table_id));
}

} // namespace client
} // namespace kudu
