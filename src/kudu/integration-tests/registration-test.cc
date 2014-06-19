// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>

#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master-test-util.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/test_util.h"
#include "kudu/util/stopwatch.h"

DECLARE_int32(heartbeat_interval_ms);
DECLARE_bool(use_hybrid_clock);
DECLARE_int32(max_clock_sync_error_usec);

namespace kudu {

using std::vector;
using std::tr1::shared_ptr;
using master::MiniMaster;
using master::TSDescriptor;
using master::TabletLocationsPB;
using tserver::MiniTabletServer;

// Tests for the Tablet Server registering with the Master,
// and the master maintaining the tablet descriptor.
class RegistrationTest : public KuduTest {
 public:
  RegistrationTest()
    : schema_(boost::assign::list_of
              (ColumnSchema("c1", UINT32)),
              1) {
  }

  virtual void SetUp() OVERRIDE {
    // Make heartbeats faster to speed test runtime.
    FLAGS_heartbeat_interval_ms = 10;

    KuduTest::SetUp();

    // Use the hybrid clock for TS tests
    FLAGS_use_hybrid_clock = true;

    // increase the max error tolerance, for tests, to 10 seconds.
    FLAGS_max_clock_sync_error_usec = 10000000;


    cluster_.reset(new MiniCluster(env_.get(), test_dir_, 1));
    ASSERT_STATUS_OK(cluster_->Start());
  }

  virtual void TearDown() OVERRIDE {
    cluster_->Shutdown();
  }

  void CheckTabletServersPage() {
    EasyCurl c;
    faststring buf;
    string addr = cluster_->mini_master()->bound_http_addr().ToString();
    ASSERT_STATUS_OK(c.FetchURL(strings::Substitute("http://$0/tablet-servers", addr),
                                &buf));

    // Should include the TS UUID
    string expected_uuid =
      cluster_->mini_tablet_server(0)->server()->instance_pb().permanent_uuid();
    ASSERT_STR_CONTAINS(buf.ToString(), expected_uuid);
  }

 protected:
  gscoped_ptr<MiniCluster> cluster_;
  Schema schema_;
};

TEST_F(RegistrationTest, TestTSRegisters) {
  // Wait for the TS to register.
  vector<shared_ptr<TSDescriptor> > descs;
  ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(1, &descs));
  ASSERT_EQ(1, descs.size());

  // Verify that the registration is sane.
  master::TSRegistrationPB reg;
  descs[0]->GetRegistration(&reg);
  {
    SCOPED_TRACE(reg.ShortDebugString());
    ASSERT_EQ(reg.ShortDebugString().find("0.0.0.0"), string::npos)
      << "Should not include wildcards in registration";
  }

  ASSERT_NO_FATAL_FAILURE(CheckTabletServersPage());

  // Restart the master, so it loses the descriptor, and ensure that the
  // hearbeater thread handles re-registering.
  ASSERT_STATUS_OK(cluster_->mini_master()->Restart());

  ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(1));

  // TODO: when the instance ID / sequence number stuff is implemented,
  // restart the TS and ensure that it re-registers with the newer sequence
  // number.
}

// Test starting multiple tablet servers and ensuring they both register with the master.
TEST_F(RegistrationTest, TestMultipleTS) {
  ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(1));
  ASSERT_STATUS_OK(cluster_->AddTabletServer());
  ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(2));
}

// TODO: this doesn't belong under "RegistrationTest" - rename this file
// to something more appropriate - doesn't seem worth having separate
// whole test suites for registration, tablet reports, etc.
TEST_F(RegistrationTest, TestTabletReports) {
  string tablet_id_1;
  string tablet_id_2;

  ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(1));

  MiniTabletServer* ts = cluster_->mini_tablet_server(0);
  string ts_root = cluster_->GetTabletServerFsRoot(0);

  // Add a tablet, make sure it reports itself.
  CreateTabletForTesting(cluster_->mini_master(), "fake-table", schema_, &tablet_id_1);

  TabletLocationsPB locs;
  ASSERT_STATUS_OK(cluster_->WaitForReplicaCount(tablet_id_1, 1, &locs));
  ASSERT_EQ(1, locs.replicas_size());
  LOG(INFO) << "Tablet successfully reported on " << locs.replicas(0).ts_info().permanent_uuid();

  // Add another tablet, make sure it is reported via incremental.
  CreateTabletForTesting(cluster_->mini_master(), "fake-table2", schema_, &tablet_id_2);
  ASSERT_STATUS_OK(cluster_->WaitForReplicaCount(tablet_id_2, 1, &locs));

  // Shut down the whole system, bring it back up, and make sure the tablets
  // are reported.
  ts->Shutdown();
  ASSERT_STATUS_OK(cluster_->mini_master()->Restart());
  ASSERT_STATUS_OK(ts->Start());

  ASSERT_STATUS_OK(cluster_->WaitForReplicaCount(tablet_id_1, 1, &locs));
  ASSERT_STATUS_OK(cluster_->WaitForReplicaCount(tablet_id_2, 1, &locs));

  // Restart the TS after clearing its master blocks. On restart, it will send
  // a full tablet report, without any of the tablets. This causes the
  // master to remove the tablet locations.
  LOG(INFO) << "Shutting down TS, clearing data, and restarting it";
  ts->Shutdown();
  string master_block_dir = ts->fs_manager()->GetMasterBlockDir();
  ASSERT_STATUS_OK(env_->DeleteRecursively(master_block_dir));
  ASSERT_STATUS_OK(env_->CreateDir(master_block_dir));
  ASSERT_STATUS_OK(ts->Start());
  ASSERT_STATUS_OK(cluster_->WaitForReplicaCount(tablet_id_1, 0, &locs));
  ASSERT_STATUS_OK(cluster_->WaitForReplicaCount(tablet_id_2, 0, &locs));
}

} // namespace kudu
