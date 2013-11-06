// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>

#include "common/schema.h"
#include "gutil/gscoped_ptr.h"
#include "integration-tests/mini_cluster.h"
#include "master/mini_master.h"
#include "master/master.h"
#include "master/ts_descriptor.h"
#include "tserver/mini_tablet_server.h"
#include "tserver/tablet_server.h"
#include "util/curl_util.h"
#include "util/faststring.h"
#include "util/test_util.h"
#include "util/stopwatch.h"

DECLARE_int32(heartbeat_interval_ms);

namespace kudu {

using std::vector;
using std::tr1::shared_ptr;
using master::MiniMaster;
using master::TSDescriptor;
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

  virtual void SetUp() {
    // Make heartbeats faster to speed test runtime.
    FLAGS_heartbeat_interval_ms = 10;

    KuduTest::SetUp();

    cluster_.reset(new MiniCluster(env_.get(), test_dir_, 1));
    ASSERT_STATUS_OK(cluster_->Start());
  }

  virtual void TearDown() {
    ASSERT_STATUS_OK(cluster_->Shutdown());
  }

  void CheckTabletServersPage() {
    EasyCurl c;
    faststring buf;
    string addr = cluster_->mini_master()->bound_http_addr().ToString();
    ASSERT_STATUS_OK(c.FetchURL(strings::Substitute("http://$0/tablet-servers", addr),
                                &buf));

    // Should include the TS UUID
    string expected_uuid = cluster_->mini_tablet_server(0)->server()->instance_pb().permanent_uuid();
    ASSERT_STR_CONTAINS(buf.ToString(), expected_uuid);
  }

 protected:
  gscoped_ptr<MiniCluster> cluster_;
  Schema schema_;
};

TEST_F(RegistrationTest, TestTSRegisters) {
  ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(1));

  ASSERT_NO_FATAL_FAILURE(CheckTabletServersPage());

  // Restart the master, so it loses the descriptor, and ensure that the
  // hearbeater thread handles re-registering.
  ASSERT_STATUS_OK(cluster_->mini_master()->Restart());

  ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(1));

  // TODO: when the instance ID / sequence number stuff is implemented,
  // restart the TS and ensure that it re-registers with the newer sequence
  // number.
}


// TODO: this doesn't belong under "RegistrationTest" - rename this file
// to something more appropriate - doesn't seem worth having separate
// whole test suites for registration, tablet reports, etc.
TEST_F(RegistrationTest, TestTabletReports) {
  ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(1));

  MiniTabletServer* ts = cluster_->mini_tablet_server(0);
  string ts_root = cluster_->GetTabletServerFsRoot(0);

  // Add a tablet, make sure it reports itself.
  ASSERT_STATUS_OK(ts->AddTestTablet("tablet-1", schema_));

  vector<TSDescriptor*> locs;
  ASSERT_STATUS_OK(cluster_->WaitForReplicaCount("tablet-1", 1, &locs));
  ASSERT_EQ(1, locs.size());
  LOG(INFO) << "Tablet successfully reported on " << locs[0]->permanent_uuid();

  // Add another tablet, make sure it is reported via incremental.
  ASSERT_STATUS_OK(ts->AddTestTablet("tablet-2", schema_));
  ASSERT_STATUS_OK(cluster_->WaitForReplicaCount("tablet-2", 1, &locs));

  // Shut down the whole system, bring it back up, and make sure the tablets
  // are reported.
  ASSERT_STATUS_OK(ts->Shutdown());
  ASSERT_STATUS_OK(cluster_->mini_master()->Restart());
  ASSERT_STATUS_OK(ts->Start());
  ASSERT_STATUS_OK(cluster_->WaitForReplicaCount("tablet-1", 1, &locs));
  ASSERT_STATUS_OK(cluster_->WaitForReplicaCount("tablet-2", 1, &locs));

  // Restart the TS after clearing its data. On restart, it will send
  // a full tablet report, without any of the tablets. This causes the
  // master to remove the tablet locations.
  LOG(INFO) << "Shutting down TS, clearing data, and restarting it";
  ASSERT_STATUS_OK(ts->Shutdown());
  ASSERT_STATUS_OK(env_->DeleteRecursively(ts_root));
  ASSERT_STATUS_OK(ts->Start());
  ASSERT_STATUS_OK(cluster_->WaitForReplicaCount("tablet-1", 0, &locs));
  ASSERT_STATUS_OK(cluster_->WaitForReplicaCount("tablet-2", 0, &locs));
}

} // namespace kudu
