// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>

#include "common/schema.h"
#include "common/wire_protocol-test-util.h"
#include "gutil/gscoped_ptr.h"
#include "integration-tests/mini_cluster.h"
#include "master/mini_master.h"
#include "master/master.h"
#include "master/master.pb.h"
#include "master/master-test-util.h"
#include "master/ts_descriptor.h"
#include "tablet/tablet_peer.h"
#include "tserver/mini_tablet_server.h"
#include "tserver/tablet_server.h"
#include "tserver/ts_tablet_manager.h"
#include "util/faststring.h"
#include "util/test_util.h"
#include "util/stopwatch.h"

DECLARE_int32(heartbeat_interval_ms);

namespace kudu {

using std::vector;
using std::tr1::shared_ptr;
using master::MiniMaster;
using master::AlterTableRequestPB;
using master::AlterTableResponsePB;
using master::IsAlterTableDoneRequestPB;
using master::IsAlterTableDoneResponsePB;
using master::TabletLocationsPB;
using master::TSDescriptor;
using tablet::TabletPeer;
using tserver::MiniTabletServer;

class AlterTableTest : public KuduTest {
 public:
  AlterTableTest()
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
    ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(1));



    // Add a tablet, make sure it reports itself.
    CreateTabletForTesting(cluster_->mini_master(), kTableName, schema_, &tablet_id_);

    TabletLocationsPB locs;
    ASSERT_STATUS_OK(cluster_->WaitForReplicaCount(tablet_id_, 1, &locs));
    ASSERT_EQ(1, locs.replicas_size());
    LOG(INFO) << "Tablet successfully reported on " << locs.replicas(0).ts_info().permanent_uuid();

    ASSERT_TRUE(cluster_->mini_tablet_server(0)->server()->tablet_manager()->LookupTablet(
                tablet_id_, &tablet_peer_));
  }

  virtual void TearDown() {
    cluster_->Shutdown();
  }

  void RestartTabletServer() {
    if (cluster_->mini_tablet_server(0)->server() != NULL) {
      cluster_->mini_tablet_server(0)->Shutdown();
    }

    ASSERT_STATUS_OK(cluster_->mini_tablet_server(0)->Start());
    ASSERT_STATUS_OK(cluster_->mini_tablet_server(0)->WaitStarted());

    ASSERT_TRUE(cluster_->mini_tablet_server(0)->server()->tablet_manager()->LookupTablet(
                tablet_id_, &tablet_peer_));
  }

  Status AlterTable(const AlterTableRequestPB& alter_req) {
    return AlterTable(alter_req, 50);
  }

  Status AlterTable(const AlterTableRequestPB& alter_req, int wait_attempts) {
    AlterTableResponsePB resp;
    RETURN_NOT_OK(
        cluster_->mini_master()->master()->catalog_manager()->AlterTable(&alter_req, &resp, NULL));

    // spin waiting for alter to be complete
    return WaitAlterTableCompletion(alter_req.table().table_name(), wait_attempts);
  }

  Status WaitAlterTableCompletion(const std::string& table_name, int attempts) {
    int wait_time = 1000;
    for (int i = 0; i < attempts; ++i) {
      IsAlterTableDoneRequestPB req;
      IsAlterTableDoneResponsePB resp;

      req.mutable_table()->set_table_name(table_name);
      RETURN_NOT_OK(
        cluster_->mini_master()->master()->catalog_manager()->IsAlterTableDone(&req, &resp, NULL));
      if (resp.done()) {
        return Status::OK();
      }

      usleep(wait_time);
      wait_time = std::min(wait_time * 5 / 4, 1000000);
    }

    return Status::TimedOut("AlterTable not completed within the timeout");
  }

 protected:
  static const char *kTableName;

  gscoped_ptr<MiniCluster> cluster_;
  Schema schema_;

  string tablet_id_;
  std::tr1::shared_ptr<TabletPeer> tablet_peer_;
};

const char *AlterTableTest::kTableName = "fake-table";

// Simple test to verify that the "alter table" command sent and executed
// on the TS handling the tablet of the altered table.
// TODO: create and verify multiple tablets when the client will support that.
TEST_F(AlterTableTest, TestTabletReports) {
  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());

  {
    AlterTableRequestPB req;
    req.mutable_table()->set_table_name(kTableName);

    AlterTableRequestPB::Step *step = req.add_alter_schema_steps();
    step->set_type(AlterTableRequestPB::ADD_COLUMN);
    uint32_t x = 10;
    ColumnSchemaToPB(ColumnSchema("new-u32", UINT32, true, &x),
                     step->mutable_add_column()->mutable_schema());
    ASSERT_STATUS_OK(AlterTable(req));
  }

  ASSERT_EQ(1, tablet_peer_->tablet()->metadata()->schema_version());
}

// Verify that adding an existing column will return an "already present" error
TEST_F(AlterTableTest, TestAddExistingColumn) {
  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());

  {
    AlterTableRequestPB req;
    req.mutable_table()->set_table_name(kTableName);

    AlterTableRequestPB::Step *step = req.add_alter_schema_steps();
    step->set_type(AlterTableRequestPB::ADD_COLUMN);
    ColumnSchemaToPB(ColumnSchema("c1", UINT32, true),
                     step->mutable_add_column()->mutable_schema());
    Status s = AlterTable(req);
    ASSERT_TRUE(s.IsAlreadyPresent());
    ASSERT_STR_CONTAINS(s.ToString(), "The column already exists: c1");
  }

  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());
}

// Verify that adding a NOT NULL column without defaults will return an error
TEST_F(AlterTableTest, TestAddNotNullableColumnWithoutDefaults) {
  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());

  {
    AlterTableRequestPB req;
    req.mutable_table()->set_table_name(kTableName);

    AlterTableRequestPB::Step *step = req.add_alter_schema_steps();
    step->set_type(AlterTableRequestPB::ADD_COLUMN);
    ColumnSchemaToPB(ColumnSchema("c2", UINT32),
                     step->mutable_add_column()->mutable_schema());
    Status s = AlterTable(req);
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "c2 is NOT NULL but does not have a default");
  }

  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());
}

// Verify that the alter command is sent to the TS down on restart
TEST_F(AlterTableTest, TestAlterOnRestart) {
  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());

  // Shutdown the TS
  cluster_->mini_tablet_server(0)->Shutdown();

  // Send the Alter request
  {
    AlterTableRequestPB req;
    req.mutable_table()->set_table_name(kTableName);

    AlterTableRequestPB::Step *step = req.add_alter_schema_steps();
    step->set_type(AlterTableRequestPB::ADD_COLUMN);
    uint32_t x = 10;
    ColumnSchemaToPB(ColumnSchema("new-u32", UINT32, true, &x),
                     step->mutable_add_column()->mutable_schema());
    Status s = AlterTable(req, 20);
    ASSERT_TRUE(s.IsTimedOut());
  }

  // Verify that the Schema is the old one
  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());

  // Restart the TS and wait for the new schema
  RestartTabletServer();
  ASSERT_STATUS_OK(WaitAlterTableCompletion(kTableName, 50));
  ASSERT_EQ(1, tablet_peer_->tablet()->metadata()->schema_version());
}


} // namespace kudu
