// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>

#include "kudu/client/schema.h"
#include "kudu/client/client.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master-test-util.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/atomic.h"
#include "kudu/util/faststring.h"
#include "kudu/util/test_util.h"
#include "kudu/util/stopwatch.h"

DECLARE_int32(heartbeat_interval_ms);

namespace kudu {

using std::vector;
using std::tr1::shared_ptr;
using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduInsert;
using client::KuduSchema;
using client::KuduSession;
using client::KuduTable;
using master::MiniMaster;
using master::AlterTableRequestPB;
using master::AlterTableResponsePB;
using tablet::TabletPeer;
using tserver::MiniTabletServer;

class AlterTableTest : public KuduTest {
 public:
  AlterTableTest()
    : schema_(boost::assign::list_of
              (KuduColumnSchema("c1", KuduColumnSchema::UINT32)),
              1),
      stop_threads_(false) {
  }

  virtual void SetUp() OVERRIDE {
    // Make heartbeats faster to speed test runtime.
    FLAGS_heartbeat_interval_ms = 10;

    KuduTest::SetUp();

    cluster_.reset(new MiniCluster(env_.get(), MiniClusterOptions()));
    ASSERT_STATUS_OK(cluster_->Start());
    ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(1));

    CHECK_OK(KuduClientBuilder()
             .master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
             .Build(&client_));

    // Add a table, make sure it reports itself.
    CHECK_OK(client_->NewTableCreator()
             ->table_name(kTableName)
             .schema(&schema_)
             .num_replicas(1)
             .Create());

    tablet_peer_ = LookupTabletPeer();
    LOG(INFO) << "Tablet successfully located";
  }

  virtual void TearDown() OVERRIDE {
    cluster_->Shutdown();
  }

  scoped_refptr<TabletPeer> LookupTabletPeer() {
    vector<scoped_refptr<TabletPeer> > peers;
    cluster_->mini_tablet_server(0)->server()->tablet_manager()->GetTabletPeers(&peers);
    CHECK_EQ(1, peers.size());
    return peers[0];
  }

  void RestartTabletServer() {
    if (cluster_->mini_tablet_server(0)->server() != NULL) {
      cluster_->mini_tablet_server(0)->Shutdown();
    }

    ASSERT_STATUS_OK(cluster_->mini_tablet_server(0)->Start());
    ASSERT_STATUS_OK(cluster_->mini_tablet_server(0)->WaitStarted());

    tablet_peer_ = LookupTabletPeer();
  }

  Status WaitAlterTableCompletion(const std::string& table_name, int attempts) {
    int wait_time = 1000;
    for (int i = 0; i < attempts; ++i) {
      bool in_progress;
      RETURN_NOT_OK(client_->IsAlterTableInProgress(table_name, &in_progress));
      if (!in_progress) {
        return Status::OK();
      }

      usleep(wait_time);
      wait_time = std::min(wait_time * 5 / 4, 1000000);
    }

    return Status::TimedOut("AlterTable not completed within the timeout");
  }

  Status AddNewU32Column(const string& table_name,
                         const string& column_name,
                         uint32_t default_value) {
    return AddNewU32Column(table_name, column_name, default_value,
                           MonoDelta::FromSeconds(60));
  }

  Status AddNewU32Column(const string& table_name,
                         const string& column_name,
                         uint32_t default_value,
                         const MonoDelta& timeout) {
    return client_->NewTableAlterer()
      ->table_name(table_name)
      .add_column(column_name, KuduColumnSchema::UINT32, &default_value)
      .timeout(timeout)
      .Alter();
  }

  void InserterThread();

 protected:
  static const char *kTableName;

  gscoped_ptr<MiniCluster> cluster_;
  std::tr1::shared_ptr<KuduClient> client_;

  KuduSchema schema_;

  scoped_refptr<TabletPeer> tablet_peer_;

  AtomicBool stop_threads_;
};

const char *AlterTableTest::kTableName = "fake-table";

// Simple test to verify that the "alter table" command sent and executed
// on the TS handling the tablet of the altered table.
// TODO: create and verify multiple tablets when the client will support that.
TEST_F(AlterTableTest, TestTabletReports) {
  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());
  ASSERT_STATUS_OK(AddNewU32Column(kTableName, "new-u32", 0));
  ASSERT_EQ(1, tablet_peer_->tablet()->metadata()->schema_version());
}

// Verify that adding an existing column will return an "already present" error
TEST_F(AlterTableTest, TestAddExistingColumn) {
  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());

  {
    Status s = AddNewU32Column(kTableName, "c1", 0);
    ASSERT_TRUE(s.IsAlreadyPresent());
    ASSERT_STR_CONTAINS(s.ToString(), "The column already exists: c1");
  }

  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());
}

// Verify that adding a NOT NULL column without defaults will return an error.
//
// This doesn't use the KuduClient because it's trying to make an invalid request.
// Our APIs for the client are designed such that it's impossible to send such
// a request.
TEST_F(AlterTableTest, TestAddNotNullableColumnWithoutDefaults) {
  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());

  {
    AlterTableRequestPB req;
    req.mutable_table()->set_table_name(kTableName);

    AlterTableRequestPB::Step *step = req.add_alter_schema_steps();
    step->set_type(AlterTableRequestPB::ADD_COLUMN);
    ColumnSchemaToPB(ColumnSchema("c2", UINT32),
                     step->mutable_add_column()->mutable_schema());
    AlterTableResponsePB resp;
    Status s = cluster_->mini_master()->master()->catalog_manager()->AlterTable(
      &req, &resp, NULL);
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "c2 is NOT NULL but does not have a default");
  }

  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());
}

// Verify that the alter command is sent to the TS down on restart
TEST_F(AlterTableTest, TestAlterOnTSRestart) {
  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());

  // Shutdown the TS
  cluster_->mini_tablet_server(0)->Shutdown();

  // Send the Alter request
  {
    Status s = AddNewU32Column(kTableName, "new-u32", 10,
                               MonoDelta::FromMilliseconds(500));
    ASSERT_TRUE(s.IsTimedOut());
  }

  // Verify that the Schema is the old one
  KuduSchema schema;
  bool alter_in_progress = false;
  ASSERT_STATUS_OK(client_->GetTableSchema(kTableName, &schema));
  ASSERT_TRUE(schema_.Equals(schema));
  ASSERT_STATUS_OK(client_->IsAlterTableInProgress(kTableName, &alter_in_progress))
  ASSERT_TRUE(alter_in_progress);

  // Restart the TS and wait for the new schema
  RestartTabletServer();
  ASSERT_STATUS_OK(WaitAlterTableCompletion(kTableName, 50));
  ASSERT_EQ(1, tablet_peer_->tablet()->metadata()->schema_version());
}

// Verify that nothing is left behind on cluster shutdown with pending async tasks
TEST_F(AlterTableTest, TestShutdownWithPendingTasks) {
  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());

  // Shutdown the TS
  cluster_->mini_tablet_server(0)->Shutdown();

  // Send the Alter request
  {
    Status s = AddNewU32Column(kTableName, "new-u32", 10,
                               MonoDelta::FromMilliseconds(500));
    ASSERT_TRUE(s.IsTimedOut());
  }
}

// Verify that the new schema is applied/reported even when
// the TS is going down with the alter operation in progress.
// On TS restart the master should:
//  - get the new schema state, and mark the alter as complete
//  - get the old schema state, and ask the TS again to perform the alter.
TEST_F(AlterTableTest, TestRestartTSDuringAlter) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipping slow test";
    return;
  }

  ASSERT_EQ(0, tablet_peer_->tablet()->metadata()->schema_version());

  Status s = AddNewU32Column(kTableName, "new-u32", 10,
                             MonoDelta::FromMilliseconds(1));
  ASSERT_TRUE(s.IsTimedOut());

  // Restart the TS while alter is running
  for (int i = 0; i < 3; i++) {
    usleep(500);
    RestartTabletServer();
  }

  // Wait for the new schema
  ASSERT_STATUS_OK(WaitAlterTableCompletion(kTableName, 50));
  ASSERT_EQ(1, tablet_peer_->tablet()->metadata()->schema_version());
}

TEST_F(AlterTableTest, TestGetSchemaAfterAlterTable) {
  ASSERT_STATUS_OK(AddNewU32Column(kTableName, "new-u32", 10));

  KuduSchema s;
  ASSERT_STATUS_OK(client_->GetTableSchema(kTableName, &s));
}

void AlterTableTest::InserterThread() {
  shared_ptr<KuduSession> session = client_->NewSession();
  scoped_refptr<KuduTable> table;
  CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(15 * 1000);

  CHECK_OK(client_->OpenTable(kTableName, &table));
  uint32_t i = 0;
  while (!stop_threads_.Load()) {
    gscoped_ptr<KuduInsert> insert = table->NewInsert();
    // Endian-swap the key so that we spew inserts randomly
    // instead of just a sequential write pattern. This way
    // compactions may actually be triggered.
    uint32_t key = bswap_32(i++);
    CHECK_OK(insert->mutable_row()->SetUInt32(0, key));
    CHECK_OK(session->Apply(insert.Pass()));

    if (i % 50 == 0) {
      CHECK_OK(session->Flush());
      CHECK_EQ(0, session->CountPendingErrors());
    }
  }

  CHECK_OK(session->Flush());
}

// Test altering a table while also sending a lot of writes,
// checking for races between the two.
TEST_F(AlterTableTest, TestAlterUnderWriteLoad) {
  scoped_refptr<Thread> writer;
  CHECK_OK(Thread::Create("test", "writer",
                          boost::bind(&AlterTableTest::InserterThread, this),
                          &writer));

  // Add columns until we reach 10.
  for (int i = 2; i < 10; i++) {
    if (AllowSlowTests()) {
      // In slow test mode, let more writes accumulate in between
      // alters, so that we get enough writes to cause flushes,
      // compactions, etc.
      sleep(3);
    }

    ASSERT_STATUS_OK(AddNewU32Column(kTableName,
                                     strings::Substitute("c$0", i),
                                     i));
  }

  stop_threads_.Store(true);
  writer->Join();
}

} // namespace kudu
