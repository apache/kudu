// Copyright (c) 2013, Cloudera, inc.

#include "tserver/ts_tablet_manager.h"

#include <boost/assign/list_of.hpp>
#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>

#include "common/schema.h"
#include "master/master.pb.h"
#include "server/fsmanager.h"
#include "server/metadata.pb.h"
#include "tablet/tablet_peer.h"
#include "tserver/mini_tablet_server.h"
#include "tserver/tablet_server.h"
#include "util/metrics.h"
#include "util/test_util.h"

namespace kudu {
namespace tserver {

using master::TabletReportPB;
using metadata::QuorumPB;
using metadata::TabletMasterBlockPB;
using tablet::TabletPeer;
using std::tr1::shared_ptr;

static const char* const kTabletId = "my-tablet-id";

class TsTabletManagerTest : public KuduTest {
 public:
  TsTabletManagerTest()
    : schema_(boost::assign::list_of
             (ColumnSchema("key", UINT32)),
             1),
      metric_ctx_(&metric_registry_, "ts_tablet_manager_test") {
  }

  virtual void SetUp() {
    KuduTest::SetUp();

    quorum_.set_seqno(0);

    mini_server_.reset(new MiniTabletServer(env_.get(), GetTestPath("TsTabletManagerTest-fsroot")));
    ASSERT_STATUS_OK(mini_server_->Start());

    tablet_manager_ = mini_server_->server()->tablet_manager();
    fs_manager_ = mini_server_->fs_manager();
  }

  Status CreateNewTablet(const std::string& tablet_id,
                         const std::string& start_key,
                         const std::string& end_key,
                         const Schema& schema,
                         std::tr1::shared_ptr<tablet::TabletPeer>* tablet_peer) {
    return tablet_manager_->CreateNewTablet(tablet_id, tablet_id, start_key, end_key,
                                            SchemaBuilder(schema).Build(),
                                            quorum_,
                                            tablet_peer);
  }

 protected:
  void CreateTestMasterBlock(const string& tid, TabletMasterBlockPB* pb) {
    pb->set_table_id("table");
    pb->set_tablet_id(tid);
    pb->set_block_a("block-a");
    pb->set_block_b("block-b");
  }

  gscoped_ptr<MiniTabletServer> mini_server_;
  FsManager* fs_manager_;
  TSTabletManager* tablet_manager_;

  Schema schema_;
  QuorumPB quorum_;
  MetricRegistry metric_registry_;
  MetricContext metric_ctx_;

};

// Test that master blocks can be persisted and loaded back from disk.
TEST_F(TsTabletManagerTest, TestPersistBlocks) {
  const string kTabletA = "tablet-a";
  const string kTabletB = "tablet-b";

  // Persist two master blocks.
  TabletMasterBlockPB mb_a, mb_b;
  CreateTestMasterBlock(kTabletA, &mb_a);
  CreateTestMasterBlock(kTabletB, &mb_b);
  ASSERT_STATUS_OK(tablet_manager_->PersistMasterBlock(mb_a));
  ASSERT_STATUS_OK(tablet_manager_->PersistMasterBlock(mb_b));

  // Read them back and make sure they match what we persisted.
  TabletMasterBlockPB read_a, read_b;
  ASSERT_STATUS_OK(tablet_manager_->LoadMasterBlock(kTabletA, &read_a));
  ASSERT_STATUS_OK(tablet_manager_->LoadMasterBlock(kTabletB, &read_b));
  ASSERT_EQ(mb_a.ShortDebugString(), read_a.ShortDebugString());
  ASSERT_EQ(mb_b.ShortDebugString(), read_b.ShortDebugString());
}

TEST_F(TsTabletManagerTest, TestCreateTablet) {
  // Create a new tablet.
  shared_ptr<TabletPeer> peer;
  ASSERT_STATUS_OK(CreateNewTablet(kTabletId, "", "",schema_, &peer));
  ASSERT_EQ(kTabletId, peer->tablet()->tablet_id());
  peer.reset();

  // Re-load the tablet manager from the filesystem.
  LOG(INFO) << "Shutting down tablet manager";
  mini_server_->Shutdown();
  LOG(INFO) << "Restarting tablet manager";
  mini_server_.reset(new MiniTabletServer(env_.get(), GetTestPath("TsTabletManagerTest-fsroot")));
  ASSERT_STATUS_OK(mini_server_->Start());
  ASSERT_STATUS_OK(mini_server_->WaitStarted());
  tablet_manager_ = mini_server_->server()->tablet_manager();

  // Ensure that the tablet got re-loaded and re-opened off disk.
  ASSERT_TRUE(tablet_manager_->LookupTablet(kTabletId, &peer));
  ASSERT_EQ(kTabletId, peer->tablet()->tablet_id());
}

TEST_F(TsTabletManagerTest, TestTabletReports) {
  TabletReportPB report;

  // Generate a tablet report before any tablets are loaded. Should be empty.
  tablet_manager_->GenerateFullTabletReport(&report);
  ASSERT_FALSE(report.is_incremental());
  ASSERT_EQ(0, report.updated_tablets().size());
  ASSERT_EQ(0, report.sequence_number());
  tablet_manager_->AcknowledgeTabletReport(report);

  // Another report should now be incremental, but with no changes.
  tablet_manager_->GenerateTabletReport(&report);
  ASSERT_TRUE(report.is_incremental());
  ASSERT_EQ(0, report.updated_tablets().size());
  ASSERT_EQ(1, report.sequence_number());
  tablet_manager_->AcknowledgeTabletReport(report);

  // Create a tablet and do another incremental report - should include the tablet.
  ASSERT_STATUS_OK(CreateNewTablet("tablet-1", "", "", schema_, NULL));
  tablet_manager_->GenerateTabletReport(&report);
  ASSERT_TRUE(report.is_incremental());
  ASSERT_EQ(1, report.updated_tablets().size());
  ASSERT_EQ("tablet-1", report.updated_tablets(0).tablet_id());
  ASSERT_EQ(2, report.sequence_number());

  // If we don't acknowledge the report, and ask for another incremental report,
  // it should include the tablet again.
  tablet_manager_->GenerateTabletReport(&report);
  ASSERT_TRUE(report.is_incremental());
  ASSERT_EQ(1, report.updated_tablets().size());
  ASSERT_EQ("tablet-1", report.updated_tablets(0).tablet_id());
  ASSERT_EQ(3, report.sequence_number());

  // Now acknowledge the last report, and further incrementals should be empty.
  tablet_manager_->AcknowledgeTabletReport(report);
  tablet_manager_->GenerateTabletReport(&report);
  ASSERT_TRUE(report.is_incremental());
  ASSERT_EQ(0, report.updated_tablets().size());
  ASSERT_EQ(4, report.sequence_number());
  tablet_manager_->AcknowledgeTabletReport(report);

  // Create a second tablet, and ensure the incremental report shows it.
  ASSERT_STATUS_OK(CreateNewTablet("tablet-2", "", "", schema_, NULL));
  tablet_manager_->GenerateTabletReport(&report);
  ASSERT_TRUE(report.is_incremental());
  ASSERT_EQ(1, report.updated_tablets().size());
  ASSERT_EQ("tablet-2", report.updated_tablets(0).tablet_id());
  ASSERT_EQ(5, report.sequence_number());
  tablet_manager_->AcknowledgeTabletReport(report);

  // Asking for a full tablet report should re-report both tablets
  tablet_manager_->GenerateFullTabletReport(&report);
  ASSERT_FALSE(report.is_incremental());
  ASSERT_EQ(2, report.updated_tablets().size());
  ASSERT_EQ(6, report.sequence_number());
}

} // namespace tserver
} // namespace kudu
