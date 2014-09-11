// Copyright (c) 2013, Cloudera, inc.

#include "kudu/tserver/ts_tablet_manager.h"

#include <boost/assign/list_of.hpp>
#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>

#include "kudu/common/schema.h"
#include "kudu/master/master.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/server/metadata.pb.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace tserver {

using master::TabletReportPB;
using metadata::QuorumPB;
using tablet::TabletMasterBlockPB;
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

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    mini_server_.reset(
        new MiniTabletServer(env_.get(), GetTestPath("TsTabletManagerTest-fsroot"), 0));
    ASSERT_STATUS_OK(mini_server_->Start());
    mini_server_->FailHeartbeats();

    quorum_ = mini_server_->CreateLocalQuorum();

    tablet_manager_ = mini_server_->server()->tablet_manager();
    fs_manager_ = mini_server_->fs_manager();
  }

  Status CreateNewTablet(const std::string& tablet_id,
                         const std::string& start_key,
                         const std::string& end_key,
                         const Schema& schema,
                         scoped_refptr<tablet::TabletPeer>* out_tablet_peer) {
    scoped_refptr<tablet::TabletPeer> tablet_peer;
    RETURN_NOT_OK(tablet_manager_->CreateNewTablet(tablet_id, tablet_id, start_key, end_key,
                                                   tablet_id,
                                                   SchemaBuilder(schema).Build(),
                                                   quorum_,
                                                   &tablet_peer));
    if (out_tablet_peer) {
      (*out_tablet_peer) = tablet_peer;
    }

    return tablet_peer->WaitUntilRunning(MonoDelta::FromMilliseconds(2000));
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
  scoped_refptr<TabletPeer> peer;
  ASSERT_STATUS_OK(CreateNewTablet(kTabletId, "", "",schema_, &peer));
  ASSERT_EQ(kTabletId, peer->tablet()->tablet_id());
  peer.reset();

  // Re-load the tablet manager from the filesystem.
  LOG(INFO) << "Shutting down tablet manager";
  mini_server_->Shutdown();
  LOG(INFO) << "Restarting tablet manager";
  mini_server_.reset(
      new MiniTabletServer(env_.get(), GetTestPath("TsTabletManagerTest-fsroot"), 0));
  ASSERT_STATUS_OK(mini_server_->Start());
  ASSERT_STATUS_OK(mini_server_->WaitStarted());
  tablet_manager_ = mini_server_->server()->tablet_manager();

  // Ensure that the tablet got re-loaded and re-opened off disk.
  ASSERT_TRUE(tablet_manager_->LookupTablet(kTabletId, &peer));
  ASSERT_EQ(kTabletId, peer->tablet()->tablet_id());
}

static void CheckSequenceNumber(int64_t *seqno,
                                  const TabletReportPB &report) {
  ASSERT_LT(*seqno, report.sequence_number());
  *seqno = report.sequence_number();
}

TEST_F(TsTabletManagerTest, TestTabletReports) {
  TabletReportPB report;
  int64_t seqno = -1;

  // Generate a tablet report before any tablets are loaded. Should be empty.
  tablet_manager_->GenerateFullTabletReport(&report);
  ASSERT_FALSE(report.is_incremental());
  ASSERT_EQ(0, report.updated_tablets().size());
  CheckSequenceNumber(&seqno, report);
  tablet_manager_->MarkTabletReportAcknowledged(report);

  // Another report should now be incremental, but with no changes.
  tablet_manager_->GenerateIncrementalTabletReport(&report);
  ASSERT_TRUE(report.is_incremental());
  ASSERT_EQ(0, report.updated_tablets().size());
  CheckSequenceNumber(&seqno, report);
  tablet_manager_->MarkTabletReportAcknowledged(report);

  // Create a tablet and do another incremental report - should include the tablet.
  ASSERT_STATUS_OK(CreateNewTablet("tablet-1", "", "", schema_, NULL));
  int updated_tablets = 0;
  while (updated_tablets != 1) {
    tablet_manager_->GenerateIncrementalTabletReport(&report);
    updated_tablets = report.updated_tablets().size();
    ASSERT_TRUE(report.is_incremental());
    CheckSequenceNumber(&seqno, report);
  }
  ASSERT_EQ("tablet-1", report.updated_tablets(0).tablet_id());

  // If we don't acknowledge the report, and ask for another incremental report,
  // it should include the tablet again.
  tablet_manager_->GenerateIncrementalTabletReport(&report);
  ASSERT_TRUE(report.is_incremental());
  ASSERT_EQ(1, report.updated_tablets().size());
  ASSERT_EQ("tablet-1", report.updated_tablets(0).tablet_id());
  CheckSequenceNumber(&seqno, report);

  // Now acknowledge the last report, and further incrementals should be empty.
  tablet_manager_->MarkTabletReportAcknowledged(report);
  tablet_manager_->GenerateIncrementalTabletReport(&report);
  ASSERT_TRUE(report.is_incremental());
  ASSERT_EQ(0, report.updated_tablets().size());
  CheckSequenceNumber(&seqno, report);
  tablet_manager_->MarkTabletReportAcknowledged(report);

  // Create a second tablet, and ensure the incremental report shows it.
  ASSERT_STATUS_OK(CreateNewTablet("tablet-2", "", "", schema_, NULL));
  updated_tablets = 0;

  // In this report we might get one or two tablets. We'll definitely
  // have a report from tablet-2, which we just created, but since
  // TabletPeer does not mark tablets dirty until after it commits the
  // initial configuration change, there is a window for tablet-1 to
  // have been marked dirty since the last report.
  while (updated_tablets == 0) {
    tablet_manager_->GenerateIncrementalTabletReport(&report);
    updated_tablets = report.updated_tablets().size();
    ASSERT_TRUE(report.is_incremental());
    CheckSequenceNumber(&seqno, report);
  }

  bool found_tablet_2 = false;
  BOOST_FOREACH(const ::kudu::master::ReportedTabletPB& reported_tablet,
                report.updated_tablets()) {
    if (reported_tablet.tablet_id() == "tablet-2") {
      found_tablet_2  = true;
      break;
    }
  }
  ASSERT_TRUE(found_tablet_2);
  tablet_manager_->MarkTabletReportAcknowledged(report);

  // Asking for a full tablet report should re-report both tablets
  tablet_manager_->GenerateFullTabletReport(&report);
  ASSERT_FALSE(report.is_incremental());
  ASSERT_EQ(2, report.updated_tablets().size());
  CheckSequenceNumber(&seqno, report);
}

} // namespace tserver
} // namespace kudu
