// Copyright (c) 2013, Cloudera, inc.

#include "tserver/ts_tablet_manager.h"

#include <boost/assign/list_of.hpp>
#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>

#include "common/schema.h"
#include "server/fsmanager.h"
#include "server/metadata.pb.h"
#include "tablet/tablet_peer.h"
#include "util/test_util.h"

namespace kudu {
namespace tserver {

using metadata::TabletMasterBlockPB;
using tablet::TabletPeer;
using std::tr1::shared_ptr;

class TsTabletManagerTest : public KuduTest {
 public:
  virtual void SetUp() {
    KuduTest::SetUp();

    fs_manager_.reset(new FsManager(env_.get(), GetTestPath("fs-root")));
    ASSERT_STATUS_OK(fs_manager_->CreateInitialFileSystemLayout());

    tablet_manager_.reset(new TSTabletManager(fs_manager_.get()));
    ASSERT_STATUS_OK(tablet_manager_->Init());
  }

 protected:
  void CreateTestMasterBlock(const string& tid, TabletMasterBlockPB* pb) {
    pb->set_tablet_id(tid);
    pb->set_block_a("block-a");
    pb->set_block_b("block-b");
  }

  gscoped_ptr<FsManager> fs_manager_;
  gscoped_ptr<TSTabletManager> tablet_manager_;
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
  const string kTabletId = "my-tablet-id";
  const Schema schema(boost::assign::list_of
                      (ColumnSchema("c1", UINT32)),
                      1);

  // Create a new tablet.
  shared_ptr<TabletPeer> peer;
  ASSERT_STATUS_OK(tablet_manager_->CreateNewTablet(
                     kTabletId, "", "", schema, &peer));
  ASSERT_EQ(kTabletId, peer->tablet()->tablet_id());
  peer.reset();

  // Re-load the tablet manager from the filesystem.
  LOG(INFO) << "Shutting down tablet manager";
  tablet_manager_->Shutdown();
  LOG(INFO) << "Restarting tablet manager";
  tablet_manager_.reset(new TSTabletManager(fs_manager_.get()));
  ASSERT_STATUS_OK(tablet_manager_->Init());

  // Ensure that the tablet got re-loaded and re-opened off disk.
  ASSERT_TRUE(tablet_manager_->LookupTablet(kTabletId, &peer));
  ASSERT_EQ(kTabletId, peer->tablet()->tablet_id());
}

} // namespace tserver
} // namespace kudu
