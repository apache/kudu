// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <tr1/memory>
#include <vector>

#include "common/wire_protocol.h"
#include "gutil/stl_util.h"
#include "master/catalog_manager.h"
#include "master/master.h"
#include "master/master.proxy.h"
#include "master/mini_master.h"
#include "master/sys_tables.h"
#include "server/rpc_server.h"
#include "util/net/sockaddr.h"
#include "util/status.h"
#include "util/test_util.h"
#include "rpc/messenger.h"

using std::string;
using std::tr1::shared_ptr;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;

namespace kudu {
namespace master {

class SysTablesTest : public KuduTest {
 protected:
  void SetUp() {
    KuduTest::SetUp();

    // Start master
    mini_master_.reset(new MiniMaster(Env::Default(), GetTestPath("Master")));
    ASSERT_STATUS_OK(mini_master_->Start());
    master_ = mini_master_->master();

    // Create a client proxy to it.
    MessengerBuilder bld("Client");
    ASSERT_STATUS_OK(bld.Build(&client_messenger_));
    proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr()));
  }


  shared_ptr<Messenger> client_messenger_;
  gscoped_ptr<MiniMaster> mini_master_;
  Master* master_;
  gscoped_ptr<MasterServiceProxy> proxy_;
};

class TableLoader : public SysTablesTable::Visitor {
 public:
  TableLoader() {}
  ~TableLoader() { Reset(); }

  void Reset() {
    STLDeleteElements(&tables);
  }

  virtual Status VisitTable(const std::string& table_id,
                            const SysTablesEntryPB& metadata) {
    // Setup the table info
    TableInfo *table = new TableInfo(table_id);
    table->mutable_metadata()->CopyFrom(metadata);
    table->Commit();
    tables.push_back(table);
    return Status::OK();
  }

  vector<TableInfo *> tables;
};

bool PbEquals(const google::protobuf::Message& a, const google::protobuf::Message& b) {
  return a.DebugString() == b.DebugString();
}

// Test the sys-tables basic operations (add, update, delete, visit)
TEST_F(SysTablesTest, TestSysTablesOperations) {
  gscoped_ptr<TableInfo> table(new TableInfo("abc"));
  table->mutable_metadata()->set_name("testtb");
  table->mutable_metadata()->set_version(0);
  table->mutable_metadata()->set_state(SysTablesEntryPB::kTableStatePreparing);
  ASSERT_STATUS_OK(SchemaToPB(Schema(), table->mutable_metadata()->mutable_schema()));
  table->Commit();

  TableLoader loader;
  ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->VisitTables(&loader));
  ASSERT_EQ(0, loader.tables.size());

  // Add the table
  loader.Reset();
  ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->AddTable(table.get()));
  ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->VisitTables(&loader));
  ASSERT_EQ(1, loader.tables.size());
  ASSERT_TRUE(PbEquals(table.get()->metadata(), loader.tables[0]->metadata()));

  // Update the table
  loader.Reset();
  table->mutable_metadata()->set_version(1);
  table->mutable_metadata()->set_state(SysTablesEntryPB::kTableStateRemoved);
  ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->UpdateTable(table.get()));
  table->Commit();
  ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->VisitTables(&loader));
  ASSERT_EQ(1, loader.tables.size());
  ASSERT_TRUE(PbEquals(table.get()->metadata(), loader.tables[0]->metadata()));

  // Delete the table
  loader.Reset();
  ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->DeleteTable(table.get()));
  ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->VisitTables(&loader));
  ASSERT_EQ(0, loader.tables.size());
}

// Verify that data mutations are not available from metadata() until commit.
TEST_F(SysTablesTest, TestTableInfoCommit) {
  gscoped_ptr<TableInfo> table(new TableInfo("123"));

  // Mutate the tablet, the changes should not be visible
  table->mutable_metadata()->set_name("foo");
  ASSERT_NE("foo", table->metadata().name());
  table->set_state(SysTablesEntryPB::kTableStateRunning, "running");
  ASSERT_NE("running", table->metadata().state_msg());
  ASSERT_NE(SysTablesEntryPB::kTableStateRunning, table->metadata().state());
  ASSERT_FALSE(PbEquals(table->metadata(), table->staging_metadata()));

  // Commit the changes
  table->Commit();

  // Verify that the data is visible
  ASSERT_EQ("foo", table->metadata().name());
  ASSERT_EQ("running", table->metadata().state_msg());
  ASSERT_EQ(SysTablesEntryPB::kTableStateRunning, table->metadata().state());
  ASSERT_TRUE(PbEquals(table->metadata(), table->staging_metadata()));
}

class TabletLoader : public SysTabletsTable::Visitor {
 public:
  TabletLoader() {}
  ~TabletLoader() { Reset(); }

  void Reset() {
    STLDeleteElements(&tablets);
  }

  virtual Status VisitTablet(const std::string& table_id,
                             const std::string& tablet_id,
                             const SysTabletsEntryPB& metadata) {
    // Setup the tablet info
    TabletInfo *tablet = new TabletInfo(NULL, tablet_id);
    tablet->mutable_metadata()->CopyFrom(metadata);
    tablet->Commit();
    tablets.push_back(tablet);
    return Status::OK();
  }

  vector<TabletInfo *> tablets;
};

static TabletInfo *CreateTablet(TableInfo *table,
                                const string& tablet_id,
                                const string& start_key,
                                const string& end_key) {
  TabletInfo *tablet = new TabletInfo(table, tablet_id);
  tablet->mutable_metadata()->set_state(SysTabletsEntryPB::kTabletStatePreparing);
  tablet->mutable_metadata()->set_start_key(start_key);
  tablet->mutable_metadata()->set_end_key(end_key);
  tablet->Commit();
  return tablet;
}

// Test the sys-tablets basic operations (add, update, delete, visit)
TEST_F(SysTablesTest, TestSysTabletsOperations) {
  gscoped_ptr<TableInfo> table(new TableInfo("abc"));
  gscoped_ptr<TabletInfo> tablet1(CreateTablet(table.get(), "123", "a", "b"));
  gscoped_ptr<TabletInfo> tablet2(CreateTablet(table.get(), "456", "b", "c"));
  gscoped_ptr<TabletInfo> tablet3(CreateTablet(table.get(), "789", "c", "d"));

  SysTabletsTable *sys_tablets = master_->catalog_manager()->sys_tablets();

  TabletLoader loader;
  ASSERT_STATUS_OK(master_->catalog_manager()->sys_tablets()->VisitTablets(&loader));
  ASSERT_EQ(0, loader.tablets.size());

  // Add tablet1 and tablet2
  {
    std::vector<TabletInfo*> tablets;
    tablets.push_back(tablet1.get());
    tablets.push_back(tablet2.get());

    loader.Reset();
    ASSERT_STATUS_OK(sys_tablets->AddTablets(tablets));
    ASSERT_STATUS_OK(sys_tablets->VisitTablets(&loader));
    ASSERT_EQ(2, loader.tablets.size());
    ASSERT_TRUE(PbEquals(tablet1.get()->metadata(), loader.tablets[0]->metadata()));
    ASSERT_TRUE(PbEquals(tablet2.get()->metadata(), loader.tablets[1]->metadata()));
  }

  // Update tablet1
  {
    std::vector<TabletInfo*> tablets;
    tablets.push_back(tablet1.get());

    tablet1->mutable_metadata()->set_state(SysTabletsEntryPB::kTabletStateRunning);

    loader.Reset();
    ASSERT_STATUS_OK(sys_tablets->UpdateTablets(tablets));
    tablet1->Commit();

    ASSERT_STATUS_OK(sys_tablets->VisitTablets(&loader));
    ASSERT_EQ(2, loader.tablets.size());
    ASSERT_TRUE(PbEquals(tablet1.get()->metadata(), loader.tablets[0]->metadata()));
    ASSERT_TRUE(PbEquals(tablet2.get()->metadata(), loader.tablets[1]->metadata()));
  }

  // Add tablet3 and Update tablet1 and tablet2
  {
    std::vector<TabletInfo *> to_add;
    std::vector<TabletInfo *> to_update;

    to_add.push_back(tablet3.get());
    to_update.push_back(tablet1.get());
    to_update.push_back(tablet2.get());

    tablet1->mutable_metadata()->set_state(SysTabletsEntryPB::kTabletStateReplaced);
    tablet2->mutable_metadata()->set_state(SysTabletsEntryPB::kTabletStateRunning);

    loader.Reset();
    ASSERT_STATUS_OK(sys_tablets->AddAndUpdateTablets(to_add, to_update));
    tablet1->Commit();
    tablet2->Commit();

    ASSERT_STATUS_OK(sys_tablets->VisitTablets(&loader));
    ASSERT_EQ(3, loader.tablets.size());
    ASSERT_TRUE(PbEquals(tablet1.get()->metadata(), loader.tablets[0]->metadata()));
    ASSERT_TRUE(PbEquals(tablet2.get()->metadata(), loader.tablets[1]->metadata()));
    ASSERT_TRUE(PbEquals(tablet3.get()->metadata(), loader.tablets[2]->metadata()));
  }

  // Delete tablet1 and tablet3 tablets
  {
    std::vector<TabletInfo*> tablets;
    tablets.push_back(tablet1.get());
    tablets.push_back(tablet3.get());

    loader.Reset();
    ASSERT_STATUS_OK(master_->catalog_manager()->sys_tablets()->DeleteTablets(tablets));
    ASSERT_STATUS_OK(master_->catalog_manager()->sys_tablets()->VisitTablets(&loader));
    ASSERT_EQ(1, loader.tablets.size());
    ASSERT_TRUE(PbEquals(tablet2.get()->metadata(), loader.tablets[0]->metadata()));
  }
}

// Verify that data mutations are not available from metadata() until commit.
TEST_F(SysTablesTest, TestTabletInfoCommit) {
  gscoped_ptr<TabletInfo> tablet(new TabletInfo(NULL, "123"));

  // Mutate the tablet, the changes should not be visible
  tablet->mutable_metadata()->set_start_key("a");
  ASSERT_NE("a", tablet->metadata().start_key());
  tablet->mutable_metadata()->set_end_key("b");
  ASSERT_NE("b", tablet->metadata().start_key());
  tablet->set_state(SysTabletsEntryPB::kTabletStateRunning, "running");
  ASSERT_NE("running", tablet->metadata().state_msg());
  ASSERT_NE(SysTabletsEntryPB::kTabletStateRunning, tablet->metadata().state());
  ASSERT_FALSE(PbEquals(tablet->metadata(), tablet->staging_metadata()));

  // Commit the changes
  tablet->Commit();

  // Verify that the data is visible
  ASSERT_EQ("a", tablet->metadata().start_key());
  ASSERT_EQ("b", tablet->metadata().end_key());
  ASSERT_EQ("running", tablet->metadata().state_msg());
  ASSERT_EQ(SysTabletsEntryPB::kTabletStateRunning, tablet->metadata().state());
  ASSERT_TRUE(PbEquals(tablet->metadata(), tablet->staging_metadata()));
}

} // namespace master
} // namespace kudu
