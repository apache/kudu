// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <tr1/memory>
#include <vector>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/sys_tables.h"
#include "kudu/server/rpc_server.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"
#include "kudu/rpc/messenger.h"

using std::string;
using std::tr1::shared_ptr;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;

namespace kudu {
namespace master {

class SysTablesTest : public KuduTest {
 protected:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    // Start master
    mini_master_.reset(new MiniMaster(Env::Default(), GetTestPath("Master"), 0));
    ASSERT_STATUS_OK(mini_master_->Start());
    master_ = mini_master_->master();

    // Create a client proxy to it.
    MessengerBuilder bld("Client");
    ASSERT_STATUS_OK(bld.Build(&client_messenger_));
    proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr()));
  }

  virtual void TearDown() OVERRIDE {
    mini_master_->Shutdown();
    KuduTest::TearDown();
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
    BOOST_FOREACH(TableInfo* ti, tables) {
      ti->Release();
    }
    tables.clear();
  }

  virtual Status VisitTable(const std::string& table_id,
                            const SysTablesEntryPB& metadata) OVERRIDE {
    // Setup the table info
    TableInfo *table = new TableInfo(table_id);
    TableMetadataLock l(table, TableMetadataLock::WRITE);
    l.mutable_data()->pb.CopyFrom(metadata);
    l.Commit();
    table->AddRef();
    tables.push_back(table);
    return Status::OK();
  }

  vector<TableInfo* > tables;
};

static bool PbEquals(const google::protobuf::Message& a, const google::protobuf::Message& b) {
  return a.DebugString() == b.DebugString();
}

template<class C>
static bool MetadatasEqual(C* ti_a, C* ti_b) {
  MetadataLock<C> l_a(ti_a, MetadataLock<C>::READ);
  MetadataLock<C> l_b(ti_a, MetadataLock<C>::READ);
  return PbEquals(l_a.data().pb, l_b.data().pb);
}

// Test the sys-tables basic operations (add, update, delete, visit)
TEST_F(SysTablesTest, TestSysTablesOperations) {
  TableLoader loader;
  ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->VisitTables(&loader));
  ASSERT_EQ(0, loader.tables.size());

  // Create new table.
  scoped_refptr<TableInfo> table(new TableInfo("abc"));
  {
    TableMetadataLock l(table.get(), TableMetadataLock::WRITE);
    l.mutable_data()->pb.set_name("testtb");
    l.mutable_data()->pb.set_version(0);
    l.mutable_data()->pb.set_num_replicas(1);
    l.mutable_data()->pb.set_state(SysTablesEntryPB::kTableStatePreparing);
    ASSERT_STATUS_OK(SchemaToPB(Schema(), l.mutable_data()->pb.mutable_schema()));
    // Add the table
    ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->AddTable(table.get()));
    l.Commit();
  }

  // Verify it showed up.
  loader.Reset();
  ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->VisitTables(&loader));
  ASSERT_EQ(1, loader.tables.size());
  ASSERT_TRUE(MetadatasEqual(table.get(), loader.tables[0]));

  // Update the table
  {
    TableMetadataLock l(table.get(), TableMetadataLock::WRITE);
    l.mutable_data()->pb.set_version(1);
    l.mutable_data()->pb.set_state(SysTablesEntryPB::kTableStateRemoved);
    ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->UpdateTable(table.get()));
    l.Commit();
  }

  loader.Reset();
  ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->VisitTables(&loader));
  ASSERT_EQ(1, loader.tables.size());
  ASSERT_TRUE(MetadatasEqual(table.get(), loader.tables[0]));

  // Delete the table
  loader.Reset();
  ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->DeleteTable(table.get()));
  ASSERT_STATUS_OK(master_->catalog_manager()->sys_tables()->VisitTables(&loader));
  ASSERT_EQ(0, loader.tables.size());
}

// Verify that data mutations are not available from metadata() until commit.
TEST_F(SysTablesTest, TestTableInfoCommit) {
  scoped_refptr<TableInfo> table(new TableInfo("123"));

  // Mutate the table, under the write lock.
  TableMetadataLock writer_lock(table.get(), TableMetadataLock::WRITE);
  writer_lock.mutable_data()->pb.set_name("foo");

  // Changes should not be visible to a reader.
  // The reader can still lock for read, since readers don't block
  // writers in the RWC lock.
  {
    TableMetadataLock reader_lock(table.get(), TableMetadataLock::READ);
    ASSERT_NE("foo", reader_lock.data().name());
  }
  writer_lock.mutable_data()->set_state(SysTablesEntryPB::kTableStateRunning, "running");


  {
    TableMetadataLock reader_lock(table.get(), TableMetadataLock::READ);
    ASSERT_NE("foo", reader_lock.data().pb.name());
    ASSERT_NE("running", reader_lock.data().pb.state_msg());
    ASSERT_NE(SysTablesEntryPB::kTableStateRunning, reader_lock.data().pb.state());
  }

  // Commit the changes
  writer_lock.Commit();

  // Verify that the data is visible
  {
    TableMetadataLock reader_lock(table.get(), TableMetadataLock::READ);
    ASSERT_EQ("foo", reader_lock.data().pb.name());
    ASSERT_EQ("running", reader_lock.data().pb.state_msg());
    ASSERT_EQ(SysTablesEntryPB::kTableStateRunning, reader_lock.data().pb.state());
  }
}

class TabletLoader : public SysTabletsTable::Visitor {
 public:
  TabletLoader() {}
  ~TabletLoader() { Reset(); }

  void Reset() {
    BOOST_FOREACH(TabletInfo* ti, tablets) {
      ti->Release();
    }
    tablets.clear();
  }

  virtual Status VisitTablet(const std::string& table_id,
                             const std::string& tablet_id,
                             const SysTabletsEntryPB& metadata) OVERRIDE {
    // Setup the tablet info
    TabletInfo *tablet = new TabletInfo(NULL, tablet_id);
    TabletMetadataLock l(tablet, TabletMetadataLock::WRITE);
    l.mutable_data()->pb.CopyFrom(metadata);
    l.Commit();
    tablet->AddRef();
    tablets.push_back(tablet);
    return Status::OK();
  }

  vector<TabletInfo *> tablets;
};

// Create a new TabletInfo. The object is in uncommitted
// state.
static TabletInfo *CreateTablet(TableInfo *table,
                                const string& tablet_id,
                                const string& start_key,
                                const string& end_key) {
  TabletInfo *tablet = new TabletInfo(table, tablet_id);
  TabletMetadataLock l(tablet, TabletMetadataLock::WRITE);
  l.mutable_data()->pb.set_state(SysTabletsEntryPB::kTabletStatePreparing);
  l.mutable_data()->pb.set_start_key(start_key);
  l.mutable_data()->pb.set_end_key(end_key);
  l.Commit();
  return tablet;
}

// Test the sys-tablets basic operations (add, update, delete, visit)
TEST_F(SysTablesTest, TestSysTabletsOperations) {
  scoped_refptr<TableInfo> table(new TableInfo("abc"));
  scoped_refptr<TabletInfo> tablet1(CreateTablet(table.get(), "123", "a", "b"));
  scoped_refptr<TabletInfo> tablet2(CreateTablet(table.get(), "456", "b", "c"));
  scoped_refptr<TabletInfo> tablet3(CreateTablet(table.get(), "789", "c", "d"));

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
    TabletMetadataLock l1(tablet1.get(), TabletMetadataLock::WRITE);
    TabletMetadataLock l2(tablet2.get(), TabletMetadataLock::WRITE);
    ASSERT_STATUS_OK(sys_tablets->AddTablets(tablets));
    l1.Commit();
    l2.Commit();

    ASSERT_STATUS_OK(sys_tablets->VisitTablets(&loader));
    ASSERT_EQ(2, loader.tablets.size());
    ASSERT_TRUE(MetadatasEqual(tablet1.get(), loader.tablets[0]));
    ASSERT_TRUE(MetadatasEqual(tablet2.get(), loader.tablets[1]));
  }

  // Update tablet1
  {
    std::vector<TabletInfo*> tablets;
    tablets.push_back(tablet1.get());

    TabletMetadataLock l1(tablet1.get(), TabletMetadataLock::WRITE);
    l1.mutable_data()->pb.set_state(SysTabletsEntryPB::kTabletStateRunning);
    ASSERT_STATUS_OK(sys_tablets->UpdateTablets(tablets));
    l1.Commit();

    loader.Reset();
    ASSERT_STATUS_OK(sys_tablets->VisitTablets(&loader));
    ASSERT_EQ(2, loader.tablets.size());
    ASSERT_TRUE(MetadatasEqual(tablet1.get(), loader.tablets[0]));
    ASSERT_TRUE(MetadatasEqual(tablet2.get(), loader.tablets[1]));
  }

  // Add tablet3 and Update tablet1 and tablet2
  {
    std::vector<TabletInfo *> to_add;
    std::vector<TabletInfo *> to_update;

    TabletMetadataLock l3(tablet3.get(), TabletMetadataLock::WRITE);
    to_add.push_back(tablet3.get());
    to_update.push_back(tablet1.get());
    to_update.push_back(tablet2.get());

    TabletMetadataLock l1(tablet1.get(), TabletMetadataLock::WRITE);
    l1.mutable_data()->pb.set_state(SysTabletsEntryPB::kTabletStateReplaced);
    TabletMetadataLock l2(tablet2.get(), TabletMetadataLock::WRITE);
    l2.mutable_data()->pb.set_state(SysTabletsEntryPB::kTabletStateRunning);

    loader.Reset();
    ASSERT_STATUS_OK(sys_tablets->AddAndUpdateTablets(to_add, to_update));

    l1.Commit();
    l2.Commit();
    l3.Commit();

    ASSERT_STATUS_OK(sys_tablets->VisitTablets(&loader));
    ASSERT_EQ(3, loader.tablets.size());
    ASSERT_TRUE(MetadatasEqual(tablet1.get(), loader.tablets[0]));
    ASSERT_TRUE(MetadatasEqual(tablet2.get(), loader.tablets[1]));
    ASSERT_TRUE(MetadatasEqual(tablet3.get(), loader.tablets[2]));
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
    ASSERT_TRUE(MetadatasEqual(tablet2.get(), loader.tablets[0]));
  }
}

// Verify that data mutations are not available from metadata() until commit.
TEST_F(SysTablesTest, TestTabletInfoCommit) {
  scoped_refptr<TabletInfo> tablet(new TabletInfo(NULL, "123"));

  // Mutate the tablet, the changes should not be visible
  TabletMetadataLock l(tablet.get(), TabletMetadataLock::WRITE);
  l.mutable_data()->pb.set_start_key("a");
  l.mutable_data()->pb.set_end_key("b");
  l.mutable_data()->set_state(SysTabletsEntryPB::kTabletStateRunning, "running");
  {
    // Changes shouldn't be visible, and lock should still be
    // acquired even though the mutation is under way.
    TabletMetadataLock read_lock(tablet.get(), TabletMetadataLock::READ);
    ASSERT_NE("a", read_lock.data().pb.start_key());
    ASSERT_NE("b", read_lock.data().pb.end_key());
    ASSERT_NE("running", read_lock.data().pb.state_msg());
    ASSERT_NE(SysTabletsEntryPB::kTabletStateRunning,
              read_lock.data().pb.state());
  }

  // Commit the changes
  l.Commit();

  // Verify that the data is visible
  {
    TabletMetadataLock read_lock(tablet.get(), TabletMetadataLock::READ);
    ASSERT_EQ("a", read_lock.data().pb.start_key());
    ASSERT_EQ("b", read_lock.data().pb.end_key());
    ASSERT_EQ("running", read_lock.data().pb.state_msg());
    ASSERT_EQ(SysTabletsEntryPB::kTabletStateRunning,
              read_lock.data().pb.state());
  }
}

} // namespace master
} // namespace kudu
