// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/rpc/messenger.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/server/rpc_server.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

using std::string;
using std::shared_ptr;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::security::Cert;
using kudu::security::DataFormat;
using kudu::security::PrivateKey;

namespace kudu {
namespace master {

class SysCatalogTest : public KuduTest {
 protected:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    // Start master
    mini_master_.reset(new MiniMaster(Env::Default(), GetTestPath("Master"), 0));
    ASSERT_OK(mini_master_->Start());
    master_ = mini_master_->master();
    ASSERT_OK(master_->WaitUntilCatalogManagerIsLeaderAndReadyForTests(MonoDelta::FromSeconds(5)));

    // Create a client proxy to it.
    MessengerBuilder bld("Client");
    ASSERT_OK(bld.Build(&client_messenger_));
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

class TableLoader : public TableVisitor {
 public:
  TableLoader() {}
  ~TableLoader() { Reset(); }

  void Reset() {
    for (TableInfo* ti : tables) {
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
  return SecureDebugString(a) == SecureDebugString(b);
}

template<class C>
static bool MetadatasEqual(C* ti_a, C* ti_b) {
  MetadataLock<C> l_a(ti_a, MetadataLock<C>::READ);
  MetadataLock<C> l_b(ti_a, MetadataLock<C>::READ);
  return PbEquals(l_a.data().pb, l_b.data().pb);
}

// Test the sys-catalog tables basic operations (add, update, delete,
// visit)
TEST_F(SysCatalogTest, TestSysCatalogTablesOperations) {
  TableLoader loader;
  ASSERT_OK(master_->catalog_manager()->sys_catalog()->VisitTables(&loader));
  ASSERT_EQ(0, loader.tables.size());

  // Create new table.
  scoped_refptr<TableInfo> table(new TableInfo("abc"));
  {
    TableMetadataLock l(table.get(), TableMetadataLock::WRITE);
    l.mutable_data()->pb.set_name("testtb");
    l.mutable_data()->pb.set_version(0);
    l.mutable_data()->pb.set_num_replicas(1);
    l.mutable_data()->pb.set_state(SysTablesEntryPB::PREPARING);
    ASSERT_OK(SchemaToPB(Schema(), l.mutable_data()->pb.mutable_schema()));
    // Add the table
    SysCatalogTable::Actions actions;
    actions.table_to_add = table.get();
    ASSERT_OK(master_->catalog_manager()->sys_catalog()->Write(actions));
    l.Commit();
  }

  // Verify it showed up.
  loader.Reset();
  ASSERT_OK(master_->catalog_manager()->sys_catalog()->VisitTables(&loader));
  ASSERT_EQ(1, loader.tables.size());
  ASSERT_TRUE(MetadatasEqual(table.get(), loader.tables[0]));

  // Update the table
  {
    TableMetadataLock l(table.get(), TableMetadataLock::WRITE);
    l.mutable_data()->pb.set_version(1);
    l.mutable_data()->pb.set_state(SysTablesEntryPB::REMOVED);
    SysCatalogTable::Actions actions;
    actions.table_to_update = table.get();
    ASSERT_OK(master_->catalog_manager()->sys_catalog()->Write(actions));
    l.Commit();
  }

  loader.Reset();
  ASSERT_OK(master_->catalog_manager()->sys_catalog()->VisitTables(&loader));
  ASSERT_EQ(1, loader.tables.size());
  ASSERT_TRUE(MetadatasEqual(table.get(), loader.tables[0]));

  // Delete the table
  loader.Reset();
  SysCatalogTable::Actions actions;
  actions.table_to_delete = table.get();
  ASSERT_OK(master_->catalog_manager()->sys_catalog()->Write(actions));
  ASSERT_OK(master_->catalog_manager()->sys_catalog()->VisitTables(&loader));
  ASSERT_EQ(0, loader.tables.size());
}

// Verify that data mutations are not available from metadata() until commit.
TEST_F(SysCatalogTest, TestTableInfoCommit) {
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
  writer_lock.mutable_data()->set_state(SysTablesEntryPB::RUNNING, "running");


  {
    TableMetadataLock reader_lock(table.get(), TableMetadataLock::READ);
    ASSERT_NE("foo", reader_lock.data().pb.name());
    ASSERT_NE("running", reader_lock.data().pb.state_msg());
    ASSERT_NE(SysTablesEntryPB::RUNNING, reader_lock.data().pb.state());
  }

  // Commit the changes
  writer_lock.Commit();

  // Verify that the data is visible
  {
    TableMetadataLock reader_lock(table.get(), TableMetadataLock::READ);
    ASSERT_EQ("foo", reader_lock.data().pb.name());
    ASSERT_EQ("running", reader_lock.data().pb.state_msg());
    ASSERT_EQ(SysTablesEntryPB::RUNNING, reader_lock.data().pb.state());
  }
}

class TabletLoader : public TabletVisitor {
 public:
  TabletLoader() {}
  ~TabletLoader() { Reset(); }

  void Reset() {
    for (TabletInfo* ti : tablets) {
      ti->Release();
    }
    tablets.clear();
  }

  virtual Status VisitTablet(const std::string& table_id,
                             const std::string& tablet_id,
                             const SysTabletsEntryPB& metadata) OVERRIDE {
    // Setup the tablet info
    TabletInfo *tablet = new TabletInfo(nullptr, tablet_id);
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
  l.mutable_data()->pb.set_state(SysTabletsEntryPB::PREPARING);
  l.mutable_data()->pb.mutable_partition()->set_partition_key_start(start_key);
  l.mutable_data()->pb.mutable_partition()->set_partition_key_end(end_key);
  l.mutable_data()->pb.set_table_id(table->id());
  l.Commit();
  return tablet;
}

// Test the sys-catalog tablets basic operations (add, update, delete,
// visit)
TEST_F(SysCatalogTest, TestSysCatalogTabletsOperations) {
  scoped_refptr<TableInfo> table(new TableInfo("abc"));
  scoped_refptr<TabletInfo> tablet1(CreateTablet(table.get(), "123", "a", "b"));
  scoped_refptr<TabletInfo> tablet2(CreateTablet(table.get(), "456", "b", "c"));
  scoped_refptr<TabletInfo> tablet3(CreateTablet(table.get(), "789", "c", "d"));

  SysCatalogTable* sys_catalog = master_->catalog_manager()->sys_catalog();

  TabletLoader loader;
  ASSERT_OK(master_->catalog_manager()->sys_catalog()->VisitTablets(&loader));
  ASSERT_EQ(0, loader.tablets.size());

  // Add tablet1 and tablet2
  {
    std::vector<TabletInfo*> tablets;
    tablets.push_back(tablet1.get());
    tablets.push_back(tablet2.get());

    loader.Reset();
    TabletMetadataLock l1(tablet1.get(), TabletMetadataLock::WRITE);
    TabletMetadataLock l2(tablet2.get(), TabletMetadataLock::WRITE);
    SysCatalogTable::Actions actions;
    actions.tablets_to_add = tablets;
    ASSERT_OK(sys_catalog->Write(actions));
    l1.Commit();
    l2.Commit();

    ASSERT_OK(sys_catalog->VisitTablets(&loader));
    ASSERT_EQ(2, loader.tablets.size());
    ASSERT_TRUE(MetadatasEqual(tablet1.get(), loader.tablets[0]));
    ASSERT_TRUE(MetadatasEqual(tablet2.get(), loader.tablets[1]));
  }

  // Update tablet1
  {
    std::vector<TabletInfo*> tablets;
    tablets.push_back(tablet1.get());

    TabletMetadataLock l1(tablet1.get(), TabletMetadataLock::WRITE);
    l1.mutable_data()->pb.set_state(SysTabletsEntryPB::RUNNING);
    SysCatalogTable::Actions actions;
    actions.tablets_to_update = tablets;
    ASSERT_OK(sys_catalog->Write(actions));
    l1.Commit();

    loader.Reset();
    ASSERT_OK(sys_catalog->VisitTablets(&loader));
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
    l1.mutable_data()->pb.set_state(SysTabletsEntryPB::REPLACED);
    TabletMetadataLock l2(tablet2.get(), TabletMetadataLock::WRITE);
    l2.mutable_data()->pb.set_state(SysTabletsEntryPB::RUNNING);

    loader.Reset();
    SysCatalogTable::Actions actions;
    actions.tablets_to_add = to_add;
    actions.tablets_to_update = to_update;
    ASSERT_OK(sys_catalog->Write(actions));

    l1.Commit();
    l2.Commit();
    l3.Commit();

    ASSERT_OK(sys_catalog->VisitTablets(&loader));
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
    SysCatalogTable::Actions actions;
    actions.tablets_to_delete = tablets;
    ASSERT_OK(master_->catalog_manager()->sys_catalog()->Write(actions));
    ASSERT_OK(master_->catalog_manager()->sys_catalog()->VisitTablets(&loader));
    ASSERT_EQ(1, loader.tablets.size());
    ASSERT_TRUE(MetadatasEqual(tablet2.get(), loader.tablets[0]));
  }
}

// Verify that data mutations are not available from metadata() until commit.
TEST_F(SysCatalogTest, TestTabletInfoCommit) {
  scoped_refptr<TabletInfo> tablet(new TabletInfo(nullptr, "123"));

  // Mutate the tablet, the changes should not be visible
  TabletMetadataLock l(tablet.get(), TabletMetadataLock::WRITE);
  PartitionPB* partition = l.mutable_data()->pb.mutable_partition();
  partition->set_partition_key_start("a");
  partition->set_partition_key_end("b");
  l.mutable_data()->set_state(SysTabletsEntryPB::RUNNING, "running");
  {
    // Changes shouldn't be visible, and lock should still be
    // acquired even though the mutation is under way.
    TabletMetadataLock read_lock(tablet.get(), TabletMetadataLock::READ);
    ASSERT_NE("a", read_lock.data().pb.partition().partition_key_start());
    ASSERT_NE("b", read_lock.data().pb.partition().partition_key_end());
    ASSERT_NE("running", read_lock.data().pb.state_msg());
    ASSERT_NE(SysTabletsEntryPB::RUNNING,
              read_lock.data().pb.state());
  }

  // Commit the changes
  l.Commit();

  // Verify that the data is visible
  {
    TabletMetadataLock read_lock(tablet.get(), TabletMetadataLock::READ);
    ASSERT_EQ("a", read_lock.data().pb.partition().partition_key_start());
    ASSERT_EQ("b", read_lock.data().pb.partition().partition_key_end());
    ASSERT_EQ("running", read_lock.data().pb.state_msg());
    ASSERT_EQ(SysTabletsEntryPB::RUNNING,
              read_lock.data().pb.state());
  }
}

// Check loading the auto-generated certificate authority information
// upon startup.
TEST_F(SysCatalogTest, LoadCertAuthorityInfo) {
  // The system catalog should already contain newly generated CA private key
  // and certificate: the SetUp() method awaits for the catalog manager
  // becoming leader master, and by that time the certificate authority
  // information should be loaded.
  SysCertAuthorityEntryPB ca_entry;
  ASSERT_OK(master_->catalog_manager()->sys_catalog()->
            GetCertAuthorityEntry(&ca_entry));

  // The CA private key data should be valid (DER format).
  PrivateKey pkey;
  EXPECT_OK(pkey.FromString(ca_entry.private_key(), DataFormat::DER));

  // The data should be valid CA certificate in (DER format).
  Cert cert;
  EXPECT_OK(cert.FromString(ca_entry.certificate(), DataFormat::DER));
}

// Check that if the certificate authority information is already present,
// it cannot be overwritten using SysCatalogTable::AddCertAuthorityInfo().
TEST_F(SysCatalogTest, AttemptOverwriteCertAuthorityInfo) {
  // The system catalog should already contain newly generated CA private key
  // and certificate: the SetUp() method awaits for the catalog manager
  // becoming leader master, and by that time the certificate authority
  // information should be loaded.
  SysCertAuthorityEntryPB ca_entry;
  ASSERT_OK(master_->catalog_manager()->sys_catalog()->
            GetCertAuthorityEntry(&ca_entry));
  const Status s = master_->catalog_manager()->sys_catalog()->
      AddCertAuthorityEntry(ca_entry);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_EQ("Corruption: One or more rows failed to write", s.ToString());
}

} // namespace master
} // namespace kudu
