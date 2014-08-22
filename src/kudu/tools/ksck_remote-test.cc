// Copyright (c) 2014, Cloudera, inc.

#include <gtest/gtest.h>
#include <boost/assign/list_of.hpp>

#include "kudu/client/client.h"
#include "kudu/client/encoded_key.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/mini_master.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/util/test_util.h"

DECLARE_int32(heartbeat_interval_ms);

namespace kudu {
namespace tools {

using client::KuduColumnSchema;
using std::tr1::static_pointer_cast;
using std::tr1::shared_ptr;
using std::vector;
using std::string;

class RemoteKsckTest : public KuduTest {
 public:
  RemoteKsckTest()
      : schema_(boost::assign::list_of
              (KuduColumnSchema("key", KuduColumnSchema::UINT32))
              (KuduColumnSchema("int_val", KuduColumnSchema::UINT32)),
              1) {
  }

  ~RemoteKsckTest() {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    // Speed up testing, saves about 700ms per TEST_F.
    FLAGS_heartbeat_interval_ms = 10;

    MiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    mini_cluster_.reset(new MiniCluster(env_.get(), opts));
    ASSERT_OK(mini_cluster_->Start());

    master_rpc_addr_ = mini_cluster_->mini_master()->bound_rpc_addr().ToString();

    // Connect to the cluster.
    ASSERT_OK(client::KuduClientBuilder()
                     .master_server_addr(master_rpc_addr_)
                     .Build(&client_));

    // Create one table.
    ASSERT_OK(client_->NewTableCreator()
                     ->table_name(kTableName)
                     .schema(&schema_)
                     .num_replicas(3)
                     .split_keys(GenerateSplitKeys())
                     .Create());
    // Make sure we can open the table.
    ASSERT_OK(client_->OpenTable(kTableName, &client_table_));

    master_.reset(new RemoteKsckMaster(master_rpc_addr_));
    cluster_.reset(new KsckCluster(master_));
    ksck_.reset(new Ksck(cluster_));
  }

  virtual void TearDown() OVERRIDE {
    if (mini_cluster_) {
      mini_cluster_->Shutdown();
      mini_cluster_.reset();
    }
    KuduTest::TearDown();
  }

 protected:
  // Generate a set of split keys for tablets used in this test.
  vector<string> GenerateSplitKeys() {
    vector<string> keys;
    for (int i = 0; i < 2; i++) {
      client::KuduEncodedKeyBuilder builder(schema_);
      builder.AddColumnKey(&i);
      gscoped_ptr<client::KuduEncodedKey> key(builder.BuildEncodedKey());
      keys.push_back(key->ToString());
    }
    return keys;
  }


  static const char *kTableName;
  shared_ptr<Ksck> ksck_;

 private:
  string master_rpc_addr_;
  shared_ptr<MiniCluster> mini_cluster_;
  shared_ptr<client::KuduClient> client_;
  client::KuduSchema schema_;
  scoped_refptr<client::KuduTable> client_table_;
  shared_ptr<RemoteKsckMaster> master_;
  shared_ptr<KsckCluster> cluster_;
};

const char *RemoteKsckTest::kTableName = "ksck-test-table";

TEST_F(RemoteKsckTest, TestMasterOk) {
  ASSERT_OK(ksck_->CheckMasterRunning());
}

TEST_F(RemoteKsckTest, TestTabletServersOk) {
  ASSERT_OK(ksck_->CheckMasterRunning());
  ASSERT_OK(ksck_->CheckTabletServersRunning());
}

TEST_F(RemoteKsckTest, TestTableOk) {
  ASSERT_OK(ksck_->CheckMasterRunning());
  ASSERT_OK(ksck_->CheckTabletServersRunning());
  ASSERT_OK(ksck_->CheckTablesConsistency());
}

} // namespace tools
} // namespace kudu
