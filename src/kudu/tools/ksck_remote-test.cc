// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gtest/gtest.h>
#include <boost/assign/list_of.hpp>

#include "kudu/client/client.h"
#include "kudu/client/encoded_key.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/mini_master.h"
#include "kudu/tools/data_gen_util.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/test_util.h"

DECLARE_int32(heartbeat_interval_ms);

namespace kudu {
namespace tools {

using client::KuduColumnSchema;
using client::KuduInsert;
using client::KuduSession;
using client::KuduTable;
using std::tr1::static_pointer_cast;
using std::tr1::shared_ptr;
using std::vector;
using std::string;

static const char *kTableName = "ksck-test-table";

class RemoteKsckTest : public KuduTest {
 public:
  RemoteKsckTest()
      : schema_(boost::assign::list_of
              (KuduColumnSchema("key", KuduColumnSchema::INT32))
              (KuduColumnSchema("int_val", KuduColumnSchema::INT32)),
              1),
        random_(SeedRandom()) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    // Speed up testing, saves about 700ms per TEST_F.
    FLAGS_heartbeat_interval_ms = 10;

    MiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    mini_cluster_.reset(new MiniCluster(env_.get(), opts));
    ASSERT_OK(mini_cluster_->Start());

    master_rpc_addr_ = mini_cluster_->mini_master()->bound_rpc_addr_str();

    // Connect to the cluster.
    ASSERT_OK(client::KuduClientBuilder()
                     .add_master_server_addr(master_rpc_addr_)
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
    vector<int> split_nums = boost::assign::list_of(33)(66);
    BOOST_FOREACH(int i, split_nums) {
      client::KuduEncodedKeyBuilder builder(schema_);
      builder.AddColumnKey(&i);
      gscoped_ptr<client::KuduEncodedKey> key(builder.BuildEncodedKey());
      keys.push_back(key->ToString());
    }
    return keys;
  }

  Status GenerateRowWrites(uint64_t num_rows) {
    scoped_refptr<KuduTable> table;
    RETURN_NOT_OK(client_->OpenTable(kTableName, &table));
    shared_ptr<KuduSession> session(client_->NewSession());
    session->SetTimeoutMillis(10000);
    RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    for (uint64_t i = 0; i < num_rows; i++) {
      VLOG(1) << "Generating write for row id " << i;
      gscoped_ptr<KuduInsert> insert = table->NewInsert();
      GenerateDataForRow(table->schema(), i, &random_, insert->mutable_row());
      RETURN_NOT_OK(session->Apply(insert.Pass()));
    }
    RETURN_NOT_OK(session->Flush());
    return Status::OK();
  }

  shared_ptr<Ksck> ksck_;

 private:
  string master_rpc_addr_;
  shared_ptr<MiniCluster> mini_cluster_;
  shared_ptr<client::KuduClient> client_;
  client::KuduSchema schema_;
  scoped_refptr<client::KuduTable> client_table_;
  shared_ptr<RemoteKsckMaster> master_;
  shared_ptr<KsckCluster> cluster_;
  Random random_;
};

TEST_F(RemoteKsckTest, TestMasterOk) {
  ASSERT_OK(ksck_->CheckMasterRunning());
}

TEST_F(RemoteKsckTest, TestTabletServersOk) {
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  ASSERT_OK(ksck_->CheckTabletServersRunning());
}

TEST_F(RemoteKsckTest, TestTableConsistency) {
  Status s;
  // We may have to sleep and loop because it takes some time for the
  // tablet leader to be elected and report back to the Master.
  for (int i = 1; i <= 10; i++) {
    LOG(INFO) << "Consistency check attempt " << i << "...";
    SleepFor(MonoDelta::FromMilliseconds(700));
    ASSERT_OK(ksck_->FetchTableAndTabletInfo());
    s = ksck_->CheckTablesConsistency();
    if (s.ok()) break;
  }
  ASSERT_OK(s);
}

TEST_F(RemoteKsckTest, TestChecksum) {
  uint64_t num_writes = 100;
  LOG(INFO) << "Generating row writes...";
  ASSERT_OK(GenerateRowWrites(num_writes));
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  Status s;
  // We may have to sleep and loop because it may take a little while for all
  // followers to sync up with the leader.
  for (int i = 1; i <= 10; i++) {
    LOG(INFO) << "Checksum attempt " << i << "...";
    SleepFor(MonoDelta::FromMilliseconds(700));
    s = ksck_->ChecksumData(vector<string>(),
                            vector<string>(),
                            ChecksumOptions(MonoDelta::FromSeconds(1), 16));
    if (s.ok()) break;
  }
  ASSERT_OK(s);
}

TEST_F(RemoteKsckTest, TestChecksumTimeout) {
  uint64_t num_writes = 100;
  LOG(INFO) << "Generating row writes...";
  ASSERT_OK(GenerateRowWrites(num_writes));
  ASSERT_OK(ksck_->FetchTableAndTabletInfo());
  // Use an impossibly low timeout value of zero!
  Status s = ksck_->ChecksumData(vector<string>(),
                                 vector<string>(),
                                 ChecksumOptions(MonoDelta::FromNanoseconds(0), 16));
  ASSERT_TRUE(s.IsTimedOut()) << "Expected TimedOut Status, got: " << s.ToString();
}

} // namespace tools
} // namespace kudu
