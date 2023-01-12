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
//
// Integration test for flexible partitioning (eg buckets, range partitioning
// of PK subsets, etc).

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::rpc::RpcController;
using kudu::tablet::TabletReplica;
using kudu::tserver::NewScanRequestPB;
using kudu::tserver::ScanResponsePB;
using kudu::tserver::ScanRequestPB;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace itest {

static const char* const kTableName = "test-table";
static const int kNumTabletServers = 3;
static const int kNumRows = 200;

class AutoIncrementingItest : public tserver::TabletServerTestBase {
 public:

  void SetUp() override {
    TabletServerTestBase::SetUp();
    cluster::InternalMiniClusterOptions opts;
    opts.num_tablet_servers = kNumTabletServers;
    cluster_.reset(new cluster::InternalMiniCluster(env_, opts));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }

  Status CreateTableWithData() {
    KuduSchemaBuilder b;
    b.AddColumn("c0")->Type(KuduColumnSchema::INT32)->NotNull()->NonUniquePrimaryKey();
    RETURN_NOT_OK(b.Build(&kudu_schema_));

    // Create a table with a range partition
    int lower_bound = 0;
    int upper_bound = 200;
    unique_ptr<KuduPartialRow> lower(kudu_schema_.NewRow());
    unique_ptr<KuduPartialRow> upper(kudu_schema_.NewRow());

    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    RETURN_NOT_OK(lower->SetInt32("c0", lower_bound));
    RETURN_NOT_OK(upper->SetInt32("c0", upper_bound));

    return(table_creator->table_name(kTableName)
           .schema(&kudu_schema_)
           .set_range_partition_columns({"c0"})
           .add_range_partition(lower.release(), upper.release())
           .num_replicas(3)
           .Create());
  }

  Status InsertData() {
    RETURN_NOT_OK(client_->OpenTable(kTableName, &table_));
    shared_ptr<KuduSession> session(client_->NewSession());
    for (int i = 0; i < kNumRows; i++) {
      unique_ptr<KuduInsert> insert(table_->NewInsert());
      KuduPartialRow* row = insert->mutable_row();
      RETURN_NOT_OK(row->SetInt32("c0", i));
      RETURN_NOT_OK(session->Apply(insert.release()));
    }
    return Status::OK();
  }

  // Returns a scan response from the tablet on the given tablet server.
  Status ScanTablet(int ts, const string& tablet_id, vector<string>* results) {
    ScanResponsePB resp;
    RpcController rpc;
    ScanRequestPB req;

    NewScanRequestPB* scan = req.mutable_new_scan_request();
    scan->set_tablet_id(tablet_id);
    scan->set_read_mode(READ_LATEST);

    Schema schema = Schema({ ColumnSchema("c0", INT32),
                             ColumnSchema(Schema::GetAutoIncrementingColumnName(),
                                          INT64, false,false, true),
                           },2);
    RETURN_NOT_OK(SchemaToColumnPBs(schema, scan->mutable_projected_columns()));
    RETURN_NOT_OK(cluster_->tserver_proxy(ts)->Scan(req, &resp, &rpc));
    StringifyRowsFromResponse(schema, rpc, &resp, results);
    return Status::OK();
  }

  protected:
  unique_ptr<cluster::InternalMiniCluster> cluster_;
  KuduSchema kudu_schema_;
  shared_ptr<KuduClient> client_;
  shared_ptr<KuduTable> table_;
};

TEST_F(AutoIncrementingItest, TestAutoIncrementingItest) {
  // Create a table and insert data
  ASSERT_OK(CreateTableWithData());
  ASSERT_OK(InsertData());
  // Scan all the tablet replicas
  for (int j = 0; j < kNumTabletServers; j++) {
    vector<scoped_refptr<TabletReplica>> replicas;
    auto* server = cluster_->mini_tablet_server(j);
    server->server()->tablet_manager()->GetTabletReplicas(&replicas);
    DCHECK_EQ(1, replicas.size());
    vector<string> results;
    ASSERT_OK(ScanTablet(j, replicas[0]->tablet_id(), &results));
    for (int i = 0; i < kNumRows; i++) {
      ASSERT_EQ(Substitute("(int32 c0=$0, int64 $1=$2)", i,
                           Schema::GetAutoIncrementingColumnName(), i + 1), results[i]);
    }
  }
}
} // namespace itest
} // namespace kudu
