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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/client.pb.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scan_configuration.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/scanners.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/async_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(tserver_enforce_access_control);

METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_GetTableSchema);
METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_GetTableLocations);

using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::master::CatalogManager;
using kudu::master::TabletInfo;
using kudu::tablet::TabletReplica;
using kudu::tserver::MiniTabletServer;
using std::atomic;
using std::map;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_set;
using std::vector;

namespace kudu {
namespace client {

static constexpr const int32_t kRecordCount = 1000;
static constexpr const int32_t kBucketNum = 10;

class ScanTokenTest : public KuduTest {
 protected:
  void SetUp() override {
    // Enable access control so we can validate the requests in secure environment.
    // Specifically that authz tokens in the scan tokens work.
    FLAGS_tserver_enforce_access_control = true;

    // Set up the mini cluster
    cluster_.reset(new InternalMiniCluster(env_, InternalMiniClusterOptions()));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }

  // Count the rows in a table which satisfy the specified predicates. Simulates
  // a central query planner / remote task execution by creating a thread per
  // token, each with a new client.
  int CountRows(const vector<KuduScanToken*>& tokens) {
    atomic<uint32_t> rows(0);
    vector<thread> threads;
    for (KuduScanToken* token : tokens) {
      string buf;
      CHECK_OK(token->Serialize(&buf));

      threads.emplace_back([this, &rows] (string serialized_token) {
        shared_ptr<KuduClient> client;
        CHECK_OK(cluster_->CreateClient(nullptr, &client));
        KuduScanner* scanner_ptr;
        CHECK_OK(KuduScanToken::DeserializeIntoScanner(
            client.get(), serialized_token, &scanner_ptr));
        unique_ptr<KuduScanner> scanner(scanner_ptr);
        CHECK_OK(scanner->Open());

        while (scanner->HasMoreRows()) {
          KuduScanBatch batch;
          CHECK_OK(scanner->NextBatch(&batch));
          rows += batch.NumRows();
        }
        scanner->Close();
      }, std::move(buf));
    }

    for (thread& thread : threads) {
      thread.join();
    }

    return rows;
  }

  // Similar to CountRows() above, but use the specified client handle
  // and run all the scanners sequentially, one by one.
  static Status CountRowsSeq(
      KuduClient* client,
      const vector<KuduScanToken*>& tokens,
      int64_t* row_count,
      KuduClient::ReplicaSelection replica_selection = KuduClient::ReplicaSelection::LEADER_ONLY) {
    int64_t count = 0;
    for (auto* t : tokens) {
      unique_ptr<KuduScanner> scanner;
      RETURN_NOT_OK(IntoUniqueScanner(client, *t, &scanner));
      RETURN_NOT_OK(scanner->SetSelection(replica_selection));
      RETURN_NOT_OK(scanner->Open());
      while (scanner->HasMoreRows()) {
        KuduScanBatch batch;
        RETURN_NOT_OK(scanner->NextBatch(&batch));
        count += batch.NumRows();
      }
    }
    *row_count = count;
    return Status::OK();
  }

  void VerifyTabletInfo(const vector<KuduScanToken*>& tokens) {
    unordered_set<string> tablet_ids;
    for (auto t : tokens) {
      tablet_ids.insert(t->tablet().id());

      // Check that there's only one replica; this is a non-replicated table.
      ASSERT_EQ(1, t->tablet().replicas().size());

      // Check that this replica is a leader; since there's only one tserver,
      // it must be.
      const KuduReplica* r = t->tablet().replicas()[0];
      ASSERT_TRUE(r->is_leader());

      // Check that the tserver associated with the replica is the sole tserver
      // started for this cluster.
      const MiniTabletServer* ts = cluster_->mini_tablet_server(0);
      ASSERT_EQ(ts->server()->instance_pb().permanent_uuid(), r->ts().uuid());
      ASSERT_EQ(ts->bound_rpc_addr().host(), r->ts().hostname());
      ASSERT_EQ(ts->bound_rpc_addr().port(), r->ts().port());
    }
    // Check that there are no duplicate tablet IDs.
    ASSERT_EQ(tokens.size(), tablet_ids.size());
  }

  static Status IntoUniqueScanner(KuduClient* client,
                                  const KuduScanToken& token,
                                  unique_ptr<KuduScanner>* scanner_ptr) {
    string serialized_token;
    RETURN_NOT_OK(token.Serialize(&serialized_token));
    KuduScanner* scanner_ptr_raw;
    RETURN_NOT_OK(KuduScanToken::DeserializeIntoScanner(
        client, serialized_token, &scanner_ptr_raw));
    scanner_ptr->reset(scanner_ptr_raw);
    return Status::OK();
  }

  // Create a table with the specified name and schema with replication factor
  // of one and empty list of range partitions.
  Status CreateAndOpenTable(const string& table_name,
                            const KuduSchema& schema,
                            shared_ptr<KuduTable>* table) {
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    RETURN_NOT_OK(table_creator->table_name(table_name)
                  .schema(&schema)
                  .set_range_partition_columns({})
                  .num_replicas(1)
                  .Create());
    return client_->OpenTable(table_name, table);
  }

  uint64_t NumGetTableSchemaRequests() const {
    const auto& ent = cluster_->mini_master()->master()->metric_entity();
    return METRIC_handler_latency_kudu_master_MasterService_GetTableSchema
        .Instantiate(ent)->TotalCount();
  }

  uint64_t NumGetTableLocationsRequests() const {
    const auto& ent = cluster_->mini_master()->master()->metric_entity();
    return METRIC_handler_latency_kudu_master_MasterService_GetTableLocations
        .Instantiate(ent)->TotalCount();
  }

  Status CheckLiveRowCount(const char* table_name,
                           uint64_t* expected_count) {
    shared_ptr<KuduTable> table;
    RETURN_NOT_OK(client_->OpenTable(table_name, &table));

    vector<scoped_refptr<TabletInfo>> all_tablets_info;
    {
      auto* cm = cluster_->mini_master(0)->master()->catalog_manager();
      CatalogManager::ScopedLeaderSharedLock l(cm);
      scoped_refptr<master::TableInfo> table_info;
      RETURN_NOT_OK(cm->GetTableInfo(table->id(), &table_info));
      table_info->GetAllTablets(&all_tablets_info);
    }
    vector<scoped_refptr<TabletReplica>> replicas;
    for (const auto& tablet_info : all_tablets_info) {
      for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
        scoped_refptr<TabletReplica> r;
        EXPECT_TRUE(cluster_->mini_tablet_server(i)->server()->
        tablet_manager()->LookupTablet(tablet_info->id(), &r));
        replicas.emplace_back(std::move(r));
      }
    }

    uint64_t count = 0;
    for (const auto& r : replicas) {
      count += r->CountLiveRowsNoFail();
    }
    *expected_count = count;
    return Status::OK();
  }

  void PrepareEnvForTestReplicaSelection(shared_ptr<KuduTable>* table, vector<string>* tablet_ids) {
    constexpr const char* const kTableName = "replica_selection";
    // Set up the mini cluster
    InternalMiniClusterOptions options;
    options.num_tablet_servers = 3;
    cluster_.reset(new InternalMiniCluster(env_, options));
    ASSERT_OK(cluster_->Start());
    constexpr int kReplicationFactor = 3;

    // Populate the table with data to scan later.
    {
      // Create a table with 10 partitions, 3 replication factor.
      // and write some rows to make sure all partitions have data.
      TestWorkload workload(cluster_.get(), TestWorkload::PartitioningType::HASH);
      workload.set_table_name(kTableName);
      workload.set_num_tablets(kBucketNum);
      workload.set_num_replicas(kReplicationFactor);
      workload.set_num_write_threads(10);
      workload.set_write_batch_size(128);
      workload.Setup();
      workload.Start();
      ASSERT_EVENTUALLY([&]() { ASSERT_GE(workload.rows_inserted(), kRecordCount); });
      workload.StopAndJoin();
    }
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
    ASSERT_OK(client_->OpenTable(kTableName, table));
    ASSERT_NE(nullptr, table->get());

    vector<client::KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    client::KuduScanTokenBuilder builder(table->get());
    ASSERT_OK(builder.Build(&tokens));

    tablet_ids->clear();
    tablet_ids->reserve(tokens.size());
    for (const auto* token : tokens) {
      tablet_ids->emplace_back(token->tablet().id());
    }
  }

  void GetSelectedReplicaCount(const vector<string>& tablet_ids,
                               KuduClient::ReplicaSelection replication_selection,
                               map<string, int32_t>* replica_num_by_ts_uuid) {
    for (const auto& tablet_id : tablet_ids) {
      scoped_refptr<internal::RemoteTablet> rt;
      Synchronizer sync;
      client_->data_->meta_cache_->LookupTabletById(
          client_.get(), tablet_id, MonoTime::Max(), &rt, sync.AsStatusCallback());
      sync.Wait();
      vector<internal::RemoteTabletServer*> tservers;
      rt->GetRemoteTabletServers(&tservers);
      ASSERT_EQ(3, tservers.size());

      vector<internal::RemoteTabletServer*> candidates;
      internal::RemoteTabletServer* tserver_picked;
      ASSERT_OK(client_->data_->GetTabletServer(
          client_.get(), rt, replication_selection, {}, &candidates, &tserver_picked));
      auto& count = LookupOrInsert(replica_num_by_ts_uuid, tserver_picked->permanent_uuid(), 0);
      count++;
    }
  }

  void GetScannerCount(map<string, int32_t>* scanner_count_by_ts_uuid) {
    scanner_count_by_ts_uuid->clear();
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      vector<tserver::ScanDescriptor> scanners =
          cluster_->mini_tablet_server(i)->server()->scanner_manager()->ListScans();
      scanner_count_by_ts_uuid->insert(
          {cluster_->mini_tablet_server(i)->server()->instance_pb().permanent_uuid(),
           static_cast<int32_t>(scanners.size())});
    }
  }

  shared_ptr<KuduClient> client_;
  unique_ptr<InternalMiniCluster> cluster_;
};

TEST_F(ScanTokenTest, TestScanTokens) {
  // Create schema
  KuduSchema schema;
  {
    KuduSchemaBuilder builder;
    builder.AddColumn("col")->NotNull()->Type(KuduColumnSchema::INT64)->PrimaryKey();
    ASSERT_OK(builder.Build(&schema));
  }

  // Create table
  shared_ptr<KuduTable> table;
  {
    unique_ptr<KuduPartialRow> split(schema.NewRow());
    ASSERT_OK(split->SetInt64("col", 0));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    ASSERT_OK(table_creator->table_name("table")
              .schema(&schema)
              .add_hash_partitions({ "col" }, 4)
              .split_rows({ split.release() })
              .num_replicas(1)
              .Create());
#pragma GCC diagnostic pop
    ASSERT_OK(client_->OpenTable("table", &table));
  }

  // Create session
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  // Insert rows
  for (int i = -100; i < 100; i++) {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetInt64("col", i));
    ASSERT_OK(session->Apply(insert.release()));
  }
  ASSERT_OK(session->Flush());

  { // KUDU-1809, with batchSizeBytes configured to '0',
    // the first call to the tablet server won't return
    // any data.
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    ASSERT_OK(builder.SetBatchSizeBytes(0));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(8, tokens.size());
    unique_ptr<KuduScanner> scanner;
    ASSERT_OK(IntoUniqueScanner(client_.get(), *tokens[0], &scanner));
    ASSERT_OK(scanner->Open());
    ASSERT_EQ(0, scanner->data_->last_response_.data().num_rows());
  }

  { // no predicates with READ_YOUR_WRITES mode
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    ASSERT_OK(builder.SetReadMode(KuduScanner::READ_YOUR_WRITES));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(8, tokens.size());
    ASSERT_EQ(200, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // Set snapshot timestamp with READ_YOUR_WRITES mode
    // gives InvalidArgument error.
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    ASSERT_OK(builder.SetReadMode(KuduScanner::READ_YOUR_WRITES));
    ASSERT_OK(builder.SetSnapshotMicros(1));
    ASSERT_TRUE(builder.Build(&tokens).IsInvalidArgument());
  }

  { // no predicates
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    ASSERT_OK(KuduScanTokenBuilder(table.get()).Build(&tokens));

    ASSERT_EQ(8, tokens.size());
    ASSERT_EQ(200, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // disable table metadata
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    ASSERT_OK(builder.IncludeTableMetadata(false));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(8, tokens.size());
    ASSERT_EQ(200, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // range predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate(
        "col", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(0)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(4, tokens.size());
    ASSERT_EQ(100, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // equality predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate(
        "col", KuduPredicate::EQUAL, KuduValue::FromInt(42)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(1, tokens.size());
    ASSERT_EQ(1, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // IS NOT NULL predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewIsNotNullPredicate("col"));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_GE(8, tokens.size());
    ASSERT_EQ(200, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // IS NULL predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewIsNullPredicate("col"));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_GE(0, tokens.size());
    ASSERT_EQ(0, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // primary key bound
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
    ASSERT_OK(lower_bound->SetInt64("col", 40));

    ASSERT_OK(builder.AddLowerBound(*lower_bound));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(4, tokens.size());
    ASSERT_EQ(60, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // Scan timeout.
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    const int64_t kTimeoutMillis = 300001; // 5 minutes + 1 ms.
    ASSERT_OK(builder.SetTimeoutMillis(kTimeoutMillis));
    ASSERT_OK(builder.Build(&tokens));

    for (const auto& token : tokens) {
      string buf;
      ASSERT_OK(token->Serialize(&buf));
      KuduScanner* scanner_raw;
      ASSERT_OK(KuduScanToken::DeserializeIntoScanner(
          client_.get(), buf, &scanner_raw));
      unique_ptr<KuduScanner> scanner(scanner_raw); // Caller gets ownership of the scanner.
      // Ensure the timeout configuration gets carried through the serialization process.
      ASSERT_EQ(kTimeoutMillis,
                scanner->data_->configuration().timeout().ToMilliseconds());
    }

    ASSERT_GE(8, tokens.size());
    ASSERT_EQ(200, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }
}

TEST_F(ScanTokenTest, TestScanTokensWithNonCoveringRange) {
  // Create schema
  KuduSchema schema;
  {
    KuduSchemaBuilder builder;
    builder.AddColumn("col")->NotNull()->Type(KuduColumnSchema::INT64)->PrimaryKey();
    ASSERT_OK(builder.Build(&schema));
  }

  // Create table
  shared_ptr<KuduTable> table;
  {
    unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
    unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    table_creator->table_name("table");
    table_creator->num_replicas(1);
    table_creator->schema(&schema);

    ASSERT_OK(lower_bound->SetInt64("col", 0));
    ASSERT_OK(upper_bound->SetInt64("col", 100));
    table_creator->add_range_partition(lower_bound.release(), upper_bound.release());

    lower_bound.reset(schema.NewRow());
    upper_bound.reset(schema.NewRow());
    ASSERT_OK(lower_bound->SetInt64("col", 200));
    ASSERT_OK(upper_bound->SetInt64("col", 400));
    table_creator->add_range_partition(lower_bound.release(), upper_bound.release());

    unique_ptr<KuduPartialRow> split(schema.NewRow());
    ASSERT_OK(split->SetInt64("col", 300));
    table_creator->add_range_partition_split(split.release());
    table_creator->add_hash_partitions({ "col" }, 2);

    ASSERT_OK(table_creator->Create());
    ASSERT_OK(client_->OpenTable("table", &table));
  }

  // Create session
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  // Insert rows
  for (int i = 0; i < 100; i++) {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetInt64("col", i));
    ASSERT_OK(session->Apply(insert.release()));
  }
  for (int i = 200; i < 400; i++) {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetInt64("col", i));
    ASSERT_OK(session->Apply(insert.release()));
  }
  ASSERT_OK(session->Flush());

  { // no predicates
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    ASSERT_OK(KuduScanTokenBuilder(table.get()).Build(&tokens));

    ASSERT_EQ(6, tokens.size());
    ASSERT_EQ(300, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // range predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate(
        "col", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(200)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(4, tokens.size());
    ASSERT_EQ(200, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // equality predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate(
        "col", KuduPredicate::EQUAL, KuduValue::FromInt(42)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(1, tokens.size());
    ASSERT_EQ(1, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // IS NOT NULL predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewIsNotNullPredicate("col"));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(6, tokens.size());
    ASSERT_EQ(300, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // IS NULL predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewIsNullPredicate("col"));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_GE(0, tokens.size());
    ASSERT_EQ(0, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // primary key bound
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
    ASSERT_OK(upper_bound->SetInt64("col", 40));

    ASSERT_OK(builder.AddUpperBound(*upper_bound));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(2, tokens.size());
    ASSERT_EQ(40, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }
}

TEST_F(ScanTokenTest, ScanTokensWithCustomHashSchemasPerRange) {
  KuduSchema schema;
  // Create schema
  {
    KuduSchemaBuilder builder;
    builder.AddColumn("col")->NotNull()->Type(KuduColumnSchema::INT64)->PrimaryKey();
    ASSERT_OK(builder.Build(&schema));
  }

  // Create table
  shared_ptr<KuduTable> table;
  {
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    table_creator->table_name("table");
    table_creator->num_replicas(1);
    table_creator->schema(&schema);
    table_creator->add_hash_partitions({ "col" }, 2);

    {
      unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
      unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
      ASSERT_OK(lower_bound->SetInt64("col", 0));
      ASSERT_OK(upper_bound->SetInt64("col", 100));
      unique_ptr<KuduRangePartition> range_partition(
          new KuduRangePartition(lower_bound.release(), upper_bound.release()));
      range_partition->add_hash_partitions({ "col" }, 4);
      table_creator->add_custom_range_partition(range_partition.release());
    }

    {
      unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
      unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
      ASSERT_OK(lower_bound->SetInt64("col", 100));
      ASSERT_OK(upper_bound->SetInt64("col", 200));
      unique_ptr<KuduRangePartition> range_partition(
          new KuduRangePartition(lower_bound.release(), upper_bound.release()));
      range_partition->add_hash_partitions({ "col" }, 2);
      table_creator->add_custom_range_partition(range_partition.release());
    }

    ASSERT_OK(table_creator->Create());
    ASSERT_OK(client_->OpenTable("table", &table));
  }

  // Create session
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  // Insert rows
  for (int i = 0; i < 200; i++) {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetInt64("col", i));
    ASSERT_OK(session->Apply(insert.release()));
  }
  ASSERT_OK(session->Flush());

  uint64_t expected_count = 0;
  CheckLiveRowCount("table", &expected_count);
  ASSERT_EQ(expected_count, 200);

  { // no predicates
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    ASSERT_OK(KuduScanTokenBuilder(table.get()).Build(&tokens));

    ASSERT_EQ(6, tokens.size());
    ASSERT_EQ(200, CountRows(tokens));
    shared_ptr<KuduClient> new_client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &new_client));
    int64_t row_count = 0;
    ASSERT_OK(CountRowsSeq(new_client.get(), tokens, &row_count));
    ASSERT_EQ(200, row_count);
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // range predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate(
        "col", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(100)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(2, tokens.size());
    ASSERT_EQ(100, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // equality predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate(
        "col", KuduPredicate::EQUAL, KuduValue::FromInt(42)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(1, tokens.size());
    ASSERT_EQ(1, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // IS NOT NULL predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewIsNotNullPredicate("col"));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(6, tokens.size());
    ASSERT_EQ(200, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // IS NULL predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewIsNullPredicate("col"));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_GE(0, tokens.size());
    ASSERT_EQ(0, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // primary key bound
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
    ASSERT_OK(upper_bound->SetInt64("col", 40));

    ASSERT_OK(builder.AddUpperBound(*upper_bound));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(4, tokens.size());
    ASSERT_EQ(40, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }
}

TEST_F(ScanTokenTest, TestScanTokensWithCustomHashSchemasPerNonCoveringRange) {
  KuduSchema schema;
  // Create schema
  {
    KuduSchemaBuilder builder;
    builder.AddColumn("col")->NotNull()->Type(KuduColumnSchema::INT64)->PrimaryKey();
    ASSERT_OK(builder.Build(&schema));
  }

  // Create table
  shared_ptr<KuduTable> table;
  {
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    table_creator->table_name("table");
    table_creator->num_replicas(1);
    table_creator->schema(&schema);
    table_creator->add_hash_partitions({ "col" }, 2);

    {
      unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
      unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
      ASSERT_OK(lower_bound->SetInt64("col", 0));
      ASSERT_OK(upper_bound->SetInt64("col", 100));
      unique_ptr<KuduRangePartition> range_partition(
          new KuduRangePartition(lower_bound.release(), upper_bound.release()));
      range_partition->add_hash_partitions({ "col" }, 4);
      table_creator->add_custom_range_partition(range_partition.release());
    }

    {
      unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
      unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
      ASSERT_OK(lower_bound->SetInt64("col", 200));
      ASSERT_OK(upper_bound->SetInt64("col", 300));
      unique_ptr<KuduRangePartition> range_partition(
          new KuduRangePartition(lower_bound.release(), upper_bound.release()));
      range_partition->add_hash_partitions({ "col" }, 2);
      table_creator->add_custom_range_partition(range_partition.release());
    }

    ASSERT_OK(table_creator->Create());
    ASSERT_OK(client_->OpenTable("table", &table));
  }

  // Create session
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  // Insert rows
  for (int i = 0; i < 100; i++) {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetInt64("col", i));
    ASSERT_OK(session->Apply(insert.release()));
  }
  for (int i = 200; i < 300; i++) {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetInt64("col", i));
    ASSERT_OK(session->Apply(insert.release()));
  }
  ASSERT_OK(session->Flush());

  uint64_t expected_count = 0;
  CheckLiveRowCount("table", &expected_count);
  ASSERT_EQ(expected_count, 200);

  { // no predicates
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    ASSERT_OK(KuduScanTokenBuilder(table.get()).Build(&tokens));

    ASSERT_EQ(6, tokens.size());
    ASSERT_EQ(200, CountRows(tokens));
    shared_ptr<KuduClient> new_client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &new_client));
    int64_t row_count = 0;
    ASSERT_OK(CountRowsSeq(new_client.get(), tokens, &row_count));
    ASSERT_EQ(200, row_count);
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // range predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate(
        "col", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(150)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(2, tokens.size());
    ASSERT_EQ(100, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // range predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate(
        "col", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(100)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    unique_ptr<KuduPredicate> predicate1(table->NewComparisonPredicate(
        "col", KuduPredicate::LESS, KuduValue::FromInt(200)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate1.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(0, tokens.size());
    ASSERT_EQ(0, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // range predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate(
        "col", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(50)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    unique_ptr<KuduPredicate> predicate1(table->NewComparisonPredicate(
        "col", KuduPredicate::LESS, KuduValue::FromInt(250)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate1.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(6, tokens.size());
    ASSERT_EQ(100, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // range predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate(
        "col", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(-50)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    unique_ptr<KuduPredicate> predicate1(table->NewComparisonPredicate(
        "col", KuduPredicate::LESS, KuduValue::FromInt(350)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate1.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(6, tokens.size());
    ASSERT_EQ(200, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // equality predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate(
        "col", KuduPredicate::EQUAL, KuduValue::FromInt(242)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(1, tokens.size());
    ASSERT_EQ(1, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // IS NOT NULL predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewIsNotNullPredicate("col"));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(6, tokens.size());
    ASSERT_EQ(200, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // IS NULL predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewIsNullPredicate("col"));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_GE(0, tokens.size());
    ASSERT_EQ(0, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // primary key bound
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
    ASSERT_OK(upper_bound->SetInt64("col", 40));

    ASSERT_OK(builder.AddUpperBound(*upper_bound));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(4, tokens.size());
    ASSERT_EQ(40, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }
}

class TimestampPropagationParamTest :
    public ScanTokenTest,
    public ::testing::WithParamInterface<kudu::ReadMode> {
};

// When building a scanner from a serialized scan token,
// verify that the propagated timestamp from the token makes its way into the
// latest observed timestamp of the client object.
TEST_P(TimestampPropagationParamTest, Test) {
  const kudu::ReadMode read_mode = GetParam();
  static const string kTableName = "p_ts_table";

  // Create a table to work with:
  //   * Deserializing a scan token into a scanner requires the table to exist.
  //   * Creating a scan token requires the table to exist.
  shared_ptr<KuduTable> table;
  {
    static const string kKeyColumnName = "c_key";
    KuduSchema schema;
    {
      KuduSchemaBuilder builder;
      builder.AddColumn(kKeyColumnName)->NotNull()->
          Type(KuduColumnSchema::INT64)->PrimaryKey();
      ASSERT_OK(builder.Build(&schema));
    }

    {
      unique_ptr<KuduPartialRow> split(schema.NewRow());
      ASSERT_OK(split->SetInt64(kKeyColumnName, 0));
      unique_ptr<KuduTableCreator> creator(client_->NewTableCreator());
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
      ASSERT_OK(creator->table_name(kTableName)
                .schema(&schema)
                .add_hash_partitions({ kKeyColumnName }, 2)
                .split_rows({ split.release() })
                .num_replicas(1)
                .Create());
#pragma GCC diagnostic pop
    }
  }

  // Deserialize a scan token and make sure the client's last observed timestamp
  // is always updated accordingly for any read modes.
  const uint64_t ts_prev = client_->GetLatestObservedTimestamp();
  const uint64_t ts_propagated = ts_prev + 1000000;

  ScanTokenPB pb;
  pb.set_table_name(kTableName);
  pb.set_read_mode(read_mode);
  pb.set_propagated_timestamp(ts_propagated);
  const string serialized_token = pb.SerializeAsString();
  EXPECT_EQ(ts_prev, client_->GetLatestObservedTimestamp());

  KuduScanner* scanner_raw;
  ASSERT_OK(KuduScanToken::DeserializeIntoScanner(
      client_.get(), serialized_token, &scanner_raw));
  // The caller of the DeserializeIntoScanner() is responsible for
  // de-allocating the result scanner object.
  unique_ptr<KuduScanner> scanner(scanner_raw);
  EXPECT_EQ(ts_propagated, client_->GetLatestObservedTimestamp());

  // Build the set of scan tokens for the table, serialize them and
  // make sure the serialized tokens contain the propagated timestamp.
  {
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    const uint64_t ts_prev = client_->GetLatestObservedTimestamp();
    const uint64_t ts_propagated = ts_prev + 1000000;

    client_->SetLatestObservedTimestamp(ts_propagated);
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    ASSERT_OK(KuduScanTokenBuilder(table.get()).Build(&tokens));
    for (const auto* t : tokens) {
      string serialized_token;
      ASSERT_OK(t->Serialize(&serialized_token));

      ScanTokenPB pb;
      ASSERT_TRUE(pb.ParseFromString(serialized_token));
      ASSERT_TRUE(pb.has_propagated_timestamp());
      EXPECT_EQ(ts_propagated, pb.propagated_timestamp());
    }
  }
}

const kudu::ReadMode kReadModes[] = {
  kudu::READ_LATEST,
  kudu::READ_AT_SNAPSHOT,
  kudu::READ_YOUR_WRITES,
};
INSTANTIATE_TEST_SUITE_P(Params, TimestampPropagationParamTest,
                         testing::ValuesIn(kReadModes));

// Tests the results of creating scan tokens, altering the columns being
// scanned, and then executing the scan tokens.
TEST_F(ScanTokenTest, TestConcurrentAlterTable) {
  const char* kTableName = "scan-token-alter";
  // Create schema
  KuduSchema schema;
  {
    KuduSchemaBuilder builder;
    builder.AddColumn("key")->NotNull()->Type(KuduColumnSchema::INT64)->PrimaryKey();
    builder.AddColumn("a")->NotNull()->Type(KuduColumnSchema::INT64);
    ASSERT_OK(builder.Build(&schema));
  }

  // Create table
  shared_ptr<KuduTable> table;
  ASSERT_OK(CreateAndOpenTable(kTableName, schema, &table));

  vector<KuduScanToken*> tokens;
  vector<KuduScanToken*> tokens_with_metadata;
  KuduScanTokenBuilder builder(table.get());
  ASSERT_OK(builder.IncludeTableMetadata(false));
  ASSERT_OK(builder.Build(&tokens));
  ASSERT_OK(builder.IncludeTableMetadata(true));
  ASSERT_OK(builder.Build(&tokens_with_metadata));
  ASSERT_EQ(1, tokens.size());
  ASSERT_EQ(1, tokens_with_metadata.size());
  unique_ptr<KuduScanToken> token(tokens[0]);
  unique_ptr<KuduScanToken> token_with_metadata(tokens_with_metadata[0]);

  // Drop a column.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->DropColumn("a");
    ASSERT_OK(table_alterer->Alter());
  }

  unique_ptr<KuduScanner> scanner;
  Status s = IntoUniqueScanner(client_.get(), *token, &scanner);
  ASSERT_EQ("Illegal state: unknown column in scan token: a", s.ToString());

  unique_ptr<KuduScanner> scanner_with_metadata;
  ASSERT_OK(IntoUniqueScanner(client_.get(), *token_with_metadata, &scanner_with_metadata));
  s = scanner_with_metadata->Open();
  ASSERT_EQ("Invalid argument: Some columns are not present in the current schema: a",
      s.ToString());

  // Add back the column with the wrong type.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AddColumn("a")->Type(KuduColumnSchema::STRING);
    ASSERT_OK(table_alterer->Alter());
  }

  s = IntoUniqueScanner(client_.get(), *token, &scanner);
  ASSERT_EQ("Illegal state: invalid type INT64 for column 'a' in scan token, expected: STRING",
            s.ToString());

  // Add back the column with the wrong nullability.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->DropColumn("a");
    table_alterer->AddColumn("a")->Type(KuduColumnSchema::INT64)->Nullable();
    ASSERT_OK(table_alterer->Alter());
  }

  s = IntoUniqueScanner(client_.get(), *token, &scanner);
  ASSERT_EQ("Illegal state: invalid nullability for column 'a' in scan token, expected: NULLABLE",
            s.ToString());

  // Add the column with the correct type and nullability.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->DropColumn("a");
    table_alterer->AddColumn("a")
                 ->Type(KuduColumnSchema::INT64)
                 ->NotNull()
                 ->Default(KuduValue::FromInt(0));
    ASSERT_OK(table_alterer->Alter());
  }

  ASSERT_OK(IntoUniqueScanner(client_.get(), *token, &scanner));
}

// Tests the results of creating scan tokens, renaming the table being
// scanned, and then executing the scan tokens.
TEST_F(ScanTokenTest, TestConcurrentRenameTable) {
  constexpr const char* const kTableName = "scan-token-rename";
  // Create schema
  KuduSchema schema;
  {
    KuduSchemaBuilder builder;
    builder.AddColumn("key")->NotNull()->Type(KuduColumnSchema::INT64)->PrimaryKey();
    builder.AddColumn("a")->NotNull()->Type(KuduColumnSchema::INT64);
    ASSERT_OK(builder.Build(&schema));
  }

  // Create table
  shared_ptr<KuduTable> table;
  ASSERT_OK(CreateAndOpenTable(kTableName, schema, &table));

  vector<KuduScanToken*> tokens;
  ASSERT_OK(KuduScanTokenBuilder(table.get()).Build(&tokens));
  ASSERT_EQ(1, tokens.size());
  unique_ptr<KuduScanToken> token(tokens[0]);

  // Rename the table.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->RenameTo("scan-token-rename-renamed");
    ASSERT_OK(table_alterer->Alter());
  }

  unique_ptr<KuduScanner> scanner;
  ASSERT_OK(IntoUniqueScanner(client_.get(), *token, &scanner));

  size_t row_count;
  ASSERT_OK(CountRowsWithRetries(scanner.get(), &row_count));
  ASSERT_EQ(0, row_count);
}

TEST_F(ScanTokenTest, TestMasterRequestsWithMetadata) {
  const char* kTableName = "scan-token-requests";
  // Create schema
  KuduSchema schema;
  {
    KuduSchemaBuilder builder;
    builder.AddColumn("key")->NotNull()->Type(KuduColumnSchema::INT64)->PrimaryKey();
    builder.AddColumn("a")->NotNull()->Type(KuduColumnSchema::INT64);
    ASSERT_OK(builder.Build(&schema));
  }

  // Create table
  shared_ptr<KuduTable> table;
  ASSERT_OK(CreateAndOpenTable(kTableName, schema, &table));

  vector<KuduScanToken*> tokens;
  KuduScanTokenBuilder builder(table.get());
  ASSERT_OK(builder.IncludeTableMetadata(true));
  ASSERT_OK(builder.IncludeTabletMetadata(true));
  ASSERT_OK(builder.Build(&tokens));
  ASSERT_EQ(1, tokens.size());
  unique_ptr<KuduScanToken> token(tokens[0]);

  shared_ptr<KuduClient> new_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &new_client));
  // List the tables to prevent counting initialization RPCs.
  vector<string> tables;
  ASSERT_OK(new_client->ListTables(&tables));

  const auto init_schema_requests = NumGetTableSchemaRequests();
  const auto init_location_requests = NumGetTableLocationsRequests();

  // Validate that hydrating a token doesn't result in a GetTableSchema
  // or GetTableLocations request.
  unique_ptr<KuduScanner> scanner;
  ASSERT_OK(IntoUniqueScanner(new_client.get(), *token, &scanner));
  ASSERT_EQ(init_schema_requests, NumGetTableSchemaRequests());
  ASSERT_EQ(init_location_requests, NumGetTableLocationsRequests());

  // Validate that hydrating a token doesn't result in a GetTableSchema
  // or GetTableLocations request.
  ASSERT_OK(scanner->Open());
  KuduScanBatch batch;
  ASSERT_OK(scanner->NextBatch(&batch));
  ASSERT_EQ(init_schema_requests, NumGetTableSchemaRequests());
  ASSERT_EQ(init_location_requests, NumGetTableLocationsRequests());
}

TEST_F(ScanTokenTest, TestMasterRequestsNoMetadata) {
  const char* kTableName = "scan-token-requests-no-meta";
  // Create schema
  KuduSchema schema;
  {
    KuduSchemaBuilder builder;
    builder.AddColumn("key")->NotNull()->Type(KuduColumnSchema::INT64)->PrimaryKey();
    builder.AddColumn("a")->NotNull()->Type(KuduColumnSchema::INT64);
    ASSERT_OK(builder.Build(&schema));
  }

  // Create table
  shared_ptr<KuduTable> table;
  ASSERT_OK(CreateAndOpenTable(kTableName, schema, &table));

  shared_ptr<KuduClient> new_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &new_client));
  // List the tables to prevent counting initialization RPCs.
  vector<string> tables;
  ASSERT_OK(new_client->ListTables(&tables));

  vector<KuduScanToken*> tokens;
  KuduScanTokenBuilder builder(table.get());
  ASSERT_OK(builder.IncludeTableMetadata(false));
  ASSERT_OK(builder.IncludeTabletMetadata(false));
  ASSERT_OK(builder.Build(&tokens));
  ASSERT_EQ(1, tokens.size());
  unique_ptr<KuduScanToken> token(tokens[0]);

  const auto init_schema_requests = NumGetTableSchemaRequests();
  const auto init_location_requests = NumGetTableLocationsRequests();

  // Validate that hydrating a token into a scanner results in a single GetTableSchema request.
  unique_ptr<KuduScanner> scanner;
  ASSERT_OK(IntoUniqueScanner(new_client.get(), *token, &scanner));
  ASSERT_EQ(init_schema_requests + 1, NumGetTableSchemaRequests());

  // Validate that opening the scanner results in a GetTableLocations request.
  ASSERT_OK(scanner->Open());
  KuduScanBatch batch;
  ASSERT_OK(scanner->NextBatch(&batch));
  ASSERT_EQ(init_location_requests + 1, NumGetTableLocationsRequests());
}

enum FirstRangeChangeMode {
  BEGIN = 0,
  RANGE_DROPPED = 0,
  RANGE_DROPPED_AND_PRECEDING_RANGE_ADDED = 1,
  RANGE_DROPPED_AND_LARGER_ONE_ADDED = 2,
  RANGE_DROPPED_AND_SMALLER_ONE_ADDED = 3,
  RANGE_REPLACED_WITH_SAME = 4,
  RANGE_REPLACED_WITH_SAME_AND_PRECEDING_RANGE_ADDED = 5,
  RANGE_REPLACED_WITH_TWO_SMALLER_ONES = 6,
  END = 7,
};

class StaleScanTokensParamTest :
    public ScanTokenTest,
    public ::testing::WithParamInterface<FirstRangeChangeMode> {
};

// Create scan tokens for one state of the table and store it for future use.
// Use the tokens to scan the table. Alter the table dropping first range
// partition, optionally replacing it according to FirstRangeChangeMode
// enum. Open the altered table via the client handle which was used to run
// the token-based scan prior. Now, attempt to scan the table using stale
// tokens generated with the original state of the table.
TEST_P(StaleScanTokensParamTest, DroppingFirstRange) {
  constexpr const char* const kTableName = "stale-scan-tokens-dfr";
  KuduSchema schema;
  {
    KuduSchemaBuilder builder;
    builder.AddColumn("key")->
        NotNull()->
        Type(KuduColumnSchema::INT64)->
        PrimaryKey();
    ASSERT_OK(builder.Build(&schema));
  }

  shared_ptr<KuduTable> table;
  {
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    {
      unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
      ASSERT_OK(lower_bound->SetInt64("key", -100));
      unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
      ASSERT_OK(upper_bound->SetInt64("key", 0));
      table_creator->add_range_partition(
          lower_bound.release(), upper_bound.release());
    }
    {
      unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
      ASSERT_OK(lower_bound->SetInt64("key", 0));
      unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
      ASSERT_OK(upper_bound->SetInt64("key", 100));
      table_creator->add_range_partition(
          lower_bound.release(), upper_bound.release());
    }

    ASSERT_OK(table_creator->table_name(kTableName)
        .schema(&schema)
        .set_range_partition_columns({ "key" })
        .num_replicas(1)
        .Create());
    ASSERT_OK(client_->OpenTable(kTableName, &table));
  }

  // Populate the table with data.
  {
    shared_ptr<KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(10000);
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

    for (int i = -50; i < 50; ++i) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i));
      ASSERT_OK(session->Apply(insert.release()));
    }
  }

  // Prepare two sets of scan tokens.
  vector<KuduScanToken*> tokens_a;
  ElementDeleter deleter_a(&tokens_a);
  {
    KuduScanTokenBuilder builder(table.get());
    ASSERT_OK(builder.IncludeTableMetadata(true));
    ASSERT_OK(builder.IncludeTabletMetadata(true));
    ASSERT_OK(builder.Build(&tokens_a));
  }
  ASSERT_EQ(2, tokens_a.size());

  vector<KuduScanToken*> tokens_b;
  ElementDeleter deleter_b(&tokens_b);
  {
    KuduScanTokenBuilder builder(table.get());
    ASSERT_OK(builder.IncludeTableMetadata(true));
    ASSERT_OK(builder.IncludeTabletMetadata(true));
    ASSERT_OK(builder.Build(&tokens_b));
  }
  ASSERT_EQ(2, tokens_b.size());

  // Drop the first range partition, running the operation via the 'client_'
  // handle.
  {
    unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
    unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
    ASSERT_OK(lower_bound->SetInt64("key", -100));
    ASSERT_OK(upper_bound->SetInt64("key", 0));
    unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    ASSERT_OK(alterer->DropRangePartition(
        lower_bound.release(), upper_bound.release())->Alter());
  }

  shared_ptr<KuduClient> new_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &new_client));

  int64_t row_count_a = 0;
  ASSERT_OK(CountRowsSeq(new_client.get(), tokens_a, &row_count_a));
  ASSERT_EQ(50, row_count_a);

  // Open the test table via 'new_client' handle to populate the metadata
  // with actual table metadata, including non-covered ranges. This purges
  // an entry for the [-100, 0) range from the 'tablet_by_key_' map, still
  // keeping corresponding RemoteTable entry in the 'tablets_by_id_' map.
  {
    shared_ptr<KuduTable> t;
    ASSERT_OK(new_client->OpenTable(kTableName, &t));
  }

  const auto range_adder = [&schema](
      KuduClient* c, int64_t range_beg, int64_t range_end) {
    unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
    unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
    RETURN_NOT_OK(lower_bound->SetInt64("key", range_beg));
    RETURN_NOT_OK(upper_bound->SetInt64("key", range_end));
    unique_ptr<KuduTableAlterer> alterer(c->NewTableAlterer(kTableName));
    return alterer->AddRangePartition(lower_bound.release(),
                                      upper_bound.release())->Alter();
   };

  // The bifurcation point.
  const auto mode = GetParam();
  switch (mode) {
    case RANGE_DROPPED:
      break;
    case RANGE_DROPPED_AND_PRECEDING_RANGE_ADDED:
      ASSERT_OK(range_adder(client_.get(), -200, -100));
      break;
    case RANGE_DROPPED_AND_LARGER_ONE_ADDED:
      ASSERT_OK(range_adder(client_.get(), -200, 0));
      break;
    case RANGE_DROPPED_AND_SMALLER_ONE_ADDED:
      ASSERT_OK(range_adder(client_.get(), -50, 0));
      break;
    case RANGE_REPLACED_WITH_SAME:
      ASSERT_OK(range_adder(client_.get(), -100, 0));
      break;
    case RANGE_REPLACED_WITH_SAME_AND_PRECEDING_RANGE_ADDED:
      ASSERT_OK(range_adder(client_.get(), -100, 0));
      ASSERT_OK(range_adder(client_.get(), -200, -100));
      break;
    case RANGE_REPLACED_WITH_TWO_SMALLER_ONES:
      ASSERT_OK(range_adder(client_.get(), -100, -50));
      ASSERT_OK(range_adder(client_.get(), -50, 0));
      break;
    default:
      FAIL() << strings::Substitute("$0: unsupported partition change mode",
                                    static_cast<uint16_t>(mode));
  }

  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  int64_t expected_row_count = 50;
  switch (mode) {
    case RANGE_DROPPED_AND_LARGER_ONE_ADDED:
      expected_row_count += 100;
      [[fallthrough]];
    case RANGE_REPLACED_WITH_SAME_AND_PRECEDING_RANGE_ADDED:
      for (int i = -200; i < -100; ++i) {
        unique_ptr<KuduInsert> insert(table->NewInsert());
        ASSERT_OK(insert->mutable_row()->SetInt64("key", i));
        ASSERT_OK(session->Apply(insert.release()));
      }
      // The rows in the preceding range should not be read if using the
      // token for the [-100, 0) original range.
      [[fallthrough]];
    case RANGE_DROPPED_AND_SMALLER_ONE_ADDED:
    case RANGE_REPLACED_WITH_SAME:
    case RANGE_REPLACED_WITH_TWO_SMALLER_ONES:
      for (int i = -25; i < 0; ++i) {
        unique_ptr<KuduInsert> insert(table->NewInsert());
        ASSERT_OK(insert->mutable_row()->SetInt64("key", i));
        ASSERT_OK(session->Apply(insert.release()));
      }
      expected_row_count += 25;
      break;
    default:
      break;
  }

  // Start another tablet scan using the other identical set of scan tokens.
  // The client metacache should not produce any errors: it should re-fetch
  // the information about the current partition schema and scan the table
  // within the range of the new partitions which correspond to the originally
  // supplied range.
  int64_t row_count_b = -1;
  ASSERT_OK(CountRowsSeq(new_client.get(), tokens_b, &row_count_b));
  ASSERT_EQ(expected_row_count, row_count_b);
}

INSTANTIATE_TEST_SUITE_P(FirstRangeDropped, StaleScanTokensParamTest,
                         testing::Range(FirstRangeChangeMode::BEGIN,
                                        FirstRangeChangeMode::END));

TEST_F(ScanTokenTest, ToggleFaultToleranceForScanConfiguration) {
  constexpr const char* const kTableName = "fault_tolerance_toggle";
  KuduSchema schema;
  {
    KuduSchemaBuilder builder;
    builder.AddColumn("key")->NotNull()->Type(KuduColumnSchema::INT64)->PrimaryKey();
    ASSERT_OK(builder.Build(&schema));
  }

  shared_ptr<KuduTable> table;
  ASSERT_OK(CreateAndOpenTable(kTableName, schema, &table));
  ScanConfiguration sc(table.get());
  ASSERT_FALSE(sc.is_fault_tolerant());
  ASSERT_EQ(KuduScanner::READ_LATEST, sc.read_mode());

  ASSERT_OK(sc.SetFaultTolerant(true));
  ASSERT_TRUE(sc.is_fault_tolerant());
  ASSERT_EQ(KuduScanner::READ_AT_SNAPSHOT, sc.read_mode());

  ASSERT_OK(sc.SetFaultTolerant(false));
  ASSERT_FALSE(sc.is_fault_tolerant());
  ASSERT_EQ(KuduScanner::READ_AT_SNAPSHOT, sc.read_mode());

  ASSERT_OK(sc.SetReadMode(KuduScanner::READ_YOUR_WRITES));
  ASSERT_OK(sc.SetFaultTolerant(false));
  ASSERT_FALSE(sc.is_fault_tolerant());
  ASSERT_EQ(KuduScanner::READ_YOUR_WRITES, sc.read_mode());
}

class ReplicaSelectionTest : public ScanTokenTest,
                             public ::testing::WithParamInterface<KuduClient::ReplicaSelection> {};

INSTANTIATE_TEST_SUITE_P(PickServer,
                         ReplicaSelectionTest,
                         ::testing::Values(KuduClient::ReplicaSelection::LEADER_ONLY,
                                           KuduClient::ReplicaSelection::CLOSEST_REPLICA,
                                           KuduClient::ReplicaSelection::FIRST_REPLICA));

// TODO(duyuqi)
// Using location assignment to test replica selection for ScanToken, refer to:
//   src/kudu/integration-tests/location_assignment-itest.cc#L76-L150
//
// This unit test checks whether LEADER_ONLY/CLOSEST_REPLICA/FIRST_REPLICA replica selection works
// as expected.
TEST_P(ReplicaSelectionTest, ReplicaSelection) {
  shared_ptr<KuduTable> table;
  map<string, int32_t> replica_num_by_ts_uuid;
  vector<string> tablet_ids;
  auto replica_selection = GetParam();
  PrepareEnvForTestReplicaSelection(&table, &tablet_ids);
  GetSelectedReplicaCount(tablet_ids, replica_selection, &replica_num_by_ts_uuid);

  map<string, int32_t> scanner_count_by_ts_uuid;
  GetScannerCount(&scanner_count_by_ts_uuid);
  vector<KuduScanToken*> tokens;
  ElementDeleter deleter(&tokens);
  // Scan all the partitions by specific replica selection.
  // Launch scan requests.
  ASSERT_OK(KuduScanTokenBuilder(table.get()).Build(&tokens));
  int64_t row_count = 0;
  CountRowsSeq(client_.get(), tokens, &row_count, replica_selection);

  int result = 0;
  map<string, int32_t> now_scanner_count_by_ts_uuid;
  GetScannerCount(&now_scanner_count_by_ts_uuid);
  for (auto& ts_uuid_scanner_count : now_scanner_count_by_ts_uuid) {
    const auto& permanent_uuid = ts_uuid_scanner_count.first;
    ASSERT_EQ(replica_num_by_ts_uuid[permanent_uuid],
              (ts_uuid_scanner_count.second - scanner_count_by_ts_uuid[permanent_uuid]));
    result += replica_num_by_ts_uuid[permanent_uuid];
  }
  ASSERT_EQ(kBucketNum, result);
}

} // namespace client
} // namespace kudu
