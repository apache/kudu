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

#include "kudu/client/client.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gmock/gmock.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "kudu/client/batcher.h"
#include "kudu/client/callbacks.h"
#include "kudu/client/client-internal.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/client.pb.h"
#include "kudu/client/columnar_scan_batch.h"
#include "kudu/client/error_collector.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/resource_metrics.h"
#include "kudu/client/row_result.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scan_configuration.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/client/schema.h"
#include "kudu/client/session-internal.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/table_alterer-internal.h"
#include "kudu/client/transaction-internal.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/txn_id.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/data_gen_util.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/token.pb.h"
#include "kudu/server/rpc_server.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/transactions/transactions.pb.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/scanners.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server_options.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/array_view.h"
#include "kudu/util/async_util.h"
#include "kudu/util/barrier.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"  // IWYU pragma: keep
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/semaphore.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread_restrictions.h"

namespace kudu {
class PartitionKey;
}  // namespace kudu

DECLARE_bool(allow_unsafe_replication_factor);
DECLARE_bool(catalog_manager_support_live_row_count);
DECLARE_bool(catalog_manager_support_on_disk_size);
DECLARE_bool(client_use_unix_domain_sockets);
DECLARE_bool(enable_rowset_compaction);
DECLARE_bool(enable_txn_system_client_init);
DECLARE_bool(fail_dns_resolution);
DECLARE_bool(location_mapping_by_uuid);
DECLARE_bool(log_inject_latency);
DECLARE_bool(master_client_location_assignment_enabled);
DECLARE_bool(master_support_connect_to_master_rpc);
DECLARE_bool(master_support_immutable_column_attribute);
DECLARE_bool(mock_table_metrics_for_testing);
DECLARE_bool(rpc_listen_on_unix_domain_socket);
DECLARE_bool(rpc_trace_negotiation);
DECLARE_bool(scanner_inject_service_unavailable_on_continue_scan);
DECLARE_bool(txn_manager_enabled);
DECLARE_bool(txn_manager_lazily_initialized);
DECLARE_int32(client_tablet_locations_by_id_ttl_ms);
DECLARE_int32(check_expired_table_interval_seconds);
DECLARE_int32(flush_threshold_mb);
DECLARE_int32(flush_threshold_secs);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_int32(leader_failure_exp_backoff_max_delta_ms);
DECLARE_int32(log_inject_latency_ms_mean);
DECLARE_int32(log_inject_latency_ms_stddev);
DECLARE_int32(master_inject_latency_on_tablet_lookups_ms);
DECLARE_int32(max_column_comment_length);
DECLARE_int32(max_create_tablets_per_ts);
DECLARE_int32(max_num_replicas);
DECLARE_int32(max_table_comment_length);
DECLARE_int32(min_num_replicas);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_int32(scanner_batch_size_rows);
DECLARE_int32(scanner_gc_check_interval_us);
DECLARE_int32(scanner_inject_latency_on_each_batch_ms);
DECLARE_int32(scanner_max_batch_size_bytes);
DECLARE_int32(scanner_ttl_ms);
DECLARE_int32(stress_cpu_threads);
DECLARE_int32(table_locations_ttl_ms);
DECLARE_int32(txn_status_manager_inject_latency_load_from_tablet_ms);
DECLARE_int64(live_row_count_for_testing);
DECLARE_int64(on_disk_size_for_testing);
DECLARE_string(location_mapping_cmd);
DECLARE_string(superuser_acl);
DECLARE_string(user_acl);
DECLARE_uint32(dns_resolver_cache_capacity_mb);
DECLARE_uint32(txn_keepalive_interval_ms);
DECLARE_uint32(txn_staleness_tracker_interval_ms);
DECLARE_uint32(txn_manager_status_table_num_replicas);
DEFINE_int32(test_scan_num_rows, 1000, "Number of rows to insert and scan");
DECLARE_uint32(default_deleted_table_reserve_seconds);

METRIC_DECLARE_counter(block_manager_total_bytes_read);
METRIC_DECLARE_counter(location_mapping_cache_hits);
METRIC_DECLARE_counter(location_mapping_cache_queries);
METRIC_DECLARE_counter(rpc_connections_accepted_unix_domain_socket);
METRIC_DECLARE_counter(rpcs_queue_overflow);
METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_GetMasterRegistration);
METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_GetTableLocations);
METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_GetTableSchema);
METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_GetTabletLocations);
METRIC_DECLARE_histogram(handler_latency_kudu_tserver_TabletServerService_Scan);

using base::subtle::Atomic32;
using base::subtle::NoBarrier_AtomicIncrement;
using base::subtle::NoBarrier_Load;
using base::subtle::NoBarrier_Store;
using google::protobuf::util::MessageDifferencer;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::itest::GetClusterId;
using kudu::master::CatalogManager;
using kudu::master::GetTableLocationsRequestPB;
using kudu::master::GetTableLocationsResponsePB;
using kudu::rpc::MessengerBuilder;
using kudu::security::SignedTokenPB;
using kudu::client::sp::shared_ptr;
using kudu::tablet::TabletReplica;
using kudu::transactions::TxnTokenPB;
using kudu::tserver::MiniTabletServer;
using std::atomic;
using std::function;
using std::map;
using std::nullopt;
using std::optional;
using std::pair;
using std::set;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

class RWMutex;

namespace client {

class ClientTest : public KuduTest {
 public:
  virtual Status BuildSchema() {
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->Nullable();
    b.AddColumn("non_null_with_default")->Type(KuduColumnSchema::INT32)->NotNull()
        ->Default(KuduValue::FromInt(12345));
    return b.Build(&schema_);
  }

  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(BuildSchema());

    // Reduce the TS<->Master heartbeat interval
    FLAGS_heartbeat_interval_ms = 10;
    FLAGS_scanner_gc_check_interval_us = 50 * 1000; // 50 milliseconds.
    FLAGS_check_expired_table_interval_seconds = 1;

    // Enable TxnManager in Kudu master.
    FLAGS_txn_manager_enabled = true;
    FLAGS_enable_txn_system_client_init = true;
    // Basic txn-related scenarios in this test assume there is only one
    // replica of the transaction status table.
    FLAGS_txn_manager_status_table_num_replicas = 1;

    // Speed up test scenarios which are related to txn keepalive interval.
#if defined(THREAD_SANITIZER) || defined(ADDRESS_SANITIZER)
    FLAGS_txn_keepalive_interval_ms = 1500;
    FLAGS_txn_staleness_tracker_interval_ms = 300;
#else
    FLAGS_txn_keepalive_interval_ms = 250;
    FLAGS_txn_staleness_tracker_interval_ms = 50;
#endif

    SetLocationMappingCmd();

    // Start minicluster and wait for tablet servers to connect to master.
    cluster_.reset(new InternalMiniCluster(env_, InternalMiniClusterOptions()));
    ASSERT_OK(cluster_->Start());

    // Connect to the cluster.
    ASSERT_OK(KuduClientBuilder()
        .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
        .Build(&client_));

    ASSERT_OK(CreateTable(kTableName, 1, GenerateSplitRows(), {}, &client_table_));
  }

  // Looks up the remote tablet entry for a given partition key in the meta cache.
  scoped_refptr<internal::RemoteTablet> MetaCacheLookup(
      KuduTable* table, const PartitionKey& partition_key) const {
    scoped_refptr<internal::RemoteTablet> rt;
    Synchronizer sync;
    client_->data_->meta_cache_->LookupTabletByKey(
        table, partition_key, MonoTime::Max(),
        internal::MetaCache::LookupType::kPoint,
        &rt,
        sync.AsStatusCallback());
    CHECK_OK(sync.Wait());
    return rt;
  }

  Status MetaCacheLookupById(
      const string& tablet_id, scoped_refptr<internal::RemoteTablet>* remote_tablet) const {
    remote_tablet->reset();
    scoped_refptr<internal::RemoteTablet> rt;
    Synchronizer sync;
    client_->data_->meta_cache_->LookupTabletById(
        client_.get(), tablet_id, MonoTime::Max(), &rt,
        sync.AsStatusCallback());
    RETURN_NOT_OK(sync.Wait());
    *remote_tablet = std::move(rt);
    return Status::OK();
  }

  // Generate a set of split rows for tablets used in this test.
  vector<unique_ptr<KuduPartialRow>> GenerateSplitRows() const {
    vector<unique_ptr<KuduPartialRow>> rows;
    unique_ptr<KuduPartialRow> row(schema_.NewRow());
    CHECK_OK(row->SetInt32(0, 9));
    rows.emplace_back(std::move(row));
    return rows;
  }

  // Count the rows of a table, checking that the operation succeeds.
  //
  // Must be public to use as a thread closure.
  void CheckRowCount(KuduTable* table, int expected) const {
    CHECK_EQ(CountRowsFromClient(table), expected);
  }

  // Continuously sample the total size of the buffer used by pending operations
  // of the specified KuduSession object.
  //
  // Must be public to use as a thread closure.
  static void MonitorSessionBufferSize(const KuduSession* session,
                                       CountDownLatch* run_ctl,
                                       int64_t* result_max_size) {
    int64_t max_size = 0;
    while (!run_ctl->WaitFor(MonoDelta::FromMilliseconds(1))) {
      int size = session->data_->GetPendingOperationsSizeForTests();
      if (size > max_size) {
        max_size = size;
      }
    }
    if (result_max_size != nullptr) {
      *result_max_size = max_size;
    }
  }

  // Continuously sample the total count of batchers of the specified
  // KuduSession object.
  //
  // Must be public to use as a thread closure.
  static void MonitorSessionBatchersCount(const KuduSession* session,
                                          CountDownLatch* run_ctl,
                                          size_t* result_max_count) {
    size_t max_count = 0;
    while (!run_ctl->WaitFor(MonoDelta::FromMilliseconds(1))) {
      size_t count = session->data_->GetBatchersCountForTests();
      if (count > max_count) {
        max_count = count;
      }
    }
    if (result_max_count != nullptr) {
      *result_max_count = max_count;
    }
  }

 protected:
  static constexpr const char* const kTableName = "client-testtb";
  static constexpr int32_t kNoBound = kint32max;

  // Set the location mapping command for the test's masters. Overridden by
  // derived classes to test client location assignment.
  virtual void SetLocationMappingCmd() {}

  string GetFirstTabletId(KuduTable* table) const {
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    req.mutable_table()->set_table_name(table->name());
    CatalogManager* catalog =
        cluster_->mini_master()->master()->catalog_manager();
    CatalogManager::ScopedLeaderSharedLock l(catalog);
    CHECK_OK(l.first_failed_status());
    CHECK_OK(catalog->GetTableLocations(
        &req, &resp, /*use_external_addr=*/false, /*user=*/nullopt));
    CHECK(resp.tablet_locations_size() > 0);
    return resp.tablet_locations(0).tablet_id();
  }

  void CheckNoRpcOverflow() const {
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      MiniTabletServer* server = cluster_->mini_tablet_server(i);
      if (server->is_started()) {
        ASSERT_EQ(0, server->server()->rpc_server()->
                  service_pool("kudu.tserver.TabletServerService")->
                  RpcsQueueOverflowMetric()->value());
      }
    }
  }

  void CheckTokensInfo(const vector<KuduScanToken*>& tokens,
                       int replica_num = 1) {
    for (const auto* t : tokens) {
      const KuduTablet& tablet = t->tablet();
      ASSERT_EQ(replica_num, tablet.replicas().size());
      const KuduReplica* replica = tablet.replicas()[0];
      ASSERT_TRUE(replica->is_leader());
      const MiniTabletServer* ts = cluster_->mini_tablet_server(0);
      ASSERT_EQ(ts->server()->instance_pb().permanent_uuid(),
          replica->ts().uuid());
      ASSERT_EQ(ts->bound_rpc_addr().host(), replica->ts().hostname());
      ASSERT_EQ(ts->bound_rpc_addr().port(), replica->ts().port());

      unique_ptr<KuduTablet> tablet_copy;
      {
        KuduTablet* ptr;
        ASSERT_OK(client_->GetTablet(tablet.id(), &ptr));
        tablet_copy.reset(ptr);
      }
      ASSERT_EQ(tablet.id(), tablet_copy->id());
      ASSERT_EQ(1, tablet_copy->replicas().size());
      const KuduReplica* replica_copy = tablet_copy->replicas()[0];

      ASSERT_EQ(replica->is_leader(), replica_copy->is_leader());
      ASSERT_EQ(replica->ts().uuid(), replica_copy->ts().uuid());
      ASSERT_EQ(replica->ts().hostname(), replica_copy->ts().hostname());
      ASSERT_EQ(replica->ts().port(), replica_copy->ts().port());
    }
  }

  int CountRows(const vector<KuduScanToken*>& tokens) {
    atomic<uint32_t> rows(0);
    vector<thread> threads;
    for (KuduScanToken* token : tokens) {
      string buf;
      CHECK_OK(token->Serialize(&buf));

      threads.emplace_back([this, &rows] (const string& serialized_token) {
        shared_ptr<KuduClient> client;
        ASSERT_OK(cluster_->CreateClient(nullptr, &client));
        KuduScanner* scanner_ptr;
        ASSERT_OK(KuduScanToken::DeserializeIntoScanner(
            client.get(), serialized_token, &scanner_ptr));
        unique_ptr<KuduScanner> scanner(scanner_ptr);
        ASSERT_OK(scanner->Open());

        while (scanner->HasMoreRows()) {
          KuduScanBatch batch;
          ASSERT_OK(scanner->NextBatch(&batch));
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

  // Return the number of lookup-related RPCs which have been serviced by the master.
  int CountMasterLookupRPCs() const {
    auto ent = cluster_->mini_master()->master()->metric_entity();
    int ret = 0;
    ret += METRIC_handler_latency_kudu_master_MasterService_GetMasterRegistration
        .Instantiate(ent)->TotalCount();
    ret += METRIC_handler_latency_kudu_master_MasterService_GetTableLocations
        .Instantiate(ent)->TotalCount();
    ret += METRIC_handler_latency_kudu_master_MasterService_GetTabletLocations
        .Instantiate(ent)->TotalCount();
    return ret;
  }

  // Inserts given number of tests rows into the specified table
  // in the context of the session.
  void InsertTestRows(KuduTable* table, KuduSession* session,
                      int num_rows, int first_row = 0) const {
    for (int i = first_row; i < num_rows + first_row; ++i) {
      unique_ptr<KuduInsert> insert(BuildTestInsert(table, i));
      ASSERT_OK(session->Apply(insert.release()));
    }
  }

  // Inserts 'num_rows' test rows using 'client'
  void InsertTestRows(KuduClient* client, KuduTable* table,
                      int num_rows, int first_row = 0) const {
    shared_ptr<KuduSession> session = client->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
    session->SetTimeoutMillis(60000);
    NO_FATALS(InsertTestRows(table, session.get(), num_rows, first_row));
    FlushSessionOrDie(session);
    NO_FATALS(CheckNoRpcOverflow());
  }

  // Inserts 'num_rows' using the default client.
  void InsertTestRows(KuduTable* table, int num_rows, int first_row = 0) const {
    InsertTestRows(client_.get(), table, num_rows, first_row);
  }

  void UpdateTestRows(KuduTable* table, int lo, int hi) const {
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
    session->SetTimeoutMillis(10000);
    for (int i = lo; i < hi; i++) {
      unique_ptr<KuduUpdate> update(UpdateTestRow(table, i));
      ASSERT_OK(session->Apply(update.release()));
    }
    FlushSessionOrDie(session);
    NO_FATALS(CheckNoRpcOverflow());
  }

  void DeleteTestRows(KuduTable* table, int lo, int hi) const {
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
    session->SetTimeoutMillis(10000);
    for (int i = lo; i < hi; i++) {
      unique_ptr<KuduDelete> del(DeleteTestRow(table, i));
      ASSERT_OK(session->Apply(del.release()));
    }
    FlushSessionOrDie(session);
    NO_FATALS(CheckNoRpcOverflow());
  }

  unique_ptr<KuduInsert> BuildTestInsert(KuduTable* table, int index) const {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    KuduPartialRow* row = insert->mutable_row();
    PopulateDefaultRow(row, index);
    return insert;
  }

  unique_ptr<KuduInsertIgnore> BuildTestInsertIgnore(KuduTable* table, int index) const {
    unique_ptr<KuduInsertIgnore> insert_ignore(table->NewInsertIgnore());
    KuduPartialRow* row = insert_ignore->mutable_row();
    PopulateDefaultRow(row, index);
    return insert_ignore;
  }

  unique_ptr<KuduUpdate> BuildTestUpdate(KuduTable* table, int index) const {
    unique_ptr<KuduUpdate> update(table->NewUpdate());
    KuduPartialRow* row = update->mutable_row();
    PopulateDefaultRow(row, index);
    return update;
  }

  unique_ptr<KuduUpdateIgnore> BuildTestUpdateIgnore(KuduTable* table, int index) const {
    unique_ptr<KuduUpdateIgnore> update_ignore(table->NewUpdateIgnore());
    KuduPartialRow* row = update_ignore->mutable_row();
    PopulateDefaultRow(row, index);
    return update_ignore;
  }

  unique_ptr<KuduUpsertIgnore> BuildTestUpsertIgnore(KuduTable* table, int index) const {
    unique_ptr<KuduUpsertIgnore> upsert_ignore(table->NewUpsertIgnore());
    KuduPartialRow* row = upsert_ignore->mutable_row();
    PopulateDefaultRow(row, index);
    return upsert_ignore;
  }

  static unique_ptr<KuduDeleteIgnore> BuildTestDeleteIgnore(KuduTable* table, int index) {
    unique_ptr<KuduDeleteIgnore> delete_ignore(table->NewDeleteIgnore());
    KuduPartialRow* row = delete_ignore->mutable_row();
    CHECK_OK(row->SetInt32(0, index));
    return delete_ignore;
  }

  virtual void PopulateDefaultRow(KuduPartialRow* row, int index) const {
    CHECK_OK(row->SetInt32(0, index));
    CHECK_OK(row->SetInt32(1, index * 2));
    CHECK_OK(row->SetStringCopy(2, Slice(StringPrintf("hello %d", index))));
    CHECK_OK(row->SetInt32(3, index * 3));
  }

  virtual void DoTestVerifyRows(const shared_ptr<KuduTable>& tbl, int num_rows) const {
    vector<string> rows;
    KuduScanner scanner(tbl.get());
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(num_rows, rows.size());
    for (int i = 0; i < num_rows; i++) {
      int key = i + 1;
      ASSERT_EQ(StringPrintf("(int32 key=%d, int32 int_val=%d, string string_val=\"hello %d\", "
                             "int32 non_null_with_default=%d)", key, key*2, key, key*3),
                rows[i]);
    }
  }

  static unique_ptr<KuduUpdate> UpdateTestRow(KuduTable* table, int index) {
    unique_ptr<KuduUpdate> update(table->NewUpdate());
    KuduPartialRow* row = update->mutable_row();
    CHECK_OK(row->SetInt32(0, index));
    CHECK_OK(row->SetInt32(1, index * 2 + 1));
    CHECK_OK(row->SetStringCopy(2, Slice(StringPrintf("hello again %d", index))));
    return update;
  }

  static unique_ptr<KuduDelete> DeleteTestRow(KuduTable* table, int index) {
    unique_ptr<KuduDelete> del(table->NewDelete());
    KuduPartialRow* row = del->mutable_row();
    CHECK_OK(row->SetInt32(0, index));
    return del;
  }

  void DoTestScanResourceMetrics() {
    KuduScanner scanner(client_table_.get());
    string tablet_id = GetFirstTabletId(client_table_.get());
    // Flush to ensure we scan disk later
    ASSERT_OK(cluster_->FlushTablet(tablet_id));
    ASSERT_OK(scanner.SetProjectedColumnNames({ "key" }));
    LOG_TIMING(INFO, "Scanning disk with no predicates") {
      ASSERT_OK(scanner.Open());
      ASSERT_TRUE(scanner.HasMoreRows());
      KuduScanBatch batch;
      while (scanner.HasMoreRows()) {
        ASSERT_OK(scanner.NextBatch(&batch));
      }
      std::map<std::string, int64_t> metrics = scanner.GetResourceMetrics().Get();
      ASSERT_TRUE(ContainsKey(metrics, "cfile_cache_miss_bytes"));
      ASSERT_TRUE(ContainsKey(metrics, "cfile_cache_hit_bytes"));
      ASSERT_GT(metrics["cfile_cache_miss_bytes"] + metrics["cfile_cache_hit_bytes"], 0);

      ASSERT_TRUE(ContainsKey(metrics, "total_duration_nanos"));
      ASSERT_GT(metrics["total_duration_nanos"], 0);
      ASSERT_TRUE(ContainsKey(metrics, "queue_duration_nanos"));
      ASSERT_GT(metrics["queue_duration_nanos"], 0);
      ASSERT_TRUE(ContainsKey(metrics, "cpu_user_nanos"));
      ASSERT_TRUE(ContainsKey(metrics, "cpu_system_nanos"));

      ASSERT_TRUE(ContainsKey(metrics, "bytes_read"));
      ASSERT_GT(metrics["bytes_read"], 0);
    }
  }

  // Compares rows as obtained through a KuduScanBatch::RowPtr and through the
  // the raw direct and indirect data blocks exposed by KuduScanBatch,
  // asserting that they are the same.
  void AssertRawDataMatches(const KuduSchema& projection_schema,
                            const KuduScanBatch& batch,
                            const KuduScanBatch::RowPtr& row,
                            int row_idx,
                            int num_projected_cols) {

    const Schema& schema = *projection_schema.schema_;
    size_t row_stride = ContiguousRowHelper::row_size(schema);
    const uint8_t* row_data = batch.direct_data().data() + row_idx * row_stride;

    int32_t raw_key_val = *reinterpret_cast<const int32_t*>(
        ContiguousRowHelper::cell_ptr(schema, row_data, 0));
    int key_val;
    ASSERT_OK(row.GetInt32(0, &key_val));
    EXPECT_EQ(key_val, raw_key_val);

    // Test projections have either 1 or 4 columns.
    if (num_projected_cols == 1) return;
    ASSERT_EQ(4, num_projected_cols);

    int32_t raw_int_col_val = *reinterpret_cast<const int32_t*>(
        ContiguousRowHelper::cell_ptr(schema, row_data, 1));
    int int_col_val;
    ASSERT_OK(row.GetInt32(1, &int_col_val));
    EXPECT_EQ(int_col_val, raw_int_col_val);

    Slice raw_nullable_slice_col_val = *reinterpret_cast<const Slice*>(
        DCHECK_NOTNULL(ContiguousRowHelper::nullable_cell_ptr(schema, row_data, 2)));
    Slice nullable_slice_col_val;
    ASSERT_OK(row.GetString(2, &nullable_slice_col_val));
    EXPECT_EQ(nullable_slice_col_val, raw_nullable_slice_col_val);

    int32_t raw_col_val = *reinterpret_cast<const int32_t*>(
        ContiguousRowHelper::cell_ptr(schema, row_data, 3));
    int col_val;
    ASSERT_OK(row.GetInt32(3, &col_val));
    EXPECT_EQ(col_val, raw_col_val);
  }

  void DoTestScanWithoutPredicates() {
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(scanner.SetProjectedColumnNames({ "key" }));

    LOG_TIMING(INFO, "Scanning with no predicates") {
      ASSERT_OK(scanner.Open());

      ASSERT_TRUE(scanner.HasMoreRows());
      KuduScanBatch batch;
      uint64_t sum = 0;
      while (scanner.HasMoreRows()) {
        ASSERT_OK(scanner.NextBatch(&batch));
        int count = 0;
        for (const KuduScanBatch::RowPtr& row : batch) {
          int32_t value;
          ASSERT_OK(row.GetInt32(0, &value));
          sum += value;
          AssertRawDataMatches(
              scanner.GetProjectionSchema(), batch, row, count++, 1 /* num projected cols */);
        }
      }
      // The sum should be the sum of the arithmetic series from
      // 0..FLAGS_test_scan_num_rows-1
      uint64_t expected = implicit_cast<uint64_t>(FLAGS_test_scan_num_rows) *
                            (0 + (FLAGS_test_scan_num_rows - 1)) / 2;
      ASSERT_EQ(expected, sum);
    }
  }

  void DoTestScanWithStringPredicate() {
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(scanner.AddConjunctPredicate(
                  client_table_->NewComparisonPredicate("string_val", KuduPredicate::GREATER_EQUAL,
                                                        KuduValue::CopyString("hello 2"))));
    ASSERT_OK(scanner.AddConjunctPredicate(
                  client_table_->NewComparisonPredicate("string_val", KuduPredicate::LESS_EQUAL,
                                                        KuduValue::CopyString("hello 3"))));

    LOG_TIMING(INFO, "Scanning with string predicate") {
      ASSERT_OK(scanner.Open());

      ASSERT_TRUE(scanner.HasMoreRows());
      KuduScanBatch batch;
      while (scanner.HasMoreRows()) {
        ASSERT_OK(scanner.NextBatch(&batch));
        int count = 0;
        for (const KuduScanBatch::RowPtr& row : batch) {
          Slice s;
          ASSERT_OK(row.GetString(2, &s));
          if (!s.starts_with("hello 2") && !s.starts_with("hello 3")) {
            FAIL() << row.ToString();
          }
          AssertRawDataMatches(
              scanner.GetProjectionSchema(), batch, row, count++, 4 /* num projected cols */);
        }
      }
    }
  }

  void DoTestScanWithKeyPredicate() {
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(scanner.AddConjunctPredicate(
                  client_table_->NewComparisonPredicate("key", KuduPredicate::GREATER_EQUAL,
                                                        KuduValue::FromInt(5))));
    ASSERT_OK(scanner.AddConjunctPredicate(
                  client_table_->NewComparisonPredicate("key", KuduPredicate::LESS_EQUAL,
                                                        KuduValue::FromInt(10))));

    LOG_TIMING(INFO, "Scanning with key predicate") {
      ASSERT_OK(scanner.Open());

      ASSERT_TRUE(scanner.HasMoreRows());
      KuduScanBatch batch;
      while (scanner.HasMoreRows()) {
        ASSERT_OK(scanner.NextBatch(&batch));
        for (const KuduScanBatch::RowPtr& row : batch) {
          int32_t k;
          ASSERT_OK(row.GetInt32(0, &k));
          if (k < 5 || k > 10) {
            FAIL() << row.ToString();
          }
        }
      }
    }
  }

  int CountRowsFromClient(KuduTable* table) const {
    return CountRowsFromClient(table, KuduScanner::READ_LATEST);
  }

  int CountRowsFromClient(KuduTable* table,
                          KuduScanner::ReadMode scan_mode,
                          int32_t lower_bound = kNoBound,
                          int32_t upper_bound = kNoBound) const {
    return CountRowsFromClient(table, KuduClient::LEADER_ONLY, scan_mode,
                               lower_bound, upper_bound);
  }

  int CountRowsFromClient(KuduTable* table,
                          KuduClient::ReplicaSelection selection,
                          KuduScanner::ReadMode scan_mode,
                          int32_t lower_bound = kNoBound,
                          int32_t upper_bound = kNoBound) const {
    KuduScanner scanner(table);
    CHECK_OK(scanner.SetSelection(selection));
    CHECK_OK(scanner.SetProjectedColumnNames({}));
    if (lower_bound != kNoBound) {
      CHECK_OK(scanner.AddConjunctPredicate(
                   client_table_->NewComparisonPredicate("key", KuduPredicate::GREATER_EQUAL,
                                                         KuduValue::FromInt(lower_bound))));
    }
    if (upper_bound != kNoBound) {
      CHECK_OK(scanner.AddConjunctPredicate(
                   client_table_->NewComparisonPredicate("key", KuduPredicate::LESS_EQUAL,
                                                         KuduValue::FromInt(upper_bound))));
    }
    CHECK_OK(scanner.SetReadMode(scan_mode));
    CHECK_OK(scanner.Open());

    int count = 0;
    KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      CHECK_OK(scanner.NextBatch(&batch));
      count += batch.NumRows();
    }
    return count;
  }

  // Creates a table with 'num_replicas', split into tablets based on
  // 'split_rows' and 'range_bounds' (or single tablet if both are empty).
  Status CreateTable(const string& table_name,
                     int num_replicas,
                     vector<unique_ptr<KuduPartialRow>> split_rows,
                     vector<pair<unique_ptr<KuduPartialRow>,
                                 unique_ptr<KuduPartialRow>>> range_bounds,
                     shared_ptr<KuduTable>* table) {

    bool added_replicas = false;
    // Add more tablet servers to satisfy all replicas, if necessary.
    while (cluster_->num_tablet_servers() < num_replicas) {
      RETURN_NOT_OK(cluster_->AddTabletServer());
      added_replicas = true;
    }

    if (added_replicas) {
      RETURN_NOT_OK(cluster_->WaitForTabletServerCount(num_replicas));
    }

    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    for (auto& split_row : split_rows) {
      table_creator->add_range_partition_split(split_row.release());
    }
    for (auto& bound : range_bounds) {
      table_creator->add_range_partition(bound.first.release(), bound.second.release());
    }
    RETURN_NOT_OK(table_creator->table_name(table_name)
                            .schema(&schema_)
                            .num_replicas(num_replicas)
                            .set_range_partition_columns({ "key" })
                            .timeout(MonoDelta::FromSeconds(60))
                            .Create());

    return client_->OpenTable(table_name, table);
  }

  // Kills a tablet server.
  // Boolean flags control whether to restart the tserver, and if so, whether to wait for it to
  // finish bootstrapping.
  Status KillTServerImpl(const string& uuid, const bool restart, const bool wait_started) {
    bool ts_found = false;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      MiniTabletServer* ts = cluster_->mini_tablet_server(i);
      if (ts->server()->instance_pb().permanent_uuid() == uuid) {
        if (restart) {
          LOG(INFO) << "Restarting TS at " << ts->bound_rpc_addr().ToString();
          ts->Shutdown();
          RETURN_NOT_OK(ts->Restart());
          if (wait_started) {
            LOG(INFO) << "Waiting for TS " << ts->bound_rpc_addr().ToString()
                << " to finish bootstrapping";
            RETURN_NOT_OK(ts->WaitStarted());
          }
        } else {
          LOG(INFO) << "Killing TS " << uuid << " at " << ts->bound_rpc_addr().ToString();
          ts->Shutdown();
        }
        ts_found = true;
        break;
      }
    }
    if (!ts_found) {
      return Status::InvalidArgument(Substitute("Could not find tablet server $1", uuid));
    }

    return Status::OK();
  }

  Status RestartTServerAndWait(const string& uuid) {
    return KillTServerImpl(uuid, true, true);
  }

  Status RestartTServerAsync(const string& uuid) {
    return KillTServerImpl(uuid, true, false);
  }

  Status KillTServer(const string& uuid) {
    return KillTServerImpl(uuid, false, false);
  }

  void DoApplyWithoutFlushTest(int sleep_micros);

  // Wait for the operations to be flushed when running the session in
  // AUTO_FLUSH_BACKGROUND mode. In most scenarios, operations should be
  // flushed after the maximum wait time. Adding an extra 5x multiplier
  // due to other OS activity and slow runs under TSAN to avoid flakiness.
  void WaitForAutoFlushBackground(const shared_ptr<KuduSession>& session,
                                  int32_t flush_interval_ms) {
    const int32_t kMaxWaitMs = 5 * flush_interval_ms;

    const MonoTime now(MonoTime::Now());
    const MonoTime deadline(now + MonoDelta::FromMilliseconds(kMaxWaitMs));
    for (MonoTime t = now; session->HasPendingOperations() && t < deadline;
       t = MonoTime::Now()) {
      SleepFor(MonoDelta::FromMilliseconds(flush_interval_ms / 2));
    }
  }

  // Measure how much time a batch of insert operations with
  // the specified parameters takes to reach the tablet server if running
  // ApplyInsertToSession() in the specified flush mode.
  void TimeInsertOpBatch(KuduSession::FlushMode mode,
                         size_t buffer_size,
                         size_t run_idx,
                         size_t run_num,
                         const vector<size_t>& string_sizes,
                         CpuTimes* elapsed);

  // Perform scan on the given table in the specified read mode, counting
  // number of returned rows. The 'scan_ts' parameter is effective only in case
  // of READ_AT_SNAPHOT mode. All scans are performed against the tablet's
  // leader server.
  static Status CountRowsOnLeaders(KuduTable* table,
                                   KuduScanner::ReadMode scan_mode,
                                   uint64_t scan_ts,
                                   size_t* row_count) {
    KuduScanner scanner(table);
    RETURN_NOT_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));
    RETURN_NOT_OK(scanner.SetReadMode(scan_mode));
    if (scan_mode == KuduScanner::READ_AT_SNAPSHOT) {
      RETURN_NOT_OK(scanner.SetSnapshotRaw(scan_ts + 1));
    }
    RETURN_NOT_OK(scanner.Open());
    KuduScanBatch batch;
    size_t count = 0;
    while (scanner.HasMoreRows()) {
      RETURN_NOT_OK(scanner.NextBatch(&batch));
      count += batch.NumRows();
    }
    if (row_count) {
      *row_count = count;
    }
    return Status::OK();
  }

  static string FlushModeToString(KuduSession::FlushMode mode) {
    string mode_str = "UNKNOWN";
    switch (mode) {
      case KuduSession::AUTO_FLUSH_BACKGROUND:
        mode_str = "AUTO_FLUSH_BACKGROND";
        break;
      case KuduSession::AUTO_FLUSH_SYNC:
        mode_str = "AUTO_FLUSH_SYNC";
        break;
      case KuduSession::MANUAL_FLUSH:
        mode_str = "MANUAL_FLUSH";
        break;
    }
    return mode_str;
  }

  enum WhichServerToKill {
    DEAD_MASTER,
    DEAD_TSERVER
  };
  void DoTestWriteWithDeadServer(WhichServerToKill which);

  KuduSchema schema_;

  unique_ptr<InternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
  shared_ptr<KuduTable> client_table_;
};

// The goal of this test is to check a scenario where RPC timeout occurred, but session timeout
// didn't. It verifies that by creating an artificial latency in tablet lookup that makes the RPC
// call timeout and the client retries. After some time, the latency is set back to zero and client
// has a successful operation without session timeout, even though the RPC timed out at least once.
// This is the implementation for KUDU-1698.
TEST_F(ClientTest, TestDefaultRPCTimeoutSessionTimeoutDifferent) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  shared_ptr<KuduTable> table;
  ASSERT_OK(CreateTable("rpctimeouttest", 1, {}, {}, &table));
  constexpr int rpcTimeoutMs = 3000;
  // Tablet lookup will trigger timeout.
  FLAGS_master_inject_latency_on_tablet_lookups_ms = rpcTimeoutMs * 2;

  CountDownLatch latch(1);

  thread set_timeout_back_to_zero([&]() {
    const auto sleep_interval = MonoDelta::FromMilliseconds(rpcTimeoutMs * 2L);
    latch.WaitFor(sleep_interval);
    // After some time, tablet lookup will be fast enough to be successful.
    FLAGS_master_inject_latency_on_tablet_lookups_ms = 0;
  });
  SCOPED_CLEANUP({
    latch.CountDown();
    set_timeout_back_to_zero.join();
  });

  KuduClientBuilder builder;
  builder.connection_negotiation_timeout(MonoDelta::FromMilliseconds(rpcTimeoutMs / 2));
  builder.default_rpc_timeout(MonoDelta::FromMilliseconds(rpcTimeoutMs));
  ASSERT_OK(cluster_->CreateClient(&builder, &client_));
  const shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(rpcTimeoutMs * 3);

  auto ent = cluster_->mini_master()->master()->metric_entity();
  auto tablelocation_request_count =
      METRIC_handler_latency_kudu_master_MasterService_GetTableLocations.Instantiate(ent)
          ->TotalCount();
  ASSERT_OK(session->Apply(BuildTestInsert(table.get(), 2).release()));
  // Check that there were more than one tries which indicates RPC timeout and retry.
  ASSERT_LT(1,
            METRIC_handler_latency_kudu_master_MasterService_GetTableLocations.Instantiate(ent)
                    ->TotalCount() -
                tablelocation_request_count);
}

TEST_F(ClientTest, TestClusterId) {
  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  string cluster_id;
  ASSERT_OK(GetClusterId(cluster_->master_proxy(leader_idx),
                         MonoDelta::FromSeconds(30),
                         &cluster_id));
  ASSERT_TRUE(!cluster_id.empty());
  ASSERT_EQ(cluster_id, client_->cluster_id());
}

TEST_F(ClientTest, TestListTables) {
  const char* kTable2Name = "client-testtb2";
  shared_ptr<KuduTable> second_table;
  ASSERT_OK(CreateTable(kTable2Name, 1, {}, {}, &second_table));

  vector<string> tables;
  ASSERT_OK(client_->ListTables(&tables));
  std::sort(tables.begin(), tables.end());
  ASSERT_EQ(string(kTableName), tables[0]);
  ASSERT_EQ(string(kTable2Name), tables[1]);
  ASSERT_OK(client_->ListTables(&tables, "testtb2"));
  ASSERT_EQ(1, tables.size());
  ASSERT_EQ(string(kTable2Name), tables[0]);

  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_EQ(0, tables.size());

  ASSERT_OK(client_->SoftDeleteTable(kTable2Name, 1000));
  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_EQ(1, tables.size());
}

TEST_F(ClientTest, TestListTabletServers) {
  vector<KuduTabletServer*> tss;
  ElementDeleter deleter(&tss);
  ASSERT_OK(client_->ListTabletServers(&tss));
  ASSERT_EQ(1, tss.size());
  ASSERT_EQ(cluster_->mini_tablet_server(0)->server()->instance_pb().permanent_uuid(),
            tss[0]->uuid());
  ASSERT_EQ(cluster_->mini_tablet_server(0)->server()->first_rpc_address().host(),
            tss[0]->hostname());
}

TEST_F(ClientTest, TestGetTableStatistics) {
  unique_ptr<KuduTableStatistics> statistics;
  FLAGS_mock_table_metrics_for_testing = true;
  FLAGS_on_disk_size_for_testing = 1024;
  FLAGS_live_row_count_for_testing = 1000;
  const auto GetTableStatistics = [&] () {
    KuduTableStatistics *table_statistics;
    ASSERT_OK(client_->GetTableStatistics(kTableName, &table_statistics));
    statistics.reset(table_statistics);
  };

  // Master supports 'on disk size' and 'live row count'.
  NO_FATALS(GetTableStatistics());
  ASSERT_EQ(FLAGS_on_disk_size_for_testing, statistics->on_disk_size());
  ASSERT_EQ(FLAGS_live_row_count_for_testing, statistics->live_row_count());

  // Master doesn't support 'on disk size' and 'live row count'.
  FLAGS_catalog_manager_support_on_disk_size = false;
  FLAGS_catalog_manager_support_live_row_count = false;
  NO_FATALS(GetTableStatistics());
  ASSERT_EQ(-1, statistics->on_disk_size());
  ASSERT_EQ(-1, statistics->live_row_count());
}

TEST_F(ClientTest, TestBadTable) {
  shared_ptr<KuduTable> t;
  Status s = client_->OpenTable("xxx-does-not-exist", &t);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Not found: the table does not exist");
}

// If no master address is specified, KuduClientBuilder::Build() should
// immediately return corresponding status code instead of stalling with
// ConnectToCluster() for a long time.
TEST_F(ClientTest, ConnectToClusterNoMasterAddressSpecified) {
  shared_ptr<KuduClient> c;
  KuduClientBuilder b;
  auto s = b.Build(&c);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "no master address specified");
}

// Verify setting of the connection negotiation timeout (KUDU-2966).
TEST_F(ClientTest, ConnectionNegotiationTimeout) {
  const auto master_addr = cluster_->mini_master()->bound_rpc_addr().ToString();

  // The connection negotiation timeout isn't set explicitly: it should be
  // equal to the default setting for the RPC messenger.
  {
    KuduClientBuilder b;
    b.add_master_server_addr(master_addr);
    shared_ptr<KuduClient> c;
    ASSERT_OK(b.Build(&c));
    auto t = c->connection_negotiation_timeout();
    ASSERT_TRUE(t.Initialized());
    ASSERT_EQ(MessengerBuilder::kRpcNegotiationTimeoutMs, t.ToMilliseconds());
    auto t_ms = c->data_->messenger_->rpc_negotiation_timeout_ms();
    ASSERT_EQ(t_ms, t.ToMilliseconds());
  }

  // The connection negotiation timeout is set explicitly via KuduBuilder. It
  // should be propagated to the RPC messenger and reported back correspondingly
  // by the KuduClient::connection_negotiaton_timeout() method.
  {
    const MonoDelta kNegotiationTimeout = MonoDelta::FromMilliseconds(12321);
    KuduClientBuilder b;
    b.add_master_server_addr(master_addr);
    b.connection_negotiation_timeout(kNegotiationTimeout);
    shared_ptr<KuduClient> c;
    ASSERT_OK(b.Build(&c));
    auto t = c->connection_negotiation_timeout();
    ASSERT_EQ(kNegotiationTimeout, t);
    auto t_ms = c->data_->messenger_->rpc_negotiation_timeout_ms();
    ASSERT_EQ(t_ms, t.ToMilliseconds());
  }
}

// Test that, if the master is down, we experience a network error talking
// to it (no "find the new leader master" since there's only one master).
TEST_F(ClientTest, TestMasterDown) {
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);
  shared_ptr<KuduTable> t;
  client_->data_->default_admin_operation_timeout_ = MonoDelta::FromSeconds(1);
  Status s = client_->OpenTable("other-tablet", &t);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
}

// Test that we get errors when we try incorrectly configuring the scanner, and
// that the scanner works well in a basic case.
TEST_F(ClientTest, TestConfiguringScannerLimits) {
  const int64_t kNumRows = 1234;
  const int64_t kLimit = 123;
  NO_FATALS(InsertTestRows(client_table_.get(), kNumRows));

  // Ensure we can't set the limit to some negative number.
  KuduScanner scanner(client_table_.get());
  Status s = scanner.SetLimit(-1);
  ASSERT_STR_CONTAINS(s.ToString(), "must be non-negative");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();

  // Now actually set the limit and open the scanner.
  ASSERT_OK(scanner.SetLimit(kLimit));
  ASSERT_OK(scanner.Open());

  // Ensure we can't set the limit once we've opened the scanner.
  s = scanner.SetLimit(kLimit);
  ASSERT_STR_CONTAINS(s.ToString(), "must be set before Open()");
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  int64_t count = 0;
  KuduScanBatch batch;
  while (scanner.HasMoreRows()) {
    ASSERT_OK(scanner.NextBatch(&batch));
    count += batch.NumRows();
  }
  ASSERT_EQ(kLimit, count);
}

// Test various scanner limits.
TEST_F(ClientTest, TestRandomizedLimitScans) {
  const string kTableName = "table";
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
                          .schema(&schema_)
                          .num_replicas(1)
                          .add_hash_partitions({ "key" }, 2)
                          .timeout(MonoDelta::FromSeconds(60))
                          .Create());
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  // Set the batch size such that a full scan could yield either multi-batch
  // or single-batch scans.
  int64_t num_rows = rand() % 2000;
  int64_t batch_size = rand() % 1000;

  // Set a low flush threshold so we can scan a mix of flushed data in
  // in-memory data.
  FLAGS_flush_threshold_mb = 1;
  FLAGS_flush_threshold_secs = 1;

  FLAGS_scanner_batch_size_rows = batch_size;
  NO_FATALS(InsertTestRows(client_table_.get(), num_rows));
  SleepFor(MonoDelta::FromSeconds(1));
  LOG(INFO) << Substitute("Total number of rows: $0, batch size: $1", num_rows, batch_size);

  auto test_scan_with_limit = [&] (int64_t limit, int64_t expected_rows) {
    scoped_refptr<Counter> counter;
    counter = METRIC_block_manager_total_bytes_read.Instantiate(
        cluster_->mini_tablet_server(0)->server()->metric_entity());
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(scanner.SetLimit(limit));
    ASSERT_OK(scanner.Open());
    int64_t count = 0;
    KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      ASSERT_OK(scanner.NextBatch(&batch));
      count += batch.NumRows();
    }
    NO_FATALS(scanner.Close());
    ASSERT_EQ(expected_rows, count);
    LOG(INFO) << "Total bytes read on tserver so far: " << counter.get()->value();
  };

  // As a sanity check, scanning with a limit of 0 should yield no rows.
  NO_FATALS(test_scan_with_limit(0, 0));

  // Now scan with randomly set limits. To ensure we get a spectrum of limit
  // coverage, gradiate the max limit that we can set.
  for (int i = 1; i < 200; i++) {
    const int64_t max_limit = std::max<int>(num_rows * 0.01 * i, 1);
    const int64_t limit = rand() % max_limit + 1;
    const int64_t expected_rows = std::min({ limit, num_rows });
    LOG(INFO) << Substitute("Scanning with a client-side limit of $0, expecting $1 rows",
                            limit, expected_rows);
    NO_FATALS(test_scan_with_limit(limit, expected_rows));
  }
}

TEST_F(ClientTest, TestScan) {
  NO_FATALS(InsertTestRows(client_table_.get(), FLAGS_test_scan_num_rows));

  ASSERT_EQ(FLAGS_test_scan_num_rows, CountRowsFromClient(client_table_.get()));

  // Scan after insert
  DoTestScanWithoutPredicates();
  DoTestScanResourceMetrics();
  DoTestScanWithStringPredicate();
  DoTestScanWithKeyPredicate();

  // Scan after update
  UpdateTestRows(client_table_.get(), 0, FLAGS_test_scan_num_rows);
  DoTestScanWithKeyPredicate();

  // Scan after delete half
  DeleteTestRows(client_table_.get(), 0, FLAGS_test_scan_num_rows / 2);
  DoTestScanWithKeyPredicate();

  // Scan after delete all
  DeleteTestRows(client_table_.get(), FLAGS_test_scan_num_rows / 2 + 1,
                 FLAGS_test_scan_num_rows);
  DoTestScanWithKeyPredicate();

  // Scan after re-insert
  InsertTestRows(client_table_.get(), 1);
  DoTestScanWithKeyPredicate();
}

TEST_F(ClientTest, TestScanAtSnapshot) {
  int half_the_rows = FLAGS_test_scan_num_rows / 2;

  // Insert half the rows
  NO_FATALS(InsertTestRows(client_table_.get(), half_the_rows));

  // Get the time from the server and transform to micros, disregarding any
  // logical values (we shouldn't have any with a single server anyway).
  int64_t ts = clock::HybridClock::GetPhysicalValueMicros(
      cluster_->mini_tablet_server(0)->server()->clock()->Now());

  // Insert the second half of the rows
  NO_FATALS(InsertTestRows(client_table_.get(), half_the_rows, half_the_rows));

  KuduScanner scanner(client_table_.get());
  ASSERT_OK(scanner.Open());
  uint64_t count = 0;

  // Do a "normal", READ_LATEST scan
  KuduScanBatch batch;
  while (scanner.HasMoreRows()) {
    ASSERT_OK(scanner.NextBatch(&batch));
    count += batch.NumRows();
  }

  ASSERT_EQ(FLAGS_test_scan_num_rows, count);

  // Now close the scanner and perform a scan at 'ts'
  scanner.Close();
  ASSERT_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
  ASSERT_OK(scanner.SetSnapshotMicros(ts));
  ASSERT_OK(scanner.Open());

  count = 0;
  while (scanner.HasMoreRows()) {
    ASSERT_OK(scanner.NextBatch(&batch));
    count += batch.NumRows();
  }

  ASSERT_EQ(half_the_rows, count);
}

// Test scanning at a timestamp in the future compared to the
// local clock. If we are within the clock error, this should wait.
// If we are far in the future, we should get an error.
TEST_F(ClientTest, TestScanAtFutureTimestamp) {
  KuduScanner scanner(client_table_.get());
  ASSERT_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));

  // Try to perform a scan at NowLatest(). This is in the future,
  // but the server should wait until it's in the past.
  int64_t ts = clock::HybridClock::GetPhysicalValueMicros(
      cluster_->mini_tablet_server(0)->server()->clock()->NowLatest());
  ASSERT_OK(scanner.SetSnapshotMicros(ts));
  ASSERT_OK(scanner.Open());
  scanner.Close();

  // Try to perform a scan far in the future (60s -- higher than max clock error).
  // This should return an error.
  ts += 60 * 1000000;
  ASSERT_OK(scanner.SetSnapshotMicros(ts));
  Status s = scanner.Open();
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "in the future.");
}

TEST_F(ClientTest, TestColumnarScan) {
  // Set the batch size such that a full scan could yield either multi-batch
  // or single-batch scans.
  int64_t num_rows = rand() % 2000;
  int64_t batch_size = rand() % 1000;
  FLAGS_scanner_batch_size_rows = batch_size;

  NO_FATALS(InsertTestRows(client_table_.get(), num_rows));
  KuduScanner scanner(client_table_.get());
  ASSERT_OK(scanner.SetRowFormatFlags(KuduScanner::COLUMNAR_LAYOUT));

  ASSERT_OK(scanner.Open());
  KuduColumnarScanBatch batch;
  int total_rows = 0;
  while (scanner.HasMoreRows()) {
    ASSERT_OK(scanner.NextBatch(&batch));

    // Verify the data.
    Slice col_data[4];
    Slice string_indir_data;
    ASSERT_OK(batch.GetFixedLengthColumn(0, &col_data[0]));
    ASSERT_OK(batch.GetFixedLengthColumn(1, &col_data[1]));
    ASSERT_OK(batch.GetVariableLengthColumn(2, &col_data[2], &string_indir_data));
    ASSERT_OK(batch.GetFixedLengthColumn(3, &col_data[3]));

    ArrayView<const int32_t> c0(reinterpret_cast<const int32_t*>(col_data[0].data()),
                                batch.NumRows());
    ArrayView<const int32_t> c1(reinterpret_cast<const int32_t*>(col_data[1].data()),
                                batch.NumRows());
    ArrayView<const uint32_t> c2_offsets(reinterpret_cast<const uint32_t*>(col_data[2].data()),
                                         batch.NumRows() + 1);
    ArrayView<const int32_t> c3(reinterpret_cast<const int32_t*>(col_data[3].data()),
                                batch.NumRows());

    for (int i = 0; i < batch.NumRows(); i++) {
      int row_idx = total_rows + i;
      EXPECT_EQ(row_idx, c0[i]);
      EXPECT_EQ(row_idx * 2, c1[i]);

      Slice str(&string_indir_data[c2_offsets[i]],
                c2_offsets[i + 1] - c2_offsets[i]);
      EXPECT_EQ(Substitute("hello $0", row_idx), str);
      EXPECT_EQ(row_idx * 3, c3[i]);
    }
    total_rows += batch.NumRows();
  }
  ASSERT_EQ(num_rows, total_rows);
}

const KuduScanner::ReadMode read_modes[] = {
    KuduScanner::READ_LATEST,
    KuduScanner::READ_AT_SNAPSHOT,
    KuduScanner::READ_YOUR_WRITES,
};

class ScanMultiTabletParamTest :
    public ClientTest,
    public ::testing::WithParamInterface<KuduScanner::ReadMode> {
};
// Tests multiple tablet scan with all scan modes.
TEST_P(ScanMultiTabletParamTest, Test) {
  const KuduScanner::ReadMode read_mode = GetParam();
  // 5 tablets, each with 10 rows worth of space.
  static const int kTabletsNum = 5;
  static const int kRowsPerTablet = 10;

  shared_ptr<KuduTable> table;
  {
    vector<unique_ptr<KuduPartialRow>> rows;
    for (int i = 1; i < kTabletsNum; ++i) {
      unique_ptr<KuduPartialRow> row(schema_.NewRow());
      ASSERT_OK(row->SetInt32(0, i * kRowsPerTablet));
      rows.emplace_back(std::move(row));
    }
    ASSERT_OK(CreateTable("TestScanMultiTablet", 1,
                          std::move(rows), {}, &table));
  }

  // Insert rows with keys 12, 13, 15, 17, 22, 23, 25, 27...47 into each
  // tablet, except the first which is empty.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(5000);
  for (int i = 1; i < kTabletsNum; ++i) {
    unique_ptr<KuduInsert> insert;
    insert = BuildTestInsert(table.get(), 2 + i * kRowsPerTablet);
    ASSERT_OK(session->Apply(insert.release()));
    insert = BuildTestInsert(table.get(), 3 + i * kRowsPerTablet);
    ASSERT_OK(session->Apply(insert.release()));
    insert = BuildTestInsert(table.get(), 5 + i * kRowsPerTablet);
    ASSERT_OK(session->Apply(insert.release()));
    insert = BuildTestInsert(table.get(), 7 + i * kRowsPerTablet);
    ASSERT_OK(session->Apply(insert.release()));
  }
  FlushSessionOrDie(session);

  ASSERT_EQ(4 * (kTabletsNum - 1), CountRowsFromClient(table.get(), read_mode));
  ASSERT_EQ(3, CountRowsFromClient(table.get(), read_mode, kNoBound, 15));
  ASSERT_EQ(9, CountRowsFromClient(table.get(), read_mode, 27));
  ASSERT_EQ(3, CountRowsFromClient(table.get(), read_mode, 0, 15));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, 0, 10));
  ASSERT_EQ(4, CountRowsFromClient(table.get(), read_mode, 0, 20));
  ASSERT_EQ(8, CountRowsFromClient(table.get(), read_mode, 0, 30));
  ASSERT_EQ(6, CountRowsFromClient(table.get(), read_mode, 14, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, 30, 30));
  ASSERT_EQ(0, CountRowsFromClient(
      table.get(), read_mode, kTabletsNum * kRowsPerTablet));

  // Update every other row
  for (int i = 1; i < kTabletsNum; ++i) {
    unique_ptr<KuduUpdate> update;
    update = UpdateTestRow(table.get(), 2 + i * kRowsPerTablet);
    ASSERT_OK(session->Apply(update.release()));
    update = UpdateTestRow(table.get(), 5 + i * kRowsPerTablet);
    ASSERT_OK(session->Apply(update.release()));
  }
  FlushSessionOrDie(session);

  // Check all counts the same (make sure updates don't change # of rows)
  ASSERT_EQ(4 * (kTabletsNum - 1), CountRowsFromClient(table.get(), read_mode));
  ASSERT_EQ(3, CountRowsFromClient(table.get(), read_mode, kNoBound, 15));
  ASSERT_EQ(9, CountRowsFromClient(table.get(), read_mode, 27));
  ASSERT_EQ(3, CountRowsFromClient(table.get(), read_mode, 0, 15));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, 0, 10));
  ASSERT_EQ(4, CountRowsFromClient(table.get(), read_mode, 0, 20));
  ASSERT_EQ(8, CountRowsFromClient(table.get(), read_mode, 0, 30));
  ASSERT_EQ(6, CountRowsFromClient(table.get(), read_mode, 14, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, 30, 30));
  ASSERT_EQ(0, CountRowsFromClient(
      table.get(), read_mode, kTabletsNum * kRowsPerTablet));

  // Delete half the rows
  for (int i = 1; i < kTabletsNum; ++i) {
    unique_ptr<KuduDelete> del;
    del = DeleteTestRow(table.get(), 5 + i * kRowsPerTablet);
    ASSERT_OK(session->Apply(del.release()));
    del = DeleteTestRow(table.get(), 7 + i * kRowsPerTablet);
    ASSERT_OK(session->Apply(del.release()));
  }
  FlushSessionOrDie(session);

  // Check counts changed accordingly
  ASSERT_EQ(2 * (kTabletsNum - 1),
            CountRowsFromClient(table.get(), read_mode));
  ASSERT_EQ(2, CountRowsFromClient(table.get(), read_mode, kNoBound, 15));
  ASSERT_EQ(4, CountRowsFromClient(table.get(), read_mode, 27));
  ASSERT_EQ(2, CountRowsFromClient(table.get(), read_mode, 0, 15));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, 0, 10));
  ASSERT_EQ(2, CountRowsFromClient(table.get(), read_mode, 0, 20));
  ASSERT_EQ(4, CountRowsFromClient(table.get(), read_mode, 0, 30));
  ASSERT_EQ(2, CountRowsFromClient(table.get(), read_mode, 14, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, 30, 30));
  ASSERT_EQ(0, CountRowsFromClient(
      table.get(), read_mode, kTabletsNum * kRowsPerTablet));

  // Delete rest of rows
  for (int i = 1; i < kTabletsNum; ++i) {
    unique_ptr<KuduDelete> del;
    del = DeleteTestRow(table.get(), 2 + i * kRowsPerTablet);
    ASSERT_OK(session->Apply(del.release()));
    del = DeleteTestRow(table.get(), 3 + i * kRowsPerTablet);
    ASSERT_OK(session->Apply(del.release()));
  }
  FlushSessionOrDie(session);

  // Check counts changed accordingly
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, kNoBound, 15));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, 27));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, 0, 15));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, 0, 10));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, 0, 20));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, 0, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, 14, 30));
  ASSERT_EQ(0, CountRowsFromClient(table.get(), read_mode, 30, 30));
  ASSERT_EQ(0, CountRowsFromClient(
      table.get(), read_mode, kTabletsNum * kRowsPerTablet));
}
INSTANTIATE_TEST_SUITE_P(Params, ScanMultiTabletParamTest,
                         testing::ValuesIn(read_modes));

TEST_F(ClientTest, TestScanEmptyTable) {
  KuduScanner scanner(client_table_.get());
  ASSERT_OK(scanner.SetProjectedColumnNames({}));
  ASSERT_OK(scanner.Open());

  // There are two tablets in the table, both empty. Until we scan to
  // the last tablet, HasMoreRows will return true (because it doesn't
  // know whether there's data in subsequent tablets).
  ASSERT_TRUE(scanner.HasMoreRows());
  KuduScanBatch batch;
  ASSERT_OK(scanner.NextBatch(&batch));
  ASSERT_EQ(0, batch.NumRows());
  ASSERT_FALSE(scanner.HasMoreRows());
}

// Test scanning with an empty projection. This should yield an empty
// row block with the proper number of rows filled in. Impala issues
// scans like this in order to implement COUNT(*).
TEST_F(ClientTest, TestScanEmptyProjection) {
  NO_FATALS(InsertTestRows(client_table_.get(), FLAGS_test_scan_num_rows));
  KuduScanner scanner(client_table_.get());
  ASSERT_OK(scanner.SetProjectedColumnNames({}));
  ASSERT_EQ(scanner.GetProjectionSchema().num_columns(), 0);
  LOG_TIMING(INFO, "Scanning with no projected columns") {
    ASSERT_OK(scanner.Open());

    ASSERT_TRUE(scanner.HasMoreRows());
    KuduScanBatch batch;
    uint64_t count = 0;
    while (scanner.HasMoreRows()) {
      ASSERT_OK(scanner.NextBatch(&batch));
      count += batch.NumRows();
    }
    ASSERT_EQ(FLAGS_test_scan_num_rows, count);
  }
}

TEST_F(ClientTest, TestProjectInvalidColumn) {
  KuduScanner scanner(client_table_.get());
  Status s = scanner.SetProjectedColumnNames({ "column-doesnt-exist" });
  ASSERT_EQ(R"(Not found: Column: "column-doesnt-exist" was not found in the table schema.)",
            s.ToString());

  // Test trying to use a projection where a column is used multiple times.
  // TODO: consider fixing this to support returning the column multiple
  // times, even though it's not very useful.
  s = scanner.SetProjectedColumnNames({ "key", "key" });
  ASSERT_EQ("Invalid argument: Duplicate column name: key", s.ToString());
}

// Test a scan where we have a predicate on a key column that is not
// in the projection.
TEST_F(ClientTest, TestScanPredicateKeyColNotProjected) {
  NO_FATALS(InsertTestRows(client_table_.get(), FLAGS_test_scan_num_rows));
  KuduScanner scanner(client_table_.get());
  ASSERT_OK(scanner.SetProjectedColumnNames({ "int_val" }));
  ASSERT_EQ(scanner.GetProjectionSchema().num_columns(), 1);
  ASSERT_EQ(scanner.GetProjectionSchema().Column(0).type(), KuduColumnSchema::INT32);
  ASSERT_OK(scanner.AddConjunctPredicate(
                client_table_->NewComparisonPredicate("key", KuduPredicate::GREATER_EQUAL,
                                                      KuduValue::FromInt(5))));
  ASSERT_OK(scanner.AddConjunctPredicate(
                client_table_->NewComparisonPredicate("key", KuduPredicate::LESS_EQUAL,
                                                      KuduValue::FromInt(10))));

  size_t nrows = 0;
  int32_t curr_key = 5;
  LOG_TIMING(INFO, "Scanning with predicate columns not projected") {
    ASSERT_OK(scanner.Open());

    ASSERT_TRUE(scanner.HasMoreRows());
    KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      ASSERT_OK(scanner.NextBatch(&batch));
      for (const KuduScanBatch::RowPtr& row : batch) {
        int32_t val;
        ASSERT_OK(row.GetInt32(0, &val));
        ASSERT_EQ(curr_key * 2, val);
        nrows++;
        curr_key++;
      }
    }
  }
  ASSERT_EQ(nrows, 6);
}

// Test a scan where we have a predicate on a non-key column that is
// not in the projection.
TEST_F(ClientTest, TestScanPredicateNonKeyColNotProjected) {
  NO_FATALS(InsertTestRows(client_table_.get(), FLAGS_test_scan_num_rows));
  KuduScanner scanner(client_table_.get());
  ASSERT_OK(scanner.AddConjunctPredicate(
                client_table_->NewComparisonPredicate("int_val", KuduPredicate::GREATER_EQUAL,
                                                      KuduValue::FromInt(10))));
  ASSERT_OK(scanner.AddConjunctPredicate(
                client_table_->NewComparisonPredicate("int_val", KuduPredicate::LESS_EQUAL,
                                                      KuduValue::FromInt(20))));

  size_t nrows = 0;
  int32_t curr_key = 10;

  ASSERT_OK(scanner.SetProjectedColumnNames({ "key" }));

  LOG_TIMING(INFO, "Scanning with predicate columns not projected") {
    ASSERT_OK(scanner.Open());

    ASSERT_TRUE(scanner.HasMoreRows());
    KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      ASSERT_OK(scanner.NextBatch(&batch));
      for (const KuduScanBatch::RowPtr& row : batch) {
        int32_t val;
        ASSERT_OK(row.GetInt32(0, &val));
        ASSERT_EQ(curr_key / 2, val);
        nrows++;
        curr_key += 2;
      }
    }
  }
  ASSERT_EQ(nrows, 6);
}

TEST_F(ClientTest, TestGetKuduTable) {
  KuduScanner scanner(client_table_.get());
  shared_ptr<KuduTable> table = scanner.GetKuduTable();
  ASSERT_EQ(table->name(), client_table_->name());
  ASSERT_EQ(table->id(), client_table_->id());
  ASSERT_EQ(table->schema().ToString(), client_table_->schema().ToString());
}

// Test adding various sorts of invalid binary predicates.
TEST_F(ClientTest, TestInvalidPredicates) {
  KuduScanner scanner(client_table_.get());

  // Predicate on a column that does not exist.
  Status s = scanner.AddConjunctPredicate(
      client_table_->NewComparisonPredicate("this-does-not-exist",
                                            KuduPredicate::EQUAL, KuduValue::FromInt(5)));
  EXPECT_EQ("Not found: column not found: this-does-not-exist", s.ToString());

  // Int predicate on a string column.
  s = scanner.AddConjunctPredicate(
      client_table_->NewComparisonPredicate("string_val",
                                            KuduPredicate::EQUAL, KuduValue::FromInt(5)));
  EXPECT_EQ("Invalid argument: non-string value for string column string_val",
            s.ToString());

  // String predicate on an int column.
  s = scanner.AddConjunctPredicate(
      client_table_->NewComparisonPredicate("int_val",
                                            KuduPredicate::EQUAL, KuduValue::CopyString("x")));
  EXPECT_EQ("Invalid argument: non-int value for int column int_val",
            s.ToString());

  // Out-of-range int predicate on an int column.
  s = scanner.AddConjunctPredicate(
      client_table_->NewComparisonPredicate(
          "int_val",
          KuduPredicate::EQUAL,
          KuduValue::FromInt(static_cast<int64_t>(MathLimits<int32_t>::kMax) + 10)));
  EXPECT_EQ("Invalid argument: value 2147483657 out of range for "
            "32-bit signed integer column 'int_val'", s.ToString());
}

// Check that the tserver proxy is reset on close, even for empty tables.
TEST_F(ClientTest, TestScanCloseProxy) {
  const string kEmptyTable = "TestScanCloseProxy";
  shared_ptr<KuduTable> table;
  ASSERT_OK(CreateTable(kEmptyTable, 3, GenerateSplitRows(), {}, &table));

  {
    // Open and close an empty scanner.
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.Open());
    scanner.Close();
    CHECK_EQ(0, scanner.data_->proxy_.use_count()) << "Proxy was not reset!";
  }

  // Insert some test rows.
  NO_FATALS(InsertTestRows(table.get(), FLAGS_test_scan_num_rows));
  {
    // Open and close a scanner with rows.
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.Open());
    scanner.Close();
    CHECK_EQ(0, scanner.data_->proxy_.use_count()) << "Proxy was not reset!";
  }
}

// Check that the client scanner does not redact rows.
TEST_F(ClientTest, TestRowPtrNoRedaction) {
  google::SetCommandLineOption("redact", "log");

  NO_FATALS(InsertTestRows(client_table_.get(), FLAGS_test_scan_num_rows));
  KuduScanner scanner(client_table_.get());
  ASSERT_OK(scanner.SetProjectedColumnNames({ "key" }));
  ASSERT_OK(scanner.Open());

  ASSERT_TRUE(scanner.HasMoreRows());
  KuduScanBatch batch;
  while (scanner.HasMoreRows()) {
    ASSERT_OK(scanner.NextBatch(&batch));
    for (const KuduScanBatch::RowPtr& row : batch) {
      ASSERT_NE("(int32 key=<redacted>)", row.ToString());
    }
  }
}

TEST_F(ClientTest, TestScanYourWrites) {
  // Insert the rows
  NO_FATALS(InsertTestRows(client_table_.get(), FLAGS_test_scan_num_rows));

  // Verify that no matter which replica is selected, client could
  // achieve read-your-writes/read-your-reads.
  uint64_t count = CountRowsFromClient(client_table_.get(),
                                       KuduClient::LEADER_ONLY,
                                       KuduScanner::READ_YOUR_WRITES);
  ASSERT_EQ(FLAGS_test_scan_num_rows, count);

  count = CountRowsFromClient(client_table_.get(),
                              KuduClient::CLOSEST_REPLICA,
                              KuduScanner::READ_YOUR_WRITES);
  ASSERT_EQ(FLAGS_test_scan_num_rows, count);
}

namespace internal {

static void ReadBatchToStrings(KuduScanner* scanner, vector<string>* rows) {
  KuduScanBatch batch;
  ASSERT_OK(scanner->NextBatch(&batch));
  for (int i = 0; i < batch.NumRows(); i++) {
    rows->push_back(batch.Row(i).ToString());
  }
}

static void DoScanWithCallback(KuduTable* table,
                               const vector<string>& expected_rows,
                               int64_t limit,
                               const std::function<Status(const string&)>& cb) {
  // Initialize fault-tolerant snapshot scanner.
  KuduScanner scanner(table);
  if (limit > 0) {
    ASSERT_OK(scanner.SetLimit(limit));
  }
  ASSERT_OK(scanner.SetFaultTolerant());
  // Set a long timeout as we'll be restarting nodes while performing snapshot scans.
  ASSERT_OK(scanner.SetTimeoutMillis(60 * 1000 /* 60 seconds */));
  // Set a small batch size so it reads in multiple batches.
  ASSERT_OK(scanner.SetBatchSizeBytes(1));

  ASSERT_OK(scanner.Open());
  vector<string> rows;

  // Do a first scan to get us started.
  {
    LOG(INFO) << "Setting up scanner.";
    ASSERT_TRUE(scanner.HasMoreRows());
    NO_FATALS(ReadBatchToStrings(&scanner, &rows));
    ASSERT_GT(rows.size(), 0);
    ASSERT_TRUE(scanner.HasMoreRows());
  }

  // Call the callback on the tserver serving the scan.
  LOG(INFO) << "Calling callback.";
  {
    KuduTabletServer* kts_ptr;
    ASSERT_OK(scanner.GetCurrentServer(&kts_ptr));
    unique_ptr<KuduTabletServer> kts(kts_ptr);
    ASSERT_OK(cb(kts->uuid()));
  }

  // Check that we can still read the next batch.
  LOG(INFO) << "Checking that we can still read the next batch.";
  ASSERT_TRUE(scanner.HasMoreRows());
  ASSERT_OK(scanner.SetBatchSizeBytes(1024*1024));
  while (scanner.HasMoreRows()) {
    NO_FATALS(ReadBatchToStrings(&scanner, &rows));
  }
  scanner.Close();

  // Verify results from the scan.
  LOG(INFO) << "Verifying results from scan.";
  int expected_num_rows = limit < 0 ? expected_rows.size() : limit;
  ASSERT_EQ(expected_num_rows, rows.size());
  for (int i = 0; i < expected_num_rows; i++) {
    EXPECT_EQ(expected_rows[i], rows[i]);
  }
}

} // namespace internal

// Test that ordered snapshot scans can be resumed in the case of different tablet server failures.
TEST_F(ClientTest, TestScanFaultTolerance) {
  // Create test table and insert test rows.
  const string kScanTable = "TestScanFaultTolerance";
  shared_ptr<KuduTable> table;

  // Allow creating table with even replication factor.
  FLAGS_allow_unsafe_replication_factor = true;

  // Make elections faster, otherwise we can go a long time without a leader and thus without
  // advancing safe time and unblocking scanners.
  FLAGS_raft_heartbeat_interval_ms = 50;
  FLAGS_leader_failure_exp_backoff_max_delta_ms = 1000;

  const int kNumReplicas = 3;
  ASSERT_OK(CreateTable(kScanTable, kNumReplicas, {}, {}, &table));
  NO_FATALS(InsertTestRows(table.get(), FLAGS_test_scan_num_rows));

  // Do an initial scan to determine the expected rows for later verification.
  vector<string> expected_rows;
  ASSERT_OK(ScanTableToStrings(table.get(), &expected_rows));

  // Iterate with no limit and with a lower limit than the expected rows.
  vector<int64_t> limits = { -1, static_cast<int64_t>(expected_rows.size() / 2) };
  for (int with_flush = 0; with_flush <= 1; with_flush++) {
    for (int64_t limit : limits) {
      const string kFlushString = with_flush == 1 ? "with flush" : "without flush";
      const string kLimitString = limit < 0 ?
          "without scanner limit" : Substitute("with scanner limit $0", limit);
      SCOPED_TRACE(Substitute("$0, $1", kFlushString, kLimitString));
      // The second time through, flush to ensure that we test both against MRS and
      // disk.
      if (with_flush) {
        string tablet_id = GetFirstTabletId(table.get());
        ASSERT_OK(cluster_->FlushTablet(tablet_id));
      }

      // Test a few different recoverable server-side error conditions.
      // Since these are recoverable, the scan will succeed when retried elsewhere.

      // Restarting and waiting should result in a SCANNER_EXPIRED error.
      LOG(INFO) << "Doing a scan while restarting a tserver and waiting for it to come up...";
      NO_FATALS(internal::DoScanWithCallback(
          table.get(), expected_rows, limit,
          [this](const string& uuid) { return this->RestartTServerAndWait(uuid); }));

      // Restarting and not waiting means the tserver is hopefully bootstrapping, leading to
      // a TABLET_NOT_RUNNING error.
      LOG(INFO) << "Doing a scan while restarting a tserver...";
      NO_FATALS(internal::DoScanWithCallback(
          table.get(), expected_rows, limit,
          [this](const string& uuid) { return this->RestartTServerAsync(uuid); }));
      for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
        MiniTabletServer* ts = cluster_->mini_tablet_server(i);
        ASSERT_OK(ts->WaitStarted());
      }

      // Killing the tserver should lead to an RPC timeout.
      LOG(INFO) << "Doing a scan while killing a tserver...";
      NO_FATALS(internal::DoScanWithCallback(
          table.get(), expected_rows, limit,
          [this](const string& uuid) { return this->KillTServer(uuid); }));

      // Restart the server that we killed.
      for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
        MiniTabletServer* ts = cluster_->mini_tablet_server(i);
        if (!ts->is_started()) {
          ASSERT_OK(ts->Start());
          ASSERT_OK(ts->WaitStarted());
        }
      }
    }
  }
}

// For a non-fault-tolerant scan, if we see a SCANNER_EXPIRED error, we should
// immediately fail, rather than retrying over and over on the same server.
// Regression test for KUDU-2414.
TEST_F(ClientTest, TestNonFaultTolerantScannerExpired) {
  // Create test table and insert test rows.
  const string kScanTable = "TestNonFaultTolerantScannerExpired";
  shared_ptr<KuduTable> table;

  const int kNumReplicas = 1;
  ASSERT_OK(CreateTable(kScanTable, kNumReplicas, {}, {}, &table));
  NO_FATALS(InsertTestRows(table.get(), FLAGS_test_scan_num_rows));

  KuduScanner scanner(table.get());
  ASSERT_OK(scanner.SetTimeoutMillis(30 * 1000));
  // Set a small batch size so it reads in multiple batches.
  ASSERT_OK(scanner.SetBatchSizeBytes(1));

  // First batch should be fine.
  ASSERT_OK(scanner.Open());
  KuduScanBatch batch;
  Status s = scanner.NextBatch(&batch);
  ASSERT_OK(s);

  // Restart the server hosting the scan, so that on an attempt to continue
  // the scan, it will get an error.
  {
    KuduTabletServer* kts_ptr;
    ASSERT_OK(scanner.GetCurrentServer(&kts_ptr));
    unique_ptr<KuduTabletServer> kts(kts_ptr);
    ASSERT_OK(this->RestartTServerAndWait(kts->uuid()));
  }

  // We should now get the appropriate error.
  s = scanner.NextBatch(&batch);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "Scanner .* not found");

  // It should not have performed any retries. Since we restarted the server above,
  // we should see only one Scan RPC: the latest (failed) attempt above.
  const auto& scan_rpcs = METRIC_handler_latency_kudu_tserver_TabletServerService_Scan.Instantiate(
      cluster_->mini_tablet_server(0)->server()->metric_entity());
  ASSERT_EQ(1, scan_rpcs->TotalCount());
}

TEST_F(ClientTest, TestNonCoveringRangePartitions) {
  // Create test table and insert test rows.
  const string kTableName = "TestNonCoveringRangePartitions";
  shared_ptr<KuduTable> table;

  vector<pair<unique_ptr<KuduPartialRow>, unique_ptr<KuduPartialRow>>> bounds;
  unique_ptr<KuduPartialRow> a_lower_bound(schema_.NewRow());
  ASSERT_OK(a_lower_bound->SetInt32("key", 0));
  unique_ptr<KuduPartialRow> a_upper_bound(schema_.NewRow());
  ASSERT_OK(a_upper_bound->SetInt32("key", 100));
  bounds.emplace_back(std::move(a_lower_bound), std::move(a_upper_bound));

  unique_ptr<KuduPartialRow> b_lower_bound(schema_.NewRow());
  ASSERT_OK(b_lower_bound->SetInt32("key", 200));
  unique_ptr<KuduPartialRow> b_upper_bound(schema_.NewRow());
  ASSERT_OK(b_upper_bound->SetInt32("key", 300));
  bounds.emplace_back(std::move(b_lower_bound), std::move(b_upper_bound));

  vector<unique_ptr<KuduPartialRow>> splits;
  unique_ptr<KuduPartialRow> split(schema_.NewRow());
  ASSERT_OK(split->SetInt32("key", 50));
  splits.push_back(std::move(split));

  CreateTable(kTableName, 1, std::move(splits), std::move(bounds), &table);

  // Aggresively clear the meta cache between insert batches so that the meta
  // cache will execute GetTableLocation RPCs at different partition keys.

  NO_FATALS(InsertTestRows(table.get(), 50, 0));
  client_->data_->meta_cache_->ClearCache();
  NO_FATALS(InsertTestRows(table.get(), 50, 50));
  client_->data_->meta_cache_->ClearCache();
  NO_FATALS(InsertTestRows(table.get(), 100, 200));
  client_->data_->meta_cache_->ClearCache();

  // Insert out-of-range rows.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
  session->SetTimeoutMillis(60000);
  vector<unique_ptr<KuduInsert>> out_of_range_inserts;
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), -50));
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), -1));
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), 100));
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), 150));
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), 199));
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), 300));
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), 350));

  for (auto& insert : out_of_range_inserts) {
    client_->data_->meta_cache_->ClearCache();
    Status result = session->Apply(insert.release());
    EXPECT_TRUE(result.IsIOError());
    vector<KuduError*> errors;
    ElementDeleter drop(&errors);
    bool overflowed;
    session->GetPendingErrors(&errors, &overflowed);
    EXPECT_FALSE(overflowed);
    ASSERT_EQ(1, errors.size());
    EXPECT_TRUE(errors[0]->status().IsNotFound());
  }

  // Scans

  { // Full table scan
    vector<string> rows;
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetFaultTolerant());
    ASSERT_OK(ScanToStrings(&scanner, &rows));

    ASSERT_EQ(200, rows.size());
    ASSERT_EQ(R"((int32 key=0, int32 int_val=0, string string_val="hello 0",)"
              " int32 non_null_with_default=0)", rows.front());
    ASSERT_EQ(R"((int32 key=299, int32 int_val=598, string string_val="hello 299",)"
              " int32 non_null_with_default=897)", rows.back());
  }

  { // Lower bound PK
    vector<string> rows;
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetFaultTolerant());
    ASSERT_OK(scanner.AddConjunctPredicate(table->NewComparisonPredicate(
              "key", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(100))));
    ASSERT_OK(ScanToStrings(&scanner, &rows));

    ASSERT_EQ(100, rows.size());
    ASSERT_EQ(R"((int32 key=200, int32 int_val=400, string string_val="hello 200",)"
              " int32 non_null_with_default=600)", rows.front());
    ASSERT_EQ(R"((int32 key=299, int32 int_val=598, string string_val="hello 299",)"
              " int32 non_null_with_default=897)", rows.back());
  }

  { // Upper bound PK
    vector<string> rows;
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetFaultTolerant());
    ASSERT_OK(scanner.AddConjunctPredicate(table->NewComparisonPredicate(
              "key", KuduPredicate::LESS_EQUAL, KuduValue::FromInt(199))));
    ASSERT_OK(ScanToStrings(&scanner, &rows));

    ASSERT_EQ(100, rows.size());
    ASSERT_EQ(R"((int32 key=0, int32 int_val=0, string string_val="hello 0",)"
              " int32 non_null_with_default=0)", rows.front());
    ASSERT_EQ(R"((int32 key=99, int32 int_val=198, string string_val="hello 99",)"
              " int32 non_null_with_default=297)", rows.back());
  }

  { // key <= -1
    vector<string> rows;
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetFaultTolerant());
    ASSERT_OK(scanner.AddConjunctPredicate(table->NewComparisonPredicate(
              "key", KuduPredicate::LESS_EQUAL, KuduValue::FromInt(-1))));
    ASSERT_OK(ScanToStrings(&scanner, &rows));

    ASSERT_TRUE(rows.empty());
  }

  { // key >= 120 && key <= 180
    vector<string> rows;
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetFaultTolerant());
    ASSERT_OK(scanner.AddConjunctPredicate(table->NewComparisonPredicate(
              "key", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(120))));
    ASSERT_OK(scanner.AddConjunctPredicate(table->NewComparisonPredicate(
              "key", KuduPredicate::LESS_EQUAL, KuduValue::FromInt(180))));
    ASSERT_OK(ScanToStrings(&scanner, &rows));

    ASSERT_TRUE(rows.empty());
  }

  { // key >= 300
    vector<string> rows;
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetFaultTolerant());
    ASSERT_OK(scanner.AddConjunctPredicate(table->NewComparisonPredicate(
              "key", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(300))));
    ASSERT_OK(ScanToStrings(&scanner, &rows));

    ASSERT_TRUE(rows.empty());
  }
}

// Test that OpenTable calls clear cached non-covered range partitions.
TEST_F(ClientTest, TestOpenTableClearsNonCoveringRangePartitions) {
  // Create a table with a non-covered range.
  const string kTableName = "TestNonCoveringRangePartitions";
  shared_ptr<KuduTable> table;

  vector<pair<unique_ptr<KuduPartialRow>, unique_ptr<KuduPartialRow>>> bounds;
  unique_ptr<KuduPartialRow> lower_bound(schema_.NewRow());
  unique_ptr<KuduPartialRow> upper_bound(schema_.NewRow());
  ASSERT_OK(lower_bound->SetInt32("key", 0));
  ASSERT_OK(upper_bound->SetInt32("key", 1));
  bounds.emplace_back(std::move(lower_bound), std::move(upper_bound));

  CreateTable(kTableName, 1, {}, std::move(bounds), &table);

  // Attempt to insert into the non-covered range, priming the meta cache.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
  ASSERT_TRUE(session->Apply(BuildTestInsert(table.get(), 1).release()).IsIOError());
  {
    vector<KuduError*> errors;
    ElementDeleter drop(&errors);
    bool overflowed;
    session->GetPendingErrors(&errors, &overflowed);
    EXPECT_FALSE(overflowed);
    ASSERT_EQ(1, errors.size());
    EXPECT_TRUE(errors[0]->status().IsNotFound());
  }

  // Using a separate client, fill in the non-covered range. A separate client
  // is necessary because altering the range partitioning will automatically
  // clear the meta cache, and we want to avoid that.

  lower_bound.reset(schema_.NewRow());
  upper_bound.reset(schema_.NewRow());
  ASSERT_OK(lower_bound->SetInt32("key", 1));

  shared_ptr<KuduClient> alter_client;
  ASSERT_OK(KuduClientBuilder()
      .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
      .Build(&alter_client));

  unique_ptr<KuduTableAlterer> alterer(alter_client->NewTableAlterer(kTableName));
  ASSERT_OK(alterer->AddRangePartition(lower_bound.release(), upper_bound.release())
                   ->Alter());

  // Attempt to insert again into the non-covered range. It should still fail,
  // because the meta cache still contains the non-covered entry.
  ASSERT_TRUE(session->Apply(BuildTestInsert(table.get(), 1).release()).IsIOError());
  {
    vector<KuduError*> errors;
    ElementDeleter drop(&errors);
    bool overflowed;
    session->GetPendingErrors(&errors, &overflowed);
    EXPECT_FALSE(overflowed);
    ASSERT_EQ(1, errors.size());
    EXPECT_TRUE(errors[0]->status().IsNotFound());
  }

  // Re-open the table, and attempt to insert again.  This time the meta cache
  // should clear non-covered entries, and the insert should succeed.
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  ASSERT_OK(session->Apply(BuildTestInsert(table.get(), 1).release()));
}

TEST_F(ClientTest, TestExclusiveInclusiveRangeBounds) {
  // Create test table and insert test rows.
  const string table_name = "TestExclusiveInclusiveRangeBounds";
  shared_ptr<KuduTable> table;

  unique_ptr<KuduPartialRow> lower_bound(schema_.NewRow());
  ASSERT_OK(lower_bound->SetInt32("key", -1));
  unique_ptr<KuduPartialRow> upper_bound(schema_.NewRow());
  ASSERT_OK(upper_bound->SetInt32("key", 99));

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->add_range_partition(lower_bound.release(), upper_bound.release(),
                                      KuduTableCreator::EXCLUSIVE_BOUND,
                                      KuduTableCreator::INCLUSIVE_BOUND);
  ASSERT_OK(table_creator->table_name(table_name)
                          .schema(&schema_)
                          .num_replicas(1)
                          .set_range_partition_columns({ "key" })
                          .Create());

  lower_bound.reset(schema_.NewRow());
  ASSERT_OK(lower_bound->SetInt32("key", 199));
  upper_bound.reset(schema_.NewRow());
  ASSERT_OK(upper_bound->SetInt32("key", 299));
  unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(table_name));
  alterer->AddRangePartition(lower_bound.release(), upper_bound.release(),
                             KuduTableCreator::EXCLUSIVE_BOUND,
                             KuduTableCreator::INCLUSIVE_BOUND);
  ASSERT_OK(alterer->Alter());
  ASSERT_OK(client_->OpenTable(table_name, &table));

  NO_FATALS(InsertTestRows(table.get(), 100, 0));
  NO_FATALS(InsertTestRows(table.get(), 100, 200));

  // Insert out-of-range rows.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
  session->SetTimeoutMillis(60000);
  vector<unique_ptr<KuduInsert>> out_of_range_inserts;
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), -50));
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), -1));
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), 100));
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), 150));
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), 199));
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), 300));
  out_of_range_inserts.emplace_back(BuildTestInsert(table.get(), 350));

  for (auto& insert : out_of_range_inserts) {
    Status result = session->Apply(insert.release());
    EXPECT_TRUE(result.IsIOError());
    vector<KuduError*> errors;
    ElementDeleter drop(&errors);
    bool overflowed;
    session->GetPendingErrors(&errors, &overflowed);
    EXPECT_FALSE(overflowed);
    ASSERT_EQ(1, errors.size());
    EXPECT_TRUE(errors[0]->status().IsNotFound());
  }

  ASSERT_EQ(200, CountTableRows(table.get()));

  // Drop the range partitions by normal inclusive/exclusive bounds, and by
  // exclusive/inclusive bounds.
  alterer.reset(client_->NewTableAlterer(table_name));
  lower_bound.reset(schema_.NewRow());
  ASSERT_OK(lower_bound->SetInt32("key", 0));
  upper_bound.reset(schema_.NewRow());
  ASSERT_OK(upper_bound->SetInt32("key", 100));
  alterer->DropRangePartition(lower_bound.release(), upper_bound.release());
  lower_bound.reset(schema_.NewRow());
  ASSERT_OK(lower_bound->SetInt32("key", 199));
  upper_bound.reset(schema_.NewRow());
  ASSERT_OK(upper_bound->SetInt32("key", 299));
  alterer->DropRangePartition(lower_bound.release(), upper_bound.release(),
                             KuduTableCreator::EXCLUSIVE_BOUND,
                             KuduTableCreator::INCLUSIVE_BOUND);
  ASSERT_OK(alterer->Alter());
  ASSERT_EQ(0, CountTableRows(table.get()));
}

TEST_F(ClientTest, TestExclusiveInclusiveUnixTimeMicrosRangeBounds) {
  // Create test table with range partition using non-default bound types.
  // KUDU-1722
  KuduSchemaBuilder builder;
  KuduSchema schema;
  builder.AddColumn("key")->Type(KuduColumnSchema::UNIXTIME_MICROS)->NotNull()->PrimaryKey();
  builder.AddColumn("value")->Type(KuduColumnSchema::INT32)->NotNull();
  ASSERT_OK(builder.Build(&schema));

  unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
  ASSERT_OK(lower_bound->SetUnixTimeMicros("key", -1));
  unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
  ASSERT_OK(upper_bound->SetUnixTimeMicros("key", 99));

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->add_range_partition(lower_bound.release(), upper_bound.release(),
                                      KuduTableCreator::EXCLUSIVE_BOUND,
                                      KuduTableCreator::INCLUSIVE_BOUND);

  const string table_name = "TestExclusiveInclusiveUnixTimeMicrosRangeBounds";
  ASSERT_OK(table_creator->table_name(table_name)
                          .schema(&schema)
                          .num_replicas(1)
                          .set_range_partition_columns({ "key" })
                          .Create());
}

TEST_F(ClientTest, TestExclusiveInclusiveDecimalRangeBounds) {
  KuduSchemaBuilder builder;
  KuduSchema schema;
  builder.AddColumn("key")->Type(KuduColumnSchema::DECIMAL)->NotNull()->PrimaryKey()
      ->Precision(9)->Scale(2);
  builder.AddColumn("value")->Type(KuduColumnSchema::INT32)->NotNull();
  ASSERT_OK(builder.Build(&schema));

  unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
  ASSERT_OK(lower_bound->SetUnscaledDecimal("key", -1));
  unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
  ASSERT_OK(upper_bound->SetUnscaledDecimal("key", 99));

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->add_range_partition(lower_bound.release(), upper_bound.release(),
                                     KuduTableCreator::EXCLUSIVE_BOUND,
                                     KuduTableCreator::INCLUSIVE_BOUND);

  ASSERT_OK(table_creator->table_name("TestExclusiveInclusiveDecimalRangeBounds")
                .schema(&schema)
                .num_replicas(1)
                .set_range_partition_columns({ "key" })
                .Create());
}

TEST_F(ClientTest, TestSwappedRangeBounds) {
  KuduSchemaBuilder builder;
  KuduSchema schema;
  builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  builder.AddColumn("value")->Type(KuduColumnSchema::INT32)->NotNull();
  ASSERT_OK(builder.Build(&schema));

  unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
  ASSERT_OK(lower_bound->SetInt32("key", 90));
  unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
  ASSERT_OK(upper_bound->SetInt32("key", -1));

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->add_range_partition(lower_bound.release(), upper_bound.release(),
                                     KuduTableCreator::EXCLUSIVE_BOUND,
                                     KuduTableCreator::INCLUSIVE_BOUND);

  Status s = table_creator->table_name("TestSwappedRangeBounds")
                .schema(&schema)
                .num_replicas(1)
                .set_range_partition_columns({ "key" })
                .Create();

  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "Error creating table TestSwappedRangeBounds on the master: "
                          "range partition lower bound must be less than the upper bound");
}

TEST_F(ClientTest, TestEqualRangeBounds) {
  KuduSchemaBuilder builder;
  KuduSchema schema;
  builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  builder.AddColumn("value")->Type(KuduColumnSchema::INT32)->NotNull();
  ASSERT_OK(builder.Build(&schema));

  unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
  ASSERT_OK(lower_bound->SetInt32("key", 10));
  unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
  ASSERT_OK(upper_bound->SetInt32("key", 10));

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->add_range_partition(lower_bound.release(), upper_bound.release(),
                                     KuduTableCreator::EXCLUSIVE_BOUND,
                                     KuduTableCreator::INCLUSIVE_BOUND);

  Status s = table_creator->table_name("TestEqualRangeBounds")
      .schema(&schema)
      .num_replicas(1)
      .set_range_partition_columns({ "key" })
      .Create();

  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "Error creating table TestEqualRangeBounds on the master: "
                          "range partition lower bound must be less than the upper bound");
}

TEST_F(ClientTest, TestMinMaxRangeBounds) {
  KuduSchemaBuilder builder;
  KuduSchema schema;
  builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  builder.AddColumn("value")->Type(KuduColumnSchema::INT32)->NotNull();
  ASSERT_OK(builder.Build(&schema));

  unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
  ASSERT_OK(lower_bound->SetInt32("key", INT32_MIN));
  unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
  ASSERT_OK(upper_bound->SetInt32("key", INT32_MAX));

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->add_range_partition(lower_bound.release(), upper_bound.release(),
                                     KuduTableCreator::EXCLUSIVE_BOUND,
                                     KuduTableCreator::INCLUSIVE_BOUND);

  ASSERT_OK(table_creator->table_name("TestMinMaxRangeBounds")
      .schema(&schema)
      .num_replicas(1)
      .set_range_partition_columns({ "key" })
      .Create());
}

TEST_F(ClientTest, TestMetaCacheExpiry) {
  FLAGS_table_locations_ttl_ms = 25;
  auto& meta_cache = client_->data_->meta_cache_;

  // Clear the cache.
  meta_cache->ClearCache();
  internal::MetaCacheEntry entry;
  ASSERT_FALSE(meta_cache->LookupEntryByKeyFastPath(client_table_.get(), {}, &entry));

  // Prime the cache.
  CHECK_NOTNULL(MetaCacheLookup(client_table_.get(), {}).get());
  ASSERT_TRUE(meta_cache->LookupEntryByKeyFastPath(client_table_.get(), {}, &entry));
  ASSERT_FALSE(entry.stale());

  // Sleep in order to expire the cache.
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_table_locations_ttl_ms));
  ASSERT_TRUE(entry.stale());
  ASSERT_FALSE(meta_cache->LookupEntryByKeyFastPath(client_table_.get(), {}, &entry));

  // Force a lookup and ensure the entry is refreshed.
  ASSERT_NE(nullptr, MetaCacheLookup(client_table_.get(), {}));
  ASSERT_TRUE(entry.stale());
  ASSERT_TRUE(meta_cache->LookupEntryByKeyFastPath(client_table_.get(), {}, &entry));
  ASSERT_FALSE(entry.stale());
}

// This test scenario verifies that reset of the metacache is safe while working
// with the client. The scenario calls MetaCache::ClearCache() in a rather
// synthetic way, but the natural control flow does something similar to that
// when alterting a table by adding a new range partition (see
// KuduTableAlterer::Alter() for details).
TEST_F(ClientTest, ClearCacheAndConcurrentWorkload) {
  CountDownLatch latch(1);
  thread cache_cleaner([&]() {
    const auto sleep_interval = MonoDelta::FromMilliseconds(3);
    while (!latch.WaitFor(sleep_interval)) {
      client_->data_->meta_cache_->ClearCache();
    }
  });
  auto thread_joiner = MakeScopedCleanup([&] {
    latch.CountDown();
    cache_cleaner.join();
  });

  constexpr const int kLowIdx = 0;
  constexpr const int kHighIdx = 1000;
  const auto runUntil = MonoTime::Now() + MonoDelta::FromSeconds(1);
  while (MonoTime::Now() < runUntil) {
    NO_FATALS(InsertTestRows(client_table_.get(), kHighIdx, kLowIdx));
    NO_FATALS(UpdateTestRows(client_table_.get(), kLowIdx, kHighIdx));
    NO_FATALS(DeleteTestRows(client_table_.get(), kLowIdx, kHighIdx));
  }
}

TEST_F(ClientTest, TestBasicIdBasedLookup) {
  auto tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
  ASSERT_FALSE(tablet_ids.empty());
  scoped_refptr<internal::RemoteTablet> rt;
  for (const auto& tablet_id : tablet_ids) {
    ASSERT_OK(MetaCacheLookupById(tablet_id, &rt));
    ASSERT_TRUE(rt != nullptr);
    ASSERT_EQ(tablet_id, rt->tablet_id());
  }
  const auto& kDummyId = "dummy-tablet-id";
  Status s = MetaCacheLookupById(kDummyId, &rt);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_EQ(nullptr, rt);

  auto& meta_cache = client_->data_->meta_cache_;
  internal::MetaCacheEntry entry;
  ASSERT_FALSE(meta_cache->LookupEntryByIdFastPath(kDummyId, &entry));
}

TEST_F(ClientTest, TestMetaCacheExpiryById) {
  FLAGS_client_tablet_locations_by_id_ttl_ms = 25;
  auto tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
  ASSERT_FALSE(tablet_ids.empty());
  const auto& tablet_id = tablet_ids[0];

  auto& meta_cache = client_->data_->meta_cache_;
  meta_cache->ClearCache();
  {
    internal::MetaCacheEntry entry;
    ASSERT_FALSE(meta_cache->LookupEntryByIdFastPath(tablet_id, &entry));
    ASSERT_FALSE(entry.Initialized());
  }
  {
    scoped_refptr<internal::RemoteTablet> rt;
    ASSERT_OK(MetaCacheLookupById(tablet_id, &rt));
    ASSERT_NE(nullptr, rt);
    internal::MetaCacheEntry entry;
    ASSERT_TRUE(meta_cache->LookupEntryByIdFastPath(tablet_id, &entry));
    ASSERT_TRUE(entry.Initialized());
    ASSERT_FALSE(entry.stale());

    // After some time, the entry should become stale.
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_client_tablet_locations_by_id_ttl_ms));
    ASSERT_TRUE(entry.stale());
  }
  {
    internal::MetaCacheEntry entry;
    ASSERT_FALSE(meta_cache->LookupEntryByIdFastPath(tablet_id, &entry));
    ASSERT_FALSE(entry.Initialized());

    // Force a lookup and ensure the entry is refreshed only once we refresh the
    // entry.
    scoped_refptr<internal::RemoteTablet> rt;
    ASSERT_OK(MetaCacheLookupById(tablet_id, &rt));
    ASSERT_NE(nullptr, rt);
    ASSERT_TRUE(meta_cache->LookupEntryByIdFastPath(tablet_id, &entry));
    ASSERT_FALSE(entry.stale());
  }
}

TEST_F(ClientTest, TestMetaCacheWithKeysAndIds) {
  auto& meta_cache = client_->data_->meta_cache_;
  auto tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
  ASSERT_FALSE(tablet_ids.empty());
  const auto& first_tablet_id = tablet_ids[0];

  meta_cache->ClearCache();
  {
    internal::MetaCacheEntry entry;
    ASSERT_FALSE(meta_cache->LookupEntryByIdFastPath(first_tablet_id, &entry));
    ASSERT_FALSE(entry.Initialized());
    ASSERT_FALSE(meta_cache->LookupEntryByKeyFastPath(client_table_.get(), {}, &entry));
    ASSERT_FALSE(entry.Initialized());

    ASSERT_NE(nullptr, MetaCacheLookup(client_table_.get(), {}));
    ASSERT_TRUE(meta_cache->LookupEntryByKeyFastPath(client_table_.get(), {}, &entry));
    ASSERT_FALSE(entry.stale());
  }
  // Just because we have a cache entry for key-based lookup doesn't mean we
  // have an entry for id-based lookup.
  for (const auto& tid : tablet_ids) {
    {
      internal::MetaCacheEntry entry;
      ASSERT_FALSE(meta_cache->LookupEntryByIdFastPath(tid, &entry));
      ASSERT_FALSE(entry.Initialized());

      // We should be able to force an id-based lookup even if the tablets are
      // already in the meta cache.
      scoped_refptr<internal::RemoteTablet> rt;
      ASSERT_OK(MetaCacheLookupById(tid, &rt));
      ASSERT_NE(nullptr, rt);
    }
    {
      internal::MetaCacheEntry entry;
      ASSERT_TRUE(meta_cache->LookupEntryByIdFastPath(tid, &entry));
      ASSERT_FALSE(entry.stale());
    }
  }
  // Even if we looked up new locations, the key-based cached entry should
  // still be available.
  {
    internal::MetaCacheEntry entry;
    ASSERT_TRUE(meta_cache->LookupEntryByKeyFastPath(client_table_.get(), {}, &entry));
    ASSERT_FALSE(entry.stale());
  }
  // Let's do that again but with an id-based lookup first.
  meta_cache->ClearCache();
  {
    internal::MetaCacheEntry entry;
    ASSERT_FALSE(meta_cache->LookupEntryByIdFastPath(first_tablet_id, &entry));
    ASSERT_FALSE(entry.Initialized());
    ASSERT_FALSE(meta_cache->LookupEntryByKeyFastPath(client_table_.get(), {}, &entry));
    ASSERT_FALSE(entry.Initialized());
  }
  // Once we do the lookup by ID, we should be able to fast-path the id-based
  // lookup but not the key-based lookup.
  {
    internal::MetaCacheEntry entry;
    scoped_refptr<internal::RemoteTablet> rt;
    ASSERT_OK(MetaCacheLookupById(first_tablet_id, &rt));
    ASSERT_NE(nullptr, rt);
    ASSERT_TRUE(meta_cache->LookupEntryByIdFastPath(first_tablet_id, &entry));
    ASSERT_FALSE(entry.stale());
    ASSERT_FALSE(meta_cache->LookupEntryByKeyFastPath(client_table_.get(), {}, &entry));
  }
  // And once we do the key-based lookups, we should be able to see them both
  // cached.
  {
    internal::MetaCacheEntry entry;
    ASSERT_NE(nullptr, MetaCacheLookup(client_table_.get(), {}));
    ASSERT_TRUE(meta_cache->LookupEntryByKeyFastPath(client_table_.get(), {}, &entry));
    ASSERT_FALSE(entry.stale());
  }
  {
    internal::MetaCacheEntry entry;
    ASSERT_TRUE(meta_cache->LookupEntryByIdFastPath(first_tablet_id, &entry));
    ASSERT_FALSE(entry.stale());
  }
}

TEST_F(ClientTest, TestMetaCacheExpiryWithKeysAndIds) {
  auto& meta_cache = client_->data_->meta_cache_;
  meta_cache->ClearCache();
  auto tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
  ASSERT_FALSE(tablet_ids.empty());
  const auto& first_tablet_id = tablet_ids[0];

  FLAGS_table_locations_ttl_ms = 10000;
  FLAGS_client_tablet_locations_by_id_ttl_ms = 25;
  internal::MetaCacheEntry entry;
  ASSERT_FALSE(meta_cache->LookupEntryByIdFastPath(first_tablet_id, &entry));
  ASSERT_FALSE(meta_cache->LookupEntryByKeyFastPath(client_table_.get(), {}, &entry));

  // Perform both id-based and key-based lookups.
  ASSERT_NE(nullptr, MetaCacheLookup(client_table_.get(), {}));
  scoped_refptr<internal::RemoteTablet> rt;
  ASSERT_OK(MetaCacheLookupById(first_tablet_id, &rt));
  ASSERT_NE(nullptr, rt);

  // Wait for our id-based entries to expire. This shouldn't affect our
  // key-based entries.
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_client_tablet_locations_by_id_ttl_ms));
  ASSERT_FALSE(meta_cache->LookupEntryByIdFastPath(first_tablet_id, &entry));
  ASSERT_TRUE(meta_cache->LookupEntryByKeyFastPath(client_table_.get(), {}, &entry));
  ASSERT_FALSE(entry.stale());

  FLAGS_client_tablet_locations_by_id_ttl_ms = 10000;
  FLAGS_table_locations_ttl_ms = 25;
  meta_cache->ClearCache();
  ASSERT_NE(nullptr, MetaCacheLookup(client_table_.get(), {}));
  ASSERT_OK(MetaCacheLookupById(first_tablet_id, &rt));
  ASSERT_NE(nullptr, rt);

  // Wait for our key-based entries to expire. This shouldn't affect our
  // id-based entries.
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_table_locations_ttl_ms));
  ASSERT_FALSE(meta_cache->LookupEntryByKeyFastPath(client_table_.get(), {}, &entry));
  ASSERT_TRUE(meta_cache->LookupEntryByIdFastPath(first_tablet_id, &entry));
  ASSERT_FALSE(entry.stale());
}

// Test that if our cache entry indicates there is no leader, we will perform a
// lookup and refresh our cache entry.
TEST_F(ClientTest, TestMetaCacheLookupNoLeaders) {
  auto& meta_cache = client_->data_->meta_cache_;
  auto tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
  ASSERT_FALSE(tablet_ids.empty());
  const auto& tablet_id = tablet_ids[0];

  // Populate the cache.
  scoped_refptr<internal::RemoteTablet> rt;
  ASSERT_OK(MetaCacheLookupById(tablet_id, &rt));
  ASSERT_NE(nullptr, rt);

  // Mark the cache entry's replicas as followers.
  internal::MetaCacheEntry entry;
  ASSERT_TRUE(meta_cache->LookupEntryByIdFastPath(tablet_id, &entry));
  ASSERT_FALSE(entry.stale());
  vector<internal::RemoteReplica> replicas;
  auto remote_tablet = entry.tablet();
  remote_tablet->GetRemoteReplicas(&replicas);
  for (auto& r : replicas) {
    remote_tablet->MarkTServerAsFollower(r.ts);
  }
  const auto has_leader = [&] {
    remote_tablet->GetRemoteReplicas(&replicas);
    for (auto& r : replicas) {
      if (r.role == consensus::RaftPeerPB::LEADER) {
        return true;
      }
    }
    return false;
  };
  ASSERT_FALSE(has_leader());
  // This should force a lookup RPC, rather than leaving our cache entries
  // leaderless.
  ASSERT_OK(MetaCacheLookupById(tablet_id, &rt));
  ASSERT_TRUE(has_leader());
}

TEST_F(ClientTest, TestGetTabletServerBlacklist) {
  shared_ptr<KuduTable> table;
  ASSERT_OK(CreateTable("blacklist",
                        3,
                        GenerateSplitRows(),
                        {},
                        &table));
  InsertTestRows(table.get(), 1, 0);

  // Look up the tablet and its replicas into the metadata cache.
  // We have to loop since some replicas may have been created slowly.
  scoped_refptr<internal::RemoteTablet> rt;
  while (true) {
    rt = MetaCacheLookup(table.get(), {});
    ASSERT_TRUE(rt.get() != nullptr);
    vector<internal::RemoteTabletServer*> tservers;
    rt->GetRemoteTabletServers(&tservers);
    if (tservers.size() == 3) {
      break;
    }
    rt->MarkStale();
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // Get the Leader.
  internal::RemoteTabletServer *rts;
  set<string> blacklist;
  vector<internal::RemoteTabletServer*> candidates;
  vector<internal::RemoteTabletServer*> tservers;
  ASSERT_OK(client_->data_->GetTabletServer(client_.get(), rt,
                                            KuduClient::LEADER_ONLY,
                                            blacklist, &candidates, &rts));
  tservers.push_back(rts);
  // Blacklist the leader, should not work.
  blacklist.insert(rts->permanent_uuid());
  {
    Status s = client_->data_->GetTabletServer(client_.get(), rt,
                                               KuduClient::LEADER_ONLY,
                                               blacklist, &candidates, &rts);
    ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
  }
  // Keep blacklisting replicas until we run out.
  ASSERT_OK(client_->data_->GetTabletServer(client_.get(), rt,
                                            KuduClient::CLOSEST_REPLICA,
                                            blacklist, &candidates, &rts));
  tservers.push_back(rts);
  blacklist.insert(rts->permanent_uuid());
  ASSERT_OK(client_->data_->GetTabletServer(client_.get(), rt,
                                            KuduClient::FIRST_REPLICA,
                                            blacklist, &candidates, &rts));
  tservers.push_back(rts);
  blacklist.insert(rts->permanent_uuid());

  // Make sure none of the three modes work when all nodes are blacklisted.
  vector<KuduClient::ReplicaSelection> selections;
  selections.push_back(KuduClient::LEADER_ONLY);
  selections.push_back(KuduClient::CLOSEST_REPLICA);
  selections.push_back(KuduClient::FIRST_REPLICA);
  for (KuduClient::ReplicaSelection selection : selections) {
    Status s = client_->data_->GetTabletServer(client_.get(), rt, selection,
                                               blacklist, &candidates, &rts);
    ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
  }

  // Make sure none of the modes work when all nodes are dead.
  for (internal::RemoteTabletServer* rt : tservers) {
    client_->data_->meta_cache_->MarkTSFailed(rt, Status::NetworkError("test"));
  }
  blacklist.clear();
  for (KuduClient::ReplicaSelection selection : selections) {
    Status s = client_->data_->GetTabletServer(client_.get(), rt,
                                               selection,
                                               blacklist, &candidates, &rts);
    ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
  }
}

TEST_F(ClientTest, TestGetTabletServerDeterministic) {
  shared_ptr<KuduTable> table;
  ASSERT_OK(CreateTable("selection",
                        3,
                        GenerateSplitRows(),
                        {},
                        &table));
  InsertTestRows(table.get(), 1, 0);

  // Look up the tablet and its replicas into the metadata cache.
  // We have to loop since some replicas may have been created slowly.
  scoped_refptr<internal::RemoteTablet> rt;
  while (true) {
    rt = MetaCacheLookup(table.get(), {});
    ASSERT_TRUE(rt.get() != nullptr);
    vector<internal::RemoteTabletServer*> tservers;
    rt->GetRemoteTabletServers(&tservers);
    if (tservers.size() == 3) {
      break;
    }
    // Marking stale forces a lookup.
    rt->MarkStale();
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // Get the closest replica from the same client twice and ensure they match.
  set<string> blacklist;
  vector<internal::RemoteTabletServer*> candidates;
  internal::RemoteTabletServer* rts1;
  ASSERT_OK(client_->data_->GetTabletServer(client_.get(), rt,
                                            KuduClient::CLOSEST_REPLICA,
                                            blacklist, &candidates, &rts1));
  internal::RemoteTabletServer *rts2;
  ASSERT_OK(client_->data_->GetTabletServer(client_.get(), rt,
                                            KuduClient::CLOSEST_REPLICA,
                                            blacklist, &candidates, &rts2));
  ASSERT_EQ(rts1->permanent_uuid(), rts2->permanent_uuid());

  // Get the closest replica from a different client and ensure it matches.
  shared_ptr<KuduClient> c2;
  ASSERT_OK(KuduClientBuilder()
      .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
      .Build(&c2));
  internal::RemoteTabletServer *rts3;
  ASSERT_OK(c2->data_->GetTabletServer(c2.get(), rt,
                                       KuduClient::CLOSEST_REPLICA,
                                       blacklist, &candidates, &rts3));
  ASSERT_EQ(rts2->permanent_uuid(), rts3->permanent_uuid());
}

TEST_F(ClientTest, TestScanWithEncodedRangePredicate) {
  shared_ptr<KuduTable> table;
  ASSERT_OK(CreateTable("split-table",
                        1, /* replicas */
                        GenerateSplitRows(),
                        {},
                        &table));

  NO_FATALS(InsertTestRows(table.get(), 100));

  vector<string> all_rows;
  ASSERT_OK(ScanTableToStrings(table.get(), &all_rows));
  ASSERT_EQ(100, all_rows.size());

  unique_ptr<KuduPartialRow> row(table->schema().NewRow());

  // Test a double-sided range within first tablet
  {
    KuduScanner scanner(table.get());
    CHECK_OK(row->SetInt32(0, 5));
    ASSERT_OK(scanner.AddLowerBound(*row));
    CHECK_OK(row->SetInt32(0, 8));
    ASSERT_OK(scanner.AddExclusiveUpperBound(*row));
    vector<string> rows;
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(8 - 5, rows.size());
    EXPECT_EQ(all_rows[5], rows.front());
    EXPECT_EQ(all_rows[7], rows.back());
  }

  // Test a double-sided range spanning tablets
  {
    KuduScanner scanner(table.get());
    CHECK_OK(row->SetInt32(0, 5));
    ASSERT_OK(scanner.AddLowerBound(*row));
    CHECK_OK(row->SetInt32(0, 15));
    ASSERT_OK(scanner.AddExclusiveUpperBound(*row));
    vector<string> rows;
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(15 - 5, rows.size());
    EXPECT_EQ(all_rows[5], rows.front());
    EXPECT_EQ(all_rows[14], rows.back());
  }

  // Test a double-sided range within second tablet
  {
    KuduScanner scanner(table.get());
    CHECK_OK(row->SetInt32(0, 15));
    ASSERT_OK(scanner.AddLowerBound(*row));
    CHECK_OK(row->SetInt32(0, 20));
    ASSERT_OK(scanner.AddExclusiveUpperBound(*row));
    vector<string> rows;
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(20 - 15, rows.size());
    EXPECT_EQ(all_rows[15], rows.front());
    EXPECT_EQ(all_rows[19], rows.back());
  }

  // Test a lower-bound only range.
  {
    KuduScanner scanner(table.get());
    CHECK_OK(row->SetInt32(0, 5));
    ASSERT_OK(scanner.AddLowerBound(*row));
    vector<string> rows;
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(95, rows.size());
    EXPECT_EQ(all_rows[5], rows.front());
    EXPECT_EQ(all_rows[99], rows.back());
  }

  // Test an upper-bound only range in first tablet.
  {
    KuduScanner scanner(table.get());
    CHECK_OK(row->SetInt32(0, 5));
    ASSERT_OK(scanner.AddExclusiveUpperBound(*row));
    vector<string> rows;
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(5, rows.size());
    EXPECT_EQ(all_rows[0], rows.front());
    EXPECT_EQ(all_rows[4], rows.back());
  }

  // Test an upper-bound only range in second tablet.
  {
    KuduScanner scanner(table.get());
    CHECK_OK(row->SetInt32(0, 15));
    ASSERT_OK(scanner.AddExclusiveUpperBound(*row));
    vector<string> rows;
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(15, rows.size());
    EXPECT_EQ(all_rows[0], rows.front());
    EXPECT_EQ(all_rows[14], rows.back());
  }

}

static void AssertScannersDisappear(const tserver::ScannerManager* manager) {
  // The Close call is async, so we may have to loop a bit until we see it disappear.
  // This loops for ~10sec. Typically it succeeds in only a few milliseconds.
  int i = 0;
  for (i = 0; i < 500; i++) {
    if (manager->CountActiveScanners() == 0) {
      LOG(INFO) << "Successfully saw scanner close on iteration " << i;
      return;
    }
    // Sleep 2ms on first few times through, then longer on later iterations.
    SleepFor(MonoDelta::FromMilliseconds(i < 10 ? 2 : 20));
  }
  FAIL() << "Waited too long for the scanner to close";
}

namespace {

int64_t SumResults(const KuduScanBatch& batch) {
  int64_t sum = 0;
  for (const KuduScanBatch::RowPtr& row : batch) {
    int32_t val;
    CHECK_OK(row.GetInt32(0, &val));
    sum += val;
  }
  return sum;
}

} // anonymous namespace

TEST_F(ClientTest, TestScannerKeepAlive) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  NO_FATALS(InsertTestRows(client_table_.get(), 1000));
  // Set the scanner TTL low.
  FLAGS_scanner_ttl_ms = 500;
  // Start a scan but don't get the whole data back
  KuduScanner scanner(client_table_.get());
  // This will make sure we have to do multiple NextBatch calls to the second tablet.
  ASSERT_OK(scanner.SetBatchSizeBytes(100));
  ASSERT_OK(scanner.Open());

  KuduScanBatch batch;
  int64_t sum = 0;

  ASSERT_TRUE(scanner.HasMoreRows());
  ASSERT_OK(scanner.NextBatch(&batch));

  // We should get only nine rows back (from the first tablet).
  ASSERT_EQ(batch.NumRows(), 9);
  sum += SumResults(batch);

  ASSERT_TRUE(scanner.HasMoreRows());

  // We're in between tablets but even if there isn't a live scanner the client should
  // still return OK to the keep alive call.
  ASSERT_OK(scanner.KeepAlive());

  // Start scanning the second tablet, but break as soon as we have some data so that
  // we have a live remote scanner on the second tablet.
  while (scanner.HasMoreRows()) {
    ASSERT_OK(scanner.NextBatch(&batch));
    if (batch.NumRows() > 0) break;
  }
  sum += SumResults(batch);
  ASSERT_TRUE(scanner.HasMoreRows());

  // Now loop while keeping the scanner alive. Each loop we sleep about 1/10
  // of the scanner's TTL interval to avoid flakiness due to scheduling
  // anomalies. The garbage collector runs each 50 msec as well in this test
  // scenario (controlled by FLAGS_scanner_gc_check_interval_us).
  for (int i = 0; i < 15; i++) {
    SleepFor(MonoDelta::FromMilliseconds(50));
    ASSERT_OK(scanner.KeepAlive());
  }

  // Get a second batch before sleeping/keeping alive some more. This is test for a bug
  // where we would only actually perform a KeepAlive() rpc after the first request and
  // not on subsequent ones.
  while (scanner.HasMoreRows()) {
    ASSERT_OK(scanner.NextBatch(&batch));
    if (batch.NumRows() > 0) break;
  }

  ASSERT_TRUE(scanner.HasMoreRows());
  for (int i = 0; i < 15; i++) {
    SleepFor(MonoDelta::FromMilliseconds(50));
    ASSERT_OK(scanner.KeepAlive());
  }
  sum += SumResults(batch);

  // Loop to get the remaining rows.
  while (scanner.HasMoreRows()) {
    ASSERT_OK(scanner.NextBatch(&batch));
    sum += SumResults(batch);
  }
  ASSERT_FALSE(scanner.HasMoreRows());
  ASSERT_EQ(sum, 499500);
}

// Test cleanup of scanners on the server side when closed.
TEST_F(ClientTest, TestCloseScanner) {
  NO_FATALS(InsertTestRows(client_table_.get(), 10));

  const tserver::ScannerManager* manager =
    cluster_->mini_tablet_server(0)->server()->scanner_manager();
  // Open the scanner, make sure it gets closed right away
  {
    SCOPED_TRACE("Implicit close");
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(scanner.Open());
    ASSERT_EQ(0, manager->CountActiveScanners());
    scanner.Close();
    AssertScannersDisappear(manager);
  }

  // Open the scanner, make sure we see 1 registered scanner.
  {
    SCOPED_TRACE("Explicit close");
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(scanner.SetBatchSizeBytes(0)); // won't return data on open
    ASSERT_OK(scanner.Open());
    ASSERT_EQ(1, manager->CountActiveScanners());
    scanner.Close();
    AssertScannersDisappear(manager);
  }

  {
    SCOPED_TRACE("Close when out of scope");
    {
      KuduScanner scanner(client_table_.get());
      ASSERT_OK(scanner.SetBatchSizeBytes(0));
      ASSERT_OK(scanner.Open());
      ASSERT_EQ(1, manager->CountActiveScanners());
    }
    // Above scanner went out of scope, so the destructor should close asynchronously.
    AssertScannersDisappear(manager);
  }
}

TEST_F(ClientTest, TestScanTimeout) {
  // If we set the RPC timeout to be 0, we'll time out in the GetTableLocations
  // code path and not even discover where the tablet is hosted.
  {
    client_->data_->default_rpc_timeout_ = MonoDelta::FromSeconds(0);
    KuduScanner scanner(client_table_.get());
    Status s = scanner.Open();
    EXPECT_TRUE(s.IsTimedOut()) << s.ToString();
    EXPECT_FALSE(scanner.data_->remote_) << "should not have located any tablet";
    client_->data_->default_rpc_timeout_ = MonoDelta::FromSeconds(10);
  }

  // Warm the cache so that the subsequent timeout occurs within the scan,
  // not the lookup.
  NO_FATALS(InsertTestRows(client_table_.get(), 1));

  // The "overall operation" timed out; no replicas failed.
  {
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(scanner.SetTimeoutMillis(0));
    ASSERT_TRUE(scanner.Open().IsTimedOut());
    ASSERT_TRUE(scanner.data_->remote_) << "We should have located a tablet";
    ASSERT_EQ(0, scanner.data_->remote_->GetNumFailedReplicas());
  }

  // Insert some more rows so that the scan takes multiple batches, instead of
  // fetching all the data on the 'Open()' call.
  NO_FATALS(InsertTestRows(client_table_.get(), 1000, 1));
  {
    google::FlagSaver saver;
    FLAGS_scanner_max_batch_size_bytes = 100;
    KuduScanner scanner(client_table_.get());

    // Set the single-RPC timeout low. Since we only have a single replica of this
    // table, we'll ignore this timeout for the actual scan calls, and use the
    // scanner timeout instead.
    FLAGS_scanner_inject_latency_on_each_batch_ms = 50;
    client_->data_->default_rpc_timeout_ = MonoDelta::FromMilliseconds(1);

    // Should successfully scan.
    ASSERT_OK(scanner.Open());
    ASSERT_TRUE(scanner.HasMoreRows());
    while (scanner.HasMoreRows()) {
      KuduScanBatch batch;
      ASSERT_OK(scanner.NextBatch(&batch));
    }
  }
}

static unique_ptr<KuduError> GetSingleErrorFromSession(KuduSession* session) {
  CHECK_EQ(1, session->CountPendingErrors());
  vector<KuduError*> errors;
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  CHECK(!overflow);
  CHECK_EQ(1, errors.size());
  return unique_ptr<KuduError>(errors[0]);
}

// Simplest case of inserting through the client API: a single row
// with manual batching.
TEST_F(ClientTest, TestInsertSingleRowManualBatch) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_FALSE(session->HasPendingOperations());

  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  unique_ptr<KuduInsert> insert(client_table_->NewInsert());
  // Try inserting without specifying a key: should fail.
  ASSERT_OK(insert->mutable_row()->SetInt32("int_val", 54321));
  ASSERT_OK(insert->mutable_row()->SetStringCopy("string_val", "hello world"));

  KuduInsert* ptr = insert.get();
  Status s = session->Apply(insert.release());
  ASSERT_EQ("Illegal state: Key not specified: "
            R"(INSERT int32 int_val=54321, string string_val="hello world")",
            s.ToString());

  // Get error
  ASSERT_EQ(session->CountPendingErrors(), 1) << "Should report bad key to error container";
  unique_ptr<KuduError> error = GetSingleErrorFromSession(session.get());
  KuduWriteOperation* failed_op = error->release_failed_op();
  ASSERT_EQ(failed_op, ptr) << "Should be able to retrieve failed operation";
  insert.reset(ptr);

  // Retry
  ASSERT_OK(insert->mutable_row()->SetInt32("key", 12345));
  ASSERT_OK(session->Apply(insert.release()));
  ASSERT_TRUE(session->HasPendingOperations()) << "Should be pending until we Flush";

  FlushSessionOrDie(session);
}

static void DoVerifyMetrics(const KuduSession* session,
                            int64_t successful_inserts,
                            int64_t insert_ignore_errors,
                            int64_t successful_upserts,
                            int64_t upsert_ignore_errors,
                            int64_t successful_updates,
                            int64_t update_ignore_errors,
                            int64_t successful_deletes,
                            int64_t delete_ignore_errors) {
  auto metrics = session->GetWriteOpMetrics().Get();
  ASSERT_EQ(successful_inserts, metrics["successful_inserts"]);
  ASSERT_EQ(insert_ignore_errors, metrics["insert_ignore_errors"]);
  ASSERT_EQ(successful_upserts, metrics["successful_upserts"]);
  ASSERT_EQ(upsert_ignore_errors, metrics["upsert_ignore_errors"]);
  ASSERT_EQ(successful_updates, metrics["successful_updates"]);
  ASSERT_EQ(update_ignore_errors, metrics["update_ignore_errors"]);
  ASSERT_EQ(successful_deletes, metrics["successful_deletes"]);
  ASSERT_EQ(delete_ignore_errors, metrics["delete_ignore_errors"]);
}

TEST_F(ClientTest, TestInsertIgnore) {
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  {
    unique_ptr<KuduInsert> insert(BuildTestInsert(client_table_.get(), 1));
    ASSERT_OK(session->Apply(insert.release()));
    NO_FATALS(DoTestVerifyRows(client_table_, 1));
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 0, 0, 0, 0));
  }

  {
    // INSERT IGNORE results in no error on duplicate primary key.
    unique_ptr<KuduInsertIgnore> insert_ignore(BuildTestInsertIgnore(client_table_.get(), 1));
    ASSERT_OK(session->Apply(insert_ignore.release()));
    NO_FATALS(DoTestVerifyRows(client_table_, 1));
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 1, 0, 0, 0, 0, 0, 0));
  }

  {
    // INSERT IGNORE cannot update row.
    unique_ptr<KuduInsertIgnore> insert_ignore(client_table_->NewInsertIgnore());
    ASSERT_OK(insert_ignore->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(insert_ignore->mutable_row()->SetInt32("int_val", 999));
    ASSERT_OK(insert_ignore->mutable_row()->SetStringCopy("string_val", "hello world"));
    ASSERT_OK(insert_ignore->mutable_row()->SetInt32("non_null_with_default", 999));
    ASSERT_OK(session->Apply(insert_ignore.release())); // returns ok but results in no change
    NO_FATALS(DoTestVerifyRows(client_table_, 1));
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 2, 0, 0, 0, 0, 0, 0));
  }

  {
    // INSERT IGNORE can insert new row.
    unique_ptr<KuduInsertIgnore> insert_ignore(BuildTestInsertIgnore(client_table_.get(), 2));
    ASSERT_OK(session->Apply(insert_ignore.release()));
    NO_FATALS(DoTestVerifyRows(client_table_, 2));
    NO_FATALS(DoVerifyMetrics(session.get(), 2, 2, 0, 0, 0, 0, 0, 0));
  }
}

TEST_F(ClientTest, TestUpdate) {
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  {
    // UPDATE results in an error on missing primary key.
    unique_ptr<KuduUpdate> update(BuildTestUpdate(client_table_.get(), 1));
    Status s = session->Apply(update.release());
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "failed to flush data: error details are available "
                        "via KuduSession::GetPendingErrors()");
    vector<KuduError*> errors;
    ElementDeleter d(&errors);
    bool overflow;
    session->GetPendingErrors(&errors, &overflow);
    NO_FATALS(DoTestVerifyRows(client_table_, 0));
    NO_FATALS(DoVerifyMetrics(session.get(), 0, 0, 0, 0, 0, 0, 0, 0));
  }

  {
    unique_ptr<KuduInsert> insert(BuildTestInsert(client_table_.get(), 1));
    ASSERT_OK(session->Apply(insert.release()));
    NO_FATALS(DoTestVerifyRows(client_table_, 1));
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 0, 0, 0, 0));  // successful_inserts++
  }

  {
    // UPDATE can update row.
    unique_ptr<KuduUpdate> update(client_table_->NewUpdate());
    ASSERT_OK(update->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(update->mutable_row()->SetInt32("int_val", 999));
    ASSERT_OK(update->mutable_row()->SetStringCopy("string_val", "hello world"));
    ASSERT_OK(update->mutable_row()->SetInt32("non_null_with_default", 999));
    ASSERT_OK(session->Apply(update.release()));
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 1, 0, 0, 0));  // successful_updates++

    vector<string> rows;
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(1, rows.size());
    ASSERT_EQ("(int32 key=1, int32 int_val=999, string string_val=\"hello world\", "
        "int32 non_null_with_default=999)", rows[0]);
  }
}

TEST_F(ClientTest, TestUpdateIgnore) {
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  {
    // UPDATE IGNORE results in no error on missing primary key.
    unique_ptr<KuduUpdateIgnore> update_ignore(BuildTestUpdateIgnore(client_table_.get(), 1));
    ASSERT_OK(session->Apply(update_ignore.release()));
    NO_FATALS(DoTestVerifyRows(client_table_, 0));
    NO_FATALS(DoVerifyMetrics(session.get(), 0, 0, 0, 0, 0, 1, 0, 0));
  }

  {
    unique_ptr<KuduInsert> insert(BuildTestInsert(client_table_.get(), 1));
    ASSERT_OK(session->Apply(insert.release()));
    NO_FATALS(DoTestVerifyRows(client_table_, 1));
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 0, 1, 0, 0));
  }

  {
    // UPDATE IGNORE can update row.
    unique_ptr<KuduUpdateIgnore> update_ignore(client_table_->NewUpdateIgnore());
    ASSERT_OK(update_ignore->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(update_ignore->mutable_row()->SetInt32("int_val", 999));
    ASSERT_OK(update_ignore->mutable_row()->SetStringCopy("string_val", "hello world"));
    ASSERT_OK(update_ignore->mutable_row()->SetInt32("non_null_with_default", 999));
    ASSERT_OK(session->Apply(update_ignore.release()));
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 1, 1, 0, 0));

    vector<string> rows;
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(1, rows.size());
    ASSERT_EQ("(int32 key=1, int32 int_val=999, string string_val=\"hello world\", "
              "int32 non_null_with_default=999)", rows[0]);
  }
}

TEST_F(ClientTest, TestDeleteIgnore) {
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  {
    unique_ptr<KuduInsert> insert(BuildTestInsert(client_table_.get(), 1));
    ASSERT_OK(session->Apply(insert.release()));
    DoTestVerifyRows(client_table_, 1);
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 0, 0, 0, 0));
  }

  {
    // DELETE IGNORE can delete row.
    unique_ptr<KuduDeleteIgnore> delete_ignore(BuildTestDeleteIgnore(client_table_.get(), 1));
    ASSERT_OK(session->Apply(delete_ignore.release()));
    DoTestVerifyRows(client_table_, 0);
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 0, 0, 1, 0));
  }

  {
    // DELETE IGNORE results in no error on missing primary key.
    unique_ptr<KuduDeleteIgnore> delete_ignore(BuildTestDeleteIgnore(client_table_.get(), 1));
    ASSERT_OK(session->Apply(delete_ignore.release()));
    DoTestVerifyRows(client_table_, 0);
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 0, 0, 1, 1));
  }
}

TEST_F(ClientTest, TestInsertAutoFlushSync) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_FALSE(session->HasPendingOperations());

  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  // Test in Flush() is called implicitly,
  // so there is no pending operations.
  unique_ptr<KuduInsert> insert(client_table_->NewInsert());
  ASSERT_OK(insert->mutable_row()->SetInt32("key", 12345));
  ASSERT_OK(insert->mutable_row()->SetInt32("int_val", 54321));
  ASSERT_OK(insert->mutable_row()->SetStringCopy("string_val", "hello world"));
  ASSERT_OK(session->Apply(insert.release()));
  ASSERT_TRUE(insert == nullptr) << "Successful insert should take ownership";
  ASSERT_FALSE(session->HasPendingOperations()) << "Should not have pending operation";

  // Test multiple inserts.
  for (int i = 0; i < 100; i++) {
    unique_ptr<KuduInsert> insert(client_table_->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
    ASSERT_OK(insert->mutable_row()->SetInt32("int_val", 54321));
    ASSERT_OK(insert->mutable_row()->SetStringCopy("string_val", "hello world"));
    ASSERT_OK(session->Apply(insert.release()));
  }
}

static Status ApplyInsertToSession(
    KuduSession* session,
    const shared_ptr<KuduTable>& table,
    int row_key,
    int int_val,
    const char* string_val,
    optional<int> non_null_with_default = nullopt) {
  unique_ptr<KuduInsert> insert(table->NewInsert());
  RETURN_NOT_OK(insert->mutable_row()->SetInt32("key", row_key));
  RETURN_NOT_OK(insert->mutable_row()->SetInt32("int_val", int_val));
  RETURN_NOT_OK(insert->mutable_row()->SetStringCopy("string_val", string_val));
  if (non_null_with_default) {
    RETURN_NOT_OK(insert->mutable_row()->SetInt32("non_null_with_default",
                                                  *non_null_with_default));
  }
  return session->Apply(insert.release());
}

static Status ApplyUpsertToSession(KuduSession* session,
                                   const shared_ptr<KuduTable>& table,
                                   int row_key,
                                   int int_val,
                                   const char* string_val,
                                   optional<int> imm_val = nullopt) {
  unique_ptr<KuduUpsert> upsert(table->NewUpsert());
  RETURN_NOT_OK(upsert->mutable_row()->SetInt32("key", row_key));
  RETURN_NOT_OK(upsert->mutable_row()->SetInt32("int_val", int_val));
  RETURN_NOT_OK(upsert->mutable_row()->SetStringCopy("string_val", string_val));
  if (table->schema().HasColumn("imm_val", nullptr)) {
    if (imm_val) {
      RETURN_NOT_OK(upsert->mutable_row()->SetInt32("imm_val", *imm_val));
    } else {
      RETURN_NOT_OK(upsert->mutable_row()->SetNull("imm_val"));
    }
  }
  return session->Apply(upsert.release());
}

static Status ApplyUpdateToSession(KuduSession* session,
                                   const shared_ptr<KuduTable>& table,
                                   int row_key,
                                   int int_val) {
  unique_ptr<KuduUpdate> update(table->NewUpdate());
  RETURN_NOT_OK(update->mutable_row()->SetInt32("key", row_key));
  RETURN_NOT_OK(update->mutable_row()->SetInt32("int_val", int_val));
  return session->Apply(update.release());
}

static Status ApplyDeleteToSession(KuduSession* session,
                                   const shared_ptr<KuduTable>& table,
                                   int row_key,
                                   optional<int> int_val = nullopt) {
  unique_ptr<KuduDelete> del(table->NewDelete());
  RETURN_NOT_OK(del->mutable_row()->SetInt32("key", row_key));
  if (int_val) {
    RETURN_NOT_OK(del->mutable_row()->SetInt32("int_val", *int_val));
  }
  return session->Apply(del.release());
}

TEST_F(ClientTest, TestWriteTimeout) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
#if defined(THREAD_SANITIZER) || defined(ADDRESS_SANITIZER)
  static const int kSessionTimeoutMs = 2000;
#else
  static const int kSessionTimeoutMs = 200;
#endif
  session->SetTimeoutMillis(kSessionTimeoutMs);

  // First time out the lookup on the master side.
  {
    google::FlagSaver saver;
    FLAGS_master_inject_latency_on_tablet_lookups_ms = kSessionTimeoutMs + 10;

    ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "row"));
    Status s = session->Flush();
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    unique_ptr<KuduError> error = GetSingleErrorFromSession(session.get());
    const Status& status = error->status();
    ASSERT_TRUE(status.IsTimedOut()) << status.ToString();
    ASSERT_STR_CONTAINS(status.ToString(),
                        "LookupRpc { table: 'client-testtb', "
                        "partition-key: (RANGE (key): 1), attempt: 1 } failed: "
                        "LookupRpc timed out after deadline expired");
  }

  // Next time out the actual write on the tablet server.
  {
    google::FlagSaver saver;
    FLAGS_log_inject_latency = true;
    FLAGS_log_inject_latency_ms_mean = kSessionTimeoutMs + 10;
    FLAGS_log_inject_latency_ms_stddev = 0;

    ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "row"));
    Status s = session->Flush();
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    unique_ptr<KuduError> error = GetSingleErrorFromSession(session.get());
    const Status& status = error->status();
    ASSERT_TRUE(status.IsTimedOut()) << status.ToString();
    ASSERT_STR_MATCHES(status.ToString(),
                       R"(Failed to write batch of 1 ops to tablet.*after 1 attempt.*)"
                       R"(Write RPC to 127\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:.*timed out)");
  }
}

// Test that Write RPCs get properly retried through the duration of a restart.
// This tests the narrow windows during startup and shutdown that RPC services
// are unregistered, checking that any resuling  "missing service" errors don't
// get propogated to end users.
TEST_F(ClientTest, TestWriteWhileRestarting) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  Status writer_error;
  int row_id = 1;

  // Writes a row and checks for errors.
  const auto& write_and_check_error = [&] {
    RETURN_NOT_OK(ApplyInsertToSession(session.get(), client_table_, row_id++, 1, "row"));
    RETURN_NOT_OK(session->Flush());
    // If we successfully flush, check for errors.
    vector<KuduError*> errors;
    bool overflow;
    session->GetPendingErrors(&errors, &overflow);
    CHECK(!overflow);
    if (PREDICT_FALSE(!errors.empty())) {
      return errors[0]->status();
    }
    return Status::OK();
  };

  // Until we finish restarting, hit the tablet server with write requests.
  CountDownLatch stop(1);
  thread t([&] {
    while (writer_error.ok() && stop.count() == 1) {
      writer_error = write_and_check_error();
    }
  });
  auto thread_joiner = MakeScopedCleanup([&] { t.join(); });
  auto* ts = cluster_->mini_tablet_server(0);
  ts->Shutdown();
  ASSERT_OK(ts->Restart());
  stop.CountDown();
  thread_joiner.cancel();
  t.join();

  // The writer thread should have hit no issues.
  ASSERT_OK(writer_error);
}

TEST_F(ClientTest, TestFailedDnsResolution) {
  // Create a dedicated instance of a client which doesn't cache DNS resolution
  // results. This is to avoid using the DNS resolver cache if the hostname/IP
  // address of tablet server is the same as master's address. The latter is
  // the case when using other than UNIQUE_LOOPBACK binding mode for the
  // server components of the test mini-cluster.
  FLAGS_dns_resolver_cache_capacity_mb = 0;
  shared_ptr<KuduClient> c;
  ASSERT_OK(KuduClientBuilder()
      .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
      .Build(&c));
  shared_ptr<KuduTable> t;
  ASSERT_OK(c->OpenTable(kTableName, &t));

  shared_ptr<KuduSession> session = c->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // First, make DNS resolution time out.
  // Set the timeout to be short since we know it can't succeed, but not to the
  // point where we can timeout before getting the DNS error.
  FLAGS_fail_dns_resolution = true;
  session->SetTimeoutMillis(1000);

  // Due to KUDU-1466, there is a narrow window in which the error reported
  // might be that the GetTableLocations RPC to the master timed out instead of
  // the expected DNS resolution error while trying to send Write RPC to
  // tablet server.
  for (auto i = 0;; ++i) {
    constexpr const char* const kMasterErrors[] = {
      "timed out after deadline expired: GetTableLocations RPC",
      "LookupRpc timed out after deadline expired",
    };
    ASSERT_OK(ApplyInsertToSession(session.get(), t, 1, 1, "row"));
    auto s = session->Flush();
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    unique_ptr<KuduError> error = GetSingleErrorFromSession(session.get());
    ASSERT_TRUE(error->status().IsTimedOut()) << error->status().ToString();
    const auto row_status_str = error->status().ToString();
    if (row_status_str.find(kMasterErrors[0]) != std::string::npos ||
        row_status_str.find(kMasterErrors[1]) != std::string::npos) {
      ASSERT_LE(i, 10) << "could not get DNS resolution error after 10 tries";
      continue;
    }
    ASSERT_STR_CONTAINS(row_status_str,
                        "Network error: Failed to resolve address for TS");
    break;
  }

  // Now, re-enable the DNS resolution: the write should succeed.
  FLAGS_fail_dns_resolution = false;
  ASSERT_OK(ApplyInsertToSession(session.get(), t, 1, 1, "row"));
  ASSERT_OK(session->Flush());
}

// Test which does an async flush and then drops the reference
// to the Session. This should still call the callback.
TEST_F(ClientTest, TestAsyncFlushResponseAfterSessionDropped) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "row"));
  Synchronizer s;
  KuduStatusMemberCallback<Synchronizer> cb(&s, &Synchronizer::StatusCB);
  session->FlushAsync(&cb);
  session.reset();
  ASSERT_OK(s.Wait());

  // Try again, this time with an error response (trying to re-insert the same row).
  s.Reset();
  session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "row"));
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  ASSERT_EQ(1, session->CountBufferedOperations());
  session->FlushAsync(&cb);
  ASSERT_EQ(0, session->CountBufferedOperations());
#pragma GCC diagnostic pop
  session.reset();
  ASSERT_FALSE(s.Wait().ok());
}

TEST_F(ClientTest, TestSessionClose) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "row"));
  // Closing the session now should return Status::IllegalState since we
  // have a pending operation.
  ASSERT_TRUE(session->Close().IsIllegalState());

  Synchronizer s;
  KuduStatusMemberCallback<Synchronizer> cb(&s, &Synchronizer::StatusCB);
  session->FlushAsync(&cb);
  ASSERT_OK(s.Wait());

  ASSERT_OK(session->Close());
}

// Test which sends multiple batches through the same session, each of which
// contains multiple rows spread across multiple tablets.
TEST_F(ClientTest, TestMultipleMultiRowManualBatches) {
  shared_ptr<KuduTable> second_table;
  ASSERT_OK(CreateTable("second table", 1, {}, {}, &second_table));

  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  const int kNumBatches = 5;
  const int kRowsPerBatch = 10;

  int row_key = 0;

  for (int batch_num = 0; batch_num < kNumBatches; batch_num++) {
    for (int i = 0; i < kRowsPerBatch; i++) {
      ASSERT_OK(ApplyInsertToSession(
                         session.get(),
                         (row_key % 2 == 0) ? client_table_ : second_table,
                         row_key, row_key * 10, "hello world"));
      row_key++;
    }
    ASSERT_TRUE(session->HasPendingOperations()) << "Should be pending until we Flush";
    FlushSessionOrDie(session);
    ASSERT_FALSE(session->HasPendingOperations()) << "Should have no more pending ops after flush";
  }

  const int kNumRowsPerTablet = kNumBatches * kRowsPerBatch / 2;
  ASSERT_EQ(kNumRowsPerTablet, CountRowsFromClient(client_table_.get()));
  ASSERT_EQ(kNumRowsPerTablet, CountRowsFromClient(second_table.get()));

  // Verify the data looks right.
  vector<string> rows;
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows, ScannedRowsOrder::kSorted));
  ASSERT_EQ(kNumRowsPerTablet, rows.size());
  ASSERT_EQ(R"((int32 key=0, int32 int_val=0, string string_val="hello world", )"
            "int32 non_null_with_default=12345)", rows[0]);
}

// Test a batch where one of the inserted rows succeeds while another fails.
// 1. Insert duplicate keys.
TEST_F(ClientTest, TestBatchWithPartialErrorOfDuplicateKeys) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Insert a row with key "1"
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "original row"));
  FlushSessionOrDie(session);

  // Now make a batch that has key "1" (which will fail) along with
  // key "2" which will succeed. Flushing should return an error.
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "Attempted dup"));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 2, 1, "Should succeed"));
  Status s = session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "failed to flush data: error details are available "
                      "via KuduSession::GetPendingErrors()");
  // Fetch and verify the reported error.
  unique_ptr<KuduError> error;
  NO_FATALS(error = GetSingleErrorFromSession(session.get()));
  ASSERT_TRUE(error->status().IsAlreadyPresent());
  ASSERT_EQ(error->status().ToString(),
            "Already present: key already present");
  ASSERT_EQ(error->failed_op().ToString(),
            R"(INSERT int32 key=1, int32 int_val=1, string string_val="Attempted dup")");

  // Verify that the other row was successfully inserted
  vector<string> rows;
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows, ScannedRowsOrder::kSorted));
  ASSERT_EQ(2, rows.size());
  ASSERT_EQ(R"((int32 key=1, int32 int_val=1, string string_val="original row", )"
            "int32 non_null_with_default=12345)", rows[0]);
  ASSERT_EQ(R"((int32 key=2, int32 int_val=1, string string_val="Should succeed", )"
            "int32 non_null_with_default=12345)", rows[1]);
}

// 2. Insert a row missing a required column.
TEST_F(ClientTest, TestBatchWithPartialErrorOfMissingRequiredColumn) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Remove default value of a non-nullable column.
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  table_alterer->AlterColumn("non_null_with_default")->RemoveDefault();
  ASSERT_OK(table_alterer->Alter());

  // Insert a row successfully.
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "Should succeed", 1));
  // Insert a row missing a required column, which will fail.
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 2, 2, "Missing required column"));
  Status s = session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "failed to flush data: error details are available "
                      "via KuduSession::GetPendingErrors()");
  // Fetch and verify the reported error.
  unique_ptr<KuduError> error;
  NO_FATALS(error = GetSingleErrorFromSession(session.get()));
  ASSERT_TRUE(error->status().IsInvalidArgument());
  ASSERT_EQ(error->status().ToString(),
            "Invalid argument: No value provided for required column: "
            "non_null_with_default INT32 NOT NULL");
  ASSERT_EQ(error->failed_op().ToString(),
            R"(INSERT int32 key=2, int32 int_val=2, )"
            R"(string string_val="Missing required column")");

  // Verify that the other row was successfully inserted
  vector<string> rows;
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ(R"((int32 key=1, int32 int_val=1, string string_val="Should succeed", )"
            "int32 non_null_with_default=1)", rows[0]);
}

// 3. No fields updated for a row.
TEST_F(ClientTest, TestBatchWithPartialErrorOfNoFieldsUpdated) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Insert two rows successfully.
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "One"));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 2, 2, "Two"));
  ASSERT_OK(session->Flush());

  // Update a row without any non-key fields updated, which will fail.
  unique_ptr<KuduUpdate> update(client_table_->NewUpdate());
  ASSERT_OK(update->mutable_row()->SetInt32("key", 1));
  ASSERT_OK(session->Apply(update.release()));
  // Update a row with some non-key fields updated, which will success.
  ASSERT_OK(ApplyUpdateToSession(session.get(), client_table_, 2, 22));
  Status s = session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "failed to flush data: error details are available "
                      "via KuduSession::GetPendingErrors()");
  // Fetch and verify the reported error.
  unique_ptr<KuduError> error;
  NO_FATALS(error = GetSingleErrorFromSession(session.get()));
  ASSERT_TRUE(error->status().IsInvalidArgument());
  ASSERT_EQ(error->status().ToString(),
            "Invalid argument: No fields updated, key is: (int32 key=1)");
  ASSERT_EQ(error->failed_op().ToString(), R"(UPDATE int32 key=1)");

  // Verify that the other row was successfully updated.
  vector<string> rows;
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows, ScannedRowsOrder::kSorted));
  ASSERT_EQ(2, rows.size());
  ASSERT_EQ(R"((int32 key=1, int32 int_val=1, string string_val="One", )"
            "int32 non_null_with_default=12345)", rows[0]);
  ASSERT_EQ(R"((int32 key=2, int32 int_val=22, string string_val="Two", )"
            "int32 non_null_with_default=12345)", rows[1]);
}

// 4. Delete a row with a non-key column specified.
TEST_F(ClientTest, TestBatchWithPartialErrorOfNonKeyColumnSpecifiedDelete) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Insert two rows successfully.
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "One"));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 2, 2, "Two"));
  ASSERT_OK(session->Flush());

  // Delete a row without any non-key fields, which will success.
  ASSERT_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  // Delete a row with some non-key fields, which will fail.
  ASSERT_OK(ApplyDeleteToSession(session.get(), client_table_, 2, 2));
  Status s = session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "failed to flush data: error details are available "
                      "via KuduSession::GetPendingErrors()");
  // Fetch and verify the reported error.
  unique_ptr<KuduError> error;
  NO_FATALS(error = GetSingleErrorFromSession(session.get()));
  ASSERT_TRUE(error->status().IsInvalidArgument());
  ASSERT_EQ(error->status().ToString(),
            "Invalid argument: DELETE should not have a value for column: "
            "int_val INT32 NOT NULL");
  ASSERT_EQ(error->failed_op().ToString(), R"(DELETE int32 key=2, int32 int_val=2)");

  // Verify that the other row was successfully deleted.
  vector<string> rows;
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ(R"((int32 key=2, int32 int_val=2, string string_val="Two", )"
            "int32 non_null_with_default=12345)", rows[0]);
}

// 5. All row failed in prepare phase.
TEST_F(ClientTest, TestBatchWithPartialErrorOfAllRowsFailed) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Insert two rows successfully.
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "One"));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 2, 2, "Two"));
  ASSERT_OK(session->Flush());

  // Delete rows with some non-key fields, which will fail.
  ASSERT_OK(ApplyDeleteToSession(session.get(), client_table_, 1, 1));
  ASSERT_OK(ApplyDeleteToSession(session.get(), client_table_, 2, 2));
  Status s = session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "failed to flush data: error details are available "
                      "via KuduSession::GetPendingErrors()");
  // Fetch and verify the reported error.
  vector<KuduError*> errors;
  ElementDeleter d(&errors);
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  ASSERT_TRUE(!overflow);
  ASSERT_EQ(2, errors.size());
  ASSERT_TRUE(errors[0]->status().IsInvalidArgument());
  ASSERT_EQ(errors[0]->status().ToString(),
            "Invalid argument: DELETE should not have a value for column: "
            "int_val INT32 NOT NULL");
  ASSERT_EQ(errors[0]->failed_op().ToString(), R"(DELETE int32 key=1, int32 int_val=1)");
  ASSERT_TRUE(errors[1]->status().IsInvalidArgument());
  ASSERT_EQ(errors[1]->status().ToString(),
            "Invalid argument: DELETE should not have a value for column: "
            "int_val INT32 NOT NULL");
  ASSERT_EQ(errors[1]->failed_op().ToString(), R"(DELETE int32 key=2, int32 int_val=2)");

  // Verify that no row was deleted.
  vector<string> rows;
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows, ScannedRowsOrder::kSorted));
  ASSERT_EQ(2, rows.size());
  ASSERT_EQ(R"((int32 key=1, int32 int_val=1, string string_val="One", )"
            "int32 non_null_with_default=12345)", rows[0]);
  ASSERT_EQ(R"((int32 key=2, int32 int_val=2, string string_val="Two", )"
            "int32 non_null_with_default=12345)", rows[1]);
}

TEST_F(ClientTest, TestInsertDuplicateKeys) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  constexpr const char* kTable2Name = "client-testtb2";
  shared_ptr<KuduTable> second_table;
  ASSERT_OK(CreateTable(kTable2Name, 1, {}, {}, &second_table));

  // Insert initial rows
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "one"));
  ASSERT_OK(ApplyInsertToSession(session.get(), second_table, 1, 1, "one"));
  FlushSessionOrDie(session);

  // Try to insert a row with duplicate key and two valid rows
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 2, 1, "Should suceed"));
  ASSERT_OK(ApplyInsertToSession(session.get(), second_table, 1, 1, "Attempted dup"));
  ASSERT_OK(ApplyInsertToSession(session.get(), second_table, 2, 1, "Should succeed"));
  Status s = session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "failed to flush data: error details are available "
                      "via KuduSession::GetPendingErrors()");
  // Fetch and verify the reported errors.
  vector<KuduError*> errors;
  ElementDeleter d(&errors);
  session->GetPendingErrors(&errors, nullptr);
  // Should be able to determine the table the error belongs to
  ASSERT_EQ(1, errors.size());
  ASSERT_EQ(errors[0]->failed_op().table()->name(), second_table->name());
}

void ClientTest::DoTestWriteWithDeadServer(WhichServerToKill which) {
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(1000);
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Shut down the server.
  switch (which) {
    case DEAD_MASTER:
      cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);
      break;
    case DEAD_TSERVER:
      cluster_->mini_tablet_server(0)->Shutdown();
      break;
  }

  // Try a write.
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "x"));
  Status s = session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();

  unique_ptr<KuduError> error = GetSingleErrorFromSession(session.get());
  ASSERT_TRUE(error->status().IsTimedOut());
  // TODO(KUDU-1466) Re-enable this assertion once the jira gets solved. We can't actually
  // make an assertion on the reason for the timeout since sometimes tablet server connection
  // errors get reported as GetTabletLocations timeouts.
  // if (which == DEAD_TSERVER) {
  //   ASSERT_STR_CONTAINS(error->status().ToString(), "Connection refused");
  // }

  ASSERT_EQ(error->failed_op().ToString(),
            R"(INSERT int32 key=1, int32 int_val=1, string string_val="x")");
}

// Test error handling cases where the master is down (tablet resolution fails)
TEST_F(ClientTest, TestWriteWithDeadMaster) {
  client_->data_->default_admin_operation_timeout_ = MonoDelta::FromSeconds(1);
  DoTestWriteWithDeadServer(DEAD_MASTER);
}

// Test error handling when the TS is down (actual write fails its RPC)
TEST_F(ClientTest, TestWriteWithDeadTabletServer) {
  DoTestWriteWithDeadServer(DEAD_TSERVER);
}

void ClientTest::DoApplyWithoutFlushTest(int sleep_micros) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "x"));
  SleepFor(MonoDelta::FromMicroseconds(sleep_micros));
  session.reset(); // should not crash!

  // Should have no rows.
  vector<string> rows;
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_TRUE(rows.empty());
}


// Applies some updates to the session, and then drops the reference to the
// Session before flushing. Makes sure that the tablet resolution callbacks
// properly deal with the session disappearing underneath.
//
// This test doesn't sleep between applying the operations and dropping the
// reference, in hopes that the reference will be dropped while DNS is still
// in-flight, etc.
TEST_F(ClientTest, TestApplyToSessionWithoutFlushing_OpsInFlight) {
  DoApplyWithoutFlushTest(0);
}

// Same as the above, but sleeps a little bit after applying the operations,
// so that the operations are already in the per-TS-buffer.
TEST_F(ClientTest, TestApplyToSessionWithoutFlushing_OpsBuffered) {
  DoApplyWithoutFlushTest(10000);
}

// Apply a large amount of data (relative to size of the mutation buffer)
// without calling Flush() in MANUAL_FLUSH mode,
// and ensure that we get an error on Apply() rather than sending a too-large
// RPC to the server.
TEST_F(ClientTest, TestApplyTooMuchWithoutFlushing) {
  // Applying a bunch of small rows without a flush should result
  // in an error.
  const size_t kBufferSizeBytes = 1024;
  bool got_expected_error = false;
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_OK(session->SetMutationBufferSpace(kBufferSizeBytes));
  for (int i = 0; i < kBufferSizeBytes; i++) {
    Status s = ApplyInsertToSession(session.get(), client_table_, 1, 1, "x");
    if (s.IsIncomplete()) {
      ASSERT_STR_CONTAINS(s.ToString(), "not enough mutation buffer space");
      got_expected_error = true;
      break;
    } else {
      ASSERT_OK(s);
    }
  }
  ASSERT_TRUE(got_expected_error);
  EXPECT_TRUE(session->HasPendingOperations());
}

// Applying a big operation which does not fit into the buffer should
// return an error with session running in any supported flush mode.
TEST_F(ClientTest, TestCheckMutationBufferSpaceLimitInEffect) {
  const size_t kBufferSizeBytes = 256;
  const string kLongString(kBufferSizeBytes + 1, 'x');
  const KuduSession::FlushMode kFlushModes[] = {
    KuduSession::AUTO_FLUSH_BACKGROUND,
    KuduSession::AUTO_FLUSH_SYNC,
    KuduSession::MANUAL_FLUSH,
  };

  for (auto mode : kFlushModes) {
    Status s;
    shared_ptr<KuduSession> session(client_->NewSession());
    ASSERT_OK(session->SetFlushMode(mode));
    ASSERT_FALSE(session->HasPendingOperations());

    ASSERT_OK(session->SetMutationBufferSpace(kBufferSizeBytes));
    s = ApplyInsertToSession(
          session.get(), client_table_, 0, 1, kLongString.c_str());
    ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
    EXPECT_FALSE(session->HasPendingOperations());
    vector<KuduError*> errors;
    ElementDeleter deleter(&errors);
    bool overflowed;
    session->GetPendingErrors(&errors, &overflowed);
    EXPECT_FALSE(overflowed);
    ASSERT_EQ(1, errors.size());
    EXPECT_TRUE(errors[0]->status().IsIncomplete());
    EXPECT_EQ(s.ToString(), errors[0]->status().ToString());
  }
}

// For a KuduSession object, it should be OK to switch between flush modes
// in the middle if there is no pending operations in the buffer.
TEST_F(ClientTest, TestSwitchFlushModes) {
  const size_t kBufferSizeBytes = 256;
  const string kLongString(kBufferSizeBytes / 2, 'x');
  const int32_t kFlushIntervalMs = 100;

  shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetMutationBufferSpace(kBufferSizeBytes));
  // Start with the AUTO_FLUSH_SYNC mode.
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
  ASSERT_OK(session->SetMutationBufferFlushInterval(kFlushIntervalMs));
  ASSERT_FALSE(session->HasPendingOperations());

  ASSERT_OK(ApplyInsertToSession(
      session.get(), client_table_, 0, 1, kLongString.c_str()));
  // No pending ops: flush should happen synchronously during the Apply() call.
  ASSERT_FALSE(session->HasPendingOperations());

  // Switch to the MANUAL_FLUSH mode.
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_OK(ApplyInsertToSession(
      session.get(), client_table_, 1, 2, kLongString.c_str()));
  ASSERT_TRUE(session->HasPendingOperations());
  ASSERT_OK(session->Flush());
  ASSERT_FALSE(session->HasPendingOperations());

  // Switch to the AUTO_FLUSH_BACKGROUND mode.
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  ASSERT_OK(ApplyInsertToSession(
      session.get(), client_table_, 2, 3, kLongString.c_str()));
  ASSERT_OK(session->Flush());
  // There should be no pending ops: the background flusher should do its job.
  ASSERT_FALSE(session->HasPendingOperations());

  // Switch back to the MANUAL_FLUSH mode.
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH);
  ASSERT_OK(ApplyInsertToSession(
      session.get(), client_table_, 4, 5, kLongString.c_str()));
  WaitForAutoFlushBackground(session, kFlushIntervalMs);
  // There should be some pending ops: the automatic background flush
  // should not be active after switch to the MANUAL_FLUSH mode.
  ASSERT_TRUE(session->HasPendingOperations());
  ASSERT_OK(session->Flush()));
  ASSERT_FALSE(session->HasPendingOperations());
}

void ClientTest::TimeInsertOpBatch(
    KuduSession::FlushMode mode,
    size_t buffer_size,
    size_t run_idx,
    size_t run_num,
    const vector<size_t>& string_sizes,
    CpuTimes* elapsed) {
  const string mode_str = FlushModeToString(mode);
  const size_t row_num = string_sizes.size();
  shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetMutationBufferSpace(buffer_size));
  ASSERT_OK(session->SetMutationBufferFlushWatermark(0.5));
  ASSERT_OK(session->SetMutationBufferMaxNum(2));
  ASSERT_OK(session->SetFlushMode(mode));
  Stopwatch sw(Stopwatch::ALL_THREADS);
  LOG_TIMING(INFO, "Running in " + mode_str + " mode") {
    sw.start();
    for (size_t i = 0; i < row_num; ++i) {
      const string long_string(string_sizes[i], '0');
      const size_t idx = run_num * i + run_idx;
      ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, idx, idx,
                                     long_string.c_str()));
    }
    EXPECT_OK(session->Flush());
    sw.stop();
  }
  ASSERT_EQ(0, session->CountPendingErrors());
  if (elapsed != nullptr) {
    *elapsed = sw.elapsed();
  }
}

// Test for acceptable values of the maximum number of batchers for KuduSession.
TEST_F(ClientTest, TestSetSessionMutationBufferMaxNum) {
  shared_ptr<KuduSession> session(client_->NewSession());
  // The default for the maximum number of batchers.
  EXPECT_OK(session->SetMutationBufferMaxNum(2));
  // Check for minimum acceptable limit for number of batchers.
  EXPECT_OK(session->SetMutationBufferMaxNum(1));
  // Unlimited number of batchers.
  EXPECT_OK(session->SetMutationBufferMaxNum(0));
  // Non-default acceptable number of batchers.
  EXPECT_OK(session->SetMutationBufferMaxNum(8));
  // Check it's impossible to update maximum number of batchers if there are
  // pending operations.
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 0, 0, "x"));
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  ASSERT_EQ(1, session->CountBufferedOperations());
#pragma GCC diagnostic pop
  ASSERT_EQ(0, session->CountPendingErrors());
  ASSERT_TRUE(session->HasPendingOperations());
  Status s = session->SetMutationBufferMaxNum(3);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "Cannot change the limit on maximum number of batchers");
  ASSERT_OK(session->Flush());
  ASSERT_EQ(0, session->CountPendingErrors());
  ASSERT_FALSE(session->HasPendingOperations());
}

// Check that call to Flush()/FlushAsync() is safe if there isn't current
// batcher for the session (i.e., no error is expected on calling those).
TEST_F(ClientTest, TestFlushNoCurrentBatcher) {
  shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetMutationBufferMaxNum(1));
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  ASSERT_FALSE(session->HasPendingOperations());
  ASSERT_EQ(0, session->CountPendingErrors());
  Synchronizer sync0;
  KuduStatusMemberCallback<Synchronizer> scbk0(&sync0, &Synchronizer::StatusCB);
  // No current batcher since nothing has been applied yet.
  session->FlushAsync(&scbk0);
  ASSERT_OK(sync0.Wait());
  // The same with the synchronous flush.
  ASSERT_OK(session->Flush());
  ASSERT_FALSE(session->HasPendingOperations());
  ASSERT_EQ(0, session->CountPendingErrors());

  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 0, 0, "0"));
  ASSERT_TRUE(session->HasPendingOperations());
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  ASSERT_EQ(1, session->CountBufferedOperations());
#pragma GCC diagnostic pop
  ASSERT_EQ(0, session->CountPendingErrors());
  // OK, now the current batcher should be at place.
  ASSERT_OK(session->Flush());
  ASSERT_FALSE(session->HasPendingOperations());
  ASSERT_EQ(0, session->CountPendingErrors());

  // Once more: now there isn't current batcher again.
  ASSERT_FALSE(session->HasPendingOperations());
  ASSERT_EQ(0, session->CountPendingErrors());
  Synchronizer sync1;
  KuduStatusMemberCallback<Synchronizer> scbk1(&sync1, &Synchronizer::StatusCB);
  session->FlushAsync(&scbk1);
  ASSERT_OK(sync1.Wait());
  // The same with the synchronous flush.
  ASSERT_OK(session->Flush());
  ASSERT_FALSE(session->HasPendingOperations());
  ASSERT_EQ(0, session->CountPendingErrors());
}

// Check that the limit on maximum number of batchers per KuduSession
// is enforced. KuduSession::Apply() should block if called when the number
// of batchers is at the limit already.
TEST_F(ClientTest, TestSessionMutationBufferMaxNum) {
  const size_t kBufferSizeBytes = 1024;
  const size_t kBufferMaxLimit = 8;
  for (size_t limit = 1; limit < kBufferMaxLimit; ++limit) {
    shared_ptr<KuduSession> session(client_->NewSession());
    ASSERT_OK(session->SetMutationBufferSpace(kBufferSizeBytes));
    ASSERT_OK(session->SetMutationBufferMaxNum(limit));
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

    size_t monitor_max_batchers_count = 0;
    CountDownLatch monitor_run_ctl(1);
    thread monitor([&]() {
      MonitorSessionBatchersCount(session.get(),
                                  &monitor_run_ctl, &monitor_max_batchers_count);
    });

    // Apply a big number of tiny operations, flushing after each to utilize
    // maximum possible number of session's batchers.
    for (size_t i = 0; i < limit * 16; ++i) {
      ASSERT_OK(ApplyInsertToSession(session.get(), client_table_,
                                     kBufferMaxLimit * i + limit,
                                     kBufferMaxLimit * i + limit, "x"));
      session->FlushAsync(nullptr);
    }
    EXPECT_OK(session->Flush());

    monitor_run_ctl.CountDown();
    monitor.join();
    EXPECT_GE(limit, monitor_max_batchers_count);
  }
}

enum class RowSize {
  CONSTANT,
  RANDOM,
};

class FlushModeOpRatesTest : public ClientTest,
                             public ::testing::WithParamInterface<RowSize> {
};

// A test scenario to compare rate of submission of small write operations
// in AUTO_FLUSH and AUTO_FLUSH_BACKGROUND mode; all the operations have
// the same pre-defined size.
TEST_P(FlushModeOpRatesTest, RunComparison) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  const size_t kBufferSizeBytes = 1024;
  const size_t kRowNum = 256;

  vector<size_t> str_sizes(kRowNum);
  const RowSize mode = GetParam();
  switch (mode) {
    case RowSize::CONSTANT:
      std::fill(str_sizes.begin(), str_sizes.end(), kBufferSizeBytes / 128);
      break;

    case RowSize::RANDOM:
      SeedRandom();
      std::generate(str_sizes.begin(), str_sizes.end(),
                    [] { return rand() % (kBufferSizeBytes / 128); });
      break;
  }

  // Run the scenario multiple times to factor out fluctuations of multi-tasking
  // run-time environment and avoid test flakiness.
  uint64_t t_afb_wall = 0;
  uint64_t t_afs_wall = 0;
  const size_t iter_num = 16;
  for (size_t i = 0; i < iter_num; ++i) {
    CpuTimes t_afb;
    TimeInsertOpBatch(KuduSession::AUTO_FLUSH_BACKGROUND,
                      kBufferSizeBytes, 2 * i, 2 * iter_num, str_sizes,
                      &t_afb);
    t_afb_wall += t_afb.wall;

    CpuTimes t_afs;
    TimeInsertOpBatch(KuduSession::AUTO_FLUSH_SYNC,
                      kBufferSizeBytes, 2 * i + 1, 2 * iter_num, str_sizes,
                      &t_afs);
    t_afs_wall += t_afs.wall;
  }
  // AUTO_FLUSH_BACKGROUND should be faster than AUTO_FLUSH_SYNC.
  EXPECT_GT(t_afs_wall, t_afb_wall);
}

INSTANTIATE_TEST_SUITE_P(, FlushModeOpRatesTest,
                         ::testing::Values(RowSize::CONSTANT, RowSize::RANDOM));

// A test to verify that it's safe to perform synchronous and/or asynchronous
// flush while having the auto-flusher thread running in the background.
TEST_F(ClientTest, TestAutoFlushBackgroundAndExplicitFlush) {
  const size_t kIterNum = AllowSlowTests() ? 8192 : 1024;
  shared_ptr<KuduSession> session(client_->NewSession());
  // The background flush interval is short to have more contention.
  ASSERT_OK(session->SetMutationBufferFlushInterval(3));
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  for (size_t i = 0; i < kIterNum; i += 2) {
    ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, i, i, "x"));
    SleepFor(MonoDelta::FromMilliseconds(1));
    session->FlushAsync(nullptr);
    ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, i + 1, i + 1, "y"));
    SleepFor(MonoDelta::FromMilliseconds(1));
    ASSERT_OK(session->Flush());
  }
  EXPECT_EQ(0, session->CountPendingErrors());
  EXPECT_FALSE(session->HasPendingOperations());
  // Check that all rows have reached the table.
  EXPECT_EQ(kIterNum, CountRowsFromClient(client_table_.get()));
}

// A test to verify that in case of AUTO_FLUSH_BACKGROUND information on
// _all_ the errors is delivered after Flush() finishes. Basically, it's a test
// to cover KUDU-1743 -- there was a race where Flush() returned before
// the corresponding errors were added to the error collector.
TEST_F(ClientTest, TestAutoFlushBackgroundAndErrorCollector) {
  using kudu::client::internal::ErrorCollector;
  // The main idea behind this custom error collector is to delay
  // adding the very first error: this is to expose the race which is
  // the root cause of the KUDU-1743 issue.
  class CustomErrorCollector : public ErrorCollector {
   public:
    CustomErrorCollector():
      ErrorCollector(),
      error_cnt_(0) {
    }

    void AddError(unique_ptr<KuduError> error) override {
      if (0 == error_cnt_++) {
        const bool prev_allowed = ThreadRestrictions::SetWaitAllowed(true);
        SleepFor(MonoDelta::FromSeconds(1));
        ThreadRestrictions::SetWaitAllowed(prev_allowed);
      }
      ErrorCollector::AddError(std::move(error));
    }

   private:
    std::atomic<uint64_t> error_cnt_;
  };

  const size_t kIterNum = AllowSlowTests() ? 32 : 2;
  for (size_t i = 0; i < kIterNum; ++i) {
    static const size_t kRowNum = 2;

    vector<unique_ptr<KuduPartialRow>> splits;
    unique_ptr<KuduPartialRow> split(schema_.NewRow());
    ASSERT_OK(split->SetInt32("key", kRowNum / 2));
    splits.push_back(std::move(split));

    // Create the test table: it's important the table is split into multiple
    // (at least two) tablets and replicated: that helps to get
    // RPC completion callbacks from different reactor threads, which is
    // the crux of the race condition for KUDU-1743.
    const string table_name = Substitute("table.$0", i);
    shared_ptr<KuduTable> table;
    ASSERT_OK(CreateTable(table_name, 3, std::move(splits), {}, &table));

    shared_ptr<KuduSession> session(client_->NewSession());
    scoped_refptr<ErrorCollector> ec(new CustomErrorCollector);
    ec.swap(session->data_->error_collector_);
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
    NO_FATALS(InsertTestRows(table.get(), session.get(), kRowNum));
    NO_FATALS(InsertTestRows(table.get(), session.get(), kRowNum));
    vector<KuduError*> errors;
    ElementDeleter deleter(&errors);
    ASSERT_FALSE(session->Flush().ok());
    bool overflowed;
    session->GetPendingErrors(&errors, &overflowed);
    ASSERT_FALSE(overflowed);
    // Print out the errors if the expected count differs from the actual one.
    if (kRowNum != errors.size()) {
      vector<string> errors_str;
      for (const auto e : errors) {
        errors_str.push_back(Substitute("status: $0; operation: $1",
            e->status().ToString(), e->failed_op().ToString()));
      }
      EXPECT_EQ(kRowNum, errors.size()) << errors_str;
    }
  }
}

// A test which verifies that a session in AUTO_FLUSH_BACKGROUND mode can
// be safely abandoned: its pending data should not be flushed.
// This test also checks that the reference to a session stored by the
// background flusher task is not leaked: the leak might appear due to
// circular reference between the session and the messenger's reactor
// which is used to execute the background flush task.
TEST_F(ClientTest, TestAutoFlushBackgroundDropSession) {
  const int32_t kFlushIntervalMs = 1000;
  shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetMutationBufferFlushInterval(kFlushIntervalMs));
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "x"));
  session.reset();

  // Wait for the background flusher task to awake and try to do its job.
  // The 3x extra is to deal with occasional delays to avoid flakiness.
  SleepFor(MonoDelta::FromMilliseconds(3L * kFlushIntervalMs));

  // The session should be gone, and the background flusher thread
  // should notice that and do not perform flush, so no data is supposed
  // to appear in the table.
  vector<string> rows;
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_TRUE(rows.empty());
}

// A test scenario for AUTO_FLUSH_BACKGROUND mode:
// applying a bunch of small rows without a flush should not result in
// an error, even with low limit on the buffer space. This is because
// Session::Apply() blocks and waits for buffer space to become available
// if it cannot add the operation into the buffer right away.
TEST_F(ClientTest, TestAutoFlushBackgroundSmallOps) {
  const size_t kBufferSizeBytes = 1024;
  const size_t kRowNum = kBufferSizeBytes * 10;
  const int32_t kFlushIntervalMs = 100;
  shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetMutationBufferSpace(kBufferSizeBytes));
  ASSERT_OK(session->SetMutationBufferFlushInterval(kFlushIntervalMs));
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  int64_t monitor_max_buffer_size = 0;
  CountDownLatch monitor_run_ctl(1);
  thread monitor([&]() {
    MonitorSessionBufferSize(session.get(),
                             &monitor_run_ctl, &monitor_max_buffer_size);
  });

  for (size_t i = 0; i < kRowNum; ++i) {
    ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, i, i, "x"));
  }
  EXPECT_OK(session->Flush());
  EXPECT_EQ(0, session->CountPendingErrors());
  EXPECT_FALSE(session->HasPendingOperations());

  monitor_run_ctl.CountDown();
  monitor.join();
  // Check that the limit has not been overrun.
  EXPECT_GE(kBufferSizeBytes, monitor_max_buffer_size);
  // Check that all rows have reached the table.
  EXPECT_EQ(kRowNum, CountRowsFromClient(client_table_.get()));
}

// A test scenario for AUTO_FLUSH_BACKGROUND mode:
// applying a bunch of rows every one of which is so big in size that
// a couple of those do not fit into the buffer. This should be OK:
// Session::Apply() must manage this case as well, blocking on an attempt to add
// another operation if buffer is about to overflow (no error, though).
// Once last operation is flushed out of the buffer, Session::Apply() should
// unblock and work with next operation, and so on.
TEST_F(ClientTest, TestAutoFlushBackgroundBigOps) {
  const size_t kBufferSizeBytes = 32 * 1024;
  const int32_t kFlushIntervalMs = 100;
  shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetMutationBufferSpace(kBufferSizeBytes));
  ASSERT_OK(session->SetMutationBufferFlushInterval(kFlushIntervalMs));
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  int64_t monitor_max_buffer_size = 0;
  CountDownLatch monitor_run_ctl(1);
  thread monitor([&]() {
    MonitorSessionBufferSize(session.get(),
                             &monitor_run_ctl, &monitor_max_buffer_size);
  });

  // Starting from i == 3: this is the lowest i when
  // ((i - 1) * kBufferSizeBytes / i) has a value greater than
  // kBufferSizeBytes / 2. We want a pair of operations that both don't fit
  // into the buffer, but every individual one does.
  const size_t kRowIdxBeg = 3;
  const size_t kRowIdxEnd = 256;
  for (size_t i = kRowIdxBeg; i < kRowIdxEnd; ++i) {
    const string long_string((i - 1) * kBufferSizeBytes / i, 'x');
    ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, i, i,
                                   long_string.c_str()));
  }
  EXPECT_OK(session->Flush());
  EXPECT_EQ(0, session->CountPendingErrors());
  EXPECT_FALSE(session->HasPendingOperations());

  monitor_run_ctl.CountDown();
  monitor.join();
  EXPECT_GE(kBufferSizeBytes, monitor_max_buffer_size);
  // Check that all rows have reached the table.
  EXPECT_EQ(kRowIdxEnd - kRowIdxBeg, CountRowsFromClient(client_table_.get()));
}

// A test scenario for AUTO_FLUSH_BACKGROUND mode:
// interleave write operations of random lenth.
// Every single operation fits into the buffer; no error is expected.
TEST_F(ClientTest, TestAutoFlushBackgroundRandomOps) {
  const size_t kBufferSizeBytes = 1024;
  const size_t kRowNum = 512;
  const int32_t kFlushIntervalMs = 1000;

  shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetMutationBufferSpace(kBufferSizeBytes));
  ASSERT_OK(session->SetMutationBufferFlushInterval(kFlushIntervalMs));
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  SeedRandom();
  int64_t monitor_max_buffer_size = 0;
  CountDownLatch monitor_run_ctl(1);
  thread monitor([&]() {
    MonitorSessionBufferSize(session.get(),
                             &monitor_run_ctl, &monitor_max_buffer_size);
  });

  for (size_t i = 0; i < kRowNum; ++i) {
    // Every operation takes less than 2/3 of the buffer space, so no
    // error on 'operation size is bigger than the limit' is expected.
    const string long_string(rand() % (2 * kBufferSizeBytes / 3), 'x');
    ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, i, i,
                                   long_string.c_str()));
  }
  EXPECT_OK(session->Flush());
  EXPECT_EQ(0, session->CountPendingErrors());
  EXPECT_FALSE(session->HasPendingOperations());

  monitor_run_ctl.CountDown();
  monitor.join();
  EXPECT_GE(kBufferSizeBytes, monitor_max_buffer_size);
  // Check that all rows have reached the table.
  EXPECT_EQ(kRowNum, CountRowsFromClient(client_table_.get()));
}

// Test that in AUTO_FLUSH_BACKGROUND mode even a small amount of tiny
// operations are put into the queue and flushed after some time.
// Since the buffer size limit is relatively huge compared with the total size
// of operations getting into the buffer, the high-watermark flush criteria
// is not going to be met and the timer expiration criteria starts playing here.
TEST_F(ClientTest, TestAutoFlushBackgroundFlushTimeout) {
  const int32_t kFlushIntervalMs = 200;
  shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetMutationBufferSpace(1 * 1024 * 1024));
  ASSERT_OK(session->SetMutationBufferFlushInterval(kFlushIntervalMs));
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 0, 0, "x"));
  ASSERT_TRUE(session->HasPendingOperations());
  WaitForAutoFlushBackground(session, kFlushIntervalMs);
  EXPECT_EQ(0, session->CountPendingErrors());
  EXPECT_FALSE(session->HasPendingOperations());
  // Check that all rows have reached the table.
  EXPECT_EQ(1, CountRowsFromClient(client_table_.get()));
}

// Test that in AUTO_FLUSH_BACKGROUND mode applying two big operations in a row
// works without unnecessary delay in the case illustrated by the diagram below.
//
//                                          +-------------------+
//                                          |                   |
//                                          | Data of the next  |
//                   +---buffer_limit----+  | operation to add. |
// flush watermark ->|                   |  |                   |
//                   +-------------------+  +---------0---------+
//                   |                   |
//                   | Data of the first |
//                   | operation.        |
//                   |                   |
//                   +---------0---------+
TEST_F(ClientTest, TestAutoFlushBackgroundPreFlush) {
  const size_t kBufferSizeBytes = 1024;
  const size_t kStringLenBytes = 2 * kBufferSizeBytes / 3;
  const int32 kFlushIntervalMs = 5000;

  shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetMutationBufferSpace(kBufferSizeBytes));
  ASSERT_OK(session->SetMutationBufferFlushWatermark(0.99)); // 99%
  ASSERT_OK(session->SetMutationBufferFlushInterval(kFlushIntervalMs));
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  for (size_t i = 0; i < 2; ++i) {
    unique_ptr<KuduInsert> insert(client_table_->NewInsert());
    const string str(kStringLenBytes, '0' + i);
    ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
    ASSERT_OK(insert->mutable_row()->SetInt32("int_val", i));
    ASSERT_OK(insert->mutable_row()->SetStringCopy("string_val", str));

    Stopwatch sw(Stopwatch::ALL_THREADS);
    sw.start();
    ASSERT_OK(session->Apply(insert.release()));
    sw.stop();

    ASSERT_TRUE(session->HasPendingOperations());

    // The first Apply() call must not block because the buffer was empty
    // prior to adding the first operation's data.
    //
    // The second Apply() should go through fast because a pre-flush
    // should happen before adding the data of the second operation even if
    // the flush watermark hasn't been reached with the first operation's data.
    // For details on this behavior please see the diagram in the body of the
    // KuduSession::Data::ApplyWriteOp() method.
    EXPECT_GT(kFlushIntervalMs / 10,
              static_cast<int32_t>(sw.elapsed().wall_millis()));
  }
  ASSERT_OK(session->Flush());

  // At this point nothing should be in the buffer.
  ASSERT_EQ(0, session->CountPendingErrors());
  ASSERT_FALSE(session->HasPendingOperations());
  // Check that all rows have reached the table.
  EXPECT_EQ(2, CountRowsFromClient(client_table_.get()));
}

// Test that KuduSession::Apply() call blocks in AUTO_FLUSH_BACKGROUND mode
// if the write operation/mutation buffer does not have enough space
// to accommodate an incoming write operation.
//
// The test scenario uses the combination of watermark level
// settings and the 'flush by timeout' behavior. The idea is the following:
//
//   a. Set the high-watermark level to 100% of the buffer size limit.
//      This setting and the fact that Apply() does not allow operations' data
//      to accumulate over the size of the buffer
//      guarantees that the high-watermark event will not trigger.
//   b. Set the timeout for the flush to be high enough to distinguish
//      between occasional delays due to other OS activity and waiting
//      on the invocation of the blocking Apply() method.
//   c. Try to add two write operations each of 2/3 of the buffer space in size.
//      The first Apply() call should succeed instantly, but the second one
//      should block.
//   d. Measure how much time it took to return from the second invocation
//      of the Apply() call: it should be close to the wait timeout.
//
TEST_F(ClientTest, TestAutoFlushBackgroundApplyBlocks) {
  const size_t kBufferSizeBytes = 8 * 1024;
  const size_t kStringLenBytes = 2 * kBufferSizeBytes / 3;
  const int32 kFlushIntervalMs = 1000;

  shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetMutationBufferSpace(kBufferSizeBytes));
  ASSERT_OK(session->SetMutationBufferFlushWatermark(1.0));
  session->data_->buffer_pre_flush_enabled_ = false;
  ASSERT_OK(session->SetMutationBufferFlushInterval(kFlushIntervalMs));
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  const int32_t wait_timeout_ms = kFlushIntervalMs;
  {
    unique_ptr<KuduInsert> insert(client_table_->NewInsert());
    const string str(kStringLenBytes, '0');
    ASSERT_OK(insert->mutable_row()->SetInt32("key", 0));
    ASSERT_OK(insert->mutable_row()->SetInt32("int_val", 0));
    ASSERT_OK(insert->mutable_row()->SetStringCopy("string_val", str));

    Stopwatch sw(Stopwatch::ALL_THREADS);
    sw.start();
    ASSERT_OK(session->Apply(insert.release()));
    sw.stop();

    ASSERT_TRUE(session->HasPendingOperations());

    // The first Apply() call must not block, so the time spent on calling it
    // should be at least one order of magnitude less than the periodic flush
    // interval.
    EXPECT_GT(wait_timeout_ms / 10, sw.elapsed().wall_millis());
  }
  Stopwatch sw(Stopwatch::ALL_THREADS);
  {
    unique_ptr<KuduInsert> insert(client_table_->NewInsert());
    const string str(kStringLenBytes, '1');
    ASSERT_OK(insert->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(insert->mutable_row()->SetInt32("int_val", 1));
    ASSERT_OK(insert->mutable_row()->SetStringCopy("string_val", str));

    // The second Apply() call must block until the flusher pushes already
    // scheduled operation out of the buffer. The second Apply() call should
    // unblock as soon as flusher triggers purging buffered operations
    // by timeout.
    sw.start();
    ASSERT_OK(session->Apply(insert.release()));
    sw.stop();
  }
  ASSERT_OK(session->Flush());

  // At this point nothing should be in the buffer.
  ASSERT_EQ(0, session->CountPendingErrors());
  ASSERT_FALSE(session->HasPendingOperations());
  // Check that all rows have reached the table.
  EXPECT_EQ(2, CountRowsFromClient(client_table_.get()));

  // Check that t_diff_ms is close enough to wait_time_ms.
  EXPECT_GT(wait_timeout_ms + wait_timeout_ms / 2, sw.elapsed().wall_millis());
  EXPECT_LT(wait_timeout_ms / 2, sw.elapsed().wall_millis());
}

// Test that update updates and delete deletes with expected use
TEST_F(ClientTest, TestMutationsWork) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "original row"));
  FlushSessionOrDie(session);

  ASSERT_OK(ApplyUpdateToSession(session.get(), client_table_, 1, 2));
  FlushSessionOrDie(session);
  vector<string> rows;
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ(R"((int32 key=1, int32 int_val=2, string string_val="original row", )"
            "int32 non_null_with_default=12345)", rows[0]);
  rows.clear();

  ASSERT_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  FlushSessionOrDie(session);
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_TRUE(rows.empty());
}

TEST_F(ClientTest, TestMutateDeletedRow) {
  vector<string> rows;
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "original row"));
  FlushSessionOrDie(session);
  ASSERT_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  FlushSessionOrDie(session);
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_TRUE(rows.empty());

  // Attempt update deleted row
  ASSERT_OK(ApplyUpdateToSession(session.get(), client_table_, 1, 2));
  Status s = session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
  // Verify error
  unique_ptr<KuduError> error = GetSingleErrorFromSession(session.get());
  ASSERT_EQ(error->failed_op().ToString(),
            "UPDATE int32 key=1, int32 int_val=2");
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_TRUE(rows.empty());

  // Attempt delete deleted row
  ASSERT_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  s = session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
  // Verify error
  error = GetSingleErrorFromSession(session.get());
  ASSERT_EQ(error->failed_op().ToString(),
            "DELETE int32 key=1");
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_TRUE(rows.empty());
}

TEST_F(ClientTest, TestMutateNonexistentRow) {
  vector<string> rows;
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Attempt update nonexistent row
  ASSERT_OK(ApplyUpdateToSession(session.get(), client_table_, 1, 2));
  Status s = session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
  // Verify error
  unique_ptr<KuduError> error = GetSingleErrorFromSession(session.get());
  ASSERT_EQ(error->failed_op().ToString(),
            "UPDATE int32 key=1, int32 int_val=2");
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_TRUE(rows.empty());

  // Attempt delete nonexistent row
  ASSERT_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  s = session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
  // Verify error
  error = GetSingleErrorFromSession(session.get());
  ASSERT_EQ(error->failed_op().ToString(),
            "DELETE int32 key=1");
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_TRUE(rows.empty());
}

TEST_F(ClientTest, TestUpsert) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Perform and verify UPSERT which acts as an INSERT.
  ASSERT_OK(ApplyUpsertToSession(session.get(), client_table_, 1, 1, "original row"));
  FlushSessionOrDie(session);
  NO_FATALS(DoVerifyMetrics(session.get(), 0, 0, 1, 0, 0, 0, 0, 0));

  {
    vector<string> rows;
    ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ(R"((int32 key=1, int32 int_val=1, string string_val="original row", )"
              "int32 non_null_with_default=12345)", rows[0]);
  }

  // Perform and verify UPSERT which acts as an UPDATE.
  ASSERT_OK(ApplyUpsertToSession(session.get(), client_table_, 1, 2, "upserted row"));
  FlushSessionOrDie(session);
  NO_FATALS(DoVerifyMetrics(session.get(), 0, 0, 2, 0, 0, 0, 0, 0));

  {
    vector<string> rows;
    ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ(R"((int32 key=1, int32 int_val=2, string string_val="upserted row", )"
              "int32 non_null_with_default=12345)", rows[0]);
  }

  // Apply an UPDATE including the column that has a default and verify it.
  {
    unique_ptr<KuduUpdate> update(client_table_->NewUpdate());
    KuduPartialRow* row = update->mutable_row();
    ASSERT_OK(row->SetInt32("key", 1));
    ASSERT_OK(row->SetStringCopy("string_val", "updated row"));
    ASSERT_OK(row->SetInt32("non_null_with_default", 999));
    ASSERT_OK(session->Apply(update.release()));
    FlushSessionOrDie(session);
    NO_FATALS(DoVerifyMetrics(session.get(), 0, 0, 2, 0, 1, 0, 0, 0));
  }
  {
    vector<string> rows;
    ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ(R"((int32 key=1, int32 int_val=2, string string_val="updated row", )"
              "int32 non_null_with_default=999)", rows[0]);
  }

  // Perform another UPSERT. This upsert doesn't specify the 'non_null_with_default'
  // column, and therefore should not revert it back to its default.
  ASSERT_OK(ApplyUpsertToSession(session.get(), client_table_, 1, 3, "upserted row 2"));
  FlushSessionOrDie(session);
  NO_FATALS(DoVerifyMetrics(session.get(), 0, 0, 3, 0, 1, 0, 0, 0));
  {
    vector<string> rows;
    ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ(R"((int32 key=1, int32 int_val=3, string string_val="upserted row 2", )"
              "int32 non_null_with_default=999)", rows[0]);
  }

  // Delete the row.
  ASSERT_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  FlushSessionOrDie(session);
  NO_FATALS(DoVerifyMetrics(session.get(), 0, 0, 3, 0, 1, 0, 1, 0));
  {
    vector<string> rows;
    ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
    EXPECT_TRUE(rows.empty());
  }
}

TEST_F(ClientTest, TestWriteWithBadColumn) {
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  // Try to do a write with the bad schema.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  unique_ptr<KuduInsert> insert(table->NewInsert());
  ASSERT_OK(insert->mutable_row()->SetInt32("key", 12345));
  Status s = insert->mutable_row()->SetInt32("bad_col", 12345);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "No such column: bad_col");
}

// Do a write with a bad schema on the client side. This should make the Prepare
// phase of the write fail, which will result in an error on the RPC response.
TEST_F(ClientTest, TestWriteWithBadSchema) {
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  // Remove the 'int_val' column.
  // Now the schema on the client is "old"
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  ASSERT_OK(table_alterer
            ->DropColumn("int_val")
            ->Alter());

  // Try to do a write with the bad schema.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_,
                                        12345, 12345, "x"));
  Status s = session->Flush();
  ASSERT_FALSE(s.ok());

  // Verify the specific error.
  unique_ptr<KuduError> error = GetSingleErrorFromSession(session.get());
  ASSERT_TRUE(error->status().IsInvalidArgument());
  ASSERT_STR_CONTAINS(error->status().ToString(),
            "Client provided column int_val INT32 NOT NULL "
            "not present in tablet");
  ASSERT_EQ(error->failed_op().ToString(),
            R"(INSERT int32 key=12345, int32 int_val=12345, string string_val="x")");
}

TEST_F(ClientTest, TestBasicAlterOperations) {
  const vector<string> kBadNames = {"", string(1000, 'x')};

  // Test that renaming a column to an invalid name throws an error.
  for (const string& bad_name : kBadNames) {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("int_val")->RenameTo(bad_name);
    Status s = table_alterer->Alter();
    EXPECT_TRUE(s.IsInvalidArgument());
    EXPECT_THAT(s.ToString(), testing::AnyOf(
        testing::HasSubstr("invalid column name"),
        testing::HasSubstr("column name must be non-empty")));
  }

  // Test that renaming a table to an invalid name throws an error.
  for (const string& bad_name : kBadNames) {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->RenameTo(bad_name);
    Status s = table_alterer->Alter();
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "invalid table name");
  }

  // Test trying to add columns to a table such that it exceeds the permitted
  // maximum.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    for (int i = 0; i < 1000; i++) {
      table_alterer->AddColumn(Substitute("c$0", i))->Type(KuduColumnSchema::INT32);
    }
    Status s = table_alterer->Alter();
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "number of columns 1004 is greater than the "
                        "permitted maximum 300");
  }

  // Having no steps should throw an error.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    Status s = table_alterer->Alter();
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "No alter steps provided");
  }

  // Adding a non-nullable column with no default value should throw an error.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull();
    Status s = table_alterer->Alter();
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "column `key`: NOT NULL columns must have a default");
  }

  // Removing a key should throw an error.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    Status s = table_alterer
      ->DropColumn("key")
      ->Alter();
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "cannot remove a key column: key");
  }

  // Renaming a column to an already-existing name should throw an error.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("int_val")->RenameTo("string_val");
    Status s = table_alterer->Alter();
    ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "The column already exists: string_val");
  }

  // Altering a column but specifying no alterations should throw an error.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("string_val");
    Status s = table_alterer->Alter();
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "no alter operation specified: string_val");
  }

  // Trying to change the type of a column is an error.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("string_val")->Type(KuduColumnSchema::STRING);
    Status s = table_alterer->Alter();
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "unsupported alter operation: string_val");
  }

  // Trying to alter the nullability of a column is an error.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("string_val")->Nullable();
    Status s = table_alterer->Alter();
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "unsupported alter operation: string_val");
  }

  // Trying to alter the nullability of a column is an error.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("string_val")->NotNull();
    Status s = table_alterer->Alter();
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "unsupported alter operation: string_val");
  }

  // Need a TabletReplica for the next set of tests.
  string tablet_id = GetFirstTabletId(client_table_.get());
  scoped_refptr<TabletReplica> tablet_replica;
  ASSERT_TRUE(cluster_->mini_tablet_server(0)->server()->tablet_manager()->LookupTablet(
      tablet_id, &tablet_replica));

  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->DropColumn("int_val")
      ->AddColumn("new_col")->Type(KuduColumnSchema::INT32);
    ASSERT_OK(table_alterer->Alter());
    ASSERT_EQ(1, tablet_replica->tablet()->metadata()->schema_version());
  }

  // Specifying an encoding incompatible with the column's type is an error.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AddColumn("new_string_val")->Type(KuduColumnSchema::STRING)
      ->Encoding(KuduColumnStorageAttributes::GROUP_VARINT);
    Status s = table_alterer->Alter();
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "encoding GROUP_VARINT not supported for type BINARY");
    ASSERT_EQ(1, tablet_replica->tablet()->metadata()->schema_version());
  }

  // Test adding a new column of type string.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AddColumn("new_string_val")->Type(KuduColumnSchema::STRING)
      ->Encoding(KuduColumnStorageAttributes::PREFIX_ENCODING);
    ASSERT_OK(table_alterer->Alter());
    ASSERT_EQ(2, tablet_replica->tablet()->metadata()->schema_version());
  }

  // Test renaming a primary key column.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("key")->RenameTo("key2");
    Status s = table_alterer->Alter();
    ASSERT_FALSE(s.IsInvalidArgument());
    ASSERT_EQ(3, tablet_replica->tablet()->metadata()->schema_version());
  }

  // Changing the type of a primary key column is an error.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("key2")->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();
    Status s = table_alterer->Alter();
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "Not implemented: unsupported alter operation: key2");
  }

  // Test changing a default value for a variable-length (string) column.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("string_val")
        ->Default(KuduValue::CopyString("hello!"));
    ASSERT_OK(table_alterer->Alter());
    ASSERT_EQ(4, tablet_replica->tablet()->metadata()->schema_version());
    Schema schema = *tablet_replica->tablet()->metadata()->schema();
    ColumnSchema col_schema = schema.column(schema.find_column("string_val"));
    ASSERT_FALSE(col_schema.has_read_default());
    ASSERT_TRUE(col_schema.has_write_default());
    ASSERT_EQ("hello!", *reinterpret_cast<const Slice*>(col_schema.write_default_value()));
  }

  // Test changing a default value for an integer column.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("non_null_with_default")
        ->Default(KuduValue::FromInt(54321));
    ASSERT_OK(table_alterer->Alter());
    ASSERT_EQ(5, tablet_replica->tablet()->metadata()->schema_version());
    Schema schema = *tablet_replica->tablet()->metadata()->schema();
    ColumnSchema col_schema = schema.column(schema.find_column("non_null_with_default"));
    ASSERT_TRUE(col_schema.has_read_default()); // Started with a default
    ASSERT_TRUE(col_schema.has_write_default());
    ASSERT_EQ(54321, *reinterpret_cast<const int32_t*>(col_schema.write_default_value()));
  }

  // Test clearing a default value from a nullable column.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("string_val")
        ->RemoveDefault();
    ASSERT_OK(table_alterer->Alter());
    ASSERT_EQ(6, tablet_replica->tablet()->metadata()->schema_version());
    Schema schema = *tablet_replica->tablet()->metadata()->schema();
    ColumnSchema col_schema = schema.column(schema.find_column("string_val"));
    ASSERT_FALSE(col_schema.has_read_default());
    ASSERT_FALSE(col_schema.has_write_default());
  }

  // Test clearing a default value from a non-nullable column.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("non_null_with_default")
        ->RemoveDefault();
    ASSERT_OK(table_alterer->Alter());
    ASSERT_EQ(7, tablet_replica->tablet()->metadata()->schema_version());
    Schema schema = *tablet_replica->tablet()->metadata()->schema();
    ColumnSchema col_schema = schema.column(schema.find_column("non_null_with_default"));
    ASSERT_TRUE(col_schema.has_read_default());
    ASSERT_FALSE(col_schema.has_write_default());
  }

  // Test that specifying a default of the wrong size fails.
  // Since the column's type isn't checked client-side and the client
  // stores defaults for all integer-backed types as 64-bit integers, the client
  // client can request defaults of the wrong type or size for a column's type.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("non_null_with_default")
        ->Default(KuduValue::CopyString("aaa"));
    Status s = table_alterer->Alter();
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "wrong size for default value");
    ASSERT_EQ(7, tablet_replica->tablet()->metadata()->schema_version());
  }

  // Test altering encoding, compression, and block size.
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("string_val")
        ->Encoding(KuduColumnStorageAttributes::PLAIN_ENCODING)
        ->Compression(KuduColumnStorageAttributes::LZ4)
        ->BlockSize(16 * 1024 * 1024);
    ASSERT_OK(table_alterer->Alter());
    ASSERT_EQ(8, tablet_replica->tablet()->metadata()->schema_version());
    Schema schema = *tablet_replica->tablet()->metadata()->schema();
    ColumnSchema col_schema = schema.column(schema.find_column("string_val"));
    ASSERT_EQ(KuduColumnStorageAttributes::PLAIN_ENCODING, col_schema.attributes().encoding);
    ASSERT_EQ(KuduColumnStorageAttributes::LZ4, col_schema.attributes().compression);
    ASSERT_EQ(16 * 1024 * 1024, col_schema.attributes().cfile_block_size);
  }

  // Test altering extra configuration properties.
  // 1. Alter history max age second to 3600.
  {
    map<string, string> extra_configs;
    extra_configs["kudu.table.history_max_age_sec"] = "3600";
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterExtraConfig(extra_configs);
    ASSERT_OK(table_alterer->Alter());
    ASSERT_EQ(9, tablet_replica->tablet()->metadata()->schema_version());
    ASSERT_NE(nullopt, tablet_replica->tablet()->metadata()->extra_config());
    ASSERT_TRUE(tablet_replica->tablet()->metadata()->extra_config()->has_history_max_age_sec());
    ASSERT_EQ(3600, tablet_replica->tablet()->metadata()->extra_config()->history_max_age_sec());
  }
  // 2. Alter history max age second to 7200.
  {
    map<string, string> extra_configs;
    extra_configs["kudu.table.history_max_age_sec"] = "7200";
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterExtraConfig(extra_configs);
    ASSERT_OK(table_alterer->Alter());
    ASSERT_EQ(10, tablet_replica->tablet()->metadata()->schema_version());
    ASSERT_NE(nullopt, tablet_replica->tablet()->metadata()->extra_config());
    ASSERT_TRUE(tablet_replica->tablet()->metadata()->extra_config()->has_history_max_age_sec());
    ASSERT_EQ(7200, tablet_replica->tablet()->metadata()->extra_config()->history_max_age_sec());
  }
  // 3. Set an unexpected extra config.
  {
    map<string, string> extra_configs;
    extra_configs["kudu.table.history_max_age_sec"] = "3600";
    extra_configs["kudu.table.maintenance_priority_BAD_NAME"] = "2";
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterExtraConfig(extra_configs);
    auto s = table_alterer->Alter();
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "invalid extra configuration property: "
                        "kudu.table.maintenance_priority_BAD_NAME");
    // Schema version and old properties are not changed.
    ASSERT_EQ(10, tablet_replica->tablet()->metadata()->schema_version());
    ASSERT_NE(nullopt, tablet_replica->tablet()->metadata()->extra_config());
    ASSERT_TRUE(tablet_replica->tablet()->metadata()->extra_config()->has_history_max_age_sec());
    ASSERT_EQ(7200, tablet_replica->tablet()->metadata()->extra_config()->history_max_age_sec());
    ASSERT_FALSE(tablet_replica->tablet()->metadata()->extra_config()->has_maintenance_priority());
  }
  // 4. Reset history max age second to default.
  {
    map<string, string> extra_configs;
    extra_configs["kudu.table.history_max_age_sec"] = "";
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterExtraConfig(extra_configs);
    ASSERT_OK(table_alterer->Alter());
    ASSERT_EQ(11, tablet_replica->tablet()->metadata()->schema_version());
    ASSERT_NE(nullopt, tablet_replica->tablet()->metadata()->extra_config());
    ASSERT_FALSE(tablet_replica->tablet()->metadata()->extra_config()->has_history_max_age_sec());
  }

  // Test changing a table name.
  {
    const char *kRenamedTableName = "RenamedTable";
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    ASSERT_OK(table_alterer
              ->RenameTo(kRenamedTableName)
              ->Alter());
    ASSERT_EQ(12, tablet_replica->tablet()->metadata()->schema_version());
    ASSERT_EQ(kRenamedTableName, tablet_replica->tablet()->metadata()->table_name());

    CatalogManager *catalog_manager = cluster_->mini_master()->master()->catalog_manager();
    CatalogManager::ScopedLeaderSharedLock l(catalog_manager);
    ASSERT_OK(l.first_failed_status());
    bool exists;
    ASSERT_OK(catalog_manager->TableNameExists(kRenamedTableName, &exists));
    ASSERT_TRUE(exists);
    ASSERT_OK(catalog_manager->TableNameExists(kTableName, &exists));
    ASSERT_FALSE(exists);
  }
}

TEST_F(ClientTest, TestDeleteTable) {
  // Open the table before deleting it.
  ASSERT_OK(client_->OpenTable(kTableName, &client_table_));

  // Insert a few rows, and scan them back. This is to populate the MetaCache.
  NO_FATALS(InsertTestRows(client_.get(), client_table_.get(), 10));
  vector<string> rows;
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_EQ(10, rows.size());

  // Remove the table
  // NOTE that it returns when the operation is completed on the master side
  string tablet_id = GetFirstTabletId(client_table_.get());
  ASSERT_OK(client_->DeleteTable(kTableName));
  CatalogManager *catalog_manager = cluster_->mini_master()->master()->catalog_manager();
  {
    CatalogManager::ScopedLeaderSharedLock l(catalog_manager);
    ASSERT_OK(l.first_failed_status());
    bool exists;
    ASSERT_OK(catalog_manager->TableNameExists(kTableName, &exists));
    ASSERT_FALSE(exists);
  }

  // Wait until the table is removed from the TS
  int wait_time = 1000;
  bool tablet_found = true;
  for (int i = 0; i < 80 && tablet_found; ++i) {
    scoped_refptr<TabletReplica> tablet_replica;
    tablet_found = cluster_->mini_tablet_server(0)->server()->tablet_manager()->LookupTablet(
                      tablet_id, &tablet_replica);
    SleepFor(MonoDelta::FromMicroseconds(wait_time));
    wait_time = std::min(wait_time * 5 / 4, 1000000);
  }
  ASSERT_FALSE(tablet_found);

  // Try to open the deleted table
  Status s = client_->OpenTable(kTableName, &client_table_);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "the table does not exist");

  // Create a new table with the same name. This is to ensure that the client
  // doesn't cache anything inappropriately by table name (see KUDU-1055).
  ASSERT_OK(CreateTable(kTableName, 1, GenerateSplitRows(), {}, &client_table_));

  // Should be able to insert successfully into the new table.
  NO_FATALS(InsertTestRows(client_.get(), client_table_.get(), 10));
}

TEST_F(ClientTest, TestSoftDeleteAndReserveTable) {
  // Open the table before deleting it.
  ASSERT_OK(client_->OpenTable(kTableName, &client_table_));
  string table_id = client_table_->id();

  // Insert a few rows, and scan them back. This is to populate the MetaCache.
  NO_FATALS(InsertTestRows(client_.get(), client_table_.get(), 10));
  vector<string> rows;
  ScanTableToStrings(client_table_.get(), &rows);
  ASSERT_EQ(10, rows.size());

  // Remove the table.
  // NOTE that it returns when the operation is completed on the master side
  string tablet_id = GetFirstTabletId(client_table_.get());
  ASSERT_OK(client_->SoftDeleteTable(kTableName, 600));
  CatalogManager* catalog_manager = cluster_->mini_master()->master()->catalog_manager();
  {
    CatalogManager::ScopedLeaderSharedLock l(catalog_manager);
    ASSERT_OK(l.first_failed_status());
    bool exists;
    ASSERT_OK(catalog_manager->TableNameExists(kTableName, &exists));
    ASSERT_TRUE(exists);
  }

  // Soft_deleted tablet is still visible.
  scoped_refptr<TabletReplica> tablet_replica;
  ASSERT_TRUE(cluster_->mini_tablet_server(0)->server()->tablet_manager()->LookupTablet(
              tablet_id, &tablet_replica));

  // Old table has been removed to the soft_deleted map.
  vector<string> tables;
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_EQ(0, tables.size());
  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_EQ(1, tables.size());
  ASSERT_EQ(string(kTableName), tables[0]);

  Status s;
  // Altering a soft-deleted table is not allowed.
  {
    // Not allowed to rename.
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    const string new_name = "test-new-table";
    table_alterer->RenameTo(kTableName);
    s = table_alterer->Alter();
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), Substitute("soft_deleted table $0 should not be altered",
                                                 kTableName));
  }

  {
    // Not allowed to add column.
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->DropColumn("int_val")
        ->AddColumn("new_col")->Type(KuduColumnSchema::INT32);
    s = table_alterer->Alter();
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), Substitute("soft_deleted table $0 should not be altered",
                                                 kTableName));
  }

  {
    // Not allowed to delete the soft_deleted table with new reserve_seconds value.
    s = client_->SoftDeleteTable(kTableName, 600);
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(),
        Substitute("soft_deleted table $0 should not be soft deleted again", kTableName));
  }

  {
    // Not allowed to set extra configs.
    map<string, string> extra_configs;
    extra_configs[kTableMaintenancePriority] = "3";
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterExtraConfig(extra_configs);
    s = table_alterer->Alter();
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), Substitute("soft_deleted table $0 should not be altered",
                                                 kTableName));
  }

  {
    // Read and write are allowed for soft_deleted table.
    NO_FATALS(InsertTestRows(client_.get(), client_table_.get(), 20, 10));
    ScanTableToStrings(client_table_.get(), &rows);
    ASSERT_EQ(30, rows.size());
  }

  {
    // Not allowed creating a new table with the same name.
    shared_ptr<KuduTable> new_client_table;
    s = CreateTable(kTableName, 1, GenerateSplitRows(), {}, &new_client_table);
    ASSERT_TRUE(s.IsAlreadyPresent());
    ASSERT_STR_CONTAINS(s.ToString(), Substitute("table $0 already exists with id",
                        kTableName));
  }

  // Try to recall the soft_deleted table.
  ASSERT_OK(client_->RecallTable(table_id));
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_EQ(1, tables.size());
  ASSERT_EQ(kTableName, tables[0]);
  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_TRUE(tables.empty());

  // Force to delete the soft_deleted table.
  ASSERT_OK(client_->SoftDeleteTable(kTableName));

  // No one table left.
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_EQ(0, tables.size());
  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_EQ(0, tables.size());
}

TEST_F(ClientTest, TestSoftDeleteAndRecallTable) {
  // Open the table before deleting it.
  ASSERT_OK(client_->OpenTable(kTableName, &client_table_));

  // Insert a few rows, and scan them back. This is to populate the MetaCache.
  NO_FATALS(InsertTestRows(client_.get(), client_table_.get(), 10));
  vector<string> rows;
  ScanTableToStrings(client_table_.get(), &rows);
  ASSERT_EQ(10, rows.size());

  // Remove the table
  ASSERT_OK(client_->SoftDeleteTable(kTableName, 600));
  vector<string> tables;
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_EQ(0, tables.size());

  // Recall and reopen table.
  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_EQ(1, tables.size());
  ASSERT_OK(client_->RecallTable(client_table_->id()));

  // Check the data in the table.
  ScanTableToStrings(client_table_.get(), &rows);
  ASSERT_EQ(10, rows.size());
}

TEST_F(ClientTest, TestSoftDeleteAndRecallTableWithNewTableName) {
  // Open the table before deleting it.
  ASSERT_OK(client_->OpenTable(kTableName, &client_table_));
  string old_table_id = client_table_->id();

  // Insert a few rows, and scan them back. This is to populate the MetaCache.
  NO_FATALS(InsertTestRows(client_.get(), client_table_.get(), 10));
  vector<string> rows;
  ScanTableToStrings(client_table_.get(), &rows);
  ASSERT_EQ(10, rows.size());

  // Remove the table
  ASSERT_OK(client_->SoftDeleteTable(kTableName, 600));
  vector<string> tables;
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_EQ(0, tables.size());

  // Recall and reopen table.
  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_EQ(1, tables.size());

  const string new_name = "test-new-table";
  ASSERT_OK(client_->RecallTable(client_table_->id(), new_name));
  ASSERT_OK(client_->OpenTable(new_name, &client_table_));
  ASSERT_EQ(old_table_id, client_table_->id());

  // Check the data in the table.
  ScanTableToStrings(client_table_.get(), &rows);
  ASSERT_EQ(10, rows.size());
}

TEST_F(ClientTest, TestSoftDeleteAndRecallAfterReserveTimeTable) {
  // Open the table before deleting it.
  ASSERT_OK(client_->OpenTable(kTableName, &client_table_));

  // Insert a few rows, and scan them back. This is to populate the MetaCache.
  NO_FATALS(InsertTestRows(client_.get(), client_table_.get(), 10));
  vector<string> rows;
  ScanTableToStrings(client_table_.get(), &rows);
  ASSERT_EQ(10, rows.size());

  // Remove the table
  ASSERT_OK(client_->SoftDeleteTable(kTableName));
  // Wait util the table is removed completely.
  ASSERT_EVENTUALLY([&] () {
    Status s = client_->OpenTable(kTableName, &client_table_);
    ASSERT_TRUE(s.IsNotFound());
    ASSERT_STR_CONTAINS(s.ToString(), "the table does not exist");
  });

  vector<string> tables;
  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_EQ(0, tables.size());

  // Try to recall the table.
  Status s = client_->RecallTable(client_table_->id());
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_TRUE(tables.empty());
}

TEST_F(ClientTest, TestDeleteWithDeletedTableReserveSeconds) {
  // Open the table before deleting it.
  ASSERT_OK(client_->OpenTable(kTableName, &client_table_));

  // Insert a few rows, and scan them back. This is to populate the MetaCache.
  NO_FATALS(InsertTestRows(client_.get(), client_table_.get(), 10));
  vector<string> rows;
  ScanTableToStrings(client_table_.get(), &rows);
  ASSERT_EQ(10, rows.size());

  ASSERT_EQ(0, FLAGS_default_deleted_table_reserve_seconds);
  FLAGS_default_deleted_table_reserve_seconds = 60;

  // Delete the table, the table won't be purged immediately if
  // FLAGS_default_deleted_table_reserve_seconds set to anything greater than 0.
  ASSERT_OK(client_->DeleteTable(kTableName));
  CatalogManager *catalog_manager = cluster_->mini_master()->master()->catalog_manager();
  {
    CatalogManager::ScopedLeaderSharedLock l(catalog_manager);
    ASSERT_OK(l.first_failed_status());
    bool exists;
    ASSERT_OK(catalog_manager->TableNameExists(kTableName, &exists));
    ASSERT_TRUE(exists);
  }

  // The table is in soft-deleted state.
  vector<string> tables;
  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_EQ(1, tables.size());
  ASSERT_EQ(kTableName, tables[0]);
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_EQ(0, tables.size());

  // Try to recall the table.
  ASSERT_OK(client_->RecallTable(client_table_->id()));

  // Check the data in the table.
  ScanTableToStrings(client_table_.get(), &rows);
  ASSERT_EQ(10, rows.size());

  // Force to delete the table with FLAGS_default_deleted_table_reserve_seconds set to 0.
  FLAGS_default_deleted_table_reserve_seconds = 0;
  ASSERT_OK(client_->DeleteTable(kTableName));

  // No tables left.
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_TRUE(tables.empty());
  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_TRUE(tables.empty());
}

TEST_F(ClientTest, TestDeleteWithDeletedTableReserveSecondsWorks) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  // Open the table before deleting it.
  ASSERT_OK(client_->OpenTable(kTableName, &client_table_));

  ASSERT_EQ(0, FLAGS_default_deleted_table_reserve_seconds);
  FLAGS_default_deleted_table_reserve_seconds = 5;

  // Delete the table, the table won't be purged immediately if
  // FLAGS_default_deleted_table_reserve_seconds set to anything greater than 0.
  ASSERT_OK(client_->DeleteTable(kTableName));
  CatalogManager *catalog_manager = cluster_->mini_master()->master()->catalog_manager();
  {
    CatalogManager::ScopedLeaderSharedLock l(catalog_manager);
    ASSERT_OK(l.first_failed_status());
    bool exists;
    ASSERT_OK(catalog_manager->TableNameExists(kTableName, &exists));
    ASSERT_TRUE(exists);
  }

  // The table is in soft-deleted state.
  vector<string> tables;
  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_EQ(1, tables.size());
  ASSERT_EQ(kTableName, tables[0]);
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_TRUE(tables.empty());

  // Test FLAGS_table_reserve_seconds.
  SleepFor(MonoDelta::FromMilliseconds(5 * 1000));

  // No tables left.
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_TRUE(tables.empty());
  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_TRUE(tables.empty());
}

TEST_F(ClientTest, TestDeleteSoftDeleteStatusFromDeletedTableReserveSeconds) {
  // Open the table before deleting it.
  ASSERT_OK(client_->OpenTable(kTableName, &client_table_));

  ASSERT_EQ(0, FLAGS_default_deleted_table_reserve_seconds);
  FLAGS_default_deleted_table_reserve_seconds = 600;

  // Get a soft-deleted table.
  ASSERT_OK(client_->SoftDeleteTable(kTableName, 60));
  CatalogManager *catalog_manager = cluster_->mini_master()->master()->catalog_manager();
  {
    CatalogManager::ScopedLeaderSharedLock l(catalog_manager);
    ASSERT_OK(l.first_failed_status());
    bool exists;
    ASSERT_OK(catalog_manager->TableNameExists(kTableName, &exists));
    ASSERT_TRUE(exists);
  }

  // The table is in soft-deleted state.
  vector<string> tables;
  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_EQ(1, tables.size());
  ASSERT_EQ(kTableName, tables[0]);
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_TRUE(tables.empty());

  // The behavior of DeleteTable turn to soft-delete
  // soft-deleted table should not be soft deleted
  Status s = client_->DeleteTable(kTableName);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
      Substitute("soft_deleted table $0 should not be soft deleted again", kTableName));
  // purge the soft-deleted table
  ASSERT_OK(client_->SoftDeleteTable(kTableName, 0));

  // No tables left.
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_TRUE(tables.empty());
  ASSERT_OK(client_->ListSoftDeletedTables(&tables));
  ASSERT_TRUE(tables.empty());
}

TEST_F(ClientTest, TestGetTableSchema) {
  KuduSchema schema;

  // Verify the schema for the current table
  ASSERT_OK(client_->GetTableSchema(kTableName, &schema));
  ASSERT_EQ(schema_, schema);

  // Verify that a get schema request for a missing table throws not found
  Status s = client_->GetTableSchema("MissingTableName", &schema);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "the table does not exist");
}

// Test creating and accessing a table which has multiple tablets,
// each of which is replicated.
//
// TODO: this should probably be the default for _all_ of the tests
// in this file. However, some things like alter table are not yet
// working on replicated tables - see KUDU-304
TEST_F(ClientTest, TestReplicatedMultiTabletTable) {
  const string kReplicatedTable = "replicated";
  const int kNumRowsToWrite = 100;
  const int kNumReplicas = 3;

  shared_ptr<KuduTable> table;
  ASSERT_OK(CreateTable(kReplicatedTable,
                        kNumReplicas,
                        GenerateSplitRows(),
                        {},
                        &table));

  // Should have no rows to begin with.
  ASSERT_EQ(0, CountRowsFromClient(table.get()));

  // Insert some data.
  NO_FATALS(InsertTestRows(table.get(), kNumRowsToWrite));

  // Should now see the data.
  ASSERT_EQ(kNumRowsToWrite, CountRowsFromClient(table.get()));

  // TODO: once leader re-election is in, should somehow force a re-election
  // and ensure that the client handles refreshing the leader.
}

TEST_F(ClientTest, TestReplicatedMultiTabletTableFailover) {
  const string kReplicatedTable = "replicated_failover_on_reads";
  const int kNumRowsToWrite = 100;
  const int kNumReplicas = 3;
  const int kNumTries = 100;

  shared_ptr<KuduTable> table;
  ASSERT_OK(CreateTable(kReplicatedTable,
                        kNumReplicas,
                        GenerateSplitRows(),
                        {},
                        &table));

  // Insert some data.
  NO_FATALS(InsertTestRows(table.get(), kNumRowsToWrite));

  // Find the leader of the first tablet.
  scoped_refptr<internal::RemoteTablet> rt = MetaCacheLookup(table.get(), {});
  internal::RemoteTabletServer *rts = rt->LeaderTServer();

  // Kill the leader of the first tablet.
  ASSERT_OK(KillTServer(rts->permanent_uuid()));

  // We wait until we fail over to the new leader(s).
  int tries = 0;
  for (;;) {
    int master_rpcs_before = CountMasterLookupRPCs();
    tries++;
    int num_rows = CountRowsFromClient(table.get(),
                                       KuduClient::LEADER_ONLY,
                                       KuduScanner::READ_LATEST);
    int master_rpcs = CountMasterLookupRPCs() - master_rpcs_before;

    // Regression test for KUDU-1387: we should not have any tight loops
    // hitting the master during the time when a client is awaiting leader
    // election. However, we do expect a reasonable number of lookup retries
    // as the client will retry rapidly at first and then back off. 20 is
    // enough to avoid test flakiness. Before fixing the above-mentioned bug,
    // we would see several hundred lookups here.
    ASSERT_LT(master_rpcs, 20);

    if (num_rows == kNumRowsToWrite) {
      LOG(INFO) << "Found expected number of rows: " << num_rows;
      break;
    } else {
      LOG(INFO) << "Only found " << num_rows << " rows on try "
                << tries << ", retrying";
      ASSERT_LE(tries, kNumTries);
      SleepFor(MonoDelta::FromMilliseconds(10 * tries)); // sleep a bit more with each attempt.
    }
  }
}

// This test that we can keep writing to a tablet when the leader
// tablet dies.
TEST_F(ClientTest, TestReplicatedTabletWritesWithLeaderElection) {
  const string kReplicatedTable = "replicated_failover_on_writes";
  const int kNumRowsToWrite = 100;
  const int kNumReplicas = 3;

  shared_ptr<KuduTable> table;
  ASSERT_OK(CreateTable(kReplicatedTable, kNumReplicas, {}, {}, &table));

  // Insert some data.
  NO_FATALS(InsertTestRows(table.get(), kNumRowsToWrite));

  // Find the leader replica
  scoped_refptr<internal::RemoteTablet> rt = MetaCacheLookup(table.get(), {});
  internal::RemoteTabletServer *rts;
  set<string> blacklist;
  vector<internal::RemoteTabletServer*> candidates;
  ASSERT_OK(client_->data_->GetTabletServer(client_.get(),
                                            rt,
                                            KuduClient::LEADER_ONLY,
                                            blacklist,
                                            &candidates,
                                            &rts));

  string killed_uuid = rts->permanent_uuid();
  // Kill the tserver that is serving the leader tablet.
  ASSERT_OK(KillTServer(killed_uuid));

  LOG(INFO) << "Inserting additional rows...";
  NO_FATALS(InsertTestRows(client_.get(),
                           table.get(),
                           kNumRowsToWrite,
                           kNumRowsToWrite));

  LOG(INFO) << "Counting rows...";
  ASSERT_EQ(2 * kNumRowsToWrite, CountRowsFromClient(table.get(),
                                                     KuduClient::LEADER_ONLY,
                                                     KuduScanner::READ_LATEST));
}

namespace {

void CheckCorrectness(KuduScanner* scanner, int expected[], int nrows) {
  scanner->Open();
  int readrows = 0;
  KuduScanBatch batch;
  if (nrows) {
    ASSERT_TRUE(scanner->HasMoreRows());
  } else {
    ASSERT_FALSE(scanner->HasMoreRows());
  }

  while (scanner->HasMoreRows()) {
    ASSERT_OK(scanner->NextBatch(&batch));
    for (const KuduScanBatch::RowPtr& r : batch) {
      int32_t key;
      int32_t val;
      Slice strval;
      ASSERT_OK(r.GetInt32(0, &key));
      ASSERT_OK(r.GetInt32(1, &val));
      ASSERT_OK(r.GetString(2, &strval));
      ASSERT_NE(expected[key], -1) << "Deleted key found in table in table " << key;
      ASSERT_EQ(expected[key], val) << "Incorrect int value for key " <<  key;
      ASSERT_EQ(strval.size(), 0) << "Incorrect string value for key " << key;
      ++readrows;
    }
  }
  ASSERT_EQ(readrows, nrows);
  scanner->Close();
}

} // anonymous namespace

// Randomized mutations accuracy testing
TEST_F(ClientTest, TestRandomWriteOperation) {
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(5000);
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  int row[FLAGS_test_scan_num_rows]; // -1 indicates empty
  int nrows;
  KuduScanner scanner(client_table_.get());

  // First half-fill
  for (int i = 0; i < FLAGS_test_scan_num_rows/2; ++i) {
    ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, i, i, ""));
    row[i] = i;
  }
  for (int i = FLAGS_test_scan_num_rows/2; i < FLAGS_test_scan_num_rows; ++i) {
    row[i] = -1;
  }
  nrows = FLAGS_test_scan_num_rows/2;

  // Randomized testing
  LOG(INFO) << "Randomized mutations testing.";
  SeedRandom();
  for (int i = 0; i <= 1000; ++i) {
    // Test correctness every so often
    if (i % 50 == 0) {
      LOG(INFO) << "Correctness test " << i;
      FlushSessionOrDie(session);
      NO_FATALS(CheckCorrectness(&scanner, row, nrows));
      LOG(INFO) << "...complete";
    }

    int change = rand() % FLAGS_test_scan_num_rows;
    // Insert if empty
    if (row[change] == -1) {
      ASSERT_OK(ApplyInsertToSession(session.get(),
                                            client_table_,
                                            change,
                                            change,
                                            ""));
      row[change] = change;
      ++nrows;
      VLOG(1) << "Insert " << change;
    } else {
      // Update or delete otherwise
      int update = rand() & 1;
      if (update) {
        ASSERT_OK(ApplyUpdateToSession(session.get(),
                                              client_table_,
                                              change,
                                              ++row[change]));
        VLOG(1) << "Update " << change;
      } else {
        ASSERT_OK(ApplyDeleteToSession(session.get(),
                                              client_table_,
                                              change));
        row[change] = -1;
        --nrows;
        VLOG(1) << "Delete " << change;
      }
    }
  }

  // And one more time for the last batch.
  FlushSessionOrDie(session);
  NO_FATALS(CheckCorrectness(&scanner, row, nrows));
}

// Test whether a batch can handle several mutations in a batch
TEST_F(ClientTest, TestSeveralRowMutatesPerBatch) {
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(5000);
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Test insert/update
  LOG(INFO) << "Testing insert/update in same batch, key " << 1 << ".";
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, ""));
  ASSERT_OK(ApplyUpdateToSession(session.get(), client_table_, 1, 2));
  FlushSessionOrDie(session);
  vector<string> rows;
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ(R"((int32 key=1, int32 int_val=2, string string_val="", )"
            "int32 non_null_with_default=12345)", rows[0]);
  rows.clear();


  LOG(INFO) << "Testing insert/delete in same batch, key " << 2 << ".";
  // Test insert/delete
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 2, 1, ""));
  ASSERT_OK(ApplyDeleteToSession(session.get(), client_table_, 2));
  FlushSessionOrDie(session);
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ(R"((int32 key=1, int32 int_val=2, string string_val="", )"
            "int32 non_null_with_default=12345)", rows[0]);
  rows.clear();

  // Test update/delete
  LOG(INFO) << "Testing update/delete in same batch, key " << 1 << ".";
  ASSERT_OK(ApplyUpdateToSession(session.get(), client_table_, 1, 1));
  ASSERT_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  FlushSessionOrDie(session);
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_TRUE(rows.empty());

  // Test delete/insert (insert a row first)
  LOG(INFO) << "Inserting row for delete/insert test, key " << 1 << ".";
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, ""));
  FlushSessionOrDie(session);
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ(R"((int32 key=1, int32 int_val=1, string string_val="", )"
            "int32 non_null_with_default=12345)", rows[0]);
  rows.clear();
  LOG(INFO) << "Testing delete/insert in same batch, key " << 1 << ".";
  ASSERT_OK(ApplyDeleteToSession(session.get(), client_table_, 1));
  ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, 1, 2, ""));
  FlushSessionOrDie(session);
  ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ(R"((int32 key=1, int32 int_val=2, string string_val="", )"
            "int32 non_null_with_default=12345)", rows[0]);
            rows.clear();
}

// Tests that master permits are properly released after a whole bunch of
// rows are inserted.
TEST_F(ClientTest, TestMasterLookupPermits) {
  int initial_value = client_->data_->meta_cache_->master_lookup_sem_.GetValue();
  NO_FATALS(InsertTestRows(client_table_.get(), FLAGS_test_scan_num_rows));
  ASSERT_EQ(initial_value,
            client_->data_->meta_cache_->master_lookup_sem_.GetValue());
}

// Define callback for deadlock simulation, as well as various helper methods.
namespace {
class DLSCallback : public KuduStatusCallback {
 public:
  explicit DLSCallback(Atomic32* i) : i_(i) {
  }

  virtual void Run(const Status& s) OVERRIDE {
    CHECK_OK(s);
    NoBarrier_AtomicIncrement(i_, 1);
    delete this;
  }
 private:
  Atomic32* const i_;
};

// Returns col1 value of first row.
int32_t ReadFirstRowKeyFirstCol(const shared_ptr<KuduTable>& tbl) {
  KuduScanner scanner(tbl.get());

  scanner.Open();
  KuduScanBatch batch;
  CHECK(scanner.HasMoreRows());
  CHECK_OK(scanner.NextBatch(&batch));
  KuduRowResult row = batch.Row(0);
  int32_t val;
  CHECK_OK(row.GetInt32(1, &val));
  return val;
}

// Checks that all rows have value equal to expected, return number of rows.
int CheckRowsEqual(const shared_ptr<KuduTable>& tbl, int32_t expected) {
  KuduScanner scanner(tbl.get());
  scanner.Open();
  KuduScanBatch batch;
  int cnt = 0;
  while (scanner.HasMoreRows()) {
    CHECK_OK(scanner.NextBatch(&batch));
    for (const KuduScanBatch::RowPtr& row : batch) {
      // Check that for every key:
      // 1. Column 1 int32_t value == expected
      // 2. Column 2 string value is empty
      // 3. Column 3 int32_t value is default, 12345
      int32_t key;
      int32_t val;
      Slice strval;
      int32_t val2;
      CHECK_OK(row.GetInt32(0, &key));
      CHECK_OK(row.GetInt32(1, &val));
      CHECK_OK(row.GetString(2, &strval));
      CHECK_OK(row.GetInt32(3, &val2));
      CHECK_EQ(expected, val) << "Incorrect int value for key " << key;
      CHECK_EQ(strval.size(), 0) << "Incorrect string value for key " << key;
      CHECK_EQ(12345, val2);
      ++cnt;
    }
  }
  return cnt;
}

// Return a session "loaded" with updates. Sets the session timeout
// to the parameter value. Larger timeouts decrease false positives.
shared_ptr<KuduSession> LoadedSession(const shared_ptr<KuduClient>& client,
                                      const shared_ptr<KuduTable>& tbl,
                                      bool fwd, int max, int timeout) {
  shared_ptr<KuduSession> session = client->NewSession();
  session->SetTimeoutMillis(timeout);
  CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  for (int i = 0; i < max; ++i) {
    int key = fwd ? i : max - i;
    CHECK_OK(ApplyUpdateToSession(session.get(), tbl, key, fwd));
  }
  return session;
}
} // anonymous namespace

// Starts many clients which update a table in parallel.
// Half of the clients update rows in ascending order while the other
// half update rows in descending order.
// This ensures that we don't hit a deadlock in such a situation.
TEST_F(ClientTest, TestDeadlockSimulation) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Make reverse client who will make batches that update rows
  // in reverse order. Separate client used so rpc calls come in at same time.
  shared_ptr<KuduClient> rev_client;
  ASSERT_OK(KuduClientBuilder()
                   .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
                   .Build(&rev_client));
  shared_ptr<KuduTable> rev_table;
  ASSERT_OK(client_->OpenTable(kTableName, &rev_table));

  // Load up some rows
  const int kNumRows = 300;
  const int kTimeoutMillis = 60000;
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(kTimeoutMillis);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  for (int i = 0; i < kNumRows; ++i)
    ASSERT_OK(ApplyInsertToSession(session.get(), client_table_, i, i,  ""));
  FlushSessionOrDie(session);

  // Check both clients see rows
  int fwd = CountRowsFromClient(client_table_.get());
  ASSERT_EQ(kNumRows, fwd);
  int rev = CountRowsFromClient(rev_table.get());
  ASSERT_EQ(kNumRows, rev);

  // Generate sessions
  const int kNumSessions = 100;
  shared_ptr<KuduSession> fwd_sessions[kNumSessions];
  shared_ptr<KuduSession> rev_sessions[kNumSessions];
  for (int i = 0; i < kNumSessions; ++i) {
    fwd_sessions[i] = LoadedSession(client_, client_table_, true, kNumRows, kTimeoutMillis);
    rev_sessions[i] = LoadedSession(rev_client, rev_table, true, kNumRows, kTimeoutMillis);
  }

  // Run async calls - one thread updates sequentially, another in reverse.
  Atomic32 ctr1, ctr2;
  NoBarrier_Store(&ctr1, 0);
  NoBarrier_Store(&ctr2, 0);
  for (int i = 0; i < kNumSessions; ++i) {
    // The callbacks are freed after they are invoked.
    fwd_sessions[i]->FlushAsync(new DLSCallback(&ctr1));
    rev_sessions[i]->FlushAsync(new DLSCallback(&ctr2));
  }

  // Spin while waiting for ops to complete.
  int lctr1, lctr2, prev1 = 0, prev2 = 0;
  do {
    lctr1 = NoBarrier_Load(&ctr1);
    lctr2 = NoBarrier_Load(&ctr2);
    // Display progress in 10% increments.
    if (prev1 == 0 || lctr1 + lctr2 - prev1 - prev2 > kNumSessions / 10) {
      LOG(INFO) << "# updates: " << lctr1 << " fwd, " << lctr2 << " rev";
      prev1 = lctr1;
      prev2 = lctr2;
    }
    SleepFor(MonoDelta::FromMilliseconds(100));
  } while (lctr1 != kNumSessions|| lctr2 != kNumSessions);
  int32_t expected = ReadFirstRowKeyFirstCol(client_table_);

  // Check transaction from forward client.
  fwd = CheckRowsEqual(client_table_, expected);
  ASSERT_EQ(fwd, kNumRows);

  // Check from reverse client side.
  rev = CheckRowsEqual(rev_table, expected);
  ASSERT_EQ(rev, kNumRows);
}

TEST_F(ClientTest, TestCreateDuplicateTable) {
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_TRUE(table_creator->table_name(kTableName)
              .set_range_partition_columns({ "key" })
              .schema(&schema_)
              .num_replicas(1)
              .Create().IsAlreadyPresent());
}

TEST_F(ClientTest, TestCreateTableWithTooManyTablets) {
  FLAGS_max_create_tablets_per_ts = 1;

  // Add two more tservers so that we can create a table with replication factor
  // of 3 below.
  ASSERT_OK(cluster_->AddTabletServer());
  ASSERT_OK(cluster_->AddTabletServer());
  ASSERT_OK(cluster_->WaitForTabletServerCount(3));

  KuduPartialRow* split1 = schema_.NewRow();
  ASSERT_OK(split1->SetInt32("key", 1));

  KuduPartialRow* split2 = schema_.NewRow();
  ASSERT_OK(split2->SetInt32("key", 2));

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  Status s = table_creator->table_name("foobar")
      .schema(&schema_)
      .set_range_partition_columns({ "key" })
      .split_rows({ split1, split2 })
      .num_replicas(3)
      .Create();
#pragma GCC diagnostic pop
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "the requested number of tablet replicas is over the "
                      "maximum permitted at creation time (3)");
}

TEST_F(ClientTest, TestCreateTableWithInvalidEncodings) {
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  KuduSchema schema;
  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey()
      ->Encoding(KuduColumnStorageAttributes::DICT_ENCODING);
  ASSERT_OK(schema_builder.Build(&schema));
  Status s = table_creator->table_name("foobar")
      .schema(&schema)
      .set_range_partition_columns({ "key" })
      .Create();
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "invalid encoding for column 'key': encoding "
                      "DICT_ENCODING not supported for type INT32");
}

TEST_F(ClientTest, TestCreateTableWithTooManyColumns) {
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  KuduSchema schema;
  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  for (int i = 0; i < 1000; i++) {
    schema_builder.AddColumn(Substitute("c$0", i))
        ->Type(KuduColumnSchema::INT32)->NotNull();
  }
  ASSERT_OK(schema_builder.Build(&schema));
  Status s = table_creator->table_name("foobar")
      .schema(&schema)
      .set_range_partition_columns({ "key" })
      .Create();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "number of columns 1001 is greater than the "
                      "permitted maximum 300");
}

TEST_F(ClientTest, TestCreateTable_TableNames) {
  const vector<pair<string, string>> kCases = {
    {string(1000, 'x'), "longer than maximum permitted length"},
    {string("foo\0bar", 7), "invalid table name: identifier must not contain null bytes"},
    // From http://stackoverflow.com/questions/1301402/example-invalid-utf8-string
    {string("foo\xf0\x28\x8c\xbc", 7), "invalid table name: invalid UTF8 sequence"},
    // Should pass validation but fail due to lack of tablet servers running.
    {"你好", "not enough live tablet servers"}
  };

  for (const auto& test_case : kCases) {
    const auto& bad_name = test_case.first;
    const auto& substr = test_case.second;
    SCOPED_TRACE(bad_name);

    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    KuduSchema schema;
    KuduSchemaBuilder schema_builder;
    schema_builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    ASSERT_OK(schema_builder.Build(&schema));
    Status s = table_creator->table_name(bad_name)
        .schema(&schema)
        .set_range_partition_columns({ "key" })
        .Create();
    ASSERT_STR_CONTAINS(s.ToString(), substr);
  }
}

TEST_F(ClientTest, TestCreateTableWithTooLongColumnName) {
  const string kLongName(1000, 'x');
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  KuduSchema schema;
  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn(kLongName)->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  ASSERT_OK(schema_builder.Build(&schema));
  Status s = table_creator->table_name("foobar")
      .schema(&schema)
      .set_range_partition_columns({ kLongName })
      .Create();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(),
                     "invalid column name: identifier 'xxx*' "
                     "longer than maximum permitted length 256");
}

TEST_F(ClientTest, TestCreateTableWithExtraConfigs) {
  string table_name = "extra_configs";
  {
    map<string, string> extra_configs;
    extra_configs["kudu.table.history_max_age_sec"] = "7200";
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    Status s = table_creator->table_name(table_name)
        .set_range_partition_columns({"key"})
        .schema(&schema_)
        .num_replicas(1)
        .extra_configs(extra_configs)
        .Create();
    ASSERT_TRUE(s.ok());
  }

  {
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(table_name, &table));
    map<string, string> extra_configs = table->extra_configs();
    ASSERT_TRUE(ContainsKey(extra_configs, "kudu.table.history_max_age_sec"));
    ASSERT_EQ("7200", extra_configs["kudu.table.history_max_age_sec"]);
  }
}

// Test trying to insert a row with an encoded key that is too large.
TEST_F(ClientTest, TestInsertTooLongEncodedPrimaryKey) {
  const string kLongValue(10000, 'x');
  const char* kTableName = "too-long-pk";

  // Create and open a table with a three-column composite PK.
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  KuduSchema schema;
  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("k1")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("k2")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("k3")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.SetPrimaryKey({"k1", "k2", "k3"});
  ASSERT_OK(schema_builder.Build(&schema));
  ASSERT_OK(table_creator->table_name(kTableName)
            .schema(&schema)
            .num_replicas(1)
            .set_range_partition_columns({ "k1" })
            .Create());
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  // Create a session and insert a row with a too-large composite key.
  shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  unique_ptr<KuduInsert> insert(table->NewInsert());
  for (int i = 0; i < 3; i++) {
    ASSERT_OK(insert->mutable_row()->SetStringCopy(i, kLongValue));
  }
  ASSERT_OK(session->Apply(insert.release()));
  Status s = session->Flush();
  ASSERT_FALSE(s.ok()) << s.ToString();

  // Check the error.
  vector<KuduError*> errors;
  ElementDeleter drop(&errors);
  bool overflowed;
  session->GetPendingErrors(&errors, &overflowed);
  ASSERT_EQ(1, errors.size());
  EXPECT_EQ("Invalid argument: encoded primary key too large "
            "(30004 bytes, maximum is 16384 bytes)",
            errors[0]->status().ToString());
}

// Test trying to insert a row with an empty string PK.
// Regression test for KUDU-1899.
TEST_F(ClientTest, TestInsertEmptyPK) {
  const string kLongValue(10000, 'x');
  const char* kTableName = "empty-pk";

  // Create and open a table with a three-column composite PK.
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  KuduSchema schema;
  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("k1")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("v1")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.SetPrimaryKey({"k1"});
  ASSERT_OK(schema_builder.Build(&schema));
  ASSERT_OK(table_creator->table_name(kTableName)
            .schema(&schema)
            .num_replicas(1)
            .set_range_partition_columns({ "k1" })
            .Create());
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  // Find the tablet.
  scoped_refptr<TabletReplica> tablet_replica;
  ASSERT_TRUE(cluster_->mini_tablet_server(0)->server()->tablet_manager()->LookupTablet(
          GetFirstTabletId(table.get()), &tablet_replica));

  // Utility function to get the current value of the row.
  const auto ReadRowAsString = [&]() {
    vector<string> rows;
    CHECK_OK(ScanTableToStrings(table.get(), &rows));
    if (rows.empty()) {
      return string("<none>");
    }
    CHECK_EQ(1, rows.size());
    return rows[0];
  };

  shared_ptr<KuduSession> session(client_->NewSession());
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Insert a row with empty primary key.
  {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetString(0, ""));
    ASSERT_OK(insert->mutable_row()->SetString(1, "initial"));
    ASSERT_OK(session->Apply(insert.release()));
    ASSERT_OK(session->Flush());
  }
  ASSERT_EQ("(string k1=\"\", string v1=\"initial\")", ReadRowAsString());

  // Make sure that Flush works properly, and the data is still readable.
  ASSERT_OK(tablet_replica->tablet()->Flush());
  ASSERT_EQ("(string k1=\"\", string v1=\"initial\")", ReadRowAsString());

  // Perform an update.
  {
    unique_ptr<KuduUpdate> update(table->NewUpdate());
    ASSERT_OK(update->mutable_row()->SetString(0, ""));
    ASSERT_OK(update->mutable_row()->SetString(1, "updated"));
    ASSERT_OK(session->Apply(update.release()));
    ASSERT_OK(session->Flush());
  }
  ASSERT_EQ("(string k1=\"\", string v1=\"updated\")", ReadRowAsString());
  ASSERT_OK(tablet_replica->tablet()->FlushAllDMSForTests());
  ASSERT_EQ("(string k1=\"\", string v1=\"updated\")", ReadRowAsString());
  ASSERT_OK(tablet_replica->tablet()->Compact(tablet::Tablet::FORCE_COMPACT_ALL));
  ASSERT_EQ("(string k1=\"\", string v1=\"updated\")", ReadRowAsString());

  // Perform a delete.
  {
    unique_ptr<KuduDelete> del(table->NewDelete());
    ASSERT_OK(del->mutable_row()->SetString(0, ""));
    ASSERT_OK(session->Apply(del.release()));
    ASSERT_OK(session->Flush());
  }
  ASSERT_EQ("<none>", ReadRowAsString());
  ASSERT_OK(tablet_replica->tablet()->FlushAllDMSForTests());
  ASSERT_EQ("<none>", ReadRowAsString());
  ASSERT_OK(tablet_replica->tablet()->Compact(tablet::Tablet::FORCE_COMPACT_ALL));
  ASSERT_EQ("<none>", ReadRowAsString());
}

class LatestObservedTimestampParamTest :
    public ClientTest,
    public ::testing::WithParamInterface<KuduScanner::ReadMode> {
};
// Check the behavior of the latest observed timestamp when performing
// write and read operations.
TEST_P(LatestObservedTimestampParamTest, Test) {
  const KuduScanner::ReadMode read_mode = GetParam();
  // Check that a write updates the latest observed timestamp.
  const uint64_t ts0 = client_->GetLatestObservedTimestamp();
  ASSERT_EQ(KuduClient::kNoTimestamp, ts0);
  NO_FATALS(InsertTestRows(client_table_.get(), 1, 0));
  const uint64_t ts1 = client_->GetLatestObservedTimestamp();
  ASSERT_NE(ts0, ts1);

  // Check that the latest observed timestamp moves forward when
  // a scan is performed by the same or another client, even if reading/scanning
  // at the fixed timestamp observed at the prior insert.
  uint64_t latest_ts = ts1;
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
      .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
      .Build(&client));
  vector<shared_ptr<KuduClient>> clients{ client_, client };
  for (auto& c : clients) {
    if (c != client_) {
      // Check that the new client has no latest observed timestamp.
      ASSERT_EQ(KuduClient::kNoTimestamp, c->GetLatestObservedTimestamp());
      // The observed timestamp may not move forward when scan in
      // READ_YOUR_WRITES mode by another client. Since other client
      // does not have the same propagation timestamp bound and the
      // chosen snapshot timestamp is returned as the new propagation
      // timestamp.
      if (read_mode == KuduScanner::READ_YOUR_WRITES) break;
    }
    shared_ptr<KuduTable> table;
    ASSERT_OK(c->OpenTable(client_table_->name(), &table));
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetReadMode(read_mode));
    if (read_mode == KuduScanner::READ_AT_SNAPSHOT) {
      ASSERT_OK(scanner.SetSnapshotRaw(ts1));
    }
    ASSERT_OK(scanner.Open());
    const uint64_t ts = c->GetLatestObservedTimestamp();
    ASSERT_LT(latest_ts, ts);
    latest_ts = ts;
  }
}
INSTANTIATE_TEST_SUITE_P(Params, LatestObservedTimestampParamTest,
                         testing::ValuesIn(read_modes));

// Insert bunch of rows, delete a row, and then insert the row back.
// Run scans several scan and check the results are consistent with the
// specified timestamps:
//   * at the snapshot corresponding the timestamp when the row was deleted
//   * at the snapshot corresponding the timestamp when the row was inserted
//     back
//   * read the latest data with no specified timestamp (READ_LATEST)
TEST_F(ClientTest, TestScanAtLatestObservedTimestamp) {
  const uint64_t pre_timestamp =
      cluster_->mini_tablet_server(0)->server()->clock()->Now().ToUint64();
  static const size_t kRowsNum = 2;
  NO_FATALS(InsertTestRows(client_table_.get(), kRowsNum, 0));
  const uint64_t ts_inserted_rows = client_->GetLatestObservedTimestamp();
  // Delete one row (key == 0)
  NO_FATALS(DeleteTestRows(client_table_.get(), 0, 1));
  const uint64_t ts_deleted_row = client_->GetLatestObservedTimestamp();
  // Insert the deleted row back.
  NO_FATALS(InsertTestRows(client_table_.get(), 1, 0));
  const uint64_t ts_all_rows = client_->GetLatestObservedTimestamp();
  ASSERT_GT(ts_all_rows, ts_deleted_row);

  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
      .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
      .Build(&client));
  vector<shared_ptr<KuduClient>> clients{ client_, client };
  for (auto& c : clients) {
    SCOPED_TRACE((c == client_) ? "the same client" : "another client");
    shared_ptr<KuduTable> table;
    ASSERT_OK(c->OpenTable(client_table_->name(), &table));
    // There should be no rows in the table prior to the initial insert.
    size_t row_count_before_inserted_rows;
    ASSERT_OK(CountRowsOnLeaders(table.get(), KuduScanner::READ_AT_SNAPSHOT,
                                 pre_timestamp, &row_count_before_inserted_rows));
    EXPECT_EQ(0, row_count_before_inserted_rows);
    // There should be kRowsNum rows if scanning at the initial insert timestamp.
    size_t row_count_ts_inserted_rows;
    ASSERT_OK(CountRowsOnLeaders(table.get(), KuduScanner::READ_AT_SNAPSHOT,
                                 ts_inserted_rows, &row_count_ts_inserted_rows));
    EXPECT_EQ(kRowsNum, row_count_ts_inserted_rows);
    // There should be one less row if scanning at the deleted row timestamp.
    size_t row_count_ts_deleted_row;
    ASSERT_OK(CountRowsOnLeaders(table.get(), KuduScanner::READ_AT_SNAPSHOT,
                                 ts_deleted_row, &row_count_ts_deleted_row));
    EXPECT_EQ(kRowsNum - 1, row_count_ts_deleted_row);
    // There should be kNumRows rows if scanning at the 'after last insert'
    // timestamp.
    size_t row_count_ts_all_rows;
    ASSERT_OK(CountRowsOnLeaders(table.get(), KuduScanner::READ_AT_SNAPSHOT,
                                 ts_all_rows, &row_count_ts_all_rows));
    EXPECT_EQ(kRowsNum, row_count_ts_all_rows);
    // There should be 2 rows if scanning in the 'read latest' mode.
    size_t row_count_read_latest;
    ASSERT_OK(CountRowsOnLeaders(table.get(), KuduScanner::READ_LATEST,
                                 0, &row_count_read_latest));
    EXPECT_EQ(kRowsNum, row_count_read_latest);
  }
}

// Peform a READ_AT_SNAPSHOT scan with no explicit snapshot timestamp specified
// and verify that the timestamp received in the first tablet server's response
// is set into the scan configuration and used for subsequent requests
// sent to other tablet servers hosting the table's data.
// Basically, this is a unit test for KUDU-1189.
TEST_F(ClientTest, TestReadAtSnapshotNoTimestampSet) {
  // Number of tablets which host the tablet data.
  static const size_t kTabletsNum = 3;
  static const size_t kRowsPerTablet = 2;

  shared_ptr<KuduTable> table;
  {
    vector<unique_ptr<KuduPartialRow>> rows;
    for (size_t i = 1; i < kTabletsNum; ++i) {
      unique_ptr<KuduPartialRow> row(schema_.NewRow());
      CHECK_OK(row->SetInt32(0, i * kRowsPerTablet));
      rows.push_back(std::move(row));
    }
    ASSERT_OK(CreateTable("test_table", 1, std::move(rows), {}, &table));
    // Insert some data into the table, so each tablet would get populated.
    shared_ptr<KuduSession> session(client_->NewSession());
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    for (size_t i = 0; i < kTabletsNum * kRowsPerTablet; ++i) {
      unique_ptr<KuduInsert> insert(BuildTestInsert(table.get(), i));
      ASSERT_OK(session->Apply(insert.release()));
    }
    FlushSessionOrDie(session);
  }

  // Now, run a scan in READ_AT_SNAPSHOT mode with no timestamp specified.
  // The scan should fetch all the inserted rows. Since it's a multi-tablet
  // scan, in the process there should be one NextBatch() per tablet.
  KuduScanner sc(table.get());
  ASSERT_OK(sc.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));

  // The scan configuration object should not contain the snapshot timestamp:
  // since its a fresh scan object with no snashot timestamp set.
  ASSERT_FALSE(sc.data_->configuration().has_snapshot_timestamp());

  ASSERT_OK(sc.Open());
  // The reference timestamp.
  ASSERT_TRUE(sc.data_->configuration().has_snapshot_timestamp());
  const uint64_t ts_ref = sc.data_->configuration().snapshot_timestamp();
  ASSERT_TRUE(sc.data_->last_response_.has_snap_timestamp());
  EXPECT_EQ(ts_ref, sc.data_->last_response_.snap_timestamp());

  // On some of the KuduScanner::NextBatch() calls the client connects to the
  // next tablet server and fetches rows from there. It's necessary to check
  // that the initial timestamp received from the very first tablet server
  // stays as in the scan configuration, with no modification.
  size_t total_row_count = 0;
  while (sc.HasMoreRows()) {
    const uint64_t ts_pre = sc.data_->configuration().snapshot_timestamp();
    EXPECT_EQ(ts_ref, ts_pre);

    KuduScanBatch batch;
    ASSERT_OK(sc.NextBatch(&batch));
    const size_t row_count = batch.NumRows();
    total_row_count += row_count;

    const uint64_t ts_post = sc.data_->configuration().snapshot_timestamp();
    EXPECT_EQ(ts_ref, ts_post);
  }
  EXPECT_EQ(kTabletsNum * kRowsPerTablet, total_row_count);
}

TEST_F(ClientTest, TestCreateTableWithValidComment) {
  const string kTableName = "table_comment";
  const string kLongColumnComment(FLAGS_max_column_comment_length, 'x');
  const string kLongTableComment(FLAGS_max_table_comment_length, 'x');

  // Create a table with a table and column comment.
  {
    KuduSchema schema;
    KuduSchemaBuilder schema_builder;
    schema_builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    schema_builder.AddColumn("val")->Type(KuduColumnSchema::INT32)->Comment(kLongColumnComment);
    ASSERT_OK(schema_builder.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
      .schema(&schema).num_replicas(1).set_range_partition_columns({ "key" })
      .set_comment(kLongTableComment)
      .Create());
  }

  // Open the table and verify the comments.
  {
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    ASSERT_EQ(table->comment(), kLongTableComment);
    ASSERT_EQ(table->schema().Column(1).name(), "val");
    ASSERT_EQ(table->schema().Column(1).comment(), kLongColumnComment);
  }
}

TEST_F(ClientTest, TestAlterTableWithValidComment) {
  const string kTableName = "table_comment";
  const auto AlterAndVerify = [&] (int i) {
    {
      // The comment length should be less and equal than FLAGS_max_column_comment_length
      // and FLAGS_max_table_comment_length.
      string kLongComment(i * 16, 'x');

      // Alter the table with a table and column comment.
      unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
      table_alterer->SetComment(kLongComment);
      table_alterer->AlterColumn("val")->Comment(kLongComment);
      ASSERT_OK(table_alterer->Alter());

      // Open the table and verify the comments.
      shared_ptr<KuduTable> table;
      ASSERT_OK(client_->OpenTable(kTableName, &table));
      ASSERT_EQ(table->comment(), kLongComment);
      ASSERT_EQ(table->schema().Column(1).name(), "val");
      ASSERT_EQ(table->schema().Column(1).comment(), kLongComment);
    }
    {
      // Delete the comment.
      unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
      table_alterer->SetComment("");
      table_alterer->AlterColumn("val")->Comment("");
      ASSERT_OK(table_alterer->Alter());

      // Open the table and verify the comments.
      shared_ptr<KuduTable> table;
      ASSERT_OK(client_->OpenTable(kTableName, &table));
      ASSERT_EQ(table->comment(), "");
      ASSERT_EQ(table->schema().Column(1).name(), "val");
      ASSERT_EQ(table->schema().Column(1).comment(), "");
    }
  };

  // Create a table.
  {
    KuduSchema schema;
    KuduSchemaBuilder schema_builder;
    schema_builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    schema_builder.AddColumn("val")->Type(KuduColumnSchema::INT32);
    ASSERT_OK(schema_builder.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
        .schema(&schema).num_replicas(1).set_range_partition_columns({ "key" }).Create());
  }

  for (int i = 0; i <= 16; ++i) {
    NO_FATALS(AlterAndVerify(i));
  }
}

TEST_F(ClientTest, TestCreateAndAlterTableWithInvalidColumnComment) {
  const string kTableName = "table_comment";
  const vector<pair<string, string>> kCases = {
    {string(FLAGS_max_column_comment_length + 1, 'x'), "longer than maximum permitted length"},
    {string("foo\0bar", 7), "invalid column comment: identifier must not contain null bytes"},
    {string("foo\xf0\x28\x8c\xbc", 7), "invalid column comment: invalid UTF8 sequence"}
  };

  // Create tables with invalid column comment.
  for (const auto& test_case : kCases) {
    const auto& comment = test_case.first;
    const auto& substr = test_case.second;
    SCOPED_TRACE(comment);

    KuduSchema schema;
    KuduSchemaBuilder schema_builder;
    schema_builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    schema_builder.AddColumn("val")->Type(KuduColumnSchema::INT32)->Comment(comment);
    ASSERT_OK(schema_builder.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    Status s = table_creator->table_name(kTableName)
        .schema(&schema).num_replicas(1).set_range_partition_columns({ "key" }).Create();
    ASSERT_STR_CONTAINS(s.ToString(), substr);
  }

  // Create a table for later alterer.
  {
    KuduSchema schema;
    KuduSchemaBuilder schema_builder;
    schema_builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    schema_builder.AddColumn("val")->Type(KuduColumnSchema::INT32);
    ASSERT_OK(schema_builder.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
        .schema(&schema).num_replicas(1).set_range_partition_columns({ "key" }).Create());
  }

  // Alter tables with invalid column comment.
  for (const auto& test_case : kCases) {
    const auto& comment = test_case.first;
    const auto& substr = test_case.second;
    SCOPED_TRACE(comment);

    // Alter the table.
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->AlterColumn("val")->Comment(comment);
    Status s = table_alterer->Alter();
    ASSERT_STR_CONTAINS(s.ToString(), substr);
  }
}

TEST_F(ClientTest, TestCreateAndAlterTableWithInvalidTableComment) {
  const string kTableName = "table_comment";
  const vector<pair<string, string>> kCases = {
      {string(FLAGS_max_table_comment_length + 1, 'x'), "longer than maximum permitted length"},
      {string("foo\0bar", 7), "invalid table comment: identifier must not contain null bytes"},
      {string("foo\xf0\x28\x8c\xbc", 7), "invalid table comment: invalid UTF8 sequence"}
  };

  // Create tables with invalid table comment.
  for (const auto& test_case : kCases) {
    const auto& comment = test_case.first;
    const auto& substr = test_case.second;
    SCOPED_TRACE(comment);

    KuduSchema schema;
    KuduSchemaBuilder schema_builder;
    schema_builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    schema_builder.AddColumn("val")->Type(KuduColumnSchema::INT32);
    ASSERT_OK(schema_builder.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    Status s = table_creator->table_name(kTableName)
        .schema(&schema).num_replicas(1).set_range_partition_columns({ "key" })
        .set_comment(comment)
        .Create();
    ASSERT_STR_CONTAINS(s.ToString(), substr);
  }

  // Create a table for later alterer.
  {
    KuduSchema schema;
    KuduSchemaBuilder schema_builder;
    schema_builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    schema_builder.AddColumn("val")->Type(KuduColumnSchema::INT32);
    ASSERT_OK(schema_builder.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
        .schema(&schema).num_replicas(1).set_range_partition_columns({ "key" }).Create());
  }

  // Alter tables with invalid table comment.
  for (const auto& test_case : kCases) {
    const auto& comment = test_case.first;
    const auto& substr = test_case.second;
    SCOPED_TRACE(comment);

    // Alter the table.
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
    table_alterer->SetComment(comment);
    Status s = table_alterer->Alter();
    ASSERT_STR_CONTAINS(s.ToString(), substr);
  }
}

TEST_F(ClientTest, TestAlterTableChangeOwner) {
  const string kTableName = "table_owner";
  const string kOriginalOwner = "alice";
  const string kNewOwner = "bob";

  // Create table
  KuduSchema schema;
  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  schema_builder.AddColumn("val")->Type(KuduColumnSchema::INT32);
  ASSERT_OK(schema_builder.Build(&schema));
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
      .schema(&schema).num_replicas(1).set_range_partition_columns({ "key" })
      .set_owner(kOriginalOwner).Create());
  {
    shared_ptr<KuduTable> table;
    client_->OpenTable(kTableName, &table);
    ASSERT_EQ(kOriginalOwner, table->owner());
  }

  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  table_alterer->SetOwner(kNewOwner);
  ASSERT_OK(table_alterer->Alter());
  {
    shared_ptr<KuduTable> table;
    client_->OpenTable(kTableName, &table);
    ASSERT_EQ(kNewOwner, table->owner());
  }
}

enum IntEncoding {
  kPlain,
  kBitShuffle,
  kRunLength
};

class IntEncodingNullPredicatesTest : public ClientTest,
                                      public ::testing::WithParamInterface<IntEncoding> {
};

TEST_P(IntEncodingNullPredicatesTest, TestIntEncodings) {
  // Create table with appropriate encoded, nullable column.
  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  auto int_col = b.AddColumn("int_val")->Type(KuduColumnSchema::INT32);
  IntEncoding enc = GetParam();
  switch (enc) {
    case kPlain:
      int_col->Encoding(KuduColumnStorageAttributes::PLAIN_ENCODING);
      break;
    case kBitShuffle:
      int_col->Encoding(KuduColumnStorageAttributes::BIT_SHUFFLE);
      break;
    case kRunLength:
      int_col->Encoding(KuduColumnStorageAttributes::RLE);
      break;
  }
  ASSERT_OK(b.Build(&schema));

  string table_name = "IntEncodingNullPredicatesTestTable";
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(table_name)
                .schema(&schema)
                .num_replicas(1)
                .set_range_partition_columns({ "key" })
                .Create());

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(table_name, &table));

  // Insert rows.
  shared_ptr<KuduSession> session = table->client()->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(10000);
  const size_t kNumRows = AllowSlowTests() ? 10000 : 1000;
  for (int i = 0; i < kNumRows; i++) {
    KuduInsert* insert = table->NewInsert();
    KuduPartialRow* row = insert->mutable_row();
    ASSERT_OK(row->SetInt32("key", i));
    if (i % 2 == 0) {
      ASSERT_OK(row->SetInt32("int_val", i));
    } else {
      ASSERT_OK(row->SetNull("int_val"));
    }
    ASSERT_OK(session->Apply(insert));
    if (i + 1 % 1000 == 0) {
      ASSERT_OK(session->Flush());
    }
  }
  ASSERT_OK(session->Flush());

  // Scan rows and check for correct counts.
  { // IS NULL
    KuduScanner scanner(table.get());
    KuduPredicate *p = table->NewIsNullPredicate("int_val");
    ASSERT_OK(scanner.AddConjunctPredicate(p));
    ASSERT_OK(scanner.Open());
    int count = 0;
    KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      CHECK_OK(scanner.NextBatch(&batch));
      count += batch.NumRows();
    }
    ASSERT_EQ(kNumRows / 2, count);
  }

  { // IS NOT NULL
    KuduScanner scanner(table.get());
    KuduPredicate *p = table->NewIsNotNullPredicate("int_val");
    ASSERT_OK(scanner.AddConjunctPredicate(p));
    ASSERT_OK(scanner.Open());
    int count = 0;
    KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      CHECK_OK(scanner.NextBatch(&batch));
      count += batch.NumRows();
    }
    ASSERT_EQ(kNumRows / 2, count);
  }
}

INSTANTIATE_TEST_SUITE_P(IntColEncodings,
                         IntEncodingNullPredicatesTest,
                         ::testing::Values(kPlain, kBitShuffle, kRunLength));


enum BinaryEncoding {
  kPlainBin,
  kPrefix,
  kDictionary
};

class BinaryEncodingNullPredicatesTest : public ClientTest,
                                         public ::testing::WithParamInterface<BinaryEncoding> {
};

TEST_P(BinaryEncodingNullPredicatesTest, TestBinaryEncodings) {
  // Create table with appropriate encoded, nullable column.
  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  auto string_col = b.AddColumn("string_val")->Type(KuduColumnSchema::STRING);
  BinaryEncoding enc = GetParam();
  switch (enc) {
    case kPlainBin:
      string_col->Encoding(KuduColumnStorageAttributes::PLAIN_ENCODING);
      break;
    case kPrefix:
      string_col->Encoding(KuduColumnStorageAttributes::PREFIX_ENCODING);
      break;
    case kDictionary:
      string_col->Encoding(KuduColumnStorageAttributes::DICT_ENCODING);
      break;
  }
  ASSERT_OK(b.Build(&schema));

  string table_name = "BinaryEncodingNullPredicatesTestTable";
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(table_name)
                .schema(&schema)
                .num_replicas(1)
                .set_range_partition_columns({ "key" })
                .Create());

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(table_name, &table));

  // Insert rows.
  shared_ptr<KuduSession> session = table->client()->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(10000);
  const size_t kNumRows = AllowSlowTests() ? 10000 : 1000;
  for (int i = 0; i < kNumRows; i++) {
    KuduInsert* insert = table->NewInsert();
    KuduPartialRow* row = insert->mutable_row();
    ASSERT_OK(row->SetInt32("key", i));
    if (i % 2 == 0) {
      ASSERT_OK(row->SetString("string_val", Substitute("taco %d", i % 25)));
    } else {
      ASSERT_OK(row->SetNull("string_val"));
    }
    ASSERT_OK(session->Apply(insert));
    if (i + 1 % 1000 == 0) {
      ASSERT_OK(session->Flush());
    }
  }
  ASSERT_OK(session->Flush());

  // Scan rows and check for correct counts.
  { // IS NULL
    KuduScanner scanner(table.get());
    KuduPredicate *p = table->NewIsNullPredicate("string_val");
    ASSERT_OK(scanner.AddConjunctPredicate(p));
    ASSERT_OK(scanner.Open());
    int count = 0;
    KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      CHECK_OK(scanner.NextBatch(&batch));
      count += batch.NumRows();
    }
    ASSERT_EQ(kNumRows / 2, count);
  }

  { // IS NOT NULL
    KuduScanner scanner(table.get());
    KuduPredicate *p = table->NewIsNotNullPredicate("string_val");
    ASSERT_OK(scanner.AddConjunctPredicate(p));
    ASSERT_OK(scanner.Open());
    int count = 0;
    KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      CHECK_OK(scanner.NextBatch(&batch));
      count += batch.NumRows();
    }
    ASSERT_EQ(kNumRows / 2, count);
  }
}

INSTANTIATE_TEST_SUITE_P(BinaryColEncodings,
                         BinaryEncodingNullPredicatesTest,
                         ::testing::Values(kPlainBin, kPrefix, kDictionary));

TEST_F(ClientTest, TestClonePredicates) {
  NO_FATALS(InsertTestRows(client_table_.get(), 2, 0));
  unique_ptr<KuduPredicate> predicate(client_table_->NewComparisonPredicate(
      "key",
      KuduPredicate::EQUAL,
      KuduValue::FromInt(1)));

  unique_ptr<KuduScanner> scanner(new KuduScanner(client_table_.get()));
  ASSERT_OK(scanner->AddConjunctPredicate(predicate->Clone()));
  ASSERT_OK(scanner->Open());

  int count = 0;
  KuduScanBatch batch;
  while (scanner->HasMoreRows()) {
    ASSERT_OK(scanner->NextBatch(&batch));
    count += batch.NumRows();
  }

  ASSERT_EQ(count, 1);

  scanner.reset(new KuduScanner(client_table_.get()));
  ASSERT_OK(scanner->AddConjunctPredicate(predicate->Clone()));
  ASSERT_OK(scanner->Open());

  count = 0;
  while (scanner->HasMoreRows()) {
    ASSERT_OK(scanner->NextBatch(&batch));
    count += batch.NumRows();
  }

  ASSERT_EQ(count, 1);
}

// Test that scanners will retry after receiving ERROR_SERVER_TOO_BUSY from an
// overloaded tablet server. Regression test for KUDU-1079.
TEST_F(ClientTest, TestServerTooBusyRetry) {
#ifdef THREAD_SANITIZER
  const int kNumRows = 10000;
#else
  const int kNumRows = 100000;
#endif
  NO_FATALS(InsertTestRows(client_table_.get(), kNumRows));

  // Introduce latency in each scan to increase the likelihood of
  // ERROR_SERVER_TOO_BUSY.
#ifdef THREAD_SANITIZER
  FLAGS_scanner_inject_latency_on_each_batch_ms = 100;
#else
  FLAGS_scanner_inject_latency_on_each_batch_ms = 10;
#endif

  // Reduce the service queue length of each tablet server in order to increase
  // the likelihood of ERROR_SERVER_TOO_BUSY.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    MiniTabletServer* ts = cluster_->mini_tablet_server(i);
    ts->options()->rpc_opts.service_queue_length = 1;
    ts->Shutdown();
    ASSERT_OK(ts->Restart());
    ASSERT_OK(ts->WaitStarted());
  }

  bool stop = false;
  vector<thread> threads;
  while (!stop) {
    threads.emplace_back([this]() {
      this->CheckRowCount(this->client_table_.get(), kNumRows);
    });

    // Don't start threads too fast - otherwise we could accumulate tens or hundreds
    // of threads before any of them starts their actual scans, and then it would
    // take a long time to join on them all eventually finishing down below.
    SleepFor(MonoDelta::FromMilliseconds(100));

    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      scoped_refptr<Counter> counter = METRIC_rpcs_queue_overflow.Instantiate(
          cluster_->mini_tablet_server(i)->server()->metric_entity());
      stop = counter->value() > 0;
    }
  }

  for (auto& t : threads) {
    t.join();
  }
}

TEST_F(ClientTest, TestLastErrorEmbeddedInScanTimeoutStatus) {
  // For the random() calls that take place during scan retries.
  SeedRandom();

  NO_FATALS(InsertTestRows(client_table_.get(), FLAGS_test_scan_num_rows));

  for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
    MiniTabletServer* ts = cluster_->mini_tablet_server(i);
    ts->Shutdown();
  }

  // Revert the latency injection flags at the end so the test exits faster.
  google::FlagSaver saver;

  // Restart, but inject latency so that startup is very slow.
  FLAGS_log_inject_latency = true;
  FLAGS_log_inject_latency_ms_mean = 3000;
  FLAGS_log_inject_latency_ms_stddev = 0;

  for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
    MiniTabletServer* ts = cluster_->mini_tablet_server(i);
    ASSERT_OK(ts->Restart());
  }

  // As the tservers are still starting up, the scan will retry until it
  // times out. The actual error should be embedded in the returned status.
  KuduScanner scan(client_table_.get());
  ASSERT_OK(scan.SetTimeoutMillis(1000));
  Status s = scan.Open();
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Illegal state: Tablet not RUNNING");
}

TEST_F(ClientTest, TestRetriesEmbeddedInScanTimeoutStatus) {
  // For the random() calls that take place during scan retries.
  SeedRandom();

  NO_FATALS(InsertTestRows(client_table_.get(), FLAGS_test_scan_num_rows));

  // Allow creating a scanner but fail all of the read calls.
  FLAGS_scanner_inject_service_unavailable_on_continue_scan = true;

  // The scanner will return a retriable error on read, and we will eventually
  // observe a timeout.
  KuduScanner scanner(client_table_.get());
  ASSERT_OK(scanner.SetTimeoutMillis(1000));
  Status s = scanner.Open();
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "exceeded configured scan timeout of");
  ASSERT_STR_CONTAINS(s.ToString(), "scan attempts");
}

TEST_F(ClientTest, TestNoDefaultPartitioning) {
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    Status s = table_creator->table_name("TestNoDefaultPartitioning").schema(&schema_).Create();

    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "Table partitioning must be specified");
}

TEST_F(ClientTest, TestBatchScanConstIterator) {
  // Check for iterator behavior for an empty batch.
  {
    KuduScanBatch empty_batch;
    ASSERT_EQ(0, empty_batch.NumRows());
    ASSERT_TRUE(empty_batch.begin() == empty_batch.end());
  }

  {
    // Insert a few rows
    const int kRowNum = 2;
    NO_FATALS(InsertTestRows(client_table_.get(), kRowNum));

    KuduScanner scanner(client_table_.get());
    ASSERT_OK(scanner.Open());

    // Do a scan
    KuduScanBatch batch;
    ASSERT_TRUE(scanner.HasMoreRows());
    ASSERT_OK(scanner.NextBatch(&batch));
    const int ref_count(batch.NumRows());
    ASSERT_EQ(kRowNum, ref_count);

    {
      KuduScanBatch::const_iterator it_next = batch.begin();
      std::advance(it_next, 1);

      KuduScanBatch::const_iterator it = batch.begin();
      ASSERT_TRUE(++it == it_next);
      ASSERT_TRUE(it == it_next);
    }

    {
      KuduScanBatch::const_iterator it_end = batch.begin();
      std::advance(it_end, kRowNum);
      ASSERT_TRUE(batch.end() == it_end);
    }

    {
      KuduScanBatch::const_iterator it(batch.begin());
      ASSERT_TRUE(it++ == batch.begin());

      KuduScanBatch::const_iterator it_next(batch.begin());
      ASSERT_TRUE(++it_next == it);
    }

    // Check the prefix increment iterator.
    {
      int count = 0;
      for (KuduScanBatch::const_iterator it = batch.begin();
           it != batch.end(); ++it) {
          ++count;
      }
      CHECK_EQ(ref_count, count);
    }

    // Check the postfix increment iterator.
    {
      int count = 0;
      for (KuduScanBatch::const_iterator it = batch.begin();
           it != batch.end(); it++) {
          ++count;
      }
      CHECK_EQ(ref_count, count);
    }

    // Check access to row via indirection operator *
    // and member access through pointer operator ->
    {
      KuduScanBatch::const_iterator it(batch.begin());
      ASSERT_TRUE(it != batch.end());
      int32_t x = 0, y = 0;
      ASSERT_OK((*it).GetInt32(0, &x));
      ASSERT_OK(it->GetInt32(0, &y));
      ASSERT_EQ(x, y);
    }

    {
      KuduScanBatch::const_iterator it_pre(batch.begin());
      KuduScanBatch::const_iterator it_post(batch.begin());
      for (; it_pre != batch.end(); ++it_pre, it_post++) {
          ASSERT_TRUE(it_pre == it_post);
          ASSERT_FALSE(it_pre != it_post);
      }
    }

    // Check that iterators which are going over different batches
    // are different, even if they iterate over the same raw data.
    {
      KuduScanner other_scanner(client_table_.get());
      ASSERT_OK(other_scanner.Open());

      KuduScanBatch other_batch;
      ASSERT_TRUE(other_scanner.HasMoreRows());
      ASSERT_OK(other_scanner.NextBatch(&other_batch));
      const int other_ref_count(other_batch.NumRows());
      ASSERT_EQ(kRowNum, other_ref_count);

      KuduScanBatch::const_iterator it(batch.begin());
      KuduScanBatch::const_iterator other_it(other_batch.begin());
      for (; it != batch.end(); ++it, ++other_it) {
          ASSERT_FALSE(it == other_it);
          ASSERT_TRUE(it != other_it);
      }
    }
  }
}

TEST_F(ClientTest, TestTableNumReplicas) {
  for (int i : { 1, 3, 5, 7 }) {
    shared_ptr<KuduTable> table;
    ASSERT_OK(CreateTable(Substitute("table_with_$0_replicas", i),
                          i, {}, {}, &table));
    ASSERT_EQ(i, table->num_replicas());
  }
}

// Tests that KuduClient::GetTablet() returns the same tablet information as
// KuduScanToken::tablet().
TEST_F(ClientTest, TestGetTablet) {
  KuduScanTokenBuilder builder(client_table_.get());
  vector<KuduScanToken*> tokens;
  ElementDeleter deleter(&tokens);
  ASSERT_OK(builder.Build(&tokens));

  ASSERT_EQ(2, tokens.size());
  for (const auto* t : tokens) {
    const KuduTablet& tablet = t->tablet();
    ASSERT_EQ(1, tablet.replicas().size());
    const KuduReplica* replica = tablet.replicas()[0];
    ASSERT_TRUE(replica->is_leader());
    const MiniTabletServer* ts = cluster_->mini_tablet_server(0);
    ASSERT_EQ(ts->server()->instance_pb().permanent_uuid(),
        replica->ts().uuid());
    ASSERT_EQ(ts->bound_rpc_addr().host(), replica->ts().hostname());
    ASSERT_EQ(ts->bound_rpc_addr().port(), replica->ts().port());

    unique_ptr<KuduTablet> tablet_copy;
    {
      KuduTablet* ptr;
      ASSERT_OK(client_->GetTablet(tablet.id(), &ptr));
      tablet_copy.reset(ptr);
    }
    ASSERT_EQ(tablet.id(), tablet_copy->id());
    ASSERT_EQ(1, tablet_copy->replicas().size());
    const KuduReplica* replica_copy = tablet_copy->replicas()[0];

    ASSERT_EQ(replica->is_leader(), replica_copy->is_leader());
    ASSERT_EQ(replica->ts().uuid(), replica_copy->ts().uuid());
    ASSERT_EQ(replica->ts().hostname(), replica_copy->ts().hostname());
    ASSERT_EQ(replica->ts().port(), replica_copy->ts().port());
  }
}

TEST_F(ClientTest, TestErrorCollector) {
    shared_ptr<KuduSession> session(client_->NewSession());
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

    NO_FATALS(InsertTestRows(client_table_.get(), session.get(), 1, 0));
    ASSERT_OK(session->Flush());

    // Set the maximum size limit too low even for a single error
    // and make sure the error collector reports overflow and drops the error.
    {
      ASSERT_OK(session->SetErrorBufferSpace(1));
      // Trying to insert a duplicate row.
      NO_FATALS(InsertTestRows(client_table_.get(), session.get(), 1, 0));
      // Expecting an error since tried to insert a duplicate key.
      ASSERT_TRUE((session->Flush()).IsIOError());

      // It's impossible to update the error buffer size because
      // the error collector's buffer is overflown and one error has been dropped.
      EXPECT_TRUE(session->SetErrorBufferSpace(0).IsIllegalState());

      vector<KuduError*> errors;
      ElementDeleter drop(&errors);
      bool overflowed;
      session->GetPendingErrors(&errors, &overflowed);
      EXPECT_TRUE(errors.empty());
      EXPECT_TRUE(overflowed);
    }

    // After calling the GetPendingErrors() and retrieving the errors, it's
    // possible to update the limit on the error buffer size. Besides, the error
    // collector should be able to accomodate the duplicate row error
    // if the error fits the buffer.
    {
      ASSERT_OK(session->SetErrorBufferSpace(1024));
      NO_FATALS(InsertTestRows(client_table_.get(), session.get(), 1, 0));
      // Expecting an error: tried to insert a duplicate key.
      ASSERT_TRUE((session->Flush()).IsIOError());

      // It's impossible to update the error buffer size if the error collector
      // would become overflown.
      EXPECT_TRUE(session->SetErrorBufferSpace(1).IsIllegalState());

      // It's OK to update the error buffer size because the new limit is high
      // enough and the error collector hasn't dropped a single error yet.
      EXPECT_OK(session->SetErrorBufferSpace(2048));

      vector<KuduError*> errors;
      ElementDeleter drop(&errors);
      bool overflowed;
      session->GetPendingErrors(&errors, &overflowed);
      EXPECT_EQ(1, errors.size());
      EXPECT_FALSE(overflowed);
    }
}

// Test that, when connecting to an old-version master which doesn't support
// the 'ConnectToCluster' RPC, we still fall back to the old 'GetMasterRegistration'
// RPC.
TEST_F(ClientTest, TestConnectToClusterCompatibility) {
  FLAGS_master_support_connect_to_master_rpc = false;

  const auto& ent = cluster_->mini_master()->master()->metric_entity();
  const auto& metric = METRIC_handler_latency_kudu_master_MasterService_GetMasterRegistration
      .Instantiate(ent);
  int initial_val = metric->TotalCount();

  // Reconnect the client. Since we disabled 'ConnectToCluster', the client should
  // fall back to using GetMasterRegistration instead.
  ASSERT_OK(KuduClientBuilder()
            .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
            .Build(&client_));
  int final_val = metric->TotalCount();
  ASSERT_EQ(initial_val + 1, final_val);
}

// Test that, when the client connects to a cluster, that it gets the relevant
// certificate authority and authentication token from the master. Also checks that
// the data can be exported and re-imported into a new client.
TEST_F(ClientTest, TestGetSecurityInfoFromMaster) {
  // Client is already connected when the test starts.
  ASSERT_TRUE(client_->data_->messenger_->authn_token());
  ASSERT_EQ(1, client_->data_->messenger_->tls_context().trusted_cert_count_for_tests());

  string authn_creds;
  ASSERT_OK(client_->ExportAuthenticationCredentials(&authn_creds));

  // Creating a new client by importing the authentication data should result in the
  // having the same token.
  shared_ptr<KuduClient> new_client;
  ASSERT_OK(KuduClientBuilder()
            .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
            .import_authentication_credentials(authn_creds)
            .Build(&new_client));
  ASSERT_EQ(client_->data_->messenger_->authn_token()->ShortDebugString(),
            new_client->data_->messenger_->authn_token()->ShortDebugString());

  // The new client should yield the same authentication data as the original.
  string new_authn_creds;
  ASSERT_OK(new_client->ExportAuthenticationCredentials(&new_authn_creds));
  ASSERT_EQ(authn_creds, new_authn_creds);
}

struct ServiceUnavailableRetryParams {
  MonoDelta usurper_sleep;
  MonoDelta client_timeout;
  function<bool(const Status&)> status_check;
};

class ServiceUnavailableRetryClientTest :
    public ClientTest,
    public ::testing::WithParamInterface<ServiceUnavailableRetryParams> {
};

// Test the client retries on ServiceUnavailable errors. This scenario uses
// 'CreateTable' RPC to verify the retry behavior.
TEST_P(ServiceUnavailableRetryClientTest, CreateTable) {
  // This scenario is designed to run on a single-master cluster.
  ASSERT_EQ(1, cluster_->num_masters());

  const ServiceUnavailableRetryParams& param(GetParam());

  CountDownLatch start_synchronizer(1);
  // Usurp catalog manager's leader lock for some time to block
  // the catalog manager from performing its duties when becoming a leader.
  // Keeping the catalog manager off its duties results in ServiceUnavailable
  // error response for 'CreateTable' RPC.
  thread usurper([&]() {
      std::lock_guard<RWMutex> leader_lock_guard(
            cluster_->mini_master()->master()->catalog_manager()->leader_lock_);
      start_synchronizer.CountDown();
      SleepFor(param.usurper_sleep);
    });

  // Wait for the lock to be acquired by the usurper thread to make sure
  // the following CreateTable call will get ServiceUnavailable response
  // while the lock is held.
  start_synchronizer.Wait();

  // Start issuing 'CreateTable' RPC when the catalog manager is still
  // unavailable to serve the incoming requests.
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  const Status s = table_creator->table_name("service-unavailable-retry")
      .schema(&schema_)
      .num_replicas(1)
      .set_range_partition_columns({ "key" })
      .timeout(param.client_timeout)
      .Create();
  EXPECT_TRUE(param.status_check(s)) << s.ToString();

  // Wait for the usurper thread to release the lock, otherwise there
  // would be a problem with destroying a locked mutex upon catalog manager
  // shutdown.
  usurper.join();
}

static const ServiceUnavailableRetryParams service_unavailable_retry_cases[] = {
  // Under the hood, there should be multiple retries. However, they should
  // fail as timed out because the catalog manager responds with
  // ServiceUnavailable error to all calls: the leader lock is still held
  // by the usurping thread.
  {
    MonoDelta::FromSeconds(2),        // usurper_sleep
    MonoDelta::FromMilliseconds(500), // client_timeout
    &Status::IsTimedOut,              // status_check
  },

  // After some time the usurper thread should release the lock and the catalog
  // manager should succeed. I.e., the client should succeed if retrying the
  // request for long enough time.
  {
    MonoDelta::FromSeconds(1),        // usurper_sleep
    MonoDelta::FromSeconds(60),       // client_timeout
    &Status::ok,                      // status_check
  },
};

INSTANTIATE_TEST_SUITE_P(
    , ServiceUnavailableRetryClientTest,
    ::testing::ValuesIn(service_unavailable_retry_cases));

TEST_F(ClientTest, TestPartitioner) {
  // Create a table with the following 9 partitions:
  //
  //             hash bucket
  //   key     0      1     2
  //         -----------------
  //  <3333    x      x     x
  // 3333-6666 x      x     x
  //  >=6666   x      x     x
  int num_ranges = 3;
  const int kNumHashPartitions = 3;
  const char* kTableName = "TestPartitioner";

  vector<unique_ptr<KuduPartialRow>> split_rows;
  for (int32_t split : {3333, 6666}) {
    unique_ptr<KuduPartialRow> row(schema_.NewRow());
    ASSERT_OK(row->SetInt32("key", split));
    split_rows.emplace_back(std::move(row));
  }

  shared_ptr<KuduTable> table;
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  table_creator->table_name(kTableName)
      .schema(&schema_)
      .num_replicas(1)
      .add_hash_partitions({ "key" }, kNumHashPartitions)
      .set_range_partition_columns({ "key" });

  for (const auto& row : split_rows) {
    table_creator->add_range_partition_split(new KuduPartialRow(*row));
  }
  ASSERT_OK(table_creator->Create());
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  // Build a partitioner on the table.
  unique_ptr<KuduPartitioner> part;
  {
    KuduPartitioner* part_raw;
    ASSERT_OK(KuduPartitionerBuilder(table)
              .Build(&part_raw));
    part.reset(part_raw);
  }

  ASSERT_EQ(num_ranges * kNumHashPartitions, part->NumPartitions());

  // Partition a bunch of rows, counting how many fall into each partition.
  unique_ptr<KuduPartialRow> row(table->schema().NewRow());
  vector<int> counts_by_partition(part->NumPartitions());
  const int kNumRowsToPartition = 10000;
  for (int i = 0; i < kNumRowsToPartition; i++) {
    ASSERT_OK(row->SetInt32(0, i));
    int part_index;
    ASSERT_OK(part->PartitionRow(*row, &part_index));
    counts_by_partition.at(part_index)++;
  }

  // We don't expect a completely even division of rows into partitions, but
  // we should be within 10% of that.
  int expected_per_partition = kNumRowsToPartition / part->NumPartitions();
  int fuzziness = expected_per_partition / 10;
  for (int i = 0; i < part->NumPartitions(); i++) {
    ASSERT_NEAR(counts_by_partition[i], expected_per_partition, fuzziness);
  }

  // Drop the first and third range partition.
  unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
  alterer->DropRangePartition(schema_.NewRow(), new KuduPartialRow(*split_rows[0]));
  alterer->DropRangePartition(new KuduPartialRow(*split_rows[1]), schema_.NewRow());
  ASSERT_OK(alterer->Alter());

  // The existing partitioner should still return results based on the table
  // state at the time it was created, and successfully return partitions
  // for rows in the now-dropped range.
  ASSERT_EQ(num_ranges * kNumHashPartitions, part->NumPartitions());
  ASSERT_OK(row->SetInt32(0, 1000));
  int part_index;
  ASSERT_OK(part->PartitionRow(*row, &part_index));
  ASSERT_GE(part_index, 0);

  // If we recreate the partitioner, it should get the new partitioning info.
  {
    KuduPartitioner* part_raw;
    ASSERT_OK(KuduPartitionerBuilder(table)
              .Build(&part_raw));
    part.reset(part_raw);
  }
  num_ranges = 1;
  ASSERT_EQ(num_ranges * kNumHashPartitions, part->NumPartitions());

  // ... and it should return -1 for non-covered ranges.
  ASSERT_OK(row->SetInt32(0, 1000));
  ASSERT_OK(part->PartitionRow(*row, &part_index));
  ASSERT_EQ(-1, part_index);
  ASSERT_OK(row->SetInt32(0, 8000));
  ASSERT_OK(part->PartitionRow(*row, &part_index));
  ASSERT_EQ(-1, part_index);
}

TEST_F(ClientTest, TestInvalidPartitionerBuilder) {
  KuduPartitioner* part;
  Status s = KuduPartitionerBuilder(client_table_)
      .SetBuildTimeout(MonoDelta())
      ->Build(&part);
  ASSERT_EQ("Invalid argument: uninitialized timeout", s.ToString());

  s = KuduPartitionerBuilder(sp::shared_ptr<KuduTable>())
      .Build(&part);
  ASSERT_EQ("Invalid argument: null table", s.ToString());
}

// Test that, log verbose level can be set through environment varialble
// and it reflects to the FLAGS_v.
TEST_F(ClientTest, TestVerboseLevelByEnvVar) {
  FLAGS_v = 0;
  setenv(kVerboseEnvVar, "5", 1); // 1 = overwrite if variable already exists.
  SetVerboseLevelFromEnvVar();
  ASSERT_EQ(5, FLAGS_v);

  // negative values are to be ignored.
  FLAGS_v = 0;
  setenv(kVerboseEnvVar, "-1", 1);
  SetVerboseLevelFromEnvVar();
  ASSERT_EQ(0, FLAGS_v);

  // non-parsable values are to be ignored.
  FLAGS_v = 0;
  setenv(kVerboseEnvVar, "abc", 1);
  SetVerboseLevelFromEnvVar();
  ASSERT_EQ(0, FLAGS_v);
}

// Regression test for KUDU-2167: older versions of Kudu could return a scan
// response without a 'data' field, crashing the client.
TEST_F(ClientTest, TestSubsequentScanRequestReturnsNoData) {
  // Insert some rows.
  NO_FATALS(InsertTestRows(client_table_.get(), FLAGS_test_scan_num_rows));

  // Set up a table scan.
  KuduScanner scanner(client_table_.get());
  ASSERT_OK(scanner.SetProjectedColumnNames({ "key" }));

  // Ensure that the new scan RPC does not return the data.
  //
  // It's OK to leave the scanner configured like this; after the new scan RPC
  // the server will still return at least one block of data per RPC.
  ASSERT_OK(scanner.SetBatchSizeBytes(0));

  // This scan should not match any of the inserted rows.
  unique_ptr<KuduPartialRow> row(client_table_->schema().NewRow());
  ASSERT_OK(row->SetInt32("key", -1));
  ASSERT_OK(scanner.AddExclusiveUpperBound(*row));

  // Perform the scan.
  ASSERT_OK(scanner.Open());
  ASSERT_TRUE(scanner.HasMoreRows());
  int count = 0;
  KuduScanBatch batch;
  while (scanner.HasMoreRows()) {
    ASSERT_OK(scanner.NextBatch(&batch));
    count += batch.NumRows();
  }
  ASSERT_EQ(0, count);
}

// Test that the 'real user' included in AuthenticationCredentialsPB is used
// when the client connects to remote servers with SASL PLAIN.
TEST_F(ClientTest, TestAuthenticationCredentialsRealUser) {
  // Scope down the user ACLs and restart the cluster to have it take effect.
  FLAGS_user_acl = "token-user";
  FLAGS_superuser_acl = "token-user";
  FLAGS_rpc_trace_negotiation = true;
  cluster_->ShutdownNodes(cluster::ClusterNodes::ALL);
  ASSERT_OK(cluster_->StartSync());

  // Try to delete a table without setting the user, which should fail: the
  // coarse, RPC-level authz uses "AuthorizeClient" method for the
  // MasterService::DeleteTable() RPC.
  {
    shared_ptr<KuduClient> c;
    ASSERT_OK(cluster_->CreateClient(nullptr, &c));
    ASSERT_NE(nullptr, c.get());
    // TODO(KUDU-2344): ideally, this should have failed with NotAuthorized
    const auto s = c->DeleteTable(client_table_->name());
    const auto errmsg = s.ToString();
    ASSERT_TRUE(s.IsRemoteError()) << errmsg;
    ASSERT_STR_CONTAINS(
        errmsg, "Not authorized: unauthorized access to method: DeleteTable");
  }

  // Create a new client with the imported user name and smoke test it.
  KuduClientBuilder client_builder;
  string authn_creds;
  AuthenticationCredentialsPB pb;
  pb.set_real_user("token-user");
  ASSERT_TRUE(pb.SerializeToString(&authn_creds));
  client_builder.import_authentication_credentials(authn_creds);

  // Recreate the client and open the table.
  ASSERT_OK(cluster_->CreateClient(&client_builder, &client_));
  ASSERT_OK(client_->OpenTable(client_table_->name(), &client_table_));

  // Insert some rows and do a scan to force a new connection to the tablet servers.
  NO_FATALS(InsertTestRows(client_table_.get(), FLAGS_test_scan_num_rows));
  vector<string> rows;
  KuduScanner scanner(client_table_.get());
  ASSERT_OK(ScanToStrings(&scanner, &rows));
}

// Test that clients that aren't authenticated as the appropriate user will be
// unable to hijack a specific scanner ID.
TEST_F(ClientTest, TestBlockScannerHijackingAttempts) {
  const string kUser = "token-user";
  const string kBadGuy = "bad-guy";
  const string table_name = client_table_->name();
  FLAGS_user_acl = Substitute("$0,$1", kUser, kBadGuy);
  cluster_->ShutdownNodes(cluster::ClusterNodes::ALL);
  ASSERT_OK(cluster_->StartSync());

  // Insert some rows to the table.
  NO_FATALS(InsertTestRows(client_table_.get(), FLAGS_test_scan_num_rows));

  // First authenticate as a user and create a scanner for the existing table.
  const auto get_table_as_user = [&] (const string& user, shared_ptr<KuduTable>* table) {
    KuduClientBuilder client_builder;
    string authn_creds;
    AuthenticationCredentialsPB pb;
    pb.set_real_user(user);
    ASSERT_TRUE(pb.SerializeToString(&authn_creds));
    client_builder.import_authentication_credentials(authn_creds);

    // Create the client and table for the user.
    shared_ptr<KuduClient> user_client;
    ASSERT_OK(cluster_->CreateClient(&client_builder, &user_client));
    ASSERT_OK(user_client->OpenTable(table_name, table));
  };

  shared_ptr<KuduTable> user_table;
  shared_ptr<KuduTable> bad_guy_table;
  NO_FATALS(get_table_as_user(kUser, &user_table));
  NO_FATALS(get_table_as_user(kBadGuy, &bad_guy_table));

  // Test both fault-tolerant scanners and non-fault-tolerant scanners.
  for (bool fault_tolerance : { true, false }) {
    // Scan the table as the user to get a scanner ID, and set up a malicious
    // scanner that will try to hijack that scanner ID. Set an initial batch
    // size of 0 so the calls to Open() don't buffer any rows.
    KuduScanner user_scanner(user_table.get());
    ASSERT_OK(user_scanner.SetBatchSizeBytes(0));
    KuduScanner bad_guy_scanner(bad_guy_table.get());
    ASSERT_OK(bad_guy_scanner.SetBatchSizeBytes(0));
    if (fault_tolerance) {
      ASSERT_OK(user_scanner.SetFaultTolerant());
      ASSERT_OK(bad_guy_scanner.SetFaultTolerant());
    }
    ASSERT_OK(user_scanner.Open());
    ASSERT_OK(bad_guy_scanner.Open());
    const string scanner_id = user_scanner.data_->next_req_.scanner_id();
    ASSERT_FALSE(scanner_id.empty());

    // Now attempt to get that scanner id as a different user.
    LOG(INFO) << Substitute("Attempting to extract data from $0 scan $1 as $2",
        fault_tolerance ? "fault-tolerant" : "non-fault-tolerant", scanner_id, kBadGuy);
    bad_guy_scanner.data_->next_req_.set_scanner_id(scanner_id);
    bad_guy_scanner.data_->last_response_.set_has_more_results(true);
    KuduScanBatch batch;
    Status s = bad_guy_scanner.NextBatch(&batch);
    ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "Not authorized");
    ASSERT_EQ(0, batch.NumRows());
  }
}

// Basic functionality test for the client's authz token cache.
TEST_F(ClientTest, TestCacheAuthzTokens) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const string& table_id = client_table_->id();
  KuduClient::Data* data = client_->data_;
  // The client should have already gotten a token when it opened the table.
  SignedTokenPB first_token;
  ASSERT_TRUE(data->FetchCachedAuthzToken(table_id, &first_token));

  // Retrieving a token from the master should overwrite what's in the cache.
  // Wait some time to ensure the new token will be different than the one
  // already in the cache (different expiration).
  SleepFor(MonoDelta::FromSeconds(3));
  SignedTokenPB new_token;
  ASSERT_OK(data->RetrieveAuthzToken(client_table_.get(), MonoTime::Now() + kTimeout));
  ASSERT_TRUE(data->FetchCachedAuthzToken(table_id, &new_token));
  ASSERT_FALSE(MessageDifferencer::Equals(first_token, new_token));

  // Now store the token directly into the cache of a new client.
  shared_ptr<KuduClient> new_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &new_client));
  KuduClient::Data* new_data = new_client->data_;
  SignedTokenPB cached_token;
  ASSERT_FALSE(new_data->FetchCachedAuthzToken(table_id, &cached_token));
  new_data->StoreAuthzToken(table_id, first_token);

  // Check that we actually stored the token.
  ASSERT_TRUE(new_data->FetchCachedAuthzToken(table_id, &cached_token));
  ASSERT_TRUE(MessageDifferencer::Equals(first_token, cached_token));

  // Storing tokens directly also overwrites existing ones.
  new_data->StoreAuthzToken(table_id, new_token);
  ASSERT_TRUE(new_data->FetchCachedAuthzToken(table_id, &cached_token));
  ASSERT_TRUE(MessageDifferencer::Equals(new_token, cached_token));

  // Sanity check that the operations on this new client didn't affect the
  // tokens of the old client.
  ASSERT_TRUE(data->FetchCachedAuthzToken(table_id, &cached_token));
  ASSERT_TRUE(MessageDifferencer::Equals(new_token, cached_token));
}

// Test to ensure that we don't send calls to retrieve authz tokens when one is
// already in-flight for the same table ID.
TEST_F(ClientTest, TestRetrieveAuthzTokenInParallel) {
  const auto kThreads = base::NumCPUs();
  if (kThreads < 2) {
    LOG(WARNING) << "no sense to run at a single CPU core";
    GTEST_SKIP();
  }
  if (FLAGS_stress_cpu_threads > 0) {
    LOG(WARNING) << "test is not designed to run with --stress_cpu_threads";
    GTEST_SKIP();
  }

  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  vector<Synchronizer> syncs(kThreads);
  vector<thread> threads;
  Barrier barrier(kThreads);
  const auto deadline = MonoTime::Now() + kTimeout;
  for (auto& s : syncs) {
    threads.emplace_back([&] {
      barrier.Wait();
      client_->data_->RetrieveAuthzTokenAsync(
          client_table_.get(), s.AsStatusCallback(), deadline);
    });
  }
  for (int i = 0 ; i < kThreads; i++) {
    syncs[i].Wait();
    threads[i].join();
  }
  SignedTokenPB token;
  ASSERT_TRUE(client_->data_->FetchCachedAuthzToken(client_table_->id(), &token));
  // The authz token retrieval requests shouldn't send one request per table;
  // rather they should group together.
  auto ent = cluster_->mini_master()->master()->metric_entity();
  int num_reqs = METRIC_handler_latency_kudu_master_MasterService_GetTableSchema
      .Instantiate(ent)->TotalCount();
  LOG(INFO) << Substitute("$0 concurrent threads sent $1 RPC(s) to get authz tokens",
                          kThreads, num_reqs);
  ASSERT_LT(num_reqs, kThreads);
}

// This is test verifies that txn_id is properly set in a transactional session
// and its current batcher. This is a unit-level test scenario.
TEST_F(ClientTest, TxnIdOfTransactionalSession) {
  const auto apply_single_insert = [this] (KuduSession* s) {
    ASSERT_OK(s->SetFlushMode(KuduSession::MANUAL_FLUSH));
    unique_ptr<KuduInsert> insert(client_table_->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetInt32("key", 0));
    ASSERT_OK(insert->mutable_row()->SetInt32("int_val", 0));
    ASSERT_OK(s->Apply(insert.release()));
  };

  // Check how relevant member fields are populated in case of
  // non-transactional session.
  {
    KuduSession s(client_);

    const auto& session_data_txn_id = s.data_->txn_id_;
    ASSERT_FALSE(session_data_txn_id.IsValid());

    NO_FATALS(apply_single_insert(&s));

    // Make sure current batcher has txn_id_ set to an non-valid transaction
    // identifier.
    ASSERT_NE(nullptr, s.data_->batcher_.get());
    const auto& batcher_txn_id = s.data_->batcher_->txn_id();
    ASSERT_FALSE(batcher_txn_id.IsValid());
  }

  // Check how relevant member fields are populated in case of
  // transactional session.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    const TxnId kTxnId(0);
    KuduSession s(client_, kTxnId);

    const auto& session_data_txn_id = s.data_->txn_id_;
    ASSERT_TRUE(session_data_txn_id.IsValid());
    const auto& txnId = txn->data_->txn_id_;
    ASSERT_EQ(txnId.value(), session_data_txn_id.value());

    NO_FATALS(apply_single_insert(&s));

    // Make sure current batcher has txn_id_ member properly set.
    ASSERT_NE(nullptr, s.data_->batcher_.get());
    const auto& batcher_txn_id = s.data_->batcher_->txn_id();
    ASSERT_TRUE(batcher_txn_id.IsValid());
    ASSERT_EQ(txnId.value(), batcher_txn_id.value());
  }
}

// This test verifies that rows with column schema violations such as
// unset non-nullable columns (with no default value) are detected at the client
// side while calling Apply() for corresponding write operations. So, that sort
// of schema violations can be detected prior sending an RPC to a tablet server.
TEST_F(ClientTest, WritingRowsWithUnsetNonNullableColumns) {
  // Make sure if all non-nullable columns (without defaults) are set for an
  // insert operation, the operation should be successfully applied and flushed.
  {
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    unique_ptr<KuduInsert> op(client_table_->NewInsert());
    auto* row = op->mutable_row();
    ASSERT_OK(row->SetInt32("key", 0));
    // Set the non-nullable column without default.
    ASSERT_OK(row->SetInt32("int_val", 1));
    // Even if the non-nullable column with default 'non_null_with_default'
    // is not set, apply (and underlying Flush()) should succeed.
    ASSERT_OK(session->Apply(op.release()));
  }

  // Of course, update write operations do not have to have all non-nullable
  // columns specified.
  {
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    unique_ptr<KuduUpdate> op(client_table_->NewUpdate());
    auto* row = op->mutable_row();
    ASSERT_OK(row->SetInt32("key", 0));
    ASSERT_OK(row->SetInt32("non_null_with_default", 1));
    ASSERT_OK(session->Apply(op.release()));
  }

  // Make sure if a non-nullable column (without defaults) is not set for an
  // insert operation, the operation cannot be applied.
  {
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    unique_ptr<KuduInsert> op(client_table_->NewInsert());
    auto* row = op->mutable_row();
    ASSERT_OK(row->SetInt32("key", 1));
    // The non-nullable column with default 'non_null_with_default' is set.
    ASSERT_OK(row->SetInt32("non_null_with_default", 1));
    // The non-nullable column 'int_val' without default is not set, so
    // Apply() should fail.
    const auto apply_status = session->Apply(op.release());
    ASSERT_TRUE(apply_status.IsIllegalState()) << apply_status.ToString();
    ASSERT_STR_CONTAINS(apply_status.ToString(),
                        "non-nullable column 'int_val' is not set");
    // Flush() should return an error.
    const auto flush_status = session->Flush();
    ASSERT_TRUE(flush_status.IsIOError()) << flush_status.ToString();
    ASSERT_STR_CONTAINS(flush_status.ToString(), "failed to flush data");
  }

  // Make sure if a non-nullable column (without defaults) is not set for an
  // upsert operation, the operation cannot be applied.
  {
    shared_ptr<KuduSession> session(client_->NewSession());
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    unique_ptr<KuduUpsert> op(client_table_->NewUpsert());
    auto* row = op->mutable_row();
    ASSERT_OK(row->SetInt32("key", 1));
    // The non-nullable column 'int_val' without default is not set, so
    // Apply() should fail.
    const auto apply_status = session->Apply(op.release());
    ASSERT_TRUE(apply_status.IsIllegalState()) << apply_status.ToString();
    ASSERT_STR_CONTAINS(apply_status.ToString(),
                        "non-nullable column 'int_val' is not set");
    // Of course, Flush() should fail as well.
    const auto flush_status = session->Flush();
    ASSERT_TRUE(flush_status.IsIOError()) << flush_status.ToString();
    ASSERT_STR_CONTAINS(flush_status.ToString(), "failed to flush data");
  }

  // Do delete a row, only the key is necessary.
  {
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    unique_ptr<KuduDelete> op(client_table_->NewDelete());
    auto* row = op->mutable_row();
    ASSERT_OK(row->SetInt32("key", 0));
    ASSERT_OK(session->Apply(op.release()));
  }
}

TEST_F(ClientTest, TestClientLocationNoLocationMappingCmd) {
  ASSERT_TRUE(client_->location().empty());
}

// Check basic operations of the transaction-related API.
TEST_F(ClientTest, TxnBasicOperations) {
  // KuduClient::NewTransaction() populates the output parameter on success.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    ASSERT_NE(nullptr, txn.get());

    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_NE(nullptr, session.get());
    ASSERT_EQ(0, session->CountPendingErrors());
    ASSERT_OK(session->Close());
  }

  // Multiple Rollback() calls are OK.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    ASSERT_OK(txn->Rollback());
    ASSERT_OK(txn->Rollback());
  }

  // It's impossible to rollback a transaction that has finalized
  // its commit phase.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    ASSERT_OK(txn->Commit());
    auto cs = Status::Incomplete("other than Status::OK() initial status");
    bool is_complete = false;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &cs));
    ASSERT_OK(cs);
    ASSERT_TRUE(is_complete);
    auto s = txn->Rollback();
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  }

  // It's impossible to commit a transaction that's being rolled back.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    ASSERT_OK(txn->Rollback());
    auto s = txn->StartCommit();
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "is not open: state: ABORT");
  }

  // Insert rows in a transactional session, then rollback the transaction
  // and make sure the rows are gone.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    NO_FATALS(InsertTestRows(client_table_.get(), session.get(), 10));
    ASSERT_OK(txn->Rollback());
    ASSERT_EQ(0, CountRowsFromClient(client_table_.get()));
  }

  // Insert rows in a transactional session, then commit the transaction
  // and make sure the expected rows are there.
  {
    constexpr auto kRowsNum = 123;
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    NO_FATALS(InsertTestRows(client_table_.get(), session.get(), kRowsNum));
    ASSERT_OK(txn->Commit());
    ASSERT_EQ(kRowsNum, CountRowsFromClient(
        client_table_.get(), KuduScanner::READ_YOUR_WRITES));
    ASSERT_EQ(0, session->CountPendingErrors());
  }
}

// Verify the basic functionality of the KuduTransaction::Commit() and
// KuduTransaction::IsCommitComplete() methods.
TEST_F(ClientTest, TxnCommit) {
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    bool is_complete = true;
    Status cs;
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &cs));
    ASSERT_FALSE(is_complete);
    ASSERT_TRUE(cs.IsIllegalState()) << cs.ToString();
    ASSERT_STR_CONTAINS(cs.ToString(), "transaction is still open");

    ASSERT_OK(txn->Rollback());
    // We need to ASSERT_EVENTUALLY here to allow the abort tasks to complete.
    // Until then, we may not consider the transaction as complete.
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &cs));
    ASSERT_TRUE(cs.IsAborted()) << cs.ToString();
    ASSERT_EVENTUALLY([&] {
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &cs));
      ASSERT_TRUE(is_complete);
      ASSERT_TRUE(cs.IsAborted()) << cs.ToString();
      ASSERT_STR_CONTAINS(cs.ToString(), "transaction has been aborted");
    });
  }

  {
    string txn_token;
    {
      shared_ptr<KuduTransaction> txn;
      ASSERT_OK(client_->NewTransaction(&txn));
      ASSERT_OK(txn->Commit());
      bool is_complete = false;
      Status cs;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &cs));
      ASSERT_TRUE(is_complete);
      ASSERT_OK(cs);
      ASSERT_OK(txn->Serialize(&txn_token));
    }

    // Make sure the transaction isn't aborted once its KuduTransaction handle
    // goes out of scope.
    shared_ptr<KuduTransaction> serdes_txn;
    ASSERT_OK(KuduTransaction::Deserialize(client_, txn_token, &serdes_txn));
    bool is_complete = false;
    Status cs;
    ASSERT_OK(serdes_txn->IsCommitComplete(&is_complete, &cs));
    ASSERT_TRUE(is_complete);
    ASSERT_OK(cs);
  }

  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    ASSERT_OK(txn->StartCommit());
    ASSERT_EVENTUALLY([&] {
      bool is_complete = false;
      Status cs;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &cs));
      ASSERT_TRUE(is_complete);
      ASSERT_OK(cs);
    });
  }

  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    ASSERT_OK(txn->Commit());
    bool is_complete = false;
    Status cs = Status::Incomplete("other than Status::OK() initial status");
    ASSERT_OK(txn->IsCommitComplete(&is_complete, &cs));
    ASSERT_TRUE(is_complete);
    ASSERT_OK(cs);
  }
}

// Make sure transactional sessions are automatically flushed upon committing
// the corresponding transaction.
TEST_F(ClientTest, FlushTxnSessionsOnCommit) {
  constexpr auto kNumRows = 64;
  auto rows_inserted = 0;

  for (auto mode : { KuduSession::AUTO_FLUSH_BACKGROUND,
                     KuduSession::AUTO_FLUSH_SYNC,
                     KuduSession::MANUAL_FLUSH, }) {
    SCOPED_TRACE(Substitute("session flush mode $0", FlushModeToString(mode)));
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_NE(nullptr, session.get());
    ASSERT_OK(session->SetFlushMode(mode));

    NO_FATALS(InsertTestRows(
        client_table_.get(), session.get(), kNumRows, rows_inserted));
    rows_inserted += kNumRows;

    if (mode == KuduSession::MANUAL_FLUSH) {
      // In AUTO_FLUSH_SYNC there will be no pending operations since ops are
      // flushed upon KuduSession::Apply(). Due to scheduling anomalies,
      // in AUTO_FLUSH_BACKGROUND there might be a case when the background
      // flushing thread flushes the accumulated operations if the main test
      // thread is put off CPU for a long enough time: to avoid flakiness,
      // check for the pending operations only in case of MANUAL_FLUSH.
      ASSERT_TRUE(session->HasPendingOperations());
    }
    ASSERT_OK(txn->Commit());
    ASSERT_FALSE(session->HasPendingOperations());
    ASSERT_EQ(0, session->CountPendingErrors());
    // Just in case, check how it works when closing the session explicitly
    // after it's been automatically flushed by committing its transaction.
    ASSERT_OK(session->Close());

    const auto row_count = CountRowsFromClient(client_table_.get(),
                                               KuduClient::LEADER_ONLY,
                                               KuduScanner::READ_YOUR_WRITES);
    ASSERT_EQ(rows_inserted, row_count);
  }

  // Make sure that all the sessions originated from a transaction are flushed
  // automatically upon committing a transaction, even if there are many.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));

    vector<shared_ptr<KuduSession>> sessions;
    for (auto i = 0; i < 10; ++i) {
      shared_ptr<KuduSession> session;
      ASSERT_OK(txn->CreateSession(&session));
      ASSERT_NE(nullptr, session.get());
      ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

      NO_FATALS(InsertTestRows(
          client_table_.get(), session.get(), kNumRows, rows_inserted));
      rows_inserted += kNumRows;
      ASSERT_TRUE(session->HasPendingOperations());

      sessions.emplace_back(std::move(session));
    }

    ASSERT_OK(txn->Commit());
    for (const auto& session : sessions) {
      ASSERT_FALSE(session->HasPendingOperations());
      ASSERT_EQ(0, session->CountPendingErrors());
    }

    const auto row_count = CountRowsFromClient(client_table_.get(),
                                               KuduClient::LEADER_ONLY,
                                               KuduScanner::READ_YOUR_WRITES);
    ASSERT_EQ(rows_inserted, row_count);
  }

  // A sub-scenario where the handle of a non-flushed session has gone
  // out of the scope when its originating transaction starts committing.
  // This is scenario is added to be explicit on what happens in such a case.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    {
      shared_ptr<KuduSession> session;
      ASSERT_OK(txn->CreateSession(&session));
      ASSERT_NE(nullptr, session.get());
      ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
      NO_FATALS(InsertTestRows(
          client_table_.get(), session.get(), kNumRows, rows_inserted));
      rows_inserted += kNumRows;
      ASSERT_TRUE(session->HasPendingOperations());
    }
    ASSERT_OK(txn->Commit());
    const auto row_count = CountRowsFromClient(client_table_.get(),
                                               KuduClient::LEADER_ONLY,
                                               KuduScanner::READ_YOUR_WRITES);
    // The rows inserted above should be there even if the session handle went
    // out of the scope.
    ASSERT_EQ(rows_inserted, row_count);
  }
}

// Make sure it's possible to retry committing a transaction with rows from
// multiple sessions, even if one session failed to flush its rows on the first
// commit attempt. Since the first attempt to commit failed due to an error
// while flushing a session's pending operations, the transaction is still open.
// It should be still possible to insert more rows and commit those rows
// originated from already existing transactional sessions.
TEST_F(ClientTest, TxnRetryCommitAfterSessionFlushErrors) {
  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));

  shared_ptr<KuduSession> session;
  ASSERT_OK(txn->CreateSession(&session));
  ASSERT_NE(nullptr, session.get());
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  NO_FATALS(InsertTestRows(client_table_.get(), session.get(), 1));
  auto rows_inserted = 1;

  // Try to insert a row with the same key. With a duplicate row, at attempt
  // to flush this session should fail.
  NO_FATALS(InsertTestRows(client_table_.get(), session.get(), 1));
  ASSERT_TRUE(session->HasPendingOperations());

  // Attempt to commit the transaction.
  const auto s = txn->Commit();
  const auto errmsg = s.ToString();
  ASSERT_TRUE(s.IsIOError()) << errmsg;
  ASSERT_STR_MATCHES(errmsg, "failed to flush data");

  ASSERT_FALSE(session->HasPendingOperations());
  ASSERT_EQ(1, session->CountPendingErrors());
  {
    vector<KuduError*> errors;
    ElementDeleter drop(&errors);
    session->GetPendingErrors(&errors, nullptr);
    ASSERT_EQ(1, errors.size());
    EXPECT_TRUE(errors[0]->status().IsAlreadyPresent());
  }

  // Nothing is committed yet.
  auto row_count = CountRowsFromClient(client_table_.get(),
                                       KuduClient::LEADER_ONLY,
                                       KuduScanner::READ_YOUR_WRITES);
  ASSERT_EQ(0, row_count);

  // However, it's not possible to start another transactional session once
  // KuduTransaction::{Commit,StartCommit}() has already been called.
  {
    shared_ptr<KuduSession> null_session;
    const auto s = txn->CreateSession(&null_session);
    ASSERT_EQ(nullptr, null_session.get());
  }

  // Retrying the commit with one row from the first session and extra rows
  // from the new session. Since the first attempt to commit failed due
  // to an error while flushing first session's pending operations,
  // the transaction is still open. It should be still possible to insert more
  // rows and commit those along with the rows successfully flushed in the
  // context of the first session.
  NO_FATALS(InsertTestRows(client_table_.get(), session.get(), 10, 1));
  rows_inserted += 10;
  ASSERT_OK(txn->Commit());

  // There should be no pending operations for either session. All pending
  // errors have been handled for the first session, no new ones should appear.
  ASSERT_FALSE(session->HasPendingOperations());
  ASSERT_EQ(0, session->CountPendingErrors());

  row_count = CountRowsFromClient(client_table_.get(),
                                  KuduClient::LEADER_ONLY,
                                  KuduScanner::READ_YOUR_WRITES);
  ASSERT_EQ(rows_inserted, row_count);
}

// Make sure KuduTransaction::StartCommit() succeeds when called on
// a transaction handle which has all of its transactional sessions flushed.
TEST_F(ClientTest, StartCommitWithFlushedTxnSessions) {
  constexpr auto kNumRows = 1024;
  auto rows_inserted = 0;

  for (auto mode : { KuduSession::AUTO_FLUSH_BACKGROUND,
                     KuduSession::AUTO_FLUSH_SYNC,
                     KuduSession::MANUAL_FLUSH, }) {
    SCOPED_TRACE(Substitute("session flush mode $0", FlushModeToString(mode)));
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_NE(nullptr, session.get());
    ASSERT_OK(session->SetFlushMode(mode));

    NO_FATALS(InsertTestRows(
        client_table_.get(), session.get(), kNumRows, rows_inserted));
    rows_inserted += kNumRows;

    ASSERT_OK(session->Flush());
    ASSERT_FALSE(session->HasPendingOperations());
    ASSERT_EQ(0, session->CountPendingErrors());
    ASSERT_OK(txn->StartCommit());

    // Wait for the transaction to finalize its commit phase.
    ASSERT_EVENTUALLY([&] {
      bool is_complete = false;
      Status commit_status;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &commit_status));
      ASSERT_OK(commit_status);
      ASSERT_TRUE(is_complete);
    });

    const auto row_count = CountRowsFromClient(client_table_.get(),
                                               KuduClient::LEADER_ONLY,
                                               KuduScanner::READ_YOUR_WRITES);
    ASSERT_EQ(rows_inserted, row_count);
  }

  // A sub-scenario where the handle of an already flushed session has gone
  // out of the scope when its originating transaction starts committing.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    {
      shared_ptr<KuduSession> session;
      ASSERT_OK(txn->CreateSession(&session));
      ASSERT_NE(nullptr, session.get());
      ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
      NO_FATALS(InsertTestRows(
          client_table_.get(), session.get(), kNumRows, rows_inserted));
      rows_inserted += kNumRows;
      ASSERT_OK(session->Flush());
      ASSERT_FALSE(session->HasPendingOperations());
    }
    ASSERT_OK(txn->StartCommit());

    // Wait for the transaction to finalize its commit phase.
    ASSERT_EVENTUALLY([&] {
      bool is_complete = false;
      Status commit_status;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &commit_status));
      ASSERT_OK(commit_status);
      ASSERT_TRUE(is_complete);
    });

    const auto row_count = CountRowsFromClient(client_table_.get(),
                                               KuduClient::LEADER_ONLY,
                                               KuduScanner::READ_YOUR_WRITES);
    ASSERT_EQ(rows_inserted, row_count);
  }
}

// Check the behavior of KuduTransaction::StartCommit() when there are
// transactional non-flushed sessions started off a transaction handle.
TEST_F(ClientTest, TxnNonFlushedSessionsOnStartCommit) {
  constexpr auto kNumRows = 1024;
  auto rows_inserted = 0;

  // It should not be possible to call KuduTransaction::StartCommit()
  // when there are transactional sessions with pending write operations.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_NE(nullptr, session.get());
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

    NO_FATALS(InsertTestRows(
        client_table_.get(), session.get(), kNumRows, rows_inserted));
    rows_inserted += kNumRows;

    ASSERT_TRUE(session->HasPendingOperations());
    ASSERT_EQ(0, session->CountPendingErrors());
    const auto s = txn->StartCommit();
    const auto errmsg = s.ToString();
    ASSERT_TRUE(s.IsIllegalState()) << errmsg;
    ASSERT_STR_MATCHES(errmsg,
        "cannot start committing transaction: "
        "at least one transactional session has write operations pending");
  }

  // A sub-scenario where the handle of a non-flushed session has gone
  // out of the scope when its originating transaction starts committing.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    {
      shared_ptr<KuduSession> session;
      ASSERT_OK(txn->CreateSession(&session));
      ASSERT_NE(nullptr, session.get());
      ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
      NO_FATALS(InsertTestRows(
          client_table_.get(), session.get(), kNumRows, rows_inserted));
      ASSERT_TRUE(session->HasPendingOperations());
    }
    const auto s = txn->StartCommit();
    const auto errmsg = s.ToString();
    ASSERT_TRUE(s.IsIllegalState()) << errmsg;
    ASSERT_STR_MATCHES(errmsg,
        "cannot start committing transaction: "
        "at least one transactional session has write operations pending");
  }
}

// Verify the behavior of KuduTransaction::CreateSession() when the commit
// process has already been started for the corresponding transaction.
TEST_F(ClientTest, TxnCreateSessionAfterCommit) {
  // An empty transaction case.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    ASSERT_OK(txn->Commit());

    shared_ptr<KuduSession> session;
    const auto s = txn->CreateSession(&session);
    const auto errmsg = s.ToString();
    ASSERT_TRUE(s.IsIllegalState()) << errmsg;
    ASSERT_STR_MATCHES(errmsg, "transaction commit has already started");
    ASSERT_EQ(nullptr, session.get());
  }

  // A non-empty transaction case: a transaction with one empty session.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    {
      shared_ptr<KuduSession> session;
      ASSERT_OK(txn->CreateSession(&session));
      ASSERT_NE(nullptr, session.get());
      ASSERT_OK(txn->Commit());
      ASSERT_FALSE(session->HasPendingOperations());
      ASSERT_EQ(0, session->CountPendingErrors());
    }
    const auto row_count = CountRowsFromClient(client_table_.get(),
                                               KuduClient::LEADER_ONLY,
                                               KuduScanner::READ_YOUR_WRITES);
    ASSERT_EQ(0, row_count);

    // Now attempt to create a new session based off the committed transaction.
    {
      shared_ptr<KuduSession> session;
      const auto s = txn->CreateSession(&session);
      const auto errmsg = s.ToString();
      ASSERT_TRUE(s.IsIllegalState()) << errmsg;
      ASSERT_STR_MATCHES(errmsg, "transaction commit has already started");
      ASSERT_EQ(nullptr, session.get());
    }
  }

  // A non-empty transaction case: a transaction with a session that inserts
  // at least one row in a table.
  {
    constexpr auto kNumRows = 1;

    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_NE(nullptr, session.get());
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    NO_FATALS(InsertTestRows(client_table_.get(), session.get(), kNumRows));
    ASSERT_OK(txn->Commit());

    ASSERT_FALSE(session->HasPendingOperations());
    ASSERT_EQ(0, session->CountPendingErrors());
    const auto row_count = CountRowsFromClient(client_table_.get(),
                                               KuduClient::LEADER_ONLY,
                                               KuduScanner::READ_YOUR_WRITES);
    ASSERT_EQ(kNumRows, row_count);

    // Now try to create a new session based off the committed transaction.
    {
      shared_ptr<KuduSession> session;
      const auto s = txn->CreateSession(&session);
      const auto errmsg = s.ToString();
      ASSERT_TRUE(s.IsIllegalState()) << errmsg;
      ASSERT_STR_MATCHES(errmsg, "transaction commit has already started");
      ASSERT_EQ(nullptr, session.get());
    }
  }
}

// Similar to the TxnCreateSessionAfterCommit scenario above, but calls
// KuduTransaction::StartCommit() instead of KuduTransaction::Commit().
TEST_F(ClientTest, TxnCreateSessionAfterStartCommit) {
  // An empty transaction case.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    ASSERT_OK(txn->StartCommit());

    shared_ptr<KuduSession> session;
    const auto s = txn->CreateSession(&session);
    const auto errmsg = s.ToString();
    ASSERT_TRUE(s.IsIllegalState()) << errmsg;
    ASSERT_STR_MATCHES(errmsg, "transaction commit has already started");
    ASSERT_EQ(nullptr, session.get());
  }

  // A non-empty transaction case: a transaction with one empty session.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    {
      shared_ptr<KuduSession> session;
      ASSERT_OK(txn->CreateSession(&session));
      ASSERT_NE(nullptr, session.get());
      ASSERT_OK(txn->StartCommit());
    }

    // Now attempt to create a new session based off the transaction that
    // has started committing.
    {
      shared_ptr<KuduSession> session;
      const auto s = txn->CreateSession(&session);
      const auto errmsg = s.ToString();
      ASSERT_TRUE(s.IsIllegalState()) << errmsg;
      ASSERT_STR_MATCHES(errmsg, "transaction commit has already started");
      ASSERT_EQ(nullptr, session.get());
    }

    // Wait for the transaction to finalize its commit phase.
    ASSERT_EVENTUALLY([&] {
      bool is_complete = false;
      Status commit_status;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &commit_status));
      ASSERT_OK(commit_status);
      ASSERT_TRUE(is_complete);
    });
    const auto row_count = CountRowsFromClient(client_table_.get(),
                                               KuduClient::LEADER_ONLY,
                                               KuduScanner::READ_YOUR_WRITES);
    ASSERT_EQ(0, row_count);
  }

  // A non-empty transaction case: a transaction with a session that inserts
  // at least one row in a table.
  {
    constexpr auto kNumRows = 1;

    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_NE(nullptr, session.get());
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    NO_FATALS(InsertTestRows(client_table_.get(), session.get(), kNumRows));
    ASSERT_OK(session->Flush());
    ASSERT_FALSE(session->HasPendingOperations());
    ASSERT_OK(txn->StartCommit());

    // Now try to create a new session based off the committed transaction.
    {
      shared_ptr<KuduSession> session;
      const auto s = txn->CreateSession(&session);
      const auto errmsg = s.ToString();
      ASSERT_TRUE(s.IsIllegalState()) << errmsg;
      ASSERT_STR_MATCHES(errmsg, "transaction commit has already started");
      ASSERT_EQ(nullptr, session.get());
    }

    // Wait for the transaction to finalize its commit phase.
    ASSERT_EVENTUALLY([&] {
      bool is_complete = false;
      Status commit_status;
      ASSERT_OK(txn->IsCommitComplete(&is_complete, &commit_status));
      ASSERT_OK(commit_status);
      ASSERT_TRUE(is_complete);
    });
    ASSERT_EQ(0, session->CountPendingErrors());
    const auto row_count = CountRowsFromClient(client_table_.get(),
                                               KuduClient::LEADER_ONLY,
                                               KuduScanner::READ_YOUR_WRITES);
    ASSERT_EQ(kNumRows, row_count);
  }
}

// A test scenario to verify the behavior of the client API when a write
// operation submitted into a transaction session after the transaction
// has been committed.
TEST_F(ClientTest, SubmitWriteOpAfterTxnCommit) {
  constexpr auto kNumRows = 1024;
  auto rows_inserted = 0;
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    shared_ptr<KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_NE(nullptr, session.get());
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

    NO_FATALS(InsertTestRows(
        client_table_.get(), session.get(), kNumRows, rows_inserted));
    rows_inserted += kNumRows;
    ASSERT_TRUE(session->HasPendingOperations());
    ASSERT_OK(txn->Commit());
    ASSERT_FALSE(session->HasPendingOperations());
    ASSERT_EQ(0, session->CountPendingErrors());

    auto row_count = CountRowsFromClient(client_table_.get(),
                                         KuduClient::LEADER_ONLY,
                                         KuduScanner::READ_YOUR_WRITES);
    ASSERT_EQ(kNumRows, row_count);

    // Try to insert one more row.
    NO_FATALS(InsertTestRows(
        client_table_.get(), session.get(), 1, rows_inserted));
    const auto s = session->Flush();
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    {
      vector<KuduError*> errors;
      ElementDeleter drop(&errors);
      session->GetPendingErrors(&errors, nullptr);
      ASSERT_EQ(1, errors.size());
      const auto& s = errors[0]->status();
      const auto errmsg = s.ToString();
      ASSERT_TRUE(s.IsIllegalState()) << errmsg;
      ASSERT_STR_MATCHES(errmsg, "transaction ID .* not open: COMMITTED");
    }

    // Row count stays the same as before, of course.
    row_count = CountRowsFromClient(client_table_.get(),
                                    KuduClient::LEADER_ONLY,
                                    KuduScanner::READ_YOUR_WRITES);
    ASSERT_EQ(kNumRows, row_count);
  }
}

// This test verifies the behavior of KuduTransaction instance when the bound
// KuduClient instance gets out of scope.
TEST_F(ClientTest, TxnHandleLifecycle) {
  shared_ptr<KuduTransaction> txn;
  {
    const auto master_addr = cluster_->mini_master()->bound_rpc_addr().ToString();
    KuduClientBuilder b;
    b.add_master_server_addr(master_addr);
    shared_ptr<KuduClient> c;
    ASSERT_OK(b.Build(&c));
    ASSERT_OK(c->NewTransaction(&txn));
  }
  auto s = txn->Rollback();
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "associated KuduClient is gone");
}

TEST_F(ClientTest, TxnCorruptedToken) {
  vector<pair<string, string>> token_test_cases;
  token_test_cases.emplace_back("an empty string", "");
  {
    TxnTokenPB token;
    string buf;
    ASSERT_TRUE(token.SerializeToString(&buf));
    token_test_cases.emplace_back("empty token", std::move(buf));
  }
  {
    TxnTokenPB token;
    token.set_txn_id(0);
    string buf;
    ASSERT_TRUE(token.SerializeToString(&buf));
    token_test_cases.emplace_back("missing keepalive_millis field",
                                  std::move(buf));
  }
  {
    TxnTokenPB token;
    token.set_keepalive_millis(1000);
    string buf;
    ASSERT_TRUE(token.SerializeToString(&buf));
    token_test_cases.emplace_back("missing txn_id field", std::move(buf));
  }

  for (const auto& description_and_token : token_test_cases) {
    SCOPED_TRACE(description_and_token.first);
    const auto& token = description_and_token.second;
    shared_ptr<KuduTransaction> serdes_txn;
    auto s = KuduTransaction::Deserialize(client_, token, &serdes_txn);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  }
}

// Test scenario to verify serialization/deserialization of transaction tokens.
TEST_F(ClientTest, TxnToken) {
  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));

  const TxnId txn_id = txn->data_->txn_id_;
  ASSERT_TRUE(txn_id.IsValid());
  const uint32_t txn_keepalive_ms = txn->data_->txn_keep_alive_ms_;
  ASSERT_GT(txn_keepalive_ms, 0);

  string txn_token;
  ASSERT_OK(txn->Serialize(&txn_token));

  // Serializing the same transaction again produces the same result.
  {
    string token;
    ASSERT_OK(txn->Serialize(&token));
    ASSERT_EQ(txn_token, token);
  }

  // Check the token for consistency.
  {
    TxnTokenPB token;
    ASSERT_TRUE(token.ParseFromString(txn_token));
    ASSERT_TRUE(token.has_txn_id());
    ASSERT_EQ(txn_id.value(), token.txn_id());
    ASSERT_TRUE(token.has_keepalive_millis());
    ASSERT_EQ(txn_keepalive_ms, token.keepalive_millis());
  }

  shared_ptr<KuduTransaction> serdes_txn;
  ASSERT_OK(KuduTransaction::Deserialize(client_, txn_token, &serdes_txn));
  ASSERT_NE(nullptr, serdes_txn.get());
  ASSERT_EQ(txn_id, serdes_txn->data_->txn_id_);
  ASSERT_EQ(txn_keepalive_ms, serdes_txn->data_->txn_keep_alive_ms_);

  // Make sure the KuduTransaction object deserialized from a token is fully
  // functional.
  string serdes_txn_token;
  ASSERT_OK(serdes_txn->Serialize(&serdes_txn_token));
  ASSERT_EQ(txn_token, serdes_txn_token);

  {
    static constexpr auto kNumRows = 10;
    shared_ptr<KuduSession> session;
    ASSERT_OK(serdes_txn->CreateSession(&session));
    NO_FATALS(InsertTestRows(client_table_.get(), session.get(), kNumRows));
    ASSERT_OK(serdes_txn->StartCommit());

    // The state of a transaction isn't stored in the token, so initiating
    // commit of the transaction doesn't change the result of the serialization.
    string token;
    ASSERT_OK(serdes_txn->Serialize(&token));
    ASSERT_EQ(serdes_txn_token, token);
  }

  // A new transaction should produce in a new, different token.
  shared_ptr<KuduTransaction> other_txn;
  ASSERT_OK(client_->NewTransaction(&other_txn));
  string other_txn_token;
  ASSERT_OK(other_txn->Serialize(&other_txn_token));
  ASSERT_NE(txn_token, other_txn_token);

  // The state of a transaction isn't stored in the token, so aborting
  // the doesn't change the result of the serialization.
  string token;
  ASSERT_OK(other_txn->Rollback());
  ASSERT_OK(other_txn->Serialize(&token));
  ASSERT_EQ(other_txn_token, token);
}

// Begin a transaction under one user, and then try to commit/rollback the
// transaction under different user. The latter should result in
// Status::NotAuthorized() status.
TEST_F(ClientTest, AttemptToControlTxnByOtherUser) {
  static constexpr const char* const kOtherTxnUser = "other-txn-user";
  const KuduTransaction::SerializationOptions kSerOptions;

  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));
  string txn_token;
  ASSERT_OK(txn->Serialize(&txn_token));

  // Transaction identifier is surfacing here only to build the reference error
  // message for Status::NotAuthorized() returned by attempts to perform
  // commit/rollback operations below.
  TxnId txn_id;
  {
    TxnTokenPB token;
    ASSERT_TRUE(token.ParseFromString(txn_token));
    ASSERT_TRUE(token.has_txn_id());
    txn_id = token.txn_id();
  }
  ASSERT_TRUE(txn_id.IsValid());
  const auto ref_msg = Substitute(
      "transaction ID $0 not owned by $1", txn_id.value(), kOtherTxnUser);

  KuduClientBuilder client_builder;
  string authn_creds;
  AuthenticationCredentialsPB pb;
  pb.set_real_user(kOtherTxnUser);
  ASSERT_TRUE(pb.SerializeToString(&authn_creds));
  client_builder.import_authentication_credentials(authn_creds);
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(&client_builder, &client));
  shared_ptr<KuduTransaction> serdes_txn;
  ASSERT_OK(KuduTransaction::Deserialize(client, txn_token, &serdes_txn));
  const vector<pair<string, Status>> txn_ctl_results = {
    { "rollback", serdes_txn->Rollback() },
    { "commit", serdes_txn->StartCommit() },
  };
  for (const auto& op_and_status : txn_ctl_results) {
    SCOPED_TRACE(op_and_status.first);
    const auto& s = op_and_status.second;
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), ref_msg);
  }
}

TEST_F(ClientTest, NoTxnManager) {
  shared_ptr<KuduTransaction> txn;
  KuduClientBuilder builder;
  builder.default_admin_operation_timeout(MonoDelta::FromSeconds(1));
  builder.default_rpc_timeout(MonoDelta::FromMilliseconds(100));
  ASSERT_OK(cluster_->CreateClient(&builder, &client_));
  ASSERT_OK(client_->NewTransaction(&txn));

  // Shutdown all masters: a TxnManager is a part of master, so after shutting
  // down all masters in the cluster there will be no single TxnManager running.
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);

  {
    shared_ptr<KuduTransaction> txn;
    auto s = client_->NewTransaction(&txn);
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  }

  const vector<pair<string, Status>> txn_ctl_results = {
    { "rollback", txn->Rollback() },
    { "commit", txn->StartCommit() },
  };
  for (const auto& op_and_status : txn_ctl_results) {
    SCOPED_TRACE(op_and_status.first);
    const auto& s = op_and_status.second;
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  }
}

class TableKeyRangeTest : public ClientTest {
 public:
   Status BuildSchema() override {
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->Nullable();
    return b.Build(&schema_);
  }

  void SetUp() override {
    ClientTest::SetUp();

    // Set a low flush threshold so we can scan a mix of flushed data in
    // in-memory data.
    FLAGS_flush_threshold_mb = 0;
    FLAGS_flush_threshold_secs = 1;

    // Disable rowset compact to prevent DRSs being merged because they are too small.
    FLAGS_enable_rowset_compaction = false;

    ASSERT_OK(CreateTable(kTableName, 1, {}, GeneratePartialRows(), &client_table_));
  }

  typedef vector<pair<unique_ptr<KuduPartialRow>, unique_ptr<KuduPartialRow>>> KuduPartialRowsVec;
  KuduPartialRowsVec GeneratePartialRows() const {
    KuduPartialRowsVec rows;
    vector<int> keys = { 0, 250, 500, 750 };
    for (int i = 0; i < keys.size(); i++) {
      unique_ptr<KuduPartialRow> lower_bound(schema_.NewRow());
      CHECK_OK(lower_bound->SetInt32("key", keys[i]));
      unique_ptr<KuduPartialRow> upper_bound(schema_.NewRow());
      CHECK_OK(upper_bound->SetInt32("key", keys[i] + 250));
      rows.emplace_back(lower_bound.release(), upper_bound.release());
    }

    return rows;
  }

  static void InsertTestRowsWithStrings(KuduTable* table, KuduSession* session, int num_rows) {
    vector<int> keys = { 0, 250, 500, 750 };
    string str_val = "*";
    int diff_value = 120; // use to create discontinuous data in a tablet
    for (int k = 0; k < keys.size(); k++) {
      for (int i = keys[k]; i < keys[k] + num_rows; i++) {
        unique_ptr<KuduInsert> insert(table->NewInsert());
        ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
        ASSERT_OK(insert->mutable_row()->SetInt32("int_val", i * 2));
        ASSERT_OK(insert->mutable_row()->SetString("string_val", str_val));
        ASSERT_OK(session->Apply(insert.release()));
        ASSERT_OK(session->Flush());
      }
      for (int i = keys[k] + diff_value; i < keys[k] + diff_value + num_rows; i++) {
        unique_ptr<KuduInsert> insert(table->NewInsert());
        ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
        ASSERT_OK(insert->mutable_row()->SetInt32("int_val", i * 2));
        ASSERT_OK(insert->mutable_row()->SetString("string_val", str_val));
        ASSERT_OK(session->Apply(insert.release()));
        ASSERT_OK(session->Flush());
      }
    }
  }

 protected:
  static constexpr const char* const kTableName = "client-testrange";

  shared_ptr<KuduTable> range_table_;
};

TEST_F(TableKeyRangeTest, TestGetTableKeyRange) {
  client::sp::shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  {
    // Create session
    shared_ptr<KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(10000);
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

    // Should have no rows to begin with.
    ASSERT_EQ(0, CountRowsFromClient(table.get()));
    // Insert rows
    NO_FATALS(InsertTestRowsWithStrings(client_table_.get(), session.get(), 100));
    NO_FATALS(CheckNoRpcOverflow());
  }

  {
    // search meta cache by default
    //
    // There are tablet information in the meta cache.
    // We give priority to the data in the cache by default.
    KuduScanTokenBuilder builder(table.get());
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    ASSERT_OK(builder.Build(&tokens));
    ASSERT_EQ(4, tokens.size());

    NO_FATALS(CheckTokensInfo(tokens));
    ASSERT_EQ(800, CountRows(tokens));
  }

  {
    // search meta cache by local
    //
    // If the splitSizeBytes set to 0 , we search the meta cache.
    KuduScanTokenBuilder builder(table.get());
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    // set splitSizeBytes to 0
    builder.SetSplitSizeBytes(0);
    ASSERT_OK(builder.Build(&tokens));
    ASSERT_EQ(4, tokens.size());

    NO_FATALS(CheckTokensInfo(tokens));
    ASSERT_EQ(800, CountRows(tokens));
  }

  uint32_t token_size_a = 0;
  {
    // search from tserver
    KuduScanTokenBuilder builder(table.get());
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    // set splitSizeBytes < tablet's size
    builder.SetSplitSizeBytes(700);
    ASSERT_OK(builder.Build(&tokens));
    token_size_a = tokens.size();
    ASSERT_LT(4, token_size_a);

    NO_FATALS(CheckTokensInfo(tokens));
    ASSERT_EQ(800, CountRows(tokens));
  }

  uint32_t token_size_b = 0;
  {
    // search from tserver
    KuduScanTokenBuilder builder(table.get());
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    // set splitSizeBytes < tablet's size
    builder.SetSplitSizeBytes(20);
    ASSERT_OK(builder.Build(&tokens));
    token_size_b = tokens.size();
    ASSERT_LT(4, token_size_b);

    NO_FATALS(CheckTokensInfo(tokens));
    ASSERT_EQ(800, CountRows(tokens));
  }

  // diffferent splitSizeBytes leads to different token
  ASSERT_NE(token_size_a, token_size_b);

  {
    // search from tserver
    KuduScanTokenBuilder builder(table.get());
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    // set splitSizeBytes > tablet's size
    builder.SetSplitSizeBytes(1024 * 1024 * 1024);
    ASSERT_OK(builder.Build(&tokens));
    ASSERT_EQ(tokens.size(), 4);

    NO_FATALS(CheckTokensInfo(tokens));
    ASSERT_EQ(800, CountRows(tokens));
  }
}
class ClientTxnManagerProxyTest : public ClientTest {
 public:
  void SetUp() override {
    // To avoid extra latency in addition to already injected ones, scenarios
    // based on setup can assume assume the initial txn status tablet is already
    // created.
    FLAGS_txn_manager_lazily_initialized = false;

    // Inject latency into the process of loading txn status data from the
    // backing tablet, so TxnStatusManager would respond with
    // ServiceUnavailable() for some time right after starting up.
    FLAGS_txn_status_manager_inject_latency_load_from_tablet_ms = 3000;

    ClientTest::SetUp();
    ASSERT_OK(cluster_->mini_master()->master()->WaitForTxnManagerInit());
  }
};

// This is a scenario to verify the retry logic in the client: if receiving
// ServiceNotAvailable() error status, it should retry its RPCs to TxnManager
// a bit later.
TEST_F(ClientTxnManagerProxyTest, RetryOnServiceUnavailable) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));
}

TEST_F(ClientTest, TxnKeepAlive) {
  // Begin a transaction and wait for longer than the keepalive interval
  // (with some margin). If there were no txn keepalive messages sent,
  // the transaction would be automatically aborted. Since the client
  // sends keepalive heartbeats under the hood, it's still possible to commit
  // the transaction after some period of inactivity.
  {
    shared_ptr<KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));

    SleepFor(MonoDelta::FromMilliseconds(2 * FLAGS_txn_keepalive_interval_ms));

    ASSERT_OK(txn->Commit());
  }

  // Begin a transaction and move its KuduTransaction object out of the
  // scope. After txn keepalive interval (with some margin) the system should
  // automatically abort the transaction.
  {
    string txn_token;
    {
      shared_ptr<KuduTransaction> txn;
      ASSERT_OK(client_->NewTransaction(&txn));
      ASSERT_OK(txn->Serialize(&txn_token));
    }

    SleepFor(MonoDelta::FromMilliseconds(2 * FLAGS_txn_keepalive_interval_ms));

    // The transaction should be automatically aborted since no keepalive
    // requests were sent once the original KuduTransaction object went out
    // of the scope.
    shared_ptr<KuduTransaction> serdes_txn;
    ASSERT_OK(KuduTransaction::Deserialize(client_, txn_token, &serdes_txn));
    auto s = serdes_txn->StartCommit();
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "not open: state: ABORT");
  }

  // Begin a new transaction and move the KuduTransaction object out of the
  // scope. If the transaction handle is deserialized from a txn token that
  // hadn't keepalive enabled when serialized, the transaction should be
  // automatically aborted after txn keepalive timeout.
  {
    string txn_token;
    {
      shared_ptr<KuduTransaction> txn;
      ASSERT_OK(client_->NewTransaction(&txn));
      ASSERT_OK(txn->Serialize(&txn_token));
    }

    shared_ptr<KuduTransaction> serdes_txn;
    ASSERT_OK(KuduTransaction::Deserialize(client_, txn_token, &serdes_txn));
    ASSERT_NE(nullptr, serdes_txn.get());

    SleepFor(MonoDelta::FromMilliseconds(2 * FLAGS_txn_keepalive_interval_ms));

    auto s = serdes_txn->StartCommit();
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "not open: state: ABORT");
  }

  // Begin a new transaction and move the KuduTransaction object out of the
  // scope. If the transaction handle is deserialized from a txn token that
  // had keepalive enabled when serialized, the transaction should stay alive
  // even if the original KuduTransaction object went out of the scope.
  // It's assumed that no longer than the keepalive timeout interval has passed
  // between the original transaction handle got out of scope and the new
  // one has been created from the token.
  {
    string txn_token;
    {
      shared_ptr<KuduTransaction> txn;
      ASSERT_OK(client_->NewTransaction(&txn));
      KuduTransaction::SerializationOptions options;
      options.enable_keepalive(true);
      ASSERT_OK(txn->Serialize(&txn_token, options));
    }

    shared_ptr<KuduTransaction> serdes_txn;
    ASSERT_OK(KuduTransaction::Deserialize(client_, txn_token, &serdes_txn));

    SleepFor(MonoDelta::FromMilliseconds(2 * FLAGS_txn_keepalive_interval_ms));

    ASSERT_OK(serdes_txn->Commit());
  }
}

// A scenario to explicitly show that long-running transactions are not aborted
// if Kudu masters are not available for periods of time shorter than the txn
// keepalive interval.
TEST_F(ClientTest, TxnKeepAliveAndUnavailableTxnManagerShortTime) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr const auto kUnavailabilityIntervalMs = 5000;
  // Use a short timeout for the RPC so our attempts to commit don't trigger a
  // keepalive failure.
  const auto kShortTimeout = MonoDelta::FromMilliseconds(kUnavailabilityIntervalMs / 2);
  KuduClientBuilder builder;
  builder.default_admin_operation_timeout(kShortTimeout);
  builder.default_rpc_timeout(kShortTimeout);
  ASSERT_OK(cluster_->CreateClient(&builder, &client_));

  // To avoid flakiness, set the txn keepalive interval longer than it is in
  // other scenarios (NOTE: it's still much shorter than it's default value
  // to be used in real life).
  //
  // The cluster is restarted to avoid TSAN warnings on the access to the
  // flag values by the stale txn monitoring thread and the assignments below.
  cluster_->Shutdown();
  FLAGS_txn_keepalive_interval_ms = 3 * kUnavailabilityIntervalMs;
  FLAGS_txn_staleness_tracker_interval_ms = kUnavailabilityIntervalMs / 8;
  ASSERT_OK(cluster_->Start());

  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));

  // Shutdown masters -- they host TxnManager instances which proxy txn-related
  // RPC calls from clients to corresponding TxnStatusManager.
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);

  // An attempt to commit a transaction should fail due to unreachable masters.
  {
    auto s = txn->StartCommit();
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  }

  // Start masters back.
  for (auto idx = 0; idx < cluster_->num_masters(); ++idx) {
    ASSERT_OK(cluster_->mini_master(idx)->Restart());
  }

  // Now, when masters are back and running, the client should be able
  // to commit the transaction. It should not be automatically aborted.
  ASSERT_OK(txn->Commit());
}

// A scenario to explicitly show that long-running transactions are
// aborted if Kudu masters are not available for time period longer than
// the txn keepalive interval.
TEST_F(ClientTest, TxnKeepAliveAndUnavailableTxnManagerLongTime) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Set the timeout to be long so when we fail to commit, the system will
  // automatically abort the transaction.
  KuduClientBuilder builder;
  builder.default_admin_operation_timeout(
      MonoDelta::FromMilliseconds(5 * FLAGS_txn_keepalive_interval_ms));
  builder.default_rpc_timeout(MonoDelta::FromSeconds(1));
  ASSERT_OK(cluster_->CreateClient(&builder, &client_));

  shared_ptr<KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));

  // Shutdown masters -- they host TxnManager instances which proxy txn-related
  // RPC calls from clients to corresponding TxnStatusManager.
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);

  // An attempt to commit a transaction should fail due to unreachable masters.
  {
    auto s = txn->StartCommit();
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  }

  // Start masters back.
  for (auto idx = 0; idx < cluster_->num_masters(); ++idx) {
    ASSERT_OK(cluster_->mini_master(idx)->Restart());
  }

  // Now, when masters are back and running, the client should get the
  // Status::IllegalState() status back when trying to commit the transaction
  // which has been automatically aborted by the system due to not receiving
  // any txn keepalive messages for longer than prescribed by the
  // --txn_keepalive_interval_ms flag.
  {
    auto s = txn->StartCommit();
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "not open: state: ABORT");
  }
}

// Client test that assigns locations to clients and tablet servers.
// For now, assigns a uniform location to all clients and tablet servers.
class ClientWithLocationTest : public ClientTest {
 protected:
  void SetLocationMappingCmd() override {
    const string location_cmd_path = JoinPathSegments(GetTestExecutableDirectory(),
                                                      "testdata/first_argument.sh");
    const string location = "/foo";
    FLAGS_location_mapping_cmd = strings::Substitute("$0 $1",
                                                     location_cmd_path, location);
    FLAGS_location_mapping_by_uuid = true;

    // Some of these tests assume no client activity, so disable the
    // transaction system client.
    FLAGS_enable_txn_system_client_init = false;

    // By default, master doesn't assing locations to connecting clients.
    FLAGS_master_client_location_assignment_enabled = true;
  }
};

TEST_F(ClientWithLocationTest, TestClientLocation) {
  ASSERT_EQ("/foo", client_->location());
}

TEST_F(ClientWithLocationTest, LocationCacheMetricsOnClientConnectToCluster) {
  ASSERT_EQ("/foo", client_->location());

  auto& metric_entity = cluster_->mini_master()->master()->metric_entity();
  scoped_refptr<Counter> counter_hits(
      METRIC_location_mapping_cache_hits.Instantiate(metric_entity));
  const auto hits_before = counter_hits->value();
  ASSERT_EQ(0, hits_before);
  scoped_refptr<Counter> counter_queries(
      METRIC_location_mapping_cache_queries.Instantiate(metric_entity));
  const auto queries_before = counter_queries->value();
  // Expecting location assignment queries from all tablet servers and
  // the client.
  ASSERT_EQ(cluster_->num_tablet_servers() + 1, queries_before);

  static constexpr int kIterNum = 10;
  for (auto iter = 0; iter < kIterNum; ++iter) {
    shared_ptr<KuduClient> client;
    ASSERT_OK(KuduClientBuilder()
        .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
        .Build(&client));
    ASSERT_EQ("/foo", client->location());
  }

  // The location mapping cache should be hit every time a client is connecting
  // from the same host as the former client. Nothing else should be touching
  // the location assignment logic but ConnectToCluster() requests coming from
  // the clients instantiated above.
  const auto queries_after = counter_queries->value();
  ASSERT_EQ(queries_before + kIterNum, queries_after);
  const auto hits_after = counter_hits->value();
  ASSERT_EQ(hits_before + kIterNum, hits_after);
}

// Regression test for KUDU-2980.
TEST_F(ClientTest, TestProjectionPredicatesFuzz) {
  const int kNumColumns = 20;
  const int kNumPKColumns = kNumColumns / 2;
  const char* const kTableName = "test";

  // Create a schema with half of the columns in the primary key.
  //
  // Use a smattering of different physical data types to increase the
  // likelihood of a size transition between primary key components.
  KuduSchemaBuilder b;
  vector<string> pk_col_names;
  vector<string> all_col_names;
  KuduColumnSchema::DataType data_type = KuduColumnSchema::STRING;
  for (int i = 0; i < kNumColumns; i++) {
    string col_name = std::to_string(i);
    b.AddColumn(col_name)->Type(data_type)->NotNull();
    if (i < kNumPKColumns) {
      pk_col_names.emplace_back(col_name);
    }
    all_col_names.emplace_back(std::move(col_name));

    // Rotate to next data type.
    switch (data_type) {
      case KuduColumnSchema::INT8: data_type = KuduColumnSchema::INT16; break;
      case KuduColumnSchema::INT16: data_type = KuduColumnSchema::INT32; break;
      case KuduColumnSchema::INT32: data_type = KuduColumnSchema::INT64; break;
      case KuduColumnSchema::INT64: data_type = KuduColumnSchema::STRING; break;
      case KuduColumnSchema::STRING: data_type = KuduColumnSchema::INT8; break;
      default: LOG(FATAL) << "Unexpected data type " << data_type;
    }
  }
  b.SetPrimaryKey(pk_col_names);
  KuduSchema schema;
  ASSERT_OK(b.Build(&schema));

  // Create and open the table. We don't care about replication or partitioning.
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
                          .schema(&schema)
                          .set_range_partition_columns({})
                          .num_replicas(1)
                          .Create());
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  unique_ptr<KuduScanner> scanner;
  scanner.reset(new KuduScanner(table.get()));

  // Pick a random selection of columns to project.
  //
  // Done before inserting data so the projection can be used to determine the
  // expected scan results.
  Random rng(SeedRandom());
  vector<string> projected_col_names =
      SelectRandomSubset<vector<string>, string, Random>(all_col_names, 0, &rng);
  std::mt19937 gen(SeedRandom());
  std::shuffle(projected_col_names.begin(), projected_col_names.end(), gen);
  ASSERT_OK(scanner->SetProjectedColumnNames(projected_col_names));

  // Insert some rows with randomized keys, flushing the tablet periodically.
  const int kNumRows = 20;
  shared_ptr<KuduSession> session = client_->NewSession();
  vector<string> expected_rows;
  for (int i = 0; i < kNumRows; i++) {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    KuduPartialRow* row = insert->mutable_row();
    GenerateDataForRow(schema, &rng, row);

    // Store a copy of the row for later, to compare with the scan results.
    //
    // The copy should look like KuduScanBatch::RowPtr::ToString() and must
    // conform to the projection schema.
    ConstContiguousRow ccr(row->schema(), row->row_data_);
    string row_str = "(";
    row_str += JoinMapped(projected_col_names, [&](const string& col_name) {
        int col_idx = row->schema()->find_column(col_name);
        const auto& col = row->schema()->column(col_idx);
        DCHECK_NE(Schema::kColumnNotFound, col_idx);
        string cell;
        col.DebugCellAppend(ccr.cell(col_idx), &cell);
        return cell;
      }, ", ");
    row_str += ")";
    expected_rows.emplace_back(std::move(row_str));

    ASSERT_OK(session->Apply(insert.release()));

    // Leave one row in the tablet's MRS so that the scan includes one rowset
    // without bounds. This affects the behavior of FT scans.
    if (i < kNumRows - 1 && rng.OneIn(2)) {
      ASSERT_OK(cluster_->FlushTablet(GetFirstTabletId(table.get())));
    }
  }

  // Pick a random selection of columns for predicates.
  //
  // We use NOT NULL predicates so as to tickle the server-side code for dealing
  // with predicates without actually affecting the scan results.
  vector<string> predicate_col_names =
      SelectRandomSubset<vector<string>, string, Random>(all_col_names, 0, &rng);
  for (const auto& col_name : predicate_col_names) {
    unique_ptr<KuduPredicate> pred(table->NewIsNotNullPredicate(col_name));
    ASSERT_OK(scanner->AddConjunctPredicate(pred.release()));
  }

  // Use a fault tolerant scan half the time.
  if (rng.OneIn(2)) {
    ASSERT_OK(scanner->SetFaultTolerant());
  }

  // Perform the scan and verify the results.
  //
  // We ignore result ordering because although FT scans will produce rows
  // sorted by primary keys, regular scans will not.
  vector<string> rows;
  ASSERT_OK(ScanToStrings(scanner.get(), &rows));
  ASSERT_EQ(unordered_set<string>(expected_rows.begin(), expected_rows.end()),
            unordered_set<string>(rows.begin(), rows.end())) << rows;
}

class ClientTestImmutableColumn : public ClientTest,
                                  public ::testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  ClientTestImmutableColumn() {
    init_immu_col_to_null_ = std::get<0>(GetParam());
    update_immu_col_to_null_ = std::get<1>(GetParam());
  }

  Status BuildSchema() override {
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->Nullable();
    b.AddColumn("non_null_with_default")->Type(KuduColumnSchema::INT32)->NotNull()
        ->Default(KuduValue::FromInt(12345));
    b.AddColumn("imm_val")->Type(KuduColumnSchema::INT32)->Immutable()->Nullable();
    return b.Build(&schema_);
  }

  void PopulateImmutableCell(KuduPartialRow* row, int index) const {
    if (init_immu_col_to_null_) {
      ASSERT_OK(row->SetNull(4));
    } else {
      ASSERT_OK(row->SetInt32(4, index * 4));
    }
  }

  void PopulateDefaultRow(KuduPartialRow* row, int index) const override {
    ClientTest::PopulateDefaultRow(row, index);
    NO_FATALS(PopulateImmutableCell(row, index));
  }

  string ExpectImmuColCell(int index) const {
    return init_immu_col_to_null_ ? "NULL" : std::to_string(index*4);
  }

  void DoTestVerifyRows(const shared_ptr<KuduTable>& tbl, int num_rows) const override {
    vector<string> rows;
    KuduScanner scanner(tbl.get());
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(num_rows, rows.size());
    for (int i = 0; i < num_rows; i++) {
      int key = i + 1;
      ASSERT_EQ(
          Substitute("(int32 key=$0, int32 int_val=$1, string string_val=\"hello $2\", "
                     "int32 non_null_with_default=$3, int32 imm_val=$4)",
                     key, key*2, key, key*3, ExpectImmuColCell(key)),
          rows[i]);
    }
  }

 protected:
  bool init_immu_col_to_null_;
  bool update_immu_col_to_null_;
};

INSTANTIATE_TEST_SUITE_P(Params, ClientTestImmutableColumn,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Bool()));

TEST_P(ClientTestImmutableColumn, TestUpdate) {
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  {
    unique_ptr<KuduInsert> insert(BuildTestInsert(client_table_.get(), 1));
    ASSERT_OK(session->Apply(insert.release()));
    NO_FATALS(DoTestVerifyRows(client_table_, 1));
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 0, 0, 0, 0));  // successful_inserts++
  }

  const string expect_row = Substitute(
      "(int32 key=1, int32 int_val=999, string string_val=\"hello world\", "
      "int32 non_null_with_default=999, int32 imm_val=$0)",
      ExpectImmuColCell(1));
  {
    // UPDATE can update row without immutable column set.
    unique_ptr<KuduUpdate> update(client_table_->NewUpdate());
    ASSERT_OK(update->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(update->mutable_row()->SetInt32("int_val", 999));
    ASSERT_OK(update->mutable_row()->SetStringCopy("string_val", "hello world"));
    ASSERT_OK(update->mutable_row()->SetInt32("non_null_with_default", 999));
    ASSERT_OK(session->Apply(update.release()));
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 1, 0, 0, 0));  // successful_updates++

    vector<string> rows;
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(1, rows.size());
    ASSERT_EQ(expect_row, rows[0]);
  }

  {
    // UPDATE results in an error when attempting to update row having at least one column with the
    // immutable attribute set.
    unique_ptr<KuduUpdate> update(client_table_->NewUpdate());
    ASSERT_OK(update->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(update->mutable_row()->SetInt32("int_val", 888));
    ASSERT_OK(update->mutable_row()->SetStringCopy("string_val", "world hello"));
    ASSERT_OK(update->mutable_row()->SetInt32("non_null_with_default", 888));
    if (update_immu_col_to_null_) {
      ASSERT_OK(update->mutable_row()->SetNull("imm_val"));
    } else {
      ASSERT_OK(update->mutable_row()->SetInt32("imm_val", 888));
    }
    Status s = session->Apply(update.release());
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "failed to flush data: error details are available "
                        "via KuduSession::GetPendingErrors()");
    vector<KuduError*> errors;
    ElementDeleter d(&errors);
    bool overflow;
    session->GetPendingErrors(&errors, &overflow);
    ASSERT_FALSE(overflow);
    ASSERT_EQ(1, errors.size());
    ASSERT_STR_CONTAINS(
        errors[0]->status().ToString(),
        "Immutable: UPDATE not allowed for immutable column: imm_val INT32 NULLABLE IMMUTABLE");
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 1, 0, 0, 0));  // nothing changed

    vector<string> rows;
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(1, rows.size());
    ASSERT_EQ(expect_row, rows[0]);
  }
}

TEST_P(ClientTestImmutableColumn, TestUpdateIgnore) {
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  {
    unique_ptr<KuduInsert> insert(BuildTestInsert(client_table_.get(), 1));
    ASSERT_OK(session->Apply(insert.release()));
    NO_FATALS(DoTestVerifyRows(client_table_, 1));
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 0, 0, 0, 0));  // successful_inserts++
  }

  const string expect_row = Substitute(
      "(int32 key=1, int32 int_val=888, string string_val=\"world hello\", "
      "int32 non_null_with_default=888, int32 imm_val=$0)",
      ExpectImmuColCell(1));
  {
    // UPDATE IGNORE can update a row without changing the immutable column cell, the error of
    // updating the immutable column will be ignored.
    unique_ptr<KuduUpdateIgnore> update_ignore(client_table_->NewUpdateIgnore());
    ASSERT_OK(update_ignore->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(update_ignore->mutable_row()->SetInt32("int_val", 888));
    ASSERT_OK(update_ignore->mutable_row()->SetStringCopy("string_val", "world hello"));
    ASSERT_OK(update_ignore->mutable_row()->SetInt32("non_null_with_default", 888));
    if (update_immu_col_to_null_) {
      ASSERT_OK(update_ignore->mutable_row()->SetNull("imm_val"));
    } else {
      ASSERT_OK(update_ignore->mutable_row()->SetInt32("imm_val", 888));
    }
    ASSERT_OK(session->Apply(update_ignore.release()));
    // successful_updates++, update_ignore_errors++,
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 1, 1, 0, 0));

    vector<string> rows;
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(1, rows.size());
    ASSERT_EQ(expect_row, rows[0]);
  }

  {
    // UPDATE IGNORE only on immutable column. Note that this will result in
    // a 'Invalid argument: No fields updated' error.
    unique_ptr<KuduUpdateIgnore> update_ignore(client_table_->NewUpdateIgnore());
    ASSERT_OK(update_ignore->mutable_row()->SetInt32("key", 1));
    if (update_immu_col_to_null_) {
      ASSERT_OK(update_ignore->mutable_row()->SetNull("imm_val"));
    } else {
      ASSERT_OK(update_ignore->mutable_row()->SetInt32("imm_val", 888));
    }
    Status s = session->Apply(update_ignore.release());
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "failed to flush data: error details are available "
                        "via KuduSession::GetPendingErrors()");
    vector<KuduError*> errors;
    ElementDeleter d(&errors);
    bool overflow;
    session->GetPendingErrors(&errors, &overflow);
    ASSERT_FALSE(overflow);
    ASSERT_EQ(1, errors.size());
    ASSERT_STR_CONTAINS(errors[0]->status().ToString(),
                        "Invalid argument: No fields updated, key is: (int32 key=1)");
    NO_FATALS(DoVerifyMetrics(session.get(), 1, 0, 0, 0, 1, 1, 0, 0));  // nothing changed

    vector<string> rows;
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(1, rows.size());
    ASSERT_EQ(expect_row, rows[0]);
  }
}

TEST_P(ClientTestImmutableColumn, TestUpsertIgnore) {
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  {
    // UPSERT IGNORE can insert row.
    unique_ptr<KuduUpsertIgnore> upsert_ignore(BuildTestUpsertIgnore(client_table_.get(), 1));
    ASSERT_OK(session->Apply(upsert_ignore.release()));
    NO_FATALS(DoTestVerifyRows(client_table_, 1));
    NO_FATALS(DoVerifyMetrics(session.get(), 0, 0, 1, 0, 0, 0, 0, 0)); // successful_upserts++
  }

  {
    // UPSERT IGNORE can update row without immutable column set.
    unique_ptr<KuduUpsertIgnore> upsert_ignore(client_table_->NewUpsertIgnore());
    ASSERT_OK(upsert_ignore->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(upsert_ignore->mutable_row()->SetInt32("int_val", 999));
    ASSERT_OK(upsert_ignore->mutable_row()->SetStringCopy("string_val", "hello world"));
    ASSERT_OK(upsert_ignore->mutable_row()->SetInt32("non_null_with_default", 999));
    ASSERT_OK(session->Apply(upsert_ignore.release()));
    NO_FATALS(DoVerifyMetrics(session.get(), 0, 0, 2, 0, 0, 0, 0, 0));  // successful_upserts++

    vector<string> rows;
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(1, rows.size());
    ASSERT_EQ(Substitute("(int32 key=1, int32 int_val=999, string string_val=\"hello world\", "
                         "int32 non_null_with_default=999, int32 imm_val=$0)",
                         ExpectImmuColCell(1)),
              rows[0]);
  }

  {
    // UPSERT IGNORE can update row with immutable column set.
    unique_ptr<KuduUpsertIgnore> upsert_ignore(client_table_->NewUpsertIgnore());
    ASSERT_OK(upsert_ignore->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(upsert_ignore->mutable_row()->SetInt32("int_val", 888));
    ASSERT_OK(upsert_ignore->mutable_row()->SetStringCopy("string_val", "world hello"));
    ASSERT_OK(upsert_ignore->mutable_row()->SetInt32("non_null_with_default", 888));
    if (update_immu_col_to_null_) {
      ASSERT_OK(upsert_ignore->mutable_row()->SetNull("imm_val"));
    } else {
      ASSERT_OK(upsert_ignore->mutable_row()->SetInt32("imm_val", 888));
    }
    ASSERT_OK(session->Apply(upsert_ignore.release()));
    // successful_upserts++, upsert_ignore_errors++
    NO_FATALS(DoVerifyMetrics(session.get(), 0, 0, 3, 1, 0, 0, 0, 0));

    vector<string> rows;
    KuduScanner scanner(client_table_.get());
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(1, rows.size());
    ASSERT_EQ(Substitute("(int32 key=1, int32 int_val=888, string string_val=\"world hello\", "
                         "int32 non_null_with_default=888, int32 imm_val=$0)",
                         ExpectImmuColCell(1)),
              rows[0]);
  }
}

TEST_P(ClientTestImmutableColumn, TestUpsert) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  // Perform and verify UPSERT which acts as an INSERT.
  ASSERT_OK(ApplyUpsertToSession(session.get(), client_table_, 1, 1, "original row", 1));
  NO_FATALS(DoVerifyMetrics(session.get(), 0, 0, 1, 0, 0, 0, 0, 0));  // successful_upserts++

  const string expect_row = R"((int32 key=1, int32 int_val=1, string string_val="original row", )"
                            "int32 non_null_with_default=12345, int32 imm_val=1)";
  {
    vector<string> rows;
    ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ(expect_row, rows[0]);
  }

  // Perform an UPSERT. This upsert will attemp to update an immutable column,
  // which will result an error.
  Status s = ApplyUpsertToSession(session.get(), client_table_, 1, 4, "upserted row 3",
                                  update_immu_col_to_null_ ? nullopt : optional<int>(999));
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "failed to flush data: error details are available "
                      "via KuduSession::GetPendingErrors()");
  vector<KuduError*> errors;
  ElementDeleter d(&errors);
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  ASSERT_FALSE(overflow);
  ASSERT_EQ(1, errors.size());
  ASSERT_STR_CONTAINS(
      errors[0]->status().ToString(),
      "Immutable: UPDATE not allowed for immutable column: imm_val INT32 NULLABLE IMMUTABLE");
  NO_FATALS(DoVerifyMetrics(session.get(), 0, 0, 1, 0, 0, 0, 0, 0));  // nothing changed
  {
    vector<string> rows;
    ASSERT_OK(ScanTableToStrings(client_table_.get(), &rows));
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ(expect_row, rows[0]);  // nothing changed
  }
}

class ClientTestImmutableColumnCompatibility : public ClientTest {
 public:
  void SetUp() override {
    // Disable the immutable column attribute feature in master for testing.
    FLAGS_master_support_immutable_column_attribute = false;

    KuduTest::SetUp();

    // Start minicluster and wait for tablet servers to connect to master.
    InternalMiniClusterOptions options;
    options.num_tablet_servers = 1;
    cluster_.reset(new InternalMiniCluster(env_, std::move(options)));
    ASSERT_OK(cluster_->StartSync());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }
};

TEST_F(ClientTestImmutableColumnCompatibility, CreateTable) {
  const string kTableName = "create_table_with_immutable_attribute_column";
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("imm_val")->Type(KuduColumnSchema::INT32)->Immutable()->Nullable();
  ASSERT_OK(b.Build(&schema_));

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  Status s = table_creator->table_name(kTableName)
                .schema(&schema_)
                .add_hash_partitions({"key"}, 2)
                .num_replicas(1)
                .Create();
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(
      s.ToString(),
      Substitute("Error creating table $0 on the master: cluster does not support "
                 "CreateTable with feature(s) IMMUTABLE_COLUMN_ATTRIBUTE", kTableName));
}

TEST_F(ClientTestImmutableColumnCompatibility, AlterTableAddColumn) {
  const string kTableName = "alter_table_adding_column_with_immutable_attribute";
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->Nullable();
  ASSERT_OK(b.Build(&schema_));

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
                .schema(&schema_)
                .add_hash_partitions({"key"}, 2)
                .num_replicas(1)
                .Create());

  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  table_alterer->AddColumn("imm_val")->Type(KuduColumnSchema::INT32)->Immutable()->Nullable();
  Status s = table_alterer->Alter();
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(
      s.ToString(),
      "cluster does not support AlterTable with feature(s) IMMUTABLE_COLUMN_ATTRIBUTE");
}

TEST_F(ClientTestImmutableColumnCompatibility, AlterTableAlterColumn) {
  const string kTableName = "alter_table_altering_column_with_immutable_attribute";
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->Nullable();
  ASSERT_OK(b.Build(&schema_));

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
                .schema(&schema_)
                .add_hash_partitions({"key"}, 2)
                .num_replicas(1)
                .Create());

  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  table_alterer->AlterColumn("int_val")->Immutable();
  Status s = table_alterer->Alter();
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(
      s.ToString(),
      "cluster does not support AlterTable with feature(s) IMMUTABLE_COLUMN_ATTRIBUTE");
}

class ClientTestUnixSocket : public ClientTest {
 public:
  void SetUp() override {
    FLAGS_rpc_listen_on_unix_domain_socket = true;
    FLAGS_client_use_unix_domain_sockets = true;
    ClientTest::SetUp();
    ASSERT_OK(BuildSchema());
  }
};

TEST_F(ClientTestUnixSocket, TestConnectViaUnixSocket) {
  static constexpr int kNumRows = 100;
  NO_FATALS(InsertTestRows(client_table_.get(), kNumRows));
  ASSERT_EQ(kNumRows, CountRowsFromClient(client_table_.get()));

  int total_unix_conns = 0;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    auto counter = METRIC_rpc_connections_accepted_unix_domain_socket.Instantiate(
        cluster_->mini_tablet_server(0)->server()->metric_entity());
    total_unix_conns += counter->value();
  }
  ASSERT_EQ(1, total_unix_conns);
}

class MultiTServerClientTest : public ClientTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(BuildSchema());

    // Start minicluster and wait for tablet servers to connect to master.
    InternalMiniClusterOptions options;
    options.num_tablet_servers = 4;
    cluster_.reset(new InternalMiniCluster(env_, std::move(options)));
    ASSERT_OK(cluster_->StartSync());

    // Scenarios of this test might require multiple retries from the client if
    // running on a slow or overloaded machine. The timeout for RPC operations
    // is set higher than the default to avoid false positives.
    KuduClientBuilder builder;
    builder.default_admin_operation_timeout(MonoDelta::FromSeconds(60));
    builder.default_rpc_timeout(MonoDelta::FromSeconds(60));
    ASSERT_OK(cluster_->CreateClient(&builder, &client_));

    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
        .schema(&schema_)
        .add_hash_partitions({ "key" }, 2)
        .num_replicas(3)
        .Create());
    ASSERT_OK(client_->OpenTable(kTableName, &client_table_));
  }
};

// Restart one tablet server in a round-robin fashion with every row written,
// not waiting for the tablet server to be up and running before trying
// to write the next row. Count the number of rows once done. There should be
// no errors: client should retry any operations failed due to tablet server
// restarting. The result row count should match the number of total rows
// written by the client.
TEST_F(MultiTServerClientTest, WriteWhileRestartingMultipleTabletServers) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr const auto read_mode_to_string =
      [](KuduScanner::ReadMode mode) constexpr {
    switch (mode) {
      case KuduScanner::READ_LATEST:
        return "READ_LATEST";
      case KuduScanner::READ_AT_SNAPSHOT:
        return "READ_AT_SNAPSHOT";
      case KuduScanner::READ_YOUR_WRITES:
        return "READ_YOUR_WRITES";
      default:
        return "UNKNOWN";
    }
  };

  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));

  static constexpr auto kNumRows = 32;
  const auto num_servers = cluster_->num_tablet_servers();
  int64_t key = 0;
  for (auto row_idx = 0; row_idx < kNumRows; ++row_idx) {
    NO_FATALS(InsertTestRows(client_table_.get(), session.get(), 1, key++));
    auto* ts = cluster_->mini_tablet_server(row_idx % num_servers);
    ts->Shutdown();
    ASSERT_OK(ts->Restart());
  }
  for (auto mode : {KuduScanner::READ_LATEST, KuduScanner::READ_YOUR_WRITES}) {
    SCOPED_TRACE(Substitute("read mode $0", read_mode_to_string(mode)));
    auto row_count = CountRowsFromClient(client_table_.get(),
                                         KuduClient::LEADER_ONLY,
                                         mode);
    ASSERT_EQ(kNumRows, row_count);
  }
}

// Test changing replication factor.
TEST_F(MultiTServerClientTest, TestSetReplicationFactor) {
  string tablet_id = GetFirstTabletId(client_table_.get());

  scoped_refptr<internal::RemoteTablet> rt;
  client_->data_->meta_cache_->ClearCache();
  ASSERT_OK(MetaCacheLookupById(tablet_id, &rt));
  ASSERT_NE(nullptr, rt);
  vector<internal::RemoteReplica> replicas;
  rt->GetRemoteReplicas(&replicas);
  ASSERT_EQ(3, replicas.size());

  // Set replication factor from 3 to 1.
  unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  table_alterer->data_->set_replication_factor_to_ = 1;
  ASSERT_OK(table_alterer->Alter());
  ASSERT_EVENTUALLY([&] {
    client_->data_->meta_cache_->ClearCache();
    ASSERT_OK(MetaCacheLookupById(tablet_id, &rt));
    ASSERT_NE(nullptr, rt);
    rt->GetRemoteReplicas(&replicas);
    ASSERT_EQ(1, replicas.size());
  });

  // Set replication factor from 1 to 3.
  table_alterer->data_->set_replication_factor_to_ = 3;
  ASSERT_OK(table_alterer->Alter());
  ASSERT_EVENTUALLY([&] {
    client_->data_->meta_cache_->ClearCache();
    ASSERT_OK(MetaCacheLookupById(tablet_id, &rt));
    ASSERT_NE(nullptr, rt);
    rt->GetRemoteReplicas(&replicas);
    ASSERT_EQ(3, replicas.size());
  });
}

class ReplicationFactorLimitsTest : public ClientTest {
 public:
  static constexpr const char* const kTableName = "replication_limits";

  void SetUp() override {
    // Reduce the TS<->Master heartbeat interval to speed up testing.
    FLAGS_heartbeat_interval_ms = 10;

    // Set RF-related flags.
    FLAGS_min_num_replicas = 3;
    FLAGS_max_num_replicas = 5;

    KuduTest::SetUp();
    ASSERT_OK(BuildSchema());

    // Start minicluster and wait for tablet servers to connect to master.
    InternalMiniClusterOptions options;
    options.num_tablet_servers = 7;
    cluster_.reset(new InternalMiniCluster(env_, std::move(options)));
    ASSERT_OK(cluster_->StartSync());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }
};

TEST_F(ReplicationFactorLimitsTest, MinReplicationFactor) {
  // Creating table with number of replicas equal to --min_num_replicas should
  // succeed.
  {
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
        .schema(&schema_)
        .add_hash_partitions({ "key" }, 2)
        .num_replicas(3)
        .Create());
  }

  // An attempt to create a table with replication factor less than
  // the specified by --min_num_replicas should fail.
  for (auto rf : { -1, 0, 1, 2 }) {
    SCOPED_TRACE(Substitute("replication factor $0", rf));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    const auto s = table_creator->table_name(kTableName)
        .schema(&schema_)
        .add_hash_partitions({ "key" }, 2)
        .num_replicas(rf)
        .Create();
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "illegal replication factor");
    ASSERT_STR_CONTAINS(s.ToString(), "minimum allowed replication factor is 3");
  }

  // Test a couple of other cases: even number of replicas when in [min, max]
  // range and an attempt to create a table with number of replicas more than
  // the number of tablet servers currently alive in the cluster.
  {
    FLAGS_min_num_replicas = 1;
    const vector<pair<int, string>> cases = {
      {2, "illegal replication factor 2: replication factor must be odd"},
      {3, "not enough live tablet servers to create a table with the requested "
          "replication factor 3"},
    };

    for (auto i = 1; i < cluster_->num_tablet_servers(); ++i) {
      cluster_->mini_tablet_server(i)->Shutdown();
    }
    // Restart masters so only the alive tablet servers: that's a faster way
    // to update tablet servers' liveliness status in the master's registry.
    for (auto i = 0; i < cluster_->num_masters(); ++i) {
      cluster_->mini_master(i)->Shutdown();
      ASSERT_OK(cluster_->mini_master(i)->Restart());
    }

    SleepFor(MonoDelta::FromMilliseconds(3 * FLAGS_heartbeat_interval_ms));

    for (const auto& c : cases) {
      SCOPED_TRACE(Substitute("num_replicas=$0", c.first));
      unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
      Status s = table_creator->table_name("foobar")
          .schema(&schema_)
          .set_range_partition_columns({ "key" })
          .num_replicas(c.first)
          .Create();
      ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), c.second);
    }
  }
}

TEST_F(ReplicationFactorLimitsTest, MaxReplicationFactor) {
  // Creating table with number of replicas equal to --max_num_replicas should
  // succeed.
  {
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
        .schema(&schema_)
        .add_hash_partitions({ "key" }, 2)
        .num_replicas(5)
        .Create());
  }

  // An attempt to create a table with replication factor greater than
  // the specified by --max_num_replicas should fail.
  for (auto rf : { 6, 7 }) {
    SCOPED_TRACE(Substitute("replication factor $0", rf));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    const auto s = table_creator->table_name(kTableName)
        .schema(&schema_)
        .add_hash_partitions({ "key" }, 2)
        .num_replicas(rf)
        .Create();
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "illegal replication factor");
    ASSERT_STR_CONTAINS(s.ToString(), "maximum allowed replication factor is 5");
  }
}

class ClientTestAutoIncrementingColumn : public ClientTest {
 public:
  void SetUp() override {
    // TODO:achennaka Enable Feature flag here once implemented

    KuduTest::SetUp();

    // Start minicluster and wait for tablet servers to connect to master.
    InternalMiniClusterOptions options;
    options.num_tablet_servers = 3;
    cluster_.reset(new InternalMiniCluster(env_, std::move(options)));
    ASSERT_OK(cluster_->StartSync());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }
};

TEST_F(ClientTestAutoIncrementingColumn, ReadAndWrite) {
  const string kTableName = "table_with_auto_incrementing_column";
  KuduSchemaBuilder b;
  // TODO(Marton): Once the NonUnique column Spec is in place
  // update the column specs below to match the implementation
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("auto_incrementing_id")->Type(KuduColumnSchema::INT64)
      ->NotNull()->AutoIncrementing();
  ASSERT_OK(b.Build(&schema_));

  // Create a table with a couple of range partitions
  int lower_bound = 0;
  int mid_bound = 10;
  int upper_bound = 20;
  unique_ptr<KuduPartialRow> lower0(schema_.NewRow());
  unique_ptr<KuduPartialRow> upper0(schema_.NewRow());
  unique_ptr<KuduPartialRow> lower1(schema_.NewRow());
  unique_ptr<KuduPartialRow> upper1(schema_.NewRow());

  ASSERT_OK(lower0->SetInt32("key", lower_bound));
  ASSERT_OK(upper0->SetInt32("key", mid_bound));
  ASSERT_OK(lower1->SetInt32("key", mid_bound));
  ASSERT_OK(upper1->SetInt32("key", upper_bound));

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
            .schema(&schema_)
            .set_range_partition_columns({"key"})
            .add_range_partition(lower0.release(), upper0.release())
            .add_range_partition(lower1.release(), upper1.release())
            .num_replicas(3)
            .Create());

  // Write into these two partitions without specifying values for
  // auto-incrementing column.
  shared_ptr<KuduSession> session = client_->NewSession();
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  static constexpr auto kNumRows = 20;
  for (int i = 0; i < kNumRows; i++) {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    KuduPartialRow* row = insert->mutable_row();
    ASSERT_OK(row->SetInt32("key", i));
    ASSERT_OK(session->Apply(insert.release()));
  }
  FlushSessionOrDie(session);

  // Read back the rows and confirm the values of auto-incrementing column set
  // correctly for each of the partitions in different scan modes.
  for (const auto mode: {KuduClient::LEADER_ONLY, KuduClient::CLOSEST_REPLICA,
                         KuduClient::FIRST_REPLICA}) {
    vector<string> rows;
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetSelection(mode));
    ASSERT_OK(ScanToStrings(&scanner, &rows));
    ASSERT_EQ(kNumRows, rows.size());
    for (int i = 0; i < rows.size(); i++) {
      ASSERT_EQ(Substitute("(int32 key=$0, int64 auto_incrementing_id=$1)", i,
                           (i % 10) + 1), rows[i]);
    }
  }
}


TEST_F(ClientTestAutoIncrementingColumn, ConcurrentWrites) {
  const string kTableName = "concurrent_writes_auto_incrementing_column";
  KuduSchemaBuilder b;
  // TODO(Marton): Once the NonUnique column Spec is in place
  // update the column specs below to match the implementation
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("auto_incrementing_id")->Type(KuduColumnSchema::INT64)
      ->NotNull()->AutoIncrementing();
  ASSERT_OK(b.Build(&schema_));

  static constexpr int num_clients = 8;
  static constexpr int num_rows_per_client = 1000;

  // Create a table with a single range partition
  static constexpr int lower_bound = 0;
  static constexpr int upper_bound = num_clients * num_rows_per_client;
  unique_ptr<KuduPartialRow> lower(schema_.NewRow());
  unique_ptr<KuduPartialRow> upper(schema_.NewRow());

  ASSERT_OK(lower->SetInt32("key", lower_bound));
  ASSERT_OK(upper->SetInt32("key", upper_bound));

  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
            .schema(&schema_)
            .set_range_partition_columns({"key"})
            .add_range_partition(lower.release(), upper.release())
            .num_replicas(3)
            .Create());

  // Write into the table with eight clients concurrently without specifying values
  // for the auto-incrementing column.
  CountDownLatch client_latch(num_clients);
  vector<thread> threads;
  for (int i = 0; i < num_clients; i++) {
    threads.emplace_back([&, i] {
      shared_ptr<KuduClient> client;
      ASSERT_OK(KuduClientBuilder()
                .add_master_server_addr(cluster_->mini_master()->bound_rpc_addr().ToString())
                .Build(&client));
      shared_ptr<KuduSession> session = client->NewSession();
      shared_ptr<KuduTable> table;
      ASSERT_OK(client->OpenTable(kTableName, &table));
      for (int j = num_rows_per_client * i; j < num_rows_per_client * (i + 1); j++) {
        unique_ptr<KuduInsert> insert(table->NewInsert());
        KuduPartialRow *row = insert->mutable_row();
        ASSERT_OK(row->SetInt32("key", j));
        ASSERT_OK(session->Apply(insert.release()));
      }
      FlushSessionOrDie(session);
      client_latch.CountDown();
    });
  }
  client_latch.Wait();
  for (int i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
  // Read back the rows and confirm the values of auto-incrementing column set
  // correctly for each of the partitions in different scan modes.
  static constexpr auto kNumRows = num_clients * num_rows_per_client;
  vector<vector<string>> rows;
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  for (const auto mode: {KuduClient::LEADER_ONLY, KuduClient::CLOSEST_REPLICA,
                         KuduClient::FIRST_REPLICA}) {
    vector<string> row;
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetSelection(mode));
    ASSERT_OK(ScanToStrings(&scanner, &row));
    ASSERT_EQ(kNumRows, row.size());
    rows.push_back(row);
  }
  for (int i = 0; i < 2; i++) {
    SCOPED_TRACE(i);
    ASSERT_TRUE(rows[i] == rows[i + 1]);
  }
}

TEST_F(ClientTestAutoIncrementingColumn, Negatives) {
  // TODO(Marton): Once the NonUnique column Spec is in place
  // update the column specs below to match the implementation
  {
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("auto_incrementing_id")->Type(KuduColumnSchema::INT32)
        ->NotNull()->AutoIncrementing();
    Status s = b.Build(&schema_);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_EQ("Invalid argument: auto-incrementing column should be of type INT64", s.ToString());
  }

  {
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("auto_incrementing_id")->Type(KuduColumnSchema::INT64)
        ->Nullable()->AutoIncrementing();
    Status s = b.Build(&schema_);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_EQ("Invalid argument: auto-incrementing column should not be nullable", s.ToString());
  }

  {
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("auto_incrementing_id")->Type(KuduColumnSchema::INT64)
        ->NotNull()->Default(KuduValue::FromInt(20))->AutoIncrementing();
    Status s = b.Build(&schema_);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_EQ("Invalid argument: auto-incrementing column cannot have a "
                            "default value", s.ToString());
  }

  {
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("auto_incrementing_id")->Type(KuduColumnSchema::INT64)
        ->NotNull()->Immutable()->AutoIncrementing();
    Status s = b.Build(&schema_);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_EQ("Invalid argument: auto-incrementing column should not be immutable", s.ToString());
  }
}
} // namespace client
} // namespace kudu
