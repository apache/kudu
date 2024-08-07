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

#include "kudu/tserver/tablet_server-test-base.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server_options.h"
#include "kudu/tserver/tablet_server_test_util.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_graph.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(rpc_timeout, 1000, "Timeout for RPC calls, in seconds");
DECLARE_bool(enable_maintenance_manager);
DECLARE_int32(heartbeat_rpc_timeout_ms);
DECLARE_string(test_server_key);
DECLARE_string(test_server_key_iv);
DECLARE_string(test_server_key_version);

METRIC_DEFINE_entity(test);

using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::RpcController;
using std::nullopt;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tserver {

const char* TabletServerTestBase::kTableId = "TestTable";
const char* TabletServerTestBase::kTabletId = "ffffffffffffffffffffffffffffffff";

TabletServerTestBase::TabletServerTestBase()
    : schema_(GetSimpleTestSchema()),
      ts_test_metric_entity_(METRIC_ENTITY_test.Instantiate(
                                 &ts_test_metric_registry_, "ts_server-test")) {
  // Disable the maintenance ops manager since we want to trigger our own
  // maintenance operations at predetermined times.
  FLAGS_enable_maintenance_manager = false;

  // Decrease heartbeat timeout: we keep re-trying heartbeats when a
  // single master server fails due to a network error. Decreasing
  // the heartbeat timeout to 1 second speeds up unit tests which
  // purposefully specify non-running Master servers.
  FLAGS_heartbeat_rpc_timeout_ms = 1000;

  GetEncryptionKey(nullptr,
                   nullptr,
                   &FLAGS_test_server_key,
                   &FLAGS_test_server_key_iv,
                   &FLAGS_test_server_key_version);
}

// Starts the tablet server, override to start it later.
void TabletServerTestBase::SetUp() {
  KuduTest::SetUp();

  key_schema_ = schema_.CreateKeyProjection();
  rb_.reset(new RowBuilder(&schema_));

  rpc::MessengerBuilder bld("Client");
  ASSERT_OK(bld.Build(&client_messenger_));
}

void TabletServerTestBase::StartTabletServer(int num_data_dirs) {
  CHECK(!mini_server_);

  // Start server with an invalid master address, so it never successfully
  // heartbeats, even if there happens to be a master running on this machine.
  mini_server_.reset(new MiniTabletServer(GetTestPath("TabletServerTest-fsroot"),
                                          HostPort("127.0.0.1", 0), num_data_dirs));
  mini_server_->options()->master_addresses.clear();
  mini_server_->options()->master_addresses.emplace_back("255.255.255.255", 1);
  ASSERT_OK(mini_server_->Start());

  // Set up a tablet inside the server.
  const Schema &schema_with_ids = SchemaBuilder(schema_).Build();
  PartitionSchema partition_schema;
  CHECK_OK(PartitionSchema::FromPB(partition_schema_pb_,
                                   schema_with_ids,
                                   &partition_schema));
  ASSERT_OK(mini_server_->AddTestTablet(kTableId, kTabletId, schema_, nullopt, partition_schema));
  ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId,
                                                                     &tablet_replica_));

  // Creating a tablet is async, we wait here instead of having to handle errors later.
  ASSERT_OK(WaitForTabletRunning(kTabletId));

  // Connect to it.
  ResetClientProxies();
}

Status TabletServerTestBase::WaitForTabletRunning(const char *tablet_id, bool check_consensus) {
  scoped_refptr<tablet::TabletReplica> tablet_replica;
  const auto* tablet_manager = mini_server_->server()->tablet_manager();
  const auto kTimeout = MonoDelta::FromSeconds(10);
  RETURN_NOT_OK(tablet_manager->GetTabletReplica(tablet_id, &tablet_replica));
  const auto s = tablet_replica->WaitUntilConsensusRunning(kTimeout);
  if (check_consensus) {
    RETURN_NOT_OK(s);
    RETURN_NOT_OK(
      tablet_replica->consensus()->WaitUntilLeader(kTimeout));

    // KUDU-2463: Even though the tablet thinks its leader, for correctness, it
    // must wait to finish replicating its no-op (even as a single replica)
    // before being available to scans.
    MonoTime deadline = MonoTime::Now() + kTimeout;
    while (!tablet_replica->tablet()->mvcc_manager()->CheckIsCleanTimeInitialized().ok()) {
      if (MonoTime::Now() >= deadline) {
        return Status::TimedOut("mvcc did not advance safe time within timeout");
      }
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
  }

  // KUDU-2444: Even though the tablet replica is fully running, the tablet
  // manager may regard it as still transitioning to the running state.
  return tablet_manager->WaitForNoTransitionsForTests(MonoDelta::FromSeconds(10));
}

void TabletServerTestBase::UpdateTestRowRemote(int32_t row_idx,
                                               int32_t new_val,
                                               TimeSeries* ts) {
  WriteRequestPB req;
  req.set_tablet_id(kTabletId);
  ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));

  WriteResponsePB resp;
  RpcController controller;
  controller.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));
  string new_string_val(Substitute("mutated$0", row_idx));

  AddTestRowToPB(RowOperationsPB::UPDATE, schema_, row_idx, new_val, new_string_val,
                 req.mutable_row_operations());
  ASSERT_OK(proxy_->Write(req, &resp, &controller));

  SCOPED_TRACE(SecureDebugString(resp));
  ASSERT_FALSE(resp.has_error()) << SecureShortDebugString(resp);
  ASSERT_EQ(0, resp.per_row_errors_size());
  if (ts) {
    ts->AddValue(1);
  }
}

void TabletServerTestBase::ResetClientProxies() {
  CreateTsClientProxies(mini_server_->bound_rpc_addr(),
                        client_messenger_,
                        &tablet_copy_proxy_,
                        &proxy_,
                        &admin_proxy_,
                        &consensus_proxy_,
                        &generic_proxy_);
}

// Inserts 'num_rows' test rows directly into the tablet (i.e not via RPC)
void TabletServerTestBase::InsertTestRowsDirect(int32_t start_row,
                                                int32_t num_rows,
                                                const string& tablet_id) {
  scoped_refptr<tablet::TabletReplica> tablet_replica;
  ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(tablet_id,
                                                                     &tablet_replica));
  tablet::LocalTabletWriter writer(tablet_replica->tablet(), &schema_);
  KuduPartialRow row(&schema_);
  for (int32_t i = 0; i < num_rows; i++) {
    BuildTestRow(start_row + i, &row);
    CHECK_OK(writer.Insert(row));
  }
}

// Inserts 'num_rows' test rows remotely into the tablet (i.e via RPC)
// Rows are grouped in batches of 'count'/'num_batches' size.
// Batch size defaults to 1.
void TabletServerTestBase::InsertTestRowsRemote(
    int32_t first_row,
    int32_t count,
    int32_t num_batches,
    TabletServerServiceProxy* proxy,
    string tablet_id,
    vector<uint64_t>* write_timestamps_collector,
    TimeSeries* ts,
    bool string_field_defined) {
  if (!proxy) {
    proxy = proxy_.get();
  }

  if (num_batches == -1) {
    num_batches = count;
  }

  WriteRequestPB req;
  req.set_tablet_id(std::move(tablet_id));

  WriteResponsePB resp;
  RpcController controller;

  RowOperationsPB* data = req.mutable_row_operations();

  ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));

  int32_t inserted_since_last_report = 0;
  for (int i = 0; i < num_batches; ++i) {

    // reset the controller and the request
    controller.Reset();
    controller.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));
    data->Clear();

    int32_t first_row_in_batch = first_row + (i * count / num_batches);
    int32_t last_row_in_batch = first_row_in_batch + count / num_batches;

    for (int32_t j = first_row_in_batch; j < last_row_in_batch; j++) {
      string str_val = Substitute("original$0", j);
      const char* cstr_val = str_val.c_str();
      if (!string_field_defined) {
        cstr_val = NULL;
      }
      AddTestRowWithNullableStringToPB(RowOperationsPB::INSERT, schema_, j, j,
                                       cstr_val, data);
    }
    CHECK_OK(DCHECK_NOTNULL(proxy)->Write(req, &resp, &controller));
    if (write_timestamps_collector) {
      write_timestamps_collector->push_back(resp.timestamp());
    }

    if (resp.has_error() || resp.per_row_errors_size() > 0) {
      LOG(FATAL) << "Failed to insert batch "
                 << first_row_in_batch << "-" << last_row_in_batch
                 << ": " << SecureDebugString(resp);
    }

    inserted_since_last_report += count / num_batches;
    if ((inserted_since_last_report > 100) && ts) {
      ts->AddValue(static_cast<double>(inserted_since_last_report));
      inserted_since_last_report = 0;
    }
  }

  if (ts) {
    ts->AddValue(static_cast<double>(inserted_since_last_report));
  }
}

// Delete specified test row range.
void TabletServerTestBase::DeleteTestRowsRemote(int32_t first_row,
                                                int32_t count,
                                                TabletServerServiceProxy* proxy,
                                                string tablet_id) {
  if (!proxy) {
    proxy = proxy_.get();
  }

  WriteRequestPB req;
  WriteResponsePB resp;
  RpcController controller;

  req.set_tablet_id(std::move(tablet_id));
  ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));

  RowOperationsPB* ops = req.mutable_row_operations();
  for (int32_t rowid = first_row; rowid < first_row + count; rowid++) {
    AddTestKeyToPB(RowOperationsPB::DELETE, schema_, rowid, ops);
  }

  SCOPED_TRACE(SecureDebugString(req));
  ASSERT_OK(DCHECK_NOTNULL(proxy)->Write(req, &resp, &controller));
  SCOPED_TRACE(SecureDebugString(resp));
  ASSERT_FALSE(resp.has_error()) << SecureShortDebugString(resp);
}

void TabletServerTestBase::BuildTestRow(int index, KuduPartialRow* row) {
  ASSERT_OK(row->SetInt32(0, index));
  ASSERT_OK(row->SetInt32(1, index * 2));
  ASSERT_OK(row->SetStringCopy(2, StringPrintf("hello %d", index)));
}

void TabletServerTestBase::DrainScannerToStrings(const string& scanner_id,
                                                 const Schema& projection,
                                                 vector<string>* results,
                                                 TabletServerServiceProxy* proxy,
                                                 uint32_t call_seq_id) {
  if (!proxy) {
    proxy = proxy_.get();
  }

  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));
  ScanRequestPB req;
  ScanResponsePB resp;
  req.set_scanner_id(scanner_id);

  // NOTE: we do not sort the results here, since this function is used
  // by test cases which are verifying the server side's ability to
  // do ordered scans.
  do {
    rpc.Reset();
    req.set_batch_size_bytes(10000);
    req.set_call_seq_id(call_seq_id);
    SCOPED_TRACE(SecureDebugString(req));
    ASSERT_OK(DCHECK_NOTNULL(proxy)->Scan(req, &resp, &rpc));
    SCOPED_TRACE(SecureDebugString(resp));
    ASSERT_FALSE(resp.has_error());

    StringifyRowsFromResponse(projection, rpc, &resp, results);
    call_seq_id += 1;
  } while (resp.has_more_results());
}

void TabletServerTestBase::StringifyRowsFromResponse(
    const Schema& projection,
    const RpcController& rpc,
    ScanResponsePB* resp,
    vector<string>* results) {
  RowwiseRowBlockPB* rrpb = resp->mutable_data();
  Slice direct, indirect; // sidecar data buffers
  ASSERT_OK(rpc.GetInboundSidecar(rrpb->rows_sidecar(), &direct));
  if (rrpb->has_indirect_data_sidecar()) {
    ASSERT_OK(rpc.GetInboundSidecar(rrpb->indirect_data_sidecar(),
            &indirect));
  }
  vector<const uint8_t*> rows;
  ASSERT_OK(ExtractRowsFromRowBlockPB(projection, *rrpb,
                                      indirect, &direct, &rows));
  VLOG(1) << "Round trip got " << rows.size() << " rows";
  for (const uint8_t* row_ptr : rows) {
    ConstContiguousRow row(&projection, row_ptr);
    results->push_back(projection.DebugRow(row));
  }
}

void TabletServerTestBase::ShutdownTablet() {
  if (mini_server_.get()) {
    // The TabletReplica must be destroyed before the TS, otherwise data
    // blocks may be destroyed after their owning block manager.
    tablet_replica_.reset();
    mini_server_->Shutdown();
    mini_server_.reset();
  }
}

Status TabletServerTestBase::ShutdownAndRebuildTablet(int num_data_dirs) {
  ShutdownTablet();

  // Start server.
  mini_server_.reset(new MiniTabletServer(GetTestPath("TabletServerTest-fsroot"),
                                          HostPort("127.0.0.1", 0), num_data_dirs));
  mini_server_->options()->master_addresses.clear();
  mini_server_->options()->master_addresses.emplace_back("255.255.255.255", 1);
  // this should open the tablet created on StartTabletServer()
  RETURN_NOT_OK(mini_server_->Start());

  // Don't RETURN_NOT_OK immediately -- even if we fail, we may still get a TabletReplica object
  // which has information about the failure.
  Status wait_status = mini_server_->WaitStarted();
  bool found_peer = mini_server_->server()->tablet_manager()->LookupTablet(
      kTabletId, &tablet_replica_);
  RETURN_NOT_OK(wait_status);
  if (!found_peer) {
    return Status::NotFound("Tablet was not found");
  }

  // Connect to it.
  ResetClientProxies();

  // Opening a tablet is async, we wait here instead of having to handle errors later.
  return WaitForTabletRunning(kTabletId);
}

// Verifies that a set of expected rows (key, value) is present in the tablet.
void TabletServerTestBase::VerifyRows(const Schema& schema,
                                      const vector<KeyValue>& expected) {
  unique_ptr<RowwiseIterator> iter;
  ASSERT_OK(tablet_replica_->tablet()->NewRowIterator(schema, &iter));
  ASSERT_OK(iter->Init(nullptr));

  int batch_size = std::max<int>(1,
     std::min<int>(expected.size() / 10,
                   4*1024*1024 / schema.byte_size()));

  RowBlockMemory mem(32 * 1024);
  RowBlock block(&schema, batch_size, &mem);

  int count = 0;
  while (iter->HasNext()) {
    mem.Reset();
    ASSERT_OK_FAST(iter->NextBlock(&block));
    RowBlockRow rb_row = block.row(0);
    for (int i = 0; i < block.nrows(); i++) {
      if (block.selection_vector()->IsRowSelected(i)) {
        rb_row.Reset(&block, i);
        VLOG(1) << "Verified row " << schema.DebugRow(rb_row);
        ASSERT_LT(count, expected.size()) << "Got more rows than expected!";
        EXPECT_EQ(expected[count].first, *schema.ExtractColumnFromRow<INT32>(rb_row, 0))
            << "Key mismatch at row: " << count;
        EXPECT_EQ(expected[count].second, *schema.ExtractColumnFromRow<INT32>(rb_row, 1))
            << "Value mismatch at row: " << count;
        count++;
      }
    }
  }
  ASSERT_EQ(count, expected.size());
}

void TabletServerTestBase::VerifyScanRequestFailure(const ScanRequestPB& req,
                                                    TabletServerErrorPB::Code expected_code,
                                                    const char *expected_message) {
  ScanResponsePB resp;
  RpcController rpc;

  // Send the call.
  {
    SCOPED_TRACE(SecureDebugString(req));
    ASSERT_OK(proxy_->Scan(req, &resp, &rpc));
    SCOPED_TRACE(SecureDebugString(resp));
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(expected_code, resp.error().code());
    ASSERT_STR_CONTAINS(resp.error().status().message(), expected_message);
  }
}

void TabletServerTestBase::VerifyScanRequestFailure(const Schema& projection,
                                                    TabletServerErrorPB::Code expected_code,
                                                    const char *expected_message) {
  ScanRequestPB req;
  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(kTabletId);
  ASSERT_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
  req.set_call_seq_id(0);
  NO_FATALS(VerifyScanRequestFailure(req, expected_code, expected_message));
}

Status TabletServerTestBase::FillNewScanRequest(ReadMode read_mode, NewScanRequestPB* scan) const {
  scan->set_tablet_id(kTabletId);
  scan->set_read_mode(read_mode);
  return SchemaToColumnPBs(schema_, scan->mutable_projected_columns());
}

// Open a new scanner which scans all of the columns in the table.
void TabletServerTestBase::OpenScannerWithAllColumns(ScanResponsePB* resp,
                                                     ReadMode read_mode) {
  ScanRequestPB req;
  RpcController rpc;

  // Set up a new request with no predicates, all columns.
  ASSERT_OK(FillNewScanRequest(read_mode, req.mutable_new_scan_request()));
  req.set_call_seq_id(0);
  req.set_batch_size_bytes(0); // so it won't return data right away

  // Send the call
  {
    SCOPED_TRACE(SecureDebugString(req));
    ASSERT_OK(proxy_->Scan(req, resp, &rpc));
    SCOPED_TRACE(SecureDebugString(*resp));
    ASSERT_FALSE(resp->has_error());
    ASSERT_TRUE(resp->has_more_results());
  }
}

} // namespace tserver
} // namespace kudu
