// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TSERVER_TABLET_SERVER_TEST_BASE_H_
#define KUDU_TSERVER_TABLET_SERVER_TEST_BASE_H_

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <tr1/memory>
#include <vector>
#include <string>
#include <assert.h>
#include <stdint.h>
#include <sys/mman.h>
#include <iostream>
#include <sys/types.h>
#include <signal.h>
#include <utility>

#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/maintenance_manager.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/tserver/scanners.h"
#include "kudu/util/test_graph.h"
#include "kudu/util/test_util.h"
#include "kudu/util/metrics.h"
#include "kudu/rpc/messenger.h"

DEFINE_int32(rpc_timeout, 1000, "Timeout for RPC calls, in seconds");
DEFINE_int32(num_updater_threads, 1, "Number of updating threads to launch");
DECLARE_bool(use_hybrid_clock);
DECLARE_bool(log_force_fsync_all);
DECLARE_int32(max_clock_sync_error_usec);
DECLARE_bool(enable_maintenance_manager);

namespace kudu {
namespace tserver {

static const int kSharedRegionSize = 4096;

// NOTE: Supports up to 256 inserters/updaters
struct SharedData {
  // Keeps the last ACK'd insert for each inserter thread.
  int64_t last_inserted[256];

  // Returns the last value which one of the rows in the
  // updater's thread was updated to.
  int64_t last_updated[256];
};

class TabletServerTest : public KuduTest {
 public:
  typedef pair<uint32_t, uint32_t> KeyValue;

  TabletServerTest()
    : schema_(GetSimpleTestSchema()),
      ts_test_metric_context_(&ts_test_metric_registry_, "ts_server-test") {

    // Use the hybrid clock for TS tests
    FLAGS_use_hybrid_clock = true;

    // increase the max error tolerance, for tests, to 10 seconds.
    FLAGS_max_clock_sync_error_usec = 10000000;

    // Disable the maintenance ops manager since we want to trigger our own
    // maintenance operations at predetermined times.
    FLAGS_enable_maintenance_manager = false;
  }

  // Starts the tablet server, override to start it later.
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    key_schema_ = schema_.CreateKeyProjection();
    rb_.reset(new RowBuilder(schema_));

    CreateSharedRegion();
    StartTabletServer();
  }

  virtual void TearDown() OVERRIDE {
    DeleteSharedRegion();
    KuduTest::TearDown();
  }

  // create a shared region for the processes to be able to communicate
  virtual void CreateSharedRegion() {
    shared_region_ = mmap(NULL, kSharedRegionSize,
                          PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS,
                          -1, 0);
    CHECK(shared_region_) << "Could not mmap: " << ErrnoToString(errno);
    shared_data_ = reinterpret_cast<volatile SharedData*>(shared_region_);
  }

  virtual void DeleteSharedRegion() {
    munmap(shared_region_, kSharedRegionSize);
  }

  virtual void StartTabletServer() {
    // Start server with an invalid master address, so it never successfully
    // heartbeats, even if there happens to be a master running on this machine.
    mini_server_.reset(new MiniTabletServer(env_.get(), GetTestPath("TabletServerTest-fsroot"), 0));
    mini_server_->options()->master_hostport = HostPort("255.255.255.255", 1);
    ASSERT_STATUS_OK(mini_server_->Start());

    // Set up a tablet inside the server.
    ASSERT_STATUS_OK(mini_server_->AddTestTablet(kTableId, kTabletId, schema_));
    ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet_peer_));

    // Creating a tablet is async, we wait here instead of having to handle errors later.
    ASSERT_STATUS_OK(WaitForTabletRunning(kTabletId));

    // Connect to it.
    ASSERT_NO_FATAL_FAILURE(CreateClientProxies(mini_server_->bound_rpc_addr(),
                                                &proxy_, &admin_proxy_, &consensus_proxy_));
  }

  Status WaitForTabletRunning(const char *tablet_id) {
    scoped_refptr<tablet::TabletPeer> tablet_peer;
    RETURN_NOT_OK(mini_server_->server()->tablet_manager()->GetTabletPeer(tablet_id, &tablet_peer));
    return tablet_peer->WaitUntilRunning(MonoDelta::FromMilliseconds(2000));
  }

  void UpdateTestRowRemote(int tid,
                           uint64_t row_idx,
                           uint32_t new_val,
                           TimeSeries *ts = NULL) {

    WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));

    WriteResponsePB resp;
    rpc::RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));
    string new_string_val(strings::Substitute("mutated$0", row_idx));

    AddTestRowToPB(RowOperationsPB::UPDATE, schema_, row_idx, new_val, new_string_val,
                   req.mutable_row_operations());
    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));

    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error())<< resp.ShortDebugString();
    ASSERT_EQ(0, resp.per_row_errors_size());
    shared_data_->last_updated[tid] = new_val;
    if (ts) {
      ts->AddValue(1);
    }
  }

  void CreateClientProxies(const Sockaddr &addr, gscoped_ptr<TabletServerServiceProxy>* proxy,
                           gscoped_ptr<TabletServerAdminServiceProxy>* admin_proxy,
                           gscoped_ptr<consensus::ConsensusServiceProxy>* consensus_proxy) {
    if (!client_messenger_) {
      rpc::MessengerBuilder bld("Client");
      ASSERT_STATUS_OK(bld.Build(&client_messenger_));
    }
    proxy->reset(new TabletServerServiceProxy(client_messenger_, addr));
    admin_proxy->reset(new TabletServerAdminServiceProxy(client_messenger_, addr));
    consensus_proxy->reset(new consensus::ConsensusServiceProxy(client_messenger_, addr));
  }

 protected:
  static const char* kTableId;
  static const char* kTabletId;

  // Inserts 'num_rows' test rows directly into the tablet (i.e not via RPC)
  void InsertTestRowsDirect(uint64_t start_row, uint64_t num_rows) {
    tablet::LocalTabletWriter writer(tablet_peer_->tablet(), &schema_);
    KuduPartialRow row(&schema_);
    for (uint64_t i = 0; i < num_rows; i++) {
      BuildTestRow(start_row + i, &row);
      CHECK_OK(writer.Insert(row));
    }
  }

  // Inserts 'num_rows' test rows remotely into the tablet (i.e via RPC)
  // Rows are grouped in batches of 'count'/'num_batches' size.
  // Batch size defaults to 1.
  void InsertTestRowsRemote(int tid,
                            uint64_t first_row,
                            uint64_t count,
                            uint64_t num_batches = -1,
                            TabletServerServiceProxy* proxy = NULL,
                            string tablet_id = kTabletId,
                            vector<uint64_t>* write_timestamps_collector = NULL,
                            TimeSeries *ts = NULL) {

    if (!proxy) {
      proxy = proxy_.get();
    }

    if (num_batches == -1) {
      num_batches = count;
    }

    WriteRequestPB req;
    req.set_tablet_id(tablet_id);

    WriteResponsePB resp;
    rpc::RpcController controller;

    RowOperationsPB* data = req.mutable_row_operations();

    ASSERT_STATUS_OK(SchemaToPB(schema_, req.mutable_schema()));

    uint64_t inserted_since_last_report = 0;
    for (int i = 0; i < num_batches; ++i) {

      // reset the controller and the request
      controller.Reset();
      controller.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));
      data->Clear();

      uint64_t first_row_in_batch = first_row + (i * count / num_batches);
      uint64_t last_row_in_batch = first_row_in_batch + count / num_batches;

      for (int j = first_row_in_batch; j < last_row_in_batch; j++) {
        AddTestRowToPB(RowOperationsPB::INSERT, schema_, j, j,
                       strings::Substitute("original$0", j), data);
      }
      CHECK_OK(DCHECK_NOTNULL(proxy)->Write(req, &resp, &controller));
      if (write_timestamps_collector) {
        write_timestamps_collector->push_back(resp.write_timestamp());
      }

      if (resp.has_error() || resp.per_row_errors_size() > 0) {
        LOG(FATAL) << "Failed to insert batch "
                    << first_row_in_batch << "-" << last_row_in_batch
                    << ": " << resp.DebugString();
      }
      shared_data_->last_inserted[tid] = last_row_in_batch;

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

  void BuildTestRow(int index, KuduPartialRow* row) {
    CHECK_OK(row->SetUInt32(0, index));
    CHECK_OK(row->SetUInt32(1, index * 2));
    CHECK_OK(row->SetStringCopy(2, StringPrintf("hello %d", index)));
  }

  void DrainScannerToStrings(const string& scanner_id,
                             const Schema& projection,
                             vector<string>* results,
                             TabletServerServiceProxy* proxy = NULL) {

    if (!proxy) {
      proxy = proxy_.get();
    }

    rpc::RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));
    ScanRequestPB req;
    ScanResponsePB resp;
    req.set_scanner_id(scanner_id);

    do {
      rpc.Reset();
      req.set_batch_size_bytes(10000);
      SCOPED_TRACE(req.DebugString());
      ASSERT_STATUS_OK(DCHECK_NOTNULL(proxy)->Scan(req, &resp, &rpc));
      SCOPED_TRACE(resp.DebugString());
      ASSERT_FALSE(resp.has_error());

      vector<const uint8_t*> rows;
      ASSERT_STATUS_OK(ExtractRowsFromRowBlockPB(projection,resp.mutable_data(), &rows));
      VLOG(1) << "Round trip got " << rows.size() << " rows";
      BOOST_FOREACH(const uint8_t* row_ptr, rows) {
        ConstContiguousRow row(&projection, row_ptr);
        results->push_back(projection.DebugRow(row));
      }
    } while (resp.has_more_results());
  }

  void ShutdownAndRebuildTablet() {
    if (mini_server_.get()) {
      mini_server_->Shutdown();
    }

    // Start server.
    mini_server_.reset(new MiniTabletServer(env_.get(), GetTestPath("TabletServerTest-fsroot"), 0));
    // this should open the tablet created on StartTabletServer()
    ASSERT_STATUS_OK(mini_server_->Start());
    ASSERT_STATUS_OK(mini_server_->WaitStarted());

    ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet_peer_));
    // Connect to it.
    ASSERT_NO_FATAL_FAILURE(CreateClientProxies(mini_server_->bound_rpc_addr(),
                                                &proxy_, &admin_proxy_, &consensus_proxy_));
  }

  // Verifies that a set of expected rows (key, value) is present in the tablet.
  void VerifyRows(const Schema& schema, const vector<KeyValue>& expected) {
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_STATUS_OK(tablet_peer_->tablet()->NewRowIterator(schema, &iter));
    ASSERT_STATUS_OK(iter->Init(NULL));

    int batch_size = std::max(
        (size_t)1, std::min((size_t)(expected.size() / 10),
                            4*1024*1024 / schema.byte_size()));

    Arena arena(32*1024, 256*1024);
    RowBlock block(schema, batch_size, &arena);

    int count = 0;
    while (iter->HasNext()) {
      ASSERT_STATUS_OK_FAST(iter->NextBlock(&block));
      RowBlockRow rb_row = block.row(0);
      for (int i = 0; i < block.nrows(); i++) {
        if (block.selection_vector()->IsRowSelected(i)) {
          rb_row.Reset(&block, i);
          VLOG(1) << "Verified row " << schema.DebugRow(rb_row);
          ASSERT_LT(count, expected.size()) << "Got more rows than expected!";
          ASSERT_EQ(expected[count].first, *schema.ExtractColumnFromRow<UINT32>(rb_row, 0));
          ASSERT_EQ(expected[count].second, *schema.ExtractColumnFromRow<UINT32>(rb_row, 1));
          count++;
        }
      }
    }
    ASSERT_EQ(count, expected.size());
  }

  // Verifies that a simple scan request fails with the specified error code/message.
  void VerifyScanRequestFailure(const Schema& projection,
                                TabletServerErrorPB::Code expected_code,
                                const char *expected_message) {
    ScanRequestPB req;
    ScanResponsePB resp;
    rpc::RpcController rpc;

    NewScanRequestPB* scan = req.mutable_new_scan_request();
    scan->set_tablet_id(kTabletId);
    ASSERT_STATUS_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
    req.set_call_seq_id(0);

    // Send the call
    {
      SCOPED_TRACE(req.DebugString());
      ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
      SCOPED_TRACE(resp.DebugString());
      ASSERT_TRUE(resp.has_error());
      ASSERT_EQ(expected_code, resp.error().code());
      ASSERT_STR_CONTAINS(resp.error().status().message(), expected_message);
    }
  }

  // Open a new scanner which scans all of the columns in the table.
  void OpenScannerWithAllColumns(ScanResponsePB* resp) {
    ScanRequestPB req;
    rpc::RpcController rpc;

    // Set up a new request with no predicates, all columns.
    const Schema& projection = schema_;
    NewScanRequestPB* scan = req.mutable_new_scan_request();
    scan->set_tablet_id(kTabletId);
    ASSERT_STATUS_OK(SchemaToColumnPBs(projection, scan->mutable_projected_columns()));
    req.set_call_seq_id(0);
    req.set_batch_size_bytes(0); // so it won't return data right away

    // Send the call
    {
      SCOPED_TRACE(req.DebugString());
      ASSERT_STATUS_OK(proxy_->Scan(req, resp, &rpc));
      SCOPED_TRACE(resp->DebugString());
      ASSERT_FALSE(resp->has_error());
      ASSERT_TRUE(resp->has_more_results());
    }
  }


  const Schema schema_;
  Schema key_schema_;
  gscoped_ptr<RowBuilder> rb_;

  shared_ptr<rpc::Messenger> client_messenger_;

  gscoped_ptr<MiniTabletServer> mini_server_;
  scoped_refptr<tablet::TabletPeer> tablet_peer_;
  gscoped_ptr<TabletServerServiceProxy> proxy_;
  gscoped_ptr<TabletServerAdminServiceProxy> admin_proxy_;
  gscoped_ptr<consensus::ConsensusServiceProxy> consensus_proxy_;

  MetricRegistry ts_test_metric_registry_;
  MetricContext ts_test_metric_context_;

  volatile SharedData* shared_data_;
  void* shared_region_;
};

const char* TabletServerTest::kTableId = "TestTable";
const char* TabletServerTest::kTabletId = "TestTablet";

} // namespace tserver
} // namespace kudu


#endif /* KUDU_TSERVER_TABLET_SERVER_TEST_BASE_H_ */
