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

#include "common/wire_protocol-test-util.h"
#include "consensus/log_reader.h"
#include "gutil/atomicops.h"
#include "gutil/stl_util.h"
#include "tablet/tablet.h"
#include "tablet/tablet_peer.h"
#include "tablet/transactions/write_transaction.h"
#include "tserver/mini_tablet_server.h"
#include "tserver/tablet_server.h"
#include "tserver/ts_tablet_manager.h"
#include "tserver/tserver_service.proxy.h"
#include "tserver/scanners.h"
#include "util/test_graph.h"
#include "util/test_util.h"
#include "rpc/messenger.h"

DEFINE_int32(rpc_timeout, 1000, "Timeout for RPC calls, in seconds");
DEFINE_int32(num_updater_threads, 1, "Number of updating threads to launch");

using std::string;
using std::tr1::shared_ptr;
using base::subtle::Atomic64;
using base::subtle::Barrier_AtomicIncrement;
using kudu::log::LogEntry;
using kudu::log::COMMIT;
using kudu::log::LogReader;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::tablet::Tablet;
using kudu::tablet::TabletPeer;
using kudu::tablet::TxOperationPB;
using kudu::tablet::MissedDeltaMutationPB;

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

  TabletServerTest() {
    CreateTestSchema(&schema_);
    key_schema_ = schema_.CreateKeyProjection();
    rb_.reset(new RowBuilder(schema_));
  }

  // Starts the tablet server, override to start it later.
  virtual void SetUp() {
    KuduTest::SetUp();
    CreateSharedRegion();
    StartTabletServer();
  }

  virtual void TearDown() {
    DeleteSharedRegion();
    KuduTest::TearDown();
  }

  // create a shared region for the processes to be able to communicate
  virtual void CreateSharedRegion() {
    shared_region_ = mmap(NULL, kSharedRegionSize, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    CHECK(shared_region_) << "Could not mmap: " << ErrnoToString(errno);
    shared_data_ = reinterpret_cast<volatile SharedData*>(shared_region_);
  }

  virtual void DeleteSharedRegion() {
    munmap(shared_region_, kSharedRegionSize);
  }

  virtual void StartTabletServer() {
    // Start server.
    mini_server_.reset(new MiniTabletServer(env_.get(), GetTestPath("TabletServerTest-fsroot")));
    ASSERT_STATUS_OK(mini_server_->Start());

    // Set up a tablet inside the server.
    ASSERT_STATUS_OK(mini_server_->AddTestTablet(kTabletId, schema_));
    ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet_peer_));

    // Connect to it.
    ASSERT_NO_FATAL_FAILURE(CreateClientProxy(mini_server_->bound_rpc_addr(), &proxy_));
  }

  void UpdateTestRowRemote(int tid,
                           uint64_t row_idx,
                           uint32_t new_val,
                           TimeSeries *ts = NULL) {

    WriteRequestPB req;
    req.set_tablet_id(kTabletId);

    WriteResponsePB resp;
    RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));

    RowwiseRowBlockPB* data = req.mutable_to_mutate_row_keys();
    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());

    string new_string_val(strings::Substitute("mutated$0", row_idx));
    Slice mutation(new_string_val);
    faststring mutations;
    AddTestMutationToRowBlockAndBuffer(schema_,
                                       row_idx,
                                       new_val,
                                       mutation,
                                       data,
                                       &mutations);

    req.set_encoded_mutations(mutations.data(), mutations.size());

    ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));

    SCOPED_TRACE(resp.DebugString());
    ASSERT_FALSE(resp.has_error())<< resp.ShortDebugString();
    ASSERT_EQ(0, resp.per_row_errors_size());
    shared_data_->last_updated[tid] = new_val;
    if (ts) {
      ts->AddValue(1);
    }
  }

 private:

  void CreateClientProxy(const Sockaddr &addr, gscoped_ptr<TabletServerServiceProxy>* proxy) {
    if (!client_messenger_) {
      MessengerBuilder bld("Client");
      ASSERT_STATUS_OK(bld.Build(&client_messenger_));
    }
    proxy->reset(new TabletServerServiceProxy(client_messenger_, addr));
  }

 protected:
  static const char* kTabletId;

  // Inserts 'num_rows' test rows directly into the tablet (i.e not via RPC)
  void InsertTestRowsDirect(uint64_t num_rows) {
    tablet::WriteTransactionContext tx_ctx;
    for (uint64_t i = 0; i < num_rows; i++) {
      CHECK_OK(tablet_peer_->tablet()->Insert(&tx_ctx, BuildTestRow(i)));
      tx_ctx.Reset();
    }
  }

  // Inserts 'num_rows' test rows remotely into the tablet (i.e via RPC)
  // Rows are grouped in batches of 'count'/'num_batches' size.
  // Batch size defaults to 1.
  void InsertTestRowsRemote(int tid,
                            uint64_t first_row,
                            uint64_t count,
                            TimeSeries *ts = NULL,
                            uint64_t num_batches = -1) {

    if (num_batches == -1) {
      num_batches = count;
    }

    WriteRequestPB req;
    req.set_tablet_id(kTabletId);

    WriteResponsePB resp;
    RpcController controller;

    gscoped_ptr<RowwiseRowBlockPB> data(req.mutable_to_insert_rows());
    req.release_to_insert_rows();

    ASSERT_STATUS_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());

    uint64_t inserted_since_last_report = 0;
    for (int i = 0; i < num_batches; ++i) {

      // reset the controller and the request
      controller.Reset();
      controller.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));
      req.clear_to_insert_rows();

      // clear the data (keeping the schema)
      data->clear_rows();
      data->clear_indirect_data();
      data->clear_num_rows();

      uint64_t first_row_in_batch = first_row + (i * count/num_batches);
      uint64_t last_row_in_batch = first_row_in_batch + count/num_batches;

      for (int j = first_row_in_batch; j < last_row_in_batch; j++) {
        AddTestRowToBlockPB(schema_, j, j, strings::Substitute("original$0", j), data.get());
      }

      req.set_allocated_to_insert_rows(data.release());
      ASSERT_STATUS_OK(proxy_->Write(req, &resp, &controller));
      SCOPED_TRACE(resp.DebugString());
      ASSERT_FALSE(resp.has_error());
      ASSERT_EQ(0, resp.per_row_errors_size());
      shared_data_->last_inserted[tid] = last_row_in_batch;

      inserted_since_last_report += count/num_batches;
      if ((inserted_since_last_report > 100) && ts) {
        ts->AddValue(static_cast<double>(inserted_since_last_report));
        inserted_since_last_report = 0;
      }

      // release the data so that we can reuse it
      data.reset(req.release_to_insert_rows());

    }

    if (ts) {
      ts->AddValue(static_cast<double>(inserted_since_last_report));
    }
  }

  ConstContiguousRow BuildTestRow(int index) {
    rb_->Reset();
    rb_->AddUint32(index);
    rb_->AddUint32(index * 2);
    rb_->AddString(StringPrintf("hello %d", index));
    return rb_->row();
  }

  void DrainScannerToStrings(const string& scanner_id, const Schema& projection,
                             vector<string>* results) {
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(FLAGS_rpc_timeout));
    ScanRequestPB req;
    ScanResponsePB resp;
    req.set_scanner_id(scanner_id);

    do {
      rpc.Reset();
      req.set_batch_size_bytes(10000);
      SCOPED_TRACE(req.DebugString());
      ASSERT_STATUS_OK(proxy_->Scan(req, &resp, &rpc));
      SCOPED_TRACE(resp.DebugString());
      ASSERT_FALSE(resp.has_error());

      vector<const uint8_t*> rows;
      ASSERT_STATUS_OK(ExtractRowsFromRowBlockPB(projection,resp.mutable_data(), &rows));
      VLOG(1) << "Round trip got " << rows.size() << " rows";
      BOOST_FOREACH(const uint8_t* row_ptr, rows) {
        ConstContiguousRow row(projection, row_ptr);
        results->push_back(projection.DebugRow(row));
      }
    } while (resp.has_more_results());
  }

  void ShutdownAndRebuildTablet() {
    if (mini_server_.get()) {
      mini_server_->Shutdown();
    }

    // Start server.
    mini_server_.reset(new MiniTabletServer(env_.get(), GetTestPath("TabletServerTest-fsroot")));
    // this should open the tablet created on StartTabletServer()
    ASSERT_STATUS_OK(mini_server_->Start());

    ASSERT_TRUE(mini_server_->server()->tablet_manager()->LookupTablet(kTabletId, &tablet_peer_));
    // Connect to it.
    ASSERT_NO_FATAL_FAILURE(CreateClientProxy(mini_server_->bound_rpc_addr(), &proxy_));
  }

  // Verifies that a set of expected rows (key, value) is present in the tablet.
  void VerifyRows(const vector<KeyValue>& expected) {
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_STATUS_OK(tablet_peer_->tablet()->NewRowIterator(schema_, &iter));
    ASSERT_STATUS_OK(iter->Init(NULL));

    int batch_size = std::max(
        (size_t)1, std::min((size_t)(expected.size() / 10),
                            4*1024*1024 / schema_.byte_size()));

    Arena arena(32*1024, 256*1024);
    RowBlock block(schema_, batch_size, &arena);

    int count = 0;
    while (iter->HasNext()) {
      ASSERT_STATUS_OK_FAST(RowwiseIterator::CopyBlock(iter.get(), &block));
      RowBlockRow rb_row = block.row(0);
      for (int i = 0; i < block.nrows(); i++) {
        if (block.selection_vector()->IsRowSelected(i)) {
          rb_row.Reset(&block, i);
          ASSERT_LT(count, expected.size());
          ASSERT_EQ(expected[count].first, *schema_.ExtractColumnFromRow<UINT32>(rb_row, 0));
          ASSERT_EQ(expected[count].second, *schema_.ExtractColumnFromRow<UINT32>(rb_row, 1));
          count++;
        }
      }
    }
    ASSERT_EQ(count, expected.size());
  }

  Schema schema_;
  Schema key_schema_;
  gscoped_ptr<RowBuilder> rb_;

  shared_ptr<Messenger> client_messenger_;

  gscoped_ptr<MiniTabletServer> mini_server_;
  shared_ptr<TabletPeer> tablet_peer_;
  gscoped_ptr<TabletServerServiceProxy> proxy_;

  volatile SharedData* shared_data_;
  void* shared_region_;
};

const char* TabletServerTest::kTabletId = "TestTablet";

} // namespace tserver
} // namespace kudu


#endif /* KUDU_TSERVER_TABLET_SERVER_TEST_BASE_H_ */
