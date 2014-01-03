// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include <tr1/memory>
#include <vector>

#include "client/client.h"
#include "common/row.h"
#include "common/wire_protocol.h"
#include "integration-tests/mini_cluster.h"
#include "master/master-test-util.h"
#include "master/mini_master.h"
#include "tablet/tablet_peer.h"
#include "tablet/transactions/write_transaction.h"
#include "tserver/mini_tablet_server.h"
#include "tserver/scanners.h"
#include "tserver/tablet_server.h"
#include "tserver/ts_tablet_manager.h"
#include "util/net/sockaddr.h"
#include "util/status.h"
#include "util/stopwatch.h"
#include "util/test_util.h"

DECLARE_int32(heartbeat_interval_ms);
DEFINE_int32(test_scan_num_rows, 1000, "Number of rows to insert and scan");

using std::tr1::shared_ptr;

namespace kudu {
namespace client {

using tablet::TabletPeer;
using tserver::MiniTabletServer;
using tserver::ColumnRangePredicatePB;

class ClientTest : public KuduTest {
 public:
  ClientTest()
    : schema_(boost::assign::list_of
              (ColumnSchema("key", UINT32))
              (ColumnSchema("int_val", UINT32))
              (ColumnSchema("string_val", STRING, true)),
              1),
      rb_(schema_) {
  }

  virtual void SetUp() {
    KuduTest::SetUp();

    FLAGS_heartbeat_interval_ms = 10;

    // Start minicluster
    cluster_.reset(new MiniCluster(env_.get(), test_dir_, 1));
    ASSERT_STATUS_OK(cluster_->Start());

    // Create the tablet that we are going to register
    CreateTabletForTesting(cluster_->mini_master(), "fake-table", &tablet_id_);

    // Set up a tablet inside the server.
    ASSERT_STATUS_OK(cluster_->mini_tablet_server(0)->AddTestTablet(tablet_id_, schema_));
    ASSERT_TRUE(cluster_->mini_tablet_server(0)->server()->tablet_manager()->LookupTablet(
                tablet_id_, &tablet_peer_));

    // Wait for the tablet to be reported to the master.
    ASSERT_STATUS_OK(cluster_->WaitForReplicaCount(tablet_id_, 1));

    // Connect to it.
    KuduClientOptions opts;
    opts.master_server_addr = cluster_->mini_master()->bound_rpc_addr().ToString();
    ASSERT_STATUS_OK(KuduClient::Create(opts, &client_));
    ASSERT_STATUS_OK(client_->OpenTable(tablet_id_, schema_, &client_table_));
  }

 protected:

  string tablet_id_;

  // Inserts 'num_rows' test rows directly into the tablet (i.e not via RPC)
  void InsertTestRows(int num_rows) {
    tablet::WriteTransactionContext tx_ctx;
    for (int i = 0; i < num_rows; i++) {
      CHECK_OK(tablet_peer_->tablet()->Insert(&tx_ctx, BuildTestRow(i)));
      tx_ctx.Reset();
    }
    CHECK_OK(tablet_peer_->tablet()->Flush());
  }

  ConstContiguousRow BuildTestRow(int index) {
    rb_.Reset();
    rb_.AddUint32(index);
    rb_.AddUint32(index * 2);
    rb_.AddString(StringPrintf("hello %d", index));
    return rb_.row();
  }

  void DoTestScanWithoutPredicates() {
    Schema projection = schema_.CreateKeyProjection();
    KuduScanner scanner(client_table_.get());
    ASSERT_STATUS_OK(scanner.SetProjection(projection));
    LOG_TIMING(INFO, "Scanning with no predicates") {
      ASSERT_STATUS_OK(scanner.Open());

      ASSERT_TRUE(scanner.HasMoreRows());
      vector<const uint8_t*> rows;
      uint64_t sum = 0;
      while (scanner.HasMoreRows()) {
        ASSERT_STATUS_OK(scanner.NextBatch(&rows));

        BOOST_FOREACH(const uint8_t* row_ptr, rows) {
          ConstContiguousRow row(projection, row_ptr);
          uint32_t to_add = *projection.ExtractColumnFromRow<UINT32>(row, 0);
          sum += implicit_cast<uint64_t>(to_add);
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
    ASSERT_STATUS_OK(scanner.SetProjection(schema_));
    ColumnRangePredicatePB pred;
    ColumnSchemaToPB(schema_.column(2), pred.mutable_column());
    pred.set_lower_bound("hello 2");
    pred.set_upper_bound("hello 3");
    ASSERT_STATUS_OK(scanner.AddConjunctPredicate(pred));

    LOG_TIMING(INFO, "Scanning with no predicates") {
      ASSERT_STATUS_OK(scanner.Open());

      ASSERT_TRUE(scanner.HasMoreRows());
      vector<const uint8_t*> rows;
      while (scanner.HasMoreRows()) {
        ASSERT_STATUS_OK(scanner.NextBatch(&rows));

        BOOST_FOREACH(const uint8_t* row_ptr, rows) {
          ConstContiguousRow row(schema_, row_ptr);
          Slice s = *schema_.ExtractColumnFromRow<STRING>(row, 2);
          if (!s.starts_with("hello 2") && s != Slice("hello 3")) {
            FAIL() << schema_.DebugRow(row);
          }
        }
      }
    }
  }

  Schema schema_;
  RowBuilder rb_;

  gscoped_ptr<MiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
  shared_ptr<KuduTable> client_table_;
  shared_ptr<TabletPeer> tablet_peer_;
};

TEST_F(ClientTest, TestBadTable) {
  shared_ptr<KuduTable> t;
  Status s = client_->OpenTable("xxx-does-not-exist", Schema(), &t);
  ASSERT_EQ("Not found: No replicas for tablet xxx-does-not-exist", s.ToString());
}

// Test that, if the master is down, we get an appropriate error
// message.
TEST_F(ClientTest, TestMasterDown) {
  ASSERT_STATUS_OK(cluster_->mini_master()->Shutdown());
  shared_ptr<KuduTable> t;
  Status s = client_->OpenTable("other-tablet", Schema(), &t);
  ASSERT_TRUE(s.IsNetworkError());
  ASSERT_STR_CONTAINS(s.ToString(), "Connection refused");
}

TEST_F(ClientTest, TestScan) {
  InsertTestRows(FLAGS_test_scan_num_rows);

  DoTestScanWithoutPredicates();
  DoTestScanWithStringPredicate();
}

TEST_F(ClientTest, TestScanEmptyTable) {
  KuduScanner scanner(client_table_.get());
  ASSERT_STATUS_OK(scanner.Open());
  ASSERT_FALSE(scanner.HasMoreRows());
  scanner.Close();
}

// Test scanning with an empty projection. This should yield an empty
// row block with the proper number of rows filled in. Impala issues
// scans like this in order to implement COUNT(*).
TEST_F(ClientTest, TestScanEmptyProjection) {
  InsertTestRows(FLAGS_test_scan_num_rows);
  Schema empty_projection(vector<ColumnSchema>(), 0);
  KuduScanner scanner(client_table_.get());
  ASSERT_STATUS_OK(scanner.SetProjection(empty_projection));
  LOG_TIMING(INFO, "Scanning with no projected columns") {
    ASSERT_STATUS_OK(scanner.Open());

    ASSERT_TRUE(scanner.HasMoreRows());
    vector<const uint8_t*> rows;
    uint64_t count = 0;
    while (scanner.HasMoreRows()) {
      ASSERT_STATUS_OK(scanner.NextBatch(&rows));
      count += rows.size();
    }
    ASSERT_EQ(FLAGS_test_scan_num_rows, count);
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
    usleep(i < 10 ? 2000 : 20000);
  }
  FAIL() << "Waited too long for the scanner to close";
}

// Test cleanup of scanners on the server side when closed.
TEST_F(ClientTest, TestCloseScanner) {
  InsertTestRows(10);

  const tserver::ScannerManager* manager =
    cluster_->mini_tablet_server(0)->server()->scanner_manager();
  // Open the scanner, make sure it gets closed right away
  {
    SCOPED_TRACE("Implicit close");
    KuduScanner scanner(client_table_.get());
    ASSERT_STATUS_OK(scanner.SetProjection(schema_));
    ASSERT_STATUS_OK(scanner.Open());
    ASSERT_EQ(0, manager->CountActiveScanners());
    scanner.Close();
    AssertScannersDisappear(manager);
  }

  // Open the scanner, make sure we see 1 registered scanner.
  {
    SCOPED_TRACE("Explicit close");
    KuduScanner scanner(client_table_.get());
    ASSERT_STATUS_OK(scanner.SetProjection(schema_));
    ASSERT_STATUS_OK(scanner.SetBatchSizeBytes(0)); // won't return data on open
    ASSERT_STATUS_OK(scanner.Open());
    ASSERT_EQ(1, manager->CountActiveScanners());
    scanner.Close();
    AssertScannersDisappear(manager);
  }

  {
    SCOPED_TRACE("Close when out of scope");
    {
      KuduScanner scanner(client_table_.get());
      ASSERT_STATUS_OK(scanner.SetProjection(schema_));
      ASSERT_STATUS_OK(scanner.SetBatchSizeBytes(0));
      ASSERT_STATUS_OK(scanner.Open());
      ASSERT_EQ(1, manager->CountActiveScanners());
    }
    // Above scanner went out of scope, so the destructor should close asynchronously.
    AssertScannersDisappear(manager);
  }
}

TEST_F(ClientTest, TestInsert) {
  gscoped_ptr<KuduSession> session = client_->NewSession();
  gscoped_ptr<Insert> insert = client_table_->NewInsert();
  // Try inserting without specifying a key: should fail.
  ASSERT_STATUS_OK(insert->SetUInt32("int_val", 54321));
  ASSERT_STATUS_OK(insert->SetStringCopy("string_val", "hello world"));

  Status s = session->Apply(&insert);
  ASSERT_EQ("Illegal state: Key not specified: uint32 int_val=54321, string string_val=hello world",
            s.ToString());

  ASSERT_STATUS_OK(insert->SetUInt32("key", 12345));
  ASSERT_STATUS_OK(session->Apply(&insert));
  ASSERT_TRUE(insert == NULL) << "Successful insert should take ownership";
}

} // namespace client
} // namespace kudu
