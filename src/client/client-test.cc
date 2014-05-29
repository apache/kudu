// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include <tr1/memory>
#include <vector>

#include "client/client.h"
#include "common/maintenance_manager.h"
#include "common/row.h"
#include "common/wire_protocol.h"
#include "gutil/stl_util.h"
#include "gutil/strings/serialize.h"
#include "integration-tests/mini_cluster.h"
#include "master/catalog_manager.h"
#include "master/master-test-util.h"
#include "master/master.proxy.h"
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

using master::CatalogManager;
using master::GetTableLocationsRequestPB;
using master::GetTableLocationsResponsePB;
using master::TabletLocationsPB;
using tablet::TabletPeer;
using tserver::MiniTabletServer;
using tserver::ColumnRangePredicatePB;

const uint32_t kNonNullDefault = 12345;

class ClientTest : public KuduTest {
 public:
  ClientTest()
    : schema_(boost::assign::list_of
              (ColumnSchema("key", UINT32))
              (ColumnSchema("int_val", UINT32))
              (ColumnSchema("string_val", STRING, true))
              (ColumnSchema("non_null_with_default", UINT32, false,
                            &kNonNullDefault, &kNonNullDefault)),
              1),
      rb_(schema_) {
  }

  virtual void SetUp() {
    KuduTest::SetUp();

    FLAGS_heartbeat_interval_ms = 10;

    // Start minicluster and wait for tablet servers to connect to master.
    cluster_.reset(new MiniCluster(env_.get(), test_dir_, 1));
    ASSERT_STATUS_OK(cluster_->Start());

    // Connect to the cluster.
    KuduClientOptions opts;
    opts.master_server_addr = cluster_->mini_master()->bound_rpc_addr().ToString();

    ASSERT_STATUS_OK(KuduClient::Create(opts, &client_));

    // Set up two test tables inside the server. One has a single split, just so
    // that code is exercised a little more.
    ASSERT_STATUS_OK(client_->CreateTable(kTableName, schema_,
                                          CreateTableOptions()
                                            .WithSplitKeys(GenerateSplitKeys())));
    ASSERT_STATUS_OK(client_->CreateTable(kTable2Name, schema_));

    ASSERT_STATUS_OK(client_->OpenTable(kTableName, &client_table_));
    ASSERT_STATUS_OK(client_->OpenTable(kTable2Name, &client_table2_));
  }

  // Generate a set of split keys for tablets used in this test.
  vector<string> GenerateSplitKeys() {
    vector<string> keys;
    EncodedKeyBuilder builder(schema_);
    int val = 9;
    builder.AddColumnKey(&val);
    gscoped_ptr<EncodedKey> key(builder.BuildEncodedKey());
    keys.push_back(key->encoded_key().ToString());
    return keys;
  }

  virtual void TearDown() {
    if (cluster_) {
      cluster_.reset();
    }
    KuduTest::TearDown();
  }

 protected:

  static const char *kTableName;
  static const char *kTable2Name;
  static const uint32_t kNoBound;

  string GetFirstTabletId(KuduTable* table) {
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;
    req.mutable_table()->set_table_name(table->name());
    CHECK_OK(cluster_->mini_master()->master()->catalog_manager()->GetTableLocations(
        &req, &resp));
    CHECK(resp.tablet_locations_size() > 0);
    return resp.tablet_locations(0).tablet_id();
  }

  // Inserts 'num_rows' test rows via RPC.
  void InsertTestRows(int num_rows) {
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    session->SetTimeoutMillis(5000);
    for (int i = 0; i < num_rows; i++) {
      gscoped_ptr<Insert> insert(BuildTestRow(i));
      ASSERT_STATUS_OK(session->Apply(&insert));
      if (i % 25 == 0) { // to avoid backpressure
        ASSERT_STATUS_OK(session->Flush());
      }
    }
    ASSERT_STATUS_OK(session->Flush()); // and one more for good measure
  }

  gscoped_ptr<Insert> BuildTestRow(int index) {
    gscoped_ptr<Insert> insert = client_table_->NewInsert();
    PartialRow* row = insert->mutable_row();
    CHECK_OK(row->SetUInt32(0, index));
    CHECK_OK(row->SetUInt32(1, index * 2));
    CHECK_OK(row->SetStringCopy(2, Slice(StringPrintf("hello %d", index))));
    CHECK_OK(row->SetUInt32(3, index * 3));
    return insert.Pass();
  }

  void DoTestScanWithoutPredicates() {
    Schema projection = schema_.CreateKeyProjection();
    KuduScanner scanner(client_table_.get());
    ASSERT_STATUS_OK(scanner.SetProjection(&projection));
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
        rows.clear();
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
    ASSERT_STATUS_OK(scanner.SetProjection(&schema_));
    ColumnRangePredicatePB pred;
    ColumnSchemaToPB(schema_.column(2), pred.mutable_column());
    pred.set_lower_bound("hello 2");
    pred.set_upper_bound("hello 3");
    ASSERT_STATUS_OK(scanner.AddConjunctPredicate(pred));

    LOG_TIMING(INFO, "Scanning with string predicate") {
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
        rows.clear();
      }
    }
  }

  void DoTestScanWithKeyPredicate() {
    KuduScanner scanner(client_table_.get());
    ASSERT_STATUS_OK(scanner.SetProjection(&schema_));
    ColumnRangePredicatePB pred;
    ColumnSchemaToPB(schema_.column(0), pred.mutable_column());
    pred.set_lower_bound(EncodeUint32(5));
    pred.set_upper_bound(EncodeUint32(10));
    ASSERT_STATUS_OK(scanner.AddConjunctPredicate(pred));

    LOG_TIMING(INFO, "Scanning with key predicate") {
      ASSERT_STATUS_OK(scanner.Open());

      ASSERT_TRUE(scanner.HasMoreRows());
      vector<const uint8_t*> rows;
      while (scanner.HasMoreRows()) {
        ASSERT_STATUS_OK(scanner.NextBatch(&rows));

        BOOST_FOREACH(const uint8_t* row_ptr, rows) {
          ConstContiguousRow row(schema_, row_ptr);
          uint32_t k = *schema_.ExtractColumnFromRow<UINT32>(row, 0);
          if (k < 5 || k > 10) {
            FAIL() << schema_.DebugRow(row);
          }
        }
        rows.clear();
      }
    }
  }

  int CountRowsFromClient(KuduTable* table) {
    return CountRowsFromClient(table, kNoBound, kNoBound);
  }

  int CountRowsFromClient(KuduTable* table, uint32_t lower_bound, uint32_t upper_bound) {
    KuduScanner scanner(table);
    Schema empty_projection(vector<ColumnSchema>(), 0);
    CHECK_OK(scanner.SetProjection(&empty_projection));
    if (lower_bound != kNoBound || upper_bound != kNoBound) {
      ColumnRangePredicatePB pred;
      ColumnSchemaToPB(table->schema().column(0), pred.mutable_column());
      if (lower_bound != kNoBound) {
        pred.set_lower_bound(EncodeUint32(lower_bound));
      }
      if (upper_bound != kNoBound) {
        pred.set_upper_bound(EncodeUint32(upper_bound));
      }
      CHECK_OK(scanner.AddConjunctPredicate(pred));
    }
    CHECK_OK(scanner.Open());
    int count = 0;
    vector<const uint8_t*> rows;
    while (scanner.HasMoreRows()) {
      CHECK_OK(scanner.NextBatch(&rows));
      count += rows.size();
      rows.clear();
    }
    return count;
  }

  void ScanRowsToStrings(KuduTable* table, vector<string>* row_strings) {
    row_strings->clear();
    KuduScanner scanner(table);
    scanner.SetProjection(&schema_);
    ASSERT_STATUS_OK(scanner.Open());
    vector<const uint8_t*> rows;
    while (scanner.HasMoreRows()) {
      ASSERT_STATUS_OK(scanner.NextBatch(&rows));
      BOOST_FOREACH(const uint8_t* row_ptr, rows) {
        ConstContiguousRow row(schema_, row_ptr);
        row_strings->push_back(schema_.DebugRow(row));
      }
      rows.clear();
    }
  }

  void DoApplyWithoutFlushTest(int sleep_micros);

  enum WhichServerToKill {
    DEAD_MASTER,
    DEAD_TSERVER
  };
  void DoTestWriteWithDeadServer(WhichServerToKill which);

  Schema schema_;
  RowBuilder rb_;

  gscoped_ptr<MiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
  scoped_refptr<KuduTable> client_table_;
  scoped_refptr<KuduTable> client_table2_;
};

const char *ClientTest::kTableName = "client-testtb";
const char *ClientTest::kTable2Name = "client-testtb2";
const uint32_t ClientTest::kNoBound = kuint32max;

TEST_F(ClientTest, TestBadTable) {
  scoped_refptr<KuduTable> t;
  Status s = client_->OpenTable("xxx-does-not-exist", &t);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "Not found: The table does not exist");
}

// Test that, if the master is down, we get an appropriate error
// message.
TEST_F(ClientTest, TestMasterDown) {
  cluster_->mini_master()->Shutdown();
  scoped_refptr<KuduTable> t;
  Status s = client_->OpenTable("other-tablet", &t);
  ASSERT_TRUE(s.IsNetworkError());
  const string& msg = s.ToString();
  ASSERT_TRUE(msg.find("Connection refused") != string::npos ||
              msg.find("EOF") != string::npos ||
              msg.find("Connection reset") != string::npos)
      << "Unexpected client error message: " << msg;
}

TEST_F(ClientTest, TestScan) {
  InsertTestRows(FLAGS_test_scan_num_rows);

  DoTestScanWithoutPredicates();
  DoTestScanWithStringPredicate();
  DoTestScanWithKeyPredicate();
}

TEST_F(ClientTest, TestScanMultiTablet) {
  // 5 tablets, each with 10 rows worth of space.
  EncodedKeyBuilder key_builder(schema_);
  gscoped_ptr<EncodedKey> key;
  vector<string> keys;
  for (int i = 1; i < 5; i++) {
    int val = i * 10;
    key_builder.Reset();
    key_builder.AddColumnKey(&val);
    key.reset(key_builder.BuildEncodedKey());
    keys.push_back(key->encoded_key().ToString());
  }
  ASSERT_STATUS_OK(client_->CreateTable("TestScanMultiTablet", schema_,
                                        kudu::client::CreateTableOptions()
                                            .WithSplitKeys(keys)));
  scoped_refptr<KuduTable> table;
  ASSERT_STATUS_OK(client_->OpenTable("TestScanMultiTablet", &table));

  // Insert rows with keys 12, 13, 15, 17, 22, 23, 25, 27...57 into each
  // tablet, except the first which is empty.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(5000);
  for (int i = 1; i < 5; i++) {
    gscoped_ptr<Insert> insert;
    insert = BuildTestRow(2 + (i * 10));
    ASSERT_STATUS_OK(session->Apply(&insert));
    insert = BuildTestRow(3 + (i * 10));
    ASSERT_STATUS_OK(session->Apply(&insert));
    insert = BuildTestRow(5 + (i * 10));
    ASSERT_STATUS_OK(session->Apply(&insert));
    insert = BuildTestRow(7 + (i * 10));
    ASSERT_STATUS_OK(session->Apply(&insert));
  }
  ASSERT_STATUS_OK(session->Flush());

  // Run through various scans.
  ASSERT_EQ(16, CountRowsFromClient(client_table_.get(), kNoBound, kNoBound));
  ASSERT_EQ(3, CountRowsFromClient(client_table_.get(), kNoBound, 15));
  ASSERT_EQ(9, CountRowsFromClient(client_table_.get(), 27, kNoBound));
  ASSERT_EQ(3, CountRowsFromClient(client_table_.get(), 0, 15));
  ASSERT_EQ(0, CountRowsFromClient(client_table_.get(), 0, 10));
  ASSERT_EQ(4, CountRowsFromClient(client_table_.get(), 0, 20));
  ASSERT_EQ(8, CountRowsFromClient(client_table_.get(), 0, 30));
  ASSERT_EQ(6, CountRowsFromClient(client_table_.get(), 14, 30));
  ASSERT_EQ(0, CountRowsFromClient(client_table_.get(), 30, 30));
  ASSERT_EQ(0, CountRowsFromClient(client_table_.get(), 50, kNoBound));
}

TEST_F(ClientTest, TestScanEmptyTable) {
  KuduScanner scanner(client_table_.get());
  Schema empty_projection(vector<ColumnSchema>(), 0);
  ASSERT_STATUS_OK(scanner.SetProjection(&empty_projection));
  ASSERT_STATUS_OK(scanner.Open());

  // There are two tablets in the table, both empty. Until we scan to
  // the last tablet, HasMoreRows will return true (because it doesn't
  // know whether there's data in subsequent tablets).
  ASSERT_TRUE(scanner.HasMoreRows());
  vector<const uint8_t*> rows;
  ASSERT_STATUS_OK(scanner.NextBatch(&rows));
  ASSERT_TRUE(rows.empty());
  ASSERT_FALSE(scanner.HasMoreRows());
}

// Test scanning with an empty projection. This should yield an empty
// row block with the proper number of rows filled in. Impala issues
// scans like this in order to implement COUNT(*).
TEST_F(ClientTest, TestScanEmptyProjection) {
  InsertTestRows(FLAGS_test_scan_num_rows);
  Schema empty_projection(vector<ColumnSchema>(), 0);
  KuduScanner scanner(client_table_.get());
  ASSERT_STATUS_OK(scanner.SetProjection(&empty_projection));
  LOG_TIMING(INFO, "Scanning with no projected columns") {
    ASSERT_STATUS_OK(scanner.Open());

    ASSERT_TRUE(scanner.HasMoreRows());
    vector<const uint8_t*> rows;
    uint64_t count = 0;
    while (scanner.HasMoreRows()) {
      ASSERT_STATUS_OK(scanner.NextBatch(&rows));
      count += rows.size();
      rows.clear();
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
    ASSERT_STATUS_OK(scanner.SetProjection(&schema_));
    ASSERT_STATUS_OK(scanner.Open());
    ASSERT_EQ(0, manager->CountActiveScanners());
    scanner.Close();
    AssertScannersDisappear(manager);
  }

  // Open the scanner, make sure we see 1 registered scanner.
  {
    SCOPED_TRACE("Explicit close");
    KuduScanner scanner(client_table_.get());
    ASSERT_STATUS_OK(scanner.SetProjection(&schema_));
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
      ASSERT_STATUS_OK(scanner.SetProjection(&schema_));
      ASSERT_STATUS_OK(scanner.SetBatchSizeBytes(0));
      ASSERT_STATUS_OK(scanner.Open());
      ASSERT_EQ(1, manager->CountActiveScanners());
    }
    // Above scanner went out of scope, so the destructor should close asynchronously.
    AssertScannersDisappear(manager);
  }
}

// Simplest case of inserting through the client API: a single row
// with manual batching.
TEST_F(ClientTest, TestInsertSingleRowManualBatch) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_FALSE(session->HasPendingOperations());

  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  gscoped_ptr<Insert> insert = client_table_->NewInsert();
  // Try inserting without specifying a key: should fail.
  ASSERT_STATUS_OK(insert->mutable_row()->SetUInt32("int_val", 54321));
  ASSERT_STATUS_OK(insert->mutable_row()->SetStringCopy("string_val", "hello world"));

  Status s = session->Apply(&insert);
  ASSERT_EQ("Illegal state: Key not specified: "
            "INSERT uint32 int_val=54321, string string_val=hello world",
            s.ToString());

  ASSERT_STATUS_OK(insert->mutable_row()->SetUInt32("key", 12345));
  ASSERT_STATUS_OK(session->Apply(&insert));
  ASSERT_TRUE(insert == NULL) << "Successful insert should take ownership";
  ASSERT_TRUE(session->HasPendingOperations()) << "Should be pending until we Flush";

  ASSERT_STATUS_OK(session->Flush());
}

static Status ApplyInsertToSession(KuduSession* session,
                                   const scoped_refptr<KuduTable>& table,
                                   int row_key,
                                   int int_val,
                                   const char* string_val) {
  gscoped_ptr<Insert> insert = table->NewInsert();
  RETURN_NOT_OK(insert->mutable_row()->SetUInt32("key", row_key));
  RETURN_NOT_OK(insert->mutable_row()->SetUInt32("int_val", int_val));
  RETURN_NOT_OK(insert->mutable_row()->SetStringCopy("string_val", string_val));
  return session->Apply(&insert);
}

// Test which does an async flush and then drops the reference
// to the Session. This should still call the callback.
TEST_F(ClientTest, TestAsyncFlushResponseAfterSessionDropped) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "row"));
  Synchronizer s;
  session->FlushAsync(s.callback());
  session.reset();
  ASSERT_STATUS_OK(s.Wait());

  // Try again, this time with an error response (trying to re-insert the same row).
  s.Reset();
  session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "row"));
  ASSERT_EQ(1, session->CountBufferedOperations());
  session->FlushAsync(s.callback());
  ASSERT_EQ(0, session->CountBufferedOperations());
  session.reset();
  ASSERT_FALSE(s.Wait().ok());
}

// Test which sends multiple batches through the same session, each of which
// contains multiple rows spread across multiple tablets.
TEST_F(ClientTest, TestMultipleMultiRowManualBatches) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  const int kNumBatches = 5;
  const int kRowsPerBatch = 10;

  int row_key = 0;

  for (int batch_num = 0; batch_num < kNumBatches; batch_num++) {
    for (int i = 0; i < kRowsPerBatch; i++) {
      ASSERT_STATUS_OK(ApplyInsertToSession(
                         session.get(),
                         (row_key % 2 == 0) ? client_table_ : client_table2_,
                         row_key, row_key * 10, "hello world"));
      row_key++;
    }
    ASSERT_TRUE(session->HasPendingOperations()) << "Should be pending until we Flush";
    ASSERT_STATUS_OK(session->Flush());
    ASSERT_FALSE(session->HasPendingOperations()) << "Should have no more pending ops after flush";
  }

  const int kNumRowsPerTablet = kNumBatches * kRowsPerBatch / 2;
  ASSERT_EQ(kNumRowsPerTablet, CountRowsFromClient(client_table_.get()));
  ASSERT_EQ(kNumRowsPerTablet, CountRowsFromClient(client_table2_.get()));

  // Verify the data looks right.
  vector<string> rows;
  ScanRowsToStrings(client_table_.get(), &rows);
  std::sort(rows.begin(), rows.end());
  ASSERT_EQ(kNumRowsPerTablet, rows.size());
  ASSERT_EQ("(uint32 key=0, uint32 int_val=0, string string_val=hello world, "
            "uint32 non_null_with_default=12345)"
            , rows[0]);
}

// Test a batch where one of the inserted rows succeeds while another
// fails.
TEST_F(ClientTest, TestBatchWithPartialError) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Insert a row with key "1"
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "original row"));
  ASSERT_STATUS_OK(session->Flush());

  // Now make a batch that has key "1" (which will fail) along with
  // key "2" which will succeed. Flushing should return an error.
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "Attempted dup"));
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 2, 1, "Should succeed"));
  Status s = session->Flush();
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "Some errors occurred");

  // Fetch and verify the reported error.
  ASSERT_EQ(1, session->CountPendingErrors());
  vector<Error*> errors;
  ElementDeleter d(&errors);
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  ASSERT_FALSE(overflow);
  ASSERT_EQ(1, errors.size());
  ASSERT_TRUE(errors[0]->status().IsAlreadyPresent());
  ASSERT_EQ(errors[0]->failed_op().ToString(),
            "INSERT uint32 key=1, uint32 int_val=1, string string_val=Attempted dup");

  // Verify that the other row was successfully inserted
  vector<string> rows;
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(2, rows.size());
  std::sort(rows.begin(), rows.end());
  ASSERT_EQ("(uint32 key=1, uint32 int_val=1, string string_val=original row, "
            "uint32 non_null_with_default=12345)", rows[0]);
  ASSERT_EQ("(uint32 key=2, uint32 int_val=1, string string_val=Should succeed, "
            "uint32 non_null_with_default=12345)", rows[1]);
}

// Test flushing an empty batch (should be a no-op).
TEST_F(ClientTest, TestEmptyBatch) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_STATUS_OK(session->Flush());
}

void ClientTest::DoTestWriteWithDeadServer(WhichServerToKill which) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // Shut down the server.
  switch (which) {
    case DEAD_MASTER:
      cluster_->mini_master()->Shutdown();
      break;
    case DEAD_TSERVER:
      cluster_->mini_tablet_server(0)->Shutdown();
      break;
  }

  // Try a write.
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "x"));
  Status s = session->Flush();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_EQ(1, session->CountPendingErrors());

  vector<Error*> errors;
  ElementDeleter d(&errors);
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  ASSERT_FALSE(overflow);
  ASSERT_EQ(1, errors.size());
  ASSERT_TRUE(errors[0]->status().IsNetworkError());
  ASSERT_EQ(errors[0]->failed_op().ToString(),
            "INSERT uint32 key=1, uint32 int_val=1, string string_val=x");
}

// Test error handling cases where the master is down (tablet resolution fails)
TEST_F(ClientTest, TestWriteWithDeadMaster) {
  DoTestWriteWithDeadServer(DEAD_MASTER);
}

// Test error handling when the TS is down (actual write fails its RPC)
TEST_F(ClientTest, TestWriteWithDeadTabletServer) {
  DoTestWriteWithDeadServer(DEAD_TSERVER);
}

void ClientTest::DoApplyWithoutFlushTest(int sleep_micros) {
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  ASSERT_STATUS_OK(ApplyInsertToSession(session.get(), client_table_, 1, 1, "x"));
  usleep(sleep_micros);
  session.reset(); // should not crash!

  // Should have no rows.
  vector<string> rows;
  ScanRowsToStrings(client_table_.get(), &rows);
  ASSERT_EQ(0, rows.size());
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


TEST_F(ClientTest, TestWriteWithBadColumn) {
  scoped_refptr<KuduTable> table;
  ASSERT_STATUS_OK(client_->OpenTable(kTableName, &table));

  // Try to do a write with the bad schema.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  gscoped_ptr<Insert> insert = table->NewInsert();
  ASSERT_STATUS_OK(insert->mutable_row()->SetUInt32("key", 12345));
  Status s = insert->mutable_row()->SetUInt32("bad_col", 12345);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "No such column: bad_col");
}

// Do a write with a bad schema on the client side. This should make the Prepare
// phase of the write fail, which will result in an error on the RPC response.
TEST_F(ClientTest, TestWriteWithBadSchema) {
  scoped_refptr<KuduTable> table;
  ASSERT_STATUS_OK(client_->OpenTable(kTableName, &table));

  // Remove the 'int_val' column.
  // Now the schema on the client is "old"
  AlterTableBuilder alter;
  alter.DropColumn("int_val");
  ASSERT_STATUS_OK(client_->AlterTable(kTableName, alter));

  // Try to do a write with the bad schema.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  gscoped_ptr<Insert> insert = table->NewInsert();
  ASSERT_STATUS_OK(insert->mutable_row()->SetUInt32("key", 12345));
  ASSERT_STATUS_OK(insert->mutable_row()->SetUInt32("int_val", 12345));
  ASSERT_STATUS_OK(session->Apply(&insert));
  Status s = session->Flush();
  ASSERT_FALSE(s.ok());

  // Verify the specific error.
  vector<Error*> errors;
  ElementDeleter d(&errors);
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  ASSERT_FALSE(overflow);
  ASSERT_EQ(1, errors.size());
  ASSERT_TRUE(errors[0]->status().IsInvalidArgument());
  ASSERT_EQ(errors[0]->status().ToString(),
            "Invalid argument: Client provided column int_val[uint32 NOT NULL] "
            "not present in tablet");
  ASSERT_EQ(errors[0]->failed_op().ToString(),
            "INSERT uint32 key=12345, uint32 int_val=12345");
}

TEST_F(ClientTest, TestBasicAlterOperations) {
  // TODO: These tests explicitly use a single-tablet table (client_table2_),
  // because multi-tablet tables are prone to deadlocking.
  //
  // See KUDU-273 for more details.
  AlterTableBuilder alter;

  // test that remove key should throws an error
  {
    alter.Reset();
    alter.DropColumn("key");
    Status s = client_->AlterTable(kTable2Name, alter);
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "cannot remove a key column");
  }

  // test that renaming a key should throws an error
  {
    alter.Reset();
    alter.RenameColumn("key", "key2");
    Status s = client_->AlterTable(kTable2Name, alter);
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "cannot rename a key column");
  }

  // test that renaming to an already-existing name throws an error
  {
    alter.Reset();
    alter.RenameColumn("int_val", "string_val");
    Status s = client_->AlterTable(kTable2Name, alter);
    ASSERT_TRUE(s.IsAlreadyPresent());
    ASSERT_STR_CONTAINS(s.ToString(), "The column already exists: string_val");
  }

  // Need a tablet peer for the next set of tests.
  string tablet_id = GetFirstTabletId(client_table2_.get());
  shared_ptr<TabletPeer> tablet_peer;
  ASSERT_TRUE(cluster_->mini_tablet_server(0)->server()->tablet_manager()->LookupTablet(
      tablet_id, &tablet_peer));

  {
    alter.Reset();
    alter.DropColumn("int_val");
    alter.AddNullableColumn("new_col", UINT32);
    ASSERT_STATUS_OK(client_->AlterTable(kTable2Name, alter));
    ASSERT_EQ(1, tablet_peer->tablet()->metadata()->schema_version());
  }

  // test that specifying an encoding incompatible with the column's
  // type throws an error
  {
    alter.Reset();
    alter.AddNullableColumn("new_string_val", STRING, ColumnStorageAttributes(GROUP_VARINT));
    Status s = client_->AlterTable(kTable2Name, alter);
    ASSERT_TRUE(s.IsNotSupported());
    ASSERT_STR_CONTAINS(s.ToString(), "Unsupported type/encoding pair");
    ASSERT_EQ(1, tablet_peer->tablet()->metadata()->schema_version());
  }

  {
    alter.Reset();
    alter.AddNullableColumn("new_string_val", STRING, ColumnStorageAttributes(PREFIX_ENCODING));
    ASSERT_STATUS_OK(client_->AlterTable(kTable2Name, alter));
    ASSERT_EQ(2, tablet_peer->tablet()->metadata()->schema_version());
  }

  {
    const char *kRenamedTableName = "RenamedTable";
    alter.Reset();
    alter.RenameTable(kRenamedTableName);
    ASSERT_STATUS_OK(client_->AlterTable(kTable2Name, alter));
    ASSERT_EQ(3, tablet_peer->tablet()->metadata()->schema_version());
    ASSERT_EQ(kRenamedTableName, tablet_peer->tablet()->metadata()->table_name());

    CatalogManager *catalog_manager = cluster_->mini_master()->master()->catalog_manager();
    ASSERT_TRUE(catalog_manager->TableNameExists(kRenamedTableName));
    ASSERT_FALSE(catalog_manager->TableNameExists(kTable2Name));
  }
}

TEST_F(ClientTest, TestDeleteTable) {
  string tablet_id = GetFirstTabletId(client_table2_.get());

  // Remove the table
  // NOTE that it returns when the operation is completed on the master side
  ASSERT_STATUS_OK(client_->DeleteTable(kTable2Name));
  CatalogManager *catalog_manager = cluster_->mini_master()->master()->catalog_manager();
  ASSERT_FALSE(catalog_manager->TableNameExists(kTable2Name));

  // Wait until the table is removed from the TS
  int wait_time = 1000;
  bool tablet_found = true;
  for (int i = 0; i < 80 && tablet_found; ++i) {
    shared_ptr<TabletPeer> tablet_peer;
    tablet_found = cluster_->mini_tablet_server(0)->server()->tablet_manager()->LookupTablet(
                      tablet_id, &tablet_peer);
    usleep(wait_time);
    wait_time = std::min(wait_time * 5 / 4, 1000000);
  }
  ASSERT_FALSE(tablet_found);

  // Try to open the deleted table
  Status s = client_->OpenTable(kTable2Name, &client_table_);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "The table does not exist");
}

TEST_F(ClientTest, TestGetTableSchema) {
  Schema schema;

  // Verify the schema for the current table
  ASSERT_STATUS_OK(client_->GetTableSchema(kTableName, &schema));
  ASSERT_TRUE(schema_.Equals(schema));

  // Verify that a get schema request for a missing table throws not found
  Status s = client_->GetTableSchema("MissingTableName", &schema);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "The table does not exist");
}

TEST_F(ClientTest, TestStaleLocations) {
  string tablet_id = GetFirstTabletId(client_table2_.get());

  // The Tablet is up and running the location should not be stale
  master::TabletLocationsPB locs_pb;
  ASSERT_TRUE(cluster_->mini_master()->master()->catalog_manager()->GetTabletLocations(
                  tablet_id, &locs_pb));
  ASSERT_FALSE(locs_pb.stale());

  // On Master restart and no tablet report we expect the locations to be stale
  cluster_->mini_tablet_server(0)->Shutdown();
  cluster_->mini_master()->Restart();
  ASSERT_TRUE(cluster_->mini_master()->master()->catalog_manager()->GetTabletLocations(
                  tablet_id, &locs_pb));
  ASSERT_TRUE(locs_pb.stale());

  // Restart the TS and Wait for the tablets to be reported to the master.
  cluster_->mini_tablet_server(0)->Start();
  ASSERT_STATUS_OK(cluster_->WaitForTabletServerCount(1));
  ASSERT_TRUE(cluster_->mini_master()->master()->catalog_manager()->GetTabletLocations(
                  tablet_id, &locs_pb));

  // It may take a while to bootstrap the tablet and send the location report
  // so spin until we get a non-stale location.
  int wait_time = 1000;
  for (int i = 0; i < 80; ++i) {
    ASSERT_TRUE(cluster_->mini_master()->master()->catalog_manager()->GetTabletLocations(
                    tablet_id, &locs_pb));
    if (!locs_pb.stale()) {
      break;
    }
    usleep(wait_time);
    wait_time = std::min(wait_time * 5 / 4, 1000000);
  }
  ASSERT_FALSE(locs_pb.stale());
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

  // Add two more tablet servers so that the Create can succeed.
  for (int i = 0; i < 2; i++) {
    ASSERT_STATUS_OK(cluster_->AddTabletServer());
  }

  ASSERT_STATUS_OK(client_->CreateTable(kReplicatedTable, schema_,
                                        CreateTableOptions()
                                        .WithNumReplicas(3)
                                        .WithSplitKeys(GenerateSplitKeys())));
  scoped_refptr<KuduTable> table;
  ASSERT_STATUS_OK(client_->OpenTable(kReplicatedTable, &table));

  // Should have no rows to begin with.
  ASSERT_EQ(0, CountRowsFromClient(table.get()));

  // Insert some data.
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_STATUS_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  for (int row_key = 0; row_key < kNumRowsToWrite; row_key++) {
    ASSERT_STATUS_OK(ApplyInsertToSession(
                       session.get(), table, row_key, row_key * 10, "hello world"));
  }

  ASSERT_STATUS_OK(session->Flush());

  // Should now see the data.
  ASSERT_EQ(kNumRowsToWrite, CountRowsFromClient(table.get()));

  // TODO: once leader re-election is in, should somehow force a re-election
  // and ensure that the client handles refreshing the leader.
}

} // namespace client
} // namespace kudu
