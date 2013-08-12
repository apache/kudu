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
#include "tablet/tablet.h"
#include "tserver/mini_tablet_server.h"
#include "tserver/tablet_server.h"
#include "util/net/sockaddr.h"
#include "util/status.h"
#include "util/stopwatch.h"
#include "util/test_util.h"

DEFINE_int32(test_scan_num_rows, 1000, "Number of rows to insert and scan");

using std::tr1::shared_ptr;

namespace kudu {
namespace client {

using tserver::MiniTabletServer;
using tserver::ColumnRangePredicatePB;
using tablet::Tablet;

class ClientTest : public KuduTest {
 public:
  ClientTest()
    : schema_(boost::assign::list_of
              (ColumnSchema("key", UINT32))
              (ColumnSchema("int_val", UINT32))
              (ColumnSchema("string_val", STRING)),
              1),
      rb_(schema_) {
  }

  virtual void SetUp() {
    KuduTest::SetUp();

    // Start server.
    mini_server_.reset(new MiniTabletServer(env_.get(), GetTestPath("TabletServerTest-fsroot")));
    ASSERT_STATUS_OK(mini_server_->Start());

    // Set up a tablet inside the server.
    ASSERT_STATUS_OK(mini_server_->AddTestTablet(kTabletId, schema_));
    ASSERT_TRUE(mini_server_->server()->LookupTablet(kTabletId, &tablet_));

    // Connect to it.
    KuduClientOptions opts;
    opts.tablet_server_addr = mini_server_->bound_addr().ToString();
    ASSERT_STATUS_OK(KuduClient::Create(opts, &client_));
    ASSERT_STATUS_OK(client_->OpenTable(kTabletId, &client_table_));
  }

 protected:

  static const char* const kTabletId;

  // Inserts 'num_rows' test rows directly into the tablet (i.e not via RPC)
  void InsertTestRows(int num_rows) {
    for (int i = 0; i < num_rows; i++) {
      CHECK_OK(tablet_->Insert(BuildTestRow(i)));
    }
    CHECK_OK(tablet_->Flush());
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
      uint64_t expected = implicit_cast<uint64_t>(FLAGS_test_scan_num_rows) * (0 + (FLAGS_test_scan_num_rows - 1)) / 2;
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

  gscoped_ptr<MiniTabletServer> mini_server_;
  shared_ptr<KuduClient> client_;
  shared_ptr<KuduTable> client_table_;
  shared_ptr<Tablet> tablet_;
};

const char* const ClientTest::kTabletId = "TestTablet";

TEST_F(ClientTest, TestScan) {
  InsertTestRows(FLAGS_test_scan_num_rows);

  DoTestScanWithoutPredicates();
  DoTestScanWithStringPredicate();
}

} // namespace client
} // namespace kudu
