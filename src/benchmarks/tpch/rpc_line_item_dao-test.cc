// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include "benchmarks/tpch/rpc_line_item_dao.h"
#include "benchmarks/tpch/tpch-schemas.h"
#include "common/partial_row.h"
#include "common/row.h"
#include "common/row_changelist.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "common/wire_protocol.h"
#include "integration-tests/mini_cluster.h"
#include "master/master-test-util.h"
#include "master/mini_master.h"
#include "tserver/mini_tablet_server.h"
#include "util/status.h"
#include "util/test_util.h"

namespace kudu {

using tserver::MiniTabletServer;

class RpcLineItemDAOTest : public KuduTest {

 public:
  RpcLineItemDAOTest() : schema_(tpch::CreateLineItemSchema()), rb_(schema_) {}

  virtual void SetUp() {
    KuduTest::SetUp();

    // Start minicluster
    cluster_.reset(new MiniCluster(env_.get(), test_dir_, 1));
    ASSERT_STATUS_OK(cluster_->Start());

    const char *kTableName = "tpch1";

    // Create the table and Connect to it.
    string master_address(cluster_->mini_master()->bound_rpc_addr().ToString());
    dao_.reset(new kudu::RpcLineItemDAO(master_address, kTableName, 5));
    dao_->Init();
  }

 protected:
  gscoped_ptr<MiniCluster> cluster_;
  gscoped_ptr<RpcLineItemDAO> dao_;
  Schema schema_;
  RowBuilder rb_;

  static void BuildTestRow(int order, int line, PartialRow* row) {
    CHECK_OK(row->SetUInt32(tpch::kOrderKeyColIdx, order));
    CHECK_OK(row->SetUInt32(tpch::kLineNumberColIdx, line));
    CHECK_OK(row->SetUInt32(tpch::kPartKeyColIdx, 12345));
    CHECK_OK(row->SetUInt32(tpch::kSuppKeyColIdx, 12345));
    CHECK_OK(row->SetUInt32(tpch::kQuantityColIdx, 12345));
    CHECK_OK(row->SetUInt32(tpch::kExtendedPriceColIdx, 12345));
    CHECK_OK(row->SetUInt32(tpch::kDiscountColIdx, 12345));
    CHECK_OK(row->SetUInt32(tpch::kTaxColIdx, 12345));
    CHECK_OK(row->SetStringCopy(tpch::kReturnFlagColIdx, StringPrintf("hello %d", line)));
    CHECK_OK(row->SetStringCopy(tpch::kLineStatusColIdx, StringPrintf("hello %d", line)));
    CHECK_OK(row->SetStringCopy(tpch::kShipDateColIdx, Slice("2013-11-13")));
    CHECK_OK(row->SetStringCopy(tpch::kCommitDateColIdx, Slice("2013-11-13")));
    CHECK_OK(row->SetStringCopy(tpch::kReceiptDateColIdx, Slice("2013-11-13")));
    CHECK_OK(row->SetStringCopy(tpch::kShipInstructColIdx, StringPrintf("hello %d", line)));
    CHECK_OK(row->SetStringCopy(tpch::kShipModeColIdx, StringPrintf("hello %d", line)));
    CHECK_OK(row->SetStringCopy(tpch::kCommentColIdx, StringPrintf("hello %d", line)));
  }

  static void UpdateTestRow(int key, int line_number, int quantity, PartialRow* row) {
    CHECK_OK(row->SetUInt32(tpch::kOrderKeyColIdx, key));
    CHECK_OK(row->SetUInt32(tpch::kLineNumberColIdx, line_number));
    CHECK_OK(row->SetUInt32(tpch::kQuantityColIdx, quantity));
  }

  int CountRows() {
    Schema query_schema = schema_.CreateKeyProjection();
    ScanSpec spec;
    dao_->OpenScanner(query_schema, &spec);
    vector<client::KuduRowResult> rows;
    int count = 0;
    while (dao_->HasMore()) {
      dao_->GetNext(&rows);
      count += rows.size();
      rows.clear();
    }
    return count;
  }
}; // class RpcLineItemDAOTest

TEST_F(RpcLineItemDAOTest, TestInsert) {
  dao_->WriteLine(boost::bind(BuildTestRow, 1, 1, _1));
  dao_->FinishWriting();
  ASSERT_EQ(1, CountRows());
  for (int i = 2; i < 7; i++) {
    for (int y = 0; y < 5; y++) {
      dao_->WriteLine(boost::bind(BuildTestRow, i, y, _1));
    }
  }
  dao_->FinishWriting();
  ASSERT_EQ(26, CountRows());
}

TEST_F(RpcLineItemDAOTest, TestUpdate) {
  dao_->WriteLine(boost::bind(BuildTestRow, 1, 1, _1));
  dao_->FinishWriting();
  ASSERT_EQ(1, CountRows());

  dao_->MutateLine(boost::bind(UpdateTestRow, 1, 1, 12345, _1));
  dao_->FinishWriting();
  ScanSpec spec;
  dao_->OpenScanner(schema_, &spec);
  vector<client::KuduRowResult> rows;
  while (dao_->HasMore()) {
    dao_->GetNext(&rows);
    BOOST_FOREACH(const client::KuduRowResult& row, rows) {
      uint32_t l_quantity;
      ASSERT_STATUS_OK(row.GetUInt32(tpch::kQuantityColIdx, &l_quantity));
      ASSERT_EQ(12345, l_quantity);
    }
    rows.clear();
  }
}

} // namespace kudu
