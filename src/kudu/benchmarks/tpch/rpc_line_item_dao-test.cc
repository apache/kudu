// Copyright (c) 2013, Cloudera, inc.

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <string>
#include <vector>

#include "kudu/benchmarks/tpch/rpc_line_item_dao.h"
#include "kudu/benchmarks/tpch/tpch-schemas.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/mini_master.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

namespace kudu {

using client::KuduColumnRangePredicate;
using client::KuduRowResult;
using client::KuduSchema;
using std::string;
using std::vector;

class RpcLineItemDAOTest : public KuduTest {

 public:
  RpcLineItemDAOTest() : schema_(tpch::CreateLineItemSchema()) {}

  virtual void SetUp() OVERRIDE {
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

  virtual void TearDown() OVERRIDE {
    cluster_->Shutdown();
    KuduTest::TearDown();
  }

 protected:
  gscoped_ptr<MiniCluster> cluster_;
  gscoped_ptr<RpcLineItemDAO> dao_;
  KuduSchema schema_;

  static void BuildTestRow(int order, int line, KuduPartialRow* row) {
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

  static void UpdateTestRow(int key, int line_number, int quantity, KuduPartialRow* row) {
    CHECK_OK(row->SetUInt32(tpch::kOrderKeyColIdx, key));
    CHECK_OK(row->SetUInt32(tpch::kLineNumberColIdx, line_number));
    CHECK_OK(row->SetUInt32(tpch::kQuantityColIdx, quantity));
  }

  int CountRows() {
    KuduSchema query_schema = schema_.CreateKeyProjection();
    vector<KuduColumnRangePredicate> preds;
    dao_->OpenScanner(query_schema, preds);
    vector<KuduRowResult> rows;
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
  vector<KuduColumnRangePredicate> preds;
  dao_->OpenScanner(schema_, preds);
  vector<KuduRowResult> rows;
  while (dao_->HasMore()) {
    dao_->GetNext(&rows);
    BOOST_FOREACH(const KuduRowResult& row, rows) {
      uint32_t l_quantity;
      ASSERT_STATUS_OK(row.GetUInt32(tpch::kQuantityColIdx, &l_quantity));
      ASSERT_EQ(12345, l_quantity);
    }
    rows.clear();
  }
}

} // namespace kudu
