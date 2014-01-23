// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include "benchmarks/tpch/rpc_line_item_dao.h"
#include "benchmarks/tpch/tpch-schemas.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "common/row.h"
#include "common/wire_protocol.h"
#include "common/row_changelist.h"
#include "util/status.h"
#include "util/test_util.h"
#include "master/master-test-util.h"
#include "master/mini_master.h"
#include "tserver/mini_tablet_server.h"
#include "integration-tests/mini_cluster.h"

namespace kudu {

using tserver::MiniTabletServer;
using tserver::ColumnRangePredicatePB;

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

  ConstContiguousRow BuildTestRow(int order, int line) {
    rb_.Reset();
    rb_.AddUint32(order);
    rb_.AddUint32(line);
    rb_.AddUint32(line);
    rb_.AddUint32(line);
    rb_.AddUint32(line);
    rb_.AddUint32(line);
    rb_.AddUint32(line);
    rb_.AddUint32(line);
    rb_.AddString(StringPrintf("hello %d", line));
    rb_.AddString(StringPrintf("hello %d", line));
    rb_.AddString(Slice("2013-11-13"));
    rb_.AddString(Slice("2013-11-13"));
    rb_.AddString(Slice("2013-11-13"));
    rb_.AddString(StringPrintf("hello %d", line));
    rb_.AddString(StringPrintf("hello %d", line));
    rb_.AddString(StringPrintf("hello %d", line));
    return rb_.row();
  }

  int CountRows() {
    Schema query_schema = schema_.CreateKeyProjection();
    ColumnRangePredicatePB pred;
    dao_->OpenScanner(query_schema, pred);
    vector<const uint8_t*> rows;
    int count = 0;
    while (dao_->HasMore()) {
      dao_->GetNext(&rows);
      count += rows.size();
    }
    return count;
  }
}; // class RpcLineItemDAOTest

TEST_F(RpcLineItemDAOTest, TestInsert) {
  ConstContiguousRow row(BuildTestRow(1, 1));
  dao_->WriteLine(row);
  dao_->FinishWriting();
  ASSERT_EQ(1, CountRows());
  for (int i = 2; i < 7; i++) {
    for (int y = 0; y < 5; y++) {
      ConstContiguousRow r(BuildTestRow(i, y));
      dao_->WriteLine(r);
    }
  }
  dao_->FinishWriting();
  ASSERT_EQ(26, CountRows());
}

TEST_F(RpcLineItemDAOTest, TestUpdate) {
  ConstContiguousRow row(BuildTestRow(1, 1));
  dao_->WriteLine(row);
  dao_->FinishWriting();

  RowBuilder rb(schema_.CreateKeyProjection());
  rb.AddUint32(1);
  rb.AddUint32(1);
  faststring mutations;
  RowChangeListEncoder encoder(schema_, &mutations);
  int new_val = 2;
  encoder.AddColumnUpdate(2, &new_val);
  dao_->MutateLine(rb.row(), mutations);
  dao_->FinishWriting();

  ColumnRangePredicatePB pred;
  dao_->OpenScanner(schema_, pred);
  vector<const uint8_t*> rows;
  while (dao_->HasMore()) {
    dao_->GetNext(&rows);
    BOOST_FOREACH(const uint8_t* row_ptr, rows) {
      ConstContiguousRow row(schema_, row_ptr);
      uint32_t l_quantity = *schema_.ExtractColumnFromRow<UINT32>(row, 2);
      ASSERT_EQ(new_val, l_quantity);
    }
  }
}

} // namespace kudu
