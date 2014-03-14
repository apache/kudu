// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include "benchmarks/tpch/local_line_item_dao.h"
#include "benchmarks/tpch/tpch-schemas.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "common/row.h"
#include "common/wire_protocol.h"
#include "common/row_changelist.h"
#include "common/partial_row.h"
#include "util/status.h"
#include "util/test_util.h"

namespace kudu {

using tserver::ColumnRangePredicatePB;

class LocalLineItemDAOTest : public KuduTest {

 public:
  LocalLineItemDAOTest() : schema_(tpch::CreateLineItemSchema()), rb_(schema_) {}

  virtual void SetUp() {
    KuduTest::SetUp();

    // Create the table and Connect to it.
    dao_.reset(new kudu::LocalLineItemDAO(test_dir_));
    dao_->Init();
  }

 protected:
  gscoped_ptr<LineItemDAO> dao_;
  Schema schema_;
  RowBuilder rb_;

  void BuildTestRow(int order, int line, PartialRow* row) {
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

  int CountRows() {
    Schema query_schema = schema_.CreateKeyProjection();
    ColumnRangePredicatePB pred;
    ScanSpec spec;
    dao_->OpenScanner(query_schema, &spec);
    int count = 0;
    while (dao_->HasMore()) {
      gscoped_ptr<Arena> arena(new Arena(256*1000, 256*1000*1000));
      RowBlock rows(schema_, 1000, arena.get());
      dao_->GetNext(&rows);
      count += rows.nrows();
    }
    return count;
  }
}; // class LocalLineItemDAOTest

TEST_F(LocalLineItemDAOTest, TestInsert) {
  PartialRow row(&schema_);
  BuildTestRow(1, 1, &row);
  dao_->WriteLine(row);
  dao_->FinishWriting();
  ASSERT_EQ(1, CountRows());
  for (int i = 2; i < 7; i++) {
    for (int y = 0; y < 5; y++) {
      BuildTestRow(i, y, &row);
      dao_->WriteLine(row);
    }
  }
  dao_->FinishWriting();
  ASSERT_EQ(26, CountRows());
}

// FIXME: Something is wrong with this test.
TEST_F(LocalLineItemDAOTest, DISABLED_TestUpdate) {
  PartialRow row(&schema_);
  BuildTestRow(1, 1, &row);
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

  const uint32_t rowid = 1;
  SimpleConstCell c(schema_.column_by_id(1), &rowid);
  ColumnRangePredicate pred(schema_.column_by_id(1),
                            boost::optional<const void*>(&c),
                            boost::optional<const void*>(&c));
  ScanSpec spec;
  spec.AddPredicate(pred);
  dao_->OpenScanner(schema_, &spec);
  while (dao_->HasMore()) {
    LOG(INFO) << "Seen a row";
    gscoped_ptr<Arena> arena(new Arena(256*1000, 256*1000*1000));
    RowBlock rows(schema_, 1000, arena.get());
    dao_->GetNext(&rows);
    for (size_t i = 0; i < rows.nrows(); i++) {
      RowBlockRow row = rows.row(i);
      uint32_t l_quantity = *schema_.ExtractColumnFromRow<UINT32>(row, 2);
      ASSERT_EQ(new_val, l_quantity);
    }
  }
}

} // namespace kudu
