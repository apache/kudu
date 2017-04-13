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

#include <vector>

#include <boost/optional.hpp>
#include <gtest/gtest.h>
#include "kudu/common/column_predicate.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::vector;

namespace kudu {

class WireProtocolTest : public KuduTest {
 public:
  WireProtocolTest()
      : schema_({ ColumnSchema("col1", STRING),
              ColumnSchema("col2", STRING),
              ColumnSchema("col3", UINT32, true /* nullable */) },
        1),
        test_data_arena_(4096, 256 * 1024) {
  }

  void FillRowBlockWithTestRows(RowBlock* block) {
    test_data_arena_.Reset();
    block->selection_vector()->SetAllTrue();

    for (int i = 0; i < block->nrows(); i++) {
      RowBlockRow row = block->row(i);

      // We make new copies of these strings into the Arena for each row so that
      // the workload is more realistic. If we just re-use the same Slice object
      // for each row, the memory accesses fit entirely into a smaller number of
      // cache lines and we may micro-optimize for the wrong thing.
      Slice col1, col2;
      CHECK(test_data_arena_.RelocateSlice("hello world col1", &col1));
      CHECK(test_data_arena_.RelocateSlice("hello world col2", &col2));
      *reinterpret_cast<Slice*>(row.mutable_cell_ptr(0)) = col1;
      *reinterpret_cast<Slice*>(row.mutable_cell_ptr(1)) = col2;
      *reinterpret_cast<uint32_t*>(row.mutable_cell_ptr(2)) = i;
      row.cell(2).set_null(false);
    }
  }
 protected:
  Schema schema_;
  Arena test_data_arena_;
};

TEST_F(WireProtocolTest, TestOKStatus) {
  Status s = Status::OK();
  AppStatusPB pb;
  StatusToPB(s, &pb);
  EXPECT_EQ(AppStatusPB::OK, pb.code());
  EXPECT_FALSE(pb.has_message());
  EXPECT_FALSE(pb.has_posix_code());

  Status s2 = StatusFromPB(pb);
  ASSERT_OK(s2);
}

TEST_F(WireProtocolTest, TestBadStatus) {
  Status s = Status::NotFound("foo", "bar");
  AppStatusPB pb;
  StatusToPB(s, &pb);
  EXPECT_EQ(AppStatusPB::NOT_FOUND, pb.code());
  EXPECT_TRUE(pb.has_message());
  EXPECT_EQ("foo: bar", pb.message());
  EXPECT_FALSE(pb.has_posix_code());

  Status s2 = StatusFromPB(pb);
  EXPECT_TRUE(s2.IsNotFound());
  EXPECT_EQ(s.ToString(), s2.ToString());
}

TEST_F(WireProtocolTest, TestBadStatusWithPosixCode) {
  Status s = Status::NotFound("foo", "bar", 1234);
  AppStatusPB pb;
  StatusToPB(s, &pb);
  EXPECT_EQ(AppStatusPB::NOT_FOUND, pb.code());
  EXPECT_TRUE(pb.has_message());
  EXPECT_EQ("foo: bar", pb.message());
  EXPECT_TRUE(pb.has_posix_code());
  EXPECT_EQ(1234, pb.posix_code());

  Status s2 = StatusFromPB(pb);
  EXPECT_TRUE(s2.IsNotFound());
  EXPECT_EQ(1234, s2.posix_code());
  EXPECT_EQ(s.ToString(), s2.ToString());
}

TEST_F(WireProtocolTest, TestSchemaRoundTrip) {
  google::protobuf::RepeatedPtrField<ColumnSchemaPB> pbs;

  ASSERT_OK(SchemaToColumnPBs(schema_, &pbs));
  ASSERT_EQ(3, pbs.size());

  // Column 0.
  EXPECT_TRUE(pbs.Get(0).is_key());
  EXPECT_EQ("col1", pbs.Get(0).name());
  EXPECT_EQ(STRING, pbs.Get(0).type());
  EXPECT_FALSE(pbs.Get(0).is_nullable());

  // Column 1.
  EXPECT_FALSE(pbs.Get(1).is_key());
  EXPECT_EQ("col2", pbs.Get(1).name());
  EXPECT_EQ(STRING, pbs.Get(1).type());
  EXPECT_FALSE(pbs.Get(1).is_nullable());

  // Column 2.
  EXPECT_FALSE(pbs.Get(2).is_key());
  EXPECT_EQ("col3", pbs.Get(2).name());
  EXPECT_EQ(UINT32, pbs.Get(2).type());
  EXPECT_TRUE(pbs.Get(2).is_nullable());

  // Convert back to a Schema object and verify they're identical.
  Schema schema2;
  ASSERT_OK(ColumnPBsToSchema(pbs, &schema2));
  EXPECT_EQ(schema_.ToString(), schema2.ToString());
  EXPECT_EQ(schema_.num_key_columns(), schema2.num_key_columns());
}

// Test that, when non-contiguous key columns are passed, an error Status
// is returned.
TEST_F(WireProtocolTest, TestBadSchema_NonContiguousKey) {
  google::protobuf::RepeatedPtrField<ColumnSchemaPB> pbs;

  // Column 0: key
  ColumnSchemaPB* col_pb = pbs.Add();
  col_pb->set_name("c0");
  col_pb->set_type(STRING);
  col_pb->set_is_key(true);

  // Column 1: not a key
  col_pb = pbs.Add();
  col_pb->set_name("c1");
  col_pb->set_type(STRING);
  col_pb->set_is_key(false);

  // Column 2: marked as key. This is an error.
  col_pb = pbs.Add();
  col_pb->set_name("c2");
  col_pb->set_type(STRING);
  col_pb->set_is_key(true);

  Schema schema;
  Status s = ColumnPBsToSchema(pbs, &schema);
  ASSERT_STR_CONTAINS(s.ToString(), "Got out-of-order key column");
}

// Test that, when multiple columns with the same name are passed, an
// error Status is returned.
TEST_F(WireProtocolTest, TestBadSchema_DuplicateColumnName) {
  google::protobuf::RepeatedPtrField<ColumnSchemaPB> pbs;

  // Column 0:
  ColumnSchemaPB* col_pb = pbs.Add();
  col_pb->set_name("c0");
  col_pb->set_type(STRING);
  col_pb->set_is_key(true);

  // Column 1:
  col_pb = pbs.Add();
  col_pb->set_name("c1");
  col_pb->set_type(STRING);
  col_pb->set_is_key(false);

  // Column 2: same name as column 0
  col_pb = pbs.Add();
  col_pb->set_name("c0");
  col_pb->set_type(STRING);
  col_pb->set_is_key(false);

  Schema schema;
  Status s = ColumnPBsToSchema(pbs, &schema);
  ASSERT_EQ("Invalid argument: Duplicate column name: c0", s.ToString());
}

// Create a block of rows in columnar layout and ensure that it can be
// converted to and from protobuf.
TEST_F(WireProtocolTest, TestColumnarRowBlockToPB) {
  Arena arena(1024, 1024 * 1024);
  RowBlock block(schema_, 10, &arena);
  FillRowBlockWithTestRows(&block);

  // Convert to PB.
  RowwiseRowBlockPB pb;
  faststring direct, indirect;
  SerializeRowBlock(block, &pb, nullptr, &direct, &indirect);
  SCOPED_TRACE(SecureDebugString(pb));
  SCOPED_TRACE("Row data: " + direct.ToString());
  SCOPED_TRACE("Indirect data: " + indirect.ToString());

  // Convert back to a row, ensure that the resulting row is the same
  // as the one we put in.
  vector<const uint8_t*> row_ptrs;
  Slice direct_sidecar = direct;
  ASSERT_OK(ExtractRowsFromRowBlockPB(schema_, pb, indirect,
                                             &direct_sidecar, &row_ptrs));
  ASSERT_EQ(block.nrows(), row_ptrs.size());
  for (int i = 0; i < block.nrows(); ++i) {
    ConstContiguousRow row_roundtripped(&schema_, row_ptrs[i]);
    EXPECT_EQ(schema_.DebugRow(block.row(i)),
              schema_.DebugRow(row_roundtripped));
  }
}

// Create a block of rows in columnar layout and ensure that it can be
// converted to and from protobuf.
TEST_F(WireProtocolTest, TestColumnarRowBlockToPBWithPadding) {
  int kNumRows = 10;
  Arena arena(1024, 1024 * 1024);
  // Create a schema with multiple UNIXTIME_MICROS columns in different
  // positions.
  Schema tablet_schema({ ColumnSchema("key", UNIXTIME_MICROS),
                         ColumnSchema("col1", STRING),
                         ColumnSchema("col2", UNIXTIME_MICROS),
                         ColumnSchema("col3", INT32, true /* nullable */),
                         ColumnSchema("col4", UNIXTIME_MICROS, true /* nullable */)}, 1);
  RowBlock block(tablet_schema, kNumRows, &arena);
  block.selection_vector()->SetAllTrue();

  for (int i = 0; i < block.nrows(); i++) {
    RowBlockRow row = block.row(i);

    *reinterpret_cast<int64_t*>(row.mutable_cell_ptr(0)) = i;
    Slice col1;
    // See: FillRowBlockWithTestRows() for the reason why we relocate these
    // to 'test_data_arena_'.
    CHECK(test_data_arena_.RelocateSlice("hello world col1", &col1));
    *reinterpret_cast<Slice*>(row.mutable_cell_ptr(1)) = col1;
    *reinterpret_cast<int64_t*>(row.mutable_cell_ptr(2)) = i;
    *reinterpret_cast<int32_t*>(row.mutable_cell_ptr(3)) = i;
    row.cell(3).set_null(false);
    *reinterpret_cast<int64_t*>(row.mutable_cell_ptr(4)) = i;
    row.cell(4).set_null(true);
  }

  // Have the projection schema have columns in a different order from the table schema.
  Schema proj_schema({ ColumnSchema("col1", STRING),
                       ColumnSchema("key",  UNIXTIME_MICROS),
                       ColumnSchema("col2", UNIXTIME_MICROS),
                       ColumnSchema("col4", UNIXTIME_MICROS, true /* nullable */),
                       ColumnSchema("col3", INT32, true /* nullable */)}, 0);

  // Convert to PB.
  RowwiseRowBlockPB pb;
  faststring direct, indirect;
  SerializeRowBlock(block, &pb, &proj_schema, &direct, &indirect, true /* pad timestamps */);
  SCOPED_TRACE(SecureDebugString(pb));
  SCOPED_TRACE("Row data: " + HexDump(direct));
  SCOPED_TRACE("Indirect data: " + HexDump(indirect));

  // Convert back to a row, ensure that the resulting row is the same
  // as the one we put in. Can't reuse the decoding methods since we
  // won't support decoding padded rows within Kudu.
  vector<const uint8_t*> row_ptrs;
  Slice direct_sidecar = direct;
  Slice indirect_sidecar = indirect;
  ASSERT_OK(RewriteRowBlockPointers(proj_schema, pb, indirect_sidecar, &direct_sidecar, true));

  // Row stride is the normal size for the schema + the number of UNIXTIME_MICROS columns * 8,
  // the size of the padding per column.
  size_t row_stride = ContiguousRowHelper::row_size(proj_schema) + 3 * 8;
  ASSERT_EQ(direct_sidecar.size(), row_stride * kNumRows);
  const uint8_t* base_data;
  for (int i = 0; i < kNumRows; i++) {
    base_data = direct_sidecar.data() + i * row_stride;
    // With padding, the null bitmap is at offset 68.
    // See the calculations below to understand why.
    const uint8_t* null_bitmap = base_data + 68;

    // 'col1' comes at 0 bytes offset in the projection schema.
    const Slice* col1 = reinterpret_cast<const Slice*>(base_data);
    ASSERT_EQ(col1->compare(Slice("hello world col1")), 0) << "Unexpected val for the "
                                                           << i << "th row:"
                                                           << col1->ToDebugString();
    // 'key' comes at 16 bytes offset.
    const int64_t key = *reinterpret_cast<const int64_t*>(base_data + 16);
    EXPECT_EQ(key, i);

    // 'col2' comes at 32 bytes offset: 16 bytes previous, 16 bytes 'key'
    const int64_t col2 = *reinterpret_cast<const int64_t*>(base_data + 32);
    EXPECT_EQ(col2, i);

    // 'col4' is supposed to be null, but should also read 0 since we memsetted the
    // memory to 0. It should come at 48 bytes offset:  32 bytes previous + 8 bytes 'col2' +
    // 8 bytes padding.
    const int64_t col4 = *reinterpret_cast<const int64_t*>(base_data + 48);
    EXPECT_EQ(col4, 0);
    EXPECT_TRUE(BitmapTest(null_bitmap, 3));

    // 'col3' comes at 64 bytes offset: 48 bytes previous, 8 bytes 'col4', 8 bytes padding
    const int32_t col3 = *reinterpret_cast<const int32_t*>(base_data + 64);
    EXPECT_EQ(col3, i);
    EXPECT_FALSE(BitmapTest(null_bitmap, 4));
  }
}

#ifdef NDEBUG
TEST_F(WireProtocolTest, TestColumnarRowBlockToPBBenchmark) {
  Arena arena(1024, 1024 * 1024);
  const int kNumTrials = AllowSlowTests() ? 100 : 10;
  RowBlock block(schema_, 10000 * kNumTrials, &arena);
  FillRowBlockWithTestRows(&block);

  RowwiseRowBlockPB pb;

  LOG_TIMING(INFO, "Converting to PB") {
    for (int i = 0; i < kNumTrials; i++) {
      pb.Clear();
      faststring direct, indirect;
      SerializeRowBlock(block, &pb, NULL, &direct, &indirect);
    }
  }
}
#endif

// Test that trying to extract rows from an invalid block correctly returns
// Corruption statuses.
TEST_F(WireProtocolTest, TestInvalidRowBlock) {
  Schema schema({ ColumnSchema("col1", STRING) }, 1);
  RowwiseRowBlockPB pb;
  vector<const uint8_t*> row_ptrs;

  // Too short to be valid data.
  const char* shortstr = "x";
  pb.set_num_rows(1);
  Slice direct = shortstr;
  Status s = ExtractRowsFromRowBlockPB(schema, pb, Slice(), &direct, &row_ptrs);
  ASSERT_STR_CONTAINS(s.ToString(), "Corruption: Row block has 1 bytes of data");

  // Bad pointer into indirect data.
  shortstr = "xxxxxxxxxxxxxxxx";
  pb.set_num_rows(1);
  direct = Slice(shortstr);
  s = ExtractRowsFromRowBlockPB(schema, pb, Slice(), &direct, &row_ptrs);
  ASSERT_STR_CONTAINS(s.ToString(),
                      "Corruption: Row #0 contained bad indirect slice");
}

// Test serializing a block which has a selection vector but no columns.
// This is the sort of result that is returned from a scan with an empty
// projection (a COUNT(*) query).
TEST_F(WireProtocolTest, TestBlockWithNoColumns) {
  Schema empty(std::vector<ColumnSchema>(), 0);
  Arena arena(1024, 1024 * 1024);
  RowBlock block(empty, 1000, &arena);
  block.selection_vector()->SetAllTrue();
  // Unselect 100 rows
  for (int i = 0; i < 100; i++) {
    block.selection_vector()->SetRowUnselected(i * 2);
  }
  ASSERT_EQ(900, block.selection_vector()->CountSelected());

  // Convert it to protobuf, ensure that the results look right.
  RowwiseRowBlockPB pb;
  faststring direct, indirect;
  SerializeRowBlock(block, &pb, nullptr, &direct, &indirect);
  ASSERT_EQ(900, pb.num_rows());
}

TEST_F(WireProtocolTest, TestColumnDefaultValue) {
  Slice write_default_str("Hello Write");
  Slice read_default_str("Hello Read");
  uint32_t write_default_u32 = 512;
  uint32_t read_default_u32 = 256;
  ColumnSchemaPB pb;

  ColumnSchema col1("col1", STRING);
  ColumnSchemaToPB(col1, &pb);
  ColumnSchema col1fpb = ColumnSchemaFromPB(pb);
  ASSERT_FALSE(col1fpb.has_read_default());
  ASSERT_FALSE(col1fpb.has_write_default());
  ASSERT_TRUE(col1fpb.read_default_value() == nullptr);

  ColumnSchema col2("col2", STRING, false, &read_default_str);
  ColumnSchemaToPB(col2, &pb);
  ColumnSchema col2fpb = ColumnSchemaFromPB(pb);
  ASSERT_TRUE(col2fpb.has_read_default());
  ASSERT_FALSE(col2fpb.has_write_default());
  ASSERT_EQ(read_default_str, *static_cast<const Slice *>(col2fpb.read_default_value()));
  ASSERT_EQ(nullptr, static_cast<const Slice *>(col2fpb.write_default_value()));

  ColumnSchema col3("col3", STRING, false, &read_default_str, &write_default_str);
  ColumnSchemaToPB(col3, &pb);
  ColumnSchema col3fpb = ColumnSchemaFromPB(pb);
  ASSERT_TRUE(col3fpb.has_read_default());
  ASSERT_TRUE(col3fpb.has_write_default());
  ASSERT_EQ(read_default_str, *static_cast<const Slice *>(col3fpb.read_default_value()));
  ASSERT_EQ(write_default_str, *static_cast<const Slice *>(col3fpb.write_default_value()));

  ColumnSchema col4("col4", UINT32, false, &read_default_u32);
  ColumnSchemaToPB(col4, &pb);
  ColumnSchema col4fpb = ColumnSchemaFromPB(pb);
  ASSERT_TRUE(col4fpb.has_read_default());
  ASSERT_FALSE(col4fpb.has_write_default());
  ASSERT_EQ(read_default_u32, *static_cast<const uint32_t *>(col4fpb.read_default_value()));
  ASSERT_EQ(nullptr, static_cast<const uint32_t *>(col4fpb.write_default_value()));

  ColumnSchema col5("col5", UINT32, false, &read_default_u32, &write_default_u32);
  ColumnSchemaToPB(col5, &pb);
  ColumnSchema col5fpb = ColumnSchemaFromPB(pb);
  ASSERT_TRUE(col5fpb.has_read_default());
  ASSERT_TRUE(col5fpb.has_write_default());
  ASSERT_EQ(read_default_u32, *static_cast<const uint32_t *>(col5fpb.read_default_value()));
  ASSERT_EQ(write_default_u32, *static_cast<const uint32_t *>(col5fpb.write_default_value()));
}

TEST_F(WireProtocolTest, TestColumnPredicateInList) {
  ColumnSchema col1("col1", INT32);
  vector<ColumnSchema> cols = { col1 };
  Schema schema(cols, 1);
  Arena arena(1024,1024*1024);
  boost::optional<ColumnPredicate> predicate;

  { // col1 IN (5, 6, 10)
    int five = 5;
    int six = 6;
    int ten = 10;
    vector<const void*> values { &five, &six, &ten };

    kudu::ColumnPredicate cp = kudu::ColumnPredicate::InList(col1, &values);
    ColumnPredicatePB pb;
    ASSERT_NO_FATAL_FAILURE(ColumnPredicateToPB(cp, &pb));

    ASSERT_OK(ColumnPredicateFromPB(schema, &arena, pb, &predicate));
    ASSERT_EQ(predicate->predicate_type(), PredicateType::InList);
    ASSERT_EQ(3, predicate->raw_values().size());
  }

  { // col1 IN (0, 0)
    // We can't construct a single element IN list directly since it would be
    // simplified to an equality predicate, so we hack around it by directly
    // constructing it as a protobuf message.
    ColumnPredicatePB pb;
    pb.set_column("col1");
    *pb.mutable_in_list()->mutable_values()->Add() = string("\0\0\0\0", 4);
    *pb.mutable_in_list()->mutable_values()->Add() = string("\0\0\0\0", 4);

    ASSERT_OK(ColumnPredicateFromPB(schema, &arena, pb, &predicate));
    ASSERT_EQ(PredicateType::Equality, predicate->predicate_type());
  }

  { // col1 IN ()
    ColumnPredicatePB pb;
    pb.set_column("col1");
    pb.mutable_in_list();

    Arena arena(1024,1024*1024);
    boost::optional<ColumnPredicate> predicate;
    ASSERT_OK(ColumnPredicateFromPB(schema, &arena, pb, &predicate));
    ASSERT_EQ(PredicateType::None, predicate->predicate_type());
  }

  { // IN list corruption
    ColumnPredicatePB pb;
    pb.set_column("col1");
    pb.mutable_in_list();
    *pb.mutable_in_list()->mutable_values()->Add() = string("\0", 1);

    Arena arena(1024,1024*1024);
    boost::optional<ColumnPredicate> predicate;
    ASSERT_TRUE(ColumnPredicateFromPB(schema, &arena, pb, &predicate).IsInvalidArgument());
  }
}
} // namespace kudu
