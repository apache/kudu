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

#include "kudu/common/wire_protocol.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <list>
#include <numeric>
#include <optional>
#include <ostream>
#include <random>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/map.h>
#include <gtest/gtest.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/columnar_serialization.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/block_bloom_filter.h"
#include "kudu/util/faststring.h"
#include "kudu/util/hash.pb.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/int128.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"  // IWYU pragma: keep
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using google::protobuf::Map;
using std::optional;
using std::string;
using std::tuple;
using std::vector;
using strings::Substitute;

DECLARE_int32(max_cell_size_bytes);
DECLARE_int32(max_huge_cell_size_bytes);

namespace kudu {

class WireProtocolTest : public KuduTest {
 public:
  WireProtocolTest()
      : schema_({ ColumnSchema("string", STRING),
                  ColumnSchema("nullable_string", STRING, ColumnSchema::NULLABLE),
                  ColumnSchema("int", INT32),
                  ColumnSchema("nullable_int", INT32, ColumnSchema::NULLABLE),
                  ColumnSchema("int64", INT64) },
        1),
        test_data_arena_(4096) {
  }

  static void FillRowBlockWithTestRows(RowBlock* block) {
    Random rng(SeedRandom());

    block->selection_vector()->SetAllTrue();

    for (int i = 0; i < block->nrows(); i++) {
      if (rng.OneIn(10)) {
        block->selection_vector()->SetRowUnselected(i);
        continue;
      }

      RowBlockRow row = block->row(i);

      // We make new copies of these strings into the Arena for each row so that
      // the workload is more realistic. If we just re-use the same Slice object
      // for each row, the memory accesses fit entirely into a smaller number of
      // cache lines and we may micro-optimize for the wrong thing.
      CHECK(block->arena()->RelocateSlice(
          "hello world col0",
          reinterpret_cast<Slice*>(row.mutable_cell_ptr(0))));

      if (rng.OneIn(3)) {
        row.cell(1).set_null(true);
      } else {
        row.cell(1).set_null(false);
        CHECK(block->arena()->RelocateSlice(
            "hello world col1",
            reinterpret_cast<Slice*>(row.mutable_cell_ptr(1))));
      }

      *reinterpret_cast<int32_t*>(row.mutable_cell_ptr(2)) = i;
      *reinterpret_cast<int32_t*>(row.mutable_cell_ptr(3)) = i;
      row.cell(3).set_null(rng.OneIn(7));

      *reinterpret_cast<int64_t*>(row.mutable_cell_ptr(4)) = i;
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

namespace {
// google::protobuf::Map has no initializer-list constructor
Map<string, string> ConfigMap(const string& key, const string& value) {
  Map<string, string> ret;
  ret[key] = value;
  return ret;
}
} // anonymous namespace

TEST_F(WireProtocolTest, TestExtraConfigBatchSizing) {
  // "auto" is accepted, matched case-insensitively.
  for (const auto* val : {"auto", "AUTO", "Auto"}) {
    SCOPED_TRACE(Substitute("setting value to: $0", val));
    TableExtraConfigPB pb;
    ASSERT_OK(ExtraConfigPBFromPBMap(ConfigMap(kTableBatchSizing, val), &pb));
    ASSERT_TRUE(pb.has_batch_sizing());
    EXPECT_EQ(TableExtraConfigPB::AUTO, pb.batch_sizing());
  }

  // "none" is accepted (case-insensitively) and is equivalent to the default.
  for (const auto* val : {"none", "NONE", "None"}) {
    SCOPED_TRACE(Substitute("setting value to: $0", val));
    TableExtraConfigPB pb;
    ASSERT_OK(ExtraConfigPBFromPBMap(ConfigMap(kTableBatchSizing, val), &pb));
    EXPECT_EQ(TableExtraConfigPB::NONE, pb.batch_sizing());
  }

  // An empty value resets the property to its default.
  {
    TableExtraConfigPB pb;
    ASSERT_OK(ExtraConfigPBFromPBMap(ConfigMap(kTableBatchSizing, ""), &pb));
    EXPECT_FALSE(pb.has_batch_sizing());
  }

  // Anything else is rejected -- including "manual", which is reserved but not
  // yet implemented.
  for (const auto* val : {"bogus", "manual", "auto_batch_size", "1", "true"}) {
    SCOPED_TRACE(Substitute("trying to set value: $0", val));
    TableExtraConfigPB pb;
    const auto s =
        ExtraConfigPBFromPBMap(ConfigMap(kTableBatchSizing, val), &pb);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        Substitute("unable to parse $0", kTableBatchSizing));
    EXPECT_FALSE(pb.has_batch_sizing());
  }

  // In the other direction, AUTO maps back to its string form.
  {
    TableExtraConfigPB pb;
    pb.set_batch_sizing(TableExtraConfigPB::AUTO);
    Map<string, string> configs;
    ASSERT_OK(ExtraConfigPBToPBMap(pb, &configs));
    ASSERT_EQ(1, configs.count(kTableBatchSizing));
    EXPECT_EQ("auto", configs[kTableBatchSizing]);
  }

  // An unset property is omitted, and so is an explicit NONE: the two mean the
  // same thing, so ExtraConfigPBToPBMap() deliberately does not distinguish
  // them. As a result the PB -> map -> PB trip isn't bit-exact for an
  // explicitly set NONE; it comes back unset, which is semantically equivalent.
  for (bool set_none : {false, true}) {
    SCOPED_TRACE(Substitute("explicitly set NONE: $0", set_none));
    TableExtraConfigPB pb;
    if (set_none) {
      pb.set_batch_sizing(TableExtraConfigPB::NONE);
    }
    Map<string, string> configs;
    ASSERT_OK(ExtraConfigPBToPBMap(pb, &configs));
    EXPECT_EQ(0, configs.count(kTableBatchSizing));

    TableExtraConfigPB round_tripped;
    ASSERT_OK(ExtraConfigPBFromPBMap(configs, &round_tripped));
    EXPECT_FALSE(round_tripped.has_batch_sizing());
  }
}

TEST_F(WireProtocolTest, TestExtraConfigOverrideCellLimit) {
  // A valid value is parsed through. Cross-field constraints (positivity,
  // cell-size limits) are enforced by ValidateBatchAndCellSizing(), not here.
  {
    TableExtraConfigPB pb;
    ASSERT_OK(ExtraConfigPBFromPBMap(
        ConfigMap(kTableOverrideCellLimitBytes,
                  std::to_string(FLAGS_max_cell_size_bytes)),
        &pb));
    ASSERT_TRUE(pb.has_override_cell_limit_bytes());
    EXPECT_EQ(FLAGS_max_cell_size_bytes, pb.override_cell_limit_bytes());
  }

  // An empty value resets the property to its default.
  {
    TableExtraConfigPB pb;
    ASSERT_OK(ExtraConfigPBFromPBMap(
        ConfigMap(kTableOverrideCellLimitBytes, ""), &pb));
    EXPECT_FALSE(pb.has_override_cell_limit_bytes());
  }

  // Non-integer values are rejected while parsing.
  for (const auto* val : {"bogus", "1.5"}) {
    SCOPED_TRACE(Substitute("trying to set value: $0", val));
    TableExtraConfigPB pb;
    const auto s = ExtraConfigPBFromPBMap(
        ConfigMap(kTableOverrideCellLimitBytes, val), &pb);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(
        s.ToString(),
        Substitute("unable to parse $0", kTableOverrideCellLimitBytes));
  }

  // Round-trip through PB -> map -> PB.
  {
    TableExtraConfigPB pb;
    pb.set_override_cell_limit_bytes(12345);
    Map<string, string> configs;
    ASSERT_OK(ExtraConfigPBToPBMap(pb, &configs));
    ASSERT_EQ(1, configs.count(kTableOverrideCellLimitBytes));
    EXPECT_EQ("12345", configs[kTableOverrideCellLimitBytes]);
  }
}

TEST_F(WireProtocolTest, TestValidateBatchAndCellSizing) {
  const auto with_override = [](int32_t v) {
    TableExtraConfigPB pb;
    pb.set_override_cell_limit_bytes(v);
    return pb;
  };
  const auto with_batch = [](TableExtraConfigPB::BatchSizing v) {
    TableExtraConfigPB pb;
    pb.set_batch_sizing(v);
    return pb;
  };
  const TableExtraConfigPB kEmpty;

  // --- Cross-field / flag constraints (stored config empty, as on create) ---

  // A value in (0, --max_cell_size_bytes] is accepted regardless of batch
  // sizing.
  ASSERT_OK(ValidateBatchAndCellSizing(
      kEmpty, with_override(FLAGS_max_cell_size_bytes)));

  // Non-positive values are rejected.
  for (int32_t v : {0, -1}) {
    SCOPED_TRACE(Substitute("override: $0", v));
    const auto s = ValidateBatchAndCellSizing(kEmpty, with_override(v));
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "must be positive");
  }

  // Exceeding --max_huge_cell_size_bytes is always rejected, even with batch
  // sizing enabled.
  {
    TableExtraConfigPB pb = with_override(FLAGS_max_huge_cell_size_bytes + 1);
    pb.set_batch_sizing(TableExtraConfigPB::AUTO);
    const auto s = ValidateBatchAndCellSizing(kEmpty, pb);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "--max_huge_cell_size_bytes");
  }

  // Exceeding --max_cell_size_bytes requires batch_sizing != none...
  {
    const auto s = ValidateBatchAndCellSizing(
        kEmpty, with_override(FLAGS_max_cell_size_bytes + 1));
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), kTableBatchSizing);
  }
  // ... and is accepted once batch sizing is enabled.
  {
    TableExtraConfigPB pb = with_override(FLAGS_max_cell_size_bytes + 1);
    pb.set_batch_sizing(TableExtraConfigPB::AUTO);
    ASSERT_OK(ValidateBatchAndCellSizing(kEmpty, pb));
  }

  // --- One-way constraints vs. the stored config ---

  // Enabling the override from unset, and raising or keeping it, are allowed.
  ASSERT_OK(ValidateBatchAndCellSizing(kEmpty, with_override(1024)));
  ASSERT_OK(
      ValidateBatchAndCellSizing(with_override(1024), with_override(2048)));
  ASSERT_OK(
      ValidateBatchAndCellSizing(with_override(1024), with_override(1024)));

  // Lowering the override is rejected.
  {
    const auto s =
        ValidateBatchAndCellSizing(with_override(2048), with_override(1024));
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), kTableOverrideCellLimitBytes);
  }
  // Clearing the override is rejected.
  {
    const auto s = ValidateBatchAndCellSizing(with_override(2048), kEmpty);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), kTableOverrideCellLimitBytes);
  }

  // batch_sizing: enabling NONE -> AUTO is allowed; reverting AUTO -> NONE
  // (explicit or cleared) is rejected.
  ASSERT_OK(ValidateBatchAndCellSizing(with_batch(TableExtraConfigPB::NONE),
                                       with_batch(TableExtraConfigPB::AUTO)));
  ASSERT_OK(ValidateBatchAndCellSizing(with_batch(TableExtraConfigPB::AUTO),
                                       with_batch(TableExtraConfigPB::AUTO)));
  for (const auto& reverted :
       {with_batch(TableExtraConfigPB::NONE), TableExtraConfigPB()}) {
    const auto s = ValidateBatchAndCellSizing(
        with_batch(TableExtraConfigPB::AUTO), reverted);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), kTableBatchSizing);
  }

  // An alter that touches neither property leaves both one-way fields unchanged
  // and is accepted.
  {
    TableExtraConfigPB old_config = with_override(2048);
    old_config.set_batch_sizing(TableExtraConfigPB::AUTO);
    TableExtraConfigPB new_config = old_config;
    new_config.set_history_max_age_sec(3600);
    ASSERT_OK(ValidateBatchAndCellSizing(old_config, new_config));
  }
}

TEST_F(WireProtocolTest, TestSchemaRoundTrip) {
  google::protobuf::RepeatedPtrField<ColumnSchemaPB> pbs;

  ASSERT_OK(SchemaToColumnPBs(schema_, &pbs));
  ASSERT_EQ(5, pbs.size());

  // Column 0.
  EXPECT_TRUE(pbs.Get(0).is_key());
  EXPECT_EQ("string", pbs.Get(0).name());
  EXPECT_EQ(STRING, pbs.Get(0).type());
  EXPECT_FALSE(pbs.Get(0).is_nullable());

  // Column 1.
  EXPECT_FALSE(pbs.Get(1).is_key());
  EXPECT_EQ("nullable_string", pbs.Get(1).name());
  EXPECT_EQ(STRING, pbs.Get(1).type());
  EXPECT_TRUE(pbs.Get(1).is_nullable());

  // Column 2.
  EXPECT_FALSE(pbs.Get(2).is_key());
  EXPECT_EQ("int", pbs.Get(2).name());
  EXPECT_EQ(INT32, pbs.Get(2).type());
  EXPECT_FALSE(pbs.Get(2).is_nullable());


  // Column 3.
  EXPECT_FALSE(pbs.Get(3).is_key());
  EXPECT_EQ("nullable_int", pbs.Get(3).name());
  EXPECT_EQ(INT32, pbs.Get(3).type());
  EXPECT_TRUE(pbs.Get(3).is_nullable());

  // Column 4.
  EXPECT_FALSE(pbs.Get(4).is_key());
  EXPECT_EQ("int64", pbs.Get(4).name());
  EXPECT_EQ(INT64, pbs.Get(4).type());
  EXPECT_FALSE(pbs.Get(4).is_nullable());

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

// Create a block of rows and ensure that it can be converted to and from protobuf.
TEST_F(WireProtocolTest, TestRowBlockToRowwisePB) {
  RowBlockMemory mem(1024);
  RowBlock block(&schema_, 30, &mem);
  FillRowBlockWithTestRows(&block);

  // Convert to PB.
  faststring direct, indirect;
  int num_rows = SerializeRowBlock(block, nullptr, &direct, &indirect);
  SCOPED_TRACE("Row data: " + direct.ToString());
  SCOPED_TRACE("Indirect data: " + indirect.ToString());

  // Convert back to a row, ensure that the resulting row is the same
  // as the one we put in.
  RowwiseRowBlockPB pb;
  pb.set_num_rows(num_rows);

  vector<const uint8_t*> row_ptrs;
  Slice direct_sidecar = direct;
  ASSERT_OK(ExtractRowsFromRowBlockPB(schema_, pb, indirect,
                                      &direct_sidecar, &row_ptrs));
  ASSERT_EQ(block.selection_vector()->CountSelected(), row_ptrs.size());
  int dst_row_idx = 0;
  for (int i = 0; i < block.nrows(); ++i) {
    if (!block.selection_vector()->IsRowSelected(i)) {
      continue;
    }
    ConstContiguousRow row_roundtripped(&schema_, row_ptrs[dst_row_idx]);
    EXPECT_EQ(schema_.DebugRow(block.row(i)),
              schema_.DebugRow(row_roundtripped));
    dst_row_idx++;
  }
}

// Create blocks of rows and ensure that they can be converted to the columnar serialized
// layout.
TEST_F(WireProtocolTest, TestRowBlockToColumnarPB) {
  // Generate several blocks of random data.
  static constexpr int kNumBlocks = 3;
  static constexpr int kBatchSizeBytes = 8192 * 1024;
  RowBlockMemory mem(1024);
  std::list<RowBlock> blocks;
  for (int i = 0; i < kNumBlocks; i++) {
    blocks.emplace_back(&schema_, 30, &mem);
    FillRowBlockWithTestRows(&blocks.back());
  }

  // Convert all of the RowBlocks to a single serialized (concatenated) columnar format.
  ColumnarSerializedBatch batch(schema_, schema_, kBatchSizeBytes);
  for (const auto& block : blocks) {
    batch.AddRowBlock(block);
  }

  // Verify that the resulting serialized data matches the concatenated original data blocks.
  ASSERT_EQ(5, batch.columns().size());
  int dst_row_idx = 0;
  for (const auto& block : blocks) {
    for (int src_row_idx = 0; src_row_idx < block.nrows(); src_row_idx++) {
      if (!block.selection_vector()->IsRowSelected(src_row_idx)) {
        continue;
      }
      SCOPED_TRACE(src_row_idx);
      SCOPED_TRACE(dst_row_idx);
      const auto& row = block.row(src_row_idx);
      for (int c = 0; c < schema_.num_columns(); c++) {
        SCOPED_TRACE(c);
        const auto& col = schema_.column(c);
        const auto& serialized_col = batch.columns()[c];
        if (col.is_nullable()) {
          bool expect_null = row.is_null(c);;
          EXPECT_EQ(!BitmapTest(serialized_col.non_null_bitmap->data(), dst_row_idx),
                    expect_null);
          if (expect_null) {
            continue;
          }
        }
        int type_size = col.type_info()->size();
        Slice serialized_val;
        Slice orig_val;
        if (col.type_info()->physical_type() == BINARY) {
          const uint8_t* offset_ptr = serialized_col.data.data() + sizeof(uint32_t) * dst_row_idx;
          uint32_t start_offset = UnalignedLoad<uint32_t>(offset_ptr);
          uint32_t end_offset = UnalignedLoad<uint32_t>(offset_ptr + sizeof(uint32_t));
          ASSERT_GE(end_offset, start_offset);
          serialized_val = Slice(serialized_col.varlen_data->data() + start_offset,
                                 end_offset - start_offset);
          memcpy(&orig_val, row.cell_ptr(c), type_size);
        } else {
          serialized_val = Slice(serialized_col.data.data() + type_size * dst_row_idx,
                                 type_size);
          orig_val = Slice(row.cell_ptr(c), type_size);
        }
        EXPECT_EQ(orig_val, serialized_val);
      }
      dst_row_idx++;
    }
  }
}


// Create a block of rows in columnar layout and ensure that it can be
// converted to and from protobuf.
TEST_F(WireProtocolTest, TestColumnarRowBlockToPBWithPadding) {
  int kNumRows = 10;
  RowBlockMemory mem(1024);
  // Create a schema with multiple UNIXTIME_MICROS columns in different
  // positions.
  Schema tablet_schema({ ColumnSchema("key", UNIXTIME_MICROS),
                         ColumnSchema("col1", STRING),
                         ColumnSchema("col2", UNIXTIME_MICROS),
                         ColumnSchema("col3", INT32, ColumnSchema::NULLABLE),
                         ColumnSchema("col4", UNIXTIME_MICROS, ColumnSchema::NULLABLE) },
                       1);
  RowBlock block(&tablet_schema, kNumRows, &mem);
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
                       ColumnSchema("col4", UNIXTIME_MICROS, ColumnSchema::NULLABLE),
                       ColumnSchema("col3", INT32, ColumnSchema::NULLABLE)},
                     0);

  // Convert to PB.
  faststring direct, indirect;
  int num_rows = SerializeRowBlock(block, &proj_schema, &direct, &indirect,
                                   true /* pad timestamps */);
  SCOPED_TRACE("Row data: " + HexDump(direct));
  SCOPED_TRACE("Indirect data: " + HexDump(indirect));

  // Convert back to a row, ensure that the resulting row is the same
  // as the one we put in. Can't reuse the decoding methods since we
  // won't support decoding padded rows within Kudu.
  RowwiseRowBlockPB pb;
  pb.set_num_rows(num_rows);
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
    const uint8_t* non_null_bitmap = base_data + 68;

    // 'col1' comes at 0 bytes offset in the projection schema.
    const Slice* col1 = reinterpret_cast<const Slice*>(base_data);
    ASSERT_EQ(Slice("hello world col1"), *col1)
        << "Unexpected val for the " << i << "th row:" << col1->ToDebugString();
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
    EXPECT_TRUE(BitmapTest(non_null_bitmap, 3));

    // 'col3' comes at 64 bytes offset: 48 bytes previous, 8 bytes 'col4', 8 bytes padding
    const int32_t col3 = *reinterpret_cast<const int32_t*>(base_data + 64);
    EXPECT_EQ(col3, i);
    EXPECT_FALSE(BitmapTest(non_null_bitmap, 4));
  }
}

struct RowwiseConverter {
  static void Run(const RowBlock& block) {
    faststring direct;
    faststring indirect;
    SerializeRowBlock(block, nullptr, &direct, &indirect);
  }

  static constexpr const char* kName = "row-wise";
};


struct ColumnarConverter {
  static void Run(const RowBlock& block) {
    constexpr int kBatchSizeBytes = 8192 * 1024;
    ColumnarSerializedBatch batch(*block.schema(), *block.schema(), kBatchSizeBytes);
    batch.AddRowBlock(block);
  }

  static constexpr const char* kName = "columnar";
};

struct BenchmarkColumnsSpec {
  struct Col {
    DataType type;
    double null_fraction; // negative for non-null
  };
  vector<Col> columns;
  string name;
};

class WireProtocolBenchmark :
      public WireProtocolTest,
      public testing::WithParamInterface<tuple<BenchmarkColumnsSpec, double>> {
 public:

  void ResetBenchmarkSchema(const BenchmarkColumnsSpec& spec) {
    vector<ColumnSchema> column_schemas;
    int i = 0;
    for (const auto& c : spec.columns) {
      column_schemas.emplace_back(Substitute("col$0", i++),
                                  c.type,
                                  c.null_fraction >= 0 ? ColumnSchema::NULLABLE
                                                       : ColumnSchema::NOT_NULL);
    }
    CHECK_OK(benchmark_schema_.Reset(std::move(column_schemas), 0));
  }

  void FillRowBlockForBenchmark(const BenchmarkColumnsSpec& spec,
                                RowBlock* block) {
    Random rng(SeedRandom());

    test_data_arena_.Reset();
    for (int i = 0; i < block->nrows(); i++) {
      RowBlockRow row = block->row(i);
      for (int j = 0; j < benchmark_schema_.num_columns(); j++) {
        const ColumnSchema& column_schema = benchmark_schema_.column(j);
        DataType type = spec.columns[j].type;
        bool is_null = rng.NextDoubleFraction() <= spec.columns[j].null_fraction;
        if (column_schema.is_nullable()) {
          row.cell(j).set_null(is_null);
        }
        if (!is_null) {
          switch (type) {
            case STRING: {
              Slice col;
              CHECK(test_data_arena_.RelocateSlice(Substitute("hello world $0",
                                                              column_schema.name()), &col));
              memcpy(row.mutable_cell_ptr(j), &col, sizeof(Slice));
              break;
            }
            case INT128:
              UnalignedStore<int128_t>(row.mutable_cell_ptr(j), i);
              break;
            case INT64:
              UnalignedStore<int64_t>(row.mutable_cell_ptr(j), i);
              break;
            case INT32:
              UnalignedStore<int32_t>(row.mutable_cell_ptr(j), i);
              break;
            case INT16:
              UnalignedStore<int16_t>(row.mutable_cell_ptr(j), i);
              break;
            case INT8:
              UnalignedStore<int8_t>(row.mutable_cell_ptr(j), i);
              break;
            default:
              LOG(FATAL) << "Unexpected type: " << type;
          }
        }
      }
    }
  }

  static void SelectRandomRowsWithRate(RowBlock* block, double rate) {
    CHECK_LE(rate, 1.0);
    CHECK_GE(rate, 0.0);
    auto select_count = block->nrows() * rate;
    SelectionVector* select_vector = block->selection_vector();
    if (rate == 1.0) {
      select_vector->SetAllTrue();
    } else if (rate == 0.0) {
      select_vector->SetAllFalse();
    } else {
      vector<int> indexes(block->nrows());
      std::iota(indexes.begin(), indexes.end(), 0);
      std::mt19937 gen(SeedRandom());
      std::shuffle(indexes.begin(), indexes.end(), gen);
      indexes.resize(select_count);
      select_vector->SetAllFalse();
      for (auto index : indexes) {
        select_vector->SetRowSelected(index);
      }
    }
    CHECK_EQ(select_vector->CountSelected(), select_count);
  }


  // Use column_count to control the schema scale.
  // Use select_rate to control the number of selected rows.
  template<class Converter>
  double RunBenchmark(const BenchmarkColumnsSpec& spec,
                    double select_rate) {
    ResetBenchmarkSchema(spec);
    RowBlockMemory mem(1024);
    RowBlock block(&benchmark_schema_, 1000, &mem);
    // Regardless of the config, use a constant number of selected cells for the test by
    // looping the conversion an appropriate number of times.
    const int64_t kNumCellsToConvert = AllowSlowTests() ? 100000000 : 1000000;
    const int64_t kCellsPerBlock = block.nrows() * spec.columns.size();
    const double kSelectedCellsPerBlock = kCellsPerBlock * select_rate;
    const int kNumTrials = static_cast<int>(kNumCellsToConvert /  kSelectedCellsPerBlock);
    FillRowBlockForBenchmark(spec, &block);
    SelectRandomRowsWithRate(&block, select_rate);

    int64_t cycle_start = CycleClock::Now();
    for (int i = 0; i < kNumTrials; ++i) {
      Converter::Run(block);
    }
    int64_t cycle_end = CycleClock::Now();
    double cycles_per_cell = static_cast<double>(cycle_end - cycle_start) / kNumCellsToConvert;
    LOG(INFO) << Substitute(
        "Converting $0 to PB (method $3) row select rate $1: $2 cycles/cell",
        spec.name, select_rate, cycles_per_cell,
        Converter::kName);
    return cycles_per_cell;
  }

 protected:
  Schema benchmark_schema_;
};

TEST_P(WireProtocolBenchmark, TestRowBlockToPBBenchmark) {
  const auto& spec = std::get<0>(GetParam());
  double select_rate = std::get<1>(GetParam());
  double cycles_per_cell_rowwise = RunBenchmark<RowwiseConverter>(spec, select_rate);
  double cycles_per_cell_columnar = RunBenchmark<ColumnarConverter>(spec, select_rate);
  double ratio = cycles_per_cell_rowwise / cycles_per_cell_columnar;
  LOG(INFO) << Substitute(
      "Converting $0 to PB row select rate $1: columnar/rowwise throughput ratio: $2x",
      spec.name, select_rate, ratio);
}

BenchmarkColumnsSpec UniformColumns(int n_cols, DataType type, double null_fraction) {
  vector<BenchmarkColumnsSpec::Col> cols(n_cols);
  for (int i = 0; i < n_cols; i++) {
    cols[i] = {type, null_fraction};
  }
  string null_str;
  if (null_fraction >= 0) {
    null_str = Substitute("$0pct_null", static_cast<int>(null_fraction * 100));
  } else {
    null_str = "non_null";
  }
  return {cols, Substitute("$0_$1_$2",
                           n_cols,
                           GetTypeInfo(type)->name(),
                           null_str) };
}

INSTANTIATE_TEST_SUITE_P(
    ColumnarRowBlockToPBBenchmarkParams, WireProtocolBenchmark,
    testing::Combine(
        testing::Values(
            UniformColumns(10, INT64, -1),
            UniformColumns(10, INT32, -1),
            UniformColumns(10, STRING, -1),

            UniformColumns(10, INT128, 0),
            UniformColumns(10, INT64, 0),
            UniformColumns(10, INT32, 0),
            UniformColumns(10, INT16, 0),
            UniformColumns(10, INT8, 0),
            UniformColumns(10, STRING, 0),

            UniformColumns(10, INT128, 0.1),
            UniformColumns(10, INT64, 0.1),
            UniformColumns(10, INT32, 0.1),
            UniformColumns(10, INT16, 0.1),
            UniformColumns(10, INT8, 0.1),
            UniformColumns(10, STRING, 0.1)),
        // Selection rates.
        testing::Values(1.0, 0.8, 0.5, 0.2)),
    [](const testing::TestParamInfo<WireProtocolBenchmark::ParamType>& info) {
      return Substitute("$0_sel_$1pct",
                        std::get<0>(info.param).name,
                        static_cast<int>(std::get<1>(info.param)*100));
    });


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
  RowBlockMemory mem(1024);
  RowBlock block(&empty, 1000, &mem);
  block.selection_vector()->SetAllTrue();
  // Unselect 100 rows
  for (int i = 0; i < 100; i++) {
    block.selection_vector()->SetRowUnselected(i * 2);
  }
  ASSERT_EQ(900, block.selection_vector()->CountSelected());

  // Convert it to protobuf, ensure that the results look right.
  faststring direct, indirect;
  int num_rows = SerializeRowBlock(block, nullptr, &direct, &indirect);
  ASSERT_EQ(900, num_rows);
}

TEST_F(WireProtocolTest, TestColumnDefaultValue) {
  Slice write_default_str("Hello Write");
  Slice read_default_str("Hello Read");
  uint32_t write_default_u32 = 512;
  uint32_t read_default_u32 = 256;
  ColumnSchemaPB pb;

  ColumnSchema col1("col1", STRING);
  ColumnSchemaToPB(col1, &pb);
  ColumnSchemaBuilder col1bld;
  ASSERT_OK(ColumnSchemaBuilderFromPB(pb, &col1bld));
  ColumnSchema col1fpb(col1bld);
  ASSERT_FALSE(col1fpb.has_read_default());
  ASSERT_FALSE(col1fpb.has_write_default());
  ASSERT_TRUE(col1fpb.read_default_value() == nullptr);

  ColumnSchema col2(ColumnSchemaBuilder()
                        .name("col2")
                        .type(STRING)
                        .read_default(&read_default_str));
  ColumnSchemaToPB(col2, &pb);
  ColumnSchemaBuilder col2bld;
  ASSERT_OK(ColumnSchemaBuilderFromPB(pb, &col2bld));
  ColumnSchema col2fpb(col2bld);
  ASSERT_TRUE(col2fpb.has_read_default());
  ASSERT_FALSE(col2fpb.has_write_default());
  ASSERT_EQ(read_default_str, *static_cast<const Slice*>(col2fpb.read_default_value()));
  ASSERT_EQ(nullptr, static_cast<const Slice*>(col2fpb.write_default_value()));

  ColumnSchema col3(ColumnSchemaBuilder()
                        .name("col3")
                        .type(STRING)
                        .read_default(&read_default_str)
                        .write_default(&write_default_str));
  ColumnSchemaToPB(col3, &pb);
  ColumnSchemaBuilder col3bld;
  ASSERT_OK(ColumnSchemaBuilderFromPB(pb, &col3bld));
  ColumnSchema col3fpb(col3bld);
  ASSERT_TRUE(col3fpb.has_read_default());
  ASSERT_TRUE(col3fpb.has_write_default());
  ASSERT_EQ(read_default_str, *static_cast<const Slice*>(col3fpb.read_default_value()));
  ASSERT_EQ(write_default_str, *static_cast<const Slice*>(col3fpb.write_default_value()));

  ColumnSchema col4(ColumnSchemaBuilder()
                        .name("col4")
                        .type(UINT32)
                        .read_default(&read_default_u32));
  ColumnSchemaToPB(col4, &pb);
  ColumnSchemaBuilder col4bld;
  ASSERT_OK(ColumnSchemaBuilderFromPB(pb, &col4bld));
  ColumnSchema col4fpb(col4bld);
  ASSERT_TRUE(col4fpb.has_read_default());
  ASSERT_FALSE(col4fpb.has_write_default());
  ASSERT_EQ(read_default_u32, *static_cast<const uint32_t*>(col4fpb.read_default_value()));
  ASSERT_EQ(nullptr, static_cast<const uint32_t*>(col4fpb.write_default_value()));

  ColumnSchema col5(ColumnSchemaBuilder()
                        .name("col5")
                        .type(UINT32)
                        .read_default(&read_default_u32)
                        .write_default(&write_default_u32));
  ColumnSchemaToPB(col5, &pb);
  ColumnSchemaBuilder col5bld;
  ASSERT_OK(ColumnSchemaBuilderFromPB(pb, &col5bld));
  ColumnSchema col5fpb(col5bld);
  ASSERT_TRUE(col5fpb.has_read_default());
  ASSERT_TRUE(col5fpb.has_write_default());
  ASSERT_EQ(read_default_u32, *static_cast<const uint32_t*>(col5fpb.read_default_value()));
  ASSERT_EQ(write_default_u32, *static_cast<const uint32_t*>(col5fpb.write_default_value()));
}

// Regression test for KUDU-2378; the call to ColumnSchemaFromPB yielded a crash.
TEST_F(WireProtocolTest, TestCrashOnAlignedLoadOf128BitReadDefault) {
  ColumnSchemaPB pb;
  pb.set_name("col");
  pb.set_type(DECIMAL128);
  pb.set_read_default_value(string(16, 'a'));
  ColumnSchemaBuilder bld;
  ASSERT_OK(ColumnSchemaBuilderFromPB(pb, &bld));
}

// Regression test for KUDU-2622; Validate read and write default value sizes.
TEST_F(WireProtocolTest, TestInvalidReadAndWriteDefault) {
  {
    ColumnSchemaPB pb;
    pb.set_name("col");
    pb.set_type(DECIMAL128);
    pb.set_read_default_value(string(8, 'a'));
    ColumnSchemaBuilder bld;
    const auto s = ColumnSchemaBuilderFromPB(pb, &bld);
    EXPECT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "Corruption: Not enough bytes for decimal: read default");
  }
  {
    ColumnSchemaPB pb;
    pb.set_name("col");
    pb.set_type(DECIMAL128);
    pb.set_write_default_value(string(8, 'a'));
    ColumnSchemaBuilder bld;
    const auto s = ColumnSchemaBuilderFromPB(pb, &bld);
    EXPECT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "Corruption: Not enough bytes for decimal: write default");
  }
}

TEST_F(WireProtocolTest, ScalarArray1DFromPB) {
  {
    ColumnSchemaPB pb;
    pb.set_name("col");
    pb.set_type(NESTED);
    pb.set_is_nullable(true);
    auto* array_info = pb.mutable_nested_type()->mutable_array();
    array_info->set_type(INT8);
    ColumnSchemaBuilder bld;
    ASSERT_OK(ColumnSchemaBuilderFromPB(pb, &bld));
    const ColumnSchema schema = bld.Build();
    ASSERT_TRUE(schema.is_nullable());
    ASSERT_TRUE(schema.is_array());
    const TypeInfo* ti = schema.type_info();
    ASSERT_EQ("int8 1d-array", ti->name());
    ASSERT_FALSE(ti->is_virtual());
    ASSERT_TRUE(ti->is_array());
    ASSERT_EQ(BINARY, ti->physical_type());
    ASSERT_EQ(NESTED, ti->type());
  }
  {
    // IS_DELETED isn't accepted as array element type.
    ColumnSchemaPB pb;
    pb.set_name("col");
    pb.set_type(NESTED);
    pb.set_is_nullable(true);
    auto* array_info = pb.mutable_nested_type()->mutable_array();
    array_info->set_type(IS_DELETED);
    ColumnSchemaBuilder bld;
    const auto s = ColumnSchemaBuilderFromPB(pb, &bld);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "nested types: invalid scalar type 'IS_DELETED' for array elements");
  }
}

TEST_F(WireProtocolTest, ScalarArray1DToPB) {
  {
    ColumnSchema col(ColumnSchemaBuilder()
                         .name("col")
                         .type(STRING)
                         .array(true));
    const auto* type_info = col.type_info();
    ASSERT_EQ(DataType::NESTED, type_info->type());
    ASSERT_NE(nullptr, type_info->nested_type_info());

    ColumnSchemaPB pb;
    ColumnSchemaToPB(col, &pb);
    ColumnSchemaBuilder col_bld;
    ASSERT_OK(ColumnSchemaBuilderFromPB(pb, &col_bld));
    ColumnSchema col_fpb(col_bld);
    ASSERT_TRUE(col_fpb.is_array());
    ASSERT_FALSE(col_fpb.is_nullable());
    ASSERT_FALSE(col_fpb.is_immutable());
    ASSERT_FALSE(col_fpb.is_auto_incrementing());
    const auto* fpb_type_info = col_fpb.type_info();
    ASSERT_EQ(DataType::NESTED, fpb_type_info->type());

    // A shortcut to verify the result: based on details of the type registry
    // implementation, it's possible to compare pointers for type_info()
    // for both the original and 'from-pb' schemas.
    ASSERT_EQ(col.type_info(), fpb_type_info);

    // Extra sanity checks.
    const auto* fpb_nested_type_info = fpb_type_info->nested_type_info();
    ASSERT_TRUE(fpb_nested_type_info->is_array());
    const auto& fpb_array_type_info = fpb_nested_type_info->array();
    ASSERT_EQ(DataType::STRING, fpb_array_type_info.elem_type_info()->type());
  }
}

TEST_F(WireProtocolTest, ScalarArrayWithTypeAttributes1DToPB) {
  // Column 1: DECIMAL(10,5) array
  ColumnSchema dec_col(ColumnSchemaBuilder()
                           .name("dec_col")
                           .type(DECIMAL32)
                           .type_attributes(ColumnTypeAttributes(10, 5))
                           .array(true));

  // Column 2: STRING(10) array
  ColumnSchema str_col(ColumnSchemaBuilder()
                           .name("str_col")
                           .type(VARCHAR)
                           .type_attributes(ColumnTypeAttributes(10))
                           .array(true));

  // Serialize both
  ColumnSchemaPB dec_pb;
  ColumnSchemaToPB(dec_col, &dec_pb);
  ColumnSchemaPB str_pb;
  ColumnSchemaToPB(str_col, &str_pb);

  // Deserialize both
  ColumnSchemaBuilder dec_bld;
  ASSERT_OK(ColumnSchemaBuilderFromPB(dec_pb, &dec_bld));
  ColumnSchema dec_col_fpb(dec_bld);

  ColumnSchemaBuilder str_bld;
  ASSERT_OK(ColumnSchemaBuilderFromPB(str_pb, &str_bld));
  ColumnSchema str_col_fpb(str_bld);

  // DECIMAL array column checks
  ASSERT_TRUE(dec_col_fpb.is_array());
  const auto* dec_type_info = dec_col_fpb.type_info();
  ASSERT_EQ(DataType::NESTED, dec_col_fpb.type_info()->type());

  const auto* dec_nested = dec_type_info->nested_type_info();
  ASSERT_TRUE(dec_nested->is_array());
  ASSERT_EQ(DataType::DECIMAL32, dec_nested->array().elem_type_info()->type());
  ASSERT_EQ(10, dec_col_fpb.type_attributes().precision);
  ASSERT_EQ(5, dec_col_fpb.type_attributes().scale);

  // VARCHAR array column checks
  ASSERT_TRUE(str_col_fpb.is_array());
  const auto* str_type_info = str_col_fpb.type_info();
  ASSERT_EQ(DataType::NESTED, str_type_info->type());

  const auto* str_nested = str_type_info->nested_type_info();
  ASSERT_TRUE(str_nested->is_array());
  ASSERT_EQ(DataType::VARCHAR, str_nested->array().elem_type_info()->type());
  ASSERT_EQ(10, str_col_fpb.type_attributes().length);
}

TEST_F(WireProtocolTest, NestedTypeValidityChecks) {
  {
    ColumnSchemaPB pb;
    pb.set_name("col");
    pb.set_is_nullable(true);
    auto* array_info = pb.mutable_nested_type()->mutable_array();
    array_info->set_type(INT64);
    // The 'type' field should be set even if it's enough information on the
    // nested type in the 'nested_type' field.
    ColumnSchemaBuilder bld;
    const auto s = ColumnSchemaBuilderFromPB(pb, &bld);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "'type' field is missing");
  }
  {
    ColumnSchemaPB pb;
    pb.set_name("col");
    pb.set_type(NESTED);
    // With the 'type' field set to NESTED and the 'nested_type' field not
    // present it should result in an error when trying to build ColumnSchema
    // from ColumnSchemaPB.
    ColumnSchemaBuilder bld;
    const auto s = ColumnSchemaBuilderFromPB(pb, &bld);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "missing 'nested_type' field for NESTED type");
  }
  {
    ColumnSchemaPB pb;
    pb.set_name("col");
    pb.set_type(BOOL);
    pb.set_is_nullable(true);
    auto* array_info = pb.mutable_nested_type()->mutable_array();
    array_info->set_type(BOOL);
    // When 'nested_type' field is set, the 'type' field must be set 'NESTED'.
    ColumnSchemaBuilder bld;
    const auto s = ColumnSchemaBuilderFromPB(pb, &bld);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "found BOOL instead of NESTED in the 'type' field");
  }
  {
    ColumnSchemaPB pb;
    pb.set_name("col");
    pb.set_type(NESTED);
    pb.set_is_nullable(true);
    auto* array_info = pb.mutable_nested_type()->mutable_array();
    array_info->set_type(NESTED);
    // Nested (multi-dimensional) arrays aren't supported yet.
    ColumnSchemaBuilder bld;
    const auto s = ColumnSchemaBuilderFromPB(pb, &bld);
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "only 1d-arrays of scalar types are supported yet");
  }
}

TEST_F(WireProtocolTest, TestColumnPredicateInList) {
  ColumnSchema col1("col1", INT32);
  vector<ColumnSchema> cols = { col1 };
  Schema schema(cols, 1);
  RowBlockMemory mem(1024);
  optional<ColumnPredicate> predicate;

  { // col1 IN (5, 6, 10)
    int five = 5;
    int six = 6;
    int ten = 10;
    vector<const void*> values { &five, &six, &ten };

    kudu::ColumnPredicate cp = kudu::ColumnPredicate::InList(col1, &values);
    ColumnPredicatePB pb;
    NO_FATALS(ColumnPredicateToPB(cp, &pb));

    ASSERT_OK(ColumnPredicateFromPB(schema, &mem.arena, pb, &predicate));
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

    ASSERT_OK(ColumnPredicateFromPB(schema, &mem.arena, pb, &predicate));
    ASSERT_EQ(PredicateType::Equality, predicate->predicate_type());
  }

  { // col1 IN ()
    ColumnPredicatePB pb;
    pb.set_column("col1");
    pb.mutable_in_list();

    RowBlockMemory mem(1024);
    optional<ColumnPredicate> predicate;
    ASSERT_OK(ColumnPredicateFromPB(schema, &mem.arena, pb, &predicate));
    ASSERT_EQ(PredicateType::None, predicate->predicate_type());
  }

  { // IN list corruption
    ColumnPredicatePB pb;
    pb.set_column("col1");
    pb.mutable_in_list();
    *pb.mutable_in_list()->mutable_values()->Add() = string("\0", 1);

    RowBlockMemory mem(1024);
    optional<ColumnPredicate> predicate;
    ASSERT_TRUE(ColumnPredicateFromPB(schema, &mem.arena, pb, &predicate).IsInvalidArgument());
  }
}

class BFWireProtocolTest : public KuduTest {
 public:
  BFWireProtocolTest()
      : schema_({ ColumnSchema("col1", INT32)}, 1),
        arena_(1024),
        allocator_(&arena_),
        n_keys_(100),
        b1_(&allocator_),
        b2_(&allocator_) {}

  void SetUp() override {
    int log_space_bytes1 = BlockBloomFilter::MinLogSpace(n_keys_, 0.01);
    ASSERT_OK(b1_.Init(log_space_bytes1, FAST_HASH, 0));
    ASSERT_LE(BlockBloomFilter::FalsePositiveProb(n_keys_, log_space_bytes1), 0.01);

    int log_space_bytes2 = BlockBloomFilter::MinLogSpace(n_keys_, 0.01);
    ASSERT_OK(b2_.Init(log_space_bytes2, FAST_HASH, 0));
    ASSERT_LE(BlockBloomFilter::FalsePositiveProb(n_keys_, log_space_bytes2), 0.01);

    for (int i = 0; i < n_keys_; ++i) {
      Slice key_slice(reinterpret_cast<const uint8_t*>(&i), sizeof(i));
      b1_.Insert(key_slice);
      b2_.Insert(key_slice);
    }
  }

protected:
  Schema schema_;
  Arena arena_;
  ArenaBlockBloomFilterBufferAllocator allocator_;
  int n_keys_;
  BlockBloomFilter b1_;
  BlockBloomFilter b2_;
};

TEST_F(BFWireProtocolTest, TestColumnPredicateBloomFilter) {
  optional<ColumnPredicate> predicate;
  ColumnSchema col1 = schema_.column(0);
  { // Single BloomFilter predicate.
    kudu::ColumnPredicate ibf =
        kudu::ColumnPredicate::InBloomFilter(col1, {&b1_}, nullptr, nullptr);
    ColumnPredicatePB pb;
    NO_FATALS(ColumnPredicateToPB(ibf, &pb));
    ASSERT_OK(ColumnPredicateFromPB(schema_, &arena_, pb, &predicate));
    ASSERT_EQ(predicate->predicate_type(), PredicateType::InBloomFilter);
    ASSERT_EQ(predicate, ibf);
  }

  { // Multi BloomFilter predicate.
    kudu::ColumnPredicate ibf =
        kudu::ColumnPredicate::InBloomFilter(col1, {&b1_, &b2_}, nullptr, nullptr);
    ColumnPredicatePB pb;
    NO_FATALS(ColumnPredicateToPB(ibf, &pb));
    ASSERT_OK(ColumnPredicateFromPB(schema_, &arena_, pb, &predicate));
    ASSERT_EQ(predicate->predicate_type(), PredicateType::InBloomFilter);
    ASSERT_EQ(predicate, ibf);
  }
}

TEST_F(BFWireProtocolTest, TestColumnPredicateBloomFilterWithBound) {
  optional<ColumnPredicate> predicate;
  ColumnSchema col1 = schema_.column(0);
  { // Simply BloomFilter with lower bound.
    int lower = 1;
    auto ibf = kudu::ColumnPredicate::InBloomFilter(col1, {&b1_}, &lower, nullptr);
    ColumnPredicatePB pb;
    NO_FATALS(ColumnPredicateToPB(ibf, &pb));
    ASSERT_OK(ColumnPredicateFromPB(schema_, &arena_, pb, &predicate));
    ASSERT_EQ(predicate->predicate_type(), PredicateType::InBloomFilter);
    ASSERT_EQ(predicate, ibf);
  }

  { // Single bloom filter with upper bound.
    int upper = 4;
    auto ibf = kudu::ColumnPredicate::InBloomFilter(col1, {&b1_}, nullptr, &upper);
    ColumnPredicatePB pb;
    NO_FATALS(ColumnPredicateToPB(ibf, &pb));
    ASSERT_OK(ColumnPredicateFromPB(schema_, &arena_, pb, &predicate));
    ASSERT_EQ(predicate->predicate_type(), PredicateType::InBloomFilter);
    ASSERT_EQ(predicate, ibf);
  }

  { // Single bloom filter with both lower and upper bound.
    int lower = 1;
    int upper = 4;
    auto ibf = kudu::ColumnPredicate::InBloomFilter(col1, {&b1_}, &lower, &upper);
    ColumnPredicatePB pb;
    NO_FATALS(ColumnPredicateToPB(ibf, &pb));
    ASSERT_OK(ColumnPredicateFromPB(schema_, &arena_, pb, &predicate));
    ASSERT_EQ(predicate->predicate_type(), PredicateType::InBloomFilter);
    ASSERT_EQ(predicate, ibf);
  }

  { // Multi bloom filter with both lower and upper bound.
    int lower = 1;
    int upper = 4;
    auto ibf = kudu::ColumnPredicate::InBloomFilter(col1, {&b1_, &b2_}, &lower,
                                                    &upper);
    ColumnPredicatePB pb;
    NO_FATALS(ColumnPredicateToPB(ibf, &pb));
    ASSERT_OK(ColumnPredicateFromPB(schema_, &arena_, pb, &predicate));
    ASSERT_EQ(predicate->predicate_type(), PredicateType::InBloomFilter);
    ASSERT_EQ(predicate->bloom_filters().size(), ibf.bloom_filters().size());
    ASSERT_EQ(predicate, ibf);
  }
}

} // namespace kudu
