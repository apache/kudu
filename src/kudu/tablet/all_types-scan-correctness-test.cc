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

#include <algorithm>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"

using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace tablet {

// Column numbers; kColX refers to column with name "val_x".
static const int kColA = 1;
static const int kColB = 2;
static const int kColC = 3;

// Rows added after altering and before altering the table.
static const int kNumAddedRows = 2000;
static const int kNumBaseRows = 10000;

// Number of unique values in each column.
static const int kCardinality = 21;

// Lower and upper bounds of the predicates.
static const int kLower = 3;
static const int kUpper = 10;

// Number of elements to allocate memory for. Each range predicate requires two elements, and each
// call to AddColumn(read_default) requires one. Must be greater than the number of elements
// actually needed.
static const int kNumAllocatedElements = 32;

// Strings and binaries will have the following string length.
static const int kStrlen = 10;

struct RowOpsBase {
  RowOpsBase(DataType type, EncodingType encoding) : type_(type), encoding_(encoding) {
    schema_ = Schema({ColumnSchema("key", INT32),
                     ColumnSchema("val_a", type, true, nullptr, nullptr,
                         ColumnStorageAttributes(encoding, DEFAULT_COMPRESSION)),
                     ColumnSchema("val_b", type, true, nullptr, nullptr,
                         ColumnStorageAttributes(encoding, DEFAULT_COMPRESSION))}, 1);

  }
  DataType type_;
  EncodingType encoding_;
  Schema schema_;
  Schema altered_schema_;
};

template<typename KeyTypeWrapper>
struct SliceTypeRowOps : public RowOpsBase {
  SliceTypeRowOps() : RowOpsBase(KeyTypeWrapper::kType, KeyTypeWrapper::kEncoding),
    strs_(kNumAllocatedElements), slices_(kNumAllocatedElements), cur(0) {}

  // Assumes the string representation of n is under strlen characters.
  std::string LeftZeroPadded(int n, int strlen) {
    return StringPrintf(Substitute("%0$0$1", strlen, PRId64).c_str(), static_cast<int64_t>(n));
  }

  void GenerateRow(int value, bool altered, KuduPartialRow* row) {
    if (value < 0) {
      CHECK_OK(row->SetNull(kColA));
      CHECK_OK(row->SetNull(kColB));
      if (altered) {
        CHECK_OK(row->SetNull(kColC));
      }
    } else {
      std::string string_a = LeftZeroPadded(value, 10);
      std::string string_b = LeftZeroPadded(value, 10);
      std::string string_c = LeftZeroPadded(value, 10);
      Slice slice_a(string_a);
      Slice slice_b(string_b);
      Slice slice_c(string_c);
      CHECK_OK(row->SetSliceCopy<TypeTraits<KeyTypeWrapper::kType>>(kColA, slice_a));
      CHECK_OK(row->SetSliceCopy<TypeTraits<KeyTypeWrapper::kType>>(kColB, slice_b));
      if (altered) {
        CHECK_OK(row->SetSliceCopy<TypeTraits<KeyTypeWrapper::kType>>(kColC, slice_c));
      }
    }
  }

  void* GenerateElement(int value) {
    strs_[cur] = LeftZeroPadded(value, kStrlen);
    slices_[cur] = Slice(strs_[cur]);
    return &slices_[cur++];
  }

  ColumnPredicate GenerateRangePredicate(const Schema& schema, size_t col, int lower, int upper) {
    // Key predicate strings and slices in scope in a vector.
    strs_[cur] = LeftZeroPadded(lower, 10);
    strs_[cur + 1] = LeftZeroPadded(upper, 10);
    slices_[cur] = Slice(strs_[cur]);
    slices_[cur + 1] = Slice(strs_[cur + 1]);
    auto pred = ColumnPredicate::Range(schema.column(col), &slices_[cur], &slices_[cur + 1]);
    cur += 2;
    return pred;
  }

  // To avoid issues where either vector is resized, potentially disrupting the correspondence of
  // address/Slice to string, preallocate the sizes of these vectors as needed.
  std::vector<std::string> strs_;
  std::vector<Slice> slices_;
  size_t cur;
};

template<typename KeyTypeWrapper>
struct NumTypeRowOps : public RowOpsBase {
  NumTypeRowOps() : RowOpsBase(KeyTypeWrapper::kType, KeyTypeWrapper::kEncoding),
    nums_(kNumAllocatedElements), cur(0) {}

  typedef typename TypeTraits<KeyTypeWrapper::kType>::cpp_type CppType;

  void GenerateRow(int value, bool altered, KuduPartialRow* row) {
    if (value < 0) {
      CHECK_OK(row->SetNull(kColA));
      CHECK_OK(row->SetNull(kColB));
      if (altered) {
        CHECK_OK(row->SetNull(kColC));
      }
    } else {
      row->Set<TypeTraits<KeyTypeWrapper::kType>>(kColA, value);
      row->Set<TypeTraits<KeyTypeWrapper::kType>>(kColB, value);
      if (altered) {
        row->Set<TypeTraits<KeyTypeWrapper::kType>>(kColC, value);
      }
    }
  }

  void* GenerateElement(int value) {
    nums_[cur] = value;
    return &nums_[cur++];
  }

  ColumnPredicate GenerateRangePredicate(const Schema& schema, size_t col, int lower, int upper) {
    nums_[cur] = lower;
    nums_[cur + 1] = upper;
    auto pred = ColumnPredicate::Range(schema.column(col), &nums_[cur], &nums_[cur + 1]);
    cur += 2;
    return pred;
  }

  // To avoid issues where the vector is resized, potentially disrupting the correspondence of
  // address to value, preallocate the sizes of this vector as needed.
  std::vector<CppType> nums_;
  size_t cur;
};

// Calculates the number of values in the range [lower, upper) given a sequential, completely
// non-null pattern that is repeated with the specified cardinality and the specified number
// of rows.
int ExpectedCountSequential(int nrows, int cardinality, int lower_val, int upper_val) {
  if (lower_val >= upper_val || lower_val >= cardinality) {
    return 0;
  }
  int lower = std::max(0, lower_val);
  int upper = std::min(cardinality, upper_val);
  int last_chunk_count = 0;
  int last_chunk_size = nrows % cardinality;

  if (last_chunk_size == 0 || last_chunk_size <= lower) {
    // e.g. lower: 3, upper: 8, cardinality:10, nrows: 23, last_chunk_size: 3
    // Resulting vector: [0001111100 0001111100 000]
    last_chunk_count = 0;
  } else if (last_chunk_size <= upper) {
    // e.g. lower: 3, upper: 8, cardinality:10, nrows: 25, last_chunk_size: 5
    // Resulting vector: [0001111100 0001111100 00011]
    last_chunk_count = last_chunk_size - lower;
  } else {
    // e.g. lower: 3, upper: 8, cardinality:10, nrows: 29, last_chunk_size: 9
    // Resulting vector: [0001111100 0001111100 000111110]
    last_chunk_count = upper - lower;
  }
  return (nrows / cardinality) * (upper - lower) + last_chunk_count;
}

// Calculates number of values in the range [lower_val, upper_val) for a repeating pattern
// like [00111 00222 00333]. The value lies between range [0, cardinality) and
// repeats every "cardinality" chunks where each chunk is "cardinality" rows.
// E.g. nrows: 34, cardinality: 5, null_upper: 2, lower_val: 1, upper_val: 4
// [-, -, 0, 0, 0, -, -, 1, 1, 1, -, -, 2, 2, 2, -, -, 3, 3, 3, -, -, 4, 4, 4, <-- stride
//  -, -, 0, 0, 0, -, -, 1, 1]
int ExpectedCountRepeating(int nrows, int cardinality, int null_upper, int lower_val,
                           int upper_val) {

  if (lower_val >= upper_val || lower_val >= cardinality || null_upper >= cardinality) {
    return 0;
  }
  int lower = std::max(0, lower_val);
  int upper = std::min(cardinality, upper_val);

  // Each stride comprises of cardinality chunks and each chunk comprises of cardinality rows.
  // For above example there is 1 full stride comprising of 25 (5 * 5) rows.
  int strides = nrows / (cardinality * cardinality);
  // For above example there are 3 full matching chunks in a full stride.
  int matching_chunks_per_stride = upper - lower;
  // Matching rows in a chunk where chunk itself is within lower and upper.
  // For above example if value in chunk lies between lower and upper
  // then there are 3 rows in each such matching chunk.
  int matching_rows_per_chunk = cardinality - null_upper;

  // For above example there is 1 full chunk in last partial stride.
  int chunks_in_last_stride = (nrows % (cardinality * cardinality)) / cardinality;
  int matching_chunks_in_last_stride = std::max(0, std::min(upper, chunks_in_last_stride) - lower);

  // For above example there are 4 remainder rows [-, -, 1, 1], 2 of which are within range.
  int remainder_rows = (nrows % (cardinality * cardinality)) % cardinality;
  int remainder_matching_rows = std::max(0, remainder_rows - null_upper);
  if (remainder_matching_rows > 0) {
    int val_in_remainder_rows = chunks_in_last_stride;
    if (!(val_in_remainder_rows >= lower && val_in_remainder_rows < upper)) {
      remainder_matching_rows = 0;
    }
  }

  return strides * matching_chunks_per_stride * matching_rows_per_chunk +
      matching_chunks_in_last_stride * matching_rows_per_chunk +
      remainder_matching_rows;
}

std::string TraceMsg(const std::string& test_name,  int expected, int actual) {
  std::ostringstream ss;
  ss << test_name << " Scan Results: " << expected << " expected, " << actual << " returned.";
  return ss.str();
}

// Tests for correctness by running predicates on repeated patterns of rows, specified by nrows,
// cardinality, and null_upper. RowOps is a specialized RowOpsBase, defining how the tablet should
// add rows and generate predicates for various types.
template <class RowOps>
class AllTypesScanCorrectnessTest : public KuduTabletTest {
public:
  AllTypesScanCorrectnessTest() : KuduTabletTest(RowOps().schema_), rowops_(RowOps()),
      schema_(rowops_.schema_), altered_schema_(schema_),
      base_nrows_(0), base_cardinality_(0), base_null_upper_(0), added_nrows_(0) {}

  void SetUp() override {
    KuduTabletTest::SetUp();
  }

  // Adds a pattern of sequential values in every "cardinality" rows with the
  // first "null_upper" being set to null. This pattern of sequential values is then
  // repeated after every "cardinality" rows.
  // E.g. nrows: 9, cardinality: 5, null_upper: 2
  // [-, -, 2, 3, 4, -, -, 2, 3]
  void FillTestTabletWithSequentialPattern(int nrows, int cardinality, int null_upper) {
    base_nrows_ = nrows;
    base_cardinality_ = cardinality;
    base_null_upper_ = null_upper;
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);
    for (int i = 0; i < nrows; i++) {
      CHECK_OK(row.SetInt32(0, i));

      // Populate the bottom of the repeating pattern with NULLs.
      // Note: Non-positive null_upper will yield a completely non-NULL column.
      if (i % cardinality < null_upper) {
        rowops_.GenerateRow(-1, false, &row);
      } else {
        rowops_.GenerateRow(i % cardinality, false, &row);
      }
      ASSERT_OK_FAST(writer.Insert(row));
    }
    ASSERT_OK(tablet()->Flush());
  }

  // Adds a pattern of repeating value for every "cardinality" rows with the
  // first "null_upper" being set to null. The repeating value cycles from
  // [0, cardinality) values after every "cardinality" rows. This helps test RLE.
  // E.g. nrows: 34, cardinality: 5, null_upper: 2
  // [-, -, 0, 0, 0, -, -, 1, 1, 1, -, -, 2, 2, 2, -, -, 3, 3, 3, -, -, 4, 4, 4, <-- stride
  //  -, -, 0, 0, 0, -, -, 1, 1]
  void FillTestTabletWithRepeatPattern(int nrows, int cardinality, int null_upper) {
    base_nrows_ = nrows;
    base_cardinality_ = cardinality;
    base_null_upper_ = null_upper;
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);
    int val = 0;
    for (int i = 0; i < nrows;) {
      CHECK_OK(row.SetInt32(0, i));
      // Populate the bottom of the repeating pattern with NULLs.
      // Note: Non-positive null_upper will yield a completely non-NULL column.
      if (i % cardinality < null_upper) {
        rowops_.GenerateRow(-1, false, &row);
      } else {
        rowops_.GenerateRow(val, false, &row);
      }
      ASSERT_OK_FAST(writer.Insert(row));
      i++;
      if (i % cardinality == 0) {
        val = (val + 1) % cardinality;
      }
    }
    ASSERT_OK(tablet()->Flush());
  }

  // Adds the above pattern to the table with keys starting after the base rows.
  void FillAlteredTestTablet(int nrows) {
    added_nrows_ = nrows;
    LocalTabletWriter writer(tablet().get(), &altered_schema_);
    KuduPartialRow row(&altered_schema_);
    for (int i = 0; i < nrows; i++) {
      CHECK_OK(row.SetInt32(0, base_nrows_ + i));
      if (i % base_cardinality_ < base_null_upper_) {
        rowops_.GenerateRow(-1, true, &row);
      } else {
        rowops_.GenerateRow(i % base_cardinality_, true, &row);
      }
      ASSERT_OK_FAST(writer.Upsert(row));
    }
    ASSERT_OK(tablet()->Flush());
  }

  // Adds a column called "val_c" to the schema with the specified read-default.
  void AddColumn(int read_default) {
    void* default_ptr;
    if (read_default < 0) {
      default_ptr = nullptr;
    } else {
      default_ptr = rowops_.GenerateElement(read_default);
    }
    SchemaBuilder builder(*tablet()->metadata()->schema());
    builder.RemoveColumn("val_c");
    ASSERT_OK(builder.AddColumn("val_c", rowops_.type_, true, default_ptr, nullptr));
    AlterSchema(builder.Build());
    altered_schema_ = Schema({ColumnSchema("key", INT32),
                     ColumnSchema("val_a", rowops_.type_, true, nullptr, nullptr,
                         ColumnStorageAttributes(rowops_.encoding_, DEFAULT_COMPRESSION)),
                     ColumnSchema("val_b", rowops_.type_, true, nullptr, nullptr,
                         ColumnStorageAttributes(rowops_.encoding_, DEFAULT_COMPRESSION)),
                     ColumnSchema("val_c", rowops_.type_, true, default_ptr, nullptr,
                         ColumnStorageAttributes(rowops_.encoding_, DEFAULT_COMPRESSION))}, 1);
  }

  // Scan the results of a query. Set "count" to the number of results satisfying the predicates.
  // ScanSpec must have all desired predicates already added to it.
  void ScanWithSpec(const Schema& schema, ScanSpec spec, int* count) {
    Arena arena(1024);
    *count = 0;
    spec.OptimizeScan(schema, &arena, true);
    unique_ptr<RowwiseIterator> iter;
    SchemaPtr schema_ptr = std::make_shared<Schema>(schema);
    ASSERT_OK(tablet()->NewRowIterator(schema_ptr, &iter));
    ASSERT_OK(iter->Init(&spec));
    ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicate.";
    ASSERT_OK(SilentIterateToStringList(iter.get(), count));
  }

  void RunSequentialTests() {
    RunUnalteredSequentialTabletTests();
    RunAlteredSequentialTabletTests();
  }

  void RunRepeatingTests() {
    RunUnalteredRepeatingTabletTests();
  }

  void RunUnalteredRepeatingTabletTests() {
    int count = 0;
    {
      ScanSpec spec;
      auto pred = rowops_.GenerateRangePredicate(schema_, kColA, kLower, kUpper);
      spec.AddPredicate(pred);
      LOG_TIMING(INFO, "Range") {
        ScanWithSpec(schema_, spec, &count);
      }
      int expected_count = ExpectedCountRepeating(base_nrows_, base_cardinality_, base_null_upper_,
                                                  kLower, kUpper);
      SCOPED_TRACE(TraceMsg("Range", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }

    {
      // MultiColumn Range scan
      // This predicates two columns:
      //   col_a: [0, upper_val)
      //   col_b: [lower_val, cardinality)
      // Since the two columns have identical data, the result will be:
      //   AND:   [lower_val, upper_val)
      ScanSpec spec;
      ColumnPredicate pred_a = rowops_.GenerateRangePredicate(schema_, kColA, 0, kUpper);
      spec.AddPredicate(pred_a);
      ColumnPredicate pred_b = rowops_.GenerateRangePredicate(schema_, kColB, kLower,
                                                              base_cardinality_);
      spec.AddPredicate(pred_b);
      LOG_TIMING(INFO, "MultiColumn Range") {
        ScanWithSpec(schema_, spec, &count);
      }
      int expected_count = ExpectedCountRepeating(base_nrows_, base_cardinality_, base_null_upper_,
                                                  kLower, kUpper);
      SCOPED_TRACE(TraceMsg("MultiColumn Range", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }

    {
      ScanSpec spec;
      auto pred = ColumnPredicate::IsNotNull(schema_.column(kColB));
      spec.AddPredicate(pred);
      LOG_TIMING(INFO, "IsNotNull") {
        ScanWithSpec(schema_, spec, &count);
      }
      int expected_count = ExpectedCountRepeating(base_nrows_, base_cardinality_, base_null_upper_,
                                                  0, base_cardinality_);
      SCOPED_TRACE(TraceMsg("IsNotNull", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }

    {
      ScanSpec spec;
      auto pred = ColumnPredicate::IsNull(schema_.column(kColB));
      spec.AddPredicate(pred);
      LOG_TIMING(INFO, "IsNull") {
        ScanWithSpec(schema_, spec, &count);
      }
      int expected_count = base_nrows_ -
          ExpectedCountRepeating(base_nrows_, base_cardinality_, base_null_upper_, 0,
                                 base_cardinality_);
      SCOPED_TRACE(TraceMsg("IsNull", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
  }

  // Runs queries on an un-altered table. Correctness is determined by comparing the number of rows
  // returned with the number of rows expected by each query.
  void RunUnalteredSequentialTabletTests() {
    int lower_non_null = kLower;
    if (kLower < base_null_upper_) {
      lower_non_null = base_null_upper_;
    }

    int count = 0;
    {
      ScanSpec spec;
      auto pred = rowops_.GenerateRangePredicate(schema_, kColA, kLower, kUpper);
      spec.AddPredicate(pred);
      ScanWithSpec(schema_, spec, &count);
      int expected_count =
          ExpectedCountSequential(base_nrows_, base_cardinality_, lower_non_null, kUpper);
      SCOPED_TRACE(TraceMsg("Range", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    {
      // MultiColumn Range scan
      // This predicates two columns:
      //   col_a: [0, upper_val)
      //   col_b: [lower_val, cardinality)
      // Since the two columns have identical data, the result will be:
      //   AND:   [lower_val, upper_val)
      ScanSpec spec;
      ColumnPredicate pred_a = rowops_.GenerateRangePredicate(schema_, kColA, 0, kUpper);
      spec.AddPredicate(pred_a);
      ColumnPredicate pred_b = rowops_.GenerateRangePredicate(schema_, kColB, kLower,
                                                              base_cardinality_);
      spec.AddPredicate(pred_b);
      ScanWithSpec(schema_, spec, &count);
      int expected_count =
          ExpectedCountSequential(base_nrows_, base_cardinality_, lower_non_null, kUpper);
      SCOPED_TRACE(TraceMsg("MultiColumn Range", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    {
      ScanSpec spec;
      auto pred = ColumnPredicate::IsNotNull(schema_.column(kColB));
      spec.AddPredicate(pred);
      ScanWithSpec(schema_, spec, &count);
      int expected_count = ExpectedCountSequential(base_nrows_, base_cardinality_, base_null_upper_,
                                                   base_cardinality_);
      SCOPED_TRACE(TraceMsg("IsNotNull", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    {
      ScanSpec spec;
      auto pred = ColumnPredicate::IsNull(schema_.column(kColB));
      spec.AddPredicate(pred);
      ScanWithSpec(schema_, spec, &count);
      int expected_count =
          ExpectedCountSequential(base_nrows_, base_cardinality_, 0, base_null_upper_);
      SCOPED_TRACE(TraceMsg("IsNull", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
  }

  // Runs tests with an altered table. Queries are run with different read-defaults and are deemed
  // correct if they return the correct number of results.
  void RunAlteredSequentialTabletTests() {
    int lower_non_null = kLower;
    // Determine the lowest non-null value in the data range. Used in getting expected counts.
    if (kLower < base_null_upper_) {
      lower_non_null = base_null_upper_;
    }
    // Tests with null read-default. Ex. case: (-: null, 1: pred satisfied, 0: pred not satisfied)
    // kLower: 5, kUpper: 8
    // Base nrows: 30                 |Altered nrows: 25
    // Cardinality: 20, null_upper: 3, read_default: NULL
    // Predicate: (val_b >= 0 && val_b < 8) && (val_c >= 5 && val_c < 20)
    //  0    5    10   15   20   25   |30   35   40   45   50     key
    // [---11111000000000000---1111100|---11111000000000000---11] val_b
    // [------------------------------|---00111111111111111---00] val_c
    // [000000000000000000000000000000|0000011100000000000000000] Result
    AddColumn(-1);
    FillAlteredTestTablet(kNumAddedRows);
    int count = 0;
    {
      ScanSpec spec;
      // val_b >= kLower && val_b <= kUpper && val_c is null
      auto pred_b = rowops_.GenerateRangePredicate(altered_schema_, kColB, kLower, kUpper);
      auto pred_c = ColumnPredicate::IsNull(altered_schema_.column(kColC));
      spec.AddPredicate(pred_b);
      spec.AddPredicate(pred_c);
      ScanWithSpec(altered_schema_, spec, &count);
      int base_expected_count = ExpectedCountSequential(base_nrows_,
                                                        base_cardinality_,
                                                        lower_non_null,
                                                        kUpper);
      // Since the new column has the same data as the base columns, IsNull with a Range predicate
      // should yield no rows from the added rows.
      int altered_expected_count = 0;
      int expected_count = base_expected_count + altered_expected_count;
      SCOPED_TRACE(TraceMsg("Null default, Range+IsNull", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    {
      ScanSpec spec;
      // val_b >= kLower && val_b <= kUpper && val_c is not null
      auto pred_b = rowops_.GenerateRangePredicate(altered_schema_, kColB, kLower, kUpper);
      auto pred_c = ColumnPredicate::IsNotNull(altered_schema_.column(kColC));
      spec.AddPredicate(pred_b);
      spec.AddPredicate(pred_c);
      ScanWithSpec(altered_schema_, spec, &count);
      // Since the table has a null read-default on the added column, the IsNotNull predicate
      // should filter out all rows in the base data.
      int base_expected_count = 0;
      int altered_expected_count = ExpectedCountSequential(added_nrows_,
                                                           base_cardinality_,
                                                           lower_non_null,
                                                           kUpper);
      int expected_count = base_expected_count + altered_expected_count;
      SCOPED_TRACE(TraceMsg("Null default, Range+IsNotNull", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    {
      ScanSpec spec;
      // val_b >= 0 && val_b < kUpper && val_c >= kLower && val_c < cardinality
      auto pred_b = rowops_.GenerateRangePredicate(altered_schema_, kColB, 0, kUpper);
      auto pred_c = rowops_.GenerateRangePredicate(altered_schema_, kColC, kLower,
                                                   base_cardinality_);
      spec.AddPredicate(pred_b);
      spec.AddPredicate(pred_c);
      ScanWithSpec(altered_schema_, spec, &count);
      // Since the added column has a null read-default, the base rows will be completely filtered.
      int base_expected_count = 0;
      // The added data will be predicated with [lower, upper).
      int altered_expected_count = ExpectedCountSequential(added_nrows_,
                                                           base_cardinality_,
                                                           lower_non_null,
                                                           kUpper);
      int expected_count = base_expected_count + altered_expected_count;
      SCOPED_TRACE(TraceMsg("Null default, Range", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    // Tests with non-null read-default. Ex. case:
    // kLower: 5, kUpper: 8
    // Base nrows: 30                |Altered nrows: 25
    // Cardinality: 20, null_upper: 3, read_default: 7
    // Predicate: (val_b >= 0 && val_b < 8) && (val_c >= 5 && val_c < 20)
    //  0    5    10   15   20   25   |30   35   40   45   50     key
    // [---11111000000000000---1111100|---11111000000000000---11] val_b
    // [111111111111111111111111111111|---00111111111111111---00] val_c
    // [000111110000000000000001111100|0000011100000000000000000] Result
    int read_default = 7;
    AddColumn(read_default);
    FillAlteredTestTablet(kNumAddedRows);
    {
      ScanSpec spec;
      // val_b >= kLower && val_b <= kUpper && val_c is null
      auto pred_b = rowops_.GenerateRangePredicate(altered_schema_, kColB, kLower, kUpper);
      auto pred_c = ColumnPredicate::IsNull(altered_schema_.column(kColC));
      spec.AddPredicate(pred_b);
      spec.AddPredicate(pred_c);
      ScanWithSpec(altered_schema_, spec, &count);
      // The base data is not null, since there is a read-default.
      int base_expected_count = 0;
      // Since the new column has the same data as the base columns, IsNull with a Range predicate
      // should yield no rows from the added rows.
      int altered_expected_count = 0;
      int expected_count = base_expected_count + altered_expected_count;
      SCOPED_TRACE(TraceMsg("Non-null default, Range+IsNull", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    {
      ScanSpec spec;
      // val_b >= kLower && val_b <= kUpper && val_c is not null
      auto pred_b = rowops_.GenerateRangePredicate(altered_schema_, kColB, kLower, kUpper);
      auto pred_c = ColumnPredicate::IsNotNull(altered_schema_.column(kColC));
      spec.AddPredicate(pred_b);
      spec.AddPredicate(pred_c);
      ScanWithSpec(altered_schema_, spec, &count);
      int base_expected_count =
          ExpectedCountSequential(base_nrows_, base_cardinality_, lower_non_null,
                                  kUpper);
      // Since the new column has the same data as the base columns, IsNotNull with a Range
      // predicate should yield the same rows as the Range query alone on the altered data.
      int altered_expected_count =
          ExpectedCountSequential(added_nrows_, base_cardinality_, lower_non_null,
                                  kUpper);
      int expected_count = base_expected_count + altered_expected_count;
      SCOPED_TRACE(TraceMsg("Non-null default, Range+IsNotNull", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    {
      int lower = 0;
      ScanSpec spec;
      // val_b >= 0 && val_b < kUpper && val_c >= kLower && val_c < cardinality
      auto pred_b = rowops_.GenerateRangePredicate(altered_schema_, kColB, 0, kUpper);
      auto pred_c = rowops_.GenerateRangePredicate(altered_schema_, kColC, kLower,
                                                   base_cardinality_);
      spec.AddPredicate(pred_b);
      spec.AddPredicate(pred_c);
      ScanWithSpec(altered_schema_, spec, &count);
      // Take into account possible null values when calculating expected counts.
      if (base_null_upper_ > 0) {
        lower = base_null_upper_;
      }
      // Because the read_default is in range, the predicate on "val_c" will be satisfied
      // by base data, and all rows that satisfy "pred_b" will be returned.
      int base_expected_count =
          ExpectedCountSequential(base_nrows_, base_cardinality_, lower, kUpper);
      int altered_expected_count = ExpectedCountSequential(added_nrows_,
                                                           base_cardinality_,
                                                           lower_non_null,
                                                           kUpper);
      int expected_count = base_expected_count + altered_expected_count;
      SCOPED_TRACE(TraceMsg("Non-null default, Range with Default", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
    {
      // Used to ensure the query does not select the read-default.
      int default_plus_one = read_default + 1;
      ScanSpec spec;
      // val_b >= 0 && val_b < kUpper && val_c >= read_default + 1 && val_c < cardinality
      auto pred_b = rowops_.GenerateRangePredicate(altered_schema_, kColB, 0, kUpper);
      auto pred_c = rowops_.GenerateRangePredicate(altered_schema_, kColC, default_plus_one,
                                                   base_cardinality_);
      spec.AddPredicate(pred_b);
      spec.AddPredicate(pred_c);
      ScanWithSpec(altered_schema_, spec, &count);
      if (default_plus_one < base_null_upper_) {
        default_plus_one = base_null_upper_;
      }
      // Because the read_default is out of range, the "pred_c" will not be satisfied by base data,
      // so all base rows will be filtered.
      int base_expected_count = 0;
      int altered_expected_count = ExpectedCountSequential(added_nrows_,
                                                           base_cardinality_,
                                                           default_plus_one,
                                                           kUpper);
      int expected_count = base_expected_count + altered_expected_count;
      SCOPED_TRACE(TraceMsg("Non-null default, Range without Default", expected_count, count));
      ASSERT_EQ(expected_count, count);
    }
  }

protected:
  RowOps rowops_;
  Schema schema_;
  Schema altered_schema_;
  int base_nrows_;
  int base_cardinality_;
  int base_null_upper_;
  int added_nrows_;
};

template<DataType KeyType, EncodingType Encoding>
struct KeyTypeWrapper {
  static const DataType kType = KeyType;
  static const EncodingType kEncoding = Encoding;
};

typedef ::testing::Types<NumTypeRowOps<KeyTypeWrapper<INT8, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT8, PLAIN_ENCODING>>,
                         NumTypeRowOps<KeyTypeWrapper<INT8, RLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT16, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT16, PLAIN_ENCODING>>,
                         NumTypeRowOps<KeyTypeWrapper<INT16, RLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT32, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT32, PLAIN_ENCODING>>,
                         NumTypeRowOps<KeyTypeWrapper<INT32, RLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT64, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT64, PLAIN_ENCODING>>,
                         NumTypeRowOps<KeyTypeWrapper<INT64, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT64, RLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT128, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<INT128, PLAIN_ENCODING>>,
                         // TODO: Uncomment when adding 128 bit support to RLE (KUDU-2284)
                         // NumTypeRowOps<KeyTypeWrapper<INT128, RLE>>,
                         NumTypeRowOps<KeyTypeWrapper<FLOAT, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<FLOAT, PLAIN_ENCODING>>,
                         NumTypeRowOps<KeyTypeWrapper<DOUBLE, BIT_SHUFFLE>>,
                         NumTypeRowOps<KeyTypeWrapper<DOUBLE, PLAIN_ENCODING>>,
                         SliceTypeRowOps<KeyTypeWrapper<STRING, DICT_ENCODING>>,
                         SliceTypeRowOps<KeyTypeWrapper<STRING, PLAIN_ENCODING>>,
                         SliceTypeRowOps<KeyTypeWrapper<STRING, PREFIX_ENCODING>>,
                         SliceTypeRowOps<KeyTypeWrapper<BINARY, DICT_ENCODING>>,
                         SliceTypeRowOps<KeyTypeWrapper<BINARY, PLAIN_ENCODING>>,
                         SliceTypeRowOps<KeyTypeWrapper<BINARY, PREFIX_ENCODING>>
                         > KeyTypes;

TYPED_TEST_SUITE(AllTypesScanCorrectnessTest, KeyTypes);

TYPED_TEST(AllTypesScanCorrectnessTest, AllNonNullSequential) {
  int null_upper = 0;
  this->FillTestTabletWithSequentialPattern(kNumBaseRows, kCardinality, null_upper);
  this->RunSequentialTests();
}

TYPED_TEST(AllTypesScanCorrectnessTest, SomeNullSequential) {
  int null_upper = kUpper/2;
  this->FillTestTabletWithSequentialPattern(kNumBaseRows, kCardinality, null_upper);
  this->RunSequentialTests();
}

TYPED_TEST(AllTypesScanCorrectnessTest, AllNullSequential) {
  int null_upper = kCardinality;
  this->FillTestTabletWithSequentialPattern(kNumBaseRows, kCardinality, null_upper);
  this->RunSequentialTests();
}

TYPED_TEST(AllTypesScanCorrectnessTest, AllNonNullRepeating) {
  int null_upper = 0;
  this->FillTestTabletWithRepeatPattern(kNumBaseRows, kCardinality, null_upper);
  this->RunRepeatingTests();
}

TYPED_TEST(AllTypesScanCorrectnessTest, AllNullRepeating) {
  int null_upper = kCardinality;
  this->FillTestTabletWithRepeatPattern(kNumBaseRows, kCardinality, null_upper);
  this->RunRepeatingTests();
}

TYPED_TEST(AllTypesScanCorrectnessTest, SomeNullRepeating) {
  int null_upper = kUpper / 2;
  this->FillTestTabletWithRepeatPattern(kNumBaseRows, kCardinality, null_upper);
  this->RunRepeatingTests();
}

}  // namespace tablet
}  // namespace kudu
