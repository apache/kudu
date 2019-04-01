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
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/auto_release_pool.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(decoder_eval_test_nrepeats, 1, "Number of times to repeat per tablet");
DEFINE_int32(decoder_eval_test_lower, 0, "Lower bound on the predicate [lower, upper)");
DEFINE_int32(decoder_eval_test_upper, 50, "Upper bound on the predicate [lower, upper)");
DEFINE_int32(decoder_eval_test_strlen, 10, "Number of strings per cell");

using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace tablet {

enum Setup {
#ifdef ADDRESS_SANITIZER
  EMPTY = 0, SMALL = 100, MEDIUM = 3000, LARGE = 10000
#else
  EMPTY = 0, SMALL = 100, MEDIUM = 5000, LARGE = 100000
#endif
};

class TabletDecoderEvalTest : public KuduTabletTest,
                              public ::testing::WithParamInterface<Setup> {
public:
  TabletDecoderEvalTest()
          : KuduTabletTest(Schema({ColumnSchema("key", INT32),
                                   ColumnSchema("string_val_a", STRING, true, NULL, NULL,
                                                ColumnStorageAttributes(DICT_ENCODING,
                                                                        DEFAULT_COMPRESSION)),
                                   ColumnSchema("string_val_b", STRING, true, NULL, NULL,
                                                ColumnStorageAttributes(DICT_ENCODING,
                                                                        DEFAULT_COMPRESSION))}, 1))
  {}

  void SetUp() override {
    KuduTabletTest::SetUp();
  }

  void ScanAndFilter(size_t cardinality, size_t lower, size_t upper, int null_upper) {
    if (GetParam() == LARGE && !AllowSlowTests()) {
      LOG(INFO) << "Skipped large test case";
      return;
    }
    size_t nrows = static_cast<size_t>(GetParam());
    // The correctness check of this test requires that the int and string
    // comparators for the values in the tablets match up. Adjust the lengths
    // of the strings to enforce this.
    // e.g. scanning ["00", ..., "99"] for > "111" would return numerically
    // incorrect values, but ["000", ..., "099"] would return correct values.
    size_t strlen = std::max({static_cast<size_t>(FLAGS_decoder_eval_test_strlen),
                             Substitute("$0", upper).length(),
                             Substitute("$0", cardinality).length()});

    // Fill tablet, a negative null_upper will yield no NULLs.
    FillTestTablet(nrows, cardinality, strlen, null_upper);
    int fetched = 0;
    size_t lower_not_null = lower;
    if (null_upper > static_cast<int>(lower)) {
      // If null_upper is greater than the lower bound, the expected results
      // will be calculated with null_upper as the lower bound.
      // e.g. null_upper: 3, lower: 2, upper: 6
      // NULL, NULL, NULL, "3", "4", "5", NULL, NULL, NULL, "3", "4", "5", ...
      // Expected result will be calculated as if the query were [3, 6).
      lower_not_null = null_upper;
    }

    for (int i = 0; i < FLAGS_decoder_eval_test_nrepeats; i++) {
      TestTimedScanWithBounds(strlen, lower, upper, &fetched);

      // Calculate the expected count, potentially factoring in nulls.
      size_t expected_sel_count = ExpectedCount(nrows, cardinality, lower_not_null, upper);
      ASSERT_EQ(expected_sel_count, fetched);
      LOG(INFO) << "Nrows: " << nrows
                << ", Strlen: " << strlen
                << ", Expected: " << expected_sel_count
                << ", Actual: " << fetched;
    }
  }

  void TestScanAndFilter(size_t cardinality, size_t lower, size_t upper) {
    ScanAndFilter(cardinality, lower, upper, -1);
  }

  void TestNullableScanAndFilter(size_t cardinality, size_t lower, size_t upper, int null_upper) {
    ScanAndFilter(cardinality, lower, upper, null_upper);
  }

  void FillTestTablet(size_t nrows, size_t cardinality, size_t strlen, int null_upper) {
    RowBuilder rb(&client_schema_);
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);
    for (int64_t i = 0; i < nrows; i++) {
      CHECK_OK(row.SetInt32(0, i));

      // Populate the bottom of the repeating pattern with NULLs.
      // Note: Negative null_upper will yield a completely non-NULL column.
      if (static_cast<int>(i % cardinality) < null_upper) {
        CHECK_OK(row.SetNull(1));
        CHECK_OK(row.SetNull(2));
      } else {
        CHECK_OK(row.SetStringCopy(1, LeftZeroPadded(i % cardinality, strlen)));
        CHECK_OK(row.SetStringCopy(2, LeftZeroPadded(i % cardinality, strlen)));
      }
      ASSERT_OK_FAST(writer.Insert(row));
    }
    ASSERT_OK(tablet()->Flush());
  }

  void TestTimedScanWithBounds(size_t strlen, size_t lower_val,
                               size_t upper_val, int* fetched) {
    Arena arena(128);
    AutoReleasePool pool;
    ScanSpec spec;

    // Generate the predicate.
    const string lower_string = LeftZeroPadded(lower_val, strlen);
    const string upper_string = LeftZeroPadded(upper_val, strlen);
    Slice lower(lower_string);
    Slice upper(upper_string);
    auto string_pred = ColumnPredicate::Range(schema_.column(2), &lower, &upper);

    // Prepare the scan.
    spec.AddPredicate(string_pred);
    spec.OptimizeScan(schema_, &arena, &pool, true);
    ScanSpec orig_spec = spec;
    unique_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(client_schema_, &iter));
    spec = orig_spec;
    ASSERT_OK(iter->Init(&spec));
    ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicates";

    // Execute and time the scan. Argument fetched is an output and will be set
    // to the number of rows returned in the result set.
    LOG_TIMING(INFO, "Filtering by string value") {
      ASSERT_OK(SilentIterateToStringList(iter.get(), fetched));
    }
  }

  size_t ExpectedCount(size_t nrows, size_t cardinality, size_t lower, size_t upper) {
    if (lower >= upper || lower >= cardinality) {
      return 0;
    }
    lower = std::max(static_cast<size_t>(0), lower);
    upper = std::min(cardinality, upper);
    size_t last_chunk_count = 0;
    size_t last_chunk_size = nrows % cardinality;

    if (last_chunk_size == 0 || last_chunk_size <= lower) {
      // e.g. lower: 3, upper: 8, cardinality:10, nrows: 23, last_chunk_size: 3
      // Resulting vector: [0001111100|0001111100|000]
      last_chunk_count = 0;
    } else if (last_chunk_size <= upper) {
      // e.g. lower: 3, upper: 8, cardinality:10, nrows: 25, last_chunk_size: 5
      // Resulting vector: [0001111100|0001111100|00011]
      last_chunk_count = last_chunk_size - lower;
    } else {
      // e.g. lower: 3, upper: 8, cardinality:10, nrows: 29, last_chunk_size: 9
      // Resulting vector: [0001111100|0001111100|000111110]
      last_chunk_count = upper - lower;
    }
    return (nrows / cardinality) * (upper - lower) + last_chunk_count;
  }

  string LeftZeroPadded(size_t n, size_t strlen) {
    // Assumes the string representation of n is under strlen characters.
    return StringPrintf(Substitute("%0$0$1", strlen, PRId64).c_str(), static_cast<int64_t>(n));
  }

  void TestMultipleColumnPredicates(size_t cardinality, size_t lower, size_t upper) {
    if (GetParam() == LARGE && !AllowSlowTests()) {
      LOG(INFO) << "Skipped large test case";
      return;
    }
    size_t nrows = static_cast<size_t>(GetParam());
    size_t strlen = std::max({static_cast<size_t>(FLAGS_decoder_eval_test_strlen),
                              Substitute("$0", upper).length(),
                              Substitute("$0", cardinality).length()});
    FillTestTablet(nrows, 10, strlen, -1);
    Arena arena(128);
    AutoReleasePool pool;
    ScanSpec spec;

    // Generate the predicates [0, upper) AND [lower, cardinality).
    const string lower_string_a(LeftZeroPadded(0, strlen));
    const string upper_string_a(LeftZeroPadded(upper, strlen));
    Slice lower_a(lower_string_a);
    Slice upper_a(upper_string_a);
    const string lower_string_b = LeftZeroPadded(lower, strlen);
    const string upper_string_b = LeftZeroPadded(cardinality, strlen);
    Slice lower_b(lower_string_b);
    Slice upper_b(upper_string_b);

    // This will exercise CopyNextAndEval's skipping behavior in the decoders
    // that support evaluation. Decoders should skip over rows that have been
    // deemed to not be returned by a prior column evaluation.
    auto string_pred_a = ColumnPredicate::Range(schema_.column(1), &lower_a, &upper_a);
    auto string_pred_b = ColumnPredicate::Range(schema_.column(2), &lower_b, &upper_b);

    // Prepare the scan.
    spec.AddPredicate(string_pred_a);
    spec.AddPredicate(string_pred_b);
    spec.OptimizeScan(schema_, &arena, &pool, true);
    ScanSpec orig_spec = spec;
    unique_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(client_schema_, &iter));
    spec = orig_spec;
    ASSERT_OK(iter->Init(&spec));
    ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicates";

    Arena ret_arena(1024);
    size_t expected_count = ExpectedCount(nrows, cardinality, lower, upper);
    Schema schema = iter->schema();
    RowBlock block(&schema, 100, &ret_arena);
    int fetched = 0;
    string column_str_a, column_str_b;
    while (iter->HasNext()) {
      ASSERT_OK(iter->NextBlock(&block));
      for (size_t i = 0; i < block.nrows(); i++) {
        if (block.selection_vector()->IsRowSelected(i)) {
          column_str_a = schema.column(1).Stringify(block.row(i).cell(1).ptr());
          column_str_b = schema.column(2).Stringify(block.row(i).cell(2).ptr());
          // Correct skipping should yield matching strings between columns.
          ASSERT_TRUE(std::strcmp(column_str_a.c_str(), column_str_b.c_str()) == 0);
          fetched++;
        }
      }
    }
    ASSERT_EQ(fetched, expected_count);
  }
};

TEST_P(TabletDecoderEvalTest, LowCardinality) {
  TestScanAndFilter(50, FLAGS_decoder_eval_test_lower, FLAGS_decoder_eval_test_upper);
}

TEST_P(TabletDecoderEvalTest, MidCardinality) {
  TestScanAndFilter(1000, FLAGS_decoder_eval_test_lower, FLAGS_decoder_eval_test_upper);
}

TEST_P(TabletDecoderEvalTest, HighCardinality) {
  TestScanAndFilter(50000, FLAGS_decoder_eval_test_lower, FLAGS_decoder_eval_test_upper);
}

TEST_P(TabletDecoderEvalTest, EvaluateEmpty) {
  // Predicate [k, k+5) will not evaluate to None, but will return no rows.
  TestScanAndFilter(50, 50, 55);
}

TEST_P(TabletDecoderEvalTest, NullableLowCardinality) {
  // Fill a tablet with pattern [0, 50) but with values [0, 40) as NULL.
  // Query for values [30, 50).
  TestNullableScanAndFilter(50, 30, 50, 40);
}

TEST_P(TabletDecoderEvalTest, NullableMidCardinality) {
  // Fill a tablet with pattern [0, 1000) but with values [0, 50) as NULL.
  // Query for values [30, 100).
  TestNullableScanAndFilter(1000, 30, 100, 50);
}

TEST_P(TabletDecoderEvalTest, NullableHighCardinality) {
  // Fill a tablet with pattern [0, 50000) but with values [0, 75) as NULL.
  // Query for values [30, 200).
  TestNullableScanAndFilter(50000, 30, 200, 75);
}

TEST_P(TabletDecoderEvalTest, CompletelyNullColumn) {
  // Fill a tablet with pattern [0, 50) but with all values being NULL.
  // Query for values [30, 50).
  TestNullableScanAndFilter(50, 30, 50, 50);
}

TEST_P(TabletDecoderEvalTest, MultipleColumns) {
  // Fill a tablet with pattern [0, 10) and query a:[0, 5) AND b:[3, 10).
  // To be considered correct, returned columns must align as they do in the
  // table and the correct number of rows must be returned.
  TestMultipleColumnPredicates(10, 3, 5);
}

INSTANTIATE_TEST_CASE_P(DecoderEvaluation, TabletDecoderEvalTest, ::testing::Values(EMPTY,
                                                                                    SMALL,
                                                                                    MEDIUM,
                                                                                    LARGE));

}   // namespace tablet
}   // namespace kudu
