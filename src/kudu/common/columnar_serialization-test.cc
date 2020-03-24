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

#include "kudu/common/columnar_serialization.h"

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/bitmap.h"
#include "kudu/util/faststring.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/test_util.h"

using std::vector;

namespace kudu {

class ColumnarSerializationTest : public KuduTest {
 protected:
  ColumnarSerializationTest() : rng_(SeedRandom()) {
  }

  // TODO(todd): templatize this test for other types once we have specialized
  // implementations.
  using DataType = uint32_t;
  static constexpr int kTypeSize = sizeof(DataType);

  struct RandomCellsAndNulls {
    vector<DataType> vals;
    faststring non_nulls;

    void VerifyNullsAreZeroed() {
      for (int i = 0; i < vals.size(); i++) {
        SCOPED_TRACE(i);
        if (BitmapTest(non_nulls.data(), i)) {
          EXPECT_EQ(0xdeadbeef, vals[i]);
        } else {
          EXPECT_EQ(0, vals[i]);
        }
      }
    }
  };

  // Generate a random bitmap with the given number of bits.
  faststring RandomBitmap(int n_bits) {
    faststring bm;
    bm.resize(BitmapSize(n_bits));

    for (int i = 0; i < n_bits; i++) {
      BitmapChange(bm.data(), i, rng_.OneIn(3));
    }
    return bm;
  }

  // Create an array of 0xdeadbeef values and a corresponding
  // null bitmap with random entries set to null.
  RandomCellsAndNulls CreateDeadBeefsWithRandomNulls() {
    auto num_rows = rng_.Uniform(1000) + 1;
    vector<uint32_t> vals(num_rows, 0xdeadbeef);
    faststring non_nulls = RandomBitmap(num_rows);
    return { std::move(vals), std::move(non_nulls) };
  }

  Random rng_;
};


// Simple test of ZeroNullValues for a whole array.
TEST_F(ColumnarSerializationTest, TestZeroNullValues) {
  auto data = CreateDeadBeefsWithRandomNulls();

  internal::ZeroNullValues(
      kTypeSize, /* dst_idx= */0,
      data.vals.size(),
      reinterpret_cast<uint8_t*>(data.vals.data()),
      data.non_nulls.data());

  ASSERT_NO_FATAL_FAILURE(data.VerifyNullsAreZeroed());
}

// More complex test test of ZeroNullValues which runs on sub-ranges
// of an array.
TEST_F(ColumnarSerializationTest, TestZeroNullValuesWithOffset) {
  auto data = CreateDeadBeefsWithRandomNulls();
  int dst_idx = 0;
  while (dst_idx < data.vals.size()) {
    auto rem = data.vals.size() - dst_idx;
    auto n = rng_.Uniform(rem) + 1;
    internal::ZeroNullValues(
        kTypeSize, dst_idx, n,
        reinterpret_cast<uint8_t*>(data.vals.data()),
        data.non_nulls.data());
    dst_idx += n;
  }
  ASSERT_NO_FATAL_FAILURE(data.VerifyNullsAreZeroed());
}

TEST_F(ColumnarSerializationTest, TestCopyNonNullBitmap) {
  auto save_method = internal::g_pext_method;
  SCOPED_CLEANUP({ internal::g_pext_method = save_method; });
  // Test using all available methods. Depending on the machine where
  // the test is running we might miss some, but we typically run this
  // test on relatively recent hardware that would support BMI2 (Haswell
  // or later).
  auto available_methods = internal::GetAvailablePextMethods();
  for (auto m : available_methods) {
    SCOPED_TRACE(static_cast<int>(m));
    internal::g_pext_method = m;
    auto n_rows = 1 + rng_.Uniform(200);
    faststring non_null_bitmap = RandomBitmap(n_rows);
    faststring sel_bitmap = RandomBitmap(n_rows);
    faststring dst_bitmap;
    dst_bitmap.resize(BitmapSize(n_rows));

    internal::CopyNonNullBitmap(
        non_null_bitmap.data(), sel_bitmap.data(),
        /*dst_idx=*/0, n_rows,
        dst_bitmap.data());

    vector<bool> expected;
    ForEachSetBit(sel_bitmap.data(), n_rows,
                  [&](size_t bit) {
                    expected.push_back(BitmapTest(non_null_bitmap.data(), bit));
                  });
    LOG(INFO) << "non-null:  " << BitmapToString(non_null_bitmap.data(), n_rows);
    LOG(INFO) << "selection: " << BitmapToString(sel_bitmap.data(), n_rows);
    LOG(INFO) << "result:    " << BitmapToString(dst_bitmap.data(), expected.size());
    for (int i = 0; i < expected.size(); i++) {
      EXPECT_EQ(expected[i], BitmapTest(dst_bitmap.data(), i));
    }
  }
}

TEST_F(ColumnarSerializationTest, TestCopySelectedRows) {
  auto num_rows = rng_.Uniform(1000) + 1;
  vector<uint32_t> vals;
  for (int i = 0; i < num_rows; i++) {
    vals.push_back(rng_.Next());
  }

  vector<uint32_t> expected;
  vector<uint16_t> sel_indexes;
  for (int i = 0; i < num_rows; i++) {
    if (rng_.OneIn(3)) {
      sel_indexes.push_back(i);
      expected.push_back(vals[i]);
    }
  }

  vector<uint32_t> ret(expected.size());
  internal::CopySelectedRows(sel_indexes, kTypeSize,
                             reinterpret_cast<const uint8_t*>(vals.data()),
                             reinterpret_cast<uint8_t*>(ret.data()));
  ASSERT_EQ(expected, ret);
}

} // namespace kudu
