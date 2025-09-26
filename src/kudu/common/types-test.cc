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

#include "kudu/common/types.h"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>  // IWYU pragma: keep
#include <utility>
#include <variant>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/common/array_type_serdes.h"
#include "kudu/common/common.pb.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::get;
using std::make_tuple;
using std::nextafter;
using std::string;
using std::string_view;
using std::tuple;
using std::unique_ptr;
using std::vector;

namespace kudu {

class TestTypes : public KuduTest {
 protected:
  static void TestDateToString(const string& expected, int32_t date) {
    const TypeInfo* info = GetTypeInfo(DATE);
    string result;
    info->AppendDebugStringForValue(&date, &result);
    ASSERT_EQ(expected, result);
  }
};

TEST_F(TestTypes, TestDatePrinting) {
#if defined(__APPLE__)
  // On MacOS the `%F` date format pads the year with zeros.
  TestDateToString("0001-01-01", *DataTypeTraits<DATE>::min_value());
#else
  TestDateToString("1-01-01", *DataTypeTraits<DATE>::min_value());
#endif
  TestDateToString("9999-12-31", *DataTypeTraits<DATE>::max_value());
  TestDateToString("1970-01-01", 0);
  TestDateToString("1942-08-16", -10000);
  TestDateToString("1997-05-19", 10000);
  TestDateToString("value -2147483648 out of range for DATE type",
                   std::numeric_limits<int32_t>::min());
  TestDateToString("value 2147483647 out of range for DATE type",
                   std::numeric_limits<int32_t>::max());
  TestDateToString("value -719163 out of range for DATE type",
                   *DataTypeTraits<DATE>::min_value() - 1);
  TestDateToString("value 2932897 out of range for DATE type",
                   *DataTypeTraits<DATE>::max_value() + 1);
}

TEST_F(TestTypes, TestTimestampPrinting) {
  const TypeInfo* info = GetTypeInfo(UNIXTIME_MICROS);

  // Test the minimum value
  int64_t time;
  info->CopyMinValue(&time);
  string result;
  info->AppendDebugStringForValue(&time, &result);
  ASSERT_EQ("-290308-12-21T19:59:05.224192Z", result);
  result = "";

  // Test a regular negative timestamp.
  time = -1454368523123456;
  info->AppendDebugStringForValue(&time, &result);
  ASSERT_EQ("1923-12-01T00:44:36.876544Z", result);
  result = "";

  // Test that passing 0 microseconds returns the correct time (0 msecs after the epoch).
  // This is a test for a bug where printing a timestamp with the value 0 would return the
  // current time instead.
  time = 0;
  info->AppendDebugStringForValue(&time, &result);
  ASSERT_EQ("1970-01-01T00:00:00.000000Z", result);
  result = "";

  // Test a regular positive timestamp.
  time = 1454368523123456;
  info->AppendDebugStringForValue(&time, &result);
  ASSERT_EQ("2016-02-01T23:15:23.123456Z", result);
  result = "";

  // Test the maximum value.
  time = MathLimits<int64_t>::kMax;
  info->AppendDebugStringForValue(&time, &result);
  ASSERT_EQ("294247-01-10T04:00:54.775807Z", result);
  result = "";

  {
    // Check that row values are redacted when --redact is set with 'log'.
    google::FlagSaver flag_saver;
    ASSERT_NE("", gflags::SetCommandLineOption("redact", "log"));
    time = 0;
    info->AppendDebugStringForValue(&time, &result);
    ASSERT_EQ("<redacted>", result);
    result = "";
  }
}

namespace {
  template <typename T>
  void TestAreConsecutive(DataType type, vector<tuple<T, T, bool>> test_cases) {
    const TypeInfo* info = GetTypeInfo(type);
    for (auto test_case : test_cases) {
      string lower, upper;
      info->AppendDebugStringForValue(&get<0>(test_case), &lower);
      info->AppendDebugStringForValue(&get<1>(test_case), &upper);
      SCOPED_TRACE(strings::Substitute("lower: $0, upper: $1, expected: $2",
                                       lower, upper, get<2>(test_case)));

      ASSERT_EQ(get<2>(test_case), info->AreConsecutive(&get<0>(test_case), &get<1>(test_case)));
    }
  }
} // anonymous namespace

TEST_F(TestTypes, TestAreConsecutiveInteger) {
  vector<tuple<int64_t, int64_t, bool>> test_cases {
    make_tuple(0, 0, false),
    make_tuple(0, 1, true),
    make_tuple(-1, 0, true),
    make_tuple(INT64_MAX, 0, false),
    make_tuple(0, INT64_MAX, false),
    make_tuple(INT64_MAX - 1, INT64_MAX, true),
    make_tuple(INT64_MIN, 0, false),
    make_tuple(INT64_MIN, INT64_MIN + 1, true),
    make_tuple(INT64_MIN, 0, false),
    make_tuple(-32, -31, true),
    make_tuple(42, 43, true),
    make_tuple(99, -98, false),
  };
  TestAreConsecutive(INT64, test_cases);
}

TEST_F(TestTypes, TestAreConsecutiveDouble) {
  vector<tuple<double, double, bool>> test_cases {
    make_tuple(0.0, 1.0, false),
    make_tuple(0.0, 0.1, false),
    make_tuple(0, nextafter(0, INFINITY), true),
    make_tuple(123.45, nextafter(123.45, INFINITY), true),
    make_tuple(-123.45, nextafter(-123.45, INFINITY), true),
    make_tuple(123.45, 88.0, false),
  };
  TestAreConsecutive(DOUBLE, test_cases);
}

TEST_F(TestTypes, TestAreConsecutiveString) {
  vector<tuple<Slice, Slice, bool>> test_cases {
    make_tuple("abc", "abc", false),
    make_tuple("abc", Slice("abc\0", 4), true),
    make_tuple("", Slice("\0", 1), true),
    make_tuple("", Slice("\1", 1), false),
    make_tuple("", "", false),
    make_tuple("", "a", false),
    make_tuple("abacadaba", Slice("abacadaba\0", 10), true),
  };
  TestAreConsecutive(STRING, test_cases);
}

TEST_F(TestTypes, ArrayTypeInfo) {
  for (auto scalar_type : {DataType::UINT8,
                           DataType::INT8,
                           DataType::UINT16,
                           DataType::INT16,
                           DataType::UINT32,
                           DataType::INT32,
                           DataType::UINT64,
                           DataType::INT64,
                           DataType::STRING,
                           DataType::BOOL,
                           DataType::FLOAT,
                           DataType::DOUBLE,
                           DataType::BINARY,
                           DataType::UNIXTIME_MICROS,
                           DataType::INT128,
                           DataType::DECIMAL32,
                           DataType::DECIMAL64,
                           DataType::DECIMAL128,
                           DataType::VARCHAR,
                           DataType::DATE}
       ) {
    SCOPED_TRACE(DataType_Name(scalar_type));
    const TypeInfo* info = GetArrayTypeInfo(scalar_type);
    ASSERT_TRUE(info->is_array());
    ASSERT_EQ(DataType::NESTED, info->type());
    ASSERT_EQ(DataType::BINARY, info->physical_type());

    // Query the nested type descriptor to fetch information
    // on the array elements' type.
    const auto* descriptor = info->nested_type_info();
    ASSERT_TRUE(descriptor->is_array());
    const auto& array = descriptor->array();
    const TypeInfo* elem_info = array.elem_type_info();
    const TypeInfo* type_info = GetTypeInfo(scalar_type);
    // An implementation detail: due to the way how the type registry is built,
    // 'elem_info' and 'type_info' should be the same pointer.
    ASSERT_EQ(type_info, elem_info);
    // Extra sanity checks.
    ASSERT_FALSE(elem_info->is_array());
    ASSERT_EQ(scalar_type, elem_info->type());
  }
}

// There aren't registry entries for UNKNOWN_DATA, IS_DELETED, and NESTED
// enumerators. The latter is because only 1D arrays are supported at the time
// of writing.
TEST_F(TestTypes, ArrayTypeNoRegistryEntries) {
  for (auto t : {DataType::NESTED, DataType::IS_DELETED, DataType::UNKNOWN_DATA}) {
    SCOPED_TRACE(DataType_Name(t));
    ASSERT_DEATH({
      const TypeInfo* info = GetArrayTypeInfo(t);
      // The ASSERT_EQ() below isn't reachable since GetArrayTypeInfo() hits
      // CHECK_NOTNULL() on absent entries in the type registry.
      ASSERT_EQ(nullptr, info);
    }, "'type_info' Must be non NULL");
  }
}

// Verifying how ArrayTypeTraits<T>::AppendDebugStringForValue(...) works
// for a few data types.
TEST_F(TestTypes, ArrayTypeDebugString) {
  {
    const Slice cell;
    string out;
    ArrayTypeTraits<INT8>::AppendDebugStringForValue(&cell, &out);
    ASSERT_EQ("NULL", out);
  }
  {
    const Slice cell(static_cast<const uint8_t*>(nullptr), 0);
    string out;
    ArrayTypeTraits<INT16>::AppendDebugStringForValue(&cell, &out);
    ASSERT_EQ("NULL", out);
  }
  {
    const vector<int64_t> val;
    const vector<bool> validity;
    ASSERT_EQ(val.size(), validity.size());

    unique_ptr<uint8_t[]> buf_data;
    size_t buf_data_size = 0;
    ASSERT_OK(Serialize(GetTypeInfo(INT64),
                        reinterpret_cast<const uint8_t*>(val.data()),
                        val.size(),
                        validity,
                        &buf_data,
                        &buf_data_size));
    ASSERT_TRUE(buf_data);

    const Slice cell(buf_data.get(), buf_data_size);
    string out;
    ArrayTypeTraits<INT64>::AppendDebugStringForValue(&cell, &out);
    ASSERT_EQ("[]", out);
  }
  {
    const vector<int32_t> val{ 0, 1, 12, 5, 26, 42, };
    const vector<bool> validity{ false, true, false, true, true, true, };
    ASSERT_EQ(val.size(), validity.size());

    unique_ptr<uint8_t[]> buf_data;
    size_t buf_data_size = 0;
    ASSERT_OK(Serialize(GetTypeInfo(INT32),
                        reinterpret_cast<const uint8_t*>(val.data()),
                        val.size(),
                        validity,
                        &buf_data,
                        &buf_data_size));
    ASSERT_TRUE(buf_data);

    const Slice cell(buf_data.get(), buf_data_size);
    string out;
    ArrayTypeTraits<INT32>::AppendDebugStringForValue(&cell, &out);
    ASSERT_EQ("[NULL, 1, NULL, 5, 26, 42]", out);
  }
  {
    const vector<Slice> val{ "alphabet", "", "ABC", "mega", "", "turbo", };
    const vector<bool> validity{ true, false, true, true, true, true, };
    ASSERT_EQ(val.size(), validity.size());

    unique_ptr<uint8_t[]> buf_data;
    size_t buf_data_size = 0;
    ASSERT_OK(Serialize(GetTypeInfo(STRING),
                        reinterpret_cast<const uint8_t*>(val.data()),
                        val.size(),
                        validity,
                        &buf_data,
                        &buf_data_size));
    ASSERT_TRUE(buf_data);

    const Slice cell(buf_data.get(), buf_data_size);
    string out;
    ArrayTypeTraits<STRING>::AppendDebugStringForValue(&cell, &out);
    ASSERT_EQ("[\"alphabet\", NULL, \"ABC\", \"mega\", \"\", \"turbo\"]", out);
  }
}

template <typename T, DataType KUDU_TYPE>
static void CompareArrays(
    int expected,
    string_view tag,
    const vector<T>& lhs_val,
    const vector<bool>& lhs_validity,
    const vector<T>& rhs_val,
    const vector<bool>& rhs_validity) {

  SCOPED_TRACE(tag);
  ASSERT_EQ(lhs_val.size(), lhs_validity.size());
  unique_ptr<uint8_t[]> lhs_buf_data;
  size_t lhs_buf_data_size = 0;
  ASSERT_OK(Serialize(GetTypeInfo(KUDU_TYPE),
                      reinterpret_cast<const uint8_t*>(lhs_val.data()),
                      lhs_val.size(),
                      lhs_validity,
                      &lhs_buf_data,
                      &lhs_buf_data_size));
  ASSERT_TRUE(lhs_buf_data);
  const Slice lhs_cell(lhs_buf_data.get(), lhs_buf_data_size);

  ASSERT_EQ(rhs_val.size(), rhs_validity.size());
  unique_ptr<uint8_t[]> rhs_buf_data;
  size_t rhs_buf_data_size = 0;
  ASSERT_OK(Serialize(GetTypeInfo(KUDU_TYPE),
                      reinterpret_cast<const uint8_t*>(rhs_val.data()),
                      rhs_val.size(),
                      rhs_validity,
                      &rhs_buf_data,
                      &rhs_buf_data_size));
  ASSERT_TRUE(rhs_buf_data);
  const Slice rhs_cell(rhs_buf_data.get(), rhs_buf_data_size);

  ASSERT_EQ(expected, ArrayTypeTraits<KUDU_TYPE>::Compare(&lhs_cell, &rhs_cell));
}

TEST_F(TestTypes, CompareNullArrays) {
  Slice lhs_cell;;
  Slice rhs_cell;;
  ASSERT_EQ(-1, ArrayTypeTraits<INT32>::Compare(&lhs_cell, &rhs_cell));
  ASSERT_EQ(-1, ArrayTypeTraits<INT32>::Compare(&rhs_cell, &lhs_cell));

  ASSERT_EQ(-1, ArrayTypeTraits<STRING>::Compare(&lhs_cell, &rhs_cell));
  ASSERT_EQ(-1, ArrayTypeTraits<STRING>::Compare(&rhs_cell, &lhs_cell));
}

TEST_F(TestTypes, CompareArrays) {
  NO_FATALS((CompareArrays<Slice, BINARY>(0,
      "[] = []",
      {}, {},
      {}, {})));
  NO_FATALS((CompareArrays<Slice, STRING>(-1,
      "[] < [NULL]",
      {}, {},
      { "" }, { false })));
  NO_FATALS((CompareArrays<Slice, BINARY>(1,
      "[NULL] > []",
      { "" }, { false },
      {}, {})));
  NO_FATALS((CompareArrays<Slice, STRING>(-1,
      "[NULL, '1'] < ['', '1']",
      { "0", "1", }, { false, true, },
      { "", "1", }, { true, true, })));
  NO_FATALS((CompareArrays<int16_t, INT16>(-1,
      "[] < [NULL]",
      {}, {},
      { 0 }, { false })));
  NO_FATALS((CompareArrays<int32_t, DATE>(1,
      "[NULL] > []",
      { 0 }, { false },
      {}, {})));
  NO_FATALS((CompareArrays<int16_t, INT16>(-1,
      "[] < [-5]",
      {}, {},
      { -5 }, { true })));
  NO_FATALS((CompareArrays<int64_t, UNIXTIME_MICROS>(-1,
      "[] < [3]",
      {}, {},
      { 3 }, { true })));
  NO_FATALS((CompareArrays<int8_t, INT8>(0,
      "[NULL] = [NULL]",
      { 0 }, { false },
      { 0 }, { false })));
  NO_FATALS((CompareArrays<int8_t, INT8>(1,
      "[1] > [NULL]",
      { 1 }, { true },
      { 2 }, { false })));
  NO_FATALS((CompareArrays<int32_t, INT32>(-1,
      "[-1] < [1]",
      { -1 }, { true },
      { 1 }, { true })));
  NO_FATALS((CompareArrays<int64_t, INT64>(-1,
      "[NULL] < [-1]",
      { 1 }, { false },
      { -1 }, { true })));
  NO_FATALS((CompareArrays<Slice, STRING>(0,
      "[NULL, '1', NULL, '42'] = [NULL, '1', NULL, '42']",
      { "0", "1", "12", "42", }, { false, true, false, true, },
      { "", "1", "1", "42", }, { false, true, false, true, })));
  NO_FATALS((CompareArrays<int16_t, INT16>(0,
      "[NULL, 1, NULL, 5, 26, 42] = [NULL, 1, NULL, 5, 26, 42]",
      { 0, 1, 12, 5, 26, 42, }, { false, true, false, true, true, true, },
      { 1, 1, 1, 5, 26, 42, }, { false, true, false, true, true, true, })));
  NO_FATALS((CompareArrays<int32_t, INT32>(-1,
      "[NULL, 1, NULL, 5, 26, 42] < [NULL, 1, NULL, 5, 26, 43]",
      { 0, 1, 0, 5, 26, 42, }, { false, true, false, true, true, true, },
      { 0, 1, 0, 5, 26, 43, }, { false, true, false, true, true, true, })));
  NO_FATALS((CompareArrays<int64_t, INT64>(-1,
      "[NULL, 1, NULL, 5, 25, 42] < [NULL, 1, NULL, 5, 26, 42]",
      { 0, 1, 0, 5, 25, 42, }, { false, true, false, true, true, true, },
      { 0, 1, 0, 5, 26, 42, }, { false, true, false, true, true, true, })));
  NO_FATALS((CompareArrays<Slice, STRING>(-1,
      "[NULL, '1', NULL] < [NULL, '1', '']",
      { "0", "1", "12", }, { false, true, false, },
      { "", "1", "", }, { false, true, true, })));
  NO_FATALS((CompareArrays<Slice, VARCHAR>(-1,
      "[NULL, 1] < [NULL, 1, NULL]",
      { "0", "1", }, { false, true, },
      { "", "1", "", }, { false, true, false, })));
  NO_FATALS((CompareArrays<int32_t, INT32>(1,
      "[NULL, 1, NULL] > [NULL, 0, 2]",
      { 0, 1, 2 }, { false, true, false },
      { 0, 0, 2, }, { false, true, true, })));
  NO_FATALS((CompareArrays<int32_t, INT32>(-1,
      "[-1, 1, 0] < [1, -2, 1]",
      { -1, 1, 0, }, { true, true, true, },
      { 1, -2, 1, }, { true, true, true, })));
  NO_FATALS((CompareArrays<int16_t, INT16>(-1,
      "[0, 1, 2, NULL] < [0, 1, 3]",
      { 0, 1, 2, 0, }, { true, true, true, false, },
      { 0, 1, 3, }, { true, true, true, })));
  NO_FATALS((CompareArrays<int64_t, INT64>(-1,
      "[0, 1, 2, NULL] < [0, 1, 3, NULL]",
      { 0, 1, 2, 0, }, { true, true, true, false, },
      { 0, 1, 3, 0, }, { true, true, true, false, })));
}

template <typename T, DataType KUDU_TYPE>
static void AreArraysConsecutive(
    bool expected,
    string_view tag,
    const vector<T>& lhs_val,
    const vector<bool>& lhs_validity,
    const vector<T>& rhs_val,
    const vector<bool>& rhs_validity) {

  SCOPED_TRACE(tag);
  ASSERT_EQ(lhs_val.size(), lhs_validity.size());
  unique_ptr<uint8_t[]> lhs_buf_data;
  size_t lhs_buf_data_size = 0;
  ASSERT_OK(Serialize(GetTypeInfo(KUDU_TYPE),
                      reinterpret_cast<const uint8_t*>(lhs_val.data()),
                      lhs_val.size(),
                      lhs_validity,
                      &lhs_buf_data,
                      &lhs_buf_data_size));
  ASSERT_TRUE(lhs_buf_data);
  const Slice lhs_cell(lhs_buf_data.get(), lhs_buf_data_size);

  ASSERT_EQ(rhs_val.size(), rhs_validity.size());
  unique_ptr<uint8_t[]> rhs_buf_data;
  size_t rhs_buf_data_size = 0;
  ASSERT_OK(Serialize(GetTypeInfo(KUDU_TYPE),
                      reinterpret_cast<const uint8_t*>(rhs_val.data()),
                      rhs_val.size(),
                      rhs_validity,
                      &rhs_buf_data,
                      &rhs_buf_data_size));
  ASSERT_TRUE(rhs_buf_data);
  const Slice rhs_cell(rhs_buf_data.get(), rhs_buf_data_size);

  ASSERT_EQ(expected,
            ArrayTypeTraits<KUDU_TYPE>::AreConsecutive(&lhs_cell, &rhs_cell));
}

TEST_F(TestTypes, ConsecutiveArrays) {
  NO_FATALS((AreArraysConsecutive<int8_t, INT8>(false,
      "[-1, 1] and [-1, 1]",
      { -1, 1, }, { true, true, },
      { -1, 1, }, { true, true, })));
  NO_FATALS((AreArraysConsecutive<int16_t, INT16>(false,
      "[-1, 1] and [-1, 1, 0]",
      { -1, 1, }, { true, true, },
      { -1, 1, 0, }, { true, true, true, })));
  NO_FATALS((AreArraysConsecutive<int32_t, INT32>(true,
      "[] and [NULL]",
      {}, {},
      { 0, }, { false, })));
  NO_FATALS((AreArraysConsecutive<int64_t, INT64>(false,
      "[NULL] and []",
      { 0, }, { false, },
      {}, {})));
  NO_FATALS((AreArraysConsecutive<int8_t, INT8>(true,
      "[1] and [1, NULL]",
      { 1, }, { true, },
      { 1, 0, }, { true, false, })));
  NO_FATALS((AreArraysConsecutive<int32_t, INT32>(false,
      "[1, NULL] and [1]",
      { 1, 0, }, { true, false, },
      { 1, }, { true, })));
  NO_FATALS((AreArraysConsecutive<int16_t, INT16>(false,
      "[-1, 1, NULL] and [-1, 1, 0]",
      { -1, 1, 0, }, { true, true, false, },
      { -1, 1, 0, }, { true, true, true, })));
  NO_FATALS((AreArraysConsecutive<int16_t, INT16>(false,
      "[-1, 1, 0] and [-1, 1, NULL]",
      { -1, 1, 0, }, { true, true, true, },
      { -1, 1, 0, }, { true, true, false, })));
}

} // namespace kudu
