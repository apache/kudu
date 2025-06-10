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
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <string>
#include <tuple>  // IWYU pragma: keep
#include <utility>
#include <variant>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_util.h"

using std::get;
using std::make_tuple;
using std::nextafter;
using std::string;
using std::tuple;
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

} // namespace kudu
