// Licensed to the Apache Software Foundation (ASF) under values[1]
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

#include "kudu/common/column_predicate.h"

#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_util.h"

using std::vector;

namespace kudu {

class TestColumnPredicate : public KuduTest {
 public:

  // Test that when a is merged into b and vice versa, the result is equal to
  // expected, and the resulting type is equal to type.
  void TestMerge(const ColumnPredicate& a,
                const ColumnPredicate& b,
                const ColumnPredicate& expected,
                PredicateType type) {
    ColumnPredicate a_base(a);
    ColumnPredicate b_base(b);

    SCOPED_TRACE(strings::Substitute("a: $0, b: $1", a.ToString(), b.ToString()));

    a_base.Merge(b);
    b_base.Merge(a);

    ASSERT_EQ(expected, a_base) << "expected: " << expected.ToString()
                                << ", actual: " << a_base.ToString();
    ASSERT_EQ(expected, b_base) << "expected: " << expected.ToString()
                                << ", actual: " << b_base.ToString();
    ASSERT_EQ(a_base, b_base)  << "expected: " << a_base.ToString()
                              << ", actual: " << b_base.ToString();

    ASSERT_EQ(expected.predicate_type(), type);
    ASSERT_EQ(a_base.predicate_type(), type);
    ASSERT_EQ(b_base.predicate_type(), type);
  }

  template <typename T>
  void TestMergeCombinations(const ColumnSchema& column, vector<T> values) {
    // Range + Range

    // [--------) AND
    // [--------)
    // =
    // [--------)
    TestMerge(ColumnPredicate::Range(column, &values[0], &values[4]),
              ColumnPredicate::Range(column, &values[0], &values[4]),
              ColumnPredicate::Range(column, &values[0], &values[4]),
              PredicateType::Range);

    // [--------) AND
    // [----)
    // =
    // [----)
    TestMerge(ColumnPredicate::Range(column, &values[0], &values[4]),
              ColumnPredicate::Range(column, &values[0], &values[2]),
              ColumnPredicate::Range(column, &values[0], &values[2]),
              PredicateType::Range);

    // [--------) AND
    //   [----)
    // =
    //   [----)
    TestMerge(ColumnPredicate::Range(column, &values[0], &values[4]),
              ColumnPredicate::Range(column, &values[1], &values[3]),
              ColumnPredicate::Range(column, &values[1], &values[3]),
              PredicateType::Range);

    // [-----) AND
    //   [------)
    // =
    //   [---)
    TestMerge(ColumnPredicate::Range(column, &values[0], &values[3]),
              ColumnPredicate::Range(column, &values[1], &values[4]),
              ColumnPredicate::Range(column, &values[1], &values[3]),
              PredicateType::Range);

    // [--) AND
    //    [---)
    // =
    // None
    TestMerge(ColumnPredicate::Range(column, &values[0], &values[2]),
              ColumnPredicate::Range(column, &values[2], &values[5]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // [--) AND
    //       [---)
    // =
    // None
    TestMerge(ColumnPredicate::Range(column, &values[0], &values[2]),
              ColumnPredicate::Range(column, &values[4], &values[6]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // [---> AND
    // [--->
    // =
    // [--->
    TestMerge(ColumnPredicate::Range(column, &values[1], nullptr),
              ColumnPredicate::Range(column, &values[1], nullptr),
              ColumnPredicate::Range(column, &values[1], nullptr),
              PredicateType::Range);

    // [-----> AND
    //   [--->
    // =
    //   [--->
    TestMerge(ColumnPredicate::Range(column, &values[1], nullptr),
              ColumnPredicate::Range(column, &values[2], nullptr),
              ColumnPredicate::Range(column, &values[2], nullptr),
              PredicateType::Range);

    // <---) AND
    // <---)
    // =
    // <---)
    TestMerge(ColumnPredicate::Range(column, nullptr, &values[4]),
              ColumnPredicate::Range(column, nullptr, &values[4]),
              ColumnPredicate::Range(column, nullptr, &values[4]),
              PredicateType::Range);

    //   <---) AND
    // <---)
    // =
    // <---)
    TestMerge(ColumnPredicate::Range(column, nullptr, &values[6]),
              ColumnPredicate::Range(column, nullptr, &values[4]),
              ColumnPredicate::Range(column, nullptr, &values[4]),
              PredicateType::Range);

    // <---) AND
    // [--->
    // =
    // [---)
    TestMerge(ColumnPredicate::Range(column, nullptr, &values[4]),
              ColumnPredicate::Range(column, &values[1], nullptr),
              ColumnPredicate::Range(column, &values[1], &values[4]),
              PredicateType::Range);

    // <---)     AND
    //     [--->
    // =
    // None
    TestMerge(ColumnPredicate::Range(column, nullptr, &values[4]),
              ColumnPredicate::Range(column, &values[4], nullptr),
              ColumnPredicate::None(column),
              PredicateType::None);

    // <---)       AND
    //       [--->
    // =
    // None
    TestMerge(ColumnPredicate::Range(column, nullptr, &values[2]),
              ColumnPredicate::Range(column, &values[4], nullptr),
              ColumnPredicate::None(column),
              PredicateType::None);

    // Range + Equality

    //   [---) AND
    // |
    // =
    // None
    TestMerge(ColumnPredicate::Range(column, &values[3], &values[5]),
              ColumnPredicate::Equality(column, &values[1]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // [---) AND
    // |
    // =
    // |
    TestMerge(ColumnPredicate::Range(column, &values[1], &values[5]),
              ColumnPredicate::Equality(column, &values[1]),
              ColumnPredicate::Equality(column, &values[1]),
              PredicateType::Equality);

    // [---) AND
    //   |
    // =
    //   |
    TestMerge(ColumnPredicate::Range(column, &values[1], &values[5]),
              ColumnPredicate::Equality(column, &values[3]),
              ColumnPredicate::Equality(column, &values[3]),
              PredicateType::Equality);

    // [---) AND
    //     |
    // =
    // None
    TestMerge(ColumnPredicate::Range(column, &values[1], &values[5]),
              ColumnPredicate::Equality(column, &values[5]),
              ColumnPredicate::None(column),
              PredicateType::None);


    // [---) AND
    //       |
    // =
    // None
    TestMerge(ColumnPredicate::Range(column, &values[1], &values[4]),
              ColumnPredicate::Equality(column, &values[5]),
              ColumnPredicate::None(column),
              PredicateType::None);

    //   [---> AND
    // |
    // =
    // None
    TestMerge(ColumnPredicate::Range(column, &values[3], nullptr),
              ColumnPredicate::Equality(column, &values[1]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // [---> AND
    // |
    // =
    // |
    TestMerge(ColumnPredicate::Range(column, &values[1], nullptr),
              ColumnPredicate::Equality(column, &values[1]),
              ColumnPredicate::Equality(column, &values[1]),
              PredicateType::Equality);

    // [-----> AND
    //   |
    // =
    //   |
    TestMerge(ColumnPredicate::Range(column, &values[0], nullptr),
              ColumnPredicate::Equality(column, &values[2]),
              ColumnPredicate::Equality(column, &values[2]),
              PredicateType::Equality);

    // <---) AND
    //   |
    // =
    //   |
    TestMerge(ColumnPredicate::Range(column, nullptr, &values[3]),
              ColumnPredicate::Equality(column, &values[1]),
              ColumnPredicate::Equality(column, &values[1]),
              PredicateType::Equality);

    // <---) AND
    //     |
    // =
    // None
    TestMerge(ColumnPredicate::Range(column, nullptr, &values[3]),
              ColumnPredicate::Equality(column, &values[3]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // <---)    AND
    //       |
    // =
    // None
    TestMerge(ColumnPredicate::Range(column, nullptr, &values[1]),
              ColumnPredicate::Equality(column, &values[3]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // InList + InList

    vector<const void*> top_list;
    vector<const void*> bot_list;
    vector<const void*> res_list;

    //   | | |  AND
    //   | | |
    // = | | |
    top_list = { &values[1], &values[3], &values[5] };
    bot_list = { &values[1], &values[3], &values[5] };
    res_list = { &values[1], &values[3], &values[5] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::InList(column, &res_list),
              PredicateType::InList);

    //   | | |  AND
    //   | |
    // = | |
    top_list = { &values[1], &values[3], &values[6] };
    bot_list = { &values[1], &values[3] };
    res_list = { &values[1], &values[3] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::InList(column, &res_list),
              PredicateType::InList);

    //   | | |  AND
    //     | |
    // =   | |
    top_list = { &values[1], &values[3], &values[6] };
    bot_list = { &values[3], &values[6] };
    res_list = { &values[6], &values[3] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::InList(column, &res_list),
              PredicateType::InList);

    //     | | |  AND
    //   | | |
    // =   | |
    top_list = { &values[2], &values[3], &values[4] };
    bot_list = { &values[1], &values[2], &values[3] };
    res_list = { &values[2], &values[3] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::InList(column, &res_list),
              PredicateType::InList);

    //      | | |  AND
    //   | | |
    // = NONE
    top_list = { &values[3], &values[5], &values[6] };
    bot_list = { &values[0], &values[2], &values[4] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::None(column),
              PredicateType::None);

    //       | | |  AND
    //   | | |
    // =     |
    top_list = { &values[1], &values[2], &values[3] };
    bot_list = { &values[3], &values[4], &values[5] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::Equality(column, &values[3]),
              PredicateType::Equality);

    //         | | |  AND
    //   | | |
    // = None
    top_list = { &values[4], &values[5], &values[6] };
    bot_list = { &values[1], &values[2], &values[3] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::None(column),
              PredicateType::None);

    //     | | |  AND
    //    |||||
    // =   | |
    top_list = { &values[1], &values[3], &values[5] };
    bot_list = { &values[0], &values[1], &values[2], &values[3], &values[4] };
    res_list = { &values[1], &values[3] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::InList(column, &res_list),
              PredicateType::InList);

    //    | | |  AND
    //     | |
    // =  none
    top_list = { &values[1], &values[3], &values[5] };
    bot_list = { &values[2], &values[4] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::None(column),
              PredicateType::None);

    //    | | |  AND
    //     |||
    // =    |
    top_list = { &values[1], &values[3], &values[5] };
    bot_list = { &values[2], &values[3], &values[4] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::Equality(column, &values[3]),
              PredicateType::Equality);

    //   | |   AND
    //    | |
    // = none
    top_list = { &values[1], &values[3] };
    bot_list = { &values[2], &values[4] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::None(column),
              PredicateType::None);

    //   | |   AND
    //       | |
    // = none
    top_list = { &values[0], &values[2] };
    bot_list = { &values[3], &values[5] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::None(column),
              PredicateType::None);


    // InList + Equality

    //   | | |  AND
    // |
    // = none
    top_list = { &values[2], &values[3], &values[4] };
    bot_list = { &values[1] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::None(column),
              PredicateType::None);


    //   | | | AND
    //   |
    // = |
    top_list = { &values[1], &values[3], &values[6] };
    bot_list = { &values[1] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::Equality(column, &values[1]),
              PredicateType::Equality);

    //  | | | AND
    //    |
    // =  |
    top_list = { &values[1], &values[3], &values[6] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::Equality(column, &values[3]),
              ColumnPredicate::Equality(column, &values[3]),
              PredicateType::Equality);

    //  | | | AND
    //     |
    // = none
    top_list = { &values[1], &values[3], &values[6] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::Equality(column, &values[4]),
              ColumnPredicate::None(column),
              PredicateType::None);

    //  | | |  AND
    //         |
    // =  none
    top_list = { &values[1], &values[3], &values[5] };
    TestMerge(ColumnPredicate::InList(column, &top_list),
              ColumnPredicate::Equality(column, &values[6]),
              ColumnPredicate::None(column),
              PredicateType::None);


    // InList + Range

    //     [---) AND
    //   | | | | |
    // =   | |
    bot_list = { &values[0], &values[1], &values[2], &values[3], &values[4] };
    res_list = { &values[1], &values[2] };
    TestMerge(ColumnPredicate::Range(column, &values[1], &values[3]),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::InList(column, &res_list),
              PredicateType::InList);

    //    [------) AND
    //  |        | |
    // = None
    bot_list = { &values[1], &values[4], &values[5] };
    TestMerge(ColumnPredicate::Range(column, &values[2], &values[4]),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::None(column),
              PredicateType::None);

    //  [------) AND
    //            | |
    // =
    // None
    bot_list = { &values[5], &values[6] };
    TestMerge(ColumnPredicate::Range(column, &values[1], &values[4]),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::None(column),
              PredicateType::None);

    //       [------) AND
    //   | |
    // =
    // None
    bot_list = { &values[0], &values[1] };
    TestMerge(ColumnPredicate::Range(column, &values[3], &values[6]),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::None(column),
              PredicateType::None);

    //   [------) AND
    //        | |
    // =
    // None
    bot_list = { &values[0], &values[1] };
    TestMerge(ColumnPredicate::Range(column, &values[3], &values[6]),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::None(column),
              PredicateType::None);

    //      [-----------> AND
    //    | |  |
    // =    |  |
    bot_list = { &values[2], &values[3], &values[4] };
    res_list = { &values[3], &values[4] };
    TestMerge(ColumnPredicate::Range(column, &values[3], nullptr),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::InList(column, &res_list),
              PredicateType::InList);

    // <----) AND
    //   |  |  |
    // = |
    bot_list = { &values[2], &values[3], &values[4] };
    res_list = { &values[3], &values[4] };
    TestMerge(ColumnPredicate::Range(column, nullptr, &values[3]),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::Equality(column, &values[2]),
              PredicateType::Equality);

    // None

    // None AND
    // [----)
    // =
    // None
    TestMerge(ColumnPredicate::None(column),
              ColumnPredicate::Range(column, &values[1], &values[5]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // None AND
    // <----)
    // =
    // None
    TestMerge(ColumnPredicate::None(column),
              ColumnPredicate::Range(column, nullptr, &values[5]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // None AND
    // [---->
    // =
    // None
    TestMerge(ColumnPredicate::None(column),
              ColumnPredicate::Range(column, &values[1], nullptr),
              ColumnPredicate::None(column),
              PredicateType::None);

    // None AND
    //  |
    // =
    // None
    TestMerge(ColumnPredicate::None(column),
              ColumnPredicate::Equality(column, &values[1]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // None AND
    // None
    // =
    // None
    TestMerge(ColumnPredicate::None(column),
              ColumnPredicate::None(column),
              ColumnPredicate::None(column),
              PredicateType::None);

    // None AND
    // | | |
    // =
    // None
    bot_list = { &values[2], &values[3], &values[4] };
    TestMerge(ColumnPredicate::None(column),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::None(column),
              PredicateType::None);

    // IS NOT NULL

    // IS NOT NULL AND
    // IS NOT NULL
    // =
    // IS NOT NULL
    TestMerge(ColumnPredicate::IsNotNull(column),
              ColumnPredicate::IsNotNull(column),
              ColumnPredicate::IsNotNull(column),
              PredicateType::IsNotNull);

    // IS NOT NULL AND
    // None
    // =
    // None
    TestMerge(ColumnPredicate::IsNotNull(column),
              ColumnPredicate::None(column),
              ColumnPredicate::None(column),
              PredicateType::None);

    // IS NOT NULL AND
    // |
    // =
    // |
    TestMerge(ColumnPredicate::IsNotNull(column),
              ColumnPredicate::Equality(column, &values[0]),
              ColumnPredicate::Equality(column, &values[0]),
              PredicateType::Equality);

    // IS NOT NULL AND
    // [------)
    // =
    // [------)
    TestMerge(ColumnPredicate::IsNotNull(column),
              ColumnPredicate::Range(column, &values[0], &values[2]),
              ColumnPredicate::Range(column, &values[0], &values[2]),
              PredicateType::Range);

    // IS NOT NULL AND
    // <------)
    // =
    // <------)
    TestMerge(ColumnPredicate::IsNotNull(column),
              ColumnPredicate::Range(column, nullptr, &values[2]),
              ColumnPredicate::Range(column, nullptr, &values[2]),
              PredicateType::Range);

    // IS NOT NULL AND
    // [------>
    // =
    // [------>
    TestMerge(ColumnPredicate::IsNotNull(column),
              ColumnPredicate::Range(column, &values[2], nullptr),
              ColumnPredicate::Range(column, &values[2], nullptr),
              PredicateType::Range);

    // IS NOT NULL AND
    // | | |
    // =
    // | | |
    bot_list = { &values[2], &values[3], &values[4] };
    res_list = { &values[2], &values[3], &values[4] };
    TestMerge(ColumnPredicate::IsNotNull(column),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::InList(column, &res_list),
              PredicateType::InList);

    // IS NULL

    // IS NULL AND
    // None
    // =
    // None
    TestMerge(ColumnPredicate::IsNull(column),
              ColumnPredicate::None(column),
              ColumnPredicate::None(column),
              PredicateType::None);

    // IS NULL AND
    // |
    // =
    // None
    TestMerge(ColumnPredicate::IsNull(column),
              ColumnPredicate::Equality(column, &values[0]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // IS NULL AND
    // [-------)
    // =
    // None
    TestMerge(ColumnPredicate::IsNull(column),
              ColumnPredicate::Range(column, &values[0], &values[2]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // IS NULL AND
    // [------->
    // =
    // None
    TestMerge(ColumnPredicate::IsNull(column),
              ColumnPredicate::Range(column, &values[0], nullptr),
              ColumnPredicate::None(column),
              PredicateType::None);

    // IS NULL AND
    // <-------)
    // =
    // None
    TestMerge(ColumnPredicate::IsNull(column),
              ColumnPredicate::Range(column, nullptr, &values[2]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // IS NULL AND
    // | | |
    // =
    // None
    bot_list = { &values[1], &values[3], &values[6] };
    TestMerge(ColumnPredicate::IsNull(column),
              ColumnPredicate::InList(column, &bot_list),
              ColumnPredicate::None(column),
              PredicateType::None);

    // IS NULL AND
    // IS NOT NULL
    // =
    // None
    TestMerge(ColumnPredicate::IsNull(column),
              ColumnPredicate::IsNotNull(column),
              ColumnPredicate::None(column),
              PredicateType::None);

    // IS NULL AND
    // IS NULL
    // =
    // IS NULL
    TestMerge(ColumnPredicate::IsNull(column),
              ColumnPredicate::IsNull(column),
              ColumnPredicate::IsNull(column),
              PredicateType::IsNull);
  }
};

TEST_F(TestColumnPredicate, TestMerge) {
  TestMergeCombinations(ColumnSchema("c", INT8, true),
                        vector<int8_t> { 0, 1, 2, 3, 4, 5, 6 });

  TestMergeCombinations(ColumnSchema("c", INT32, true),
                        vector<int32_t> { -100, -10, -1, 0, 1, 10, 100 });

  TestMergeCombinations(ColumnSchema("c", STRING, true),
                        vector<Slice> { "a", "b", "c", "d", "e", "f", "g" });

  TestMergeCombinations(ColumnSchema("c", BINARY, true),
                        vector<Slice> { Slice("", 0),
                                        Slice("\0", 1),
                                        Slice("\0\0", 2),
                                        Slice("\0\0\0", 3),
                                        Slice("\0\0\0\0", 4),
                                        Slice("\0\0\0\0\0", 5),
                                        Slice("\0\0\0\0\0\0", 6),
                                      });
}

// Test that the range constructor handles equality and empty ranges.
TEST_F(TestColumnPredicate, TestRangeConstructor) {
  {
    ColumnSchema column("c", INT32);
    int32_t zero = 0;
    int32_t one = 1;
    int32_t two = 2;

    ASSERT_EQ(PredicateType::Range,
              ColumnPredicate::Range(column, &zero, &two).predicate_type());
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::Range(column, &zero, &one).predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(column, &zero, &zero).predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(column, &one, &zero).predicate_type());
  }
  {
    ColumnSchema column("c", STRING);
    Slice zero("", 0);
    Slice one("\0", 1);
    Slice two("\0\0", 2);

    ASSERT_EQ(PredicateType::Range,
              ColumnPredicate::Range(column, &zero, &two).predicate_type());
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::Range(column, &zero, &one).predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(column, &zero, &zero).predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(column, &one, &zero).predicate_type());
  }
}

// Test that the inclusive range constructor handles transforming to exclusive
// upper bound correctly.
TEST_F(TestColumnPredicate, TestInclusiveRange) {
  Arena arena(1024);
  {
    ColumnSchema column("c", INT32);
    int32_t zero = 0;
    int32_t two = 2;
    int32_t three = 3;
    int32_t max = INT32_MAX;

    ASSERT_EQ(ColumnPredicate::Range(column, &zero, &three),
              *ColumnPredicate::InclusiveRange(column, &zero, &two, &arena));
    ASSERT_EQ(ColumnPredicate::Range(column, &zero, nullptr),
              *ColumnPredicate::InclusiveRange(column, &zero, &max, &arena));

    ASSERT_FALSE(ColumnPredicate::InclusiveRange(column, nullptr, &max, &arena));
  }
  {
    ColumnSchema column("c", INT32, true);
    int32_t zero = 0;
    int32_t two = 2;
    int32_t three = 3;
    int32_t max = INT32_MAX;

    ASSERT_EQ(ColumnPredicate::Range(column, &zero, &three),
              *ColumnPredicate::InclusiveRange(column, &zero, &two, &arena));
    ASSERT_EQ(ColumnPredicate::Range(column, &zero, nullptr),
              *ColumnPredicate::InclusiveRange(column, &zero, &max, &arena));

    ASSERT_EQ(ColumnPredicate::IsNotNull(column),
              *ColumnPredicate::InclusiveRange(column, nullptr, &max, &arena));
  }
  {
    ColumnSchema column("c", STRING);
    Slice zero("", 0);
    Slice two("\0\0", 2);
    Slice three("\0\0\0", 3);

    ASSERT_EQ(ColumnPredicate::Range(column, &zero, &three),
              *ColumnPredicate::InclusiveRange(column, &zero, &two, &arena));
  }
}

// Test that the exclusive range constructor handles transforming to inclusive
// lower bound correctly.
TEST_F(TestColumnPredicate, TestExclusive) {
  Arena arena(1024);
  {
    ColumnSchema column("c", INT32);
    int32_t zero = 0;
    int32_t one = 1;
    int32_t three = 3;
    int32_t max = INT32_MAX;

    ASSERT_EQ(ColumnPredicate::Range(column, &one, &three),
              ColumnPredicate::ExclusiveRange(column, &zero, &three, &arena));

    ASSERT_EQ(ColumnPredicate::Range(column, &one, &max),
              ColumnPredicate::ExclusiveRange(column, &zero, &max, &arena));

    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::ExclusiveRange(column, &max, nullptr, &arena).predicate_type());

    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::ExclusiveRange(column, &zero, &one, &arena).predicate_type());

    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::ExclusiveRange(column, &zero, &zero, &arena).predicate_type());
  }
  {
    ColumnSchema column("c", STRING);
    Slice zero("", 0);
    Slice one("\0", 1);
    Slice two("\0\0", 2);

    ASSERT_EQ(ColumnPredicate::Range(column, &one, &two),
              ColumnPredicate::ExclusiveRange(column, &zero, &two, &arena));
  }
}

TEST_F(TestColumnPredicate, TestLess) {
    ColumnSchema i8("i8", INT8);
    ColumnSchema i16("i16", INT16);
    ColumnSchema i32("i32", INT32);
    ColumnSchema i64("i64", INT64);
    ColumnSchema micros("micros", UNIXTIME_MICROS);
    ColumnSchema f32("f32", FLOAT);
    ColumnSchema f64("f64", DOUBLE);
    ColumnSchema string("string", STRING);
    ColumnSchema binary("binary", BINARY);

    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(i8, nullptr, TypeTraits<INT8>::min_value())
                              .predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(i16, nullptr, TypeTraits<INT16>::min_value())
                              .predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(i32, nullptr, TypeTraits<INT32>::min_value())
                              .predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(i64, nullptr, TypeTraits<INT64>::min_value())
                              .predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(micros, nullptr, TypeTraits<UNIXTIME_MICROS>::min_value())
                              .predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(f32, nullptr, TypeTraits<FLOAT>::min_value())
                              .predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(f64, nullptr, TypeTraits<DOUBLE>::min_value())
                              .predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(string, nullptr, TypeTraits<STRING>::min_value())
                              .predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(binary, nullptr, TypeTraits<BINARY>::min_value())
                              .predicate_type());
}

TEST_F(TestColumnPredicate, TestGreaterThanEquals) {
    ColumnSchema i8("i8", INT8);
    ColumnSchema i16("i16", INT16);
    ColumnSchema i32("i32", INT32);
    ColumnSchema i64("i64", INT64);
    ColumnSchema micros("micros", UNIXTIME_MICROS);
    ColumnSchema f32("f32", FLOAT);
    ColumnSchema f64("f64", DOUBLE);
    ColumnSchema string("string", STRING);
    ColumnSchema binary("binary", BINARY);

    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(i8, TypeTraits<INT8>::min_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(i16, TypeTraits<INT16>::min_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(i32, TypeTraits<INT32>::min_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(i64, TypeTraits<INT64>::min_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(micros, TypeTraits<UNIXTIME_MICROS>::min_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(f32, TypeTraits<FLOAT>::min_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(f64, TypeTraits<DOUBLE>::min_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(string, TypeTraits<STRING>::min_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(binary, TypeTraits<BINARY>::min_value(), nullptr)
                              .predicate_type());

    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::Range(i8, TypeTraits<INT8>::max_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::Range(i16, TypeTraits<INT16>::max_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::Range(i32, TypeTraits<INT32>::max_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::Range(i64, TypeTraits<INT64>::max_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::Range(micros, TypeTraits<UNIXTIME_MICROS>::max_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::Range(f32, TypeTraits<FLOAT>::max_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::Range(f64, TypeTraits<DOUBLE>::max_value(), nullptr)
                              .predicate_type());

    Slice s = "foo";
    ASSERT_EQ(PredicateType::Range,
              ColumnPredicate::Range(string, &s, nullptr).predicate_type());
    ASSERT_EQ(PredicateType::Range,
              ColumnPredicate::Range(binary, &s, nullptr).predicate_type());
}

// Test the InList constructor.
TEST_F(TestColumnPredicate, TestInList) {
    vector<const void*> values;
  {
    ColumnSchema column("c", INT32);
    int five = 5;
    int six = 6;
    int ten = 10;

    values = {};
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::InList(column, &values).predicate_type());

    values = { &five };
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::InList(column, &values).predicate_type());

    values = { &five, &six, &ten };
    ASSERT_EQ(PredicateType::InList,
              ColumnPredicate::InList(column, &values).predicate_type());
  }
  {
    Slice a("a");
    Slice b("b");
    Slice c("c");
    ColumnSchema column("c", STRING);

    values = {};
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::InList(column, &values).predicate_type());

    values = { &a };
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::InList(column, &values).predicate_type());

    values = { &a, &b, &c };
    ASSERT_EQ(PredicateType::InList,
              ColumnPredicate::InList(column, &values).predicate_type());
  }
  {
    bool t = true;
    bool f = false;
    ColumnSchema column("c", BOOL);

    values = {};
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::InList(column, &values).predicate_type());

    values = { &t };
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::InList(column, &values).predicate_type());

    values = { &t, &f, &t, &f };
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::InList(column, &values).predicate_type());
  }
}

// Test that column predicate comparison works correctly: ordered by predicate
// type first, then size of the column type.
TEST_F(TestColumnPredicate, TestSelectivity) {
  int32_t one_32 = 1;
  int64_t one_64 = 1;
  double_t one_d = 1.0;
  Slice one_s("one", 3);

  ColumnSchema column_i32("a", INT32, true);
  ColumnSchema column_i64("b", INT64, true);
  ColumnSchema column_d("c", DOUBLE, true);
  ColumnSchema column_s("d", STRING, true);

  // Predicate type
  ASSERT_LT(SelectivityComparator(ColumnPredicate::IsNull(column_i32),
                                  ColumnPredicate::Equality(column_i32, &one_32)),
            0);
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Equality(column_i32, &one_32),
                                  ColumnPredicate::Range(column_d, &one_d, nullptr)),
            0);
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Equality(column_i32, &one_32),
                                  ColumnPredicate::IsNotNull(column_s)),
            0);
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Range(column_i32, &one_32, nullptr),
                                  ColumnPredicate::IsNotNull(column_i32)),
            0);

  // Size of column type
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Equality(column_i32, &one_32),
                                  ColumnPredicate::Equality(column_i64, &one_64)),
            0);
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Equality(column_i32, &one_32),
                                  ColumnPredicate::Equality(column_d, &one_d)),
            0);
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Equality(column_i32, &one_32),
                                  ColumnPredicate::Equality(column_s, &one_s)),
            0);
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Equality(column_i64, &one_64),
                                  ColumnPredicate::Equality(column_s, &one_s)),
            0);
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Equality(column_d, &one_d),
                                  ColumnPredicate::Equality(column_s, &one_s)),
            0);
}

TEST_F(TestColumnPredicate, TestRedaction) {
  ASSERT_NE("", gflags::SetCommandLineOption("redact", "log"));
  ColumnSchema column_i32("a", INT32, true);
  int32_t one_32 = 1;
  ASSERT_EQ("a = <redacted>", ColumnPredicate::Equality(column_i32, &one_32).ToString());
}

} // namespace kudu
