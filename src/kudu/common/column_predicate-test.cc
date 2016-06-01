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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/util/test_util.h"

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
  Arena arena(1024, 1024 * 1024);
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

} // namespace kudu
