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
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/block_bloom_filter.h"
#include "kudu/util/hash.pb.h"
#include "kudu/util/hash_util.h"
#include "kudu/util/int128.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::vector;

namespace kudu {

class TestColumnPredicate : public KuduTest {
 public:
  TestColumnPredicate() : rand_(SeedRandom()) {}

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

  void FillBloomFilterAndValues(int n_keys,
                                vector<uint64_t>* values,
                                BlockBloomFilter* b1,
                                BlockBloomFilter* b2) {
    uint64_t current = 0;
    for (int i = 0; i < 2; ++i) {
      while (true) {
        uint64_t key = rand_.Next();
        if (key <= current) {
          continue;
        }
        current = key;
        Slice key_slice(reinterpret_cast<const uint8_t*>(&key), sizeof(key));
        uint32_t hash_val = HashUtil::ComputeHash32(key_slice, FAST_HASH, 0);
        b1->Insert(hash_val);
        b2->Insert(hash_val);
        values->emplace_back(key);
        break;
      }
    }
    for (int i = 2; i < n_keys; ++i) {
      while (true) {
        uint64_t key = rand_.Next();
        Slice key_slice(reinterpret_cast<const uint8_t*>(&key), sizeof(key));
        uint32_t hash_val = HashUtil::ComputeHash32(key_slice, FAST_HASH, 0);
        if (!b1->Find(hash_val) && key > current) {
          current = key;
          values->emplace_back(key);
          b2->Insert(hash_val);
          break;
        }
      }
    }
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

  template <typename T>
  void TestMergeBloomFilterCombinations(const ColumnSchema& column,
                                        const vector<BlockBloomFilter*>& bf,
                                        vector<T> values) {
    // BloomFilter AND
    // NONE
    // =
    // NONE
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, nullptr, nullptr),
              ColumnPredicate::None(column),
              ColumnPredicate::None(column),
              PredicateType::None);

    // BloomFilter AND
    // Equality
    // =
    // Equality
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, nullptr, nullptr),
              ColumnPredicate::Equality(column, &values[0]),
              ColumnPredicate::Equality(column, &values[0]),
              PredicateType::Equality);

    // BloomFilter AND
    // Equality
    // =
    // None
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, nullptr, nullptr),
              // Value in index 2 is not present in one of the bloom filters.
              ColumnPredicate::Equality(column, &values[2]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // BloomFilter AND
    // IS NOT NULL
    // =
    // BloomFilter
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, nullptr, nullptr),
              ColumnPredicate::IsNotNull(column),
              ColumnPredicate::InBloomFilter(column, bf, nullptr, nullptr),
              PredicateType::InBloomFilter);

    // BloomFilter AND
    // IS NULL
    // =
    // None
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, nullptr, nullptr),
              ColumnPredicate::IsNull(column),
              ColumnPredicate::None(column),
              PredicateType::None);

    // BloomFilter AND
    // InList
    // =
    // None(the value in list can not hit bloom filter)
    vector<const void*> in_list = { &values[2], &values[3], &values[4] };
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, nullptr, nullptr),
              ColumnPredicate::InList(column, &in_list),
              ColumnPredicate::None(column),
              PredicateType::None);

    // BloomFilter AND
    // InList
    // =
    // InList(the value in list all hits bloom filter)
    in_list = { &values[0], &values[1] };
    vector<const void*> hit_list = { &values[0], &values[1] };
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, nullptr, nullptr),
              ColumnPredicate::InList(column, &in_list),
              ColumnPredicate::InList(column, &hit_list),
              PredicateType::InList);

    // BloomFilter AND
    // InList
    // =
    // InList(only the some values in list hits bloom filter)
    in_list = { &values[0], &values[1], &values[2], &values[3] };
    hit_list = { &values[0], &values[1]};
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, nullptr, nullptr),
              ColumnPredicate::InList(column, &in_list),
              ColumnPredicate::InList(column, &hit_list),
              PredicateType::InList);

    // BloomFilter AND
    // InList
    // =
    // Equality(only the first value in list hits bloom filter, so it simplify to Equality)
    in_list = { &values[0], &values[2], &values[3] };
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, nullptr, nullptr),
              ColumnPredicate::InList(column, &in_list),
              ColumnPredicate::Equality(column, &values[0]),
              PredicateType::Equality);

    // Range AND
    // BloomFilter
    // =
    // BloomFilter with lower and upper bound
    TestMerge(ColumnPredicate::Range(column, &values[0], &values[4]),
              ColumnPredicate::InBloomFilter(column, bf, nullptr, nullptr),
              ColumnPredicate::InBloomFilter(column, bf, &values[0],
                                             &values[4]),
              PredicateType::InBloomFilter);

    // BloomFilter with lower and upper bound AND
    // Range
    // =
    // BloomFilter with lower and upper bound
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, &values[0], &values[4]),
              ColumnPredicate::Range(column, &values[1], &values[3]),
              ColumnPredicate::InBloomFilter(column, bf, &values[1],
                                             &values[3]),
              PredicateType::InBloomFilter);

    // BloomFilter with lower and upper bound AND
    // Range
    // =
    // None
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, &values[0], &values[2]),
              ColumnPredicate::Range(column, &values[2], &values[4]),
              ColumnPredicate::None(column),
              PredicateType::None);

    // BloomFilter AND
    // BloomFilter with lower and upper bound
    // =
    // BloomFilter with lower and upper bound
    auto collect = bf;
    collect.insert(collect.end(), bf.begin(), bf.end());
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, nullptr, nullptr),
              ColumnPredicate::InBloomFilter(column, bf, &values[0],
                                             &values[4]),
              ColumnPredicate::InBloomFilter(column, collect, &values[0],
                                             &values[4]),
              PredicateType::InBloomFilter);

    // BloomFilter with lower and upper bound AND
    // BloomFilter with lower and upper bound
    // =
    // BloomFilter with lower and upper bound
    collect = bf;
    collect.insert(collect.end(), bf.begin(), bf.end());
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, &values[1], &values[3]),
              ColumnPredicate::InBloomFilter(column, bf, &values[0],
                                             &values[4]),
              ColumnPredicate::InBloomFilter(column, collect, &values[1],
                                             &values[3]),
              PredicateType::InBloomFilter);

    // BloomFilter with lower and upper bound AND
    // BloomFilter with lower and upper bound
    // =
    // None
    TestMerge(ColumnPredicate::InBloomFilter(column, bf, &values[0], &values[2]),
              ColumnPredicate::InBloomFilter(column, bf, &values[2],
                                             &values[4]),
              ColumnPredicate::None(column),
              PredicateType::None);
  }

 protected:
  Random rand_;
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
    ColumnSchema d32("d32", DECIMAL32);
    ColumnSchema d64("d64", DECIMAL64);
    ColumnSchema d128("d128", DECIMAL128);
    ColumnSchema string("string", STRING);
    ColumnSchema binary("binary", BINARY);
    ColumnSchema varchar("varchar", VARCHAR);

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
              ColumnPredicate::Range(d32, nullptr, TypeTraits<DECIMAL32>::min_value())
                              .predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(d64, nullptr, TypeTraits<DECIMAL64>::min_value())
                              .predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(d128, nullptr, TypeTraits<DECIMAL128>::min_value())
                               .predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(string, nullptr, TypeTraits<STRING>::min_value())
                              .predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(binary, nullptr, TypeTraits<BINARY>::min_value())
                              .predicate_type());
    ASSERT_EQ(PredicateType::None,
              ColumnPredicate::Range(varchar, nullptr, TypeTraits<VARCHAR>::min_value())
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
    ColumnSchema d32("d32", DECIMAL32);
    ColumnSchema d64("d64", DECIMAL64);
    ColumnSchema d128("d128", DECIMAL128);
    ColumnSchema string("string", STRING);
    ColumnSchema binary("binary", BINARY);
    ColumnSchema varchar("varchar", VARCHAR);

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
              ColumnPredicate::Range(d32, TypeTraits<DECIMAL32>::min_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(d64, TypeTraits<DECIMAL64>::min_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(d128, TypeTraits<DECIMAL128>::min_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(string, TypeTraits<STRING>::min_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(binary, TypeTraits<BINARY>::min_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::IsNotNull,
              ColumnPredicate::Range(varchar, TypeTraits<VARCHAR>::min_value(), nullptr)
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
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::Range(d32, TypeTraits<DECIMAL32>::max_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::Range(d64, TypeTraits<DECIMAL64>::max_value(), nullptr)
                              .predicate_type());
    ASSERT_EQ(PredicateType::Equality,
              ColumnPredicate::Range(d128, TypeTraits<DECIMAL128>::max_value(), nullptr)
                              .predicate_type());

    Slice s = "foo";
    ASSERT_EQ(PredicateType::Range,
              ColumnPredicate::Range(string, &s, nullptr).predicate_type());
    ASSERT_EQ(PredicateType::Range,
              ColumnPredicate::Range(binary, &s, nullptr).predicate_type());
    ASSERT_EQ(PredicateType::Range,
              ColumnPredicate::Range(varchar, &s, nullptr).predicate_type());
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
  int128_t one_dec = 1;

  ColumnSchema column_i32("a", INT32, true);
  ColumnSchema column_i64("b", INT64, true);
  ColumnSchema column_d("c", DOUBLE, true);
  ColumnSchema column_s("d", STRING, true);
  ColumnSchema column_d32("e", DECIMAL32, true);
  ColumnSchema column_d64("f", DECIMAL64, true);
  ColumnSchema column_d128("g", DECIMAL128, true);

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
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Range(column_d32, &one_dec, nullptr),
                                  ColumnPredicate::IsNotNull(column_d32)),
            0);
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Range(column_d64, &one_dec, nullptr),
                                  ColumnPredicate::IsNotNull(column_d64)),
            0);
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Range(column_d128, &one_dec, nullptr),
                                  ColumnPredicate::IsNotNull(column_d128)),
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
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Equality(column_d32, &one_dec),
                                  ColumnPredicate::Equality(column_i64, &one_64)),
            0);
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Equality(column_d32, &one_dec),
                                  ColumnPredicate::Equality(column_d, &one_d)),
            0);
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Equality(column_i32, &one_32),
                                  ColumnPredicate::Equality(column_s, &one_s)),
            0);
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Equality(column_d32, &one_dec),
                                  ColumnPredicate::Equality(column_d64, &one_dec)),
            0);
  ASSERT_LT(SelectivityComparator(ColumnPredicate::Equality(column_d64, &one_dec),
                                  ColumnPredicate::Equality(column_d128, &one_dec)),
            0);
}

TEST_F(TestColumnPredicate, TestRedaction) {
  ASSERT_NE("", gflags::SetCommandLineOption("redact", "log"));
  ColumnSchema column_i32("a", INT32, true);
  int32_t one_32 = 1;
  ASSERT_EQ("a = <redacted>", ColumnPredicate::Equality(column_i32, &one_32).ToString());
}

TEST_F(TestColumnPredicate, TestBloomFilterMerge) {
  Arena arena(1024);
  ArenaBlockBloomFilterBufferAllocator allocator(&arena);

  int n_keys = 5; // 0 1 both hit bf1 and bf2, 2 3 4 only hit bf2.

  // Test for UINT64 type.
  BlockBloomFilter b1(&allocator);
  int log_space_bytes1 = BlockBloomFilter::MinLogSpace(n_keys, 0.01);
  ASSERT_OK(b1.Init(log_space_bytes1, FAST_HASH, 0));
  double expected_fp_rate1 = BlockBloomFilter::FalsePositiveProb(n_keys, log_space_bytes1);
  ASSERT_LE(expected_fp_rate1, 0.01);

  BlockBloomFilter b2(&allocator);
  int log_space_bytes2 = BlockBloomFilter::MinLogSpace(n_keys, 0.01);
  ASSERT_OK(b2.Init(log_space_bytes2, FAST_HASH, 0));
  double expected_fp_rate2 = BlockBloomFilter::FalsePositiveProb(n_keys, log_space_bytes2);
  ASSERT_LE(expected_fp_rate2, 0.01);

  vector<uint64_t> values_int;
  FillBloomFilterAndValues(n_keys, &values_int, &b1, &b2);
  TestMergeBloomFilterCombinations(ColumnSchema("c", INT64, true), {&b1}, values_int);
  TestMergeBloomFilterCombinations(ColumnSchema("c", INT64, true), {&b1, &b2},
                                   values_int);

  // Test for STRING type.
  BlockBloomFilter b3(&allocator);
  int log_space_bytes3 = BlockBloomFilter::MinLogSpace(n_keys, 0.01);
  ASSERT_OK(b3.Init(log_space_bytes3, FAST_HASH, 0));
  double expected_fp_rate3 = BlockBloomFilter::FalsePositiveProb(n_keys, log_space_bytes3);
  ASSERT_LE(expected_fp_rate3, 0.01);

  vector<std::string> keys = {"0", "00", "10", "100", "1100"};
  vector<Slice> keys_slice;
  for (int i = 0; i < keys.size(); ++i) {
    Slice key_slice(keys[i]);
    if (i < 2) {
      b3.Insert(key_slice);
    }
    keys_slice.emplace_back(key_slice);
  }

  TestMergeBloomFilterCombinations(ColumnSchema("c", STRING, true), {&b3}, keys_slice);

  // Test for BINARY type
  BlockBloomFilter b4(&allocator);
  int log_space_bytes4 = BlockBloomFilter::MinLogSpace(n_keys, 0.01);
  ASSERT_OK(b4.Init(log_space_bytes4, FAST_HASH, 0));
  double expected_fp_rate4 = BlockBloomFilter::FalsePositiveProb(n_keys, log_space_bytes4);
  ASSERT_LE(expected_fp_rate4, 0.01);

  vector<Slice> binary_keys = { Slice("", 0),
                                Slice("\0", 1),
                                Slice("\0\0", 2),
                                Slice("\0\0\0", 3),
                                Slice("\0\0\0\0", 4) };
  for (int i = 0; i < binary_keys.size(); ++i) {
    if (i < 2) {
      b4.Insert(binary_keys[i]);
    }
  }
  TestMergeBloomFilterCombinations(ColumnSchema("c", STRING, true), {&b4}, binary_keys);
}

// Test ColumnPredicate operator (in-)equality.
TEST_F(TestColumnPredicate, TestEquals) {
  ColumnSchema c1("c1", INT32, true);
  ASSERT_EQ(ColumnPredicate::None(c1), ColumnPredicate::None(c1));

  ColumnSchema c1a("c1", INT32, true);
  ASSERT_EQ(ColumnPredicate::None(c1), ColumnPredicate::None(c1a));

  ColumnSchema c2("c2", INT32, true);
  ASSERT_NE(ColumnPredicate::None(c1), ColumnPredicate::None(c2));

  ColumnSchema c1string("c1", STRING, true);
  ASSERT_NE(ColumnPredicate::None(c1), ColumnPredicate::None(c1string));

  const int kDefaultOf3 = 3;
  ColumnSchema c1dflt("c1", INT32, /*is_nullable=*/false, /*read_default=*/&kDefaultOf3);
  ASSERT_NE(ColumnPredicate::None(c1), ColumnPredicate::None(c1dflt));
}

using TestColumnPredicateDeathTest = TestColumnPredicate;

// Ensure that ColumnPredicate::Merge(other) requires the 'other' predicate to
// have the same column name and type as 'this'.
TEST_F(TestColumnPredicateDeathTest, TestMergeRequiresNameAndType) {

  ColumnSchema c1int32("c1", INT32, true);
  ColumnSchema c2int32("c2", INT32, true);
  vector<int32_t> values = { 0, 1, 2, 3 };

  EXPECT_DEATH({
    // This should crash because the columns have different names.
    TestMerge(ColumnPredicate::Equality(c1int32, &values[0]),
              ColumnPredicate::Equality(c2int32, &values[0]),
              ColumnPredicate::None(c1int32), // unused
              PredicateType::None);
  }, "COMPARE_NAME_AND_TYPE");

  ColumnSchema c1int16("c1", INT16, true);
  EXPECT_DEATH({
    // This should crash because the columns have different types.
    TestMerge(ColumnPredicate::Equality(c1int32, &values[0]),
              ColumnPredicate::Equality(c1int16, &values[0]),
              ColumnPredicate::None(c1int32), // unused
              PredicateType::None);
  }, "COMPARE_NAME_AND_TYPE");
}

template<typename TypeParam>
class ColumnPredicateBenchmark : public KuduTest {
  protected:
   static constexpr auto kColType = TypeParam::physical_type;
   static constexpr int kNumRows = 1024;

   void DoTest(const std::function<ColumnPredicate(const ColumnSchema&)>& pred_factory) {
     const int num_iters = AllowSlowTests() ? 1000000 : 100;
     const int num_evals = num_iters * kNumRows;

     for (bool nullable : {false, true}) {
       ColumnSchema cs("c", kColType, nullable);
       auto pred = pred_factory(cs);
       if (pred.predicate_type() == PredicateType::None) {
         // The predicate factory might return None in the case of
         // NULL on a NOT NULL column.
         continue;
       }

       ScopedColumnBlock<kColType> b(kNumRows);
       for (int i = 0; i < kNumRows; i++) {
         b[i] = rand() % 3;
         if (nullable) {
           b.SetCellIsNull(i, rand() % 10 == 1);
         }
       }

       SelectionVector selvec(kNumRows);
       int64_t tot_cycles = 0;
       Stopwatch sw;
       sw.start();
       for (int i = 0; i < num_iters; i++) {
         selvec.SetAllTrue();
         int64_t cycles_start = CycleClock::Now();
         pred.Evaluate(b, &selvec);
         tot_cycles += CycleClock::Now() - cycles_start;
       }
       sw.stop();
       LOG(INFO) << StringPrintf(
             "%-6s %-10s (%s) %.1fM evals/sec\t%.2f cycles/eval",
             TypeParam::name(), nullable ? "NULL" : "NOT NULL",
             pred.ToString().c_str(),
             num_evals / sw.elapsed().user_cpu_seconds() / 1000000,
             static_cast<double>(tot_cycles) / num_evals);
     }
   }
};

template<class T>
class RangePredicateBenchmark : public ColumnPredicateBenchmark<T> {};

using test_types = ::testing::Types<
  DataTypeTraits<INT8>,
  DataTypeTraits<INT16>,
  DataTypeTraits<INT32>,
  DataTypeTraits<INT64>,
  DataTypeTraits<FLOAT>,
  DataTypeTraits<DOUBLE>>;

TYPED_TEST_CASE(RangePredicateBenchmark, test_types);

TYPED_TEST(RangePredicateBenchmark, TestEquals) {
  const typename TypeParam::cpp_type ref_val = 0;
  RangePredicateBenchmark<TypeParam>::DoTest(
      [&](const ColumnSchema& cs) { return ColumnPredicate::Equality(cs, &ref_val); });
}

TYPED_TEST(RangePredicateBenchmark, TestLessThan) {
  const typename TypeParam::cpp_type upper = 0;
  RangePredicateBenchmark<TypeParam>::DoTest(
      [&](const ColumnSchema& cs) { return ColumnPredicate::Range(cs, nullptr, &upper); });
}

TYPED_TEST(RangePredicateBenchmark, TestRange) {
  const typename TypeParam::cpp_type lower = 0;
  const typename TypeParam::cpp_type upper = 2;
  RangePredicateBenchmark<TypeParam>::DoTest(
      [&](const ColumnSchema& cs) { return ColumnPredicate::Range(cs, &lower, &upper); });
}

// IS NULL and IS NOT NULL predicates don't look at the data itself, so no need
// to type-parameterize them.
class NullPredicateBenchmark : public ColumnPredicateBenchmark<DataTypeTraits<INT32>> {};

TEST_F(NullPredicateBenchmark, TestIsNull) {
  NullPredicateBenchmark::DoTest(
      [&](const ColumnSchema& cs) { return ColumnPredicate::IsNull(cs); });
}
TEST_F(NullPredicateBenchmark, TestIsNotNull) {
  NullPredicateBenchmark::DoTest(
      [&](const ColumnSchema& cs) { return ColumnPredicate::IsNotNull(cs); });
}


} // namespace kudu
