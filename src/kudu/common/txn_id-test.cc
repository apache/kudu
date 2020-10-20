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

#include "kudu/common/txn_id.h"

#include <cstdint>

#include <glog/logging.h>
#include <gtest/gtest.h>

namespace kudu {

// Basic unit test scenario for TxnId.
TEST(TxnIdTest, Basic) {
  TxnId id(0);
  ASSERT_TRUE(id.IsValid());
  ASSERT_EQ(0, id.value());

  // Implicit convesion: operator int64_t().
  int64_t id_value = id;
  ASSERT_EQ(0, id_value);

  // operator!()
  ASSERT_FALSE(!id);

  ASSERT_EQ("0", id.ToString());

  TxnId invalid_id;
  ASSERT_GT(id, invalid_id);

  TxnId id_0(0);
  ASSERT_EQ(id, id_0);
  ASSERT_TRUE(id == id_0);
  ASSERT_TRUE(id_0 >= id);
  ASSERT_TRUE(id_0 <= id);

  TxnId id_1(1);
  ASSERT_GT(id_1, id);
  ASSERT_TRUE(id_1 > id);
  ASSERT_TRUE(id_1 >= id);
  ASSERT_TRUE(id < id_1);
  ASSERT_TRUE(id <= id_1);
}

// Test scenarios specific to invalid transaction identifiers.
TEST(TxnIdTest, InvalidTxnIdValue) {
  // Non-initialized txn id has invalid value.
  TxnId invalid_id;

  // operator!()
  ASSERT_TRUE(!invalid_id);

#if DCHECK_IS_ON()
  ASSERT_DEATH({
    auto v = invalid_id.value();
    TxnId(v).ToString(); // unreachable
  },
  "TxnId contains an invalid value");
#endif // #if DCHECK_IS_ON() ...

  ASSERT_EQ("InvalidTxnId", invalid_id.ToString());

#if DCHECK_IS_ON()
  ASSERT_DEATH({
    TxnId id(-1);
  },
  "negative value is not allowed for TxnId");
  ASSERT_DEATH({
    TxnId id(-2);
  },
  "negative value is not allowed for TxnId");
#endif // #if DCHECK_IS_ON() ...
}

} // namespace kudu
