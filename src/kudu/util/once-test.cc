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

#include <ostream>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/once.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::thread;
using std::vector;

namespace kudu {

namespace {

template<class KuduOnceType>
struct Thing {
  explicit Thing(bool should_fail)
    : should_fail_(should_fail),
      value_(0) {
  }

  Status Init();

  Status InitOnce() {
    if (should_fail_) {
      return Status::IllegalState("Whoops!");
    }
    value_ = 1;
    return Status::OK();
  }

  const bool should_fail_;
  int value_;
  KuduOnceType once_;
};

template<>
Status Thing<KuduOnceDynamic>::Init() {
  return once_.Init(&Thing<KuduOnceDynamic>::InitOnce, this);
}

template<>
Status Thing<KuduOnceLambda>::Init() {
  return once_.Init([this] { return InitOnce(); });
}

template<class KuduOnceType>
static void InitOrGetInitted(Thing<KuduOnceType>* t, int i) {
  if (i % 2 == 0) {
    LOG(INFO) << "Thread " << i << " initting";
    t->Init();
  } else {
    LOG(INFO) << "Thread " << i << " value: " << t->once_.init_succeeded();
  }
}

}  // anonymous namespace

typedef ::testing::Types<KuduOnceDynamic, KuduOnceLambda> KuduOnceTypes;
TYPED_TEST_CASE(TestOnce, KuduOnceTypes);

template<class KuduOnceType>
class TestOnce : public KuduTest {};

TYPED_TEST(TestOnce, KuduOnceTest) {
  {
    Thing<TypeParam> t(false);
    ASSERT_EQ(0, t.value_);
    ASSERT_FALSE(t.once_.init_succeeded());

    for (int i = 0; i < 2; i++) {
      ASSERT_OK(t.Init());
      ASSERT_EQ(1, t.value_);
      ASSERT_TRUE(t.once_.init_succeeded());
    }
  }

  {
    Thing<TypeParam> t(true);
    for (int i = 0; i < 2; i++) {
      ASSERT_TRUE(t.Init().IsIllegalState());
      ASSERT_EQ(0, t.value_);
      ASSERT_FALSE(t.once_.init_succeeded());
    }
  }
}

TYPED_TEST(TestOnce, KuduOnceThreadSafeTest) {
  Thing<TypeParam> thing(false);

  // The threads will read and write to thing.once_.initted. If access to
  // it is not synchronized, TSAN will flag the access as data races.
  constexpr int kNumThreads = 10;
  vector<thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&thing, i]() { InitOrGetInitted<TypeParam>(&thing, i); });
  }

  for (auto& t : threads) {
    t.join();
  }
}

} // namespace kudu
