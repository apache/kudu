// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <vector>

#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include "kudu/gutil/bind.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/once.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/thread.h"

using std::vector;
using strings::Substitute;

namespace kudu {

namespace {

struct Thing {
  explicit Thing(bool should_fail)
    : should_fail_(should_fail),
      value_(0) {
  }

  Status Init() {
    return once_.Init(&Thing::InitOnce, this);
  }

  Status InitOnce() {
    if (should_fail_) {
      return Status::IllegalState("Whoops!");
    }
    value_ = 1;
    return Status::OK();
  }

  const bool should_fail_;
  int value_;
  KuduOnceDynamic once_;
};

} // anonymous namespace

TEST(TestOnce, KuduOnceDynamicTest) {
  {
    Thing t(false);
    ASSERT_EQ(0, t.value_);
    ASSERT_FALSE(t.once_.initted());

    for (int i = 0; i < 2; i++) {
      ASSERT_OK(t.Init());
      ASSERT_EQ(1, t.value_);
      ASSERT_TRUE(t.once_.initted());
    }
  }

  {
    Thing t(true);
    for (int i = 0; i < 2; i++) {
      ASSERT_TRUE(t.Init().IsIllegalState());
      ASSERT_EQ(0, t.value_);
      ASSERT_TRUE(t.once_.initted());
    }
  }
}

static void InitOrGetInitted(Thing* t, int i) {
  if (i % 2 == 0) {
    LOG(INFO) << "Thread " << i << " initting";
    t->Init();
  } else {
    LOG(INFO) << "Thread " << i << " value: " << t->once_.initted();
  }
}

TEST(TestOnce, KuduOnceDynamicThreadSafeTest) {
  Thing thing(false);

  // The threads will read and write to thing.once_.initted. If access to
  // it is not synchronized, TSAN will flag the access as data races.
  vector<scoped_refptr<Thread> > threads;
  for (int i = 0; i < 10; i++) {
    scoped_refptr<Thread> t;
    ASSERT_OK(Thread::Create("test", Substitute("thread $0", i),
                             &InitOrGetInitted, &thing, i, &t));
    threads.push_back(t);
  }

  BOOST_FOREACH(const scoped_refptr<Thread>& t, threads) {
    t->Join();
  }
}

} // namespace kudu
