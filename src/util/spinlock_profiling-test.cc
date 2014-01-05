// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "gutil/synchronization_profiling.h"
#include "gutil/spinlock.h"
#include "util/test_util.h"
#include "util/trace.h"

namespace kudu {

class SpinLockProfilingTest : public KuduTest {};

TEST_F(SpinLockProfilingTest, TestSpinlockProfiling) {
  Trace t;
  base::SpinLock lock;
  {
    ADOPT_TRACE(&t);
    gutil::SubmitSpinLockProfileData(&lock, 4000000);
  }
  std::stringstream stream;
  t.Dump(&stream);
  string result = stream.str();
  LOG(INFO) << "trace: " << result;
  // We can't assert more specifically because the CyclesPerSecond
  // on different machines might be different.
  ASSERT_STR_CONTAINS(result, "Waited ");
  ASSERT_STR_CONTAINS(result, "on lock ");
}

} // namespace kudu
