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
  scoped_refptr<Trace> t(new Trace);
  base::SpinLock lock;
  {
    ADOPT_TRACE(t.get());
    gutil::SubmitSpinLockProfileData(&lock, 4000000);
  }
  string result = t->DumpToString(true);
  LOG(INFO) << "trace: " << result;
  // We can't assert more specifically because the CyclesPerSecond
  // on different machines might be different.
  ASSERT_STR_CONTAINS(result, "Waited ");
  ASSERT_STR_CONTAINS(result, "on lock ");
}

} // namespace kudu
