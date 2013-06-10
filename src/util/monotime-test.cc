// Copyright (c) 2013, Cloudera, inc.

#include "util/monotime.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

namespace kudu {
namespace util {

TEST(TestMonotime, TestMonotonicity) {
  alarm(360);
  MonoTime prev(MonoTime::Now(MonoTime::FINE));
  MonoTime next;

  do {
    next = MonoTime::Now(MonoTime::FINE);
    //LOG(INFO) << " next = " << next.ToString();
  } while(!prev.ComesBefore(next));
  ASSERT_FALSE(next.ComesBefore(prev));
  alarm(0);
}

TEST(TestMonotime, TestDeltas) {
  alarm(360);
  const MonoDelta max_delta(MonoDelta::FromSeconds(0.1));
  MonoTime prev(MonoTime::Now(MonoTime::FINE));
  MonoTime next;
  MonoDelta cur_delta;
  do {
    next = MonoTime::Now(MonoTime::FINE);
    cur_delta = next.GetDeltaSince(prev);
  } while (cur_delta.LessThan(max_delta));
  alarm(0);
}

static void DoTestMonotimePerf(MonoTime::Granularity granularity) {
  const MonoDelta max_delta(MonoDelta::FromMilliseconds(500));
  uint64_t num_calls = 0;
  MonoTime prev(MonoTime::Now(granularity));
  MonoTime next;
  MonoDelta cur_delta;
  do {
    next = MonoTime::Now(granularity);
    cur_delta = next.GetDeltaSince(prev);
    num_calls++;
  } while (cur_delta.LessThan(max_delta));
  LOG(INFO) << "DoTestMonotimePerf(granularity="
        << ((granularity == MonoTime::FINE) ? "FINE" : "COARSE")
        << "): " << num_calls << " in "
        << max_delta.ToString() << " seconds.";
}

TEST(TestMonotimePerf, TestMonotimePerfCoarse) {
  alarm(360);
  DoTestMonotimePerf(MonoTime::COARSE);
  alarm(0);
}

TEST(TestMonotimePerf, TestMonotimePerfFine) {
  alarm(360);
  DoTestMonotimePerf(MonoTime::FINE);
  alarm(0);
}

}
}
