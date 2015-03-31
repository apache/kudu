// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <string>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include "kudu/util/trace.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/debug/trace_event_synthetic_delay.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

using kudu::debug::TraceLog;
using kudu::debug::TraceResultBuffer;
using kudu::debug::CategoryFilter;

namespace kudu {

class TraceTest : public KuduTest {
};

// Replace all digits in 's' with the character 'X'.
static string XOutDigits(const string& s) {
  string ret;
  ret.reserve(s.size());
  BOOST_FOREACH(char c, s) {
    if (isdigit(c)) {
      ret.push_back('X');
    } else {
      ret.push_back(c);
    }
  }
  return ret;
}

TEST_F(TraceTest, TestBasic) {
  scoped_refptr<Trace> t(new Trace);
  TRACE_TO(t, "hello $0, $1", "world", 12345);
  TRACE_TO(t, "goodbye $0, $1", "cruel world", 54321);

  string result = XOutDigits(t->DumpToString(false));
  ASSERT_EQ("XXXX XX:XX:XX.XXXXXX trace-test.cc:XX] hello world, XXXXX\n"
            "XXXX XX:XX:XX.XXXXXX trace-test.cc:XX] goodbye cruel world, XXXXX\n",
            result);
}

TEST_F(TraceTest, TestAttach) {
  scoped_refptr<Trace> traceA(new Trace);
  scoped_refptr<Trace> traceB(new Trace);
  {
    ADOPT_TRACE(traceA.get());
    EXPECT_EQ(traceA.get(), Trace::CurrentTrace());
    {
      ADOPT_TRACE(traceB.get());
      EXPECT_EQ(traceB.get(), Trace::CurrentTrace());
      TRACE("hello from traceB");
    }
    EXPECT_EQ(traceA.get(), Trace::CurrentTrace());
    TRACE("hello from traceA");
  }
  EXPECT_TRUE(Trace::CurrentTrace() == NULL);
  TRACE("this goes nowhere");

  EXPECT_EQ(XOutDigits(traceA->DumpToString(false)),
            "XXXX XX:XX:XX.XXXXXX trace-test.cc:XX] hello from traceA\n");
  EXPECT_EQ(XOutDigits(traceB->DumpToString(false)),
            "XXXX XX:XX:XX.XXXXXX trace-test.cc:XX] hello from traceB\n");
}

TEST_F(TraceTest, TestChildTrace) {
  scoped_refptr<Trace> traceA(new Trace);
  scoped_refptr<Trace> traceB(new Trace);
  ADOPT_TRACE(traceA.get());
  traceA->AddChildTrace(traceB.get());
  TRACE("hello from traceA");
  {
    ADOPT_TRACE(traceB.get());
    TRACE("hello from traceB");
  }
  EXPECT_EQ(XOutDigits(traceA->DumpToString(false)),
            "XXXX XX:XX:XX.XXXXXX trace-test.cc:XX] hello from traceA\n"
            "Related trace:\n"
            "XXXX XX:XX:XX.XXXXXX trace-test.cc:XX] hello from traceB\n");
}

static void GenerateTraceEvents(int thread_id,
                                int num_events) {
  for (int i = 0; i < num_events; i++) {
    TRACE_EVENT1("test", "foo", "thread_id", thread_id);
  }
}

void VerifyValidJson(const std::string& json) {
  rapidjson::Document d;
  d.Parse<0>(json.c_str());
  ASSERT_TRUE(d.IsObject());
}

// Parse the dumped trace data and return the number of events
// found within, including only those with the "test" category.
int ParseAndReturnEventCount(const std::string& trace_json) {
  rapidjson::Document d;
  d.Parse<0>(trace_json.c_str());
  CHECK(d.IsObject()) << "bad json: " << trace_json;
  const rapidjson::Value& events_json = d["traceEvents"];
  CHECK(events_json.IsArray()) << "bad json: " << trace_json;

  // Count how many of our events were seen. We have to filter out
  // the metadata events.
  int seen_real_events = 0;
  for (int i = 0; i < events_json.Size(); i++) {
    if (events_json[i]["cat"].GetString() == string("test")) {
      seen_real_events++;
    }
  }

  return seen_real_events;
}

TEST_F(TraceTest, TestChromeTracing) {
  const int kNumThreads = 4;
  const int kEventsPerThread = AllowSlowTests() ? 1000000 : 10000;

  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
                 TraceLog::RECORDING_MODE,
                 TraceLog::RECORD_CONTINUOUSLY);

  vector<scoped_refptr<Thread> > threads(kNumThreads);

  Stopwatch s;
  s.start();
  for (int i = 0; i < kNumThreads; i++) {
    CHECK_OK(Thread::Create("test", "gen-traces", &GenerateTraceEvents, i, kEventsPerThread,
                            &threads[i]));
  }

  for (int i = 0; i < kNumThreads; i++) {
    threads[i]->Join();
  }
  tl->SetDisabled();

  int total_events = kNumThreads * kEventsPerThread;
  double elapsed = s.elapsed().wall_seconds();

  LOG(INFO) << "Trace performance: " << static_cast<int>(total_events / elapsed) << " traces/sec";

  string trace_json = TraceResultBuffer::FlushTraceLogToString();

  // Verify that the JSON contains events. It won't have exactly
  // kEventsPerThread * kNumThreads because the trace buffer isn't large enough
  // for that.
  ASSERT_GE(ParseAndReturnEventCount(trace_json), 100);
}

// Test that, if a thread exits before filling a full trace buffer, we still
// see its results. This is a regression test for a bug in the earlier integration
// of Chromium tracing into Kudu.
TEST_F(TraceTest, TestTraceFromExitedThread) {
  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
                 TraceLog::RECORDING_MODE,
                 TraceLog::RECORD_CONTINUOUSLY);

  // Generate 10 trace events in a separate thread.
  int kNumEvents = 10;
  scoped_refptr<Thread> t;
  CHECK_OK(Thread::Create("test", "gen-traces", &GenerateTraceEvents, 1, kNumEvents,
                          &t));
  t->Join();
  tl->SetDisabled();
  string trace_json = TraceResultBuffer::FlushTraceLogToString();
  LOG(INFO) << trace_json;

  // Verify that the buffer contains 10 trace events
  ASSERT_EQ(10, ParseAndReturnEventCount(trace_json));
}

static void GenerateWideSpan() {
  TRACE_EVENT0("test", "GenerateWideSpan");
  for (int i = 0; i < 1000; i++) {
    TRACE_EVENT0("test", "InnerLoop");
  }
}

// Test creating a trace event which contains many other trace events.
// This ensures that we can go back and update a TraceEvent which fell in
// a different trace chunk.
TEST_F(TraceTest, TestWideSpan) {
  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
                 TraceLog::RECORDING_MODE,
                 TraceLog::RECORD_CONTINUOUSLY);

  scoped_refptr<Thread> t;
  CHECK_OK(Thread::Create("test", "gen-traces", &GenerateWideSpan, &t));
  t->Join();
  tl->SetDisabled();

  string trace_json = TraceResultBuffer::FlushTraceLogToString();
  ASSERT_EQ(1001, ParseAndReturnEventCount(trace_json));
}

// Generate trace events continuously until 'latch' fires.
// Increment *num_events_generated for each event generated.
void GenerateTracesUntilLatch(AtomicInt<int64_t>* num_events_generated,
                              CountDownLatch* latch) {
  while (latch->count()) {
    {
      // This goes in its own scope so that the event is fully generated (with
      // both its START and END times) before we do the counter increment below.
      TRACE_EVENT0("test", "GenerateTracesUntilLatch");
    }
    num_events_generated->Increment();
  }
}

// Test starting and stopping tracing while a thread is running.
// This is a regression test for bugs in earlier versions of the imported
// trace code.
TEST_F(TraceTest, TestStartAndStopCollection) {
  TraceLog* tl = TraceLog::GetInstance();

  CountDownLatch latch(1);
  AtomicInt<int64_t> num_events_generated(0);
  scoped_refptr<Thread> t;
  CHECK_OK(Thread::Create("test", "gen-traces", &GenerateTracesUntilLatch,
                          &num_events_generated, &latch, &t));

  const int num_flushes = AllowSlowTests() ? 50 : 3;
  for (int i = 0; i < num_flushes; i++) {
    tl->SetEnabled(CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
                   TraceLog::RECORDING_MODE,
                   TraceLog::RECORD_CONTINUOUSLY);

    const int64_t num_events_before = num_events_generated.Load();
    SleepFor(MonoDelta::FromMilliseconds(10));
    const int64_t num_events_after = num_events_generated.Load();
    tl->SetDisabled();

    string trace_json = TraceResultBuffer::FlushTraceLogToString();
    // We might under-count the number of events, since we only measure the sleep,
    // and tracing is enabled before and disabled after we start counting.
    // We might also over-count by at most 1, because we could enable tracing
    // right in between creating a trace event and incrementing the counter.
    // But, we should never over-count by more than 1.
    int expected_events_lowerbound = num_events_after - num_events_before - 1;
    int captured_events = ParseAndReturnEventCount(trace_json);
    ASSERT_GE(captured_events, expected_events_lowerbound);
  }

  latch.CountDown();
  t->Join();
}

TEST_F(TraceTest, TestChromeSampling) {
  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
                 TraceLog::RECORDING_MODE,
                 static_cast<TraceLog::Options>(TraceLog::RECORD_CONTINUOUSLY |
                                                TraceLog::ENABLE_SAMPLING));

  for (int i = 0; i < 100; i++) {
    switch (i % 3) {
      case 0:
        TRACE_EVENT_SET_SAMPLING_STATE("test", "state-0");
        break;
      case 1:
        TRACE_EVENT_SET_SAMPLING_STATE("test", "state-1");
        break;
      case 2:
        TRACE_EVENT_SET_SAMPLING_STATE("test", "state-2");
        break;
    }
    SleepFor(MonoDelta::FromMilliseconds(1));
  }
  tl->SetDisabled();
  string trace_json = TraceResultBuffer::FlushTraceLogToString();
  ASSERT_GT(ParseAndReturnEventCount(trace_json), 0);
}

////////////////////////////////////////////////////////////
// Tests for synthetic delay
// (from chromium-base/debug/trace_event_synthetic_delay_unittest.cc)
////////////////////////////////////////////////////////////

namespace {

const int kTargetDurationMs = 100;
// Allow some leeway in timings to make it possible to run these tests with a
// wall clock time source too.
const int kShortDurationMs = 10;

}  // namespace

namespace debug {

class TraceEventSyntheticDelayTest : public KuduTest,
                                     public TraceEventSyntheticDelayClock {
 public:
  TraceEventSyntheticDelayTest() {
    now_ = MonoTime::Min();
  }

  virtual ~TraceEventSyntheticDelayTest() {
    ResetTraceEventSyntheticDelays();
  }

  // TraceEventSyntheticDelayClock implementation.
  virtual MonoTime Now() OVERRIDE {
    AdvanceTime(MonoDelta::FromMilliseconds(kShortDurationMs / 10));
    return now_;
  }

  TraceEventSyntheticDelay* ConfigureDelay(const char* name) {
    TraceEventSyntheticDelay* delay = TraceEventSyntheticDelay::Lookup(name);
    delay->SetClock(this);
    delay->SetTargetDuration(
      MonoDelta::FromMilliseconds(kTargetDurationMs));
    return delay;
  }

  void AdvanceTime(MonoDelta delta) { now_.AddDelta(delta); }

  int TestFunction() {
    MonoTime start = Now();
    { TRACE_EVENT_SYNTHETIC_DELAY("test.Delay"); }
    MonoTime end = Now();
    return end.GetDeltaSince(start).ToMilliseconds();
  }

  int AsyncTestFunctionBegin() {
    MonoTime start = Now();
    { TRACE_EVENT_SYNTHETIC_DELAY_BEGIN("test.AsyncDelay"); }
    MonoTime end = Now();
    return end.GetDeltaSince(start).ToMilliseconds();
  }

  int AsyncTestFunctionEnd() {
    MonoTime start = Now();
    { TRACE_EVENT_SYNTHETIC_DELAY_END("test.AsyncDelay"); }
    MonoTime end = Now();
    return end.GetDeltaSince(start).ToMilliseconds();
  }

 private:
  MonoTime now_;

  DISALLOW_COPY_AND_ASSIGN(TraceEventSyntheticDelayTest);
};

TEST_F(TraceEventSyntheticDelayTest, StaticDelay) {
  TraceEventSyntheticDelay* delay = ConfigureDelay("test.Delay");
  delay->SetMode(TraceEventSyntheticDelay::STATIC);
  EXPECT_GE(TestFunction(), kTargetDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, OneShotDelay) {
  TraceEventSyntheticDelay* delay = ConfigureDelay("test.Delay");
  delay->SetMode(TraceEventSyntheticDelay::ONE_SHOT);
  EXPECT_GE(TestFunction(), kTargetDurationMs);
  EXPECT_LT(TestFunction(), kShortDurationMs);

  delay->SetTargetDuration(
      MonoDelta::FromMilliseconds(kTargetDurationMs));
  EXPECT_GE(TestFunction(), kTargetDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, AlternatingDelay) {
  TraceEventSyntheticDelay* delay = ConfigureDelay("test.Delay");
  delay->SetMode(TraceEventSyntheticDelay::ALTERNATING);
  EXPECT_GE(TestFunction(), kTargetDurationMs);
  EXPECT_LT(TestFunction(), kShortDurationMs);
  EXPECT_GE(TestFunction(), kTargetDurationMs);
  EXPECT_LT(TestFunction(), kShortDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelay) {
  ConfigureDelay("test.AsyncDelay");
  EXPECT_LT(AsyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_GE(AsyncTestFunctionEnd(), kTargetDurationMs / 2);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelayExceeded) {
  ConfigureDelay("test.AsyncDelay");
  EXPECT_LT(AsyncTestFunctionBegin(), kShortDurationMs);
  AdvanceTime(MonoDelta::FromMilliseconds(kTargetDurationMs));
  EXPECT_LT(AsyncTestFunctionEnd(), kShortDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelayNoActivation) {
  ConfigureDelay("test.AsyncDelay");
  EXPECT_LT(AsyncTestFunctionEnd(), kShortDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelayNested) {
  ConfigureDelay("test.AsyncDelay");
  EXPECT_LT(AsyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_LT(AsyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_LT(AsyncTestFunctionEnd(), kShortDurationMs);
  EXPECT_GE(AsyncTestFunctionEnd(), kTargetDurationMs / 2);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelayUnbalanced) {
  ConfigureDelay("test.AsyncDelay");
  EXPECT_LT(AsyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_GE(AsyncTestFunctionEnd(), kTargetDurationMs / 2);
  EXPECT_LT(AsyncTestFunctionEnd(), kShortDurationMs);

  EXPECT_LT(AsyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_GE(AsyncTestFunctionEnd(), kTargetDurationMs / 2);
}

TEST_F(TraceEventSyntheticDelayTest, ResetDelays) {
  ConfigureDelay("test.Delay");
  ResetTraceEventSyntheticDelays();
  EXPECT_LT(TestFunction(), kShortDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, BeginParallel) {
  TraceEventSyntheticDelay* delay = ConfigureDelay("test.AsyncDelay");
  MonoTime end_times[2];
  MonoTime start_time = Now();

  delay->BeginParallel(&end_times[0]);
  EXPECT_FALSE(!end_times[0].Initialized());

  delay->BeginParallel(&end_times[1]);
  EXPECT_FALSE(!end_times[1].Initialized());

  delay->EndParallel(end_times[0]);
  EXPECT_GE(Now().GetDeltaSince(start_time).ToMilliseconds(), kTargetDurationMs);

  start_time = Now();
  delay->EndParallel(end_times[1]);
  EXPECT_LT(Now().GetDeltaSince(start_time).ToMilliseconds(), kShortDurationMs);
}

} // namespace debug
} // namespace kudu
