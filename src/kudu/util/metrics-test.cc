// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include <boost/assign/list_of.hpp>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <string>
#include <vector>

#include "kudu/gutil/bind.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_util.h"

using boost::assign::list_of;
using std::string;
using std::vector;

DECLARE_int32(metrics_retirement_age_ms);

namespace kudu {

class MetricsTest : public KuduTest {
};

METRIC_DEFINE_counter(reqs_pending, MetricUnit::kRequests,
                      "Number of requests pending");

TEST_F(MetricsTest, SimpleCounterTest) {
  scoped_refptr<Counter> requests =
    new Counter(METRIC_reqs_pending);
  ASSERT_EQ("Number of requests pending", requests->description());
  ASSERT_EQ(0, requests->value());
  requests->Increment();
  ASSERT_EQ(1, requests->value());
  requests->IncrementBy(2);
  ASSERT_EQ(3, requests->value());
}

METRIC_DEFINE_gauge_uint64(fake_memory_usage, MetricUnit::kBytes, "Test Gauge 1");

TEST_F(MetricsTest, SimpleAtomicGaugeTest) {
  MetricRegistry registry;
  MetricContext context(&registry, "test");

  scoped_refptr<AtomicGauge<uint64_t> > mem_usage =
    AtomicGauge<uint64_t>::Instantiate(METRIC_fake_memory_usage,
                                       context);
  ASSERT_EQ(METRIC_fake_memory_usage.description(), mem_usage->description());
  ASSERT_EQ(0, mem_usage->value());
  mem_usage->IncrementBy(7);
  ASSERT_EQ(7, mem_usage->value());
  mem_usage->set_value(5);
  ASSERT_EQ(5, mem_usage->value());
}

TEST_F(MetricsTest, HighWaterMarkTest) {
  GaugePrototype<int64_t> proto("test", MetricUnit::kBytes, "Test HighWaterMark");
  scoped_refptr<HighWaterMark<int64_t> > hwm =
    new HighWaterMark<int64_t>(proto, 0);
  hwm->IncrementBy(1);
  ASSERT_EQ(1, hwm->current_value());
  ASSERT_EQ(1, hwm->value());
  hwm->IncrementBy(42);
  ASSERT_EQ(43, hwm->current_value());
  ASSERT_EQ(43, hwm->value());
  hwm->DecrementBy(1);
  ASSERT_EQ(42, hwm->current_value());
  ASSERT_EQ(43, hwm->value());
}

METRIC_DEFINE_gauge_int64(test_func_gauge, MetricUnit::kBytes, "Test Gauge 2");

static int64_t MyFunction(int* metric_val) {
  return (*metric_val)++;
}

TEST_F(MetricsTest, SimpleFunctionGaugeTest) {
  MetricRegistry registry;
  MetricContext context(&registry, "test");
  int metric_val = 1000;
  scoped_refptr<FunctionGauge<int64_t> > gauge =
    METRIC_test_func_gauge.InstantiateFunctionGauge(
      context, Bind(&MyFunction, Unretained(&metric_val)));

  ASSERT_EQ(1000, gauge->value());
  ASSERT_EQ(1001, gauge->value());

  gauge->DetachToCurrentValue();
  // After detaching, it should continue to return the same constant value.
  ASSERT_EQ(1002, gauge->value());
  ASSERT_EQ(1002, gauge->value());

  // Test resetting to a constant.
  gauge->DetachToConstant(2);
  ASSERT_EQ(2, gauge->value());
}

TEST_F(MetricsTest, AutoDetachToLastValue) {
  MetricRegistry registry;
  MetricContext context(&registry, "test");

  int metric_val = 1000;
  scoped_refptr<FunctionGauge<int64_t> > gauge =
    METRIC_test_func_gauge.InstantiateFunctionGauge(
      context, Bind(&MyFunction, Unretained(&metric_val)));

  ASSERT_EQ(1000, gauge->value());
  ASSERT_EQ(1001, gauge->value());
  {
    FunctionGaugeDetacher detacher;
    gauge->AutoDetachToLastValue(&detacher);
    ASSERT_EQ(1002, gauge->value());
    ASSERT_EQ(1003, gauge->value());
  }

  ASSERT_EQ(1004, gauge->value());
  ASSERT_EQ(1004, gauge->value());
}

TEST_F(MetricsTest, AutoDetachToConstant) {
  MetricRegistry registry;
  MetricContext context(&registry, "test");

  int metric_val = 1000;
  scoped_refptr<FunctionGauge<int64_t> > gauge =
    METRIC_test_func_gauge.InstantiateFunctionGauge(
      context, Bind(&MyFunction, Unretained(&metric_val)));

  ASSERT_EQ(1000, gauge->value());
  ASSERT_EQ(1001, gauge->value());
  {
    FunctionGaugeDetacher detacher;
    gauge->AutoDetach(&detacher, 12345);
    ASSERT_EQ(1002, gauge->value());
    ASSERT_EQ(1003, gauge->value());
  }

  ASSERT_EQ(12345, gauge->value());
}


METRIC_DEFINE_histogram(test_hist, MetricUnit::kMilliseconds, "foo", 1000000, 3);

TEST_F(MetricsTest, SimpleHistogramTest) {
  MetricRegistry registry;
  MetricContext context(&registry, "test");
  scoped_refptr<Histogram> hist = METRIC_test_hist.Instantiate(context);
  hist->Increment(2);
  hist->IncrementBy(4, 1);
  ASSERT_EQ(2, hist->histogram_->MinValue());
  ASSERT_EQ(3, hist->histogram_->MeanValue());
  ASSERT_EQ(4, hist->histogram_->MaxValue());
  ASSERT_EQ(2, hist->histogram_->TotalCount());
  // TODO: Test coverage needs to be improved a lot.
}

TEST_F(MetricsTest, JsonPrintTest) {
  MetricRegistry metrics;
  scoped_refptr<Counter> bytes_seen = CHECK_NOTNULL(
    metrics.FindOrCreateCounter("reqs_pending", METRIC_reqs_pending).get());
  bytes_seen->Increment();

  // Generate the JSON.
  std::stringstream out;
  JsonWriter writer(&out);
  ASSERT_OK(metrics.WriteAsJson(&writer,
                                       list_of("*"),
                                       vector<string>()));

  // Now parse it back out.
  rapidjson::Document d;
  d.Parse<0>(out.str().c_str());
  // Note: you need to specify 0u instead of just 0 because the rapidjson Value
  // class overloads both operator[int] and operator[char*] and 0 == NULL.
  ASSERT_EQ(string("reqs_pending"), string(d["metrics"][0u]["name"].GetString()));
  ASSERT_EQ(1L, d["metrics"][0u]["value"].GetInt64());
}

// Test that metrics are retired when they are no longer referenced.
TEST_F(MetricsTest, RetirementTest) {
  FLAGS_metrics_retirement_age_ms = 100;

  const string kMetricName = "foo";
  MetricRegistry registry;
  scoped_refptr<Counter> counter = CHECK_NOTNULL(
    registry.FindOrCreateCounter(kMetricName, METRIC_reqs_pending).get());
  ASSERT_EQ(1, registry.UnsafeMetricsMapForTests().size());

  // Since we hold a reference to the counter, it should not get retired.
  registry.RetireOldMetrics();
  ASSERT_EQ(1, registry.UnsafeMetricsMapForTests().size());

  // When we de-ref it, it should not get immediately retired, either, because
  // we keep retirable metrics around for some amount of time. We try retiring
  // a number of times to hit all the cases.
  counter = NULL;
  for (int i = 0; i < 3; i++) {
    registry.RetireOldMetrics();
    ASSERT_EQ(1, registry.UnsafeMetricsMapForTests().size());
  }

  // If we wait for longer than the retirement time, and call retire again, we'll
  // actually retire it.
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_metrics_retirement_age_ms * 1.5));
  registry.RetireOldMetrics();
  ASSERT_EQ(0, registry.UnsafeMetricsMapForTests().size());
}

// Test that we can mark a metric to never be retired.
TEST_F(MetricsTest, NeverRetireTest) {
  MetricRegistry registry;
  MetricContext context(&registry, "test");
  registry.NeverRetire(METRIC_test_hist.Instantiate(context));
  FLAGS_metrics_retirement_age_ms = 0;

  for (int i = 0; i < 3; i++) {
    registry.RetireOldMetrics();
    ASSERT_EQ(1, registry.UnsafeMetricsMapForTests().size());
  }
}

} // namespace kudu
