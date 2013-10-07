// Copyright (c) 2013, Cloudera, inc.
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "util/jsonwriter.h"
#include "util/metrics.h"
#include "util/test_util.h"

namespace kudu {

class MetricsTest : public KuduTest {
};

TEST_F(MetricsTest, SimpleIntegerGaugeTest) {
  string desc = "Number of quarters in my pocket";
  IntegerGauge quarters_in_my_pocket("quarters", desc);
  ASSERT_EQ(desc, quarters_in_my_pocket.description());
  ASSERT_EQ(0, quarters_in_my_pocket.value());
  quarters_in_my_pocket.set_value(5);
  ASSERT_EQ(5, quarters_in_my_pocket.value());
}

TEST_F(MetricsTest, SimpleCounterTest) {
  string desc = "Number of requests pending";
  Counter requests(MetricUnit::kRequests, desc);
  ASSERT_EQ(desc, requests.description());
  ASSERT_EQ(0, requests.value());
  requests.Increment();
  ASSERT_EQ(1, requests.value());
  requests.IncrementBy(2);
  ASSERT_EQ(3, requests.value());
  requests.Decrement();
  ASSERT_EQ(2, requests.value());
  requests.DecrementBy(2);
  ASSERT_EQ(0, requests.value());
}

TEST_F(MetricsTest, JsonPrintTest) {
  MetricRegistry metrics;
  Counter* bytes_seen = CHECK_NOTNULL(metrics.FindOrCreateCounter("bytes_seen",
      MetricUnit::kBytes, "Number of bytes seen"));
  bytes_seen->Increment();

  // Generate the JSON.
  std::stringstream out;
  JsonWriter writer(&out);
  ASSERT_STATUS_OK(metrics.WriteAsJson(&writer));

  // Now parse it back out.
  rapidjson::Document d;
  d.Parse<0>(out.str().c_str());
  // Note: you need to specify 0u instead of just 0 because the rapidjson Value
  // class overloads both operator[int] and operator[char*] and 0 == NULL.
  ASSERT_EQ(string("bytes_seen"), string(d["metrics"][0u]["name"].GetString()));
  ASSERT_EQ(1L, d["metrics"][0u]["value"].GetInt64());
}

} // namespace kudu
