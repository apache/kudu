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

#include "kudu/util/metrics.h"

#include <cstdint>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/monotime.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::unordered_map;
using std::unordered_set;
using std::vector;

DECLARE_int32(metrics_retirement_age_ms);

namespace kudu {

METRIC_DEFINE_entity(test_entity);

class MetricsTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    entity_ = METRIC_ENTITY_test_entity.Instantiate(&registry_, "my-test");
  }

 protected:
  MetricRegistry registry_;
  scoped_refptr<MetricEntity> entity_;
};

METRIC_DEFINE_counter(test_entity, test_counter, "My Test Counter", MetricUnit::kRequests,
                      "Description of test counter");

TEST_F(MetricsTest, SimpleCounterTest) {
  scoped_refptr<Counter> requests =
    new Counter(&METRIC_test_counter);
  ASSERT_EQ("Description of test counter", requests->prototype()->description());
  ASSERT_EQ(0, requests->value());
  requests->Increment();
  ASSERT_EQ(1, requests->value());
  requests->IncrementBy(2);
  ASSERT_EQ(3, requests->value());
}

METRIC_DEFINE_gauge_uint64(test_entity, test_gauge, "Test uint64 Gauge",
                           MetricUnit::kBytes, "Description of Test Gauge");

TEST_F(MetricsTest, SimpleAtomicGaugeTest) {
  scoped_refptr<AtomicGauge<uint64_t> > mem_usage =
    METRIC_test_gauge.Instantiate(entity_, 0);
  ASSERT_EQ(METRIC_test_gauge.description(), mem_usage->prototype()->description());
  ASSERT_EQ(0, mem_usage->value());
  mem_usage->IncrementBy(7);
  ASSERT_EQ(7, mem_usage->value());
  mem_usage->set_value(5);
  ASSERT_EQ(5, mem_usage->value());
}

METRIC_DEFINE_gauge_int64(test_entity, test_func_gauge, "Test Function Gauge",
                          MetricUnit::kBytes, "Test Gauge 2");

static int64_t MyFunction(int* metric_val) {
  return (*metric_val)++;
}

TEST_F(MetricsTest, SimpleFunctionGaugeTest) {
  int metric_val = 1000;
  scoped_refptr<FunctionGauge<int64_t> > gauge =
    METRIC_test_func_gauge.InstantiateFunctionGauge(
      entity_, Bind(&MyFunction, Unretained(&metric_val)));

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
  int metric_val = 1000;
  scoped_refptr<FunctionGauge<int64_t> > gauge =
    METRIC_test_func_gauge.InstantiateFunctionGauge(
        entity_, Bind(&MyFunction, Unretained(&metric_val)));

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
  int metric_val = 1000;
  scoped_refptr<FunctionGauge<int64_t> > gauge =
    METRIC_test_func_gauge.InstantiateFunctionGauge(
        entity_, Bind(&MyFunction, Unretained(&metric_val)));

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

METRIC_DEFINE_gauge_uint64(test_entity, counter_as_gauge, "Gauge exposed as Counter",
                           MetricUnit::kBytes, "Gauge exposed as Counter",
                           EXPOSE_AS_COUNTER);
TEST_F(MetricsTest, TEstExposeGaugeAsCounter) {
  ASSERT_EQ(MetricType::kCounter, METRIC_counter_as_gauge.type());
}

METRIC_DEFINE_histogram(test_entity, test_hist, "Test Histogram",
                        MetricUnit::kMilliseconds, "foo", 1000000, 3);

TEST_F(MetricsTest, SimpleHistogramTest) {
  scoped_refptr<Histogram> hist = METRIC_test_hist.Instantiate(entity_);
  hist->Increment(2);
  hist->IncrementBy(4, 1);
  ASSERT_EQ(2, hist->histogram_->MinValue());
  ASSERT_EQ(3, hist->histogram_->MeanValue());
  ASSERT_EQ(4, hist->histogram_->MaxValue());
  ASSERT_EQ(2, hist->histogram_->TotalCount());
  ASSERT_EQ(6, hist->histogram_->TotalSum());
  // TODO: Test coverage needs to be improved a lot.
}

TEST_F(MetricsTest, JsonPrintTest) {
  scoped_refptr<Counter> test_counter = METRIC_test_counter.Instantiate(entity_);
  test_counter->Increment();
  scoped_refptr<AtomicGauge<uint64_t>> test_gauge = METRIC_test_gauge.Instantiate(entity_, 0);
  test_gauge->IncrementBy(2);
  entity_->SetAttribute("test_attr", "attr_val");

  // Generate the JSON.
  std::ostringstream out;
  JsonWriter writer(&out, JsonWriter::PRETTY);
  ASSERT_OK(entity_->WriteAsJson(&writer, MetricJsonOptions()));

  // Now parse it back out.
  JsonReader reader(out.str());
  ASSERT_OK(reader.Init());

  vector<const rapidjson::Value*> metrics;
  ASSERT_OK(reader.ExtractObjectArray(reader.root(), "metrics", &metrics));
  ASSERT_EQ(2, metrics.size());

  // Mapping metric_name-->metric_value
  unordered_map<string, int64_t> metric_map;
  string metric_name;
  int64_t metric_value;
  ASSERT_OK(reader.ExtractString(metrics[0], "name", &metric_name));
  ASSERT_OK(reader.ExtractInt64(metrics[0], "value", &metric_value));
  InsertOrDie(&metric_map, metric_name, metric_value);
  ASSERT_OK(reader.ExtractString(metrics[1], "name", &metric_name));
  ASSERT_OK(reader.ExtractInt64(metrics[1], "value", &metric_value));
  InsertOrDie(&metric_map, metric_name, metric_value);

  ASSERT_TRUE(ContainsKey(metric_map, "test_counter"));
  ASSERT_EQ(1L, metric_map["test_counter"]);
  ASSERT_TRUE(ContainsKey(metric_map, "test_gauge"));
  ASSERT_EQ(2L, metric_map["test_gauge"]);

  const rapidjson::Value* attributes;
  ASSERT_OK(reader.ExtractObject(reader.root(), "attributes", &attributes));
  string attr_value;
  ASSERT_OK(reader.ExtractString(attributes, "test_attr", &attr_value));
  ASSERT_EQ("attr_val", attr_value);

  // Verify that metric filtering matches on substrings.
  {
    out.str("");
    JsonWriter writer(&out, JsonWriter::PRETTY);
    MetricJsonOptions opts;
    opts.entity_metrics.emplace_back("test_count");
    ASSERT_OK(entity_->WriteAsJson(&writer, opts));
    ASSERT_STR_CONTAINS(out.str(), METRIC_test_counter.name());
    ASSERT_STR_NOT_CONTAINS(out.str(), METRIC_test_gauge.name());
  }

  // Verify that, if we filter for a metric that isn't in this entity, we get no result.
  {
    out.str("");
    JsonWriter writer(&out, JsonWriter::PRETTY);
    MetricJsonOptions opts;
    opts.entity_metrics.emplace_back("not_a_matching_metric");
    ASSERT_OK(entity_->WriteAsJson(&writer, opts));
    ASSERT_EQ(out.str(), "");
  }

  // Verify that filtering is case-insensitive.
  {
    out.str("");
    JsonWriter writer(&out, JsonWriter::PRETTY);
    MetricJsonOptions opts;
    opts.entity_metrics.emplace_back("teST_coUNteR");
    ASSERT_OK(entity_->WriteAsJson(&writer, opts));
    ASSERT_STR_CONTAINS(out.str(), METRIC_test_counter.name());
    ASSERT_STR_NOT_CONTAINS(out.str(), METRIC_test_gauge.name());
  }
}

// Test that metrics are retired when they are no longer referenced.
TEST_F(MetricsTest, RetirementTest) {
  FLAGS_metrics_retirement_age_ms = 100;

  const string kMetricName = "foo";
  scoped_refptr<Counter> counter = METRIC_test_counter.Instantiate(entity_);
  ASSERT_EQ(1, entity_->UnsafeMetricsMapForTests().size());

  // Since we hold a reference to the counter, it should not get retired.
  entity_->RetireOldMetrics();
  ASSERT_EQ(1, entity_->UnsafeMetricsMapForTests().size());

  // When we de-ref it, it should not get immediately retired, either, because
  // we keep retirable metrics around for some amount of time. We try retiring
  // a number of times to hit all the cases.
  counter = nullptr;
  for (int i = 0; i < 3; i++) {
    entity_->RetireOldMetrics();
    ASSERT_EQ(1, entity_->UnsafeMetricsMapForTests().size());
  }

  // If we wait for longer than the retirement time, and call retire again, we'll
  // actually retire it.
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_metrics_retirement_age_ms * 1.5));
  entity_->RetireOldMetrics();
  ASSERT_EQ(0, entity_->UnsafeMetricsMapForTests().size());
}

TEST_F(MetricsTest, TestRetiringEntities) {
  ASSERT_EQ(1, registry_.num_entities());

  // Drop the reference to our entity.
  entity_.reset();

  // Retire metrics. Since there is nothing inside our entity, it should
  // retire immediately (no need to loop).
  registry_.RetireOldMetrics();

  ASSERT_EQ(0, registry_.num_entities());
}

// Test that we can mark a metric to never be retired.
TEST_F(MetricsTest, NeverRetireTest) {
  entity_->NeverRetire(METRIC_test_hist.Instantiate(entity_));
  FLAGS_metrics_retirement_age_ms = 0;

  for (int i = 0; i < 3; i++) {
    entity_->RetireOldMetrics();
    ASSERT_EQ(1, entity_->UnsafeMetricsMapForTests().size());
  }
}

TEST_F(MetricsTest, TestInstantiatingTwice) {
  // Test that re-instantiating the same entity ID returns the same object.
  scoped_refptr<MetricEntity> new_entity = METRIC_ENTITY_test_entity.Instantiate(
      &registry_, entity_->id());
  ASSERT_EQ(new_entity.get(), entity_.get());
}

TEST_F(MetricsTest, TestInstantiatingDifferentEntities) {
  scoped_refptr<MetricEntity> new_entity = METRIC_ENTITY_test_entity.Instantiate(
      &registry_, "some other ID");
  ASSERT_NE(new_entity.get(), entity_.get());
}

TEST_F(MetricsTest, TestDumpJsonPrototypes) {
  // Dump the prototype info.
  std::ostringstream out;
  JsonWriter w(&out, JsonWriter::PRETTY);
  MetricPrototypeRegistry::get()->WriteAsJson(&w);
  string json = out.str();

  // Quick sanity check for one of our metrics defined in this file.
  const char* expected =
    "        {\n"
    "            \"name\": \"test_func_gauge\",\n"
    "            \"label\": \"Test Function Gauge\",\n"
    "            \"type\": \"gauge\",\n"
    "            \"unit\": \"bytes\",\n"
    "            \"description\": \"Test Gauge 2\",\n"
    "            \"entity_type\": \"test_entity\"\n"
    "        }";
  ASSERT_STR_CONTAINS(json, expected);

  // Parse it.
  rapidjson::Document d;
  d.Parse<0>(json.c_str());

  // Ensure that we got a reasonable number of metrics.
  int num_metrics = d["metrics"].Size();
  int num_entities = d["entities"].Size();
  LOG(INFO) << "Parsed " << num_metrics << " metrics and " << num_entities << " entities";
  ASSERT_GT(num_metrics, 5);
  ASSERT_EQ(num_entities, 2);

  // Spot-check that some metrics were properly registered and that the JSON was properly
  // formed.
  unordered_set<string> seen_metrics;
  for (int i = 0; i < d["metrics"].Size(); i++) {
    InsertOrDie(&seen_metrics, d["metrics"][i]["name"].GetString());
  }
  ASSERT_TRUE(ContainsKey(seen_metrics, "threads_started"));
  ASSERT_TRUE(ContainsKey(seen_metrics, "test_hist"));
}

TEST_F(MetricsTest, TestDumpOnlyChanged) {
  auto GetJson = [&](int64_t since_epoch) {
    MetricJsonOptions opts;
    opts.only_modified_in_or_after_epoch = since_epoch;
    std::ostringstream out;
    JsonWriter writer(&out, JsonWriter::COMPACT);
    CHECK_OK(entity_->WriteAsJson(&writer, opts));
    return out.str();
  };

  scoped_refptr<Counter> test_counter = METRIC_test_counter.Instantiate(entity_);

  int64_t epoch_when_modified = Metric::current_epoch();
  test_counter->Increment();

  // If we pass a "since dirty" epoch from before we incremented it, we should
  // see the metric.
  for (int i = 0; i < 2; i++) {
    ASSERT_STR_CONTAINS(GetJson(epoch_when_modified), "{\"name\":\"test_counter\",\"value\":1}");
    Metric::IncrementEpoch();
  }

  // If we pass a current epoch, we should see that the metric was not modified.
  int64_t new_epoch = Metric::current_epoch();
  ASSERT_STR_NOT_CONTAINS(GetJson(new_epoch), "test_counter");
  // ... until we modify it again.
  test_counter->Increment();
  ASSERT_STR_CONTAINS(GetJson(new_epoch), "{\"name\":\"test_counter\",\"value\":2}");
}


// Test that 'include_untouched_metrics=false' prevents dumping counters and histograms
// which have never been incremented.
TEST_F(MetricsTest, TestDontDumpUntouched) {
  // Instantiate a bunch of metrics.
  int metric_val = 1000;
  scoped_refptr<Counter> test_counter = METRIC_test_counter.Instantiate(entity_);
  scoped_refptr<Histogram> hist = METRIC_test_hist.Instantiate(entity_);
  scoped_refptr<FunctionGauge<int64_t> > function_gauge =
    METRIC_test_func_gauge.InstantiateFunctionGauge(
        entity_, Bind(&MyFunction, Unretained(&metric_val)));
  scoped_refptr<AtomicGauge<uint64_t> > atomic_gauge =
    METRIC_test_gauge.Instantiate(entity_, 0);

  MetricJsonOptions opts;
  opts.include_untouched_metrics = false;
  std::ostringstream out;
  JsonWriter writer(&out, JsonWriter::COMPACT);
  CHECK_OK(entity_->WriteAsJson(&writer, opts));
  // Untouched counters and histograms should not be included.
  ASSERT_STR_NOT_CONTAINS(out.str(), "test_counter");
  ASSERT_STR_NOT_CONTAINS(out.str(), "test_hist");
  // Untouched gauges need to be included, because we don't actually
  // track whether they have been touched.
  ASSERT_STR_CONTAINS(out.str(), "test_func_gauge");
  ASSERT_STR_CONTAINS(out.str(), "test_gauge");
}

TEST_F(MetricsTest, TestFilter) {
  const int32_t kNum = 4;
  vector<string> id_uuids;
  const string attr1 = "attr1";
  const string attr2 = "attr2";
  vector<string> attr1_uuids;
  vector<string> attr2_uuids;

  // 1.Generate metrics.
  ObjectIdGenerator oid;
  for (int i = 0; i < kNum; ++i) {
    string id_uuid = oid.Next();
    string attr1_uuid = oid.Next();
    string attr2_uuid = oid.Next();

    MetricEntity::AttributeMap attrs;
    attrs[attr1] = attr1_uuid;
    attrs[attr2] = attr2_uuid;
    scoped_refptr<MetricEntity> entity =
        METRIC_ENTITY_test_entity.Instantiate(&registry_, id_uuid, attrs);
    scoped_refptr<Counter> metric1 = METRIC_test_counter.Instantiate(entity);
    scoped_refptr<AtomicGauge<uint64_t>> metric2 = METRIC_test_gauge.Instantiate(entity, 0);

    id_uuids.emplace_back(id_uuid);
    attr1_uuids.emplace_back(attr1_uuid);
    attr2_uuids.emplace_back(attr2_uuid);
  }

  // 2.Check the filter.
  Random rand(SeedRandom());
  const string not_exist_string = "not_exist_string";

  // 2.1 Filter the 'type'.
  {
    {
      std::ostringstream out;
      MetricJsonOptions opts;
      opts.entity_types = { not_exist_string };
      JsonWriter w(&out, JsonWriter::PRETTY);
      ASSERT_OK(registry_.WriteAsJson(&w, opts));
      ASSERT_EQ("[]", out.str());
    }
    {
      const string entity_type = "test_entity";
      std::ostringstream out;
      MetricJsonOptions opts;
      opts.entity_types = { entity_type };
      JsonWriter w(&out, JsonWriter::PRETTY);
      ASSERT_OK(registry_.WriteAsJson(&w, opts));
      rapidjson::Document d;
      d.Parse<0>(out.str().c_str());
      ASSERT_EQ(kNum + 1, d.Size());
      ASSERT_EQ(entity_type, d[rand.Next() % kNum]["type"].GetString());
    }
  }
  // 2.2 Filter the 'id'.
  {
    {
      std::ostringstream out;
      MetricJsonOptions opts;
      opts.entity_ids = { not_exist_string };
      JsonWriter w(&out, JsonWriter::PRETTY);
      ASSERT_OK(registry_.WriteAsJson(&w, opts));
      ASSERT_EQ("[]", out.str());
    }
    {
      const string& entity_id = id_uuids[rand.Next() % kNum];
      std::ostringstream out;
      MetricJsonOptions opts;
      opts.entity_ids = { entity_id };
      JsonWriter w(&out, JsonWriter::PRETTY);
      ASSERT_OK(registry_.WriteAsJson(&w, opts));
      rapidjson::Document d;
      d.Parse<0>(out.str().c_str());
      ASSERT_EQ(1, d.Size());
      ASSERT_EQ(entity_id, d[0]["id"].GetString());
    }
  }
  // 2.3 Filter the 'attributes'.
  {
    {
      std::ostringstream out;
      MetricJsonOptions opts;
      opts.entity_attrs = { attr1, not_exist_string };
      JsonWriter w(&out, JsonWriter::PRETTY);
      ASSERT_OK(registry_.WriteAsJson(&w, opts));
      ASSERT_EQ("[]", out.str());
    }
    {
      int i = rand.Next() % kNum;
      const string& attr1_uuid = attr1_uuids[i];
      const string& attr2_uuid = attr2_uuids[i];
      std::ostringstream out;
      MetricJsonOptions opts;
      opts.entity_attrs = { attr1, attr1_uuid };
      JsonWriter w(&out, JsonWriter::PRETTY);
      ASSERT_OK(registry_.WriteAsJson(&w, opts));
      rapidjson::Document d;
      d.Parse<0>(out.str().c_str());
      ASSERT_EQ(1, d.Size());
      ASSERT_EQ(attr1_uuid, d[0]["attributes"]["attr1"].GetString());
      ASSERT_EQ(attr2_uuid, d[0]["attributes"]["attr2"].GetString());
    }
  }
  // 2.4 Filter the 'metrics'.
  {
    {
      std::ostringstream out;
      MetricJsonOptions opts;
      opts.entity_metrics = { not_exist_string };
      JsonWriter w(&out, JsonWriter::PRETTY);
      ASSERT_OK(registry_.WriteAsJson(&w, opts));
      ASSERT_EQ("[]", out.str());
    }
    {
      const string entity_metric1 = "test_counter";
      const string entity_metric2 = "test_gauge";
      std::ostringstream out;
      MetricJsonOptions opts;
      opts.entity_metrics = { entity_metric1 };
      JsonWriter w(&out, JsonWriter::PRETTY);
      ASSERT_OK(registry_.WriteAsJson(&w, opts));
      ASSERT_STR_CONTAINS(out.str(), entity_metric1);
      ASSERT_STR_NOT_CONTAINS(out.str(), entity_metric2);
      rapidjson::Document d;
      d.Parse<0>(out.str().c_str());
      ASSERT_EQ(kNum, d.Size());
    }
  }
  // 2.5 Default filter condition.
  {
    std::ostringstream out;
    JsonWriter w(&out, JsonWriter::PRETTY);
    ASSERT_OK(registry_.WriteAsJson(&w, MetricJsonOptions()));
    rapidjson::Document d;
    d.Parse<0>(out.str().c_str());
    ASSERT_EQ(kNum + 1, d.Size());
  }
}

} // namespace kudu
