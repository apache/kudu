// Copyright (c) 2013, Cloudera, inc.
#include "util/metrics.h"

#include <map>

#include <boost/foreach.hpp>
#include <boost/thread/mutex.hpp>

#include "gutil/atomicops.h"
#include "gutil/casts.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "util/jsonwriter.h"
#include "util/locks.h"
#include "util/status.h"

namespace kudu {

using std::string;
using std::vector;
using std::tr1::unordered_map;

//
// MetricUnit
//

const char* MetricUnit::Name(Type unit) {
  switch (unit) {
    case kBytes:
      return "bytes";
    case kRequests:
      return "requests";
    default:
      return "UNKNOWN UNIT";
  }
}

//
// MetricType
//

const char* const MetricType::kGaugeType = "gauge";
const char* const MetricType::kCounterType = "counter";
const char* MetricType::Name(MetricType::Type type) {
  switch (type) {
    case kGauge:
      return kGaugeType;
    case kCounter:
      return kCounterType;
    default:
      return "UNKNOWN TYPE";
  }
}

//
// MetricRegistry
//

MetricRegistry::MetricRegistry() {
}

MetricRegistry::~MetricRegistry() {
  STLDeleteValues(&metrics_);
}

Counter* MetricRegistry::FindOrCreateCounter(const string& name, MetricUnit::Type unit,
    const string& description) {
  boost::lock_guard<simple_spinlock> l(lock_);
  Metric* metric = FindPtrOrNull(metrics_, name);
  if (metric != NULL) {
    CHECK_EQ(MetricType::kCounter, metric->type())
      << "Downcast expects " << MetricType::Name(MetricType::kCounter)
      << " but found " << MetricType::Name(metric->type());
    return down_cast<Counter*>(metric);
  }
  Counter* counter = new Counter(unit, description);
  InsertOrDie(&metrics_, name, counter);
  return counter;
}

Status MetricRegistry::WriteAsJson(JsonWriter* writer) const {
  // We want the keys to be in alphabetical order when printing, so we use an ordered map here.
  typedef std::map<string, Metric*> OrderedMetricMap;
  OrderedMetricMap metrics;
  {
    // Snapshot the metrics in this registry (not guaranteed to be a consistent snapshot)
    boost::lock_guard<simple_spinlock> l(lock_);
    BOOST_FOREACH(const UnorderedMetricMap::value_type& val, metrics_) {
      metrics.insert(val);
    }
  }
  writer->StartObject();

  writer->String("metrics");
  writer->StartArray();
  BOOST_FOREACH(OrderedMetricMap::value_type& val, metrics) {
    val.second->WriteAsJson(val.first, writer);
  }
  writer->EndArray();

  writer->EndObject();
  return Status::OK();
}

//
// MetricContext
//

MetricContext::MetricContext(const MetricContext& parent)
  : metrics_(parent.metrics_),
    prefix_(parent.prefix_) {
}

MetricContext::MetricContext(MetricRegistry* metrics, const string& root_name)
  : metrics_(metrics),
    prefix_(root_name) {
}

MetricContext::MetricContext(const MetricContext& parent, const string& name)
  : metrics_(parent.metrics_),
    prefix_(parent.prefix_ + "." + name) {
}

//
// Gauge
//

Status Gauge::WriteAsJson(const string& name, JsonWriter* writer) const {
  writer->StartObject();

  writer->String("type");
  writer->String(MetricType::Name(type()));

  writer->String("name");
  writer->String(name);

  writer->String("value");
  WriteValue(writer);

  writer->String("unit");
  writer->String(unit());

  writer->String("description");
  writer->String(description());

  writer->EndObject();
  return Status::OK();
}

//
// StringGauge
//

StringGauge::StringGauge(const std::string& unit, const std::string& description)
  : value_(),
    unit_(unit),
    description_(description) {
}

std::string StringGauge::value() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return value_;
}

void StringGauge::set_value(const std::string& value) {
  boost::lock_guard<simple_spinlock> l(lock_);
  value_ = value;
}

void StringGauge::WriteValue(JsonWriter* writer) const {
  writer->String(value());
}

//
// AtomicGauge
//

// Specializations for common primitive metric types.
template<> void AtomicGauge<bool>::WriteSpecificValue(JsonWriter* writer, bool val) {
  writer->Bool(val);
}
template<> void AtomicGauge<int64_t>::WriteSpecificValue(JsonWriter* writer, int64_t val) {
  writer->Int64(val);
}
template<> void AtomicGauge<double>::WriteSpecificValue(JsonWriter* writer, double val) {
  writer->Double(val);
}

//
// Counter
//
// This implementation can be optimized in several ways.
// See: https://github.com/codahale/metrics/blob/master/metrics-core/src/main/java/com/codahale/metrics/Striped64.java
// See: http://gee.cs.oswego.edu/dl/jsr166/dist/docs/java/util/concurrent/atomic/LongAdder.html
// See: http://www.cs.bgu.ac.il/~hendlerd/papers/flat-combining.pdf
// Or: use thread locals and then sum them

Counter::Counter(MetricUnit::Type unit, const std::string& description)
  : value_(),
    unit_(unit),
    description_(description) {
}

int64_t Counter::value() const {
  return base::subtle::NoBarrier_Load(&value_);
}

void Counter::set_value(int64_t value) {
  base::subtle::NoBarrier_Store(&value_, value);
}

void Counter::Increment() {
  IncrementBy(1);
}

void Counter::IncrementBy(int64_t amount) {
  base::subtle::NoBarrier_AtomicIncrement(&value_, amount);
}

void Counter::Decrement() {
  DecrementBy(1);
}

void Counter::DecrementBy(int64_t amount) {
  IncrementBy(-amount);
}

Status Counter::WriteAsJson(const string& name, JsonWriter* writer) const {
  writer->StartObject();

  writer->String("type");
  writer->String(MetricType::Name(type()));

  writer->String("name");
  writer->String(name);

  writer->String("value");
  writer->Int64(value());

  writer->String("unit");
  writer->String(MetricUnit::Name(unit_));

  writer->String("description");
  writer->String(description_);

  writer->EndObject();
  return Status::OK();
}

Counter* FindOrCreateCounter(const MetricContext& context, const CounterPrototype& proto) {
  return context.metrics()->FindOrCreateCounter(context.prefix() + "." + proto.name(),
    proto.unit(), proto.description());
}

} // namespace kudu
