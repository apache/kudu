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
    case kRows:
      return "rows";
    case kProbes:
      return "probes";
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

template<typename T>
T* MetricRegistry::FindMetricUnlocked(const std::string& name,
                                      MetricType::Type metric_type) {
  Metric* metric = FindPtrOrNull(metrics_, name);
  if (metric != NULL) {
    CHECK_EQ(MetricType::kCounter, metric->type())
      << "Downcast expects " << MetricType::Name(MetricType::kCounter)
      << " but found " << MetricType::Name(metric->type());
    return down_cast<T*>(metric);
  }
  return NULL;
}

Counter* MetricRegistry::FindOrCreateCounter(const std::string& name,
                                             const CounterPrototype& proto) {
  boost::lock_guard<simple_spinlock> l(lock_);
  Counter* counter = FindMetricUnlocked<Counter>(name, MetricType::kCounter);
  if (!counter) {
    counter = new Counter(proto);
    InsertOrDie(&metrics_, name, counter);
  }
  return counter;
}

template<typename T>
Gauge* MetricRegistry::CreateGauge(const std::string& name,
                                   const GaugePrototype<T>& proto,
                                   const T& initial_value) {
  return new AtomicGauge<T>(proto, initial_value);
}

// Specialization for StringGauge.
template<>
Gauge* MetricRegistry::CreateGauge(const std::string& name,
                                   const GaugePrototype<std::string>& proto,
                                   const std::string& initial_value) {
  return new StringGauge(proto, initial_value);
}

template<typename T>
Gauge* MetricRegistry::CreateFunctionGauge(const std::string& name,
                                           const GaugePrototype<T>& proto,
                                           const boost::function<T()>& function) {
  return new FunctionGauge<T>(proto, function);
}

template<typename T>
Gauge* MetricRegistry::FindOrCreateGauge(const std::string& name,
                                         const GaugePrototype<T>& proto,
                                         const T& initial_value) {
  boost::lock_guard<simple_spinlock> l(lock_);
  Gauge* gauge = FindMetricUnlocked<Gauge>(name, MetricType::kGauge);
  if (!gauge) {
    gauge = CreateGauge(name, proto, initial_value);
    InsertOrDie(&metrics_, name, gauge);
  }
  return gauge;
}

// Explicit instantiation.
template Gauge* MetricRegistry::FindOrCreateGauge<bool>(
    const std::string&, const GaugePrototype<bool>&, const bool&);
template Gauge* MetricRegistry::FindOrCreateGauge<int32_t>(
    const std::string&, const GaugePrototype<int32_t>&, const int32_t&);
template Gauge* MetricRegistry::FindOrCreateGauge<uint32_t>(
    const std::string&, const GaugePrototype<uint32_t>&, const uint32_t&);
template Gauge* MetricRegistry::FindOrCreateGauge<int64_t>(
    const std::string&, const GaugePrototype<int64_t>&, const int64_t&);
template Gauge* MetricRegistry::FindOrCreateGauge<uint64_t>(
    const std::string&, const GaugePrototype<uint64_t>&, const uint64_t&);
template Gauge* MetricRegistry::FindOrCreateGauge<double>(
    const std::string&, const GaugePrototype<double>&, const double&);
template Gauge* MetricRegistry::FindOrCreateGauge<string>(
    const std::string&, const GaugePrototype<string>&, const string&);

template<typename T>
Gauge* MetricRegistry::FindOrCreateFunctionGauge(const std::string& name,
                                                 const GaugePrototype<T>& proto,
                                                 const boost::function<T()>& function) {
  boost::lock_guard<simple_spinlock> l(lock_);
  Gauge* gauge = FindMetricUnlocked<Gauge>(name, MetricType::kGauge);
  if (!gauge) {
    gauge = CreateFunctionGauge(name, proto, function);
    InsertOrDie(&metrics_, name, gauge);
  }
  return gauge;
}

// Explicit instantiation.
template Gauge* MetricRegistry::FindOrCreateFunctionGauge<bool>(
    const std::string&, const GaugePrototype<bool>&, const boost::function<bool()>&);
template Gauge* MetricRegistry::FindOrCreateFunctionGauge<int32_t>(
    const std::string&, const GaugePrototype<int32_t>&, const boost::function<int32_t()>&);
template Gauge* MetricRegistry::FindOrCreateFunctionGauge<uint32_t>(
    const std::string&, const GaugePrototype<uint32_t>&, const boost::function<uint32_t()>&);
template Gauge* MetricRegistry::FindOrCreateFunctionGauge<int64_t>(
    const std::string&, const GaugePrototype<int64_t>&, const boost::function<int64_t()>&);
template Gauge* MetricRegistry::FindOrCreateFunctionGauge<uint64_t>(
    const std::string&, const GaugePrototype<uint64_t>&, const boost::function<uint64_t()>&);
template Gauge* MetricRegistry::FindOrCreateFunctionGauge<double>(
    const std::string&, const GaugePrototype<double>&, const boost::function<double()>&);
template Gauge* MetricRegistry::FindOrCreateFunctionGauge<string>(
    const std::string&, const GaugePrototype<string>&, const boost::function<string()>&);

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
  writer->String(MetricUnit::Name(unit()));

  writer->String("description");
  writer->String(description());

  writer->EndObject();
  return Status::OK();
}

//
// StringGauge
//

StringGauge::StringGauge(const GaugePrototype<string>& proto,
                         const string& initial_value)
  : Gauge(proto.unit(), proto.description()),
    value_(initial_value) {
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
// Counter
//
// This implementation can be optimized in several ways.
// See: https://github.com/codahale/metrics/blob/master/metrics-core/src/main/java/com/codahale/metrics/Striped64.java
// See: http://gee.cs.oswego.edu/dl/jsr166/dist/docs/java/util/concurrent/atomic/LongAdder.html
// See: http://www.cs.bgu.ac.il/~hendlerd/papers/flat-combining.pdf
// Or: use thread locals and then sum them

Counter* CounterPrototype::Instantiate(const MetricContext& context) {
  return context.metrics()->FindOrCreateCounter(
    context.prefix() + "." + name_, *this);
}

Counter::Counter(const CounterPrototype& proto)
  : value_(0),
    unit_(proto.unit()),
    description_(proto.description()) {
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

} // namespace kudu
