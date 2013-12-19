// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_METRICS_H
#define KUDU_UTIL_METRICS_H

/////////////////////////////////////////////////////
// Kudu Metrics
/////////////////////////////////////////////////////
//
// =======
// Summary
// =======
//
// This API provides a basic set of metrics primitives along the lines of the Code Hale's
// metrics library along with JSON formatted output of running metrics. Metric "prototypes"
// are defined using macros and then instantiated based on a particular MetricContext.
// A MetricContext allows for organizing metrics into hierarchies so that common code,
// such as a library class, can maintain separate counters for instances created from
// different code paths if desired.
//
// ==============
// Class Overview
// ==============
//
// MetricRegistry: Set of all metrics for a top-level service.
// MetricContext: The context within which metrics are instantiated. A context contains a
// pointer to a MetricRegistry as well as a prefix for metric keys which is appended to by
// child contexts.
// Metric: Base class that all metrics inherit from. Knows how to output JSON and has a type.
// Gauge: Set or get a point-in-time value.
//  - string: Gauge for a string value.
//  - Primitive types (bool, int64_t/uint64_t, double): Lock-free gauges.
// Counter: Get, reset, increment or decrement an int64_t value.
// Histogram: Increment buckets of values segmented by configurable max and precision.
//
// TODO: Implement Meter, Timer.
//
// =============
// Example usage
// =============
//
//   METRIC_DEFINE_counter(ping_requests, kudu::MetricUnit::kRequests,
//       "Number of Ping() RPC requests this server has handled since service start");
//
//   Counter* ping_requests_ = METRIC_ping_requests.Instantiate(metric_context_);
//   ping_requests_->Increment();
//
// Using the above API, you can pass a MetricRegistry to subsystems so that they can register
// individual counters with their specific MetricRegistry and thus have separate subsystems
// with the same counter names.
//
// ===========
// JSON output
// ===========
//
// The first-class output format for metrics is pretty-printed JSON.
// Such a format is relatively easy for humans and machines to read.
// TODO: Output to HTML.
//
// Example JSON output:
//
// {
//   "metrics": [
//     {
//       "type": "gauge",
//       "name": "kudu.tabletserver.start_time",
//       "value": 1380133734,
//       "unit": "epoch seconds",
//       "description": "Time the service started"
//     },
//     {
//       "type": "counter",
//       "name": "kudu.tabletserver.ping_requests",
//       "value": 0,
//       "unit": "requests",
//       "description": "Ping() requests handled since service start"
//     },
//   ],
// }
//
/////////////////////////////////////////////////////

#include <boost/function.hpp>
#include <string>
#include <tr1/unordered_map>

#include <gtest/gtest.h>

#include "gutil/atomicops.h"
#include "util/jsonwriter.h"
#include "util/locks.h"
#include "util/status.h"

// Convenience macros.
#define METRIC_DEFINE_counter(name, unit, desc) \
  ::kudu::CounterPrototype METRIC_##name(#name, unit, desc)

#define METRIC_DECLARE_counter(name) \
  extern ::kudu::CounterPrototype METRIC_##name

#define METRIC_DEFINE_gauge_string(name, unit, desc) \
  ::kudu::GaugePrototype<std::string> METRIC_##name(#name, unit, desc)
#define METRIC_DEFINE_gauge_bool(name, unit, desc) \
  ::kudu::GaugePrototype<bool> METRIC_##name(#name, unit, desc)
#define METRIC_DEFINE_gauge_int32(name, unit, desc) \
  ::kudu::GaugePrototype<int32_t> METRIC_##name(#name, unit, desc)
#define METRIC_DEFINE_gauge_uint32(name, unit, desc) \
  ::kudu::GaugePrototype<uint32_t> METRIC_##name(#name, unit, desc)
#define METRIC_DEFINE_gauge_int64(name, unit, desc) \
  ::kudu::GaugePrototype<int64_t> METRIC_##name(#name, unit, desc)
#define METRIC_DEFINE_gauge_uint64(name, unit, desc) \
  ::kudu::GaugePrototype<uint64_t> METRIC_##name(#name, unit, desc)
#define METRIC_DEFINE_gauge_double(name, unit, desc) \
  ::kudu::GaugePrototype<double> METRIC_##name(#name, unit, desc)

#define METRIC_DECLARE_gauge_string(name) \
  extern ::kudu::GaugePrototype<std::string> METRIC_##name
#define METRIC_DECLARE_gauge_bool(name) \
  extern ::kudu::GaugePrototype<bool> METRIC_##name
#define METRIC_DECLARE_gauge_int32(name) \
  extern ::kudu::GaugePrototype<int32_t> METRIC_##name
#define METRIC_DECLARE_gauge_uint32(name) \
  extern ::kudu::GaugePrototype<uint32_t> METRIC_##name
#define METRIC_DECLARE_gauge_int64(name) \
  extern ::kudu::GaugePrototype<int64_t> METRIC_##name
#define METRIC_DECLARE_gauge_uint64(name) \
  extern ::kudu::GaugePrototype<uint64_t> METRIC_##name
#define METRIC_DECLARE_gauge_double(name) \
  extern ::kudu::GaugePrototype<double> METRIC_##name

#define METRIC_DEFINE_histogram(name, unit, desc, max_val, num_sig_digits) \
  ::kudu::HistogramPrototype METRIC_##name(#name, unit, desc, max_val, num_sig_digits)

#define METRIC_DECLARE_histogram(name) \
  extern ::kudu::HistogramPrototype METRIC_##name

namespace kudu {

class Counter;
class CounterPrototype;

class Gauge;
template<typename T>
class GaugePrototype;

class HdrHistogram;
class Histogram;
class HistogramPrototype;

class MetricContext;

// Unit types to be used with metrics.
// As additional units are required, add them to this enum and also to Name().
struct MetricUnit {
  enum Type {
    kBytes,
    kRequests,
    kRows,
    kProbes,
    kNanoseconds,
    kMicroseconds,
    kMilliseconds,
    kSeconds,
  };
  static const char* Name(Type unit);
};

class MetricType {
 public:
  enum Type { kGauge, kCounter, kHistogram };
  static const char* Name(Type t);
 private:
  static const char* const kGaugeType;
  static const char* const kCounterType;
  static const char* const kHistogramType;
};

// Base class to allow for putting all metrics into a single container.
class Metric {
 public:
  virtual ~Metric() {}
  // All metrics must be able to render themselves as JSON.
  virtual Status WriteAsJson(const std::string& name, JsonWriter* writer) const = 0;
  virtual MetricType::Type type() const = 0;
 protected:
  Metric() {}
 private:
  DISALLOW_COPY_AND_ASSIGN(Metric);
};

// Registry of all the metrics for a given subsystem.
class MetricRegistry {
 public:
  typedef std::tr1::unordered_map<string, Metric*> UnorderedMetricMap;

  MetricRegistry();
  ~MetricRegistry();
  Status WriteAsJson(JsonWriter* writer) const;

  Counter* FindOrCreateCounter(const std::string& name,
                               const CounterPrototype& proto);

  template<typename T>
  Gauge* FindOrCreateGauge(const std::string& name,
                           const GaugePrototype<T>& proto,
                           const T& initial_value);

  template<typename T>
  Gauge* FindOrCreateFunctionGauge(const std::string& name,
                                   const GaugePrototype<T>& proto,
                                   const boost::function<T()>& function);

  Histogram* FindOrCreateHistogram(const std::string& name,
                                   const HistogramPrototype& proto);

  // Not thread-safe, used for tests.
  const UnorderedMetricMap& UnsafeMetricsMapForTests() const { return metrics_; }

 private:
  friend class MultiThreadedMetricsTest;  // For unit testing.
  FRIEND_TEST(MetricsTest, JsonPrintTest);

  // Attempt to find metric in map and downcast it to specified template type.
  // Returns NULL if the named metric is not found.
  // Must be called while holding the registry lock.
  template<typename T>
  T* FindMetricUnlocked(const std::string& name,
                        MetricType::Type metric_type);

  template<typename T>
  Gauge* CreateGauge(const std::string& name,
                     const GaugePrototype<T>& proto,
                     const T& initial_value);

  template<typename T>
  Gauge* CreateFunctionGauge(const std::string& name,
                             const GaugePrototype<T>& proto,
                             const boost::function<T()>& function);

  UnorderedMetricMap metrics_;
  mutable simple_spinlock lock_;
  DISALLOW_COPY_AND_ASSIGN(MetricRegistry);
};

// Carries the metric registry and key hierarchy through the call stack.
// An instance of this object should be plumbed into classes that want to be instrumented.
//
// We allow copy (not assign) of MetricContext so that classes may easily pass it to their
// child objects without appending to the prefix hierarchy.
class MetricContext {
 public:
  // Copy constructor.
  explicit MetricContext(const MetricContext& parent);

  // Create a "root" MetricContext based on a MetricRegistry.
  MetricContext(MetricRegistry* metrics, const std::string& root_name);

  // Create a "child" MetricContext based on a parent context.
  MetricContext(const MetricContext& parent, const std::string& name);

  MetricRegistry* metrics() const { return metrics_; }
  const std::string& prefix() const { return prefix_; }
 private:
  void operator=(const MetricContext& context); // Not assignable

  MetricRegistry* metrics_;
  const std::string prefix_;
};

// A description of a Gauge.
template<typename T>
class GaugePrototype {
 public:
  GaugePrototype(const std::string& name,
                 MetricUnit::Type unit, const std::string& description)
    : name_(name),
      unit_(unit),
      description_(description) {
  }

  const std::string& name() const { return name_; }
  MetricUnit::Type unit() const { return unit_; }
  const std::string& description() const { return description_; }

  // Instantiate a "manual" gauge.
  Gauge* Instantiate(const MetricContext& context,
                     const T& initial_value) {
    return context.metrics()->FindOrCreateGauge(
        context.prefix() + "." + name_, *this, initial_value);
  }

  // Instantiate a gauge that is backed by the given callback.
  Gauge* InstantiateFunctionGauge(const MetricContext& context,
                                  const boost::function<T()>& function) {
    return context.metrics()->FindOrCreateFunctionGauge(
        context.prefix() + "." + name_, *this, function);
  }

 private:
  const std::string name_;
  const MetricUnit::Type unit_;
  const std::string description_;
  DISALLOW_COPY_AND_ASSIGN(GaugePrototype);
};

// Abstract base class to provide point-in-time metric values.
class Gauge : public Metric {
 public:
  Gauge(const MetricUnit::Type& unit, const std::string& description)
    : unit_(unit),
      description_(description) {
  }
  virtual ~Gauge() {}
  virtual MetricType::Type type() const OVERRIDE { return MetricType::kGauge; }
  virtual const MetricUnit::Type& unit() const { return unit_; }
  virtual const std::string& description() const { return description_; }
  virtual Status WriteAsJson(const std::string& name, JsonWriter* w) const OVERRIDE;
 protected:
  virtual void WriteValue(JsonWriter* writer) const = 0;
  const MetricUnit::Type unit_;
  const std::string description_;
 private:
  DISALLOW_COPY_AND_ASSIGN(Gauge);
};

// Gauge implementation for string that uses locks to ensure thread safety.
class StringGauge : public Gauge {
 public:
  StringGauge(const GaugePrototype<std::string>& proto, const std::string& initial_value);
  std::string value() const;
  void set_value(const std::string& value);
 protected:
  virtual void WriteValue(JsonWriter* writer) const OVERRIDE;
 private:
  std::string value_;
  mutable simple_spinlock lock_;  // Guards value_
  DISALLOW_COPY_AND_ASSIGN(StringGauge);
};

// Lock-free implementation for types that are convertible to/from Atomic64.
template <typename T>
class AtomicGauge : public Gauge {
 public:
  AtomicGauge(const GaugePrototype<T>& proto, T initial_value)
    : Gauge(proto.unit(), proto.description()),
      value_(initial_value) {
  }
  T value() const {
    return static_cast<T>(base::subtle::Release_Load(&value_));
  }
  void set_value(const T& value) {
    base::subtle::NoBarrier_Store(&value_, static_cast<base::subtle::Atomic64>(value));
  }
 protected:
  virtual void WriteValue(JsonWriter* writer) const OVERRIDE {
    writer->Value(value());
  }
 private:
  base::subtle::Atomic64 value_;
  DISALLOW_COPY_AND_ASSIGN(AtomicGauge);
};

// A Gauge that calls back to a function to get its value.
template <typename T>
class FunctionGauge : public Gauge {
 public:
  FunctionGauge(const GaugePrototype<T>& proto,
                const boost::function<T()>& function)
    : Gauge(proto.unit(), proto.description()),
      function_(function) {
  }
  T value() const {
    return function_();
  }
  virtual void WriteValue(JsonWriter* writer) const OVERRIDE {
    writer->Value(value());
  }
 private:
  boost::function<T()> function_;
  DISALLOW_COPY_AND_ASSIGN(FunctionGauge);
};

// Prototype for a counter.
class CounterPrototype {
 public:
  CounterPrototype(const std::string& name, MetricUnit::Type unit,
      const std::string& description)
    : name_(name),
      unit_(unit),
      description_(description) {
  }
  const std::string& name() const { return name_; }
  MetricUnit::Type unit() const { return unit_; }
  const std::string& description() const { return description_; }

  Counter* Instantiate(const MetricContext& context);

 private:
  const std::string name_;
  const MetricUnit::Type unit_;
  const std::string description_;
  DISALLOW_COPY_AND_ASSIGN(CounterPrototype);
};

// Simple incrementing and decrementing 64-bit integer.
class Counter : public Metric {
 public:
  int64_t value() const;
  void set_value(int64_t value);
  void Increment();
  void IncrementBy(int64_t amount);
  void Decrement();
  void DecrementBy(int64_t amount);
  const std::string& description() const { return description_; }
  virtual MetricType::Type type() const OVERRIDE { return MetricType::kCounter; }
  virtual Status WriteAsJson(const std::string& name, JsonWriter* w) const OVERRIDE;

 private:
  friend class MetricRegistry;
  FRIEND_TEST(MetricsTest, SimpleCounterTest);
  FRIEND_TEST(MultiThreadedMetricsTest, CounterIncrementTest);

  explicit Counter(const CounterPrototype& proto);

  base::subtle::Atomic64 value_;
  const MetricUnit::Type unit_;
  const std::string description_;
  DISALLOW_COPY_AND_ASSIGN(Counter);
};

class HistogramPrototype {
 public:
  HistogramPrototype(const std::string& name, MetricUnit::Type unit,
                     const std::string& description,
                     uint64_t max_trackable_value, int num_sig_digits);
  Histogram* Instantiate(const MetricContext& context);
  const std::string& name() const { return name_; }
  MetricUnit::Type unit() const { return unit_; }
  const std::string& description() const { return description_; }
  uint64_t max_trackable_value() const { return max_trackable_value_; }
  int num_sig_digits() const { return num_sig_digits_; }

 private:
  const std::string name_;
  const MetricUnit::Type unit_;
  const std::string description_;
  const uint64_t max_trackable_value_;
  const int num_sig_digits_;
  DISALLOW_COPY_AND_ASSIGN(HistogramPrototype);
};

class Histogram : public Metric {
 public:
  void Increment(uint64_t value);
  void IncrementBy(uint64_t value, uint64_t amount);

  const std::string& description() const { return description_; }
  virtual MetricType::Type type() const OVERRIDE { return MetricType::kHistogram; }
  virtual Status WriteAsJson(const std::string& name, JsonWriter* w) const OVERRIDE;

  uint64_t CountInBucketForValueForTests(uint64_t value) const;
  uint64_t TotalCountForTests() const;
  uint64_t MinValueForTests() const;
  uint64_t MaxValueForTests() const;
  double MeanValueForTests() const;

 private:
  friend class MetricRegistry;
  FRIEND_TEST(MetricsTest, SimpleHistogramTest);
  explicit Histogram(const HistogramPrototype& proto);

  const gscoped_ptr<HdrHistogram> histogram_;
  const MetricUnit::Type unit_;
  const std::string description_;
  DISALLOW_COPY_AND_ASSIGN(Histogram);
};

} // namespace kudu

#endif // KUDU_UTIL_METRICS_H
