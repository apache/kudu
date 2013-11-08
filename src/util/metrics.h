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
//  - StringGauge: Gauge for a string value.
//  - BooleanGauge, IntegerGauge, DoubleGauge: Lock-free gauges.
// Counter: Get, reset, increment or decrement an int64_t value.
//
// TODO: Implement Histogram, Meter, Timer.
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
// TODO: Implement registration macros for gauges.
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

#include <string>
#include <tr1/unordered_map>

#include <gtest/gtest.h>

#include "gutil/atomicops.h"
#include "util/locks.h"
#include "util/status.h"

// Convenience macros.
#define METRIC_DEFINE_counter(name, unit, desc) \
  ::kudu::CounterPrototype METRIC_##name(#name, unit, desc)

#define METRIC_DECLARE_counter(name) \
  extern ::kudu::CounterPrototype METRIC_##name

// TODO: Implement registration macros for gauges.
#define METRIC_DEFINE_gauge_string(name, default_value, unit, desc)
#define METRIC_DEFINE_gauge_bool(name, default_value, unit, desc)
#define METRIC_DEFINE_gauge_integer(name, default_value, unit, desc)
#define METRIC_DEFINE_gauge_double(name, default_value, unit, desc)

namespace kudu {

class Counter;
class Gauge;
class JsonWriter;
class MetricContext;
class CounterPrototype;

// Unit types to be used with metrics.
// As additional units are required, add them to this enum and also to Name().
struct MetricUnit {
  enum Type {
    kBytes,
    kRequests,
    kRows,
    kProbes
  };
  static const char* Name(Type unit);
};

class MetricType {
 public:
  enum Type { kGauge, kCounter };
  static const char* Name(Type t);
 private:
  static const char* const kGaugeType;
  static const char* const kCounterType;
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
  MetricRegistry();
  ~MetricRegistry();
  Status WriteAsJson(JsonWriter* writer) const;
 private:
  friend class CounterPrototype;
  friend class MultiThreadedMetricsTest;  // For unit testing.
  FRIEND_TEST(MetricsTest, JsonPrintTest);
  FRIEND_TEST(MultiThreadedMetricsTest, AddCounterToRegistryTest);

  typedef std::tr1::unordered_map<string, Metric*> UnorderedMetricMap;

  Counter* FindOrCreateCounter(const std::string& name,
                               const CounterPrototype& proto);

  // Not thread-safe, used for tests.
  const UnorderedMetricMap& metrics() const { return metrics_; }

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

// Abstract base class to provide point-in-time metric values.
class Gauge : public Metric {
 public:
  virtual ~Gauge() {}
  virtual MetricType::Type type() const { return MetricType::kGauge; }
  // TODO: Make Gauge units an enum as well?
  virtual const std::string& unit() const = 0;
  virtual const std::string& description() const = 0;
  virtual Status WriteAsJson(const std::string& name, JsonWriter* w) const;
 protected:
  Gauge() {}
  virtual void WriteValue(JsonWriter* writer) const = 0;
 private:
  DISALLOW_COPY_AND_ASSIGN(Gauge);
};

// Gauge implementation for string that uses locks to ensure thread safety.
class StringGauge : public Gauge {
 public:
  StringGauge(const std::string& unit, const std::string& description);
  std::string value() const;
  void set_value(const std::string& value);
  virtual const std::string& unit() const { return unit_; }
  virtual const std::string& description() const { return description_; }
 protected:
  virtual void WriteValue(JsonWriter* writer) const;
 private:
  std::string value_;
  mutable simple_spinlock lock_;  // Guards value_
  const std::string unit_;
  const std::string description_;
  DISALLOW_COPY_AND_ASSIGN(StringGauge);
};

// Lock-free implementation for types that are convertible to/from Atomic64.
template <typename T>
class AtomicGauge : public Gauge {
 public:
  AtomicGauge(const std::string& unit, const std::string& description)
    : value_(),
     unit_(unit),
     description_(description) {
  }
  T value() const {
    return static_cast<T>(base::subtle::Release_Load(&value_));
  }
  void set_value(const T& value) {
    base::subtle::NoBarrier_Store(&value_, static_cast<base::subtle::Atomic64>(value));
  }
  virtual const std::string& unit() const { return unit_; }
  virtual const std::string& description() const { return description_; }
 protected:
  virtual void WriteValue(JsonWriter* writer) const {
    WriteSpecificValue(writer, value());
  }
  // Implement this method in specializations.
  static void WriteSpecificValue(JsonWriter* writer, T val);
 private:
  base::subtle::Atomic64 value_;
  const std::string unit_;
  const std::string description_;
  DISALLOW_COPY_AND_ASSIGN(AtomicGauge);
};

// Convenient typedefs for common primitive metric types.
typedef class AtomicGauge<bool> BooleanGauge;
typedef class AtomicGauge<int64_t> IntegerGauge;
typedef class AtomicGauge<double> DoubleGauge;

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
  virtual MetricType::Type type() const { return MetricType::kCounter; }
  virtual Status WriteAsJson(const std::string& name, JsonWriter* w) const;

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

} // namespace kudu

#endif // KUDU_UTIL_METRICS_H
