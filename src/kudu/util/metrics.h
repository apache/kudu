// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
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
// =================
// Gauge vs. Counter
// =================
//
// A Counter is a metric we expect to only monotonically increase. A
// Gauge is a metric that can decrease and increase. Use a Gauge to
// reflect a sample, e.g., the number of transaction in-flight at a
// given time; use a Counter when considering a metric over time,
// e.g., exposing the number of transactions processed since start to
// produce a metric for the number of transactions processed over some
// time period.
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

#include <algorithm>
#include <boost/function.hpp>
#include <string>
#include <tr1/unordered_map>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/casts.h"
#include "kudu/util/atomic.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

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
class HistogramSnapshotPB;

class MetricContext;

// Unit types to be used with metrics.
// As additional units are required, add them to this enum and also to Name().
struct MetricUnit {
  enum Type {
    kCacheHits,
    kCacheQueries,
    kCount,
    kBytes,
    kRequests,
    kRows,
    kConnections,
    kProbes,
    kNanoseconds,
    kMicroseconds,
    kMilliseconds,
    kSeconds,
    kThreads,
    kTransactions,
    kScanners,
    kMaintenanceOperations,
    kBlocks,
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

enum MetricWriteGranularity {
  NORMAL,
  DETAILED
};

// Base class to allow for putting all metrics into a single container.
class Metric {
 public:
  virtual ~Metric() {}
  // All metrics must be able to render themselves as JSON.
  virtual Status WriteAsJson(const std::string& name,
                             JsonWriter* writer,
                             MetricWriteGranularity granularity) const = 0;
  virtual MetricType::Type type() const = 0;
 protected:
  Metric() {}
 private:
  DISALLOW_COPY_AND_ASSIGN(Metric);
};

// Registry of all the metrics for a given subsystem.
class MetricRegistry {
 public:
  typedef std::tr1::unordered_map<std::string, Metric*> UnorderedMetricMap;

  MetricRegistry();
  ~MetricRegistry();

  // Writes metrics in this registry to 'writer'.
  //
  // 'requested_metrics' is a set of substrings to match metric names against,
  // where '*' matches all metrics.
  //
  // 'requested_detail_metrics' specifies which of the metrics should include
  // full detail (eg raw histogram counts). A metric must be matched by
  // _both_ 'requested_metrics' and 'requested_detail_metrics' to be included
  // with detail.
  // NOTE: Including all the counts and values can easily make the generated
  // json very large. Use with caution.
  Status WriteAsJson(JsonWriter* writer,
                     const std::vector<std::string>& requested_metrics,
                     const std::vector<std::string>& requested_detail_metrics) const;

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

  // Returns the Histogram with name equal to 'name' or NULL is no
  // such histogram can be found.
  Histogram* FindHistogram(const std::string& name) const;

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
                        MetricType::Type metric_type) const;

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
  GaugePrototype(const char* name,
                 MetricUnit::Type unit, const char* description)
    : name_(name),
      unit_(unit),
      description_(description) {
  }

  const char* name() const { return name_; }
  MetricUnit::Type unit() const { return unit_; }
  const char* description() const { return description_; }

  // Instantiate a "manual" gauge.
  Gauge* Instantiate(const MetricContext& context,
                     const T& initial_value) const {
    return context.metrics()->FindOrCreateGauge(
        context.prefix() + "." + name(), *this, initial_value);
  }

  // Instantiate a gauge that is backed by the given callback.
  Gauge* InstantiateFunctionGauge(const MetricContext& context,
                                  const boost::function<T()>& function) const {
    return context.metrics()->FindOrCreateFunctionGauge(
        context.prefix() + "." + name(), *this, function);
  }

 private:
  const char* name_;
  const MetricUnit::Type unit_;
  const char* description_;
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
  virtual Status WriteAsJson(const std::string& name,
                             JsonWriter* w,
                             MetricWriteGranularity granularity) const OVERRIDE;
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

// Lock-free implementation for types that are convertible to/from int64_t.
template <typename T>
class AtomicGauge : public Gauge {
 public:
  static AtomicGauge<T>* Instantiate(const GaugePrototype<T>& prototype,
                                     const MetricContext& context) {
    return down_cast<AtomicGauge<T>*>(prototype.Instantiate(context, 0));
  }

  AtomicGauge(const GaugePrototype<T>& proto, T initial_value)
    : Gauge(proto.unit(), proto.description()),
      value_(initial_value) {
  }
  T value() const {
    return static_cast<T>(value_.Load(kMemOrderRelease));
  }
  virtual void set_value(const T& value) {
    value_.Store(static_cast<int64_t>(value), kMemOrderNoBarrier);
  }
  void Increment() {
    value_.IncrementBy(1, kMemOrderNoBarrier);
  }
  virtual void IncrementBy(int64_t amount) {
    value_.IncrementBy(amount, kMemOrderNoBarrier);
  }
  void Decrement() {
    IncrementBy(-1);
  }
  void DecrementBy(int64_t amount) {
    IncrementBy(-amount);
  }

 protected:
  virtual void WriteValue(JsonWriter* writer) const OVERRIDE {
    writer->Value(value());
  }
  AtomicInt<int64_t> value_;
 private:
  DISALLOW_COPY_AND_ASSIGN(AtomicGauge);
};

// Like AtomicGauge, but keeps track of the highest value seen.
// Similar to Impala's RuntimeProfile::HighWaterMarkCounter.
// HighWaterMark::value() returns the highest value seen;
// HighWaterMark::current_value() returns the current value.
template <typename T>
class HighWaterMark : public AtomicGauge<T> {
 public:
  static HighWaterMark<T>* Instantiate(const GaugePrototype<T>& prototype,
                                       const MetricContext& context) {
    return down_cast<HighWaterMark<T*> >(prototype.Instantiate(context, 0));
  }

  HighWaterMark(const GaugePrototype<T>& proto, T initial_value)
      : AtomicGauge<T>(proto, initial_value),
        current_value_(initial_value) {
  }

  // Return the current value.
  T current_value() const {
    return static_cast<T>(current_value_.Load(kMemOrderNoBarrier));
  }

  void UpdateMax(int64_t value) {
    value_.StoreMax(value, kMemOrderNoBarrier);
  }

  // If current value + 'delta' is <= 'max', increment current value
  // by 'delta' and return true; return false otherwise.
  bool TryIncrementBy(int64_t delta, int64_t max) {
    while (true) {
      T old_val = current_value();
      T new_val = old_val + delta;
      if (new_val > max) {
        return false;
      }
      if (PREDICT_TRUE(current_value_.CompareAndSwap(static_cast<int64_t>(old_val),
                                                     static_cast<int64_t>(new_val),
                                                     kMemOrderNoBarrier))) {
        UpdateMax(new_val);
        return true;
      }
    }
  }

  virtual void IncrementBy(int64_t amount) OVERRIDE {
    UpdateMax(current_value_.IncrementBy(amount, kMemOrderNoBarrier));
  }

  virtual void set_value(const T& value) OVERRIDE {
    int64_t v = static_cast<int64_t>(value);
    current_value_.Store(v, kMemOrderNoBarrier);
    UpdateMax(v);
  }
 protected:
  virtual void WriteValue(JsonWriter* writer) const OVERRIDE {
    writer->StartObject();
    writer->String("current");
    writer->Value(current_value());
    writer->String("max");
    writer->Value(AtomicGauge<T>::value());
    writer->EndObject();
  }

  using AtomicGauge<T>::value_;
 private:
  AtomicInt<int64_t> current_value_;
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
  CounterPrototype(const char* name, MetricUnit::Type unit,
      const char* description)
    : name_(name),
      unit_(unit),
      description_(description) {
  }
  const char* name() const { return name_; }
  MetricUnit::Type unit() const { return unit_; }
  const char* description() const { return description_; }

  Counter* Instantiate(const MetricContext& context);

 private:
  const char* name_;
  const MetricUnit::Type unit_;
  const char* description_;
  DISALLOW_COPY_AND_ASSIGN(CounterPrototype);
};

// Simple incrementing 64-bit integer.
// Only use Counters in cases that we expect the count to only increase. For example,
// a counter is appropriate for "number of transactions processed by the server",
// but not for "number of transactions currently in flight". Monitoring software
// knows that counters only increase and thus can compute rates over time, rates
// across multiple servers, etc, which aren't appropriate in the case of gauges.
class Counter : public Metric {
 public:
  int64_t value() const;
  void Increment();
  void IncrementBy(int64_t amount);
  const std::string& description() const { return description_; }
  virtual MetricType::Type type() const OVERRIDE { return MetricType::kCounter; }
  virtual Status WriteAsJson(const std::string& name,
                             JsonWriter* w,
                             MetricWriteGranularity granularity) const OVERRIDE;

 private:
  friend class MetricRegistry;
  FRIEND_TEST(MetricsTest, SimpleCounterTest);
  FRIEND_TEST(MultiThreadedMetricsTest, CounterIncrementTest);

  explicit Counter(const CounterPrototype& proto);

  AtomicInt<int64_t> value_;
  const MetricUnit::Type unit_;
  const std::string description_;
  DISALLOW_COPY_AND_ASSIGN(Counter);
};

class HistogramPrototype {
 public:
  HistogramPrototype(const char* name, MetricUnit::Type unit,
                     const char* description,
                     uint64_t max_trackable_value, int num_sig_digits);
  Histogram* Instantiate(const MetricContext& context);
  const char* name() const { return name_; }
  MetricUnit::Type unit() const { return unit_; }
  const char* description() const { return description_; }
  uint64_t max_trackable_value() const { return max_trackable_value_; }
  int num_sig_digits() const { return num_sig_digits_; }

 private:
  const char* name_;
  const MetricUnit::Type unit_;
  const char* description_;
  const uint64_t max_trackable_value_;
  const int num_sig_digits_;
  DISALLOW_COPY_AND_ASSIGN(HistogramPrototype);
};

class Histogram : public Metric {
 public:
  // Increment the histogram for the given value.
  // 'value' must be non-negative.
  void Increment(int64_t value);

  // Increment the histogram for the given value by the given amount.
  // 'value' and 'amount' must be non-negative.
  void IncrementBy(int64_t value, int64_t amount);

  const std::string& description() const { return description_; }
  virtual MetricType::Type type() const OVERRIDE { return MetricType::kHistogram; }
  virtual Status WriteAsJson(const std::string& name,
                             JsonWriter* w,
                             MetricWriteGranularity granularity) const OVERRIDE;

  // Returns a snapshot of this histogram including the bucketed values and counts.
  Status GetHistogramSnapshotPB(HistogramSnapshotPB* snapshot,
                                MetricWriteGranularity granularity) const;

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

// Measures a duration while in scope. Adds this duration to specified histogram on destruction.
class ScopedLatencyMetric {
 public:
  explicit ScopedLatencyMetric(Histogram* latency_hist);
  ~ScopedLatencyMetric();

 private:
  Histogram* latency_hist_;
  MonoTime time_started_;
};

} // namespace kudu

#endif // KUDU_UTIL_METRICS_H
