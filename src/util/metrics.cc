// Copyright (c) 2013, Cloudera, inc.
#include "util/metrics.h"

#include <map>
#include <set>

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>

#include "gutil/atomicops.h"
#include "gutil/casts.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "util/hdr_histogram.h"
#include "util/histogram.pb.h"
#include "util/jsonwriter.h"
#include "util/locks.h"
#include "util/status.h"

namespace kudu {

using std::string;
using std::vector;
using std::tr1::unordered_map;
using strings::Substitute;

//
// MetricUnit
//

const char* MetricUnit::Name(Type unit) {
  switch (unit) {
    case kCount:
      return "units";
    case kBytes:
      return "bytes";
    case kRequests:
      return "requests";
    case kRows:
      return "rows";
    case kConnections:
      return "connections";
    case kProbes:
      return "probes";
    case kNanoseconds:
      return "nanoseconds";
    case kMicroseconds:
      return "microseconds";
    case kMilliseconds:
      return "milliseconds";
    case kSeconds:
      return "seconds";
    case kThreads:
      return "threads";
    case kTransactions:
      return "transactions";
    default:
      return "UNKNOWN UNIT";
  }
}

//
// MetricType
//

const char* const MetricType::kGaugeType = "gauge";
const char* const MetricType::kCounterType = "counter";
const char* const MetricType::kHistogramType = "histogram";
const char* MetricType::Name(MetricType::Type type) {
  switch (type) {
    case kGauge:
      return kGaugeType;
    case kCounter:
      return kCounterType;
    case kHistogram:
      return kHistogramType;
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

Histogram* MetricRegistry::FindHistogram(const std::string& name) const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return FindMetricUnlocked<Histogram>(name, MetricType::kHistogram);
}


template<typename T>
T* MetricRegistry::FindMetricUnlocked(const std::string& name,
                                      MetricType::Type metric_type) const {
  Metric* metric = FindPtrOrNull(metrics_, name);
  if (metric != NULL) {
    CHECK_EQ(metric_type, metric->type())
          << "Downcast expects " << MetricType::Name(metric_type)
          << " but found " << MetricType::Name(metric->type()) << " for " << name;
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

Histogram* MetricRegistry::FindOrCreateHistogram(const std::string& name,
                                                 const HistogramPrototype& proto) {
  boost::lock_guard<simple_spinlock> l(lock_);
  Histogram* histogram = FindMetricUnlocked<Histogram>(name, MetricType::kHistogram);
  if (!histogram) {
    histogram = new Histogram(proto);
    InsertOrDie(&metrics_, name, histogram);
  }
  return histogram;
}

Status MetricRegistry::WriteAsJson(JsonWriter* writer,
                                   const vector<string>& requested_metrics,
                                   const vector<string>& requested_detail_metrics) const {
  // We want the keys to be in alphabetical order when printing, so we use an ordered map here.
  typedef std::map<string, Metric*> OrderedMetricMap;
  OrderedMetricMap metrics;
  std::set<string> requested_detail_metrics_set;
  {
    // Snapshot the metrics in this registry (not guaranteed to be a consistent snapshot)
    boost::lock_guard<simple_spinlock> l(lock_);
    BOOST_FOREACH(const UnorderedMetricMap::value_type& val, metrics_) {
      if (!requested_metrics.empty()) {
       BOOST_FOREACH(const string& requested_metric, requested_metrics) {
         if (val.first.find(requested_metric) != std::string::npos) {
           metrics.insert(val);
           break;
         }
       }
      } else {
        metrics.insert(val);
      }
      if (!requested_detail_metrics.empty()) {
        BOOST_FOREACH(const string& requested_detail_metric, requested_detail_metrics) {
          if (val.first.find(requested_detail_metric) != std::string::npos) {
            requested_detail_metrics_set.insert(val.first);
            break;
          }
        }
      }
    }
  }

  writer->StartObject();

  writer->String("metrics");
  writer->StartArray();
  BOOST_FOREACH(OrderedMetricMap::value_type& val, metrics) {
    if (ContainsKey(requested_detail_metrics_set, val.first)) {
      WARN_NOT_OK(val.second->WriteAsJson(val.first, writer, DETAILED),
                     strings::Substitute("Failed to write $0 as JSON", val.first));
    } else {
      WARN_NOT_OK(val.second->WriteAsJson(val.first, writer, NORMAL),
                          strings::Substitute("Failed to write $0 as JSON", val.first));
    }
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

Status Gauge::WriteAsJson(const string& name,
                          JsonWriter* writer,
                          MetricWriteGranularity granularity) const {
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

Status Counter::WriteAsJson(const string& name,
                            JsonWriter* writer,
                            MetricWriteGranularity granularity) const {
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

/////////////////////////////////////////////////
// HistogramPrototype
/////////////////////////////////////////////////

HistogramPrototype::HistogramPrototype(const std::string& name, MetricUnit::Type unit,
                                       const std::string& description,
                                       uint64_t max_trackable_value, int num_sig_digits)
  : name_(name),
    unit_(unit),
    description_(description),
    max_trackable_value_(max_trackable_value),
    num_sig_digits_(num_sig_digits) {
  // Better to crash at definition time that at instantiation time.
  CHECK(HdrHistogram::IsValidHighestTrackableValue(max_trackable_value))
      << Substitute("Invalid max trackable value on histogram $0: $1",
                    name, max_trackable_value);
  CHECK(HdrHistogram::IsValidNumSignificantDigits(num_sig_digits))
      << Substitute("Invalid number of significant digits on histogram $0: $1",
                    name, num_sig_digits);
}

Histogram* HistogramPrototype::Instantiate(const MetricContext& context) {
  return context.metrics()->FindOrCreateHistogram(
    context.prefix() + "." + name_, *this);
}

/////////////////////////////////////////////////
// Histogram
/////////////////////////////////////////////////

Histogram::Histogram(const HistogramPrototype& proto)
  : histogram_(new HdrHistogram(proto.max_trackable_value(), proto.num_sig_digits())),
    unit_(proto.unit()),
    description_(proto.description()) {
}

void Histogram::Increment(int64_t value) {
  histogram_->Increment(value);
}

void Histogram::IncrementBy(int64_t value, int64_t amount) {
  histogram_->IncrementBy(value, amount);
}

Status Histogram::WriteAsJson(const std::string& name,
                              JsonWriter* writer,
                              MetricWriteGranularity granularity) const {

  HistogramSnapshotPB snapshot;
  RETURN_NOT_OK(GetHistogramSnapshotPB(&snapshot, granularity));
  snapshot.set_name(name);
  writer->Protobuf(snapshot);
  return Status::OK();
}

Status Histogram::GetHistogramSnapshotPB(HistogramSnapshotPB* snapshot_pb,
                                         MetricWriteGranularity granularity) const {
  HdrHistogram snapshot(*histogram_);
  snapshot_pb->set_type(MetricType::Name(type()));
  snapshot_pb->set_unit(MetricUnit::Name(unit_));
  snapshot_pb->set_max_trackable_value(snapshot.highest_trackable_value());
  snapshot_pb->set_num_significant_digits(snapshot.num_significant_digits());
  snapshot_pb->set_total_count(snapshot.TotalCount());
  snapshot_pb->set_min(snapshot.MinValue());
  snapshot_pb->set_mean(snapshot.MeanValue());
  snapshot_pb->set_percentile_75(snapshot.ValueAtPercentile(75));
  snapshot_pb->set_percentile_95(snapshot.ValueAtPercentile(95));
  snapshot_pb->set_percentile_99(snapshot.ValueAtPercentile(99));
  snapshot_pb->set_percentile_99_9(snapshot.ValueAtPercentile(99.9));
  snapshot_pb->set_percentile_99_99(snapshot.ValueAtPercentile(99.99));
  snapshot_pb->set_max(snapshot.MaxValue());
  snapshot_pb->set_description(description_);

  if (granularity == DETAILED) {
    // If the caller asked for the details of this histogram it probably
    // already got the description, so save some space by clearing it.
    snapshot_pb->clear_description();
    RecordedValuesIterator iter(&snapshot);
    while (iter.HasNext()) {
      HistogramIterationValue value;
      RETURN_NOT_OK(iter.Next(&value));
      snapshot_pb->add_values(value.value_iterated_to);
      snapshot_pb->add_counts(value.count_at_value_iterated_to);
    }
  }
  return Status::OK();
}

uint64_t Histogram::CountInBucketForValueForTests(uint64_t value) const {
  return histogram_->CountInBucketForValue(value);
}

uint64_t Histogram::TotalCountForTests() const {
  return histogram_->TotalCount();
}

uint64_t Histogram::MinValueForTests() const {
  return histogram_->MinValue();
}

uint64_t Histogram::MaxValueForTests() const {
  return histogram_->MaxValue();
}
double Histogram::MeanValueForTests() const {
  return histogram_->MeanValue();
}

} // namespace kudu
