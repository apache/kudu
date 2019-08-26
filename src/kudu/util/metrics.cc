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

#include <iostream>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/singleton.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/histogram.pb.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"

DEFINE_int32(metrics_retirement_age_ms, 120 * 1000,
             "The minimum number of milliseconds a metric will be kept for after it is "
             "no longer active. (Advanced option)");
TAG_FLAG(metrics_retirement_age_ms, runtime);
TAG_FLAG(metrics_retirement_age_ms, advanced);

// Process/server-wide metrics should go into the 'server' entity.
// More complex applications will define other entities.
METRIC_DEFINE_entity(server);

namespace kudu {

using std::string;
using std::unordered_set;
using std::vector;
using strings::Substitute;

template<typename Collection>
void WriteMetricsToJson(JsonWriter* writer,
                        const Collection& metrics,
                        const MetricJsonOptions& opts) {
  writer->String("metrics");
  writer->StartArray();
  for (const auto& val : metrics) {
    const auto& m = val.second;
    if (m->ModifiedInOrAfterEpoch(opts.only_modified_in_or_after_epoch)) {
      if (!opts.include_untouched_metrics && m->IsUntouched()) {
        continue;
      }
      WARN_NOT_OK(m->WriteAsJson(writer, opts),
                  Substitute("Failed to write $0 as JSON", val.first->name()));
    }
  }
  writer->EndArray();
}

void WriteToJson(JsonWriter* writer,
                 const MergedEntityMetrics &merged_entity_metrics,
                 const MetricJsonOptions &opts) {
  for (const auto& entity_metrics : merged_entity_metrics) {
    if (entity_metrics.second.empty()) {
      continue;
    }
    writer->StartObject();

    writer->String("type");
    writer->String(entity_metrics.first.type_);

    writer->String("id");
    writer->String(entity_metrics.first.id_);

    WriteMetricsToJson(writer, entity_metrics.second, opts);

    writer->EndObject();
  }
}

//
// MetricUnit
//

const char* MetricUnit::Name(Type unit) {
  switch (unit) {
    case kCacheHits:
      return "hits";
    case kCacheQueries:
      return "queries";
    case kBytes:
      return "bytes";
    case kRequests:
      return "requests";
    case kEntries:
      return "entries";
    case kRows:
      return "rows";
    case kCells:
      return "cells";
    case kConnections:
      return "connections";
    case kOperations:
      return "operations";
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
    case kUnits:
      return "units";
    case kScanners:
      return "scanners";
    case kMaintenanceOperations:
      return "operations";
    case kBlocks:
      return "blocks";
    case kHoles:
      return "holes";
    case kLogBlockContainers:
      return "log block containers";
    case kTasks:
      return "tasks";
    case kMessages:
      return "messages";
    case kContextSwitches:
      return "context switches";
    case kDataDirectories:
      return "data directories";
    case kState:
      return "state";
    case kSessions:
      return "sessions";
    case kTablets:
      return "tablets";
    default:
      DCHECK(false) << "Unknown unit with type = " << unit;
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
// MetricEntityPrototype
//

MetricEntityPrototype::MetricEntityPrototype(const char* name)
  : name_(name) {
  MetricPrototypeRegistry::get()->AddEntity(this);
}

MetricEntityPrototype::~MetricEntityPrototype() {
}

scoped_refptr<MetricEntity> MetricEntityPrototype::Instantiate(
    MetricRegistry* registry,
    const string& id,
    const MetricEntity::AttributeMap& initial_attrs) const {
  return registry->FindOrCreateEntity(this, id, initial_attrs);
}

//
// MetricEntity
//

MetricEntity::MetricEntity(const MetricEntityPrototype* prototype,
                           string id, AttributeMap attributes)
    : prototype_(prototype),
      id_(std::move(id)),
      attributes_(std::move(attributes)),
      published_(true) {}

MetricEntity::~MetricEntity() {
}

void MetricEntity::CheckInstantiation(const MetricPrototype* proto) const {
  CHECK_STREQ(prototype_->name(), proto->entity_type())
    << "Metric " << proto->name() << " may not be instantiated entity of type "
    << prototype_->name() << " (expected: " << proto->entity_type() << ")";
}

scoped_refptr<Metric> MetricEntity::FindOrNull(const MetricPrototype& prototype) const {
  std::lock_guard<simple_spinlock> l(lock_);
  return FindPtrOrNull(metric_map_, &prototype);
}

namespace {

bool MatchNameInList(const string& name, const vector<string>& names) {
  string name_uc;
  ToUpperCase(name, &name_uc);

  for (const string& e : names) {
    // The parameter is a case-insensitive substring match of the metric name.
    string e_uc;
    ToUpperCase(e, &e_uc);
    if (name_uc.find(e_uc) != string::npos) {
      return true;
    }
  }
  return false;
}

} // anonymous namespace

Status MetricEntity::GetMetricsAndAttrs(const MetricFilters& filters,
                                        MetricMap* metrics,
                                        AttributeMap* attrs) const {
  CHECK(metrics);
  CHECK(attrs);

  // Filter the 'type'.
  if (!filters.entity_types.empty() && !MatchNameInList(prototype_->name(), filters.entity_types)) {
    return Status::NotFound("entity is filtered by entity type");
  }

  // Filter the 'id'.
  if (!filters.entity_ids.empty() && !MatchNameInList(id_, filters.entity_ids)) {
    return Status::NotFound("entity is filtered by entity id");
  }

  {
    // Snapshot the metrics in this registry (not guaranteed to be a consistent snapshot)
    std::lock_guard<simple_spinlock> l(lock_);
    *attrs = attributes_;
    *metrics = metric_map_;
  }

  // Filter the 'attributes'.
  if (!filters.entity_attrs.empty()) {
    bool match_attrs = false;
    DCHECK(filters.entity_attrs.size() % 2 == 0);
    for (int i = 0; i < filters.entity_attrs.size(); i += 2) {
      // The attr_key can't be found or the attr_val can't be matched.
      AttributeMap::const_iterator it = attrs->find(filters.entity_attrs[i]);
      if (it == attrs->end() || !MatchNameInList(it->second, { filters.entity_attrs[i+1] })) {
        continue;
      }
      match_attrs = true;
      break;
    }
    // None of them match.
    if (!match_attrs) {
      return Status::NotFound("entity is filtered by some attribute");
    }
  }

  // Filter the 'metrics'.
  if (!filters.entity_metrics.empty()) {
    for (auto metric = metrics->begin(); metric != metrics->end();) {
      if (!MatchNameInList(metric->first->name(), filters.entity_metrics)) {
        metric = metrics->erase(metric);
      } else {
        ++metric;
      }
    }
    // None of them match.
    if (metrics->empty()) {
      return Status::NotFound("entity is filtered by metric types");
    }
  }

  return Status::OK();
}

Status MetricEntity::WriteAsJson(JsonWriter* writer, const MetricJsonOptions& opts) const {
  MetricMap metrics;
  AttributeMap attrs;
  Status s = GetMetricsAndAttrs(opts.filters, &metrics, &attrs);
  if (s.IsNotFound()) {
    // Status::NotFound is returned when this entity has been filtered, treat it
    // as OK, and skip printing it.
    return Status::OK();
  }

  writer->StartObject();

  writer->String("type");
  writer->String(prototype_->name());

  writer->String("id");
  writer->String(id_);

  if (opts.include_entity_attributes) {
    writer->String("attributes");
    writer->StartObject();
    for (const AttributeMap::value_type& val : attrs) {
      writer->String(val.first);
      writer->String(val.second);
    }
    writer->EndObject();
  }

  WriteMetricsToJson(writer, metrics, opts);

  writer->EndObject();

  return Status::OK();
}

Status MetricEntity::CollectTo(MergedEntityMetrics* collections,
                               const MetricFilters& filters,
                               const MetricMergeRules& merge_rules) const {
  MetricMap metrics;
  AttributeMap attrs;
  Status s = GetMetricsAndAttrs(filters, &metrics, &attrs);
  if (s.IsNotFound()) {
    // Status::NotFound is returned when this entity has been filtered, treat it
    // as OK, and skip collecting it.
    return Status::OK();
  }

  string entity_type = prototype_->name();
  string entity_id = id();
  auto* merge_rule = ::FindOrNull(merge_rules, prototype_->name());
  if (merge_rule) {
    entity_type = merge_rule->merge_to;
    auto entity_id_ptr = ::FindOrNull(attrs, merge_rule->attribute_to_merge_by);
    if (!entity_id_ptr) {
      return Status::NotFound(Substitute("attribute $0 not found in entity $1",
                                         merge_rule->attribute_to_merge_by, entity_id));
    }
    entity_id = *entity_id_ptr;
  }

  MergedEntity e(entity_type, entity_id);
  auto& entity_collection = collections->emplace(std::make_pair(e, MergedMetrics())).first->second;
  for (const auto& val : metrics) {
    const MetricPrototype* prototype = val.first;
    const scoped_refptr<Metric>& metric = val.second;

    scoped_refptr<Metric> entry = FindPtrOrNull(entity_collection, prototype);
    if (!entry) {
      scoped_refptr<Metric> new_metric = metric->snapshot();
      InsertOrDie(&entity_collection, new_metric->prototype(), new_metric);
    } else {
      entry->MergeFrom(metric);
    }
  }

  return Status::OK();
}

void MetricEntity::RetireOldMetrics() {
  MonoTime now(MonoTime::Now());

  std::lock_guard<simple_spinlock> l(lock_);
  for (auto it = metric_map_.begin(); it != metric_map_.end();) {
    const scoped_refptr<Metric>& metric = it->second;

    if (PREDICT_TRUE(!metric->HasOneRef() && published_)) {
      // The metric is still in use. Note that, in the case of "NeverRetire()", the metric
      // will have a ref-count of 2 because it is reffed by the 'never_retire_metrics_'
      // collection.

      // Ensure that it is not marked for later retirement (this could happen in the case
      // that a metric is un-reffed and then re-reffed later by looking it up from the
      // registry).
      metric->retire_time_ = MonoTime();
      ++it;
      continue;
    }

    if (!metric->retire_time_.Initialized()) {
      VLOG(3) << "Metric " << it->first << " has become un-referenced or unpublished. "
              << "Will retire after the retention interval";
      // This is the first time we've seen this metric as retirable.
      metric->retire_time_ =
          now + MonoDelta::FromMilliseconds(FLAGS_metrics_retirement_age_ms);
      ++it;
      continue;
    }

    // If we've already seen this metric in a previous scan, check if it's
    // time to retire it yet.
    if (now < metric->retire_time_) {
      VLOG(3) << "Metric " << it->first << " is un-referenced, but still within "
              << "the retention interval";
      ++it;
      continue;
    }

    VLOG(2) << "Retiring metric " << it->first;
    metric_map_.erase(it++);
  }
}

void MetricEntity::NeverRetire(const scoped_refptr<Metric>& metric) {
  std::lock_guard<simple_spinlock> l(lock_);
  never_retire_metrics_.push_back(metric);
}

void MetricEntity::SetAttributes(const AttributeMap& attrs) {
  std::lock_guard<simple_spinlock> l(lock_);
  attributes_ = attrs;
}

void MetricEntity::SetAttribute(const string& key, const string& val) {
  std::lock_guard<simple_spinlock> l(lock_);
  attributes_[key] = val;
}

//
// MetricRegistry
//

MetricRegistry::MetricRegistry() {
}

MetricRegistry::~MetricRegistry() {
}

Status MetricRegistry::WriteAsJson(JsonWriter* writer, const MetricJsonOptions& opts) const {
  EntityMap entities;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    entities = entities_;
  }

  writer->StartArray();
  if (opts.merge_rules.empty()) {
    for (const auto& e : entities) {
      WARN_NOT_OK(e.second->WriteAsJson(writer, opts),
                  Substitute("Failed to write entity $0 as JSON", e.second->id()));
    }
  } else {
    MergedEntityMetrics collections;
    for (const auto& e : entities) {
      WARN_NOT_OK(e.second->CollectTo(&collections, opts.filters, opts.merge_rules),
                  Substitute("Failed to collect entity $0", e.second->id()));
    }
    WriteToJson(writer, collections, opts);
  }
  writer->EndArray();

  // Rather than having a thread poll metrics periodically to retire old ones,
  // we'll just retire them here. The only downside is that, if no one is polling
  // metrics, we may end up leaving them around indefinitely; however, metrics are
  // small, and one might consider it a feature: if monitoring stops polling for
  // metrics, we should keep them around until the next poll.
  entities.clear(); // necessary to deref metrics we just dumped before doing retirement scan.
  const_cast<MetricRegistry*>(this)->RetireOldMetrics();
  return Status::OK();
}

void MetricRegistry::RetireOldMetrics() {
  std::lock_guard<simple_spinlock> l(lock_);
  for (auto it = entities_.begin(); it != entities_.end();) {
    it->second->RetireOldMetrics();

    if (it->second->num_metrics() == 0 &&
        (it->second->HasOneRef() || !it->second->published())) {
      // This entity has no metrics and either has no more external references or has
      // been marked as unpublished, so we can remove it.
      // Unlike retiring the metrics themselves, we don't wait for any timeout
      // to retire them -- we assume that that timed retention has been satisfied
      // by holding onto the metrics inside the entity.
      entities_.erase(it++);
    } else {
      ++it;
    }
  }
}

//
// MetricPrototypeRegistry
//
MetricPrototypeRegistry* MetricPrototypeRegistry::get() {
  return Singleton<MetricPrototypeRegistry>::get();
}

void MetricPrototypeRegistry::AddMetric(const MetricPrototype* prototype) {
  std::lock_guard<simple_spinlock> l(lock_);
  metrics_.push_back(prototype);
}

void MetricPrototypeRegistry::AddEntity(const MetricEntityPrototype* prototype) {
  std::lock_guard<simple_spinlock> l(lock_);
  entities_.push_back(prototype);
}

void MetricPrototypeRegistry::WriteAsJson(JsonWriter* writer) const {
  std::lock_guard<simple_spinlock> l(lock_);
  MetricJsonOptions opts;
  opts.include_schema_info = true;
  writer->StartObject();

  // Dump metric prototypes.
  writer->String("metrics");
  writer->StartArray();
  for (const MetricPrototype* p : metrics_) {
    writer->StartObject();
    p->WriteFields(writer, opts);
    writer->String("entity_type");
    writer->String(p->entity_type());
    writer->EndObject();
  }
  writer->EndArray();

  // Dump entity prototypes.
  writer->String("entities");
  writer->StartArray();
  for (const MetricEntityPrototype* p : entities_) {
    writer->StartObject();
    writer->String("name");
    writer->String(p->name());
    writer->EndObject();
  }
  writer->EndArray();

  writer->EndObject();
}

void MetricPrototypeRegistry::WriteAsJson() const {
  std::ostringstream s;
  JsonWriter w(&s, JsonWriter::PRETTY);
  WriteAsJson(&w);
  std::cout << s.str() << std::endl;
}

//
// MetricPrototype
//
MetricPrototype::MetricPrototype(CtorArgs args) : args_(args) {
  MetricPrototypeRegistry::get()->AddMetric(this);
}

void MetricPrototype::WriteFields(JsonWriter* writer,
                                  const MetricJsonOptions& opts) const {
  writer->String("name");
  writer->String(name());

  if (opts.include_schema_info) {
    writer->String("label");
    writer->String(label());

    writer->String("type");
    writer->String(MetricType::Name(type()));

    writer->String("unit");
    writer->String(MetricUnit::Name(unit()));

    writer->String("description");
    writer->String(description());
  }
}

//
// FunctionGaugeDetacher
//

FunctionGaugeDetacher::FunctionGaugeDetacher() {
}

FunctionGaugeDetacher::~FunctionGaugeDetacher() {
  for (const Closure& c : callbacks_) {
    c.Run();
  }
}

scoped_refptr<MetricEntity> MetricRegistry::FindOrCreateEntity(
    const MetricEntityPrototype* prototype,
    const string& id,
    const MetricEntity::AttributeMap& initial_attrs) {
  std::lock_guard<simple_spinlock> l(lock_);
  scoped_refptr<MetricEntity> e = FindPtrOrNull(entities_, id);
  if (!e) {
    e = new MetricEntity(prototype, id, initial_attrs);
    InsertOrDie(&entities_, id, e);
  } else if (!e->published()) {
    e = new MetricEntity(prototype, id, initial_attrs);
    entities_[id] = e;
  } else {
    e->SetAttributes(initial_attrs);
  }
  return e;
}

//
// Metric
//

std::atomic<int64_t> Metric::g_epoch_;

Metric::Metric(const MetricPrototype* prototype)
    : prototype_(prototype),
      m_epoch_(current_epoch()) {
}

Metric::~Metric() {
}

void Metric::IncrementEpoch() {
  g_epoch_++;
}

void Metric::UpdateModificationEpochSlowPath() {
  int64_t new_epoch, old_epoch;
  // CAS loop to ensure that we never transition a metric's epoch backwards
  // even if multiple threads race to update it.
  do {
    old_epoch = m_epoch_;
    new_epoch = g_epoch_;
  } while (old_epoch < new_epoch &&
           !m_epoch_.compare_exchange_weak(old_epoch, new_epoch));
}

//
// Gauge
//

Status Gauge::WriteAsJson(JsonWriter* writer,
                          const MetricJsonOptions& opts) const {
  writer->StartObject();

  prototype_->WriteFields(writer, opts);

  writer->String("value");
  WriteValue(writer);

  writer->EndObject();
  return Status::OK();
}

//
// StringGauge
//

StringGauge::StringGauge(const GaugePrototype<string>* proto,
                         string initial_value,
                         unordered_set<string> initial_unique_values)
    : Gauge(proto),
      value_(std::move(initial_value)),
      unique_values_(std::move(initial_unique_values)) {}

scoped_refptr<Metric> StringGauge::snapshot() const {
  std::lock_guard<simple_spinlock> l(lock_);
  scoped_refptr<Metric> m
    = new StringGauge(down_cast<const GaugePrototype<string>*>(prototype_),
                      value_,
                      unique_values_);
  return m;
}

string StringGauge::value() const {
  std::lock_guard<simple_spinlock> l(lock_);
  if (PREDICT_TRUE(unique_values_.empty())) {
    return value_;
  }
  return JoinStrings(unique_values_, ", ");
}

void StringGauge::FillUniqueValuesUnlocked() {
  if (unique_values_.empty()) {
    unique_values_.insert(value_);
  }
}

unordered_set<string> StringGauge::unique_values() {
  std::lock_guard<simple_spinlock> l(lock_);
  FillUniqueValuesUnlocked();
  return unique_values_;
}

void StringGauge::set_value(const string& value) {
  UpdateModificationEpoch();
  std::lock_guard<simple_spinlock> l(lock_);
  value_ = value;
  unique_values_.clear();
}

bool StringGauge::MergeFrom(const scoped_refptr<Metric>& other) {
  if (PREDICT_FALSE(this == other.get())) {
    return true;
  }
  UpdateModificationEpoch();

  scoped_refptr<StringGauge> other_ptr = down_cast<StringGauge*>(other.get());
  auto other_values = other_ptr->unique_values();

  std::lock_guard<simple_spinlock> l(lock_);
  FillUniqueValuesUnlocked();
  unique_values_.insert(other_values.begin(), other_values.end());
  return true;
}

void StringGauge::WriteValue(JsonWriter* writer) const {
  writer->String(value());
}

//
// Counter
//
// This implementation is optimized by using a striped counter. See LongAdder for details.

scoped_refptr<Counter> CounterPrototype::Instantiate(const scoped_refptr<MetricEntity>& entity) {
  return entity->FindOrCreateCounter(this);
}

Counter::Counter(const CounterPrototype* proto) : Metric(proto) {
}

int64_t Counter::value() const {
  return value_.Value();
}

void Counter::Increment() {
  IncrementBy(1);
}

void Counter::IncrementBy(int64_t amount) {
  UpdateModificationEpoch();
  value_.IncrementBy(amount);
}

Status Counter::WriteAsJson(JsonWriter* writer,
                            const MetricJsonOptions& opts) const {
  writer->StartObject();

  prototype_->WriteFields(writer, opts);

  writer->String("value");
  writer->Int64(value());

  writer->EndObject();
  return Status::OK();
}

/////////////////////////////////////////////////
// HistogramPrototype
/////////////////////////////////////////////////

HistogramPrototype::HistogramPrototype(const MetricPrototype::CtorArgs& args,
                                       uint64_t max_trackable_value, int num_sig_digits)
  : MetricPrototype(args),
    max_trackable_value_(max_trackable_value),
    num_sig_digits_(num_sig_digits) {
  // Better to crash at definition time that at instantiation time.
  CHECK(HdrHistogram::IsValidHighestTrackableValue(max_trackable_value))
      << Substitute("Invalid max trackable value on histogram $0: $1",
                    args.name_, max_trackable_value);
  CHECK(HdrHistogram::IsValidNumSignificantDigits(num_sig_digits))
      << Substitute("Invalid number of significant digits on histogram $0: $1",
                    args.name_, num_sig_digits);
}

scoped_refptr<Histogram> HistogramPrototype::Instantiate(
    const scoped_refptr<MetricEntity>& entity) {
  return entity->FindOrCreateHistogram(this);
}

/////////////////////////////////////////////////
// Histogram
/////////////////////////////////////////////////

Histogram::Histogram(const HistogramPrototype* proto)
  : Metric(proto),
    histogram_(new HdrHistogram(proto->max_trackable_value(), proto->num_sig_digits())) {
}

Histogram::Histogram(const HistogramPrototype* proto, const HdrHistogram& hdr_hist)
  : Metric(proto),
    histogram_(new HdrHistogram(hdr_hist)) {
}

void Histogram::Increment(int64_t value) {
  UpdateModificationEpoch();
  histogram_->Increment(value);
}

void Histogram::IncrementBy(int64_t value, int64_t amount) {
  UpdateModificationEpoch();
  histogram_->IncrementBy(value, amount);
}

Status Histogram::WriteAsJson(JsonWriter* writer,
                              const MetricJsonOptions& opts) const {

  HistogramSnapshotPB snapshot;
  RETURN_NOT_OK(GetHistogramSnapshotPB(&snapshot, opts));
  writer->Protobuf(snapshot);
  return Status::OK();
}

Status Histogram::GetHistogramSnapshotPB(HistogramSnapshotPB* snapshot_pb,
                                         const MetricJsonOptions& opts) const {
  snapshot_pb->set_name(prototype_->name());
  if (opts.include_schema_info) {
    snapshot_pb->set_type(MetricType::Name(prototype_->type()));
    snapshot_pb->set_label(prototype_->label());
    snapshot_pb->set_unit(MetricUnit::Name(prototype_->unit()));
    snapshot_pb->set_description(prototype_->description());
    snapshot_pb->set_max_trackable_value(histogram_->highest_trackable_value());
    snapshot_pb->set_num_significant_digits(histogram_->num_significant_digits());
  }
  // Fast-path for a reasonably common case of an empty histogram. This occurs
  // when a histogram is tracking some information about a feature not in
  // use, for example.
  if (histogram_->TotalCount() == 0) {
    snapshot_pb->set_total_count(0);
    snapshot_pb->set_total_sum(0);
    snapshot_pb->set_min(0);
    snapshot_pb->set_mean(0);
    snapshot_pb->set_percentile_75(0);
    snapshot_pb->set_percentile_95(0);
    snapshot_pb->set_percentile_99(0);
    snapshot_pb->set_percentile_99_9(0);
    snapshot_pb->set_percentile_99_99(0);
    snapshot_pb->set_max(0);
  } else {
    HdrHistogram snapshot(*histogram_);
    snapshot_pb->set_total_count(snapshot.TotalCount());
    snapshot_pb->set_total_sum(snapshot.TotalSum());
    snapshot_pb->set_min(snapshot.MinValue());
    snapshot_pb->set_mean(snapshot.MeanValue());
    snapshot_pb->set_percentile_75(snapshot.ValueAtPercentile(75));
    snapshot_pb->set_percentile_95(snapshot.ValueAtPercentile(95));
    snapshot_pb->set_percentile_99(snapshot.ValueAtPercentile(99));
    snapshot_pb->set_percentile_99_9(snapshot.ValueAtPercentile(99.9));
    snapshot_pb->set_percentile_99_99(snapshot.ValueAtPercentile(99.99));
    snapshot_pb->set_max(snapshot.MaxValue());

    if (opts.include_raw_histograms) {
      RecordedValuesIterator iter(&snapshot);
      while (iter.HasNext()) {
        HistogramIterationValue value;
        RETURN_NOT_OK(iter.Next(&value));
        snapshot_pb->add_values(value.value_iterated_to);
        snapshot_pb->add_counts(value.count_at_value_iterated_to);
      }
    }
  }
  return Status::OK();
}

uint64_t Histogram::CountInBucketForValueForTests(uint64_t value) const {
  return histogram_->CountInBucketForValue(value);
}

uint64_t Histogram::TotalCount() const {
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

ScopedLatencyMetric::ScopedLatencyMetric(Histogram* latency_hist)
  : latency_hist_(latency_hist) {
  if (latency_hist_) {
    time_started_ = MonoTime::Now();
  }
}

ScopedLatencyMetric::~ScopedLatencyMetric() {
  if (latency_hist_ != nullptr) {
    MonoTime time_now = MonoTime::Now();
    latency_hist_->Increment((time_now - time_started_).ToMicroseconds());
  }
}

} // namespace kudu
