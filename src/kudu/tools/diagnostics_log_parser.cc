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

#include "kudu/tools/diagnostics_log_parser.h"

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/errno.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/status.h"

using std::array;
using std::cout;
using std::endl;
using std::ifstream;
using std::string;
using std::vector;
using strings::Substitute;


namespace kudu {
namespace tools {

namespace {

Status ParseStackGroup(const rapidjson::Value& group_json,
                       StacksRecord::Group* group) {
  DCHECK(group);
  StacksRecord::Group ret;
  if (PREDICT_FALSE(!group_json.IsObject())) {
    return Status::InvalidArgument("expected stacks groups to be JSON objects");
  }
  if (!group_json.HasMember("tids") || !group_json.HasMember("stack")) {
    return Status::InvalidArgument("expected stacks groups to have frames and tids");
  }

  // Parse the tids.
  const auto& tids = group_json["tids"];
  if (PREDICT_FALSE(!tids.IsArray())) {
    return Status::InvalidArgument("expected 'tids' to be an array");
  }
  ret.tids.reserve(tids.Size());
  for (const auto* tid = tids.Begin();
       tid != tids.End();
       ++tid) {
    if (PREDICT_FALSE(!tid->IsNumber())) {
      return Status::InvalidArgument("expected 'tids' elements to be numeric");
    }
    ret.tids.push_back(tid->GetInt64());
  }

  // Parse and symbolize the stack trace itself.
  const auto& stack = group_json["stack"];
  if (PREDICT_FALSE(!stack.IsArray())) {
    return Status::InvalidArgument("expected 'stack' to be an array");
  }
  for (const auto* frame = stack.Begin();
       frame != stack.End();
       ++frame) {
    if (PREDICT_FALSE(!frame->IsString())) {
      return Status::InvalidArgument("expected 'stack' elements to be strings");
    }
    ret.frame_addrs.emplace_back(frame->GetString());
  }
  *group = std::move(ret);
  return Status::OK();
}

}  // anonymous namespace

const char* RecordTypeToString(RecordType r) {
  switch (r) {
    case RecordType::kStacks: return "stacks"; break;
    case RecordType::kSymbols: return "symbols"; break;
    case RecordType::kMetrics: return "metrics"; break;
    case RecordType::kUnknown: return "<unknown>"; break;
  }
  return "<unreachable>";
}

std::ostream& operator<<(std::ostream& o, RecordType r) {
  return o << RecordTypeToString(r);
}

Status SymbolsRecord::FromParsedLine(const ParsedLine& pl) {
  DCHECK_EQ(RecordType::kSymbols, pl.type());

  if (!pl.json()->IsObject()) {
    return Status::InvalidArgument("expected symbols data to be a JSON object");
  }
  for (auto it = pl.json()->MemberBegin();
       it != pl.json()->MemberEnd();
       ++it) {
    if (PREDICT_FALSE(!it->value.IsString())) {
      return Status::InvalidArgument("expected symbol values to be strings");
    }
    InsertIfNotPresent(&addr_to_symbol, it->name.GetString(), it->value.GetString());
  }
  return Status::OK();
}

Status MetricsRecord::FromParsedLine(const MetricsCollectingOpts& opts, const ParsedLine& pl) {
  DCHECK_EQ(RecordType::kMetrics, pl.type());
  if (!pl.json()->IsArray()) {
    return Status::InvalidArgument("expected a metric JSON array");
  }

  MetricToEntities m;
  // Initialize the metric maps based on the specified options.
  for (const auto& metric_to_display_name : opts.simple_metric_names) {
    const auto& full_metric_name = metric_to_display_name.first;
    InsertIfNotPresent(&m, full_metric_name, EntityIdToValue());
  }
  for (const auto& metric_to_display_name : opts.rate_metric_names) {
    const auto& full_metric_name = metric_to_display_name.first;
    InsertIfNotPresent(&m, full_metric_name, EntityIdToValue());
  }
  for (const auto& metric_to_display_name : opts.hist_metric_names) {
    const auto& full_metric_name = metric_to_display_name.first;
    InsertIfNotPresent(&m, full_metric_name, EntityIdToValue());
  }

  // Each json blob has a list of metrics for entities (see below for the
  // contents of each <entity>):
  //   [{<entity>}, {<entity>}, {<entity>}]
  //
  // Iterate through each entity blob and pick out the metrics.
  for (const auto* entity_json = pl.json()->Begin();
       entity_json != pl.json()->End();
       ++entity_json) {
    if (!entity_json->IsObject()) {
      return Status::InvalidArgument("expected JSON object");
    }
    if (!entity_json->HasMember("type") ||
        !entity_json->HasMember("id") ||
        !entity_json->HasMember("metrics")) {
      return Status::InvalidArgument(
          Substitute("incomplete entity entry: $0", string(entity_json->GetString())));
    }
    const string entity_type = (*entity_json)["type"].GetString();
    const string entity_id = (*entity_json)["id"].GetString();
    if (entity_type != "tablet" && entity_type != "table" && entity_type != "server") {
      return Status::InvalidArgument(
          Substitute("unexpected entity type: $0", entity_type));
    }
    if (entity_type == "table" &&
        !opts.table_ids.empty() && !ContainsKey(opts.table_ids, entity_id)) {
      // If the table id doesn't match the user's filter, ignore it. If the
      // list of table ids is empty, no table ids are ignored.
      continue;
    }
    if (entity_type == "tablet" &&
        !opts.tablet_ids.empty() && !ContainsKey(opts.tablet_ids, entity_id)) {
      // If the tablet id doesn't match the user's filter, ignore it. If the
      // tablet list is empty, no tablet ids are ignored.
      continue;
    }

    // Each entity has a list of metrics:
    //   [{"name":"<metric_name>","value":"<metric_value>"},
    //    {"name":"<hist_metric_name>","counts":"<hist_metric_counts>"]
    const auto& metrics = (*entity_json)["metrics"];
    if (!metrics.IsArray()) {
      return Status::InvalidArgument(
          Substitute("expected metric array: $0", metrics.GetString()));
    }
    for (const auto* metric_json = metrics.Begin();
         metric_json != metrics.End();
         ++metric_json) {
      if (!metric_json->HasMember("name")) {
        return Status::InvalidArgument(
            Substitute("expected 'name' field in metric entry: $0", metric_json->GetString()));
      }
      const string& metric_name = (*metric_json)["name"].GetString();
      const string& full_metric_name = Substitute("$0.$1", entity_type, metric_name);
      EntityIdToValue* entity_id_to_value = FindOrNull(m, full_metric_name);
      if (!entity_id_to_value) {
        // We're looking at a metric that the user hasn't requested. Ignore
        // this entry.
        continue;
      }
      MetricValue v;
      Status s = v.FromJson(*metric_json);
      if (!s.ok()) {
        continue;
      }
      // We expect that in a given line, each entity reports a given metric
      // only once. In case this isn't true, we just update the value.
      EmplaceOrUpdate(entity_id_to_value, entity_id, std::move(v));
    }
  }
  metric_to_entities.swap(m);
  timestamp = pl.timestamp();
  return Status::OK();
}

MetricValue::MetricValue()
    : type_(MetricType::kUninitialized) {
}

Status MetricValue::FromJson(const rapidjson::Value& metric_json) {
  DCHECK(MetricType::kUninitialized == type_);
  DCHECK(!counts_.has_value());
  DCHECK(!value_.has_value());
  if (metric_json.HasMember("counts") && metric_json.HasMember("values")) {
    // Add the counts from histogram metrics.
    const rapidjson::Value& counts_json = metric_json["counts"];
    const rapidjson::Value& values_json = metric_json["values"];
    if (!counts_json.IsArray() || !values_json.IsArray()) {
      return Status::InvalidArgument(
          Substitute("expected 'counts' and 'values' to be arrays: $0",
                     metric_json.GetString()));
    }
    if (counts_json.Size() != values_json.Size()) {
      return Status::InvalidArgument(
          "expected 'counts' and 'values' to be the same size");
    }
    std::map<int64_t, int> counts;
    for (int i = 0; i < counts_json.Size(); i++) {
      int64_t v = values_json[i].GetInt64();
      int c = counts_json[i].GetInt();
      InsertOrUpdate(&counts, v, c);
    }
    counts_ = std::move(counts);
    type_ = MetricType::kHistogram;
  } else if (metric_json.HasMember("value")) {
    const rapidjson::Value& value_json = metric_json["value"];
    if (!value_json.IsInt64()) {
      return Status::InvalidArgument("expected 'value' to be an int type");
    }
    // Add the value from plain metrics.
    value_ = value_json.GetInt64();
    type_ = MetricType::kPlain;
  } else {
    return Status::InvalidArgument(
        Substitute("unexpected metric formatting: $0", metric_json.GetString()));
  }
  return Status::OK();
}

MetricCollectingLogVisitor::MetricCollectingLogVisitor(MetricsCollectingOpts opts)
    : opts_(std::move(opts)) {
  vector<string> display_names = { "timestamp" };
  // Create an initial entity-to-value map for every metric of interest.

  for (const auto& full_and_display : opts_.simple_metric_names) {
    const auto& full_name = full_and_display.first;
    const auto& display_name = full_and_display.second;
    LOG(INFO) << Substitute("collecting simple metric $0 as $1", full_name, display_name);
    InsertIfNotPresent(&metric_to_entities_, full_name, EntityIdToValue());
    display_names.emplace_back(display_name);
  }
  for (const auto& full_and_display : opts_.rate_metric_names) {
    const auto& full_name = full_and_display.first;
    const auto& display_name = full_and_display.second;
    LOG(INFO) << Substitute("collecting rate metric $0 as $1", full_name, display_name);
    InsertIfNotPresent(&metric_to_entities_, full_name, EntityIdToValue());
    display_names.emplace_back(display_name);
  }
  for (const auto& full_and_display : opts_.hist_metric_names) {
    const auto& full_name = full_and_display.first;
    const auto& display_name = full_and_display.second;
    LOG(INFO) << Substitute("collecting histogram metric $0 as $1", full_name, display_name);
    InsertIfNotPresent(&metric_to_entities_, full_name, EntityIdToValue());
    // TODO(awong): this is ugly.
    display_names.emplace_back(Substitute("$0_count", display_name));
    display_names.emplace_back(Substitute("$0_min", display_name));
    display_names.emplace_back(Substitute("$0_p50", display_name));
    display_names.emplace_back(Substitute("$0_p75", display_name));
    display_names.emplace_back(Substitute("$0_p95", display_name));
    display_names.emplace_back(Substitute("$0_p99", display_name));
    display_names.emplace_back(Substitute("$0_p99_99", display_name));
    display_names.emplace_back(Substitute("$0_max", display_name));
  }
  cout << JoinStrings(display_names, "\t") << std::endl;
}
Status MetricCollectingLogVisitor::ParseRecord(const ParsedLine& pl) {
  // Do something with the metric record.
  if (pl.type() == RecordType::kMetrics) {
    MetricsRecord mr;
    RETURN_NOT_OK(mr.FromParsedLine(opts_, pl));
    RETURN_NOT_OK(VisitMetricsRecord(mr));
    UpdateWithMetricsRecord(mr);
  }
  return Status::OK();
}

Status MetricCollectingLogVisitor::VisitMetricsRecord(const MetricsRecord& mr) {
  // Iterate through the user-requested metrics and display what we need to, as
  // determined by 'opts_'.
  cout << mr.timestamp;
  for (const auto& full_and_display : opts_.simple_metric_names) {
    const auto& full_name = full_and_display.first;
    int64_t sum = SumPlainWithMetricRecord(mr, full_name);
    cout << Substitute("\t$0", sum);
  }
  const double time_delta_secs =
      static_cast<double>(mr.timestamp - last_visited_timestamp_) / 1e6;
  for (const auto& full_and_display : opts_.rate_metric_names) {
    // If this is the first record we're visiting, we can't compute a rate.
    const auto& full_name = full_and_display.first;
    int64_t sum = SumPlainWithMetricRecord(mr, full_name);
    if (last_visited_timestamp_ == 0) {
      InsertOrDie(&rate_metric_prev_sum_, full_name, sum);
      cout << "\t0";
      continue;
    }
    int64_t prev_sum = FindOrDie(rate_metric_prev_sum_, full_name);
    cout << Substitute("\t$0", static_cast<double>(sum - prev_sum) / time_delta_secs);
    InsertOrUpdate(&rate_metric_prev_sum_, full_name, sum);
  }
  for (const auto& full_and_display : opts_.hist_metric_names) {
    const auto& full_name = full_and_display.first;
    const auto& mr_entities_to_vals = FindOrDie(mr.metric_to_entities, full_name);
    const auto& entities_to_vals = FindOrDie(metric_to_entities_, full_name);
    std::map<int64_t, int> all_counts;
    // Aggregate any counts that didn't change, and thus, are not in 'mr'.
    for (const auto& [entity, val] : entities_to_vals) {
      CHECK(val.counts_);
      if (!ContainsKey(mr_entities_to_vals, entity)) {
        MergeTokenCounts(&all_counts, *val.counts_);
      }
    }
    // Now aggregate the counts that did.
    for (const auto& [_, val] : mr_entities_to_vals) {
      CHECK(val.counts_);
      MergeTokenCounts(&all_counts, *val.counts_);
    }
    // We need to at least display a zero count if the metric didn't show up at
    // all.
    if (all_counts.empty()) {
      EmplaceOrDie(&all_counts, 0, 1);
    }
    int counts_size = 0;
    for (const auto& [_, count]: all_counts) {
      counts_size += count;
    }
    // TODO(awong): would using HdrHistogram be cleaner?
    const vector<int> quantiles = { 0,
                                    counts_size / 2,
                                    static_cast<int>(counts_size * 0.75),
                                    static_cast<int>(counts_size * 0.95),
                                    static_cast<int>(counts_size * 0.99),
                                    static_cast<int>(counts_size * 0.9999),
                                    counts_size };
    int quantile_idx = 0;
    int count_idx = 0;
    // Iterate through all the counts, aggregating the values.
    cout << Substitute("\t$0", counts_size);
    for (const auto& vals_and_counts : all_counts) {
      count_idx += vals_and_counts.second;
      // Keep on printing for as long as this count index falls within the
      // current quantile.
      while (quantile_idx < quantiles.size() &&
             count_idx >= quantiles[quantile_idx]) {
        cout << Substitute("\t$0", vals_and_counts.first);
        quantile_idx++;
      }
    }
  }
  cout << std::endl;

  return Status::OK();
}

void MetricCollectingLogVisitor::UpdateWithMetricsRecord(const MetricsRecord& mr) {
  for (const auto& [metric_name, mr_entities_map] : mr.metric_to_entities) {
    // Update the visitor's internal map with the metrics from the record.
    // Note: The metric record should only have parsed user-requested metrics
    // that the visitor has in its internal map, so we can FindOrDie here.
    auto& visitor_entities_map = FindOrDie(metric_to_entities_, metric_name);
    for (const auto& [id, val] : mr_entities_map) {
      InsertOrUpdate(&visitor_entities_map, id, val);
    }
  }
  last_visited_timestamp_ = mr.timestamp;
}

int64_t MetricCollectingLogVisitor::SumPlainWithMetricRecord(
    const MetricsRecord& mr, const string& full_metric_name) const {
  const auto& mr_entities_to_vals = FindOrDie(mr.metric_to_entities, full_metric_name);
  const auto& entities_to_vals = FindOrDie(metric_to_entities_, full_metric_name);
  int64_t sum = 0;
  // Add all of the values for entities that didn't change, and are thus, not
  // included in the record.
  for (const auto& e : entities_to_vals) {
    if (!ContainsKey(mr_entities_to_vals, e.first)) {
      CHECK(e.second.value_);
      sum += *e.second.value_;
    }
  }
  // Now add the values for those that did.
  for (const auto& e : mr_entities_to_vals) {
    CHECK(e.second.value_);
    sum += *e.second.value_;
  }
  return sum;
}

Status StackDumpingLogVisitor::ParseRecord(const ParsedLine& pl) {
  // We're not going to do any fancy parsing, so do it up front with the default
  // json parsing.
  switch (pl.type()) {
    case RecordType::kSymbols: {
      SymbolsRecord sr;
      RETURN_NOT_OK(sr.FromParsedLine(pl));
      VisitSymbolsRecord(sr);
      break;
    }
    case RecordType::kStacks: {
      StacksRecord sr;
      RETURN_NOT_OK(sr.FromParsedLine(pl));
      VisitStacksRecord(sr);
      break;
    }
    default:
      break;
  }
  return Status::OK();
}

void StackDumpingLogVisitor::VisitSymbolsRecord(const SymbolsRecord& sr) {
  InsertOrUpdateMany(&symbols_, sr.addr_to_symbol.begin(), sr.addr_to_symbol.end());
}

void StackDumpingLogVisitor::VisitStacksRecord(const StacksRecord& sr) {
  static const string kUnknownSymbol = "<unknown>";

  if (!first_) {
    cout << endl << endl;
  }
  first_ = false;
  cout << "Stacks at " << sr.date_time << " (" << sr.reason << "):" << endl;
  for (const auto& group : sr.groups) {
    cout << "  tids=["
          << JoinMapped(group.tids, [](int t) { return std::to_string(t); }, ",")
          << "]" << endl;
    for (const auto& addr : group.frame_addrs) {
      // NOTE: passing 'kUnknownSymbol' as the default instead of a "foo"
      // literal is important to avoid capturing a reference to a temporary.
      // See the FindWithDefault() docs for details.
      const auto& sym = FindWithDefault(symbols_, addr, kUnknownSymbol);
      cout << std::setw(20) << addr << " " << sym << endl;
    }
  }
}

Status ParsedLine::Parse() {
  RETURN_NOT_OK(ParseHeader());
  return ParseJson();
}

Status ParsedLine::ParseHeader() {
  // Lines have the following format:
  //     I0220 17:38:09.950546 metrics 1519177089950546 <json blob>...
  if (line_.empty() || line_[0] != 'I') {
    return Status::InvalidArgument("lines must start with 'I'");
  }

  array<StringPiece, 5> fields = strings::Split(
      line_, strings::delimiter::Limit(" ", 4));
  fields[0].remove_prefix(1); // Remove the 'I'.
  // Sanity check the microsecond timestamp.
  // Eventually, it should be used when processing metrics records.
  int64_t time_us;
  if (!safe_strto64(fields[3].data(), fields[3].size(), &time_us)) {
    return Status::InvalidArgument("invalid timestamp", fields[3]);
  }
  timestamp_ = time_us;
  date_ = fields[0];
  time_ = fields[1];
  if (fields[2] == "symbols") {
    type_ = RecordType::kSymbols;
  } else if (fields[2] == "stacks") {
    type_ = RecordType::kStacks;
  } else if (fields[2] == "metrics") {
    type_ = RecordType::kMetrics;
  } else {
    type_ = RecordType::kUnknown;
  }
  json_.emplace(fields[4].ToString());
  return Status::OK();
}

Status ParsedLine::ParseJson() {
  // TODO(todd) JsonReader should be able to parse from a StringPiece
  // directly instead of making the copy here.
  Status s = json_->Init();
  if (!s.ok()) {
    json_.reset();
    return s.CloneAndPrepend("invalid JSON payload");
  }
  return Status::OK();
}

string ParsedLine::date_time() const {
  return Substitute("$0 $1", date_, time_);
}

Status StacksRecord::FromParsedLine(const ParsedLine& pl) {
  date_time = pl.date_time();

  const rapidjson::Value& json = *pl.json();
  if (!json.IsObject()) {
    return Status::InvalidArgument("expected stacks data to be a JSON object");
  }

  // Parse reason if present. If not, we'll just leave it empty.
  if (json.HasMember("reason")) {
    if (!json["reason"].IsString()) {
      return Status::InvalidArgument("expected stacks 'reason' to be a string");
    }
    reason = json["reason"].GetString();
  }

  // Parse groups.
  if (PREDICT_FALSE(!json.HasMember("groups"))) {
    return Status::InvalidArgument("no 'groups' field in stacks object");
  }
  const auto& groups_json = json["groups"];
  if (!groups_json.IsArray()) {
    return Status::InvalidArgument("'groups' field should be an array");
  }

  for (const rapidjson::Value* group = groups_json.Begin();
       group != groups_json.End();
       ++group) {
    StacksRecord::Group g;
    RETURN_NOT_OK(ParseStackGroup(*group, &g));
    groups.emplace_back(std::move(g));
  }
  return Status::OK();
}

Status LogFileParser::Init() {
  errno = 0;
  if (!fstream_.is_open() || !HasNext()) {
    return Status::IOError(ErrnoToString(errno));
  }
  return Status::OK();
}

bool LogFileParser::HasNext() {
  return fstream_.peek() != EOF;
}

Status LogFileParser::ParseLine() {
  DCHECK(HasNext());
  ++line_number_;
  string line;
  std::getline(fstream_, line);
  ParsedLine pl(std::move(line));
  const string error_msg = Substitute("failed to parse line $0 in file $1",
                                      line_number_, path_);
  RETURN_NOT_OK_PREPEND(pl.Parse(), error_msg);
  RETURN_NOT_OK_PREPEND(log_visitor_->ParseRecord(pl), error_msg);
  return Status::OK();
}

Status LogFileParser::Parse() {
  string line;
  Status s;
  while (HasNext()) {
    s = ParseLine();
    if (s.IsEndOfFile()) {
      LOG(INFO) << "Reached end of time range";
      return Status::OK();
    }
    RETURN_NOT_OK(s);
  }
  return Status::OK();
}

} // namespace tools
} // namespace kudu

