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

#include <algorithm>
#include <fstream> // IWYU pragma: keep
#include <functional>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/diagnostics_log_parser.h"
#include "kudu/tools/tool_action.h"
#include "kudu/util/regex.h"
#include "kudu/util/status.h"

DEFINE_string(table_ids, "",
              "comma-separated list of table identifiers to aggregate table "
              "metrics across; defaults to aggregating all tables");
DEFINE_string(tablet_ids, "",
              "comma-separated list of tablet identifiers to aggregate tablet "
              "metrics across; defaults to aggregating all tablets");
DEFINE_string(simple_metrics, "",
              "comma-separated list of metrics to parse, of the format "
              "<entity>.<metric name>[:<display name>], where <entity> must be "
              "one of 'server', 'table', or 'tablet', and <metric name> refers "
              "to the metric name; <display name> is optional and stands for "
              "the column name/tag that the metric is output with");
DEFINE_string(rate_metrics, "",
              "comma-separated list of metrics to compute the rates of");
DEFINE_string(histogram_metrics, "",
              "comma-separated list of histogram metrics to parse "
              "percentiles for");

DECLARE_int64(timeout_ms);
DECLARE_string(format);

using std::ifstream;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

constexpr const char* const kLogPathArg = "path";
constexpr const char* const kLogPathArgDesc =
    "Path(s) to log file(s) to parse, separated by a whitespace, if many";

Status ParseStacksFromPath(StackDumpingLogVisitor* dlv, const string& path) {
  LogFileParser fp(dlv, path);
  RETURN_NOT_OK(fp.Init());
  return fp.Parse();
}

Status ParseStacks(const RunnerContext& context) {
  vector<string> paths = context.variadic_args;
  // The file names are such that lexicographic sorting reflects
  // timestamp-based sorting.
  std::sort(paths.begin(), paths.end());
  StackDumpingLogVisitor dlv;
  for (const auto& path : paths) {
    RETURN_NOT_OK_PREPEND(ParseStacksFromPath(&dlv, path),
                          Substitute("failed to parse stacks from $0", path));
  }
  return Status::OK();
}

struct MetricNameParams {
  string entity;
  string metric_name;
  string display_name;
};

// Parses a metric parameter of the form <entity>.<metric>:<display name>.
Status SplitMetricNameParams(const string& name_str, MetricNameParams* out) {

  vector<string> matches;
  static KuduRegex re("([-_[:alpha:]]+)\\.([-_[:alpha:]]+):?([-_[:alpha:]]+)?", 3);
  if (!re.Match(name_str, &matches)) {
    return Status::InvalidArgument(Substitute("could not parse metric "
       "parameter. Expected <entity>.<metric>:<display name>, got $0", name_str));
  }
  DCHECK_EQ(3, matches.size());
  *out = { std::move(matches[0]), std::move(matches[1]), std::move(matches[2]) };
  return Status::OK();
}

// Splits the input string by ','.
vector<string> SplitOnComma(const string& str) {
  return Split(str, ",", strings::SkipEmpty());
}

// Takes a metric name parameter string and inserts the metric names into the
// name map.
Status AddMetricsToDisplayNameMap(const string& metric_params_str,
                                  MetricsCollectingOpts::NameMap* name_map) {
  if (metric_params_str.empty()) {
    return Status::OK();
  }
  vector<string> metric_params = SplitOnComma(metric_params_str);
  for (const auto& metric_param : metric_params) {
    MetricNameParams params;
    RETURN_NOT_OK(SplitMetricNameParams(metric_param, &params));
    if (params.display_name.empty()) {
      params.display_name = params.metric_name;
    }
    const string& entity = params.entity;
    if (entity != "server" && entity != "table" && entity != "tablet") {
      return Status::InvalidArgument(
          Substitute("unexpected entity type: $0", entity));
    }
    const string& metric_name = params.metric_name;
    const string full_name = Substitute("$0.$1", entity, metric_name);
    if (!EmplaceIfNotPresent(name_map, full_name, std::move(params.display_name))) {
      return Status::InvalidArgument(
          Substitute("duplicate metric name for $0.$1", entity, metric_name));
    }
  }
  return Status::OK();
}

Status ParseMetrics(const RunnerContext& context) {
  // Parse the metric name parameters.
  MetricsCollectingOpts opts;
  RETURN_NOT_OK(AddMetricsToDisplayNameMap(FLAGS_simple_metrics,
                                           &opts.simple_metric_names));
  RETURN_NOT_OK(AddMetricsToDisplayNameMap(FLAGS_rate_metrics,
                                           &opts.rate_metric_names));
  RETURN_NOT_OK(AddMetricsToDisplayNameMap(FLAGS_histogram_metrics,
                                           &opts.hist_metric_names));

  // Parse the table ids.
  if (!FLAGS_table_ids.empty()) {
    auto ids = SplitOnComma(FLAGS_table_ids);
    std::move(ids.begin(), ids.end(),
              std::inserter(opts.table_ids, opts.table_ids.end()));
  }
  // Parse the tablet ids.
  if (!FLAGS_tablet_ids.empty()) {
    auto ids = SplitOnComma(FLAGS_tablet_ids);
    std::move(ids.begin(), ids.end(),
              std::inserter(opts.tablet_ids, opts.tablet_ids.end()));
  }

  // Sort the files lexicographically to put them in increasing timestamp order.
  vector<string> paths = context.variadic_args;
  std::sort(paths.begin(), paths.end());
  MetricCollectingLogVisitor mlv(std::move(opts));
  for (const string& path : paths) {
    LogFileParser lp(&mlv, path);
    Status s = lp.Init().AndThen([&lp] {
      return lp.Parse();
    });
    WARN_NOT_OK(s, Substitute("Skipping file $0", path));
  }
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildDiagnoseMode() {
  unique_ptr<Action> parse_stacks =
      ActionBuilder("parse_stacks", &ParseStacks)
      .Description("Parse sampled stack traces out of a diagnostics log")
      .AddRequiredVariadicParameter({ kLogPathArg, kLogPathArgDesc })
      .Build();

  // TODO(awong): add timestamp bounds
  unique_ptr<Action> parse_metrics =
      ActionBuilder("parse_metrics", &ParseMetrics)
      .Description("Parse metrics out of a diagnostics log")
      .AddRequiredVariadicParameter({ kLogPathArg, kLogPathArgDesc })
      .AddOptionalParameter("tablet_ids")
      .AddOptionalParameter("simple_metrics")
      .AddOptionalParameter("rate_metrics")
      .AddOptionalParameter("histogram_metrics")
      .Build();

  return ModeBuilder("diagnose")
      .Description("Diagnostic tools for Kudu servers and clusters")
      .AddAction(std::move(parse_stacks))
      .AddAction(std::move(parse_metrics))
      .Build();
}

} // namespace tools
} // namespace kudu
