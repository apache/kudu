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

#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/jsonreader.h" // IWYU pragma: keep
#include "kudu/util/status.h"

namespace kudu {

namespace tools {

// One of the record types from the log.
enum class RecordType {
  kSymbols,
  kStacks,
  kMetrics,
  kUnknown
};

const char* RecordTypeToString(RecordType r);

std::ostream& operator<<(std::ostream& o, RecordType r);

// A parsed line from the diagnostics log.
//
// Each line contains a timestamp, a record type, and some JSON data.
class ParsedLine {
 public:
  explicit ParsedLine(std::string line)
      : line_(std::move(line)) {
  }

  // Parse a line from the diagnostics log.
  Status Parse();

  Status ParseHeader();
  Status ParseJson();

  RecordType type() const { return type_; }

  const rapidjson::Value* json() const {
    CHECK(json_);
    return json_->root();
  }

  std::string date_time() const;

  int64_t timestamp() const { return timestamp_; }

 private:
  const std::string line_;
  RecordType type_;

  // date_ and time_ point to substrings of line_.
  StringPiece date_;
  StringPiece time_;
  int64_t timestamp_;

  // A JsonReader initialized from the most recent line.
  // This will be 'none' before any lines have been read.
  std::optional<JsonReader> json_;
};

// A stack sample from the log.
struct StacksRecord {
  // A group of threads which share the same stack trace.
  struct Group {
    // The thread IDs in this group.
    std::vector<int> tids;
    // The non-symbolized addresses forming the stack trace.
    std::vector<std::string> frame_addrs;
  };

  Status FromParsedLine(const ParsedLine& pl);

  // The time the stack traces were collected.
  std::string date_time;

  // The reason for stack trace collection.
  std::string reason;

  // The grouped threads with their stack traces.
  std::vector<Group> groups;
};

// Interface for consuming the parsed records from a diagnostics log.
class LogVisitor {
 public:
  virtual ~LogVisitor() {}
  virtual Status ParseRecord(const ParsedLine& pl) = 0;
};

enum class MetricType {
  kUninitialized,
  // A metric represented by a single value.
  kPlain,
  // A metric represented by counts of values.
  kHistogram,
};

// A value that a metric can have. Depending on the type of metric this is for,
// the underlying value may be represented by a single value or by many (e.g.
// in the case of histograms).
class MetricValue {
 public:
  MetricValue();

  // Sets the metric values based on the input 'metric_json'.
  Status FromJson(const rapidjson::Value& metric_json);

  // The type of this metric value.
  MetricType type() const { return type_; }
 protected:
  friend class MetricCollectingLogVisitor;
  MetricType type_;

  std::optional<int64_t> value_;
  std::optional<std::map<int64_t, int>> counts_;
};

// For a given metric, a collection of entity IDs and their metric values.
typedef std::unordered_map<std::string, MetricValue> EntityIdToValue;

// Mapping from a full metric name to the collection of entity IDs and their
// metric values, i.e.
//   { <entity type>.<metric name>:string =>
//       { <entity id>:string => <metric value>:MetricValue } }
typedef std::unordered_map<std::string, EntityIdToValue> MetricToEntities;

struct MetricsCollectingOpts {
  // Maps the full metric name to its display name.
  // The full metric name refers to "<entity type>.<metric name>".
  typedef std::unordered_map<std::string, std::string> NameMap;

  // The metric names and display names of the metrics of interest.
  NameMap simple_metric_names;
  NameMap rate_metric_names;
  NameMap hist_metric_names;

  // Set of table IDs whose metrics that should be aggregated.
  // If empty, all tables' metrics are aggregated.
  std::set<std::string> table_ids;

  // Set of tablet IDs whose metrics that should be aggregated.
  // If empty, all tablets' metrics are aggregated.
  std::set<std::string> tablet_ids;
};

// A record containing the metrics for a single line.
struct MetricsRecord {
  // Populate this record with the contents of 'pl', only considering metrics
  // specified by 'opts'.
  Status FromParsedLine(const MetricsCollectingOpts& opts, const ParsedLine& pl);

  // Maps the full metric name to the mapping between entity ID and metric for
  // that entity.
  MetricToEntities metric_to_entities;

  // The timestamp associated with this record.
  int64_t timestamp;
};

// LogVisitor that collects metrics, tracking values, aggregating counts, etc.
// and prints them out.
//
// A single MetricsCollectingLogVisitor may be used by multiple LogFileParsers.
class MetricCollectingLogVisitor : public LogVisitor {
 public:
  // Initializes the internal map to include the metrics specified by 'opts'.
  explicit MetricCollectingLogVisitor(MetricsCollectingOpts opts);

  // Takes a parsed line and parses its metric record if one exists. If 'pl'
  // doesn't contain a metric record, this is a no-op.
  Status ParseRecord(const ParsedLine& pl) override;
 private:
  // Prints the appropriate metrics from 'mr' and this visitor's internal maps.
  Status VisitMetricsRecord(const MetricsRecord& mr);

  // Updates the internal maps to include the metrics in 'mr'.
  void UpdateWithMetricsRecord(const MetricsRecord& mr);

  // Calculate the sum of the plain metric (i.e. non-histogram) specified by
  // 'full_metric_name', based on the existing values in our internal map and
  // including any new values for entities in 'mr'.
  int64_t SumPlainWithMetricRecord(const MetricsRecord& mr,
                                   const std::string& full_metric_name) const;

  // Maps the full metric name to the mapping between entity IDs and their
  // metric value. As the visitor visits new metrics records, this gets updated
  // with the most up-to-date values.
  //
  // Note: we need to track per-entity metrics because, when logging, Kudu may
  // omit metrics for entities if they don't change.
  MetricToEntities metric_to_entities_;

  // Maps the full metric name of a rate metric to the previous sum computed
  // for that metric by this visitor.
  std::map<std::string, int64_t> rate_metric_prev_sum_;


  // A JsonReader initialized from the most recent line.
  // This will be 'none' before any lines have been read.
  std::optional<JsonReader> json_;
  //
  // The timestamp of the last visited metrics record.
  int64_t last_visited_timestamp_ = 0;

  const MetricsCollectingOpts opts_;
};

struct SymbolsRecord {
  Status FromParsedLine(const ParsedLine& pl);
  std::unordered_map<std::string, std::string> addr_to_symbol;
};

// LogVisitor implementation which dumps the parsed stack records to std::cout.
class StackDumpingLogVisitor : public LogVisitor {
 public:
  Status ParseRecord(const ParsedLine& pl) override;

 private:
  void VisitSymbolsRecord(const SymbolsRecord& sr);
  void VisitStacksRecord(const StacksRecord& sr);
  // True when we have not yet output any data.
  bool first_ = true;
  // Map from symbols to name.
  std::unordered_map<std::string, std::string> symbols_;
};

// Parser for a diagnostic log files that may include stacks or metrics logs.
//
// This instance follows a 'SAX' model. As records are available, the appropriate
// functions are invoked on the visitor provided in the constructor.
class LogFileParser {
 public:
  explicit LogFileParser(LogVisitor* lv, std::string path)
      : path_(std::move(path)),
        fstream_(path_),
        log_visitor_(lv) {}

  // Initializes internal state, e.g. the file stream for the log file.
  Status Init();

  // Returns whether or not the underlying file has more lines to parse.
  bool HasNext();

  // Parses the rest of the lines in the file.
  Status Parse();

 private:
  // Parses the next line in the file. Should only be called if HasNext()
  // returns true.
  Status ParseLine();

  size_t line_number_ = 0;
  const std::string path_;
  std::ifstream fstream_;

  // Visitor for doing something with each parsed line.
  LogVisitor* log_visitor_;
};

} // namespace tools
} // namespace kudu
