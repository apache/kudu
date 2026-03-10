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

#include <iosfwd>
#include <string>
#include <unordered_map>
#include <unordered_set>

class PrometheusWriter {

 public:
  explicit PrometheusWriter(std::ostringstream* output): output_(output) {}

  void WriteEntry(const std::string& data);

  // Returns true if HELP/TYPE for the given metric name has not been written yet,
  // and marks it as written. Returns false if already written (i.e. skip duplicate).
  bool ShouldWriteHelpAndType(const std::string& metric_name);

 private:
  std::ostringstream* output_;
  std::unordered_set<std::string> written_help_types_;
};

// Metric entity IDs for server-level metric entities.
extern const char kMetricEntityIdMaster[];      // "kudu.master"
extern const char kMetricEntityIdTabletServer[]; // "kudu.tabletserver"

// Escape a string value for use in a Prometheus label value.
// Escapes backslash, double-quote, and newline characters.
std::string EscapePrometheusLabelValue(const std::string& value);

// Build a Prometheus labels string from entity type, id, and attributes.
// The returned string has the form: type="tablet",tablet_id="xxx",table_id="yyy",...
// without surrounding braces and without a trailing comma.
std::string BuildPrometheusLabels(
    const std::string& entity_type,
    const std::string& entity_id,
    const std::unordered_map<std::string, std::string>& attrs);

// Normalize a Prometheus labels string for injection before additional labels.
//
// Many Prometheus output format strings append more labels after an existing labels
// string (e.g. "{<labels>unit_type=\"bytes\"}"). To avoid relying on a silent
// trailing-comma contract, call this helper to ensure the returned string is either
// empty or ends with a comma.
inline std::string PrometheusLabelPrefixForInjection(const std::string& labels) {
    if (labels.empty()) {
        return "";
    }
    if (labels.back() == ',') {
        return labels;
    }
    return labels + ",";
}

// Build Prometheus label string for server-level entities (master/tserver).
// Returns labels of the form: type="master",id="<uuid>"
std::string BuildServerPrometheusLabels(
    const std::string& entity_id,
    const std::unordered_map<std::string, std::string>& attrs);

// Build Prometheus label string for table/tablet entities.
// Returns labels of the form: type="<entity_type>",id="<entity_id>"[,table_id=...,table_name=...]
std::string BuildTableTabletPrometheusLabels(
    const std::string& entity_type,
    const std::string& entity_id,
    const std::unordered_map<std::string, std::string>& attrs);
