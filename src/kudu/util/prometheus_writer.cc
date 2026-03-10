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

#include "kudu/util/prometheus_writer.h"

#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/map-util.h"

using std::string;
using std::unordered_map;

const char kMetricEntityIdMaster[] = "kudu.master";
const char kMetricEntityIdTabletServer[] = "kudu.tabletserver";

void PrometheusWriter::WriteEntry(const string& data) {
  *output_ << data;
}

bool PrometheusWriter::ShouldWriteHelpAndType(const string& metric_name) {
  return written_help_types_.emplace(metric_name).second;
}

string EscapePrometheusLabelValue(const string& value) {
  string result;
  result.reserve(value.size());
  for (char c : value) {
    switch (c) {
      case '\\': result += "\\\\"; break;
      case '"':  result += "\\\""; break;
      case '\n': result += "\\n"; break;
      default:   result += c; break;
    }
  }
  return result;
}

string BuildPrometheusLabels(
    const string& entity_type,
    const string& entity_id,
    const unordered_map<string, string>& attrs) {
  DCHECK(entity_type == "server" ||
         entity_type == "table" ||
         entity_type == "tablet")
      << "unexpected entity type: " << entity_type;
  if (entity_type == "server") {
    return BuildServerPrometheusLabels(entity_id, attrs);
  }
  return BuildTableTabletPrometheusLabels(entity_type, entity_id, attrs);
}

string BuildServerPrometheusLabels(
    const string& entity_id,
    const unordered_map<string, string>& attrs) {
  string labels;
  if (entity_id == kMetricEntityIdMaster) {
    labels = "type=\"master\"";
  } else if (entity_id == kMetricEntityIdTabletServer) {
    labels = "type=\"tserver\"";
  } else {
    LOG(DFATAL) << "Unexpected server-level metric entity: " << entity_id;
    labels = "type=\"" + EscapePrometheusLabelValue(entity_id) + "\"";
  }

  const auto* uuid = FindOrNull(attrs, string("uuid"));
  if (uuid) {
    DCHECK(uuid->find_first_of("\\\"\n") == string::npos)
        << "uuid needs escaping: " << *uuid;
    labels += ",id=\"" + *uuid + "\"";
  }

  return labels;
}

string BuildTableTabletPrometheusLabels(
    const string& entity_type,
    const string& entity_id,
    const unordered_map<string, string>& attrs) {
  DCHECK(entity_type == "table" || entity_type == "tablet")
      << "unexpected entity type: " << entity_type;
  DCHECK(entity_id.find_first_of("\\\"\n") == string::npos)
      << "entity_id needs escaping: " << entity_id;

  string labels;
  labels += "type=\"" + entity_type + "\"";
  labels += ",id=\"" + entity_id + "\"";
  // Add well-known attributes in a stable order.
  static const char* const kKnownAttrs[] = {
    "table_id", "table_name"
  };
  for (const char* key : kKnownAttrs) {
    const auto* val = FindOrNull(attrs, string(key));
    if (val) {
      labels += "," + string(key) + "=\"" + EscapePrometheusLabelValue(*val) + "\"";
    }
  }
  return labels;
}
