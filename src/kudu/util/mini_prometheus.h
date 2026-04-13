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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <rapidjson/document.h>

#include "kudu/util/status.h"

namespace kudu {

class MonoDelta;
class Subprocess;

struct MiniPrometheusOptions {
  // List of Kudu master HTTP SD URLs. Each entry becomes one http_sd_configs
  // entry in the generated prometheus.yml.
  std::vector<std::string> master_sd_urls;

  // List of static scrape targets in "host:port" form.
  // Each entry becomes a target under static_configs in the generated
  // prometheus.yml. Use this instead of (or in addition to) master_sd_urls
  // when you want to scrape a fixed set of endpoints without HTTP SD.
  std::vector<std::string> static_targets;

  // HTTP path used to scrape metrics on discovered targets.
  std::string metrics_path = "/metrics_prometheus";

  // How frequently Prometheus scrapes discovered targets.
  std::string scrape_interval = "1s";

  // How frequently Prometheus refreshes the HTTP SD target lists.
  std::string sd_refresh_interval = "1s";
};

class MiniPrometheus {
 public:
  explicit MiniPrometheus(MiniPrometheusOptions options);
  ~MiniPrometheus();

  // Starts the Prometheus subprocess. Writes a prometheus.yml config file into
  // a temporary directory, then spawns Prometheus. Blocks until Prometheus is
  // listening on its HTTP port (up to 60 seconds).
  Status Start();

  Status Stop();

  // Queries the Prometheus HTTP API at /api/v1/targets and populates 'doc'
  // with the parsed JSON response.
  Status GetTargets(rapidjson::Document* doc);

  // Blocks until Prometheus has at least 'count' active (health=up) targets,
  // polling every second.
  Status WaitForActiveTargets(int count, MonoDelta timeout);

  // Issues a PromQL instant query via /api/v1/query and populates 'doc' with
  // the parsed JSON response.
  Status Query(const std::string& promql,
               rapidjson::Document* doc);

  // Returns the TCP port Prometheus is listening on. Only valid after Start().
  uint16_t port() const { return port_; }

  // Returns the base URL for this Prometheus instance, e.g. "http://127.0.0.1:<port>".
  std::string url() const;

 private:
  // Writes the prometheus.yml config file into data_root_.
  Status WriteConfig() const;

  // Fetches the given path (relative to the Prometheus base URL) and parses
  // the JSON response body into 'doc'.
  Status FetchJson(const std::string& path,
                   rapidjson::Document* doc);

  const MiniPrometheusOptions options_;

  const std::string host_ = "127.0.0.1";

  std::string data_root_;
  std::string prometheus_home_;

  std::unique_ptr<Subprocess> process_;

  uint16_t port_ = 0;
};

} // namespace kudu
