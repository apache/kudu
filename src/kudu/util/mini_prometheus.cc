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

#include "kudu/util/mini_prometheus.h"

#include <csignal>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <rapidjson/rapidjson.h>
#include <yaml-cpp/emitter.h>
#include <yaml-cpp/emittermanip.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"
#include "kudu/util/url-coding.h"

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

static constexpr int kPrometheusStartTimeoutMs = 60000;
static constexpr int kCurlTimeoutSecs = 10;

namespace kudu {

MiniPrometheus::MiniPrometheus(MiniPrometheusOptions options)
    : options_(std::move(options)) {
}

MiniPrometheus::~MiniPrometheus() {
  WARN_NOT_OK(Stop(), "Failed to stop MiniPrometheus");
}

Status MiniPrometheus::Start() {
  CHECK(!process_);

  VLOG(1) << "Starting MiniPrometheus";

  Env* env = Env::Default();

  string exe;
  RETURN_NOT_OK(env->GetExecutablePath(&exe));
  const string bin_dir = DirName(exe);

  RETURN_NOT_OK(FindHomeDir("prometheus", bin_dir, &prometheus_home_));

  if (data_root_.empty()) {
    data_root_ = GetTestDataDirectory();
  }

  RETURN_NOT_OK(env_util::CreateDirIfMissing(env, JoinPathSegments(data_root_, "prometheus-data")));
  RETURN_NOT_OK(WriteConfig());

  const string config_path = JoinPathSegments(data_root_, "prometheus.yml");
  const string storage_path = JoinPathSegments(data_root_, "prometheus-data");
  const string prometheus_bin = JoinPathSegments(prometheus_home_, "prometheus");

  process_.reset(new Subprocess({
      prometheus_bin,
      Substitute("--config.file=$0", config_path),
      Substitute("--storage.tsdb.path=$0", storage_path),
      // Bind to an ephemeral port; we detect the actual port via WaitForTcpBind.
      Substitute("--web.listen-address=$0:0", host_),
  }));

  RETURN_NOT_OK(process_->Start());

  VLOG(1) << "Waiting for Prometheus to bind to port";
  Status wait = WaitForTcpBind(process_->pid(), &port_, {host_},
                               MonoDelta::FromMilliseconds(kPrometheusStartTimeoutMs));
  if (!wait.ok()) {
    WARN_NOT_OK(process_->Kill(SIGQUIT), "failed to send SIGQUIT to Prometheus");
  }
  return wait;
}

Status MiniPrometheus::Stop() {
  if (process_) {
    VLOG(1) << "Stopping MiniPrometheus";
    unique_ptr<Subprocess> proc = std::move(process_);
    RETURN_NOT_OK_PREPEND(proc->KillAndWait(SIGTERM),
                          "failed to stop the Prometheus process");
  }
  return Status::OK();
}

string MiniPrometheus::url() const {
  return Substitute("http://$0:$1", host_, port_);
}

Status MiniPrometheus::WriteConfig() const {
  YAML::Emitter out;
  out << YAML::BeginMap;

  out << YAML::Key << "global" << YAML::Value << YAML::BeginMap;
  out << YAML::Key << "scrape_interval"    << YAML::Value << options_.scrape_interval;
  out << YAML::Key << "evaluation_interval" << YAML::Value << options_.scrape_interval;
  out << YAML::EndMap;

  if (!options_.master_sd_urls.empty() || !options_.static_targets.empty()) {
    out << YAML::Key << "scrape_configs" << YAML::Value << YAML::BeginSeq;
    out << YAML::BeginMap;
    out << YAML::Key << "job_name"       << YAML::Value << "kudu";
    out << YAML::Key << "metrics_path"   << YAML::Value << options_.metrics_path;
    out << YAML::Key << "scrape_interval" << YAML::Value << options_.scrape_interval;

    if (!options_.static_targets.empty()) {
      out << YAML::Key << "static_configs" << YAML::Value << YAML::BeginSeq;
      out << YAML::BeginMap;
      out << YAML::Key << "targets" << YAML::Value << YAML::BeginSeq;
      for (const auto& t : options_.static_targets) {
        out << t;
      }
      out << YAML::EndSeq;
      out << YAML::EndMap;
      out << YAML::EndSeq;
    }

    if (!options_.master_sd_urls.empty()) {
      out << YAML::Key << "http_sd_configs" << YAML::Value << YAML::BeginSeq;
      for (const auto& url : options_.master_sd_urls) {
        out << YAML::BeginMap;
        out << YAML::Key << "url"              << YAML::Value << url;
        out << YAML::Key << "refresh_interval" << YAML::Value << options_.sd_refresh_interval;
        out << YAML::EndMap;
      }
      out << YAML::EndSeq;
    }

    out << YAML::EndMap;
    out << YAML::EndSeq;
  }

  out << YAML::EndMap;

  const string config_path = JoinPathSegments(data_root_, "prometheus.yml");
  return WriteStringToFile(Env::Default(), out.c_str(), config_path);
}

Status MiniPrometheus::FetchJson(const string& path, rapidjson::Document* doc) {
  EasyCurl c;
  c.set_timeout(MonoDelta::FromSeconds(kCurlTimeoutSecs));
  faststring buf;
  RETURN_NOT_OK(c.FetchURL(Substitute("$0$1", url(), path), &buf));
  doc->Parse<0>(buf.ToString().c_str());
  if (doc->HasParseError()) {
    return Status::InvalidArgument(
        Substitute("Failed to parse JSON from Prometheus at $0", path));
  }
  return Status::OK();
}

Status MiniPrometheus::GetTargets(rapidjson::Document* doc) {
  return FetchJson("/api/v1/targets", doc);
}

Status MiniPrometheus::WaitForActiveTargets(int count, MonoDelta timeout) {
  const MonoTime deadline = MonoTime::Now() + timeout;
  while (MonoTime::Now() < deadline) {
    rapidjson::Document doc;
    Status s = GetTargets(&doc);
    if (s.ok() && doc.IsObject() && doc.HasMember("data") &&
        doc["data"].HasMember("activeTargets")) {
      const auto& active = doc["data"]["activeTargets"];
      int healthy = 0;
      for (rapidjson::SizeType i = 0; i < active.Size(); ++i) {
        if (active[i].HasMember("health") &&
            string(active[i]["health"].GetString()) == "up") {
          ++healthy;
        }
      }
      if (healthy >= count) {
        return Status::OK();
      }
    }
    SleepFor(MonoDelta::FromSeconds(1));
  }
  return Status::TimedOut(
      Substitute("Timed out waiting for $0 active Prometheus targets", count));
}

Status MiniPrometheus::Query(const string& promql, rapidjson::Document* doc) {
  EasyCurl c;
  c.set_timeout(MonoDelta::FromSeconds(kCurlTimeoutSecs));
  faststring buf;
  const string url_str = Substitute("$0/api/v1/query?query=$1", url(), UrlEncodeToString(promql));
  RETURN_NOT_OK(c.FetchURL(url_str, &buf));
  doc->Parse<0>(buf.ToString().c_str());
  if (doc->HasParseError()) {
    return Status::InvalidArgument("Failed to parse JSON response from Prometheus query");
  }
  return Status::OK();
}

} // namespace kudu
