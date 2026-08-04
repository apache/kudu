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

#include <stddef.h>
#include <stdint.h>

#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/webserver.h"
#include "kudu/server/webserver_options.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/web_callback_registry.h"

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

// A minimal HTTP server that imitates the Kudu master's /prometheus-sd and
// /metrics_prometheus endpoints, for testing MiniPrometheus without a full Kudu cluster.
//
// By default the SD handler advertises this server's own address, so Prometheus
// both discovers and scrapes from the same mock server. Pass a list of target
// addresses to the constructor to override this (e.g. when this instance plays
// the role of a master advertising separate tserver addresses).
class MockKuduServer {
 public:
  explicit MockKuduServer(vector<string> sd_targets = {})
      : sd_targets_(std::move(sd_targets)) {}

  // Starts the mock server on an ephemeral port.
  Status Start() {
    WebserverOptions opts;
    opts.port = 0;
    opts.bind_interface = "127.0.0.1";
    server_.reset(new Webserver(opts));

    // /prometheus-sd: advertises sd_targets_ if set, otherwise this server itself.
    Webserver* ws = server_.get();
    server_->RegisterJsonPathHandler(
        "/prometheus-sd", "Prometheus SD",
        [this, ws](const Webserver::WebRequest& /*req*/,
                   Webserver::PrerenderedWebResponse* resp) {
          vector<string> targets = sd_targets_;
          if (targets.empty()) {
            vector<Sockaddr> addrs;
            CHECK_OK(ws->GetBoundAddresses(&addrs));
            targets.push_back(Substitute("127.0.0.1:$0", addrs[0].port()));
          }
          string targets_json;
          for (size_t i = 0; i < targets.size(); ++i) {
            if (i > 0) targets_json += ",";
            targets_json += Substitute(R"("$0")", targets[i]);
          }
          resp->output << Substitute(
              R"([{"targets":[$0],"labels":{"job":"kudu","group":"test"}}])",
              targets_json);
          resp->status_code = HttpStatusCode::Ok;
        },
        /*is_on_nav_bar=*/false);

    // /metrics_prometheus: minimal valid Prometheus text format exposition.
    server_->RegisterPrerenderedPathHandler(
        "/metrics_prometheus", "Prometheus Metrics",
        [](const Webserver::WebRequest& /*req*/,
           Webserver::PrerenderedWebResponse* resp) {
          resp->output <<
              "# HELP kudu_test_counter A test counter.\n"
              "# TYPE kudu_test_counter counter\n"
              "kudu_test_counter 42\n";
          resp->status_code = HttpStatusCode::Ok;
        },
        StyleMode::UNSTYLED, /*is_on_nav_bar=*/false);

    RETURN_NOT_OK(server_->Start());

    vector<Sockaddr> addrs;
    RETURN_NOT_OK(server_->GetBoundAddresses(&addrs));
    port_ = addrs[0].port();
    return Status::OK();
  }

  void Stop() {
    if (server_) {
      server_->Stop();
    }
  }

  uint16_t port() const { return port_; }

  string sd_url() const {
    return Substitute("http://127.0.0.1:$0/prometheus-sd", port_);
  }

 private:
  unique_ptr<Webserver> server_;
  uint16_t port_ = 0;
  vector<string> sd_targets_;
};

// A minimal mock Kudu cluster built from MockKuduServer instances.
//
// 'Masters' are used for: /prometheus-sd advertising all tserver addresses.
// 'Tservers' are used for: /metrics_prometheus with dummy metrics.
//
// All masters advertise the full tserver list simultaneously, which models the
// brief window during a Kudu leadership transition where both the outgoing and
// the incoming leader serve identical SD data.
class MockKuduCluster {
 public:
  explicit MockKuduCluster(int num_masters = 3, int num_tservers = 3)
      : num_masters_(num_masters), num_tservers_(num_tservers) {}

  Status Start() {
    for (int i = 0; i < num_tservers_; ++i) {
      auto ts = std::make_unique<MockKuduServer>();
      RETURN_NOT_OK(ts->Start());
      tservers_.push_back(std::move(ts));
    }

    vector<string> ts_addrs;
    ts_addrs.reserve(tservers_.size());
    for (const auto& ts : tservers_) {
      ts_addrs.push_back(Substitute("127.0.0.1:$0", ts->port()));
    }

    for (int i = 0; i < num_masters_; ++i) {
      auto master = std::make_unique<MockKuduServer>(ts_addrs);
      RETURN_NOT_OK(master->Start());
      masters_.push_back(std::move(master));
    }
    return Status::OK();
  }

  void Stop() {
    for (auto& m : masters_) m->Stop();
    for (auto& ts : tservers_) ts->Stop();
  }

  vector<string> AllMasterSDUrls() const {
    vector<string> urls;
    urls.reserve(masters_.size());
    for (const auto& m : masters_) urls.push_back(m->sd_url());
    return urls;
  }

  int num_masters() const {
    return num_masters_;
  }
  int num_tservers() const {
    return num_tservers_;
  }

 private:
  const int num_masters_;
  const int num_tservers_;
  vector<unique_ptr<MockKuduServer>> masters_;
  vector<unique_ptr<MockKuduServer>> tservers_;
};

class MiniPrometheusTest : public KuduTest {};

// Verifies that MiniPrometheus starts, binds to a port, and responds to
// GetTargets() with a valid JSON envelope.
TEST_F(MiniPrometheusTest, StartStopNoTargets) {
  MiniPrometheusOptions opts;
  // No SD URLs: Prometheus starts with no targets.
  MiniPrometheus prom(opts);
  ASSERT_OK(prom.Start());
  ASSERT_GT(prom.port(), 0);

  // There are several initialization processes that are run by Prometheus
  // upon its start up. Getting HTTP 200 for '/-/ready' URL as documented
  // doesn't guarantee that '/api/v1/targets' wouldn't return HTTP 503:
  // '/-/ready' returns HTTP 200 once replaying of the WAL is complete,
  // but it seems the Scrape/Target Manager initialization routine starts
  // asynchronously after the TSDB storage layer is initialized and it can take
  // some time before it reports "all is well" even with an empty target list.
  // So, it might be a short period of time when /api/v1/targets would return
  // HTTP 503, even if the server is running and returns HTTP 200 for
  // 'GET /-/ready' or 'HEAD /-/ready' requests.
  rapidjson::Document doc;
  ASSERT_EVENTUALLY([&]() {
    ASSERT_OK(prom.GetTargets(&doc));
  });

  ASSERT_TRUE(doc.IsObject());
  ASSERT_TRUE(doc.HasMember("status"));
  ASSERT_STREQ("success", doc["status"].GetString());
  ASSERT_TRUE(doc.HasMember("data"));

  ASSERT_OK(prom.Stop());
}

// Verifies the static-config scrape path using a mock server:
// the target is written directly into prometheus.yml as a static_configs entry
// rather than being discovered via HTTP SD.
TEST_F(MiniPrometheusTest, StaticTargetScrape) {
  MockKuduServer mock;
  ASSERT_OK(mock.Start());

  MiniPrometheusOptions opts;
  opts.static_targets = { Substitute("127.0.0.1:$0", mock.port()) };

  MiniPrometheus prom(opts);
  ASSERT_OK(prom.Start());

  ASSERT_OK(prom.WaitForActiveTargets(1, MonoDelta::FromSeconds(60)));

  rapidjson::Document targets_doc;
  ASSERT_OK(prom.GetTargets(&targets_doc));
  ASSERT_STREQ("success", targets_doc["status"].GetString());

  const auto& active = targets_doc["data"]["activeTargets"];
  ASSERT_GE(active.Size(), 1u);
  ASSERT_STREQ("up", active[0]["health"].GetString());

  // Verify the metric is actually queryable (scrape succeeded).
  rapidjson::Document query_doc;
  ASSERT_OK(prom.Query("kudu_test_counter", &query_doc));
  ASSERT_STREQ("success", query_doc["status"].GetString());
  ASSERT_GE(query_doc["data"]["result"].Size(), 1u);

  ASSERT_OK(prom.Stop());
  mock.Stop();
}

// Verifies the full SD-to-scrape path using a mock server:
//   MockKuduServer -- /prometheus-sd --> MiniPrometheus discovers target
//   MockKuduServer -- /metrics_prometheus --> MiniPrometheus scrapes metrics
TEST_F(MiniPrometheusTest, DiscoverAndScrapeTarget) {
  MockKuduServer mock;
  ASSERT_OK(mock.Start());

  MiniPrometheusOptions opts;
  opts.master_sd_urls = { mock.sd_url() };

  MiniPrometheus prom(opts);
  ASSERT_OK(prom.Start());

  ASSERT_OK(prom.WaitForActiveTargets(1, MonoDelta::FromSeconds(60)));

  // Verify targets API response structure.
  rapidjson::Document targets_doc;
  ASSERT_OK(prom.GetTargets(&targets_doc));
  ASSERT_STREQ("success", targets_doc["status"].GetString());
  ASSERT_GE(targets_doc["data"]["activeTargets"].Size(), 1u);

  // The active target should be healthy.
  ASSERT_STREQ("up",
      targets_doc["data"]["activeTargets"][0]["health"].GetString());

  // SD-provided labels should be present on the active target.
  const auto& labels = targets_doc["data"]["activeTargets"][0]["labels"];
  ASSERT_TRUE(labels.HasMember("job"));
  ASSERT_STREQ("kudu", labels["job"].GetString());

  ASSERT_OK(prom.Stop());
  mock.Stop();
}

// Verifies that WaitForActiveTargets times out promptly when no targets are active.
TEST_F(MiniPrometheusTest, WaitForActiveTargetsTimeout) {
  MiniPrometheusOptions opts;
  MiniPrometheus prom(opts);
  ASSERT_OK(prom.Start());

  // No targets are configured, so this will reliably time out: Prometheus has
  // nothing to scrape, and we are waiting for one healthy target that can never
  // appear.
  Status s = prom.WaitForActiveTargets(1, MonoDelta::FromSeconds(2));
  ASSERT_TRUE(s.IsTimedOut()) << "Expected TimedOut, got: " << s.ToString();

  ASSERT_OK(prom.Stop());
}

// Verifies that Prometheus deduplicates targets when multiple HTTP SD endpoints
// advertise the same scrape address with identical labels.
TEST_F(MiniPrometheusTest, DuplicateSDTargetsAreDeduplicatedByPrometheus) {
  MockKuduCluster cluster;
  ASSERT_OK(cluster.Start());

  MiniPrometheusOptions opts;
  opts.master_sd_urls = cluster.AllMasterSDUrls();

  MiniPrometheus prom(opts);
  ASSERT_OK(prom.Start());

  ASSERT_OK(prom.WaitForActiveTargets(cluster.num_tservers(), MonoDelta::FromSeconds(60)));

  rapidjson::Document doc;
  ASSERT_OK(prom.GetTargets(&doc));
  ASSERT_STREQ("success", doc["status"].GetString());

  const auto& active = doc["data"]["activeTargets"];
  ASSERT_EQ(static_cast<rapidjson::SizeType>(cluster.num_tservers()), active.Size())
      << "Expected " << cluster.num_tservers() << " active target(s) after "
         "deduplication across " << cluster.num_masters() << " SD endpoints, "
         "but found " << active.Size();

  ASSERT_OK(prom.Stop());
  cluster.Stop();
}

// Verifies that the PromQL instant-query API returns a valid response for a
// metric that was scraped from the mock server.
TEST_F(MiniPrometheusTest, QueryScrapedMetric) {
  MockKuduServer mock;
  ASSERT_OK(mock.Start());

  MiniPrometheusOptions opts;
  opts.master_sd_urls = { mock.sd_url() };

  MiniPrometheus prom(opts);
  ASSERT_OK(prom.Start());
  ASSERT_OK(prom.WaitForActiveTargets(1, MonoDelta::FromSeconds(60)));

  rapidjson::Document query_doc;
  ASSERT_OK(prom.Query("kudu_test_counter", &query_doc));
  ASSERT_STREQ("success", query_doc["status"].GetString());
  ASSERT_STREQ("vector", query_doc["data"]["resultType"].GetString());
  ASSERT_GE(query_doc["data"]["result"].Size(), 1u);

  ASSERT_OK(prom.Stop());
  mock.Stop();
}

} // namespace kudu
