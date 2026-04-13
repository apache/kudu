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

#include <stdint.h>

#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

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
// The SD handler returns a target group pointing at this server's own address
// (for the /metrics_prometheus path), so Prometheus both discovers and scrapes
// from the same mock server.
class MockKuduServer {
 public:
  // Starts the mock server on an ephemeral port.
  Status Start() {
    WebserverOptions opts;
    opts.port = 0;
    opts.bind_interface = "127.0.0.1";
    server_.reset(new Webserver(opts));

    // /prometheus-sd: returns a single target group pointing at this server.
    Webserver* ws = server_.get();
    server_->RegisterJsonPathHandler(
        "/prometheus-sd", "Prometheus SD",
        [ws](const Webserver::WebRequest& /*req*/,
             Webserver::PrerenderedWebResponse* resp) {
          vector<Sockaddr> addrs;
          CHECK_OK(ws->GetBoundAddresses(&addrs));
          resp->output << Substitute(
              R"([{"targets":["$0"],"labels":{"job":"kudu","group":"test"}}])",
              Substitute("127.0.0.1:$0", addrs[0].port()));
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

  rapidjson::Document doc;
  ASSERT_OK(prom.GetTargets(&doc));

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
