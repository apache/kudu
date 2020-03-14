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

#include "kudu/mini-cluster/webui_checker.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

using std::string;
using std::thread;
using std::vector;
using strings::Substitute;

namespace kudu {

PeriodicWebUIChecker::PeriodicWebUIChecker(
    const cluster::ExternalMiniCluster& cluster,
    MonoDelta period,
    const string& tablet_id,
    const vector<string>& master_pages,
    const vector<string>& ts_pages)
    : period_(period),
      is_running_(true) {
  // List of tserver web pages to fetch.
  vector<string> ts_pages_spec(ts_pages);
  if (tablet_id.empty()) {
    ts_pages_spec.emplace_back("/transactions");
  } else {
    ts_pages_spec.emplace_back(
        Substitute("/transactions?tablet_id=$0", tablet_id));
  }

  // Generate list of urls for each master and tablet server
  for (int i = 0; i < cluster.num_masters(); i++) {
    for (const auto& page : master_pages) {
      urls_.emplace_back(Substitute(
          "http://$0$1",
          cluster.master(i)->bound_http_hostport().ToString(),
          page));
    }
  }
  for (int i = 0; i < cluster.num_tablet_servers(); i++) {
    for (const auto& page : ts_pages_spec) {
      urls_.emplace_back(Substitute(
          "http://$0$1",
          cluster.tablet_server(i)->bound_http_hostport().ToString(),
          page));
    }
  }
  std::random_device rdev;
  std::mt19937 gen(rdev());
  std::shuffle(urls_.begin(), urls_.end(), gen);
  checker_ = thread([this]() { this->CheckThread(); });
}

PeriodicWebUIChecker::~PeriodicWebUIChecker() {
  LOG(INFO) << "shutting down CURL thread";
  is_running_ = false;
  checker_.join();
}

void PeriodicWebUIChecker::CheckThread() {
  EasyCurl curl;
  curl.set_timeout(MonoDelta::FromSeconds(120));
  std::ostringstream ostr;
  ostr << "curl thread will poll the following URLs every "
       << period_.ToString() << ":\n";
  for (const auto& url : urls_) {
    ostr << "  " << url << "\n";
  }
  LOG(INFO) << ostr.str();
  faststring dst;
  while (is_running_) {
    // Poll all of the URLs.
    const MonoTime start = MonoTime::Now();
    bool compression_enabled = true;
    for (const auto& url : urls_) {
      // Switch compression back and forth.
      Status s = compression_enabled
          ? curl.FetchURL(url, &dst, {"Accept-Encoding: gzip"})
          : curl.FetchURL(url, &dst);
      compression_enabled = !compression_enabled;
      if (s.ok()) {
        CHECK_GT(dst.length(), 0);
      }
      CHECK(!s.IsTimedOut()) << Substitute(
          "could not fetch $0 ($1 compression, $2 connections): $3",
          url, compression_enabled ? "gzip" : "no", curl.num_connects(), s.ToString());
    }
    // Sleep until the next period
    const MonoDelta elapsed = MonoTime::Now() - start;
    const int64_t sleep_ns = period_.ToNanoseconds() - elapsed.ToNanoseconds();
    if (sleep_ns > 0) {
      SleepFor(MonoDelta::FromNanoseconds(sleep_ns));
    }
  }
}

} // namespace kudu
