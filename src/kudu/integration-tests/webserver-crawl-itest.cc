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
#include <deque>
#include <ostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>
#include <gq/Document.h>
#include <gq/Node.h>
#include <gq/Selection.h>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/url-coding.h"

using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using std::deque;
using std::string;
using std::tuple;
using std::unordered_set;
using std::vector;
using strings::SkipEmpty;
using strings::Split;

namespace kudu {

enum class UseSsl { NO, YES, };
enum class ImpersonateKnox { NO, YES, };

// Beautifies test output if a test scenario fails.
std::ostream& operator<<(std::ostream& o, UseSsl opt) {
  switch (opt) {
    case UseSsl::NO:
      return o << "UseSsl::NO";
    case UseSsl::YES:
      return o << "UseSsl::YES";
  }
  return o;
}

std::ostream& operator<<(std::ostream& o, ImpersonateKnox opt) {
  switch (opt) {
    case ImpersonateKnox::NO:
      return o << "ImpersonateKnox::NO";
    case ImpersonateKnox::YES:
      return o << "ImpersonateKnox::YES";
  }
  return o;
}

class WebserverCrawlITest : public KuduTest,
                            public ::testing::WithParamInterface<tuple<UseSsl, ImpersonateKnox>> {
 protected:
  static constexpr const char* const kKnoxPrefix = "/KNOX-BASE";
  static constexpr const char* const kHttpScheme = "http://";
  static constexpr const char* const kHttpsScheme = "https://";

  // A web server is responsible for escaping illegal URL characters in all
  // links embedded in a response. In Kudu's web UI, a prime example is the
  // /threadz page, which includes links to individual thread group pages by
  // name, and thread group names may include spaces. However, when a real Knox
  // gateway mediates the connection, it unconditionally URL-encodes all query
  // parameter values. Kudu is aware of this behavior and when Knox is detected
  // in the connection, Kudu no longer does any escaping, expecting that Knox
  // will do it instead.
  //
  // Impersonating Knox means that our links will not be escaped and will be
  // uncrawlable unless we escape them in the test. This function just does
  // that: it URL-encodes all query parameter values just as Knox would.
  static void KnoxifyURL(string* url) {
    int idx = url->find('?');
    if (idx == string::npos || idx == url->length() - 1) {
      // No query parameters found in the URL.
      return;
    }
    string query = url->substr(idx + 1);
    vector<string> encoded_query_params;
    for (const auto& param : Split(query, "&", SkipEmpty())) {
      vector<string> kv = Split(param, "=");
      ASSERT_EQ(2, kv.size());
      encoded_query_params.emplace_back(kv[0] + "=" +
                                        UrlEncodeToString(kv[1]));
    }
    string encoded_query_param_str = JoinStrings(encoded_query_params, "&");

    // Replace the original query parameter string with the new one containing
    // URL-encoded parameter values.
    *url = StringReplace(*url, query, encoded_query_param_str,
                         /* replace_all= */ false);
  }
};

INSTANTIATE_TEST_CASE_P(,
    WebserverCrawlITest,
    ::testing::Combine(
        ::testing::Values(UseSsl::NO, UseSsl::YES),
        ::testing::Values(ImpersonateKnox::NO, ImpersonateKnox::YES)));

TEST_P(WebserverCrawlITest, TestAllWebPages) {
  bool use_ssl = std::get<0>(GetParam()) == UseSsl::YES;
  bool impersonate_knox = std::get<1>(GetParam()) == ImpersonateKnox::YES;
  const char* scheme = use_ssl ? kHttpsScheme : kHttpScheme;

  // We must set up a proper docroot so that we can render the mustache templates.
  //
  // Unfortunately, this means hardcoding the test's location, and prevents us
  // from testing it with dist-test (see CMakeLists.txt).
  string bin_path;
  ASSERT_OK(Env::Default()->GetExecutablePath(&bin_path));
  string docroot = StrCat(DirName(bin_path), "/../../../www");
  bool is_dir;
  ASSERT_OK(Env::Default()->IsDirectory(docroot, &is_dir));
  ASSERT_TRUE(is_dir);

  vector<string> flags = { StrCat("--webserver_doc_root=", docroot) };
  if (use_ssl) {
    string cert_file;
    string pk_file;
    string pw;
    ASSERT_OK(security::CreateTestSSLCertWithEncryptedKey(
        GetTestDataDirectory(), &cert_file, &pk_file, &pw));
    flags.emplace_back(StrCat("--webserver_certificate_file=", cert_file));
    flags.emplace_back(StrCat("--webserver_private_key_file=", pk_file));
    flags.emplace_back(StrCat("--webserver_private_key_password_cmd=echo ", pw));
  }

  // Use multiple masters so that the /masters pages contain interesting links.
  ExternalMiniClusterOptions opts;
  opts.num_masters = 3;
  opts.num_tablet_servers = 3;
  opts.extra_master_flags = flags;
  opts.extra_tserver_flags = flags;
  ExternalMiniCluster cluster(std::move(opts));
  ASSERT_OK(cluster.Start());

  // Create a table and write soem data so that log anchors page gets populated.
  TestWorkload work(&cluster);
  work.set_num_replicas(3);
  work.set_num_read_threads(4);
  work.set_num_tablets(6);
  work.Setup();
  work.Start();
  while (work.rows_inserted() < 1000) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  work.StopAndJoin();

  // Tracks all URLs left to process. When empty, the search is finished.
  deque<string> urls;

  // Tracks every URL seen by the test. The URL is full; it includes the scheme,
  // host information, path, and query parameters.
  //
  // Note that a URL is added _before_ it is actually visited so that we can
  // avoid duplicate visits if a page has the same link multiple times.
  unordered_set<string> urls_seen;

  // Populate 'urls' with the roots of all the web UIs.
  for (int i = 0; i < cluster.num_masters(); i++) {
    string url = scheme + cluster.master(i)->bound_http_hostport().ToString();
    urls.emplace_back(url);
    urls_seen.emplace(std::move(url));
  }
  for (int i = 0; i < cluster.num_tablet_servers(); i++) {
    string url = scheme + cluster.tablet_server(i)->bound_http_hostport().ToString();
    urls.emplace_back(url);
    urls_seen.emplace(std::move(url));
  }

  // Process one link, adding it to the queue if has yet to be crawled.
  auto process_link = [&](const string& host, string link) {
    SCOPED_TRACE(link);
    if (link.empty()) {
      // The HTML parser produces empty links in some JS and CSS pages; skip them.
      return;
    }
    if (HasPrefixString(link, "#")) {
      // An anchor without a path doesn't need to be crawled.
      return;
    }

    // Verify that the link's scheme matches how we've configured the web UI.
    ASSERT_FALSE(HasPrefixString(link, use_ssl ? kHttpScheme : kHttpsScheme));

    if (HasPrefixString(link, scheme)) {
      // Full URLs should not be modified and can be visited directly.
      if (EmplaceIfNotPresent(&urls_seen, link)) {
        urls.emplace_back(std::move(link));
      }
      return;
    }

    // From here on out we're dealing with a URL with an absolute path.

    if (impersonate_knox) {
      // The web UI should have returned a link beginning with the special
      // Knox token. Verify this and remove it so that we can actually crawl
      // the link.
      ASSERT_TRUE(HasPrefixString(link, kKnoxPrefix));
      link = StringReplace(link, kKnoxPrefix, "", /* replace_all= */ false);
      NO_FATALS(KnoxifyURL(&link));
    } else {
      ASSERT_FALSE(HasPrefixString(link, kKnoxPrefix));
    }

    // Root paths are canonicalized into empty strings to match the behavior
    // of the initial URL seeding as well as the various external URLs generated
    // by the web UIs.
    if (link == "/") {
      link = "";
    }

    // Sanity check that there are no spaces in the link; they should have been
    // URL-encoded, either by the web UI or by KnoxifyURL.
    ASSERT_EQ(string::npos, link.find(' '));

    string full_url = host + link;
    if (!ContainsKey(urls_seen, full_url)) {
      urls.emplace_back(full_url);
      urls_seen.emplace(std::move(full_url));
    }
  };

  // Process all links in the page as defined by a particular pairing of element
  // and attribute type.
  auto process_page = [&](const string& host,
                          gq::CDocument* page,
                          const char* elem,
                          const char* attr) {
    gq::CSelection sel = page->find(elem);
    for (int i = 0; i < sel.nodeNum(); i++) {
      string link = sel.nodeAt(i).attribute(attr);
      NO_FATALS(process_link(host, std::move(link)));
    }
  };

  // Crawl the web UIs.
  //
  // TODO(adar): the crawl could be faster if squeasel used TCP_NODELAY in its
  // sockets. As it stands, a repeated fetch to the same host is slowed by about
  // ~40ms due to Nagle's algorithm and delayed TCP ACKs.
  EasyCurl curl;

  // We use a self-signed cert, so we need to disable cert verification in curl.
  if (use_ssl) {
    curl.set_verify_peer(false);
  }

  faststring response;
  vector<string> headers;
  if (impersonate_knox) {
    // Pretend we're Knox when communicating with the web UI.
    //
    // Note: the header value doesn't actually matter; only the key matters.
    headers.emplace_back("X-Forwarded-Context: test");
  }
  while (!urls.empty()) {
    string url = urls.front();
    urls.pop_front();
    int ret = FindNth(url, '/', 3);
    string host = ret == string::npos ? url : url.substr(0, ret);

    // Every link should be reachable.
    ASSERT_OK(curl.FetchURL(url, &response, headers));
    string resp_str = response.ToString();
    SCOPED_TRACE(resp_str);

    gq::CDocument page;
    page.parse(resp_str);

    // e.g. <a href="/rpcz">RPCs</a>
    NO_FATALS(process_page(host, &page, "a", "href"));

    // e.g. <script src='/bootstrap/js/bootstrap.min.js' defer></script>
    NO_FATALS(process_page(host, &page, "script", "src"));
  }

  // Dump the results for troubleshooting.
  vector<string> sorted_urls(urls_seen.begin(), urls_seen.end());
  std::sort(sorted_urls.begin(), sorted_urls.end());
  LOG(INFO) << "Dumping visited URLs";
  for (const auto& u : sorted_urls) {
    LOG(INFO) << u;
  }
}

} // namespace kudu
