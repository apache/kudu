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
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/client_negotiation.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/security/security_flags.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/tls_socket.h"
#include "kudu/security/token.pb.h"
#include "kudu/tools/diagnostics_log_parser.h"
#include "kudu/tools/mcp_disposition.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/openssl_util.h"
#include "kudu/util/regex.h"
#include "kudu/util/status.h"

DEFINE_bool(disable_tls, false, "If true, it tries disabling TLS.");
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

DECLARE_string(sasl_protocol_name);
DECLARE_string(tls_min_version);
DECLARE_string(tls_ciphersuites);
DECLARE_string(tls_ciphers);

DECLARE_int64(negotiation_timeout_ms);
DECLARE_int64(timeout_ms);
DECLARE_string(format);

using kudu::security::RpcEncryption;
using kudu::rpc::ClientNegotiation;
using kudu::rpc::SaslInit;
using std::cout;
using std::endl;
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

Status TlsDebug(const RunnerContext& context) {
  DCHECK(security::IsOpenSSLInitialized());
  const string& address = FindOrDie(context.required_args, "server_addr");

  unique_ptr<Socket> socket(new Socket());

  HostPort hp;
  RETURN_NOT_OK(hp.ParseString(address, 0));
  vector<Sockaddr> resolved;
  RETURN_NOT_OK(hp.ResolveAddresses(&resolved));
  if (resolved[0].port() == 0) {
    return Status::InvalidArgument("Port must be set.");
  }

  RETURN_NOT_OK(socket->Init(resolved[0].family(), 0));
  RETURN_NOT_OK(socket->Connect(resolved[0]));

  RETURN_NOT_OK(SaslInit());

  security::TlsContext tls_context(FLAGS_tls_ciphers, FLAGS_tls_ciphersuites, FLAGS_tls_min_version, {});
  RETURN_NOT_OK(tls_context.Init());

  const auto encryption = FLAGS_disable_tls ?  RpcEncryption::DISABLED : RpcEncryption::REQUIRED;

  ClientNegotiation client_negotiation(std::move(socket),
                                       &tls_context,
                                       std::nullopt,
                                       std::nullopt,
                                       encryption,
                                       true,
                                       FLAGS_sasl_protocol_name);

  // skip_authn mode: stop negotiation before SASL/token/JWT authentication.
  // This lets the tool observe the negotiated TLS parameters against
  // Kerberos-only servers without requiring a valid TGT. The resulting
  // connection is not usable for RPCs. set_server_fqdn() is still set since
  // SendNegotiate() advertises a SASL authn type and Cyrus SASL is
  // initialized as part of the negotiation flow.
  client_negotiation.set_skip_authn(true);
  client_negotiation.set_server_fqdn(hp.host());
  client_negotiation.set_deadline(
      MonoTime::Now() + MonoDelta::FromMilliseconds(FLAGS_negotiation_timeout_ms));

  // At least one authn type must be advertised in the NEGOTIATE step, even
  // though we abort before actually authenticating. PLAIN works against both
  // Kerberized and non-Kerberized servers since the server only checks the
  // mechanism list, not the credentials, during the NEGOTIATE step.
  RETURN_NOT_OK(client_negotiation.EnablePlain("tls-test", "tls-test"));
  WARN_NOT_OK(client_negotiation.EnableGSSAPI(), "Couldn't enable GSSAPI, negotiation may fail");
  RETURN_NOT_OK(client_negotiation.Negotiate());

  unique_ptr<Socket> negotiated_socket = client_negotiation.release_socket();
  auto* tls_socket = dynamic_cast<security::TlsSocket*>(negotiated_socket.get());
  if (tls_socket == nullptr) {
    cout << "TLS was not negotiated - cleartext connection" << endl;
    return Status::OK();
  }

  const auto& protocol = tls_socket->GetProtocolName();
  const string extms = protocol == "TLSv1.3" ?
    "N/A (TLSv1.3)" :
    tls_socket->GetExtMS() ? "yes" : "no";
  cout << "Negotiated protocol: " << protocol << endl;
  cout << "Negotiated ciphersuite: " << tls_socket->GetCipherName() << endl;
  cout << "Negotiated ciphersuite (description): " << tls_socket->GetCipherDescription() << endl;
  cout << "Negotiated extended master secret (RFC 7627): " << extms << endl;

  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildDiagnoseMode() {
  unique_ptr<Action> parse_stacks =
      ActionBuilder("parse_stacks", &ParseStacks)
      .Description("Parse sampled stack traces out of a diagnostics log")
      .McpDisposition(Disposition::SURFACE)
      .McpNodeLocal()
      .AddRequiredVariadicParameter({ kLogPathArg, kLogPathArgDesc })
      .Build();

  // TODO(awong): add timestamp bounds
  unique_ptr<Action> parse_metrics =
      ActionBuilder("parse_metrics", &ParseMetrics)
      .Description("Parse metrics out of a diagnostics log")
      .McpDisposition(Disposition::SURFACE)
      .McpNodeLocal()
      .AddRequiredVariadicParameter({ kLogPathArg, kLogPathArgDesc })
      .AddOptionalParameter("tablet_ids")
      .AddOptionalParameter("simple_metrics")
      .AddOptionalParameter("rate_metrics")
      .AddOptionalParameter("histogram_metrics")
      .Build();

  unique_ptr<Action> tls_debug =
      RpcActionBuilder("tls_debug", &TlsDebug)
      .Description("Connect to a running cluster and dump TLS debug information")
      .McpDisposition(Disposition::SURFACE)
      .AddRequiredParameter({"server_addr", "Address of a Kudu Master or Tablet server "
                                            "of the form 'hostname:port'."})
      .AddOptionalParameter("disable_tls")
      .Build();

  return ModeBuilder("diagnose")
      .Description("Diagnostic tools for Kudu servers and clusters")
      .AddAction(std::move(parse_stacks))
      .AddAction(std::move(parse_metrics))
      .AddAction(std::move(tls_debug))
      .Build();
}

} // namespace tools
} // namespace kudu
