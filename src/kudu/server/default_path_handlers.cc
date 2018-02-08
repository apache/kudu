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

#include "kudu/server/default_path_handlers.h"

#include <sys/stat.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/iterator/iterator_traits.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#ifdef TCMALLOC_ENABLED
#include <gperftools/malloc_extension.h>
#endif

#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/pprof_path_handlers.h"
#include "kudu/server/webserver.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/easy_json.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flags.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/logging.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/status.h"
#include "kudu/util/web_callback_registry.h"

using std::ifstream;
using std::string;
using std::vector;
using strings::Substitute;

DEFINE_int64(web_log_bytes, 1024 * 1024,
    "The maximum number of bytes to display on the debug webserver's log page");
TAG_FLAG(web_log_bytes, advanced);
TAG_FLAG(web_log_bytes, runtime);

// For configuration dashboard
DECLARE_string(redact);
DECLARE_string(rpc_encryption);
DECLARE_string(rpc_authentication);
DECLARE_string(webserver_certificate_file);

namespace kudu {

using std::shared_ptr;

namespace {
// Html/Text formatting tags
struct Tags {
  string pre_tag, end_pre_tag, line_break, header, end_header;

  // If as_text is true, set the html tags to a corresponding raw text representation.
  explicit Tags(bool as_text) {
    if (as_text) {
      pre_tag = "";
      end_pre_tag = "\n";
      line_break = "\n";
      header = "";
      end_header = "";
    } else {
      pre_tag = "<pre>";
      end_pre_tag = "</pre>";
      line_break = "<br/>";
      header = "<h2>";
      end_header = "</h2>";
    }
  }
};
} // anonymous namespace

// Writes the last FLAGS_web_log_bytes of the INFO logfile to a webpage
// Note to get best performance, set GLOG_logbuflevel=-1 to prevent log buffering
static void LogsHandler(const Webserver::WebRequest& req, Webserver::WebResponse* resp) {
  EasyJson* output = resp->output;
  (*output)["raw"] = (req.parsed_args.find("raw") != req.parsed_args.end());
  string logfile;
  GetFullLogFilename(google::INFO, &logfile);
  (*output)["logfile"] = logfile;
  struct stat file_stat;
  if (stat(logfile.c_str(), &file_stat) == 0) {
    size_t size = file_stat.st_size;
    size_t seekpos = size < FLAGS_web_log_bytes ? 0L : size - FLAGS_web_log_bytes;
    ifstream log(logfile.c_str(), std::ios::in);
    // Note if the file rolls between stat and seek, this could fail
    // (and we could wind up reading the whole file). But because the
    // file is likely to be small, this is unlikely to be an issue in
    // practice.
    log.seekg(seekpos);
    (*output)["web_log_bytes"] = FLAGS_web_log_bytes;
    std::ostringstream ss;
    ss << log.rdbuf();
    (*output)["log"] = ss.str();
  }
}

// Registered to handle "/flags", and prints out all command-line flags and their HTML
// escaped values. If --redact indicates that redaction is enabled for the web UI, the
// values of flags tagged as sensitive will be redacted. The values would not be HTML
// escaped if in the raw text mode, e.g. "/varz?raw".
static void FlagsHandler(const Webserver::WebRequest& req,
                         Webserver::PrerenderedWebResponse* resp) {
  std::ostringstream* output = resp->output;
  bool as_text = (req.parsed_args.find("raw") != req.parsed_args.end());
  Tags tags(as_text);

  (*output) << tags.header << "Command-line Flags" << tags.end_header;
  (*output) << tags.pre_tag
            << CommandlineFlagsIntoString(as_text ? EscapeMode::NONE : EscapeMode::HTML)
            << tags.end_pre_tag;
}

// Registered to handle "/stacks".
//
// Prints out the current stack trace of all threads in the process.
static void StacksHandler(const Webserver::WebRequest& /*req*/,
                          Webserver::PrerenderedWebResponse* resp) {
  std::ostringstream* output = resp->output;
  vector<pid_t> tids;
  Status s = ListThreads(&tids);
  if (!s.ok()) {
    *output << "Failed to list threads: " << s.ToString();
    return;
  }
  struct Info {
    pid_t tid;
    Status status;
    string thread_name;
    StackTrace stack;
  };
  std::multimap<string, Info> grouped_infos;
  vector<Info> failed;

  // Capture all the stacks without symbolization initially so that
  // the stack traces come from as close together in time as possible.
  //
  // TODO(todd): would be good to actually send the dump signal to all
  // threads and then wait for them all to collect their traces, to get
  // an even tighter snapshot.
  MonoTime start = MonoTime::Now();
  for (int i = 0; i < tids.size(); i++) {
    Info info;
    info.tid = tids[i];

    // Get the thread's name by reading proc.
    // TODO(todd): should we have the dumped thread fill in its own name using
    // prctl to avoid having to open and read /proc? Or maybe we should use the
    // Kudu ThreadMgr to get the thread names for the cases where we are using
    // the kudu::Thread wrapper at least.
    faststring buf;
    Status s = ReadFileToString(Env::Default(),
                                Substitute("/proc/self/task/$0/comm", info.tid),
                                &buf);
    if (!s.ok()) {
      info.thread_name = "<unknown name>";
    }  else {
      info.thread_name = buf.ToString();
      StripTrailingNewline(&info.thread_name);
    }

    info.status = GetThreadStack(info.tid, &info.stack);
    if (info.status.ok()) {
      grouped_infos.emplace(info.stack.ToHexString(), std::move(info));
    } else {
      failed.emplace_back(std::move(info));
    }
  }
  MonoDelta dur = MonoTime::Now() - start;

  *output << "Collected stacks from " << grouped_infos.size() << " threads in "
          << dur.ToString() << "\n";
  if (!failed.empty()) {
    *output << "Failed to collect stacks from " << failed.size() << " threads "
            << "(they may have exited while we were iterating over the threads)\n";
  }
  *output << "\n";
  for (auto it = grouped_infos.begin(); it != grouped_infos.end();) {
    auto end_group = grouped_infos.equal_range(it->first).second;
    const auto& stack = it->second.stack;
    int num_in_group = std::distance(it, end_group);
    if (num_in_group > 1) {
      *output << num_in_group << " threads with same stack:\n";
    }

    while (it != end_group) {
      const auto& info = it->second;
      *output << "TID " << info.tid << "(" << info.thread_name << "):\n";
      ++it;
    }
    *output << stack.Symbolize() << "\n\n";
  }
}

// Registered to handle "/memz", and prints out memory allocation statistics.
static void MemUsageHandler(const Webserver::WebRequest& req,
                            Webserver::PrerenderedWebResponse* resp) {
  std::ostringstream* output = resp->output;
  bool as_text = (req.parsed_args.find("raw") != req.parsed_args.end());
  Tags tags(as_text);

  (*output) << tags.pre_tag;
#ifndef TCMALLOC_ENABLED
  (*output) << "Memory tracking is not available unless tcmalloc is enabled.";
#else
  faststring buf;
  buf.resize(32 * 1024);
  MallocExtension::instance()->GetStats(reinterpret_cast<char*>(buf.data()), buf.size());
  // Replace new lines with <br> for html
  string tmp(reinterpret_cast<char*>(buf.data()));
  boost::replace_all(tmp, "\n", tags.line_break);
  (*output) << tmp << tags.end_pre_tag;
#endif
}

// Registered to handle "/mem-trackers", and prints out to handle memory tracker information.
static void MemTrackersHandler(const Webserver::WebRequest& /*req*/,
                               Webserver::PrerenderedWebResponse* resp) {
  std::ostringstream* output = resp->output;
  int64_t current_consumption = process_memory::CurrentConsumption();
  int64_t hard_limit = process_memory::HardLimit();
  *output << "<h1>Process memory usage</h1>\n";
  *output << "<table class='table table-striped'>\n";
  *output << Substitute("  <tr><th>Total consumption</th><td>$0</td></tr>\n",
                        HumanReadableNumBytes::ToString(current_consumption));
  *output << Substitute("  <tr><th>Memory limit</th><td>$0</td></tr>\n",
                        HumanReadableNumBytes::ToString(hard_limit));
  if (hard_limit > 0) {
    double percentage = 100 * static_cast<double>(current_consumption) / hard_limit;
    *output << Substitute("  <tr><th>Percentage consumed</th><td>$0%</td></tr>\n",
                          StringPrintf("%.2f", percentage));
  }
  *output << "</table>\n";
#ifndef TCMALLOC_ENABLED
  *output << R"(
      <div class="alert alert-warning">
        <strong>NOTE:</strong> This build of Kudu has not enabled tcmalloc.
        The above process memory stats will be inaccurate.
      </div>
               )";
#endif

  *output << "<h1>Memory usage by subsystem</h1>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "  <thead><tr><th>Id</th><th>Parent</th><th>Limit</th><th>Current Consumption</th>"
      "<th>Peak consumption</th></tr></thead>\n";
  *output << "<tbody>\n";

  vector<shared_ptr<MemTracker> > trackers;
  MemTracker::ListTrackers(&trackers);
  for (const shared_ptr<MemTracker>& tracker : trackers) {
    string parent = tracker->parent() == nullptr ? "none" : tracker->parent()->id();
    string limit_str = tracker->limit() == -1 ? "none" :
                       HumanReadableNumBytes::ToString(tracker->limit());
    string current_consumption_str = HumanReadableNumBytes::ToString(tracker->consumption());
    string peak_consumption_str = HumanReadableNumBytes::ToString(tracker->peak_consumption());
    (*output) << Substitute("  <tr><td>$0</td><td>$1</td><td>$2</td>" // id, parent, limit
                            "<td>$3</td><td>$4</td></tr>\n", // current, peak
                            tracker->id(), parent, limit_str, current_consumption_str,
                            peak_consumption_str);
  }
  *output << "</tbody></table>\n";
}

static void ConfigurationHandler(const Webserver::WebRequest& /* req */,
                                 Webserver::WebResponse* resp) {
  EasyJson* output = resp->output;
  EasyJson security_configs = output->Set("security_configs", EasyJson::kArray);

  EasyJson rpc_encryption = security_configs.PushBack(EasyJson::kObject);
  rpc_encryption["name"] = "RPC Encryption";
  rpc_encryption["value"] = FLAGS_rpc_encryption;
  rpc_encryption["secure"] = boost::iequals(FLAGS_rpc_encryption, "required");
  rpc_encryption["id"] = "rpc_encryption";
  rpc_encryption["explanation"] = "Configure with --rpc_encryption. Most secure value is "
                                  "'required'.";

  EasyJson rpc_authentication = security_configs.PushBack(EasyJson::kObject);
  rpc_authentication["name"] = "RPC Authentication";
  rpc_authentication["value"] = FLAGS_rpc_authentication;
  rpc_authentication["secure"] = boost::iequals(FLAGS_rpc_authentication, "required");
  rpc_authentication["id"] = "rpc_authentication";
  rpc_authentication["explanation"] = "Configure with --rpc_authentication. Most secure value is "
                                      "'required'.";

  EasyJson webserver_encryption = security_configs.PushBack(EasyJson::kObject);
  webserver_encryption["name"] = "Webserver Encryption";
  webserver_encryption["value"] = FLAGS_webserver_certificate_file.empty() ? "off" : "on";
  webserver_encryption["secure"] = !FLAGS_webserver_certificate_file.empty();
  webserver_encryption["id"] = "webserver_encryption";
  webserver_encryption["explanation"] = "Configure with --webserver_certificate_file and "
                                        "--webserver_private_key_file.";

  EasyJson webserver_redaction = security_configs.PushBack(EasyJson::kObject);
  webserver_redaction["name"] = "Webserver Redaction";
  webserver_redaction["value"] = FLAGS_redact;
  webserver_redaction["secure"] = boost::iequals(FLAGS_redact, "all");
  webserver_redaction["id"] = "webserver_redaction";
  webserver_redaction["explanation"] = "Configure with --redact. Most secure value is 'all'.";
}

void AddDefaultPathHandlers(Webserver* webserver) {
  bool styled = true;
  bool on_nav_bar = true;
  webserver->RegisterPathHandler("/logs", "Logs", LogsHandler, styled, on_nav_bar);
  webserver->RegisterPrerenderedPathHandler("/varz", "Flags", FlagsHandler, styled, on_nav_bar);
  webserver->RegisterPrerenderedPathHandler("/memz", "Memory (total)", MemUsageHandler,
                                            styled, on_nav_bar);
  webserver->RegisterPrerenderedPathHandler("/mem-trackers", "Memory (detail)", MemTrackersHandler,
                                            styled, on_nav_bar);
  webserver->RegisterPathHandler("/config", "Configuration", ConfigurationHandler,
                                  styled, on_nav_bar);

  webserver->RegisterPrerenderedPathHandler("/stacks", "Stacks", StacksHandler,
                                            /*is_styled=*/false,
                                            /*is_on_nav_bar=*/false);

  AddPprofPathHandlers(webserver);
}


static void WriteMetricsAsJson(const MetricRegistry* const metrics,
                               const Webserver::WebRequest& req,
                               Webserver::PrerenderedWebResponse* resp) {
  std::ostringstream* output = resp->output;
  const string* requested_metrics_param = FindOrNull(req.parsed_args, "metrics");
  vector<string> requested_metrics;
  MetricJsonOptions opts;

  {
    string arg = FindWithDefault(req.parsed_args, "include_raw_histograms", "false");
    opts.include_raw_histograms = ParseLeadingBoolValue(arg.c_str(), false);
  }
  {
    string arg = FindWithDefault(req.parsed_args, "include_schema", "false");
    opts.include_schema_info = ParseLeadingBoolValue(arg.c_str(), false);
  }
  JsonWriter::Mode json_mode;
  {
    string arg = FindWithDefault(req.parsed_args, "compact", "false");
    json_mode = ParseLeadingBoolValue(arg.c_str(), false) ?
      JsonWriter::COMPACT : JsonWriter::PRETTY;
  }

  JsonWriter writer(output, json_mode);

  if (requested_metrics_param != nullptr) {
    SplitStringUsing(*requested_metrics_param, ",", &requested_metrics);
  } else {
    // Default to including all metrics.
    requested_metrics.emplace_back("*");
  }

  WARN_NOT_OK(metrics->WriteAsJson(&writer, requested_metrics, opts),
              "Couldn't write JSON metrics over HTTP");
}

void RegisterMetricsJsonHandler(Webserver* webserver, const MetricRegistry* const metrics) {
  Webserver::PrerenderedPathHandlerCallback callback = boost::bind(WriteMetricsAsJson, metrics,
                                                                   _1, _2);
  bool not_styled = false;
  bool not_on_nav_bar = false;
  bool is_on_nav_bar = true;
  webserver->RegisterPrerenderedPathHandler("/metrics", "Metrics", callback,
                                            not_styled, is_on_nav_bar);

  // The old name -- this is preserved for compatibility with older releases of
  // monitoring software which expects the old name.
  webserver->RegisterPrerenderedPathHandler("/jsonmetricz", "Metrics", callback,
                                            not_styled, not_on_nav_bar);
}

} // namespace kudu
