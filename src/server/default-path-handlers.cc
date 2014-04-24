// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "server/default-path-handlers.h"

#include <sstream>
#include <string>
#include <fstream>
#include <sys/stat.h>
#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <google/malloc_extension.h>
#include <vector>

#include "gutil/map-util.h"
#include "gutil/strings/split.h"
#include "server/pprof-path-handlers.cc"
#include "server/webserver.h"
#include "util/histogram.pb.h"
#include "util/metrics.h"
#include "util/jsonwriter.h"

using boost::replace_all;
using google::CommandlineFlagsIntoString;
using std::string;

DECLARE_bool(enable_process_lifetime_heap_profiling);
DEFINE_int64(web_log_bytes, 1024 * 1024,
    "The maximum number of bytes to display on the debug webserver's log page");

namespace kudu {

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
static void LogsHandler(const Webserver::ArgumentMap& args, stringstream* output) {
  bool as_text = (args.find("raw") != args.end());
  Tags tags(as_text);
  string logfile;
  GetFullLogFilename(google::INFO, &logfile);
  (*output) << tags.header <<"INFO logs" << tags.end_header << endl;
  (*output) << "Log path is: " << logfile << endl;

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
    (*output) << tags.line_break <<"Showing last " << FLAGS_web_log_bytes
              << " bytes of log" << endl;
    (*output) << tags.line_break << tags.pre_tag << log.rdbuf() << tags.end_pre_tag;

  } else {
    (*output) << tags.line_break << "Couldn't open INFO log file: " << logfile;
  }
}

// Registered to handle "/flags", and prints out all command-line flags and their values
static void FlagsHandler(const Webserver::ArgumentMap& args, stringstream* output) {
  bool as_text = (args.find("raw") != args.end());
  Tags tags(as_text);
  (*output) << tags.header << "Command-line Flags" << tags.end_header;
  (*output) << tags.pre_tag << CommandlineFlagsIntoString() << tags.end_pre_tag;
}

// Registered to handle "/memz", and prints out memory allocation statistics.
static void MemUsageHandler(const Webserver::ArgumentMap& args, stringstream* output) {
  bool as_text = (args.find("raw") != args.end());
  Tags tags(as_text);

  (*output) << tags.pre_tag;
#ifndef TCMALLOC_ENABLED
  (*output) << "Memory tracking is not available unless tcmalloc is enabled.";
#else
  char buf[2048];
  MallocExtension::instance()->GetStats(buf, 2048);
  // Replace new lines with <br> for html
  string tmp(buf);
  replace_all(tmp, "\n", tags.line_break);
  (*output) << tmp << tags.end_pre_tag;
#endif
}

void AddDefaultPathHandlers(Webserver* webserver) {
  webserver->RegisterPathHandler("/logs", LogsHandler);
  webserver->RegisterPathHandler("/varz", FlagsHandler);
  webserver->RegisterPathHandler("/memz", MemUsageHandler);

#ifdef TCMALLOC_ENABLED
  // Remote (on-demand) profiling is disabled if the process is already being profiled.
  if (!FLAGS_enable_process_lifetime_heap_profiling) {
    AddPprofPathHandlers(webserver);
  }
#endif
}

static void WriteMetricsAsJson(const MetricRegistry* const metrics,
    const Webserver::ArgumentMap& args, std::stringstream* output) {
  JsonWriter writer(output);
  // check if the requestor is asking for particular histograms
  // by passing a comma separated list of histogram names.
  HistogramSnapshotsListPB histograms_pb;
  const string* histogram_names = FindOrNull(args, "histograms");
  if (histogram_names != NULL) {
    vector<string> histogram_names_vec;
    SplitStringUsing(*histogram_names, ",", &histogram_names_vec);
    BOOST_FOREACH(const string& histogram_name, histogram_names_vec) {
      Histogram* histogram = metrics->FindHistogram(histogram_name);
      if (histogram == NULL) {
        VLOG(1) << "Client requested a histogram that does not exist: " << histogram_name;
        continue;
      }
      HistogramSnapshotPB* snapshot = histograms_pb.add_histograms();
      snapshot->set_name(histogram_name);
      Status status = histogram->CaptureValueCounts(snapshot);
      if (!status.ok()) {
        WARN_NOT_OK(status, "Cannot capture histogram snapshot");
        return;
      }
    }
  }
  writer.StartObject();
  WARN_NOT_OK(metrics->WriteAsJson(&writer), "Couldn't write JSON metrics over HTTP");
  if (histograms_pb.histograms_size() > 0) {
    writer.String("histograms");
    writer.StartArray();
    // TODO maybe clean this up. We want 'histograms' to be a a top level
    // key in the same map as 'metrics', but if we json-serialize the
    // whole 'histograms_pb' it will be written an an object i.e.:
    // {metrics:[metrics...],{histograms:[histograms...]}} instead of the
    // desired {metrics:[metrics...],histograms:[histograms...]}. The list pb
    // might still be useful if we want to allow to get the histograms through
    // RPC but if not we can (and should) let it go.
    BOOST_FOREACH(const HistogramSnapshotPB& snapshot, histograms_pb.histograms()) {
      writer.Protobuf(snapshot);
    }
    writer.EndArray();
  }
  writer.EndObject();
}

void RegisterMetricsJsonHandler(Webserver* webserver, const MetricRegistry* const metrics) {
  Webserver::PathHandlerCallback callback = boost::bind(WriteMetricsAsJson, metrics, _1, _2);
  bool is_styled = false;
  bool is_on_nav_bar = true;
  webserver->RegisterPathHandler("/jsonmetricz", callback, is_styled, is_on_nav_bar);
}

} // namespace kudu
