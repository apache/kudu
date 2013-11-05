// Copyright (c) 2013, Cloudera, inc.

#include <stdint.h>
#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <curl/curl.h>
#include <fstream>

#include "twitter-demo/oauth.h"
#include "twitter-demo/insert_consumer.h"
#include "twitter-demo/twitter_streamer.h"
#include "gutil/macros.h"
#include "gutil/once.h"
#include "rpc/messenger.h"
#include "tserver/tablet_server.h"
#include "tserver/tserver_service.proxy.h"
#include "util/net/net_util.h"
#include "util/slice.h"
#include "util/status.h"

DEFINE_string(twitter_firehose_sink, "console",
              "Where to write firehose output.\n"
              "Valid values: console,rpc");
DEFINE_string(twitter_rpc_sink_address, "localhost",
              "Address of tablet server to write to");

DEFINE_string(twitter_firehose_source, "api",
              "Where to obtain firehose input.\n"
              "Valid values: api,file");
DEFINE_string(twitter_firehose_file, "/dev/fd/0",
              "File to read firehose data from, if 'file' is configured.");


using base::FreeDeleter;
using std::string;
using std::tr1::shared_ptr;

namespace kudu {
namespace twitter_demo {

using tserver::TabletServer;
using tserver::TabletServerServiceProxy;

// Consumer which simply logs messages to the console.
class LoggingConsumer : public TwitterConsumer {
 public:
  virtual void ConsumeJSON(const Slice& json) {
    std::cout << json.ToString();
  }
};

gscoped_ptr<TwitterConsumer> CreateInsertConsumer() {
  HostPort hp;
  CHECK_OK(hp.ParseString(FLAGS_twitter_rpc_sink_address, TabletServer::kDefaultPort));
  std::vector<Sockaddr> addrs;
  CHECK_OK(hp.ResolveAddresses(&addrs));
  if (addrs.size() > 1) {
    LOG(WARNING) << "Host resolved to more than one address, using: "
                 << addrs.front().ToString();
  }

  shared_ptr<rpc::Messenger> msgr;
  rpc::MessengerBuilder bld("Client");
  CHECK_OK(bld.Build(&msgr));
  shared_ptr<TabletServerServiceProxy> proxy(
    new TabletServerServiceProxy(msgr, addrs.front()));
  return gscoped_ptr<TwitterConsumer>(new InsertConsumer(proxy));
}

static void IngestFromFile(const string& file, gscoped_ptr<TwitterConsumer> consumer) {
  std::ifstream in(file.c_str());
  CHECK(in.is_open()) << "Couldn't open " << file;

  string line;
  while (std::getline(in, line)) {
    consumer->ConsumeJSON(line);
  }
}

static int main(int argc, char** argv) {
  // Since this is meant to be run by a user, not a daemon,
  // log to stderr by default.
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);

  gscoped_ptr<TwitterConsumer> consumer;
  if (FLAGS_twitter_firehose_sink == "console") {
    consumer.reset(new LoggingConsumer);
  } else if (FLAGS_twitter_firehose_sink == "rpc") {
    consumer = CreateInsertConsumer();
  } else {
    LOG(FATAL) << "Unknown sink: " << FLAGS_twitter_firehose_sink;
  }

  if (FLAGS_twitter_firehose_source == "api") {
    TwitterStreamer streamer(consumer.get());
    CHECK_OK(streamer.Init());
    CHECK_OK(streamer.Start());
    CHECK_OK(streamer.Join());
  } else if (FLAGS_twitter_firehose_source == "file") {
    IngestFromFile(FLAGS_twitter_firehose_file, consumer.Pass());
  } else {
    LOG(FATAL) << "Unknown source: " << FLAGS_twitter_firehose_source;
  }
  return 0;
}

} // namespace twitter_demo
} // namespace kudu

int main(int argc, char** argv) {
  return kudu::twitter_demo::main(argc, argv);
}
