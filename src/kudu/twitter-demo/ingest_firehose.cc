// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <stdint.h>
#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <curl/curl.h>
#include <fstream>
#include <iostream>

#include "kudu/client/client.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/once.h"
#include "kudu/rpc/messenger.h"
#include "kudu/master/master.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/twitter-demo/oauth.h"
#include "kudu/twitter-demo/insert_consumer.h"
#include "kudu/twitter-demo/twitter_streamer.h"
#include "kudu/util/flags.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DEFINE_string(twitter_firehose_sink, "console",
              "Where to write firehose output.\n"
              "Valid values: console,rpc");
DEFINE_string(twitter_rpc_master_address, "localhost",
              "Address of master for the cluster to write to");

DEFINE_string(twitter_firehose_source, "api",
              "Where to obtain firehose input.\n"
              "Valid values: api,file");
DEFINE_string(twitter_firehose_file, "/dev/fd/0",
              "File to read firehose data from, if 'file' is configured.");


using std::string;
using std::tr1::shared_ptr;

namespace kudu {
namespace twitter_demo {

using tserver::TabletServerServiceProxy;

// Consumer which simply logs messages to the console.
class LoggingConsumer : public TwitterConsumer {
 public:
  virtual void ConsumeJSON(const Slice& json) OVERRIDE {
    std::cout << json.ToString();
  }
};

gscoped_ptr<TwitterConsumer> CreateInsertConsumer() {
  shared_ptr<client::KuduClient> client;
  CHECK_OK(client::KuduClientBuilder()
           .add_master_server_addr(FLAGS_twitter_rpc_master_address)
           .Build(&client));

  gscoped_ptr<InsertConsumer> ret(new InsertConsumer(client));
  CHECK_OK(ret->Init());
  return gscoped_ptr<TwitterConsumer>(ret.Pass()); // up-cast
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
  kudu::ParseCommandLineFlags(&argc, &argv, true);
  kudu::InitGoogleLoggingSafe(argv[0]);

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
