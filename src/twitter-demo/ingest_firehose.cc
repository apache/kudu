// Copyright (c) 2013, Cloudera, inc.

#include <stdint.h>
#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <curl/curl.h>

#include "twitter-demo/oauth.h"
#include "twitter-demo/twitter_streamer.h"
#include "gutil/macros.h"
#include "gutil/once.h"
#include "util/slice.h"
#include "util/status.h"

using base::FreeDeleter;
using std::string;

namespace kudu {
namespace twitter_demo {

// Consumer which simply logs messages to the console.
class LoggingConsumer : public TwitterConsumer {
 public:
  virtual void ConsumeJSON(const Slice& json) {
    std::cout << json.ToString();
  }
};

} // namespace twitter_demo
} // namespace kudu

int main(int argc, char** argv) {
  using namespace kudu;
  using namespace kudu::twitter_demo;
  // Since this is meant to be run by a user, not a daemon,
  // log to stderr by default.
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);

  LoggingConsumer consumer;
  TwitterStreamer streamer(&consumer);
  CHECK_OK(streamer.Init());
  CHECK_OK(streamer.Start());
  CHECK_OK(streamer.Join());

  return 0;
}
