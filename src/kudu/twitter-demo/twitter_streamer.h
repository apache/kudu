// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TWITTER_DEMO_TWITTER_STREAMER_H
#define KUDU_TWITTER_DEMO_TWITTER_STREAMER_H

#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
namespace twitter_demo {

class TwitterConsumer {
 public:
  virtual void ConsumeJSON(const Slice& json) = 0;
  virtual ~TwitterConsumer() {}
};

class TwitterStreamer {
 public:
  explicit TwitterStreamer(TwitterConsumer* consumer)
    : consumer_(consumer) {
  }

  Status Init();
  Status Start();
  Status Join();

 private:
  friend size_t DataReceivedCallback(void* buffer, size_t size, size_t nmemb, void* user_ptr);
  void StreamThread();
  Status DoStreaming();
  size_t DataReceived(const Slice& data);

  boost::thread thread_;
  boost::mutex lock_;
  Status stream_status_;

  faststring recv_buf_;

  TwitterConsumer* consumer_;

  DISALLOW_COPY_AND_ASSIGN(TwitterStreamer);
};


} // namespace twitter_demo
} // namespace kudu
#endif
