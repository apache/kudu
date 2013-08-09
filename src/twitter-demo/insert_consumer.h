// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TWITTER_DEMO_INSERT_CONSUMER_H
#define KUDU_TWITTER_DEMO_INSERT_CONSUMER_H

#include "twitter-demo/twitter_streamer.h"

#include <string>
#include <tr1/memory>

#include "common/schema.h"
#include "util/slice.h"
#include "twitter-demo/parser.h"
#include "tserver/tserver.proxy.h"

namespace kudu {
namespace twitter_demo {

// Consumer of tweet data which parses the JSON and inserts
// into a remote tablet via RPC.
class InsertConsumer : public TwitterConsumer {
 public:
  InsertConsumer(const std::tr1::shared_ptr<tserver::TabletServerServiceProxy> &proxy);
  ~InsertConsumer();

  virtual void ConsumeJSON(const Slice& json);

 private:
  Schema schema_;

  TwitterEventParser parser_;

  // Reusable object for latest event.
  TwitterEvent event_;

  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> proxy_;
};

} // namespace twitter_demo
} // namespace kudu
#endif
