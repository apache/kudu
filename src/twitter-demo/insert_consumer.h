// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TWITTER_DEMO_INSERT_CONSUMER_H
#define KUDU_TWITTER_DEMO_INSERT_CONSUMER_H

#include "twitter-demo/twitter_streamer.h"

#include <string>
#include <tr1/memory>

#include "common/schema.h"
#include "rpc/rpc_controller.h"
#include "tserver/tserver.proxy.h"
#include "twitter-demo/parser.h"
#include "util/locks.h"
#include "util/slice.h"

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
  void BatchFinished();

  Schema schema_;

  TwitterEventParser parser_;

  // Reusable object for latest event.
  TwitterEvent event_;

  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> proxy_;

  simple_spinlock lock_;
  bool request_pending_;
  tserver::InsertRequestPB request_;
  tserver::InsertResponsePB response_;
  rpc::RpcController rpc_;
};

} // namespace twitter_demo
} // namespace kudu
#endif
