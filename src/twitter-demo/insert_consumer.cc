// Copyright (c) 2013, Cloudera, inc.

#include "twitter-demo/insert_consumer.h"

#include <boost/assign/list_of.hpp>
#include <string>
#include <tr1/memory>

#include "common/wire_protocol.h"
#include "common/row.h"
#include "common/schema.h"
#include "tserver/tserver.proxy.h"
#include "tserver/tserver.pb.h"
#include "twitter-demo/parser.h"
#include "twitter-demo/twitter-schema.h"
#include "util/status.h"

namespace kudu {
namespace twitter_demo {

using tserver::TabletServerServiceProxy;
using tserver::InsertRequestPB;
using tserver::InsertResponsePB;
using rpc::RpcController;

InsertConsumer::InsertConsumer(const std::tr1::shared_ptr<TabletServerServiceProxy> &proxy)
  : schema_(CreateTwitterSchema()),
    proxy_(proxy) {
}

InsertConsumer::~InsertConsumer() {
}

void InsertConsumer::ConsumeJSON(const Slice& json_slice) {
  string json = json_slice.ToString();
  Status s = parser_.Parse(json, &event_);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to parse JSON string: " << json << ": " << s.ToString();
    return;
  }

  if (event_.type == DELETE_TWEET) {
    // Not currently supported.
    return;
  }

  InsertRequestPB req;
  InsertResponsePB resp;
  RpcController controller;

  req.set_tablet_id("twitter");
  RowwiseRowBlockPB* data = req.mutable_data();
  CHECK_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
  data->set_num_key_columns(schema_.num_key_columns());

  RowBuilder rb(schema_);
  rb.AddUint64(event_.tweet_event.tweet_id);
  rb.AddString(event_.tweet_event.text);
  rb.AddString(event_.tweet_event.source);
  rb.AddString(event_.tweet_event.created_at);
  rb.AddUint64(event_.tweet_event.user_id);
  rb.AddString(event_.tweet_event.user_name);
  rb.AddString(event_.tweet_event.user_description);
  rb.AddString(event_.tweet_event.user_location);
  rb.AddUint32(event_.tweet_event.user_followers_count);
  rb.AddUint32(event_.tweet_event.user_friends_count);
  rb.AddString(event_.tweet_event.user_image_url);
  AddRowToRowBlockPB(rb.row(), data);

  controller.set_timeout(MonoDelta::FromMilliseconds(1000));
  s = proxy_->Insert(req, &resp, &controller);
  if (!s.ok()) {
    LOG(WARNING) << "RPC error inserting row: " << s.ToString();
    return;
  }

  if (resp.has_error()) {
    LOG(WARNING) << "Unable to insert row '" << schema_.DebugRow(rb.row()) << "': "
                 << resp.error().DebugString();
    return;
  }

  if (resp.per_row_errors().size() > 0) {
    LOG(WARNING) << "Per-row errors for '" << schema_.DebugRow(rb.row()) << "': "
                 << resp.per_row_errors().Get(0).DebugString();
    return;
  }
  LOG(INFO) << "Inserted " << schema_.DebugRow(rb.row());
}

} // namespace twitter_demo
} // namespace kudu
