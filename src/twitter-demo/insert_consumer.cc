// Copyright (c) 2013, Cloudera, inc.

#include "twitter-demo/insert_consumer.h"

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <string>
#include <time.h>
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
using tserver::InsertResponsePB;
using rpc::RpcController;

InsertConsumer::InsertConsumer(const std::tr1::shared_ptr<TabletServerServiceProxy> &proxy)
  : schema_(CreateTwitterSchema()),
    proxy_(proxy),
    request_pending_(false) {
  request_.set_tablet_id("twitter");
  CHECK_OK(SchemaToColumnPBs(schema_, request_.mutable_data()->mutable_schema()));
  request_.mutable_data()->set_num_key_columns(schema_.num_key_columns());
}

InsertConsumer::~InsertConsumer() {
  // TODO: to be safe, we probably need to cancel any current RPC,
  // or else the callback will get called on the destroyed object.
  // Given this is just demo code, cutting this corner.
  CHECK(!request_pending_);
}

void InsertConsumer::BatchFinished() {
  boost::lock_guard<simple_spinlock> l(lock_);
  request_pending_ = false;

  Status s = rpc_.status();
  if (!s.ok()) {
    LOG(WARNING) << "RPC error inserting row: " << s.ToString();
    return;
  }

  if (response_.has_error()) {
    LOG(WARNING) << "Unable to insert rows: " << response_.error().DebugString();
    return;
  }

  if (response_.per_row_errors().size() > 0) {
    BOOST_FOREACH(const InsertResponsePB::PerRowErrorPB& err, response_.per_row_errors()) {
      if (err.error().code() != AppStatusPB::ALREADY_PRESENT) {
        LOG(WARNING) << "Per-row errors for row " << err.row_index() << ": " << err.DebugString();
      }
    }
  }

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

  // Build the row to insert
  RowBuilder rb(schema_);
  rb.AddUint64(event_.tweet_event.tweet_id);
  rb.AddString(event_.tweet_event.text);
  rb.AddString(event_.tweet_event.source);
  rb.AddString(TwitterEventParser::ReformatTime(event_.tweet_event.created_at));
  rb.AddUint64(event_.tweet_event.user_id);
  rb.AddString(event_.tweet_event.user_name);
  rb.AddString(event_.tweet_event.user_description);
  rb.AddString(event_.tweet_event.user_location);
  rb.AddUint32(event_.tweet_event.user_followers_count);
  rb.AddUint32(event_.tweet_event.user_friends_count);
  rb.AddString(event_.tweet_event.user_image_url);

  // Append the row to the next insert to be sent
  RowwiseRowBlockPB* data = request_.mutable_data();
  AddRowToRowBlockPB(rb.row(), data);

  boost::lock_guard<simple_spinlock> l(lock_);
  if (!request_pending_) {
    request_pending_ = true;

    rpc_.Reset();
    rpc_.set_timeout(MonoDelta::FromMilliseconds(1000));
    VLOG(1) << "Sending batch of " << data->num_rows();
    proxy_->InsertAsync(request_, &response_, &rpc_, boost::bind(&InsertConsumer::BatchFinished, this));

    // TODO: add method to clear the data portions of a RowwiseRowBlockPB
    data->set_num_rows(0);
    data->clear_rows();
    data->clear_indirect_data();
  }
}

} // namespace twitter_demo
} // namespace kudu
