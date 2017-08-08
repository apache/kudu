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

#include "kudu/twitter-demo/insert_consumer.h"

#include <ctime>
#include <mutex>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/twitter-demo/parser.h"
#include "kudu/twitter-demo/twitter-schema.h"
#include "kudu/util/status.h"

using kudu::client::KuduInsert;
using kudu::client::KuduClient;
using kudu::client::KuduSession;
using kudu::client::KuduTableCreator;
using std::string;
using std::vector;

namespace kudu {
namespace twitter_demo {

InsertConsumer::InsertConsumer(client::sp::shared_ptr<KuduClient> client)
  : initted_(false),
    schema_(CreateTwitterSchema()),
    client_(std::move(client)) {
}

Status InsertConsumer::Init() {
  static const string kTableName = "twitter";
  Status s = client_->OpenTable(kTableName, &table_);
  if (s.IsNotFound()) {
    gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    RETURN_NOT_OK_PREPEND(table_creator->table_name(kTableName)
                          .schema(&schema_)
                          .Create(),
                          "Couldn't create twitter table");
    s = client_->OpenTable(kTableName, &table_);
  }
  RETURN_NOT_OK_PREPEND(s, "Couldn't open twitter table");

  session_ = client_->NewSession();
  session_->SetTimeoutMillis(1000);
  RETURN_NOT_OK(session_->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  initted_ = true;
  return Status::OK();
}

InsertConsumer::~InsertConsumer() {
  Status s(session_->Flush());
  if (!s.ok()) {
    bool overflow;
    vector<client::KuduError*> errors;
    ElementDeleter d(&errors);
    session_->GetPendingErrors(&errors, &overflow);
    for (const client::KuduError* error : errors) {
      LOG(WARNING) << "Failed to insert row " << error->failed_op().ToString()
                   << ": " << error->status().ToString();
    }
  }
}

void InsertConsumer::ConsumeJSON(const Slice& json_slice) {
  CHECK(initted_);
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

  string created_at = TwitterEventParser::ReformatTime(event_.tweet_event.created_at);

  gscoped_ptr<KuduInsert> ins(table_->NewInsert());
  KuduPartialRow* r = ins->mutable_row();
  CHECK_OK(r->SetInt64("tweet_id", event_.tweet_event.tweet_id));
  CHECK_OK(r->SetStringCopy("text", event_.tweet_event.text));
  CHECK_OK(r->SetStringCopy("source", event_.tweet_event.source));
  CHECK_OK(r->SetStringCopy("created_at", created_at));
  CHECK_OK(r->SetInt64("user_id", event_.tweet_event.user_id));
  CHECK_OK(r->SetStringCopy("user_name", event_.tweet_event.user_name));
  CHECK_OK(r->SetStringCopy("user_description", event_.tweet_event.user_description));
  CHECK_OK(r->SetStringCopy("user_location", event_.tweet_event.user_location));
  CHECK_OK(r->SetInt32("user_followers_count", event_.tweet_event.user_followers_count));
  CHECK_OK(r->SetInt32("user_friends_count", event_.tweet_event.user_friends_count));
  CHECK_OK(r->SetStringCopy("user_image_url", event_.tweet_event.user_image_url));
  CHECK_OK(session_->Apply(ins.release()));
}

} // namespace twitter_demo
} // namespace kudu
