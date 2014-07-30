// Copyright (c) 2013, Cloudera, inc.

#include "kudu/twitter-demo/parser.h"

#include <time.h>

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include "kudu/gutil/stringprintf.h"
#include "kudu/util/slice.h"

namespace kudu {
namespace twitter_demo {

TwitterEventParser::TwitterEventParser() {
}

TwitterEventParser::~TwitterEventParser() {
}

template<class DocType>
static Status ExtractInt64(const DocType& doc,
                           const char* field,
                           uint64_t* result) {
  if (PREDICT_FALSE(!doc.HasMember(field))) {
    return Status::InvalidArgument("Missing field", field);
  }
  const rapidjson::Value& val = doc[field];
  if (PREDICT_FALSE(!val.IsUint64())) {
    return Status::InvalidArgument("Bad field type for field", field);
  }
  *result = val.GetUint64();
  return Status::OK();
}

template<class DocType>
static Status ExtractInt32(const DocType& doc,
                           const char* field,
                           uint32_t* result) {
  if (PREDICT_FALSE(!doc.HasMember(field))) {
    return Status::InvalidArgument("Missing field", field);
  }
  const rapidjson::Value& val = doc[field];
  if (PREDICT_FALSE(!val.IsUint())) {
    return Status::InvalidArgument("Bad field type for field", field);
  }
  *result = val.GetUint();
  return Status::OK();
}

template<class DocType>
static Status ExtractString(const DocType& doc,
                            const char* field,
                            string* result) {
  if (PREDICT_FALSE(!doc.HasMember(field))) {
    return Status::InvalidArgument("Missing field", field);
  }
  const rapidjson::Value& val = doc[field];
  if (PREDICT_FALSE(!val.IsString())) {
    if (val.IsNull()) {
      *result = "";
      return Status::OK();
    }
    return Status::InvalidArgument("Bad field type for field", field);
  }
  result->assign(val.GetString());
  return Status::OK();
}

static Status ParseDelete(const rapidjson::Document& d, TwitterEvent* event) {
  event->type = DELETE_TWEET;
  DeleteTweetEvent* e = &event->delete_event;

  DCHECK(d.HasMember("delete")); // should be checked before calling this.
  const rapidjson::Value &delete_obj = d["delete"];
  if (!delete_obj.IsObject()) {
    return Status::InvalidArgument("'delete' field was not an object");
  }

  if (!delete_obj.HasMember("status")) {
    return Status::InvalidArgument("delete record missing 'status' field");
  }

  const rapidjson::Value &status = delete_obj["status"];
  if (!status.IsObject()) {
    return Status::InvalidArgument("'status' field was not an object");
  }

  RETURN_NOT_OK(ExtractInt64(status, "id", &e->tweet_id));
  RETURN_NOT_OK(ExtractInt64(status, "user_id", &e->user_id));
  return Status::OK();
}

static Status ParseTweet(const rapidjson::Document& d, TwitterEvent* event) {
  event->type = TWEET;
  TweetEvent* e = &event->tweet_event;

  RETURN_NOT_OK(ExtractString(d, "created_at", &e->created_at));
  RETURN_NOT_OK(ExtractInt64(d, "id", &e->tweet_id));
  RETURN_NOT_OK(ExtractString(d, "text", &e->text));
  RETURN_NOT_OK(ExtractString(d, "source", &e->source));

  if (!d.HasMember("user")) {
    return Status::InvalidArgument("missing user!");
  }

  const rapidjson::Value &user = d["user"];
  if (!user.IsObject()) {
    return Status::InvalidArgument("user field should be an object");
  }
  RETURN_NOT_OK(ExtractInt64(user, "id", &e->user_id));
  RETURN_NOT_OK(ExtractString(user, "name", &e->user_name));
  RETURN_NOT_OK(ExtractString(user, "location", &e->user_location));
  RETURN_NOT_OK(ExtractString(user, "description", &e->user_description));
  RETURN_NOT_OK(ExtractInt32(user, "followers_count", &e->user_followers_count));
  RETURN_NOT_OK(ExtractInt32(user, "friends_count", &e->user_friends_count));
  RETURN_NOT_OK(ExtractString(user, "profile_image_url", &e->user_image_url));

  return Status::OK();
}

Status TwitterEventParser::Parse(const string& json, TwitterEvent* event) {
  rapidjson::Document d;
  d.Parse<0>(json.c_str());

  if (PREDICT_FALSE(!d.IsObject())) {
    return Status::InvalidArgument("string is not a JSON object");
  }

  if (d.HasMember("delete")) {
    return ParseDelete(d, event);
  }
  return ParseTweet(d, event);
}

string TwitterEventParser::ReformatTime(const string& twitter_time) {
  struct tm t;
  memset(&t, 0, sizeof(t));
  // Example: Wed Aug 14 06:31:07 +0000 2013
  char* x = strptime(twitter_time.c_str(), "%a %b %d %H:%M:%S +0000 %Y", &t);
  if (*x != '\0') {
    return StringPrintf("unparseable date, date=%s, leftover=%s", twitter_time.c_str(), x);
  }

  char buf[100];
  size_t n = strftime(buf, arraysize(buf), "%Y%m%d%H%M%S", &t);
  CHECK_GT(n, 0);
  return string(buf);
}


} // namespace twitter_demo
} // namespace kudu
