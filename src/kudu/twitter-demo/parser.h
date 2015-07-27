// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TWITTER_DEMO_PARSER_H
#define KUDU_TWITTER_DEMO_PARSER_H

#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
namespace twitter_demo {

enum TwitterEventType {
  NONE = 0,
  TWEET = 1,
  DELETE_TWEET = 2
};


struct TweetEvent {
  int64_t tweet_id;
  std::string text;
  std::string source;
  std::string created_at;
  // TODO: add geolocation
  int64_t user_id;
  std::string user_name;
  std::string user_description;
  std::string user_location;
  int32_t user_followers_count;
  int32_t user_friends_count;
  std::string user_image_url;
};

struct DeleteTweetEvent {
  int64_t tweet_id;
  int64_t user_id;
};

struct TwitterEvent {
  TwitterEvent() : type(NONE) {}

  // The type of event. Only one of the various events below will
  // be valid, depending on this type value.
  TwitterEventType type;

  // The different event types. These are separate fields rather than
  // a union so that we can reuse string storage when parsing multiple
  // events.

  TweetEvent tweet_event;
  DeleteTweetEvent delete_event;
};

class TwitterEventParser {
 public:
  TwitterEventParser();
  ~TwitterEventParser();

  Status Parse(const std::string& json, TwitterEvent* event);

  static std::string ReformatTime(const std::string& time);

 private:
  DISALLOW_COPY_AND_ASSIGN(TwitterEventParser);
};

} // namespace twitter_demo
} // namespace kudu
#endif
