// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TWITTER_DEMO_PARSER_H
#define KUDU_TWITTER_DEMO_PARSER_H

#include <string>

#include "gutil/macros.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {
namespace twitter_demo {

enum TwitterEventType {
  NONE = 0,
  TWEET = 1,
  DELETE_TWEET = 2
};


struct TweetEvent {
  uint64_t tweet_id;
  std::string text;
  std::string source;
  string created_at;
  // TODO: add geolocation
  uint64_t user_id;
  std::string user_name;
  std::string user_description;
  std::string user_location;
  uint32_t user_followers_count;
  uint32_t user_friends_count;
  std::string user_image_url;
};

struct DeleteTweetEvent {
  uint64_t tweet_id;
  uint64_t user_id;
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

 private:
  DISALLOW_COPY_AND_ASSIGN(TwitterEventParser);
};

} // namespace twitter_demo
} // namespace kudu
#endif
