// Copyright (c) 2013, Cloudera, inc.
//
// Inline functions to create the Twitter schema
#ifndef KUDU_TWITTER_DEMO_TWITTER_SCHEMA_H
#define KUDU_TWITTER_DEMO_TWITTER_SCHEMA_H

#include <boost/assign/list_of.hpp>

#include "common/schema.h"

namespace kudu {
namespace twitter_demo {

inline Schema CreateTwitterSchema() {
  return Schema(boost::assign::list_of
                (ColumnSchema("tweet_id", UINT64))
                (ColumnSchema("text", STRING))
                (ColumnSchema("source", STRING))
                (ColumnSchema("created_at", STRING))
                (ColumnSchema("user_id", UINT64))
                (ColumnSchema("user_name", STRING))
                (ColumnSchema("user_description", STRING))
                (ColumnSchema("user_location", STRING))
                (ColumnSchema("user_followers_count", UINT32))
                (ColumnSchema("user_friends_count", UINT32))
                (ColumnSchema("user_image_url", STRING)),
                1);
}

} // namespace twitter_demo
} // namespace kudu
#endif
