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

/*

Schema for Impala:

CREATE TABLE twitter (
  tweet_id bigint,
  text string,
  source string,
  created_at string,
  user_id bigint,
  user_name string,
  user_description string,
  user_location string,
  user_followers_count int,
  user_friends_count int,
  user_image_url string);


Schema for MySQL:

CREATE TABLE twitter (
  tweet_id bigint not null primary key,
  tweet_text varchar(1000) not null,
  source varchar(1000) not null,
  created_at varchar(1000) not null,
  user_id bigint not null,
  user_name varchar(1000) not null,
  user_description varchar(1000) not null,
  user_location varchar(1000) not null,
  user_followers_count int not null,
  user_friends_count int not null,
  user_image_url varchar(1000) not null);

*/
