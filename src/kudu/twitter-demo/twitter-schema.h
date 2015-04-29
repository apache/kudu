// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Inline functions to create the Twitter schema
#ifndef KUDU_TWITTER_DEMO_TWITTER_SCHEMA_H
#define KUDU_TWITTER_DEMO_TWITTER_SCHEMA_H

#include <boost/assign/list_of.hpp>

#include "kudu/client/schema.h"

namespace kudu {
namespace twitter_demo {

using client::KuduColumnSchema;
using client::KuduSchema;

inline KuduSchema CreateTwitterSchema() {
  return KuduSchema(boost::assign::list_of
                (KuduColumnSchema("tweet_id", KuduColumnSchema::INT64))
                (KuduColumnSchema("text", KuduColumnSchema::STRING))
                (KuduColumnSchema("source", KuduColumnSchema::STRING))
                (KuduColumnSchema("created_at", KuduColumnSchema::STRING))
                (KuduColumnSchema("user_id", KuduColumnSchema::INT64))
                (KuduColumnSchema("user_name", KuduColumnSchema::STRING))
                (KuduColumnSchema("user_description", KuduColumnSchema::STRING))
                (KuduColumnSchema("user_location", KuduColumnSchema::STRING))
                (KuduColumnSchema("user_followers_count", KuduColumnSchema::INT32))
                (KuduColumnSchema("user_friends_count", KuduColumnSchema::INT32))
                (KuduColumnSchema("user_image_url", KuduColumnSchema::STRING)),
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
