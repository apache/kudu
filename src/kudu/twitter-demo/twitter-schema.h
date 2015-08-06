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
using client::KuduSchemaBuilder;

inline KuduSchema CreateTwitterSchema() {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("tweet_id")->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();
  b.AddColumn("text")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("source")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("created_at")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("user_id")->Type(KuduColumnSchema::INT64)->NotNull();
  b.AddColumn("user_name")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("user_description")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("user_location")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("user_followers_count")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("user_friends_count")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("user_image_url")->Type(KuduColumnSchema::STRING)->NotNull();
  CHECK_OK(b.Build(&s));
  return s;
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
