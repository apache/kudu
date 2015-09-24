// Copyright 2013 Cloudera, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Inline function to create the YCSB schema
#ifndef KUDU_BENCHMARKS_YCSB_SCHEMA_H
#define KUDU_BENCHMARKS_YCSB_SCHEMA_H

#include <boost/assign/list_of.hpp>
#include "kudu/client/schema.h"

namespace kudu {

static const client::KuduColumnSchema::DataType kString =
    client::KuduColumnSchema::STRING;

inline client::KuduSchema CreateYCSBSchema() {
  client::KuduSchema s;
  client::KuduSchemaBuilder b;

  b.AddColumn("key")->Type(kString)->NotNull()->PrimaryKey();
  b.AddColumn("field0")->Type(kString)->NotNull();
  b.AddColumn("field1")->Type(kString)->NotNull();
  b.AddColumn("field2")->Type(kString)->NotNull();
  b.AddColumn("field3")->Type(kString)->NotNull();
  b.AddColumn("field4")->Type(kString)->NotNull();
  b.AddColumn("field5")->Type(kString)->NotNull();
  b.AddColumn("field6")->Type(kString)->NotNull();
  b.AddColumn("field7")->Type(kString)->NotNull();
  b.AddColumn("field8")->Type(kString)->NotNull();
  b.AddColumn("field9")->Type(kString)->NotNull();
  CHECK_OK(b.Build(&s));
  return s;
}

} // namespace kudu
#endif

