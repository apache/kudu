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

#include "kudu/client/tablet-internal.h"

#include <string>
#include <utility>
#include <vector>

#include "kudu/gutil/stl_util.h"

using std::string;
using std::vector;

namespace kudu {
namespace client {

KuduTablet::Data::Data(string id, vector<const KuduReplica*> replicas)
    : id_(std::move(id)),
      replicas_(std::move(replicas)) {
}

KuduTablet::Data::Data(string id, vector<const KuduReplica*> replicas,
                       string table_id, string table_name)
    : id_(std::move(id)),
      replicas_(std::move(replicas)),
      table_id_(std::move(table_id)),
      table_name_(std::move(table_name)) {
}

KuduTablet::Data::~Data() {
  STLDeleteElements(&replicas_);
}

} // namespace client
} // namespace kudu
