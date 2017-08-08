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

#include "kudu/client/table-internal.h"

#include <string>

using std::string;

namespace kudu {
namespace client {

using sp::shared_ptr;

KuduTable::Data::Data(shared_ptr<KuduClient> client,
                      string name,
                      string id,
                      int num_replicas,
                      const KuduSchema& schema,
                      PartitionSchema partition_schema)
    : client_(std::move(client)),
      name_(std::move(name)),
      id_(std::move(id)),
      num_replicas_(num_replicas),
      schema_(schema),
      partition_schema_(std::move(partition_schema)) {
}

KuduTable::Data::~Data() {
}

} // namespace client
} // namespace kudu
