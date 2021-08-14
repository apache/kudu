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

#include "kudu/client/table_creator-internal.h"

#include <string>
#include <vector>

using std::string;
using std::vector;

namespace kudu {
namespace client {

KuduTableCreator::Data::Data(KuduClient* client)
    : client_(client),
      schema_(nullptr),
      wait_(true) {
}

KuduTableCreator::KuduRangePartition::Data::Data(
    KuduPartialRow* lower_bound,
    KuduPartialRow* upper_bound,
    RangePartitionBound lower_bound_type,
    RangePartitionBound upper_bound_type)
    : lower_bound_type_(lower_bound_type),
      upper_bound_type_(upper_bound_type),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound) {
}

Status KuduTableCreator::KuduRangePartition::Data::add_hash_partitions(
    const vector<string>& column_names,
    int32_t num_buckets,
    uint32_t seed) {
  if (column_names.empty()) {
    return Status::InvalidArgument(
        "set of columns for hash partitioning must not be empty");
  }
  if (num_buckets <= 1) {
    return Status::InvalidArgument(
        "at least two buckets are required to establish hash partitioning");
  }

  // If many hash dimensions use same columns, the server side will check
  // for such a condition and report an error appropriately. So, to simplify the
  // client-side code, there is no check for such a condition.
  hash_schema_.emplace_back(column_names, num_buckets, seed);

  return Status::OK();
}

} // namespace client
} // namespace kudu
