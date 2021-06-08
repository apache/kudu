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
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/client/client.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {
namespace client {

class KuduSchema;

struct HashBucketSchema {
  HashBucketSchema(std::vector<std::string> column_names,
                   uint32_t num_buckets,
                   int32_t seed)
      : column_names(std::move(column_names)),
        num_buckets(num_buckets),
        seed(seed) {
  }

  const std::vector<std::string> column_names;
  const uint32_t num_buckets;
  const int32_t seed;
};

class KuduTableCreator::Data {
 public:
  explicit Data(KuduClient* client);
  ~Data() = default;

  KuduClient* client_;

  std::string table_name_;

  const KuduSchema* schema_;

  std::vector<std::unique_ptr<KuduPartialRow>> range_partition_splits_;

  std::vector<std::unique_ptr<KuduRangePartition>> range_partitions_;

  PartitionSchemaPB partition_schema_;

  boost::optional<std::string> owner_;

  boost::optional<std::string> comment_;

  boost::optional<int> num_replicas_;

  boost::optional<std::string> dimension_label_;

  boost::optional<std::map<std::string, std::string>> extra_configs_;

  boost::optional<TableTypePB> table_type_;

  MonoDelta timeout_;

  bool wait_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Data);
};

class KuduTableCreator::KuduRangePartition::Data {
 public:
  Data(KuduPartialRow* lower_bound,
       KuduPartialRow* upper_bound,
       RangePartitionBound lower_bound_type,
       RangePartitionBound upper_bound_type);
  ~Data() = default;

  Status add_hash_partitions(const std::vector<std::string>& column_names,
                             int32_t num_buckets,
                             int32_t seed);

  const RangePartitionBound lower_bound_type_;
  const RangePartitionBound upper_bound_type_;

  std::unique_ptr<KuduPartialRow> lower_bound_;
  std::unique_ptr<KuduPartialRow> upper_bound_;

  std::vector<HashBucketSchema> hash_bucket_schemas_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu
