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
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {
namespace client {

class KuduSchema;

struct HashDimension {
  HashDimension(std::vector<std::string> column_names,
                int32_t num_buckets,
                uint32_t seed)
      : column_names(std::move(column_names)),
        num_buckets(num_buckets),
        seed(seed) {
    DCHECK_GE(num_buckets, 2);
  }

  const std::vector<std::string> column_names;
  const int32_t num_buckets;
  const uint32_t seed;
};
typedef std::vector<HashDimension> HashSchema;

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

  std::optional<std::string> owner_;

  std::optional<std::string> comment_;

  std::optional<int> num_replicas_;

  std::optional<std::string> dimension_label_;

  std::optional<std::map<std::string, std::string>> extra_configs_;

  std::optional<TableTypePB> table_type_;

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
                             uint32_t seed);

  const RangePartitionBound lower_bound_type_;
  const RangePartitionBound upper_bound_type_;

  std::unique_ptr<KuduPartialRow> lower_bound_;
  std::unique_ptr<KuduPartialRow> upper_bound_;

  HashSchema hash_schema_;
  bool is_table_wide_hash_schema_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu
