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

#include <map>
#include <string>
#include <utility>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

class KuduPartialRow;

namespace client {

class KuduPartitionerBuilder::Data {
 public:
  explicit Data(sp::shared_ptr<KuduTable> table)
      : table_(std::move(table)) {
    if (table_) {
      timeout_ = table_->client()->default_admin_operation_timeout();
    } else {
      status_ = Status::InvalidArgument("null table");
    }
  }

  ~Data() {}

  void SetBuildTimeout(MonoDelta timeout) {
    if (!timeout.Initialized()) {
      status_ = Status::InvalidArgument("uninitialized timeout");
      return;
    }
    timeout_ = timeout;
  }

  Status Build(KuduPartitioner** partitioner);

 private:
  const sp::shared_ptr<KuduTable> table_;

  // A deferred Status result, set to non-OK if one of the builder
  // parameters is set to an invalid value.
  Status status_;
  MonoDelta timeout_;
};

class KuduPartitioner::Data {
 public:
  Status PartitionRow(const KuduPartialRow& row, int* partition);

  sp::shared_ptr<KuduTable> table_;
  std::map<std::string, int> partitions_by_start_key_;
  int num_partitions_ = 0;
  std::string tmp_buf_;
};


} // namespace client
} // namespace kudu
