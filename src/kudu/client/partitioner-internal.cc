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

#include "kudu/client/partitioner-internal.h"

#include <glog/logging.h>
#include <map>
#include <memory>
#include <string>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/table-internal.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/status.h"

using std::string;
using std::unique_ptr;

namespace kudu {
namespace client {

Status KuduPartitionerBuilder::Data::Build(KuduPartitioner** partitioner) {
  // If any of the builder calls had generated a bad status, then return it here.
  RETURN_NOT_OK(status_);
  unique_ptr<KuduPartitioner::Data> ret_data(new KuduPartitioner::Data());

  auto deadline = MonoTime::Now() + timeout_;
  auto mc = table_->client()->data_->meta_cache_;

  // Insert a sentinel for the beginning of the table, in case they
  // query for any row which falls before the first partition.
  ret_data->partitions_by_start_key_[""] =  -1;
  string next_part_key = "";
  int i = 0;
  while (true) {
    scoped_refptr<internal::RemoteTablet> tablet;
    Synchronizer sync;
    mc->LookupTabletByKeyOrNext(table_.get(), next_part_key, deadline,
                                &tablet, sync.AsStatusCallback());
    Status s = sync.Wait();
    if (s.IsNotFound()) {
      // No more tablets
      break;
    }
    RETURN_NOT_OK(s);
    const auto& start_key = tablet->partition().partition_key_start();
    const auto& end_key = tablet->partition().partition_key_end();
    ret_data->partitions_by_start_key_[start_key] = i++;
    if (end_key.empty()) break;
    ret_data->partitions_by_start_key_[end_key] = -1;
    next_part_key = end_key;
  }
  ret_data->num_partitions_ = i;
  ret_data->table_ = table_;
  *partitioner = new KuduPartitioner(ret_data.release());
  return Status::OK();
}

Status KuduPartitioner::Data::PartitionRow(
    const KuduPartialRow& row, int* partition) {
  tmp_buf_.clear();
  RETURN_NOT_OK(table_->data_->partition_schema_.EncodeKey(row, &tmp_buf_));
  *partition = FindFloorOrDie(partitions_by_start_key_, tmp_buf_);
  return Status::OK();
}

} // namespace client
} // namespace kudu
