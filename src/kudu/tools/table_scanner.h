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

#include <stdint.h>

#include <string>
#include <utility>
#include <vector>

#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/atomic.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace kudu {
namespace client {
class KuduClient;
class KuduScanToken;
}  // namespace client
}  // namespace kudu

using kudu::client::KuduClient;
using kudu::client::KuduScanToken;
using std::string;
using std::vector;

namespace kudu {
namespace tools {
class TableScanner {
public:
  TableScanner(client::sp::shared_ptr<KuduClient> client, string table_name):
    total_count_(0),
    client_(std::move(client)),
    table_name_(std::move(table_name)) {
  }

  Status Run();

private:
  void ScannerTask(const vector<KuduScanToken *>& tokens);
  void MonitorTask();

private:
  AtomicInt<uint64_t> total_count_;
  client::sp::shared_ptr<KuduClient> client_;
  std::string table_name_;
  gscoped_ptr<ThreadPool> thread_pool_;
};
} // namespace tools
} // namespace kudu
