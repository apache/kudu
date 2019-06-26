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
#include <functional>
#include <iosfwd>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/atomic.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace kudu {
namespace client {
class KuduSchema;
}  // namespace client
}  // namespace kudu

namespace kudu {
namespace tools {
// This class is not thread-safe.
class TableScanner {
 public:
  TableScanner(client::sp::shared_ptr<kudu::client::KuduClient> client,
               std::string table_name,
               boost::optional<client::sp::shared_ptr<kudu::client::KuduClient>> dst_client
                 = boost::none,
               boost::optional<std::string> dst_table_name = boost::none):
    total_count_(0),
    client_(std::move(client)),
    table_name_(std::move(table_name)),
    dst_client_(std::move(dst_client)),
    dst_table_name_(std::move(dst_table_name)),
    out_(nullptr) {
  }

  // Set output stream of this tool, or disable output if not set.
  // 'out' must remain valid for the lifetime of this class.
  void SetOutput(std::ostream* out);

  // Set read mode, see KuduScanner::SetReadMode().
  void SetReadMode(kudu::client::KuduScanner::ReadMode mode);

  Status StartScan();
  Status StartCopy();

  uint64_t TotalScannedCount() const {
    return total_count_.Load();
  }

 private:
  enum class WorkType {
    kScan,
    kCopy
  };

  Status StartWork(WorkType type);
  Status ScanData(const std::vector<kudu::client::KuduScanToken*>& tokens,
                  const std::function<void(const kudu::client::KuduScanBatch& batch)>& cb);
  void ScanTask(const std::vector<kudu::client::KuduScanToken*>& tokens, Status* thread_status);
  void CopyTask(const std::vector<kudu::client::KuduScanToken*>& tokens, Status* thread_status);
  void MonitorTask();

  Status AddRow(const client::sp::shared_ptr<kudu::client::KuduTable>& table,
                const kudu::client::KuduSchema& table_schema,
                const kudu::client::KuduScanBatch::RowPtr& src_row,
                const client::sp::shared_ptr<kudu::client::KuduSession>& session);

  AtomicInt<uint64_t> total_count_;
  boost::optional<kudu::client::KuduScanner::ReadMode> mode_;
  client::sp::shared_ptr<kudu::client::KuduClient> client_;
  std::string table_name_;
  boost::optional<client::sp::shared_ptr<kudu::client::KuduClient>> dst_client_;
  boost::optional<std::string> dst_table_name_;
  gscoped_ptr<ThreadPool> thread_pool_;

  // Protects output to 'out_' so that rows don't get interleaved.
  Mutex output_lock_;
  std::ostream* out_;
};
} // namespace tools
} // namespace kudu
