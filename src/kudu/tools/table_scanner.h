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

#include <atomic>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace kudu {
namespace tools {

// This class is not thread-safe.
class TableScanner {
 public:
  TableScanner(client::sp::shared_ptr<client::KuduClient> client,
               std::string table_name,
               std::optional<client::sp::shared_ptr<client::KuduClient>> dst_client =
                   std::nullopt,
               std::optional<std::string> dst_table_name = std::nullopt);

  // Set output stream of this tool, or disable output if not set.
  // 'out' must remain valid for the lifetime of this class.
  void SetOutput(std::ostream* out);

  // Set read mode, see KuduScanner::SetReadMode().
  void SetReadMode(client::KuduScanner::ReadMode mode);

  // Set replica selection for scan operations.
  Status SetReplicaSelection(const std::string& selection);

  // Set the size for scan result batch size, in bytes. A negative value has
  // the semantics of relying on the server-side default: see the
  // --scanner_default_batch_size_bytes flag.
  void SetScanBatchSize(int32_t scan_batch_size);

  Status StartScan();
  Status StartCopy();

  uint64_t TotalScannedCount() const {
    return total_count_;
  }

 private:
  enum class WorkType {
    kScan,
    kCopy
  };

  static Status AddRow(client::KuduSession* session,
                       client::KuduTable* table,
                       const client::KuduScanBatch::RowPtr& src_row,
                       client::KuduWriteOperation::Type write_op_type);

  // Convert replica selection from string into the KuduClient::ReplicaSelection
  // enumerator.
  static Status ParseReplicaSelection(
      const std::string& selection_str,
      client::KuduClient::ReplicaSelection* selection);

  Status StartWork(WorkType work_type);
  Status ScanData(const std::vector<client::KuduScanToken*>& tokens,
                  const std::function<Status(const client::KuduScanBatch& batch)>& cb);
  void ScanTask(const std::vector<client::KuduScanToken*>& tokens,
                Status* thread_status);
  void CopyTask(const std::vector<client::KuduScanToken*>& tokens,
                Status* thread_status);

  std::atomic<uint64_t> total_count_;
  std::optional<client::KuduScanner::ReadMode> mode_;
  client::sp::shared_ptr<client::KuduClient> client_;
  std::string table_name_;
  client::KuduClient::ReplicaSelection replica_selection_;
  std::optional<client::sp::shared_ptr<client::KuduClient>> dst_client_;
  std::optional<std::string> dst_table_name_;
  int32_t scan_batch_size_;
  std::unique_ptr<ThreadPool> thread_pool_;

  // Protects output to 'out_' so that rows don't get interleaved.
  Mutex output_lock_;
  std::ostream* out_;
};

} // namespace tools
} // namespace kudu
