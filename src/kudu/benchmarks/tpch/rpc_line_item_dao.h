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
#include <memory>
#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/row_result.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/semaphore.h"

namespace boost {
template <typename Signature>
class function;
} // namespace boost

namespace kudu {

namespace client {
class KuduPredicate;
}

class KuduPartialRow;
class Slice;

class RpcLineItemDAO {
 public:
  class Scanner;
  enum PartitionStrategy {
    RANGE,
    HASH
  };

  RpcLineItemDAO(std::string master_address,
                 std::string table_name,
                 int batch_op_num_max,
                 int timeout_ms,
                 PartitionStrategy partition_strategy,
                 int num_buckets,
                 std::vector<const KuduPartialRow*> tablet_splits = {});
  ~RpcLineItemDAO();
  void Init();
  void WriteLine(const boost::function<void(KuduPartialRow*)>& f);
  void MutateLine(const boost::function<void(KuduPartialRow*)>& f);
  void FinishWriting();

  // Deletes previous scanner if one is open.
  // Projects only those column names listed in 'columns'.
  void OpenScanner(const std::vector<std::string>& columns,
                   std::unique_ptr<Scanner>* scanner);
  // Calls OpenScanner with the tpch1 query parameters.
  void OpenTpch1Scanner(std::unique_ptr<Scanner>* scanner);

  // Opens a scanner with the TPCH Q1 projection and filter, plus range filter to only
  // select rows in the given order key range.
  void OpenTpch1ScannerForOrderKeyRange(int64_t min_orderkey, int64_t max_orderkey,
                                        std::unique_ptr<Scanner>* scanner);
  bool IsTableEmpty();

  // TODO(unknown): this wrapper class is of limited utility now that we only
  // have a single "DAO" implementation -- we could just return the KuduScanner
  // to users directly.
  class Scanner {
   public:
    ~Scanner() {}

    // Return true if there are more rows left in the scanner.
    bool HasMore();

    // Return the next batch of rows into '*rows'. Any existing data is cleared.
    void GetNext(std::vector<client::KuduRowResult> *rows);

   private:
    friend class RpcLineItemDAO;
    Scanner() {}

    std::unique_ptr<client::KuduScanner> scanner_;
  };

 private:
  static const Slice kScanUpperBound;

  void OpenScannerImpl(const std::vector<std::string>& columns,
                       const std::vector<client::KuduPredicate*>& preds,
                       std::unique_ptr<Scanner>* scanner);
  void HandleLine();

  const std::string master_address_;
  const std::string table_name_;
  const MonoDelta timeout_;
  const int batch_op_num_max_;

  const PartitionStrategy partition_strategy_;
  const int num_buckets_;

  const std::vector<const KuduPartialRow*> tablet_splits_;
  int batch_op_num_;
  simple_spinlock lock_;
  client::sp::shared_ptr<client::KuduClient> client_;
  client::sp::shared_ptr<client::KuduSession> session_;
  client::sp::shared_ptr<client::KuduTable> client_table_;

  // Semaphore which restricts us to one batch at a time.
  Semaphore semaphore_;
};

} //namespace kudu
