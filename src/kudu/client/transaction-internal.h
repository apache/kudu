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

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/common/txn_id.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {

class MonoTime;

namespace client {

// This class contains the implementation the functionality behind
// kudu::client::KuduTransaction.
class KuduTransaction::Data {
 public:
  explicit Data(const sp::shared_ptr<KuduClient>& client);

  Status CreateSession(sp::shared_ptr<KuduSession>* session);

  Status Begin();
  Status Commit(bool wait);
  Status IsCommitComplete(bool* is_complete, Status* completion_status);
  Status Rollback();

  Status Serialize(std::string* serialized_txn) const;

  static Status Deserialize(const sp::shared_ptr<KuduClient>& client,
                            const std::string& serialized_txn,
                            sp::shared_ptr<KuduTransaction>* txn);

  static Status IsCommitCompleteImpl(
      KuduClient* client,
      const MonoTime& deadline,
      const TxnId& txn_id,
      bool* is_complete,
      Status* completion_status);

  static Status WaitForTxnCommitToFinalize(
      KuduClient* client, const MonoTime& deadline, const TxnId& txn_id);

  sp::weak_ptr<KuduClient> weak_client_;
  TxnId txn_id_;
  uint32_t txn_keep_alive_ms_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu
