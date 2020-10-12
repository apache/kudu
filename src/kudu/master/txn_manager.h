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
#include <memory>
#include <string>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {
class MonoTime;

namespace master {
class Master;
}

namespace transactions {

class TxnStatusEntryPB;
class TxnSystemClient;

// This class encapsulates the logic used by the TxnManagerService while serving
// RPC requests (see txn_manager.proto for the protobuf interface). The most
// essential piece of the logic implemented by this class is the assignment of
// an identifier for a new transaction. All other methods simply do proxying
// of corresponding requests to the underlying instance of TxnSystemClient
// aggregated by this class.
class TxnManager final {
 public:
  explicit TxnManager(master::Master* server);
  ~TxnManager();

  // Use next transaction identifier and call BeginTransaction() via
  // txn_sys_client_, adjusting the highest seen txn_id.
  Status BeginTransaction(const std::string& username,
                          const MonoTime& deadline,
                          int64_t* txn_id,
                          int32_t* keep_alive_interval_ms);

  // Initiate the commit phase for the transaction. The control is returned
  // right after initiating the commit phase: the caller can check for the
  // completion of the commit phase using the GetTransactionState() RPC.
  // So, in some sense this is an asynchronous method.
  Status CommitTransaction(int64_t txn_id,
                           const std::string& username,
                           const MonoTime& deadline);

  // The three method below proxy calls to the underlying txn_sys_client_.
  Status AbortTransaction(int64_t txn_id,
                          const std::string& username,
                          const MonoTime& deadline);

  Status GetTransactionState(int64_t txn_id,
                             const std::string& username,
                             const MonoTime& deadline,
                             TxnStatusEntryPB* txn_status);

  Status KeepTransactionAlive(int64_t txn_id,
                              const std::string& username,
                              const MonoTime& deadline);

 private:
  friend class master::Master;
  FRIEND_TEST(TxnManagerTest, LazyInitialization);
  FRIEND_TEST(TxnManagerTest, NonlazyInitialization);
  FRIEND_TEST(TxnManagerTest, LazyInitializationConcurrentCalls);

  // Initialize the internals: create transaction status table (if not exists),
  // create and initialize txn_sys_client_, etc. This method should not be
  // called concurrently or multiple times.
  Status Init();

  // Return Status::OK() if TxnManager is initialized, otherwise return
  // Status::ServiceUnavailable() unless TxnManager is configured to initialize
  // lazily. If the latter, schedule the initialization via master's thread
  // pool and wait for that to be completed no later than prescribed
  // by the 'deadline' parameter. A non-initialized instance of MonoTime
  // passed as the 'deadline' parameter has a special meaning: wait indefinitely
  // for the initialization of the TxnManager.
  Status CheckInitialized(const MonoTime& deadline);

  // Whether or not this instance is lazily initialized.
  const bool is_lazily_initialized_;

  // The span of a range partition to use when adding new ranges to the
  // transaction status table.
  const int64_t txn_status_table_range_span_;

  // The pointer to the top-level Master object. From the lifecycle perspective,
  // Master is assumed to be alive during the whole lifecycle of TxnManager
  // since the Master is the component that hosts TxnManager (this is similar
  // to the relations between the CatalogManager and the Master).
  master::Master* server_;

  // TxnSystemClient instance to communicate with TxnStatusManager.
  std::unique_ptr<TxnSystemClient> txn_sys_client_;

  // Whether it's necessary to schedule the initialization of this TxnManager
  // instance (this is relavant only in case of lazily initialized instance).
  std::atomic<bool> need_init_;

  // Whether this object is initialized. In case of lazily initialized instance,
  // the approach with atomics performs better compared with the approach using
  // the 'standard' synchronization primitives.
  std::atomic<bool> initialized_;

  // The next_txn_id_ is used as a hint for next transaction identifier to try
  // when assigning an identifier to a new transaction.
  std::atomic<int64_t> next_txn_id_;

  DISALLOW_COPY_AND_ASSIGN(TxnManager);
};

} // namespace transactions
} // namespace kudu
