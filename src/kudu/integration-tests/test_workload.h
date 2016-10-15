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
#ifndef KUDU_INTEGRATION_TESTS_TEST_WORKLOAD_H
#define KUDU_INTEGRATION_TESTS_TEST_WORKLOAD_H

#include <cstdint>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/atomic.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"

namespace kudu {

class MiniCluster;
class Status;

// Utility class for generating a workload against a test cluster.
//
// The actual data inserted is random, and thus can't be verified for
// integrity. However, this is still useful in conjunction with ClusterVerifier
// to verify that replicas do not diverge.
//
// The read workload essentially tests read-your-writes. It constantly
// issues snapshot scans in the present and asserts that we see at least as
// many rows as we have written, independently of which replica we choose
// to scan.
class TestWorkload {
 public:
  static const char* const kDefaultTableName;

  explicit TestWorkload(MiniCluster* cluster);
  ~TestWorkload();

  void set_payload_bytes(int n) {
    payload_bytes_ = n;
  }

  void set_num_write_threads(int n) {
    num_write_threads_ = n;
  }

  void set_num_read_threads(int n) {
    num_read_threads_ = n;
  }

  void set_write_batch_size(int s) {
    write_batch_size_ = s;
  }

  void set_client_default_rpc_timeout_millis(int t) {
    client_builder_.default_rpc_timeout(MonoDelta::FromMilliseconds(t));
  }

  void set_client_default_admin_operation_timeout_millis(int t) {
    client_builder_.default_admin_operation_timeout(MonoDelta::FromMilliseconds(t));
  }

  void set_read_timeout_millis(int t) {
    read_timeout_millis_ = t;
  }

  void set_write_timeout_millis(int t) {
    write_timeout_millis_ = t;
  }

  // Set whether to fail if we see a TimedOut() error inserting a row.
  // By default, this triggers a CHECK failure.
  void set_timeout_allowed(bool allowed) {
    timeout_allowed_ = allowed;
  }

  void set_network_error_allowed(bool allowed) {
    network_error_allowed_ = allowed;
  }

  // Set whether to fail if we see a NotFound() error inserting a row.
  // This sort of error is triggered if the table is deleted while the workload
  // is running.
  // By default, this triggers a CHECK failure.
  void set_not_found_allowed(bool allowed) {
    not_found_allowed_ = allowed;
  }

  // Whether per-row errors with Status::AlreadyPresent() are allowed.
  // By default this triggers a check failure.
  void set_already_present_allowed(bool allowed) {
    already_present_allowed_ = allowed;
  }

  // Override the default "simple" schema.
  void set_schema(const client::KuduSchema& schema);

  void set_num_replicas(int r) {
    num_replicas_ = r;
  }

  // Set the number of tablets for the table created by this workload.
  // The split points are evenly distributed through positive int32s.
  void set_num_tablets(int tablets) {
    CHECK_GE(tablets, 1);
    num_tablets_ = tablets;
  }

  void set_table_name(const std::string& table_name) {
    table_name_ = table_name;
  }

  const std::string& table_name() const {
    return table_name_;
  }

  static const int kNumRowsForDuplicateKeyWorkload = 20;

  enum WritePattern {
    // The default: insert random row keys. This may cause an occasional
    // duplicate, but with 32-bit keys, they won't be frequent.
    INSERT_RANDOM_ROWS,

    // All threads generate updates against a single row.
    UPDATE_ONE_ROW,

    // Insert rows in random order, but restricted to only
    // kNumRowsForDuplicateKeyWorkload unique keys. This ensures that,
    // after a very short initial warm-up period, all inserts fail with
    // duplicate keys.
    INSERT_WITH_MANY_DUP_KEYS,

    // Insert sequential rows.
    // This causes flushes but no compactions.
    INSERT_SEQUENTIAL_ROWS
  };

  void set_write_pattern(WritePattern pattern) {
    write_pattern_ = pattern;
    switch (pattern) {
      case INSERT_WITH_MANY_DUP_KEYS:
        set_already_present_allowed(true);
        break;
      case INSERT_RANDOM_ROWS:
      case UPDATE_ONE_ROW:
      case INSERT_SEQUENTIAL_ROWS:
        set_already_present_allowed(false);
        break;
      default: LOG(FATAL) << "Unsupported WritePattern.";
    }
  }

  client::sp::shared_ptr<client::KuduClient> CreateClient();

  // Sets up the internal client and creates the table which will be used for
  // writing, if it doesn't already exist.
  void Setup();

  // Start the write workload.
  void Start();

  // Stop the writers and wait for them to exit.
  void StopAndJoin();

  // Delete created table, etc.
  Status Cleanup();

  // Return the number of rows inserted so far. This may be called either
  // during or after the write workload.
  int64_t rows_inserted() const {
    return rows_inserted_.Load();
  }

  // Return the number of batches in which we have successfully inserted at
  // least one row.
  // NOTE: it is not safe to assume that this is exactly equal to the number
  // of log operations generated on the TS side. The client may split a single
  // Flush() call into multiple batches.
  int64_t batches_completed() const {
    return batches_completed_.Load();
  }

  client::sp::shared_ptr<client::KuduClient> client() const { return client_; }

 private:
  void OpenTable(client::sp::shared_ptr<client::KuduTable>* table);
  void WriteThread();
  void ReadThread();

  MiniCluster* cluster_;
  client::KuduClientBuilder client_builder_;
  client::sp::shared_ptr<client::KuduClient> client_;
  ThreadSafeRandom rng_;

  boost::optional<int> payload_bytes_;
  int num_write_threads_;
  int num_read_threads_;
  int read_timeout_millis_;
  int write_batch_size_;
  int write_timeout_millis_;
  bool timeout_allowed_;
  bool not_found_allowed_;
  bool already_present_allowed_;
  bool network_error_allowed_;
  WritePattern write_pattern_;
  client::KuduSchema schema_;

  int num_replicas_;
  int num_tablets_;
  std::string table_name_;

  CountDownLatch start_latch_;
  AtomicBool should_run_;
  AtomicInt<int64_t> rows_inserted_;
  AtomicInt<int64_t> batches_completed_;
  AtomicInt<int32_t> sequential_key_gen_;

  std::vector<std::thread> threads_;

  DISALLOW_COPY_AND_ASSIGN(TestWorkload);
};

} // namespace kudu
#endif /* KUDU_INTEGRATION_TESTS_TEST_WORKLOAD_H */
