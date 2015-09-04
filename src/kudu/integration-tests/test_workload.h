// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_INTEGRATION_TESTS_TEST_WORKLOAD_H
#define KUDU_INTEGRATION_TESTS_TEST_WORKLOAD_H

#include <string>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/atomic.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"

namespace kudu {

class ExternalMiniCluster;
class Thread;

// Utility class for generating a workload against a test cluster.
//
// The actual data inserted is random, and thus can't be verified for
// integrity. However, this is still useful in conjunction with ClusterVerifier
// to verify that replicas do not diverge.
class TestWorkload {
 public:
  static const char* const kDefaultTableName;

  explicit TestWorkload(ExternalMiniCluster* cluster);
  ~TestWorkload();

  void set_payload_bytes(int n) {
    payload_bytes_ = n;
  }

  void set_num_write_threads(int n) {
    num_write_threads_ = n;
  }

  void set_write_batch_size(int s) {
    write_batch_size_ = s;
  }

  void set_client_default_rpc_timeout_millis(int t) {
    client_builder_.default_rpc_timeout(MonoDelta::FromMilliseconds(t));
  }

  void set_write_timeout_millis(int t) {
    write_timeout_millis_ = t;
  }

  // Set whether to fail if we see a TimedOut() error inserting a row.
  // By default, this triggers a CHECK failure.
  void set_timeout_allowed(bool allowed) {
    timeout_allowed_ = allowed;
  }

  // Set whether to fail if we see a NotFound() error inserting a row.
  // This sort of error is triggered if the table is deleted while the workload
  // is running.
  // By default, this triggers a CHECK failure.
  void set_not_found_allowed(bool allowed) {
    not_found_allowed_ = allowed;
  }

  void set_num_replicas(int r) {
    num_replicas_ = r;
  }

  // Set the number of tablets for the table created by this workload.
  // The split points are evenly distributed through positive int32s.
  void set_num_tablets(int tablets) {
    CHECK_GT(tablets, 1);
    num_tablets_ = tablets;
  }

  void set_table_name(const std::string& table_name) {
    table_name_ = table_name;
  }

  const std::string& table_name() const {
    return table_name_;
  }

  void set_pathological_one_row_enabled(bool enabled) {
    pathological_one_row_enabled_ = enabled;
  }

  // Sets up the internal client and creates the table which will be used for
  // writing, if it doesn't already exist.
  void Setup();

  // Start the write workload.
  void Start();

  // Stop the writers and wait for them to exit.
  void StopAndJoin();

  // Return the number of rows inserted so far. This may be called either
  // during or after the write workload.
  int64_t rows_inserted() const {
    return rows_inserted_.Load();
  }

  // Return the number of batches in which we have successfully inserted at
  // least one row.
  int64_t batches_completed() const {
    return batches_completed_.Load();
  }

 private:
  void WriteThread();

  ExternalMiniCluster* cluster_;
  client::KuduClientBuilder client_builder_;
  std::tr1::shared_ptr<client::KuduClient> client_;

  int payload_bytes_;
  int num_write_threads_;
  int write_batch_size_;
  int write_timeout_millis_;
  bool timeout_allowed_;
  bool not_found_allowed_;
  bool pathological_one_row_enabled_;

  int num_replicas_;
  int num_tablets_;
  std::string table_name_;

  CountDownLatch start_latch_;
  AtomicBool should_run_;
  AtomicInt<int64_t> rows_inserted_;
  AtomicInt<int64_t> batches_completed_;

  std::vector<scoped_refptr<Thread> > threads_;

  DISALLOW_COPY_AND_ASSIGN(TestWorkload);
};

} // namespace kudu
#endif /* KUDU_INTEGRATION_TESTS_TEST_WORKLOAD_H */
