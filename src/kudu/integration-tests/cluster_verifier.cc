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

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/row_result.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/log_verifier.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

namespace kudu {

using cluster::MiniCluster;
using strings::Substitute;
using tools::Ksck;
using tools::KsckCluster;
using tools::RemoteKsckCluster;

ClusterVerifier::ClusterVerifier(MiniCluster* cluster)
    : cluster_(cluster),
      checksum_options_(tools::ChecksumOptions()),
      operations_timeout_(MonoDelta::FromSeconds(60)) {
  checksum_options_.use_snapshot = false;
}

void ClusterVerifier::SetOperationsTimeout(const MonoDelta& timeout) {
  operations_timeout_ = timeout;
}

void ClusterVerifier::SetVerificationTimeout(const MonoDelta& timeout) {
  checksum_options_.timeout = timeout;
}

void ClusterVerifier::SetScanConcurrency(int concurrency) {
  checksum_options_.scan_concurrency = concurrency;
}

void ClusterVerifier::CheckCluster() {
  MonoTime deadline = MonoTime::Now() + checksum_options_.timeout;

  Status s;
  double sleep_time = 0.1;
  while (MonoTime::Now() < deadline) {
    s = RunKsck();
    if (s.ok()) {
      break;
    }

    LOG(INFO) << "Check not successful yet, sleeping and retrying: " + s.ToString();
    sleep_time *= 1.5;
    if (sleep_time > 1) { sleep_time = 1; }
    SleepFor(MonoDelta::FromSeconds(sleep_time));
  }
  ASSERT_OK(s);

  LogVerifier lv(cluster_);
  // Verify that the committed op indexes match up across the servers.  We have
  // to use "AssertEventually" here because many tests verify clusters while
  // they are still running, and the verification can fail spuriously in the
  // case that
  AssertEventually([&]() {
    ASSERT_OK(lv.VerifyCommittedOpIdsMatch());
  });
}

Status ClusterVerifier::RunKsck() {
  vector<string> hp_strs;
  for (const auto& hp : cluster_->master_rpc_addrs()) {
    hp_strs.emplace_back(hp.ToString());
  }
  std::shared_ptr<KsckCluster> cluster;
  RETURN_NOT_OK(RemoteKsckCluster::Build(hp_strs, &cluster));
  std::shared_ptr<Ksck> ksck(new Ksck(cluster));

  // Some unit tests create or remove replicas of tablets, which
  // we shouldn't consider fatal.
  ksck->set_check_replica_count(false);

  // The first two calls do not depend on each other, though their results are
  // correlated. The subsequent calls depend on CheckClusterRunning().
  RETURN_NOT_OK(ksck->CheckMasterHealth());
  RETURN_NOT_OK(ksck->CheckClusterRunning());
  RETURN_NOT_OK(ksck->FetchTableAndTabletInfo());
  RETURN_NOT_OK(ksck->FetchInfoFromTabletServers());
  RETURN_NOT_OK(ksck->CheckTablesConsistency());
  RETURN_NOT_OK(ksck->ChecksumData(checksum_options_));
  return Status::OK();
}

void ClusterVerifier::CheckRowCount(const std::string& table_name,
                                    ComparisonMode mode,
                                    int expected_row_count) {
  ASSERT_OK(DoCheckRowCount(table_name, mode, expected_row_count));
}

Status ClusterVerifier::DoCheckRowCount(const std::string& table_name,
                                        ComparisonMode mode,
                                        int expected_row_count) {
  client::sp::shared_ptr<client::KuduClient> client;
  RETURN_NOT_OK_PREPEND(cluster_->CreateClient(
      &client::KuduClientBuilder()
          .default_admin_operation_timeout(operations_timeout_)
          .default_rpc_timeout(operations_timeout_),
      &client),
      "Unable to connect to cluster");
  client::sp::shared_ptr<client::KuduTable> table;
  RETURN_NOT_OK_PREPEND(client->OpenTable(table_name, &table),
                        "Unable to open table");
  client::KuduScanner scanner(table.get());
  CHECK_OK(scanner.SetReadMode(client::KuduScanner::READ_AT_SNAPSHOT));
  CHECK_OK(scanner.SetFaultTolerant());
  // Allow a long scan timeout for verification.
  CHECK_OK(scanner.SetTimeoutMillis(60 * 1000));
  CHECK_OK(scanner.SetProjectedColumns({}));
  RETURN_NOT_OK_PREPEND(scanner.Open(), "Unable to open scanner");
  int count = 0;
  vector<client::KuduRowResult> rows;
  while (scanner.HasMoreRows()) {
    RETURN_NOT_OK_PREPEND(scanner.NextBatch(&rows), "Unable to read from scanner");
    count += rows.size();
  }

  if (mode == AT_LEAST && count < expected_row_count) {
    return Status::Corruption(Substitute("row count $0 is not at least expected value $1",
                                         count, expected_row_count));
  } else if (mode == EXACTLY && count != expected_row_count) {
    return Status::Corruption(Substitute("row count $0 is not exactly expected value $1",
                                         count, expected_row_count));
  }
  return Status::OK();
}

void ClusterVerifier::CheckRowCountWithRetries(const std::string& table_name,
                                               ComparisonMode mode,
                                               int expected_row_count,
                                               const MonoDelta& timeout) {
  MonoTime deadline = MonoTime::Now() + timeout;
  Status s;
  while (true) {
    s = DoCheckRowCount(table_name, mode, expected_row_count);
    if (s.ok() || deadline < MonoTime::Now()) break;
    LOG(WARNING) << "CheckRowCount() has not succeeded yet: " << s.ToString()
                 << "... will retry";
    SleepFor(MonoDelta::FromMilliseconds(100));
  }

  ASSERT_OK(s);
}

} // namespace kudu
