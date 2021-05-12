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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::itest::TabletServerMap;
using kudu::itest::TServerDetails;
using strings::Split;
using strings::Substitute;
using std::string;
using std::vector;

namespace kudu {
namespace tools {

class KuduTxnsCliTest : public ExternalMiniClusterITestBase {
  void SetUp() override {
    NO_FATALS(ExternalMiniClusterITestBase::SetUp());
    // Some tests will depend on flushing MRSs quickly, so ensure flushes
    // happen quickly.
    NO_FATALS(StartCluster({ "--flush_threshold_mb=1", "--flush_threshold_secs=1" },
                            {"--txn_manager_enabled=true"}));
  }
};

TEST_F(KuduTxnsCliTest, TestBasicTxnsList) {
  // Commit one transaction.
  {
    TestWorkload w(cluster_.get());
    w.set_begin_txn();
    w.set_commit_txn();
    w.Setup();
    w.Start();
    while (w.rows_inserted() < 10) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    w.StopAndJoin();
  }
  // Abort one transaction.
  {
    TestWorkload w(cluster_.get());
    w.set_begin_txn();
    w.set_rollback_txn();
    w.Setup();
    w.Start();
    while (w.rows_inserted() < 10) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    w.StopAndJoin();
  }
  // Leave one transaction open.
  TestWorkload w(cluster_.get());
  w.set_begin_txn();
  w.Setup();
  w.Start();
  string out;
  ASSERT_OK(RunKuduTool({ "txn", "list", cluster_->master_rpc_addrs()[0].ToString(),
                          "--included_states=*" }, &out));
  ASSERT_STR_MATCHES(out, R"( txn_id \| *user *\|   state   \| *commit_datetime
--------\+-*\+-----------\+-*
 0      \| *[a-z]* *\| COMMITTED \| .* GMT
 1      \| *[a-z]* *\| ABORTED   \| <none>
 2      \| *[a-z]* *\| OPEN      \| <none>)");
  ASSERT_OK(RunKuduTool({ "txn", "list", cluster_->master_rpc_addrs()[0].ToString(),
                          "--included_states=aborted,open" }, &out));
  ASSERT_STR_MATCHES(out, R"( txn_id \| *user *\|  state  \| *commit_datetime
--------\+-*\+---------\+-*
 1      \| *[a-z]* *\| ABORTED \| <none>
 2      \| *[a-z]* *\| OPEN    \| <none>)");
  ASSERT_OK(RunKuduTool({ "txn", "list", cluster_->master_rpc_addrs()[0].ToString(),
                          "--included_states=open,committed" }, &out));
  ASSERT_STR_MATCHES(out, R"( txn_id \| *user *\|   state   \| *commit_datetime
--------\+-*\+-----------\+-*
 0      \| *[a-z]* *\| COMMITTED \| .* GMT
 2      \| *[a-z]* *\| OPEN      \| <none>)");
  ASSERT_OK(RunKuduTool({ "txn", "list", cluster_->master_rpc_addrs()[0].ToString() }, &out));
  ASSERT_STR_MATCHES(out, R"( txn_id \| *user *\| state \| *commit_datetime
--------\+-*\+-------\+-*
 2      \| *[a-z]* *\| OPEN  \| <none>)");
}

TEST_F(KuduTxnsCliTest, TestTxnsListMinMaxFilter) {
  // Commit one transaction.
  for (int i = 0; i < 10; i++) {
    TestWorkload w(cluster_.get());
    w.set_begin_txn();
    w.set_commit_txn();
    w.Setup();
    w.Start();
    while (w.rows_inserted() < 10) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    w.StopAndJoin();
  }
  string out;
  ASSERT_OK(RunKuduTool({ "txn", "list", cluster_->master_rpc_addrs()[0].ToString(),
                          "--min_txn_id=7", "--included_states=*" }, &out));
  ASSERT_STR_MATCHES(out, R"( txn_id \| *user *\|   state   \| *commit_datetime
--------\+-*\+-----------\+-*
 7      \| *[a-z]* *\| COMMITTED \| .* GMT
 8      \| *[a-z]* *\| COMMITTED \| .* GMT
 9      \| *[a-z]* *\| COMMITTED \| .* GMT)");
  ASSERT_OK(RunKuduTool({ "txn", "list", cluster_->master_rpc_addrs()[0].ToString(),
                          "--max_txn_id=2", "--included_states=*" }, &out));
  ASSERT_STR_MATCHES(out, R"( txn_id \| *user *\|   state   \| *commit_datetime
--------\+-*\+-----------\+-*
 0      \| *[a-z]* *\| COMMITTED \| .* GMT
 1      \| *[a-z]* *\| COMMITTED \| .* GMT
 2      \| *[a-z]* *\| COMMITTED \| .* GMT)");
  ASSERT_OK(RunKuduTool({ "txn", "list", cluster_->master_rpc_addrs()[0].ToString(),
                          "--min_txn_id=5", "--max_txn_id=7", "--included_states=*" }, &out));
  ASSERT_STR_MATCHES(out, R"( txn_id \| *user *\|   state   \| *commit_datetime
--------\+-*\+-----------\+-*
 5      \| *[a-z]* *\| COMMITTED \| .* GMT
 6      \| *[a-z]* *\| COMMITTED \| .* GMT
 7      \| *[a-z]* *\| COMMITTED \| .* GMT)");
  ASSERT_OK(RunKuduTool({ "txn", "list", cluster_->master_rpc_addrs()[0].ToString(),
                          "--min_txn_id=10", "--max_txn_id=0", "--included_states=*" }, &out));
  ASSERT_EQ(
      " txn_id | user | state | commit_datetime\n"
      "--------+------+-------+-----------------\n",
      out);
}

TEST_F(KuduTxnsCliTest, TestTxnsListHybridTimestamps) {
  {
    TestWorkload w(cluster_.get());
    w.set_begin_txn();
    w.set_commit_txn();
    w.Setup();
    w.Start();
    while (w.rows_inserted() < 10) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    w.StopAndJoin();
  }
  string out;
  ASSERT_OK(RunKuduTool({ "txn", "list", cluster_->master_rpc_addrs()[0].ToString(),
                          "--columns=txn_id,user,state,commit_datetime,commit_hybridtime",
                          "--included_states=*" }, &out));
  ASSERT_STR_MATCHES(out,
      R"( txn_id \| *user *\|   state   \| *commit_datetime *| *commit_hybridtime
--------\+-*\+-----------\+-------------------------------
 0      \| *[a-z]* *\| COMMITTED \| .* GMT \| P: .* usec, L: .*)");
}

TEST_F(KuduTxnsCliTest, TestBasicShowTxn) {
  TestWorkload w(cluster_.get());
  w.set_begin_txn();
  w.set_commit_txn();
  w.Setup();
  w.Start();
  while (w.rows_inserted() < 10) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  w.StopAndJoin();

  // Check the output of the tool with no arguments.
  string out;
  ASSERT_OK(RunKuduTool({ "txn", "show", cluster_->master_rpc_addrs()[0].ToString(), "0" }, &out));
  ASSERT_STR_MATCHES(out, R"( txn_id \| *user *\|   state   \| *commit_datetime
--------\+-*\+-----------\+-*
 0      \| *[a-z]* *\| COMMITTED \| .* GMT

            tablet_id             \| *begin_commit_datetime *\| *commit_datetime
----------------------------------\+-*\+-*
 [a-z0-9]* \| .*GMT \| .*GMT)");

  // Check the output specifying none of the transaction status columns. We
  // shouldn't display the status table at all.
  ASSERT_OK(RunKuduTool({ "txn", "show", cluster_->master_rpc_addrs()[0].ToString(), "0",
                          "--columns=participant_tablet_id,participant_begin_commit_datetime,"
                                    "participant_commit_datetime" }, &out));
  ASSERT_STR_NOT_CONTAINS(out, "txn_id");
  ASSERT_STR_MATCHES(out,
      R"(            tablet_id             \| *begin_commit_datetime *\| *commit_datetime
----------------------------------\+-*\+-*
 [a-z0-9]* \| .*GMT \| .*GMT)");

  // Check the output specifying none of the participant columns. We shouldn't
  // display the participant table at all.
  ASSERT_OK(RunKuduTool({ "txn", "show", cluster_->master_rpc_addrs()[0].ToString(), "0",
                          "--columns=txn_id,user,state,commit_datetime" }, &out));
  ASSERT_STR_NOT_CONTAINS(out, "tablet_id");
  ASSERT_STR_MATCHES(out, R"( txn_id \| *user *\|   state   \| *commit_datetime
--------\+-*\+-----------\+-*
 0      \| *[a-z]* *\| COMMITTED \| .* GMT)");
}

TEST_F(KuduTxnsCliTest, TestShowTxnHybridTimestamps) {
  TestWorkload w(cluster_.get());
  w.set_begin_txn();
  w.set_commit_txn();
  w.Setup();
  w.Start();
  while (w.rows_inserted() < 10) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  w.StopAndJoin();
  string out;
  // Check that we can specify date time in the transaction status display and
  // hybrid time in the participant status display.
  ASSERT_OK(RunKuduTool({ "txn", "show", cluster_->master_rpc_addrs()[0].ToString(), "0",
                          "--columns=txn_id,user,state,commit_datetime,participant_tablet_id,"
                                    "participant_begin_commit_hybridtime,"
                                    "participant_commit_hybridtime"}, &out));
  ASSERT_STR_MATCHES(out, R"( txn_id \| *user *\|   state   \| *commit_datetime
--------\+-*\+-----------\+-*
 0      \| *[a-z]* *\| COMMITTED \| .* GMT

            tablet_id             \| *begin_commit_hybridtime *\| *commit_hybridtime
----------------------------------\+-*\+-*
 [a-z0-9]* \| P: .* usec, L: .* \| P: .* usec, L: .*)");

  // Check that we can specify hybrid time in the transaction status display and
  // date time in the participant status display.
  ASSERT_OK(RunKuduTool({ "txn", "show", cluster_->master_rpc_addrs()[0].ToString(), "0",
                          "--columns=txn_id,user,state,commit_hybridtime,participant_tablet_id,"
                                    "participant_begin_commit_datetime,"
                                    "participant_commit_datetime"}, &out));
  ASSERT_STR_MATCHES(out, R"( txn_id \| *user *\|   state   \| *commit_hybridtime
--------\+-*\+-----------\+-*
 0      \| *[a-z]* *\| COMMITTED \| P: .* usec, L: .*

            tablet_id             \| *begin_commit_datetime *\| *commit_datetime
----------------------------------\+-*\+-*
 [a-z0-9]* \| .* GMT \| .* GMT)");
}

TEST_F(KuduTxnsCliTest, TestShowTxnFlushedMRS) {
  {
    TestWorkload w(cluster_.get());
    w.set_begin_txn();
    w.set_commit_txn();
    w.Setup();
    w.Start();
    while (w.rows_inserted() < 10) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    w.StopAndJoin();
  }
  // Our cluster is set up to flush MRSs quickly, so we should eventually
  // report that we've flushed the committed MRS.
  ASSERT_EVENTUALLY([&] {
    string out;
    ASSERT_OK(RunKuduTool({ "txn", "show", cluster_->master_rpc_addrs()[0].ToString(), "0",
                            "--columns=participant_tablet_id,participant_flushed_committed_mrs" },
                            &out));
    ASSERT_STR_NOT_CONTAINS(out, "txn_id");
    ASSERT_STR_MATCHES(out,
      R"(            tablet_id             \| *flushed_committed_mrs
----------------------------------\+-*
 [a-z0-9]* \| true)");
  });

  // In an aborted workload, the report should always report we didn't flush
  // the transactional MRS.
  TestWorkload w(cluster_.get());
  w.set_begin_txn();
  w.set_rollback_txn();
  w.Setup();
  w.Start();
  while (w.rows_inserted() < 10) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  w.StopAndJoin();
  string out;
  ASSERT_OK(RunKuduTool({ "txn", "show", cluster_->master_rpc_addrs()[0].ToString(), "1",
                          "--columns=participant_tablet_id,participant_flushed_committed_mrs" },
                          &out));
  ASSERT_STR_NOT_CONTAINS(out, "txn_id");
  ASSERT_STR_MATCHES(out,
      R"(            tablet_id             \| *flushed_committed_mrs
----------------------------------\+-*
 [a-z0-9]* \| false)");
}

} // namespace tools
} // namespace kudu
