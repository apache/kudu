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

#include "kudu/integration-tests/raft_consensus-itest-base.h"

#include <algorithm>
#include <cstdint>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/integration-tests/ts_itest-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DEFINE_int32(num_client_threads, 8,
             "Number of client threads to launch");
DEFINE_int64(client_inserts_per_thread, 50,
             "Number of rows inserted by each client thread");
DECLARE_int32(consensus_rpc_timeout_ms);
DEFINE_int64(client_num_batches_per_thread, 5,
             "In how many batches to group the rows, for each client");

METRIC_DECLARE_entity(tablet);
METRIC_DECLARE_counter(transaction_memory_pressure_rejections);
METRIC_DECLARE_gauge_int64(raft_term);

using kudu::client::KuduInsert;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalTabletServer;
using kudu::consensus::OpId;
using kudu::itest::GetInt64Metric;
using kudu::itest::TServerDetails;
using kudu::pb_util::SecureDebugString;
using kudu::rpc::RpcController;
using std::string;
using std::vector;

namespace kudu {
namespace tserver {

static const int kConsensusRpcTimeoutForTests = 50;

RaftConsensusITestBase::RaftConsensusITestBase()
      : inserters_(FLAGS_num_client_threads) {
}

void RaftConsensusITestBase::SetUp() {
  TabletServerIntegrationTestBase::SetUp();
  FLAGS_consensus_rpc_timeout_ms = kConsensusRpcTimeoutForTests;
}

void RaftConsensusITestBase::ScanReplica(TabletServerServiceProxy* replica_proxy,
                                         vector<string>* results) {
  ScanRequestPB req;
  ScanResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10)); // Squelch warnings.

  NewScanRequestPB* scan = req.mutable_new_scan_request();
  scan->set_tablet_id(tablet_id_);
  ASSERT_OK(SchemaToColumnPBs(schema_, scan->mutable_projected_columns()));

  // Send the call
  {
    req.set_batch_size_bytes(0);
    SCOPED_TRACE(SecureDebugString(req));
    ASSERT_OK(replica_proxy->Scan(req, &resp, &rpc));
    SCOPED_TRACE(SecureDebugString(resp));
    if (resp.has_error()) {
      ASSERT_OK(StatusFromPB(resp.error().status()));
    }
  }

  if (!resp.has_more_results()) {
    return;
  }

  // Drain all the rows from the scanner.
  NO_FATALS(DrainScannerToStrings(resp.scanner_id(),
                                  schema_,
                                  results,
                                  replica_proxy));

  std::sort(results->begin(), results->end());
}

void RaftConsensusITestBase::InsertTestRowsRemoteThread(
    uint64_t first_row,
    uint64_t count,
    uint64_t num_batches,
    const vector<CountDownLatch*>& latches) {
  shared_ptr<KuduTable> table;
  CHECK_OK(client_->OpenTable(kTableId, &table));

  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(60000);
  CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  for (int i = 0; i < num_batches; i++) {
    uint64_t first_row_in_batch = first_row + (i * count / num_batches);
    uint64_t last_row_in_batch = first_row_in_batch + count / num_batches;

    for (int j = first_row_in_batch; j < last_row_in_batch; j++) {
      gscoped_ptr<KuduInsert> insert(table->NewInsert());
      KuduPartialRow* row = insert->mutable_row();
      CHECK_OK(row->SetInt32(0, j));
      CHECK_OK(row->SetInt32(1, j * 2));
      CHECK_OK(row->SetStringCopy(2, Slice(StringPrintf("hello %d", j))));
      CHECK_OK(session->Apply(insert.release()));
    }

    FlushSessionOrDie(session);

    int inserted = last_row_in_batch - first_row_in_batch;
    for (CountDownLatch* latch : latches) {
      latch->CountDown(inserted);
    }
  }

  inserters_.CountDown();
}

void RaftConsensusITestBase::AddFlagsForLogRolls(vector<string>* extra_tserver_flags) {
  // We configure a small log segment size so that we roll frequently,
  // configure a small cache size so that we evict data from the cache, and
  // retain as few segments as possible. We also turn off async segment
  // allocation -- this ensures that we roll many segments of logs (with async
  // allocation, it's possible that the preallocation is slow and we wouldn't
  // roll deterministically).
  //
  // Additionally, we disable log compression, since these tests write a lot of
  // repetitive data to cause the rolls, and compression would make it all tiny.
  extra_tserver_flags->push_back("--log_compression_codec=none");
  extra_tserver_flags->push_back("--log_cache_size_limit_mb=1");
  extra_tserver_flags->push_back("--log_segment_size_mb=1");
  extra_tserver_flags->push_back("--log_async_preallocate_segments=false");
  extra_tserver_flags->push_back("--log_min_segments_to_retain=1");
  extra_tserver_flags->push_back("--log_max_segments_to_retain=3");
  extra_tserver_flags->push_back("--maintenance_manager_polling_interval_ms=100");
  extra_tserver_flags->push_back("--log_target_replay_size_mb=1");
  // We write 128KB cells in CauseFollowerToFallBehindLogGC(): bump the limit.
  extra_tserver_flags->push_back("--max_cell_size_bytes=1000000");
}

void RaftConsensusITestBase::CauseFollowerToFallBehindLogGC(
    const itest::TabletServerMap& tablet_servers,
    string* leader_uuid,
    int64_t* orig_term,
    string* fell_behind_uuid) {
  MonoDelta kTimeout = MonoDelta::FromSeconds(10);
  // Wait for all of the replicas to have acknowledged the elected
  // leader and logged the first NO_OP.
  ASSERT_OK(WaitForServersToAgree(kTimeout, tablet_servers, tablet_id_, 1));

  // Pause one server. This might be the leader, but pausing it will cause
  // a leader election to happen.
  TServerDetails* replica = (*tablet_replicas_.begin()).second;
  ExternalTabletServer* replica_ets = cluster_->tablet_server_by_uuid(replica->uuid());
  ASSERT_OK(replica_ets->Pause());

  // Find a leader. In case we paused the leader above, this will wait until
  // we have elected a new one.
  TServerDetails* leader = nullptr;
  while (true) {
    Status s = GetLeaderReplicaWithRetries(tablet_id_, &leader);
    if (s.ok() && leader != nullptr && leader != replica) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  *leader_uuid = leader->uuid();
  int leader_index = cluster_->tablet_server_index_by_uuid(*leader_uuid);

  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.set_timeout_allowed(true);
  workload.set_payload_bytes(128 * 1024); // Write ops of size 128KB.
  workload.set_write_batch_size(1);
  workload.set_num_write_threads(4);
  workload.Setup();
  workload.Start();

  LOG(INFO) << "Waiting until we've written at least 4MB...";
  while (workload.rows_inserted() < 8 * 4) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  LOG(INFO) << "Waiting for log GC on " << leader->uuid();
  // Some WAL segments must exist, but wal segment 1 must not exist.
  ASSERT_OK(inspect_->WaitForFilePatternInTabletWalDirOnTs(
      leader_index, tablet_id_, { "wal-" }, { "wal-000000001" }));

  LOG(INFO) << "Log GC complete on " << leader->uuid();

  // Then wait another couple of seconds to be sure that it has bothered to try
  // to write to the paused peer.
  // TODO(unknown): would be nice to be able to poll the leader with an RPC like
  // GetLeaderStatus() which could tell us whether it has made any requests
  // since the log GC.
  SleepFor(MonoDelta::FromSeconds(2));

  // Make a note of whatever the current term of the cluster is,
  // before we resume the follower.
  {
    OpId op_id;
    ASSERT_OK(GetLastOpIdForReplica(tablet_id_, leader, consensus::RECEIVED_OPID, kTimeout,
                                    &op_id));
    *orig_term = op_id.term();
    LOG(INFO) << "Servers converged with original term " << *orig_term;
  }

  // Resume the follower.
  LOG(INFO) << "Resuming  " << replica->uuid();
  ASSERT_OK(replica_ets->Resume());

  // Ensure that none of the tablet servers crashed.
  for (const auto& e: tablet_servers) {
  //for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    // Make sure the involved servsers didn't crash.
    ASSERT_TRUE(cluster_->tablet_server_by_uuid(e.first)->IsProcessAlive())
        << "Tablet server " << e.first << " crashed";
  }
  *fell_behind_uuid = replica->uuid();
}

Status RaftConsensusITestBase::GetTermMetricValue(ExternalTabletServer* ts,
                                                  int64_t *term) {
  return GetInt64Metric(ts->bound_http_hostport(),
                        &METRIC_ENTITY_tablet, nullptr, &METRIC_raft_term,
                        "value", term);
}

}  // namespace tserver
}  // namespace kudu

