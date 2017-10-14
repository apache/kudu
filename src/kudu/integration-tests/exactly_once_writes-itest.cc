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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/log_verifier.h"
#include "kudu/integration-tests/ts_itest-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/barrier.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DECLARE_int32(consensus_rpc_timeout_ms);
DECLARE_int32(num_replicas);
DECLARE_int32(num_tablet_servers);

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tserver {

static const int kConsensusRpcTimeoutForTests = 50;
static const int kNumDifferentRows = 1000;

class ExactlyOnceSemanticsITest : public TabletServerIntegrationTestBase {
 public:
  ExactlyOnceSemanticsITest() : seed_(SeedRandom()) {}

  void SetUp() override {
    TabletServerIntegrationTestBase::SetUp();
    FLAGS_consensus_rpc_timeout_ms = kConsensusRpcTimeoutForTests;
  }

  // Writes 'num_rows' to the tablet server listening on 'address' and collects all success
  // responses. If a write fails for some reason, continues to try until it succeeds. Since
  // followers are also able to return responses to the client, writes should succeed in bounded
  // time. Uses 'random' to generate the rows to write so that multiple threads try to write the
  // same rows.
  void WriteRowsAndCollectResponses(int thread_idx,
                                    int num_batches,
                                    int batch_size,
                                    Barrier* barrier,
                                    vector<WriteResponsePB>* responses);

  void DoTestWritesWithExactlyOnceSemantics(const vector<string>& ts_flags,
                                            const vector<string>& master_flags,
                                            int num_batches,
                                            bool allow_crashes);

 protected:
  int seed_;

};

void ExactlyOnceSemanticsITest::WriteRowsAndCollectResponses(int thread_idx,
                                                             int num_batches,
                                                             int batch_size,
                                                             Barrier* barrier,
                                                             vector<WriteResponsePB>* responses) {

  const int64_t kMaxAttempts = 100000;
  // Set the same seed in all threads so that they generate the same requests.
  Random random(seed_);
  Sockaddr address = cluster_.get()->tablet_server(
      thread_idx % FLAGS_num_replicas)->bound_rpc_addr();

  rpc::RpcController controller;

  const Schema schema = GetSimpleTestSchema();

  std::shared_ptr<rpc::Messenger> client_messenger;
  rpc::MessengerBuilder bld("Client");
  ASSERT_OK(bld.Build(&client_messenger));

  unique_ptr<TabletServerServiceProxy> proxy(new TabletServerServiceProxy(
      client_messenger, address, address.host()));
  for (int i = 0; i < num_batches; i++) {
    // Wait for all of the other writer threads to finish their attempts of the prior
    // batch before continuing on to the next one. This has two important effects:
    //   1) we are more likely to trigger races where multiple attempts of the same sequence
    //      number arrive concurrently.
    //   2) we set 'first_incomplete_seq_no' to our current sequence number, which means
    //      that each time we start a new batch, we allow garbage collection of the result
    //      tracker entries for the prior batches. So, if we let other threads continue to
    //      retry the prior batch while we moved on to the next batch, they might get a
    //      'STALE' error response.
    barrier->Wait();
    WriteRequestPB request;
    request.set_tablet_id(tablet_id_);
    SchemaToPB(schema, request.mutable_schema());

    // For 1/3 of the batches, perform an empty write. This will make sure that we also stress
    // the path where writes aren't serialized by row locks.
    if (i % 3 != 0) {
      for (int j = 0; j < batch_size; j++) {
        int row_key = random.Next() % kNumDifferentRows;
        AddTestRowToPB(RowOperationsPB::INSERT, schema, row_key, row_key, "",
                       request.mutable_row_operations());
      }
    }

    int64_t num_attempts = 0;
    int64_t base_attempt_idx = thread_idx * num_batches + i;

    while (true) {
      controller.Reset();
      WriteResponsePB response;

      unique_ptr<rpc::RequestIdPB> request_id(new rpc::RequestIdPB());
      request_id->set_client_id("test_client");
      request_id->set_seq_no(i);
      request_id->set_attempt_no(base_attempt_idx * kMaxAttempts + num_attempts);
      request_id->set_first_incomplete_seq_no(i);

      controller.SetRequestIdPB(std::move(request_id));

      Status status = proxy->Write(request, &response, &controller);
      if (status.ok() && response.has_error()) {
        status = StatusFromPB(response.error().status());
      }
      // If there was no error, store the response.
      if (status.ok()) {
        responses->push_back(response);
        break;
      }

      KLOG_EVERY_N(INFO, 100) << "[" << thread_idx << "] Couldn't write batch [" << i << "/"
          << num_batches << "]. Status: " << status.ToString();
      num_attempts++;
      SleepFor(MonoDelta::FromMilliseconds(2));
      if (num_attempts > kMaxAttempts) {
        FAIL() << "Couldn't write request to tablet server @ " << address.ToString()
                   << " Status: " << status.ToString();
      }
    }
  }
}

void ExactlyOnceSemanticsITest::DoTestWritesWithExactlyOnceSemantics(
    const vector<string>& ts_flags,
    const vector<string>& master_flags,
    int num_batches,
    bool allow_crashes) {
  const int kBatchSize = 10;
  const int kNumThreadsPerReplica = 2;

  NO_FATALS(BuildAndStart(ts_flags, master_flags));

  vector<itest::TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);

  vector<scoped_refptr<kudu::Thread>> threads;

  const int num_threads = FLAGS_num_replicas * kNumThreadsPerReplica;
  vector<vector<WriteResponsePB>> responses(num_threads);
  Barrier barrier(num_threads);

  // Create kNumThreadsPerReplica write threads per replica.
  for (int i = 0; i < num_threads; i++) {
    int thread_idx = i;
    int ts_idx = thread_idx % FLAGS_num_replicas;
    scoped_refptr<kudu::Thread> thread;
    string worker_name = strings::Substitute(
        "writer-$0-$1", thread_idx,
        cluster_.get()->tablet_server(ts_idx)->bound_rpc_addr().ToString());

    kudu::Thread::Create(
        "TestWritesWithExactlyOnceSemantics",
        worker_name,
        &ExactlyOnceSemanticsITest::WriteRowsAndCollectResponses,
        this,
        thread_idx,
        num_batches,
        kBatchSize,
        &barrier,
        &responses[i],
        &thread);
    threads.push_back(thread);
  }

  bool done = false;
  while (!done) {
    done = true;
    for (auto& thread : threads) {
      if (ThreadJoiner(thread.get()).give_up_after_ms(0).Join().IsAborted()) {
        done = false;
        break;
      }
    }
    if (allow_crashes) {
      RestartAnyCrashedTabletServers();
    } else {
      NO_FATALS(AssertNoTabletServersCrashed());
    }

    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // Make sure we're received the same responses, for the same operations, on all threads.
  bool mismatched = false;
  for (int i = 0; i < num_batches; i++) {
    for (int j = 0; j < num_threads; j++) {
      string expected_response = pb_util::SecureShortDebugString(responses[j][i]);
      string expected_ts = strings::Substitute(
          "T:$0 TSidx:$1 TSuuid:$2", j, j % FLAGS_num_replicas,
          cluster_.get()->tablet_server(j % FLAGS_num_replicas)->instance_id().permanent_uuid());
      for (int k = 0; k < num_threads; k++) {
        string got_response = pb_util::SecureShortDebugString(responses[k][i]);
        string got_ts = strings::Substitute(
            "T:$0 TSidx:$1 TSuuid:$2", k, k % FLAGS_num_replicas,
            cluster_.get()->tablet_server(k % FLAGS_num_replicas)->instance_id().permanent_uuid());
        if (expected_response != got_response) {
          mismatched = true;
          LOG(ERROR) << "Responses mismatched. Expected[" << expected_ts << "]: "
              << expected_response << " Got[" << got_ts << "]: " << got_response;
        }
      }
    }
  }
  if (mismatched) {
    FAIL() << "Got mismatched responses";
  }

  // Check that the servers have matching commit indexes. We shut down first because otherwise
  // they keep appending to the logs, and the verifier can hit checksum issues trying to
  // read from a log which is in the process of being written.
  cluster_->Shutdown();
  LogVerifier lv(cluster_.get());
  ASSERT_OK(lv.VerifyCommittedOpIdsMatch());
}

// This tests exactly once semantics by starting a cluster with multiple replicas and attempting
// to write in all the replicas at the same time.
// The write workload purposefully uses repeated rows so that we can make sure that the same
// response is obtained from all the replicas (responses without errors are trivially equal).
// Finally this crashes nodes and uses a very small election timeout to trigger rare paths that
// only happen on leader change.
TEST_F(ExactlyOnceSemanticsITest, TestWritesWithExactlyOnceSemanticsWithCrashyNodes) {
  vector<string> ts_flags, master_flags;

  // Crash 2.5% of the time right after sending an RPC. This makes sure we stress the path
  // where there are duplicate handlers for a transaction as a leader crashes right
  // after sending requests to followers.
  ts_flags.emplace_back("--fault_crash_after_leader_request_fraction=0.025");

  // Make leader elections faster so we get through more cycles of leaders.
  ts_flags.emplace_back("--raft_heartbeat_interval_ms=200");

  // Avoid preallocating segments since bootstrap is a little bit
  // faster if it doesn't have to scan forward through the preallocated
  // log area.
  ts_flags.emplace_back("--log_preallocate_segments=false");

  int num_batches = 10;
  if (AllowSlowTests()) {
    num_batches = 100;
    FLAGS_num_tablet_servers = 7;
    FLAGS_num_replicas = 7;
  }

  DoTestWritesWithExactlyOnceSemantics(ts_flags,
                                       master_flags,
                                       num_batches,
                                       true /* Allow crashes */);
}

// Like the test above but instead of crashing nodes makes sure elections are churny.
TEST_F(ExactlyOnceSemanticsITest, TestWritesWithExactlyOnceSemanticsWithChurnyElections) {
  vector<string> ts_flags, master_flags;

#if defined(THREAD_SANITIZER) || defined(ADDRESS_SANITIZER)
  // On TSAN/ASAN builds, we need to be a little bit less churny in order to make
  // any progress at all.
  ts_flags.push_back("--raft_heartbeat_interval_ms=5");
#else
  ts_flags.emplace_back("--raft_heartbeat_interval_ms=2");
#endif

  int num_batches = 200;
  if (AllowSlowTests()) {
    num_batches = 1000;
    // Only set this to 5 replicas, for slow tests, otherwise we overwhelm the jenkins slaves,
    // elections run forever and the test doesn't complete.
    FLAGS_num_tablet_servers = 5;
    FLAGS_num_replicas = 5;
  }

  DoTestWritesWithExactlyOnceSemantics(ts_flags,
                                       master_flags,
                                       num_batches,
                                       false /* No crashes */);
}

} // namespace tserver
} // namespace kudu
