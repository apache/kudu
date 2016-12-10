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

#include "kudu/client/client.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/test_util.h"

#include <string>

using std::string;

namespace kudu {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduInsert;
using client::KuduSession;
using client::KuduTable;
using client::KuduUpdate;
using client::sp::shared_ptr;

namespace {
// Generate a row key such that an increasing sequence (0...N) ends up spreading writes
// across the key space as several sequential streams rather than a single sequential
// sequence.
int IntToKey(int i) {
  return 100000000 * (i % 3) + i;
}
} // anonymous namespace

class TsRecoveryITest : public KuduTest {
 public:
  virtual void TearDown() OVERRIDE {
    if (cluster_) cluster_->Shutdown();
    KuduTest::TearDown();
  }

 protected:
  void StartCluster(const vector<string>& extra_tserver_flags = vector<string>(),
                    int num_tablet_servers = 1);

  gscoped_ptr<ExternalMiniCluster> cluster_;
};

void TsRecoveryITest::StartCluster(const vector<string>& extra_tserver_flags,
                                   int num_tablet_servers) {
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = num_tablet_servers;
  opts.extra_tserver_flags = extra_tserver_flags;
  cluster_.reset(new ExternalMiniCluster(opts));
  ASSERT_OK(cluster_->Start());
}

// Test crashing a server just before appending a COMMIT message.
// We then restart the server and ensure that all rows successfully
// inserted before the crash are recovered.
TEST_F(TsRecoveryITest, TestRestartWithOrphanedReplicates) {
  NO_FATALS(StartCluster());
  cluster_->SetFlag(cluster_->tablet_server(0),
                    "fault_crash_before_append_commit", "0.05");

  TestWorkload work(cluster_.get());
  work.set_num_replicas(1);
  work.set_num_write_threads(4);
  work.set_write_timeout_millis(100);
  work.set_timeout_allowed(true);
  work.Setup();
  work.Start();

  // Wait for the process to crash due to the injected fault.
  ASSERT_OK(cluster_->tablet_server(0)->WaitForInjectedCrash(MonoDelta::FromSeconds(30)));

  // Stop the writers.
  work.StopAndJoin();

  // Restart the server, and it should recover.
  cluster_->tablet_server(0)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(0)->Restart());


  // TODO(KUDU-796): after a restart, we may have to replay some
  // orphaned replicates from the log. However, we currently
  // allow reading while those are being replayed, which means we
  // can "go back in time" briefly. So, we have some retries here.
  // When KUDU-796 is fixed, remove the retries.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckRowCountWithRetries(work.table_name(),
                                       ClusterVerifier::AT_LEAST,
                                       work.rows_inserted(),
                                       MonoDelta::FromSeconds(20)));
}

// Regression test for KUDU-1477: a pending commit message would cause
// bootstrap to fail if that message only included errors and no
// successful operations.
TEST_F(TsRecoveryITest, TestRestartWithPendingCommitFromFailedOp) {
  NO_FATALS(StartCluster());
  cluster_->SetFlag(cluster_->tablet_server(0),
                    "fault_crash_before_append_commit", "0.01");

  // Set up the workload to write many duplicate rows, and with only
  // one operation per batch. This means that by the time we crash
  // it's likely that most of the recently appended commit messages
  // are for failed insertions (dup key). We also use many threads
  // to increase the probability that commits will be written
  // out-of-order and trigger the bug.
  TestWorkload work(cluster_.get());
  work.set_num_replicas(1);
  work.set_num_write_threads(20);
  work.set_write_timeout_millis(100);
  work.set_timeout_allowed(true);
  work.set_write_batch_size(1);
  work.set_write_pattern(TestWorkload::INSERT_WITH_MANY_DUP_KEYS);
  work.Setup();
  work.Start();

  // Wait for the process to crash due to the injected fault.
  ASSERT_OK(cluster_->tablet_server(0)->WaitForInjectedCrash(MonoDelta::FromSeconds(30)));

  // Stop the writers.
  work.StopAndJoin();

  // Restart the server, and it should recover.
  cluster_->tablet_server(0)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(0)->Restart());

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckRowCountWithRetries(work.table_name(),
                                       ClusterVerifier::AT_LEAST,
                                       work.rows_inserted(),
                                       MonoDelta::FromSeconds(20)));
}

// Test that we replay from the recovery directory, if it exists.
TEST_F(TsRecoveryITest, TestCrashDuringLogReplay) {
  NO_FATALS(StartCluster({ "--fault_crash_during_log_replay=0.05" }));

  TestWorkload work(cluster_.get());
  work.set_num_replicas(1);
  work.set_num_write_threads(4);
  work.set_write_batch_size(1);
  work.set_write_timeout_millis(100);
  work.set_timeout_allowed(true);
  work.Setup();
  work.Start();
  while (work.rows_inserted() < 200) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  work.StopAndJoin();

  // Now restart the server, which will result in log replay, which will crash
  // mid-replay with very high probability since we wrote at least 200 log
  // entries and we're injecting a fault 5% of the time.
  cluster_->tablet_server(0)->Shutdown();

  // Restart might crash very quickly and actually return a bad status, so we
  // have to check the result.
  Status s = cluster_->tablet_server(0)->Restart();

  // Wait for the process to crash during log replay (if it didn't already crash
  // above while we were restarting it).
  if (s.ok()) {
    ASSERT_OK(cluster_->tablet_server(0)->WaitForInjectedCrash(MonoDelta::FromSeconds(30)));
  }

  // Now remove the crash flag, so the next replay will complete, and restart
  // the server once more.
  cluster_->tablet_server(0)->Shutdown();
  cluster_->tablet_server(0)->mutable_flags()->clear();
  ASSERT_OK(cluster_->tablet_server(0)->Restart());

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckRowCountWithRetries(work.table_name(),
                                       ClusterVerifier::AT_LEAST,
                                       work.rows_inserted(),
                                       MonoDelta::FromSeconds(30)));
}

// Regression test for KUDU-1551: if the tserver crashes after preallocating a segment
// but before writing its header, the TS would previously crash on restart.
// Instead, it should ignore the uninitialized segment.
TEST_F(TsRecoveryITest, TestCrashBeforeWriteLogSegmentHeader) {
  NO_FATALS(StartCluster({ "--log_segment_size_mb=1" }));

  TestWorkload work(cluster_.get());
  work.set_num_replicas(1);
  work.set_write_timeout_millis(100);
  work.set_timeout_allowed(true);
  work.set_payload_bytes(10000); // make logs roll without needing lots of ops.
  work.Setup();

  // Enable the fault point after creating the table, but before writing any data.
  // Otherwise, we'd crash during creation of the tablet.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
                              "fault_crash_before_write_log_segment_header", "0.9"));
  work.Start();

  // Wait for the process to crash during log roll.
  ASSERT_OK(cluster_->tablet_server(0)->WaitForInjectedCrash(MonoDelta::FromSeconds(60)));
  work.StopAndJoin();

  cluster_->tablet_server(0)->Shutdown();
  ignore_result(cluster_->tablet_server(0)->Restart());

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckRowCountWithRetries(work.table_name(),
                                       ClusterVerifier::AT_LEAST,
                                       work.rows_inserted(),
                                       MonoDelta::FromSeconds(60)));
}


// A set of threads which pick rows which are known to exist in the table
// and issue random updates against them.
class UpdaterThreads {
 public:
  static const int kNumThreads = 4;

  // 'inserted' is an atomic integer which stores the number of rows
  // which have been inserted up to that point.
  UpdaterThreads(AtomicInt<int32_t>* inserted,
                 const shared_ptr<KuduClient>& client,
                 const shared_ptr<KuduTable>& table)
    : should_run_(false),
      inserted_(inserted),
      client_(client),
      table_(table) {
  }

  // Start running the updater threads.
  void Start() {
    CHECK(!should_run_.Load());
    should_run_.Store(true);
    threads_.resize(kNumThreads);
    for (int i = 0; i < threads_.size(); i++) {
      CHECK_OK(kudu::Thread::Create("test", "updater",
                                    &UpdaterThreads::Run, this,
                                    &threads_[i]));
    }
  }

  // Stop running the updater threads, and wait for them to exit.
  void StopAndJoin() {
    CHECK(should_run_.Load());
    should_run_.Store(false);

    for (const auto& t : threads_) {
      t->Join();
    }
    threads_.clear();
  }

 protected:
  void Run() {
    Random rng(GetRandomSeed32());
    shared_ptr<KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(2000);
    CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    while (should_run_.Load()) {
      int i = inserted_->Load();
      if (i == 0) continue;

      gscoped_ptr<KuduUpdate> up(table_->NewUpdate());
      CHECK_OK(up->mutable_row()->SetInt32("key", IntToKey(rng.Uniform(i) + 1)));
      CHECK_OK(up->mutable_row()->SetInt32("int_val", rng.Next32()));
      CHECK_OK(session->Apply(up.release()));
      // The server might crash due to a compaction while we're still updating.
      // That's OK - we expect the main thread to shut us down quickly.
      WARN_NOT_OK(session->Flush(), "failed to flush updates");
    }
  }

  AtomicBool should_run_;
  AtomicInt<int32_t>* inserted_;
  shared_ptr<KuduClient> client_;
  shared_ptr<KuduTable> table_;
  vector<scoped_refptr<Thread> > threads_;
};

// Parameterized test which acts as a regression test for KUDU-969.
//
// This test is parameterized on the name of a fault point configuration.
// The fault points exercised crashes right before flushing the tablet metadata
// during a flush/compaction. Meanwhile, a set of threads hammer rows with
// a lot of updates. The goal here is to trigger the following race:
//
// - a compaction is in "duplicating" phase (i.e. updates are written to both
//   the input and output rowsets
// - we crash (due to the fault point) before writing the new metadata
//
// This exercises the bootstrap code path which replays duplicated updates
// in the case where the flush did not complete. Prior to fixing KUDU-969,
// these updates would be mistakenly considered as "already flushed", despite
// the fact that they were only written to the input rowset's memory stores, and
// never hit disk.
class Kudu969Test : public TsRecoveryITest,
                    public ::testing::WithParamInterface<const char*> {
};
INSTANTIATE_TEST_CASE_P(DifferentFaultPoints,
                        Kudu969Test,
                        ::testing::Values("fault_crash_before_flush_tablet_meta_after_compaction",
                                          "fault_crash_before_flush_tablet_meta_after_flush_mrs"));

TEST_P(Kudu969Test, Test) {
  if (!AllowSlowTests()) return;

  // We use a replicated cluster here so that the 'REPLICATE' messages
  // and 'COMMIT' messages are spread out further in time, and it's
  // more likely to trigger races. We can also verify that the server
  // with the injected fault recovers to the same state as the other
  // servers.
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = 3;

  // Jack up the number of maintenance manager threads to try to trigger
  // concurrency bugs where a compaction and a flush might be happening
  // at the same time during the crash.
  opts.extra_tserver_flags.push_back("--maintenance_manager_num_threads=3");
  cluster_.reset(new ExternalMiniCluster(opts));
  ASSERT_OK(cluster_->Start());

  // Set a small flush threshold so that we flush a lot (causing more compactions
  // as well).
  cluster_->SetFlag(cluster_->tablet_server(0), "flush_threshold_mb", "1");

  // Use TestWorkload to create a table
  TestWorkload work(cluster_.get());
  work.set_num_replicas(3);
  work.Setup();

  // Open the client and table.
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));
  shared_ptr<KuduTable> table;
  CHECK_OK(client->OpenTable(work.table_name(), &table));

  // Keep track of how many rows have been inserted.
  AtomicInt<int32_t> inserted(0);

  // Start updater threads.
  UpdaterThreads updater(&inserted, client, table);
  updater.Start();

  // Enable the fault point to crash after a few flushes or compactions.
  auto ts = cluster_->tablet_server(0);
  cluster_->SetFlag(ts, GetParam(), "0.3");

  // Insert some data.
  shared_ptr<KuduSession> session = client->NewSession();
  session->SetTimeoutMillis(1000);
  CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  for (int i = 1; ts->IsProcessAlive(); i++) {
    gscoped_ptr<KuduInsert> ins(table->NewInsert());
    ASSERT_OK(ins->mutable_row()->SetInt32("key", IntToKey(i)));
    ASSERT_OK(ins->mutable_row()->SetInt32("int_val", i));
    ASSERT_OK(ins->mutable_row()->SetNull("string_val"));
    ASSERT_OK(session->Apply(ins.release()));
    if (i % 100 == 0) {
      WARN_NOT_OK(session->Flush(), "could not flush session");
      inserted.Store(i);
    }
  }
  LOG(INFO) << "successfully detected TS crash!";
  updater.StopAndJoin();

  // Restart the TS to trigger bootstrap, and wait for it to start up.
  ts->Shutdown();
  ASSERT_OK(ts->Restart());
  ASSERT_OK(cluster_->WaitForTabletsRunning(ts, -1, MonoDelta::FromSeconds(180)));

  // Verify that the bootstrapped server matches the other replications, which
  // had no faults.
  ClusterVerifier v(cluster_.get());
  v.SetVerificationTimeout(MonoDelta::FromSeconds(30));
  NO_FATALS(v.CheckCluster());
}


} // namespace kudu
