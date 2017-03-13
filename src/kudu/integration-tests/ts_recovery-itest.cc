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
#include <string>

#include "kudu/client/client.h"
#include "kudu/consensus/log-test-base.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/server/clock.h"
#include "kudu/server/hybrid_clock.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/test_util.h"

using std::string;
using std::unique_ptr;

namespace kudu {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduInsert;
using client::KuduSession;
using client::KuduTable;
using client::KuduUpdate;
using client::sp::shared_ptr;
using consensus::ConsensusMetadata;
using consensus::OpId;
using consensus::RECEIVED_OPID;
using log::AppendNoOpsToLogSync;
using log::Log;
using log::LogOptions;
using server::Clock;
using server::HybridClock;

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
  void StartCluster(const vector<string>& extra_tserver_flags = {},
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

// Test the following scenario:
// - an insert is written to the WAL with a cell that is valid with the configuration
//   at the time of write (eg because it's from a version of Kudu prior to cell size
//   limits)
// - the server is restarted with cell size limiting enabled (or lowered)
// - the bootstrap should fail (but not crash) because it cannot apply the cell size
// - if we manually increase the cell size limit again, it should replay correctly.
TEST_F(TsRecoveryITest, TestChangeMaxCellSize) {
  NO_FATALS(StartCluster({}));
  TestWorkload work(cluster_.get());
  work.set_num_replicas(1);
  work.set_payload_bytes(10000);
  work.Setup();
  work.Start();
  while (work.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  work.StopAndJoin();

  // Restart the server with a lower value of max_cell_size_bytes.
  auto* ts = cluster_->tablet_server(0);
  ts->Shutdown();
  ts->mutable_flags()->push_back("--max_cell_size_bytes=1000");
  ASSERT_OK(ts->Restart());

  // The bootstrap should fail and leave the tablet in FAILED state.
  std::unordered_map<std::string, itest::TServerDetails*> ts_map;
  ValueDeleter del(&ts_map);

  ASSERT_OK(itest::CreateTabletServerMap(cluster_->master_proxy().get(),
                                         cluster_->messenger(),
                                         &ts_map));
  AssertEventually([&]() {
      vector<tserver::ListTabletsResponsePB::StatusAndSchemaPB> tablets;
      ASSERT_OK(ListTablets(ts_map[ts->uuid()], MonoDelta::FromSeconds(10), &tablets));
      ASSERT_EQ(1, tablets.size());
      ASSERT_EQ(tablet::FAILED, tablets[0].tablet_status().state());
      ASSERT_STR_CONTAINS(tablets[0].tablet_status().last_status(),
                          "value too large for column");
    });

  // Restart the server again with the default max_cell_size.
  // Bootstrap should succeed and all rows should be present.
  ts->Shutdown();
  ts->mutable_flags()->pop_back();
  ASSERT_OK(ts->Restart());
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckRowCountWithRetries(work.table_name(),
                                       ClusterVerifier::EXACTLY,
                                       work.rows_inserted(),
                                       MonoDelta::FromSeconds(60)));
}

class TsRecoveryITestDeathTest : public TsRecoveryITest {};

// Test that tablet bootstrap can automatically repair itself if it finds an
// overflowed OpId index written to the log caused by KUDU-1933.
// Also serves as a regression itest for KUDU-1933 by writing ops with a high
// term and index.
TEST_F(TsRecoveryITestDeathTest, TestRecoverFromOpIdOverflow) {
#if defined(THREAD_SANITIZER)
  // TSAN cannot handle spawning threads after fork().
  return;
#endif

  // Create the initial tablet files on disk, then shut down the cluster so we
  // can meddle with the WAL.
  NO_FATALS(StartCluster());
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup();

  std::unordered_map<std::string, itest::TServerDetails*> ts_map;
  ValueDeleter del(&ts_map);
  ASSERT_OK(itest::CreateTabletServerMap(cluster_->master_proxy().get(),
                                         cluster_->messenger(),
                                         &ts_map));
  vector<tserver::ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  auto* ets = cluster_->tablet_server(0);
  auto* ts = ts_map[ets->uuid()];
  ASSERT_OK(ListTablets(ts, MonoDelta::FromSeconds(10), &tablets));
  ASSERT_EQ(1, tablets.size());
  const string& tablet_id = tablets[0].tablet_status().tablet_id();

  cluster_->Shutdown();

  const int64_t kOverflowedIndexValue = static_cast<int64_t>(INT32_MIN);
  const int64_t kDesiredIndexValue = static_cast<int64_t>(INT32_MAX) + 1;
  const int kNumOverflowedEntriesToWrite = 4;

  {
    // Append a no-op to the WAL with an overflowed term and index to simulate a
    // crash after KUDU-1933.
    gscoped_ptr<FsManager> fs_manager(new FsManager(env_, ets->data_dir()));
    ASSERT_OK(fs_manager->Open());
    scoped_refptr<Clock> clock(new HybridClock());
    ASSERT_OK(clock->Init());

    OpId opid;
    opid.set_term(kOverflowedIndexValue);
    opid.set_index(kOverflowedIndexValue);

    ASSERT_DEATH({
      scoped_refptr<Log> log;
      ASSERT_OK(Log::Open(LogOptions(),
                          fs_manager.get(),
                          tablet_id,
                          SchemaBuilder(GetSimpleTestSchema()).Build(),
                          0, // schema_version
                          nullptr,
                          &log));

      // Write a series of negative OpIds.
      // This will cause a crash, but only after they have been written to disk.
      ASSERT_OK(AppendNoOpsToLogSync(clock, log.get(), &opid, kNumOverflowedEntriesToWrite));
    }, "Check failed: log_index > 0");

    // We also need to update the ConsensusMetadata to match with the term we
    // want to end up with.
    unique_ptr<ConsensusMetadata> cmeta;
    ConsensusMetadata::Load(fs_manager.get(), tablet_id, fs_manager->uuid(), &cmeta);
    cmeta->set_current_term(kDesiredIndexValue);
    ASSERT_OK(cmeta->Flush());
  }

  // Don't become leader because that will append another NO_OP to the log.
  ets->mutable_flags()->push_back("--enable_leader_failure_detection=false");
  ASSERT_OK(cluster_->Restart());

  OpId last_written_opid;
  AssertEventually([&] {
    // Tablet bootstrap should have converted the negative OpIds to positive ones.
    ASSERT_OK(itest::GetLastOpIdForReplica(tablet_id, ts, RECEIVED_OPID, MonoDelta::FromSeconds(5),
                                           &last_written_opid));
    ASSERT_TRUE(last_written_opid.IsInitialized());
    OpId expected_opid;
    expected_opid.set_term(kDesiredIndexValue);
    expected_opid.set_index(static_cast<int64_t>(INT32_MAX) + kNumOverflowedEntriesToWrite);
    ASSERT_OPID_EQ(expected_opid, last_written_opid);
  });
  NO_FATALS();

  // Now, write some records that will have a higher opid than INT32_MAX and
  // ensure they get written. This checks for overflows in the write path.

  // We have to first remove the flag disabling failure detection.
  cluster_->Shutdown();
  ets->mutable_flags()->pop_back();
  ASSERT_OK(cluster_->Restart());

  // Write a few records.
  workload.Start();
  while (workload.batches_completed() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  // Validate.
  OpId prev_written_opid = last_written_opid;
  AssertEventually([&] {
    ASSERT_OK(itest::GetLastOpIdForReplica(tablet_id, ts, RECEIVED_OPID, MonoDelta::FromSeconds(5),
                                           &last_written_opid));
    ASSERT_TRUE(last_written_opid.IsInitialized());
    ASSERT_GT(last_written_opid.term(), INT32_MAX);
    ASSERT_GT(last_written_opid.index(), INT32_MAX);
    // Term will increase because of an election.
    ASSERT_GT(last_written_opid.term(), prev_written_opid.term());
    ASSERT_GT(last_written_opid.index(), prev_written_opid.index());
  });
  NO_FATALS();
  NO_FATALS(cluster_->AssertNoCrashes());
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
  // Speed up test by not fsyncing.
  opts.extra_tserver_flags.push_back("--never_fsync");
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
  ASSERT_OK(cluster_->WaitForTabletsRunning(ts, -1, MonoDelta::FromSeconds(90)));

  // Verify that the bootstrapped server matches the other replications, which
  // had no faults.
  ClusterVerifier v(cluster_.get());
  v.SetVerificationTimeout(MonoDelta::FromSeconds(30));
  NO_FATALS(v.CheckCluster());
}


} // namespace kudu
