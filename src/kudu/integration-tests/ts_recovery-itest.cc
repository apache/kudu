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

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/consensus-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/log-test-base.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/atomic.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

METRIC_DECLARE_gauge_uint64(tablets_num_failed);

using kudu::client::KuduClient;
using kudu::client::KuduInsert;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduUpdate;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalTabletServer;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::clock::Clock;
using kudu::clock::HybridClock;
using kudu::consensus::ConsensusMetadata;
using kudu::consensus::ConsensusMetadataManager;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::EXCLUDE_HEALTH_REPORT;
using kudu::consensus::OpId;
using kudu::consensus::RECEIVED_OPID;
using kudu::fs::BlockManager;
using kudu::itest::MiniClusterFsInspector;
using kudu::itest::TServerDetails;
using kudu::log::AppendNoOpsToLogSync;
using kudu::log::Log;
using kudu::log::LogOptions;
using kudu::tablet::TabletMetadata;
using kudu::tablet::TabletSuperBlockPB;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace {
// Generate a row key such that an increasing sequence (0...N) ends up spreading writes
// across the key space as several sequential streams rather than a single sequential
// sequence.
int IntToKey(int i) {
  return 100000000 * (i % 3) + i;
}
} // anonymous namespace

class TsRecoveryITest : public ExternalMiniClusterITestBase,
                        public ::testing::WithParamInterface<string> {
 public:

 protected:
  void StartClusterOneTs(vector<string> extra_tserver_flags = {},
                         vector<string> extra_master_flags = {});

};

void TsRecoveryITest::StartClusterOneTs(vector<string> extra_tserver_flags,
                                        vector<string> extra_master_flags) {
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = 1;
  opts.block_manager_type = GetParam();
  opts.extra_tserver_flags = std::move(extra_tserver_flags);
  opts.extra_master_flags = std::move(extra_master_flags);
  StartClusterWithOpts(opts);
}

// Regression test for KUDU-1038: This test replicates the scenario where :
// 1. A log segment is deleted from one of the Tablet Replicas or is not fsynced before a crash.
// 2. On server restart, the replica with the deleted log segment enters a FAILED state after
// being unable to complete replay of the WAL and leaves the WAL recovery directory in place.
// 3. The master should tombstone the FAILED replica, causing its recovery directory to be deleted.
// A subsequent tablet copy and tablet bootstrap should cause the replica to become healthy again.
TEST_F(TsRecoveryITest, TestTabletRecoveryAfterSegmentDelete) {
  // Start a cluster with 3 tablet servers consisting of 1 tablet with 3 replicas.
  // Configure a small log segment size to quickly create new log segments.
  // Since we want to write as quickly as possible, we disable WAL compression.
  vector<string> flags;
  flags.emplace_back("--log_segment_size_mb=1");
  flags.emplace_back("--log_min_segments_to_retain=3");
  flags.emplace_back("--log_compression_codec=''");
  NO_FATALS(StartCluster(flags));

  const int kNumTablets = 1;
  const int kNumTs = 3;
  const int kTsIndex = 0; // Index of the tablet server we'll use for the test.
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const MonoDelta kLongTimeout = MonoDelta::FromSeconds(60);

  // Create a new tablet.
  TestWorkload write_workload(cluster_.get());
  write_workload.set_payload_bytes(32 * 1024); // Write ops of size 32KB to quickly fill the logs.
  write_workload.set_num_tablets(kNumTablets);
  write_workload.set_write_batch_size(1);
  write_workload.Setup();

  // Retrieve tablet id.
  TServerDetails* ts = ts_map_[cluster_->tablet_server(kTsIndex)->uuid()];
  vector<tserver::ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts, 1, kTimeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  LOG(INFO) << "Starting workload...";

  write_workload.Start();

  LOG(INFO) << "Waiting for at least 4 files in WAL dir on tserver 0 for tablet "
            << tablets[0].tablet_status().tablet_id() << "...";
  ASSERT_OK(inspect_->WaitForMinFilesInTabletWalDirOnTS(kTsIndex,
            tablet_id, /* num wal segments + index file */ 4));

  const auto* ets = cluster_->tablet_server(0);

  write_workload.StopAndJoin();

  // Get the current consensus state.
  ConsensusStatePB orig_cstate;
  ASSERT_OK(GetConsensusState(ts, tablet_id, kTimeout, EXCLUDE_HEALTH_REPORT, &orig_cstate));

  // Shutdown the cluster.
  cluster_->Shutdown();

  // Before restarting the cluster, delete a log segment
  // from the first tablet replica.
  {
    FsManagerOpts opts;
    opts.wal_root = ets->wal_dir();
    opts.data_roots = ets->data_dirs();

    unique_ptr<FsManager> fs_manager(new FsManager(env_, opts));

    ASSERT_OK(fs_manager->Open());

    string wal_dir = fs_manager->GetTabletWalDir(tablet_id);

    // Delete one of the WAL segments so at tablet startup time we will
    // detect out-of-order WAL segments during log replay and fail to bootstrap.
    string segment = fs_manager->GetWalSegmentFileName(tablet_id, 2);

    LOG(INFO) << "Deleting WAL segment: " << segment;
    ASSERT_OK(fs_manager->env()->DeleteFile(segment));
  }

  ASSERT_OK(cluster_->Restart());

  // The master will evict and then re-add the FAILED tablet replica.
  for (unsigned i = 0; i < kNumTs; ++i) {
    TServerDetails *ts_details = ts_map_[cluster_->tablet_server(i)->uuid()];
    // Wait for a bit longer to avoid flakiness.
    ASSERT_OK(WaitUntilTabletRunning(ts_details, tablet_id, kLongTimeout));
  }

  // Ensure that the config changed since we started (evicted, re-added).
  ConsensusStatePB new_cstate;
  ASSERT_OK(GetConsensusState(ts, tablet_id, kTimeout, EXCLUDE_HEALTH_REPORT, &new_cstate));

  ASSERT_GT(new_cstate.committed_config().opid_index(),
            orig_cstate.committed_config().opid_index());
}

// Test for KUDU-2202 that ensures that blocks not found in the FS layer but
// that are referenced by a tablet will not be reused.
TEST_P(TsRecoveryITest, TestNoBlockIDReuseIfMissingBlocks) {
  if (GetParam() != "log") {
    LOG(INFO) << "Missing blocks is currently only supported by the log "
                 "block manager. Exiting early!";
    return;
  }
  // Set up a basic server that flushes often so we create blocks quickly.
  NO_FATALS(StartClusterOneTs({
    "--flush_threshold_secs=1",
    "--flush_threshold_mb=1"
  }));

  // Write to the tserver and wait for some blocks to be written.
  const auto StartSingleTabletWorkload = [&] (const string& table_name) {
    TestWorkload* write_workload(new TestWorkload(cluster_.get()));
    write_workload->set_table_name(table_name);
    write_workload->set_num_tablets(1);
    write_workload->set_num_replicas(1);
    // We want to generate orphaned blocks, so use a workload that we expect to
    // cause a lot of delta compaction.
    write_workload->set_write_pattern(TestWorkload::UPDATE_ONE_ROW);
    write_workload->Setup();
    write_workload->Start();
    return write_workload;
  };

  unique_ptr<TestWorkload> write_workload(StartSingleTabletWorkload("foo"));
  unique_ptr<MiniClusterFsInspector> inspect(
      new MiniClusterFsInspector(cluster_.get()));
  vector<string> tablets = inspect->ListTabletsOnTS(0);
  ASSERT_EQ(1, tablets.size());
  const string tablet_id = tablets[0];

  // Get the block ids for a given tablet.
  const auto BlocksForTablet = [&] (const string& tablet_id,
                                    vector<BlockId>* live_block_ids,
                                    vector<BlockId>* orphaned_block_ids = nullptr) {
    TabletSuperBlockPB superblock_pb;
    RETURN_NOT_OK(inspect->ReadTabletSuperBlockOnTS(0, tablet_id, &superblock_pb));
    if (orphaned_block_ids) {
      orphaned_block_ids->clear();
      for (const auto& pb : superblock_pb.orphaned_blocks()) {
        orphaned_block_ids->push_back(BlockId::FromPB(pb));
      }
    }
    live_block_ids->clear();
    vector<BlockIdPB> block_id_pbs = TabletMetadata::CollectBlockIdPBs(superblock_pb);
    for (const auto& pb : block_id_pbs) {
      live_block_ids->push_back(BlockId::FromPB(pb));
    }
    return Status::OK();
  };

  // Wait until we have some live blocks and some orphaned blocks and collect
  // all of the blocks ids.
  vector<BlockId> block_ids;
  vector<BlockId> orphaned_block_ids;
  ASSERT_EVENTUALLY([&] () {
    ASSERT_OK(BlocksForTablet(tablet_id, &block_ids, &orphaned_block_ids));
    ASSERT_TRUE(!block_ids.empty());
    ASSERT_TRUE(!orphaned_block_ids.empty());
    block_ids.insert(block_ids.end(), orphaned_block_ids.begin(), orphaned_block_ids.end());
  });
  write_workload->StopAndJoin();
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  ts->Shutdown();

  // Now empty the data directories of blocks. The server will start up, not
  // read any blocks, but should still avoid the old tablet's blocks.
  for (const string& dir : ts->data_dirs()) {
    const string& data_dir = JoinPathSegments(dir, "data");
    vector<string> children;
    ASSERT_OK(env_->GetChildren(data_dir, &children));
    for (const string& child : children) {
      if (child != "." && child != ".." && child != fs::kInstanceMetadataFileName) {
        ASSERT_OK(env_->DeleteFile(JoinPathSegments(data_dir, child)));
      }
    }
  }
  ASSERT_OK(ts->Restart());

  // Sanity check that the tablet goes into a failed state with its missing blocks.
  int64_t failed_on_ts = 0;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(itest::GetInt64Metric(ts->bound_http_hostport(),
        &METRIC_ENTITY_server, nullptr, &METRIC_tablets_num_failed, "value", &failed_on_ts));
    ASSERT_EQ(1, failed_on_ts);
  });

  // Create a new tablet and collect its blocks.
  write_workload.reset(StartSingleTabletWorkload("bar"));
  tablets = inspect->ListTabletsOnTS(0);
  ASSERT_EQ(2, tablets.size());
  const string& new_tablet_id = tablets[0] == tablet_id ? tablets[1] : tablets[0];
  vector<BlockId> new_block_ids;
  ASSERT_EVENTUALLY([&] () {
    ASSERT_OK(BlocksForTablet(new_tablet_id, &new_block_ids));
    ASSERT_TRUE(!new_block_ids.empty());
  });

  // Compare the tablets' block IDs and ensure there is no overlap.
  vector<BlockId> block_id_intersection;
  std::set_intersection(block_ids.begin(), block_ids.end(),
                        new_block_ids.begin(), new_block_ids.end(),
                        std::back_inserter(block_id_intersection));
  {
    SCOPED_TRACE(Substitute("First tablet's blocks: $0", BlockId::JoinStrings(block_ids)));
    SCOPED_TRACE(Substitute("Second tablet's blocks: $0", BlockId::JoinStrings(new_block_ids)));
    SCOPED_TRACE(Substitute("Intersection: $0", BlockId::JoinStrings(block_id_intersection)));
    ASSERT_TRUE(block_id_intersection.empty());
  }
}
// Test crashing a server just before appending a COMMIT message.
// We then restart the server and ensure that all rows successfully
// inserted before the crash are recovered.
TEST_P(TsRecoveryITest, TestRestartWithOrphanedReplicates) {
  NO_FATALS(StartClusterOneTs());

  TestWorkload work(cluster_.get());
  work.set_num_replicas(1);
  work.set_num_write_threads(4);
  work.set_write_timeout_millis(1000);
  work.set_timeout_allowed(true);
  work.Setup();

  // Crash when the WAL contains a replicate message but no corresponding commit.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
                              "fault_crash_before_append_commit", "0.05"));
  work.Start();

  // Wait for the process to crash due to the injected fault.
  ASSERT_OK(cluster_->tablet_server(0)->WaitForInjectedCrash(MonoDelta::FromSeconds(120)));

  // Stop the writers.
  work.StopAndJoin();

  // Restart the server, and it should recover.
  cluster_->tablet_server(0)->Shutdown();

  // Restart the server and check to make sure that the change is eventually applied.
  ASSERT_OK(cluster_->tablet_server(0)->Restart());

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckRowCount(work.table_name(),
                            ClusterVerifier::AT_LEAST,
                            work.rows_inserted()));
}

// Regression test for KUDU-1477: a pending commit message would cause
// bootstrap to fail if that message only included errors and no
// successful operations.
TEST_P(TsRecoveryITest, TestRestartWithPendingCommitFromFailedOp) {
  NO_FATALS(StartClusterOneTs());
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
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
                              "fault_crash_before_append_commit", "0.01"));
  work.Start();

  // Wait for the process to crash due to the injected fault.
  ASSERT_OK(cluster_->tablet_server(0)->WaitForInjectedCrash(MonoDelta::FromSeconds(30)));

  // Stop the writers.
  work.StopAndJoin();

  // Restart the server, and it should recover.
  cluster_->tablet_server(0)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(0)->Restart());

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckRowCount(work.table_name(),
                            ClusterVerifier::AT_LEAST,
                            work.rows_inserted()));
}

// Test that we replay from the recovery directory, if it exists.
TEST_P(TsRecoveryITest, TestCrashDuringLogReplay) {
  NO_FATALS(StartClusterOneTs({ "--fault_crash_during_log_replay=0.05" }));

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
  cluster_->tablet_server(0)->mutable_flags()->pop_back();
  ASSERT_OK(cluster_->tablet_server(0)->Restart());

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckRowCount(work.table_name(),
                            ClusterVerifier::AT_LEAST,
                            work.rows_inserted()));
}

// Regression test for KUDU-1551: if the tserver crashes after preallocating a segment
// but before writing its header, the TS would previously crash on restart.
// Instead, it should ignore the uninitialized segment.
TEST_P(TsRecoveryITest, TestCrashBeforeWriteLogSegmentHeader) {
  NO_FATALS(StartClusterOneTs({
    "--log_segment_size_mb=1",
    "--log_compression_codec=NO_COMPRESSION"
  }));
  TestWorkload work(cluster_.get());
  work.set_num_replicas(1);
  work.set_write_timeout_millis(1000);
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
  NO_FATALS(v.CheckRowCount(work.table_name(),
                            ClusterVerifier::AT_LEAST,
                            work.rows_inserted()));
}

// Test the following scenario:
// - an insert is written to the WAL with a cell that is valid with the configuration
//   at the time of write (eg because it's from a version of Kudu prior to cell size
//   limits)
// - the server is restarted with cell size limiting enabled (or lowered)
// - the bootstrap should fail (but not crash) because it cannot apply the cell size
// - if we manually increase the cell size limit again, it should replay correctly.
TEST_P(TsRecoveryITest, TestChangeMaxCellSize) {
  // Prevent the master from tombstoning the evicted tablet so we can observe
  // its FAILED state.
  NO_FATALS(StartClusterOneTs({}, { "--master_tombstone_evicted_tablet_replicas=false" }));
  TestWorkload work(cluster_.get());
  work.set_num_replicas(1);
  work.set_payload_bytes(10000);
  work.Setup();
  work.Start();
  while (work.rows_inserted() < 50) {
    SleepFor(MonoDelta::FromMilliseconds(50));
  }
  work.StopAndJoin();

  // Restart the server with a lower value of max_cell_size_bytes.
  auto* ts = cluster_->tablet_server(0);
  ts->Shutdown();
  ts->mutable_flags()->push_back("--max_cell_size_bytes=1000");
  ASSERT_OK(ts->Restart());

  // The bootstrap should fail and leave the tablet in FAILED state.
  ASSERT_EVENTUALLY([&]() {
      vector<tserver::ListTabletsResponsePB::StatusAndSchemaPB> tablets;
      ASSERT_OK(ListTablets(ts_map_[ts->uuid()], MonoDelta::FromSeconds(10), &tablets));
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
  NO_FATALS(v.CheckRowCount(work.table_name(),
                            ClusterVerifier::EXACTLY,
                            work.rows_inserted()));
}

class TsRecoveryITestDeathTest : public TsRecoveryITest {};

// Test that tablet bootstrap can automatically repair itself if it finds an
// overflowed OpId index written to the log caused by KUDU-1933.
// Also serves as a regression itest for KUDU-1933 by writing ops with a high
// term and index.
TEST_P(TsRecoveryITestDeathTest, TestRecoverFromOpIdOverflow) {
#if defined(THREAD_SANITIZER)
  // TSAN cannot handle spawning threads after fork().
  return;
#endif

  // Create the initial tablet files on disk, then shut down the cluster so we
  // can meddle with the WAL.
  NO_FATALS(StartClusterOneTs());
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup();

  vector<tserver::ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  auto* ets = cluster_->tablet_server(0);
  auto* ts = ts_map_[ets->uuid()];
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
    FsManagerOpts opts;
    opts.wal_root = ets->wal_dir();
    opts.data_roots = ets->data_dirs();
    unique_ptr<FsManager> fs_manager(new FsManager(env_, opts));
    ASSERT_OK(fs_manager->Open());
    scoped_refptr<ConsensusMetadataManager> cmeta_manager(
        new ConsensusMetadataManager(fs_manager.get()));
    unique_ptr<Clock> clock(new HybridClock);
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
      ASSERT_OK(AppendNoOpsToLogSync(clock.get(), log.get(), &opid, kNumOverflowedEntriesToWrite));
    }, "Check failed: log_index > 0");

    // Before restarting the tablet server, delete the initial log segment from
    // disk (the original leader election NO_OP) if it exists since it will
    // contain OpId 1.1; If the COMMIT message for this NO_OP (OpId 1.1) was
    // not written to disk yet, then it might get written _after_ the ops with
    // the overflowed ids above, triggering a CHECK about non sequential OpIds.
    // If we remove the first segment then the tablet will just assume that
    // commit messages for all replicates in previous segments have already
    // been written, thus avoiding the check.
    string wal_dir = fs_manager->GetTabletWalDir(tablet_id);
    vector<string> wal_children;
    ASSERT_OK(fs_manager->env()->GetChildren(wal_dir, &wal_children));
    // Skip '.', '..', and index files.
    std::unordered_set<string> wal_segments;
    for (const auto& filename : wal_children) {
      if (HasPrefixString(filename, FsManager::kWalFileNamePrefix)) {
        wal_segments.insert(filename);
      }
    }
    ASSERT_GE(wal_segments.size(), 2) << "Too few WAL segments. Files in dir (" << wal_dir << "): "
                                      << wal_children;
    // If WAL segment index 1 exists, delete it.
    string first_segment = fs_manager->GetWalSegmentFileName(tablet_id, 1);
    if (fs_manager->env()->FileExists(first_segment)) {
      LOG(INFO) << "Deleting first WAL segment: " << first_segment;
      ASSERT_OK(fs_manager->env()->DeleteFile(first_segment));
    }

    // We also need to update the ConsensusMetadata to match with the term we
    // want to end up with.
    scoped_refptr<ConsensusMetadata> cmeta;
    ASSERT_OK(cmeta_manager->Load(tablet_id, &cmeta));
    cmeta->set_current_term(kDesiredIndexValue);
    ASSERT_OK(cmeta->Flush());
  }

  // Don't become leader because that will append another NO_OP to the log.
  ets->mutable_flags()->push_back("--enable_leader_failure_detection=false");
  ASSERT_OK(cluster_->Restart());

  OpId last_written_opid;
  ASSERT_EVENTUALLY([&] {
    // Tablet bootstrap should have converted the negative OpIds to positive ones.
    ASSERT_OK(itest::GetLastOpIdForReplica(tablet_id, ts, RECEIVED_OPID, MonoDelta::FromSeconds(5),
                                           &last_written_opid));
    ASSERT_TRUE(last_written_opid.IsInitialized());
    OpId expected_opid;
    expected_opid.set_term(kDesiredIndexValue);
    expected_opid.set_index(static_cast<int64_t>(INT32_MAX) + kNumOverflowedEntriesToWrite);
    ASSERT_OPID_EQ(expected_opid, last_written_opid);
  });

  // Now, write some records that will have a higher opid than INT32_MAX and
  // ensure they get written. This checks for overflows in the write path.

  // We have to first remove the flag disabling failure detection.
  NO_FATALS(cluster_->AssertNoCrashes());
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
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(itest::GetLastOpIdForReplica(tablet_id, ts, RECEIVED_OPID, MonoDelta::FromSeconds(5),
                                           &last_written_opid));
    ASSERT_TRUE(last_written_opid.IsInitialized());
    ASSERT_GT(last_written_opid.term(), INT32_MAX);
    ASSERT_GT(last_written_opid.index(), INT32_MAX);
    // Term will increase because of an election.
    ASSERT_GT(last_written_opid.term(), prev_written_opid.term());
    ASSERT_GT(last_written_opid.index(), prev_written_opid.index());
  });
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
                 shared_ptr<KuduClient> client,
                 shared_ptr<KuduTable> table)
    : should_run_(false),
      inserted_(inserted),
      client_(std::move(client)),
      table_(std::move(table)) {
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

      unique_ptr<KuduUpdate> up(table_->NewUpdate());
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
class Kudu969Test : public TsRecoveryITest {
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
  const int kThreeReplicas = 3;

  // Jack up the number of maintenance manager threads to try to trigger
  // concurrency bugs where a compaction and a flush might be happening
  // at the same time during the crash.
  NO_FATALS(StartCluster({"--maintenance_manager_num_threads=3"}, {}, kThreeReplicas));

  // Set a small flush threshold so that we flush a lot (causing more compactions
  // as well).
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0), "flush_threshold_mb", "1"));

  // Use TestWorkload to create a table
  TestWorkload work(cluster_.get());
  work.set_num_replicas(kThreeReplicas);
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
  ASSERT_OK(cluster_->SetFlag(ts, GetParam(), "0.3"));

  // Insert some data.
  shared_ptr<KuduSession> session = client->NewSession();
  session->SetTimeoutMillis(1000);
  CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  for (int i = 1; ts->IsProcessAlive(); i++) {
    unique_ptr<KuduInsert> ins(table->NewInsert());
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

// Passes block manager types to the recovery test so we get some extra
// testing to cover non-default block manager types.
INSTANTIATE_TEST_CASE_P(BlockManagerType, TsRecoveryITest,
    ::testing::ValuesIn(BlockManager::block_manager_types()));

} // namespace kudu
