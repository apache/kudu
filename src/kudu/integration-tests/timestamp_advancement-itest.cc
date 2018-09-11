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

#include <stdint.h>

#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/timestamp.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_index.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(enable_maintenance_manager);
DECLARE_bool(log_preallocate_segments);
DECLARE_bool(log_async_preallocate_segments);
DECLARE_bool(raft_enable_pre_election);
DECLARE_double(leader_failure_max_missed_heartbeat_periods);
DECLARE_int32(consensus_inject_latency_ms_in_notifications);
DECLARE_int32(raft_heartbeat_interval_ms);

namespace kudu {

using cluster::InternalMiniCluster;
using cluster::InternalMiniClusterOptions;
using log::LogReader;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using tablet::TabletReplica;
using tserver::MiniTabletServer;

namespace itest {

class TimestampAdvancementITest : public MiniClusterITestBase {
 public:
  // Sets up a cluster and returns the tablet replica on 'ts' that has written
  // to its WAL. 'replica' will write further messages to a new WAL segment.
  void SetupClusterWithWritesInWAL(int ts, scoped_refptr<TabletReplica>* replica) {
    NO_FATALS(StartCluster(3));

    // Write some rows to the cluster.
    TestWorkload write(cluster_.get());;

    // Set a low batch size so we have finer-grained control over flushing of
    // the WAL. Too large, and the WAL may end up flushing in the background.
    write.set_write_batch_size(1);
    write.Setup();
    write.Start();
    while (write.rows_inserted() < 10) {
      SleepFor(MonoDelta::FromMilliseconds(1));
    }
    write.StopAndJoin();

    // Ensure that the replicas eventually get to a point where all of them
    // have all the rows. This will allow us to GC the WAL, as they will not
    // need to retain them if fully-replicated.
    scoped_refptr<TabletReplica> tablet_replica = tablet_replica_on_ts(ts);
    ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(30),
          ts_map_, tablet_replica->tablet_id(), write.batches_completed()));

    // Flush the current log batch and roll over to get a fresh WAL segment.
    ASSERT_OK(tablet_replica->log()->WaitUntilAllFlushed());
    ASSERT_OK(tablet_replica->log()->AllocateSegmentAndRollOver());

    // Also flush the MRS so we're free to GC the WAL segment we just wrote.
    ASSERT_OK(tablet_replica->tablet()->Flush());
    *replica = std::move(tablet_replica);
  }

  // Returns the tablet server 'ts'.
  MiniTabletServer* tserver(int ts) const {
    DCHECK(cluster_);
    return cluster_->mini_tablet_server(ts);
  }

  // Get the tablet replica on the tablet server 'ts'.
  scoped_refptr<TabletReplica> tablet_replica_on_ts(int ts) const {
    vector<scoped_refptr<TabletReplica>> replicas;
    tserver(ts)->server()->tablet_manager()->GetTabletReplicas(&replicas);
    DCHECK_EQ(1, replicas.size());
    return replicas[0];
  }

  // Returns true if there are any write replicate messages in the WALs of
  // 'tablet_id' on 'ts'.
  Status CheckForWriteReplicatesInLog(MiniTabletServer* ts, const string& tablet_id,
                                      bool* has_write_replicates) const {
    shared_ptr<LogReader> reader;
    RETURN_NOT_OK(LogReader::Open(env_,
                  ts->server()->fs_manager()->GetTabletWalDir(tablet_id),
                  scoped_refptr<log::LogIndex>(), tablet_id,
                  scoped_refptr<MetricEntity>(), &reader));
    log::SegmentSequence segs;
    RETURN_NOT_OK(reader->GetSegmentsSnapshot(&segs));
    unique_ptr<log::LogEntryPB> entry;
    for (const auto& seg : segs) {
      log::LogEntryReader reader(seg.get());
      while (true) {
        Status s = reader.ReadNextEntry(&entry);
        if (s.IsEndOfFile()) break;
        RETURN_NOT_OK(s);
        if (entry->type() == log::REPLICATE &&
            entry->replicate().op_type() == consensus::WRITE_OP) {
          *has_write_replicates = true;
          return Status::OK();
        }
      }
    }
    *has_write_replicates = false;
    return Status::OK();
  }
};

// Test that bootstrapping a Raft no-op from the WAL will advance the replica's
// MVCC safe time timestamps.
TEST_F(TimestampAdvancementITest, TestNoOpAdvancesMvccSafeTimeOnBootstrap) {
  // Set a low Raft heartbeat and encourage frequent elections so that we can
  // fill up the WAL with no-op entries naturally.
  FLAGS_raft_heartbeat_interval_ms = 100;
  FLAGS_raft_enable_pre_election = false;

  // Prevent preallocation of WAL segments in order to prevent races between
  // the WAL allocation thread and our manual rolling over of the WAL.
  FLAGS_log_preallocate_segments = false;
  FLAGS_log_async_preallocate_segments = false;

  // We're going to manually trigger maintenance ops, so disable maintenance op
  // scheduling.
  FLAGS_enable_maintenance_manager = false;

  // Setup a cluster with some writes and a new WAL segment.
  const int kTserver = 0;
  scoped_refptr<TabletReplica> replica;
  NO_FATALS(SetupClusterWithWritesInWAL(kTserver, &replica));
  MiniTabletServer* ts = tserver(kTserver);
  const string tablet_id = replica->tablet_id();

  // Now that we're on a new WAL segment, inject latency to consensus so we
  // trigger elections, and wait for some terms to pass.
  FLAGS_leader_failure_max_missed_heartbeat_periods = 1.0;
  FLAGS_consensus_inject_latency_ms_in_notifications = 100;
  const int64_t kNumExtraTerms = 10;
  int64_t initial_raft_term = replica->consensus()->CurrentTerm();
  int64_t raft_term = initial_raft_term;
  while (raft_term < initial_raft_term + kNumExtraTerms) {
    SleepFor(MonoDelta::FromMilliseconds(10));
    raft_term = replica->consensus()->CurrentTerm();
  }

  // Reduce election churn so we can achieve a stable quorum and get to a point
  // where we can GC our logs. Note: we won't GC if there are replicas that
  // need to be caught up.
  FLAGS_consensus_inject_latency_ms_in_notifications = 0;
  ASSERT_EVENTUALLY([&] {
    LOG(INFO) << "GCing logs...";
    int64_t gcable_size;
    ASSERT_OK(replica->GetGCableDataSize(&gcable_size));
    ASSERT_GT(gcable_size, 0);
    ASSERT_OK(replica->RunLogGC());

    // Ensure that we have no writes in our WALs.
    bool has_write_replicates;
    ASSERT_OK(CheckForWriteReplicatesInLog(ts, tablet_id, &has_write_replicates));
    ASSERT_FALSE(has_write_replicates);
  });

  // Note: We shut down tservers individually rather than using
  // ClusterNodes::TS, since the latter would invalidate our reference to 'ts'.
  replica.reset();
  LOG(INFO) << "Shutting down the cluster...";
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    cluster_->mini_tablet_server(i)->Shutdown();
  }

  // Now prevent elections to reduce consensus logging on the server.
  LOG(INFO) << "Restarting single tablet server...";
  ASSERT_OK(ts->Restart());
  TServerDetails* ts_details = FindOrDie(ts_map_, ts->uuid());

  // Despite there being no writes, there are no-ops, with which we can advance
  // MVCC's timestamps.
  ASSERT_OK(WaitUntilTabletRunning(ts_details, tablet_id, MonoDelta::FromSeconds(30)));
  replica = tablet_replica_on_ts(kTserver);
  Timestamp cleantime = replica->tablet()->mvcc_manager()->GetCleanTimestamp();
  ASSERT_NE(cleantime, Timestamp::kInitialTimestamp);
}

// Test to ensure that MVCC's current snapshot gets updated via Raft no-ops, in
// both the "normal" case and the single-replica case.
TEST_F(TimestampAdvancementITest, TestTimestampsAdvancedFromRaftNoOp) {
  const int kTserver = 0;
  for (int num_replicas : { 1, 3 }) {
    LOG(INFO) << strings::Substitute("Running with $0 replicas", num_replicas);
    NO_FATALS(StartCluster(num_replicas));

    // Create an empty tablet with a single replica.
    TestWorkload create_tablet(cluster_.get());
    create_tablet.set_num_replicas(num_replicas);
    create_tablet.Setup();
    scoped_refptr<TabletReplica> replica = tablet_replica_on_ts(kTserver);

    // Despite there not being any writes, the replica will eventually bump its
    // MVCC clean time on its own when a leader gets elected and replicates a
    // no-op message.
    ASSERT_EVENTUALLY([&] {
      ASSERT_NE(replica->tablet()->mvcc_manager()->GetCleanTimestamp(),
                Timestamp::kInitialTimestamp);
    });
    replica.reset();
    StopCluster();
  }
}

}  // namespace itest
}  // namespace kudu
