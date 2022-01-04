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

#include "kudu/tserver/ts_tablet_manager.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/heartbeater.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(startup_benchmark_tablet_count_for_testing, 100,
             "Tablet count to do startup benchmark.");

DECLARE_bool(enable_leader_failure_detection);
DECLARE_int32(num_tablets_to_open_simultaneously);
DECLARE_bool(tablet_bootstrap_skip_opening_tablet_for_testing);
DECLARE_int32(tablet_metadata_load_inject_latency_ms);
DECLARE_int32(update_tablet_metrics_interval_ms);

#define ASSERT_REPORT_HAS_UPDATED_TABLET(report, tablet_id) \
  NO_FATALS(AssertReportHasUpdatedTablet(report, tablet_id))

#define ASSERT_MONOTONIC_REPORT_SEQNO(report_seqno, tablet_report) \
  NO_FATALS(AssertMonotonicReportSeqno(report_seqno, tablet_report))

using kudu::consensus::kInvalidOpIdIndex;
using kudu::consensus::RaftConfigPB;
using kudu::master::ReportedTabletPB;
using kudu::master::TabletReportPB;
using kudu::pb_util::SecureShortDebugString;
using kudu::tablet::LocalTabletWriter;
using kudu::tablet::Tablet;
using kudu::tablet::TabletReplica;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

class FsManager;

namespace tserver {

class TsTabletManagerTest : public KuduTest {
 public:
  TsTabletManagerTest()
    : schema_(new Schema({ ColumnSchema("key", INT32) }, 1)) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    mini_server_.reset(new MiniTabletServer(GetTestPath("TsTabletManagerTest-fsroot"),
                                            HostPort("127.0.0.1", 0)));
    ASSERT_OK(mini_server_->Start());
    mini_server_->FailHeartbeats();

    config_ = mini_server_->CreateLocalConfig();

    tablet_manager_ = mini_server_->server()->tablet_manager();
    fs_manager_ = mini_server_->server()->fs_manager();
    heartbeater_ = mini_server_->server()->heartbeater();
  }

  virtual void TearDown() OVERRIDE {
    KuduTest::TearDown();
  }

  Status CreateNewTablet(const std::string& tablet_id,
                         const SchemaPtr& schema,
                         bool wait_leader,
                         boost::optional<TableExtraConfigPB> extra_config,
                         boost::optional<std::string> dimension_label,
                         scoped_refptr<tablet::TabletReplica>* out_tablet_replica) {
    SchemaPtr full_schema_ptr = std::make_shared<Schema>(SchemaBuilder(*schema.get()).Build());
    Schema& full_schema = *full_schema_ptr;
    std::pair<PartitionSchema, Partition> partition =
        tablet::CreateDefaultPartition(full_schema);

    scoped_refptr<tablet::TabletReplica> tablet_replica;
    RETURN_NOT_OK(tablet_manager_->CreateNewTablet(tablet_id, tablet_id, partition.second,
                                                   tablet_id,
                                                   full_schema_ptr, partition.first,
                                                   config_,
                                                   std::move(extra_config),
                                                   std::move(dimension_label),
                                                   /*table_type*/boost::none,
                                                   &tablet_replica));
    if (out_tablet_replica) {
      (*out_tablet_replica) = tablet_replica;
    }

    RETURN_NOT_OK(tablet_replica->WaitUntilConsensusRunning(MonoDelta::FromMilliseconds(2000)));
    return wait_leader ? tablet_replica->consensus()->WaitUntilLeader(MonoDelta::FromSeconds(10)) :
                         Status::OK();
  }

  void GenerateFullTabletReport(TabletReportPB* report) {
    vector<TabletReportPB> reports =
        heartbeater_->GenerateFullTabletReportsForTests();
    ASSERT_EQ(1, reports.size());
    report->CopyFrom(reports[0]);
  }

  void GenerateIncrementalTabletReport(TabletReportPB* report) {
    vector<TabletReportPB> reports =
        heartbeater_->GenerateIncrementalTabletReportsForTests();
    ASSERT_EQ(1, reports.size());
    report->CopyFrom(reports[0]);
  }

  void MarkTabletReportAcknowledged(const TabletReportPB& report) {
    heartbeater_->MarkTabletReportsAcknowledgedForTests({ report });
  }

  void InsertTestRows(Tablet* tablet, int64_t count) {
    LocalTabletWriter writer(tablet, schema_.get());
    KuduPartialRow row(schema_.get());
    for (int64_t i = 0; i < count; i++) {
      ASSERT_OK(row.SetInt32(0, i));
      ASSERT_OK(writer.Insert(row));
    }
  }

 protected:
  unique_ptr<MiniTabletServer> mini_server_;
  FsManager* fs_manager_;
  TSTabletManager* tablet_manager_;
  Heartbeater* heartbeater_;

  SchemaPtr schema_;
  RaftConfigPB config_;
};

TEST_F(TsTabletManagerTest, TestCreateTablet) {
  string tablet1 = "0fffffffffffffffffffffffffffffff";
  string tablet2 = "1fffffffffffffffffffffffffffffff";
  scoped_refptr<TabletReplica> replica1;
  scoped_refptr<TabletReplica> replica2;
  TableExtraConfigPB extra_config;
  extra_config.set_history_max_age_sec(7200);

  // Create a new tablet.
  ASSERT_OK(CreateNewTablet(tablet1, schema_, true, boost::none, boost::none, &replica1));
  // Create a new tablet with extra config.
  ASSERT_OK(CreateNewTablet(tablet2, schema_, true, extra_config, boost::none, &replica2));
  ASSERT_EQ(tablet1, replica1->tablet()->tablet_id());
  ASSERT_EQ(tablet2, replica2->tablet()->tablet_id());
  ASSERT_EQ(boost::none, replica1->tablet()->metadata()->extra_config());
  ASSERT_NE(boost::none, replica2->tablet()->metadata()->extra_config());
  ASSERT_EQ(7200, replica2->tablet()->metadata()->extra_config()->history_max_age_sec());
  replica1.reset();
  replica2.reset();

  // Re-load the tablet manager from the filesystem.
  LOG(INFO) << "Shutting down tablet manager";
  mini_server_->Shutdown();
  LOG(INFO) << "Restarting tablet manager";
  mini_server_.reset(new MiniTabletServer(GetTestPath("TsTabletManagerTest-fsroot"),
                                          HostPort("127.0.0.1", 0)));
  ASSERT_OK(mini_server_->Start());
  ASSERT_OK(mini_server_->WaitStarted());
  tablet_manager_ = mini_server_->server()->tablet_manager();

  // Ensure that the tablet got re-loaded and re-opened off disk.
  ASSERT_TRUE(tablet_manager_->LookupTablet(tablet1, &replica1));
  ASSERT_TRUE(tablet_manager_->LookupTablet(tablet2, &replica2));
  ASSERT_EQ(tablet1, replica1->tablet()->tablet_id());
  ASSERT_EQ(tablet2, replica2->tablet()->tablet_id());
  ASSERT_EQ(boost::none, replica1->tablet()->metadata()->extra_config());
  ASSERT_NE(boost::none, replica2->tablet()->metadata()->extra_config());
  ASSERT_EQ(7200, replica2->tablet()->metadata()->extra_config()->history_max_age_sec());
}

static void AssertMonotonicReportSeqno(int64_t* report_seqno,
                                       const TabletReportPB &report) {
  ASSERT_LT(*report_seqno, report.sequence_number());
  *report_seqno = report.sequence_number();
}

static void AssertReportHasUpdatedTablet(const TabletReportPB& report,
                                         const string& tablet_id) {
  ASSERT_GE(report.updated_tablets_size(), 0);
  bool found_tablet = false;
  for (const ReportedTabletPB& reported_tablet : report.updated_tablets()) {
    if (reported_tablet.tablet_id() == tablet_id) {
      found_tablet = true;
      ASSERT_TRUE(reported_tablet.has_consensus_state());
      ASSERT_TRUE(reported_tablet.consensus_state().has_current_term())
          << SecureShortDebugString(reported_tablet);
      ASSERT_FALSE(reported_tablet.consensus_state().leader_uuid().empty())
          << SecureShortDebugString(reported_tablet);
      ASSERT_TRUE(reported_tablet.consensus_state().has_committed_config());
      const RaftConfigPB& committed_config = reported_tablet.consensus_state().committed_config();
      ASSERT_EQ(kInvalidOpIdIndex, committed_config.opid_index());
      ASSERT_EQ(1, committed_config.peers_size());
      ASSERT_TRUE(committed_config.peers(0).has_permanent_uuid())
          << SecureShortDebugString(reported_tablet);
      ASSERT_EQ(committed_config.peers(0).permanent_uuid(),
                reported_tablet.consensus_state().leader_uuid())
          << SecureShortDebugString(reported_tablet);
    }
  }
  ASSERT_TRUE(found_tablet);
}

TEST_F(TsTabletManagerTest, TestTabletReports) {
  TabletReportPB report;
  int64_t seqno = -1;

  // Generate a tablet report before any tablets are loaded. Should be empty.
  GenerateFullTabletReport(&report);
  ASSERT_FALSE(report.is_incremental());
  ASSERT_EQ(0, report.updated_tablets().size());
  ASSERT_MONOTONIC_REPORT_SEQNO(&seqno, report);
  MarkTabletReportAcknowledged(report);

  // Another report should now be incremental, but with no changes.
  GenerateIncrementalTabletReport(&report);
  ASSERT_TRUE(report.is_incremental());
  ASSERT_EQ(0, report.updated_tablets().size());
  ASSERT_MONOTONIC_REPORT_SEQNO(&seqno, report);
  MarkTabletReportAcknowledged(report);

  // Create a tablet and do another incremental report - should include the tablet.
  ASSERT_OK(CreateNewTablet("tablet-1", schema_, true, boost::none, boost::none, nullptr));
  int updated_tablets = 0;
  while (updated_tablets != 1) {
    GenerateIncrementalTabletReport(&report);
    updated_tablets = report.updated_tablets().size();
    ASSERT_TRUE(report.is_incremental());
    ASSERT_MONOTONIC_REPORT_SEQNO(&seqno, report);
  }
  ASSERT_REPORT_HAS_UPDATED_TABLET(report, "tablet-1");

  // If we don't acknowledge the report, and ask for another incremental report,
  // it should include the tablet again.
  GenerateIncrementalTabletReport(&report);
  ASSERT_TRUE(report.is_incremental());
  ASSERT_EQ(1, report.updated_tablets().size());
  ASSERT_REPORT_HAS_UPDATED_TABLET(report, "tablet-1");
  ASSERT_MONOTONIC_REPORT_SEQNO(&seqno, report);

  // Now acknowledge the last report, and further incrementals should be empty.
  MarkTabletReportAcknowledged(report);
  GenerateIncrementalTabletReport(&report);
  ASSERT_TRUE(report.is_incremental());
  ASSERT_EQ(0, report.updated_tablets().size());
  ASSERT_MONOTONIC_REPORT_SEQNO(&seqno, report);
  MarkTabletReportAcknowledged(report);

  // Create a second tablet, and ensure the incremental report shows it.
  ASSERT_OK(CreateNewTablet("tablet-2", schema_, true, boost::none, boost::none, nullptr));

  // Wait up to 10 seconds to get a tablet report from tablet-2.
  // TabletReplica does not mark tablets dirty until after it commits the
  // initial configuration change, so there is also a window for tablet-1 to
  // have been marked dirty since the last report.
  MonoDelta timeout(MonoDelta::FromSeconds(10));
  MonoTime start(MonoTime::Now());
  report.Clear();
  while (true) {
    bool found_tablet_2 = false;
    GenerateIncrementalTabletReport(&report);
    ASSERT_TRUE(report.is_incremental()) << SecureShortDebugString(report);
    ASSERT_MONOTONIC_REPORT_SEQNO(&seqno, report) << SecureShortDebugString(report);
    for (const ReportedTabletPB& reported_tablet : report.updated_tablets()) {
      if (reported_tablet.tablet_id() == "tablet-2") {
        found_tablet_2  = true;
        break;
      }
    }
    if (found_tablet_2) break;
    MonoDelta elapsed(MonoTime::Now() - start);
    ASSERT_TRUE(elapsed < timeout)
        << "Waited too long for tablet-2 to be marked dirty: "
        << elapsed.ToString() << ". "
        << "Latest report: " << SecureShortDebugString(report);
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  MarkTabletReportAcknowledged(report);

  // Asking for a full tablet report should re-report both tablets
  GenerateFullTabletReport(&report);
  ASSERT_FALSE(report.is_incremental());
  ASSERT_EQ(2, report.updated_tablets().size());
  ASSERT_REPORT_HAS_UPDATED_TABLET(report, "tablet-1");
  ASSERT_REPORT_HAS_UPDATED_TABLET(report, "tablet-2");
  ASSERT_MONOTONIC_REPORT_SEQNO(&seqno, report);
}

TEST_F(TsTabletManagerTest, TestTabletStatsReports) {
  TabletReportPB report;
  int64_t seqno = -1;
  const int64_t kCount = 12;

  // 1. Create two tablets.
  scoped_refptr<tablet::TabletReplica> replica1;
  ASSERT_OK(CreateNewTablet("tablet-1", schema_, true, boost::none, boost::none, &replica1));
  ASSERT_OK(CreateNewTablet("tablet-2", schema_, true, boost::none, boost::none, nullptr));

  // 2. Do a full report - should include two tablets and statistics are uninitialized.
  NO_FATALS(GenerateFullTabletReport(&report));
  ASSERT_FALSE(report.is_incremental());
  ASSERT_EQ(2, report.updated_tablets().size());
  ASSERT_TRUE(report.updated_tablets(0).has_stats());
  ASSERT_TRUE(report.updated_tablets(1).has_stats());
  ASSERT_FALSE(report.updated_tablets(0).stats().has_on_disk_size());
  ASSERT_FALSE(report.updated_tablets(0).stats().has_live_row_count());
  ASSERT_FALSE(report.updated_tablets(1).stats().has_on_disk_size());
  ASSERT_FALSE(report.updated_tablets(1).stats().has_live_row_count());
  ASSERT_MONOTONIC_REPORT_SEQNO(&seqno, report);
  MarkTabletReportAcknowledged(report);

  // 3. Trigger updates to tablet statistics as soon as possible.
  tablet_manager_->SetNextUpdateTimeForTests();
  heartbeater_->TriggerASAP();

  // Do an incremental report - should include two tablets and statistics have been initialized.
  ASSERT_EVENTUALLY([&] () {
    NO_FATALS(GenerateIncrementalTabletReport(&report));
    ASSERT_TRUE(report.is_incremental());
    ASSERT_EQ(2, report.updated_tablets().size());
  });
  ASSERT_MONOTONIC_REPORT_SEQNO(&seqno, report);
  for (int i = 0; i < 2; ++i) {
    ASSERT_TRUE(report.updated_tablets(i).has_stats());
    ASSERT_GT(report.updated_tablets(i).stats().on_disk_size(), 0);
    ASSERT_EQ(0, report.updated_tablets(i).stats().live_row_count());
  }
  ASSERT_REPORT_HAS_UPDATED_TABLET(report, "tablet-1");
  ASSERT_REPORT_HAS_UPDATED_TABLET(report, "tablet-2");
  MarkTabletReportAcknowledged(report);

  // Clean the pending dirty tablets that are not acknowledged since the seqno race.
  ASSERT_EVENTUALLY([&] () {
    NO_FATALS(GenerateIncrementalTabletReport(&report));
    ASSERT_TRUE(report.is_incremental());
    MarkTabletReportAcknowledged(report);
    ASSERT_EQ(0, report.updated_tablets().size());
  });
  ASSERT_MONOTONIC_REPORT_SEQNO(&seqno, report);

  // 4. Write some test rows to 'tablet-1'.
  NO_FATALS(InsertTestRows(replica1->tablet(), kCount));

  // Trigger updates to tablet statistics as soon as possible again.
  tablet_manager_->SetNextUpdateTimeForTests();
  heartbeater_->TriggerASAP();

  // Do an incremental report - should include the tablet and check the statistics.
  ASSERT_EVENTUALLY([&] () {
    NO_FATALS(GenerateIncrementalTabletReport(&report));
    ASSERT_TRUE(report.is_incremental());
    ASSERT_EQ(1, report.updated_tablets().size());
  });
  ASSERT_MONOTONIC_REPORT_SEQNO(&seqno, report);
  ASSERT_TRUE(report.updated_tablets(0).has_stats());
  ASSERT_GT(report.updated_tablets(0).stats().on_disk_size(), 0);
  ASSERT_EQ(kCount, report.updated_tablets(0).stats().live_row_count());
  ASSERT_REPORT_HAS_UPDATED_TABLET(report, "tablet-1");
  MarkTabletReportAcknowledged(report);
}

TEST_F(TsTabletManagerTest, StartupBenchmark) {
  const int64_t kTabletCount = FLAGS_startup_benchmark_tablet_count_for_testing;

  FLAGS_enable_leader_failure_detection = false;

  // Mute logs, cause there are too many tablets to be created.
  FLAGS_minloglevel = 2;
  ObjectIdGenerator generator;
  for (int i = 0; i < kTabletCount; i++) {
    KLOG_EVERY_N_SECS(ERROR, 1) << Substitute("Created tablet ($0/$1 complete)", i, kTabletCount);
    ASSERT_OK(CreateNewTablet(generator.Next(), schema_, false, boost::none, boost::none, nullptr));
  }

  mini_server_->Shutdown();
  // Revert log level to see how much time cost when load tablet metadata.
  FLAGS_minloglevel = 0;
  FLAGS_tablet_bootstrap_skip_opening_tablet_for_testing = true;
  FLAGS_tablet_metadata_load_inject_latency_ms = 2;
  ASSERT_OK(mini_server_->Start());
  // Mute logs, cause there are too many tablets to be shutdown.
  FLAGS_minloglevel = 2;
}

} // namespace tserver
} // namespace kudu
