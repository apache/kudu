// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <tr1/memory>
#include <string>

#include "kudu/client/client.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/util/pstack_watcher.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

class DeleteTableTest : public KuduTest {
 public:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    cluster_.reset(new ExternalMiniCluster(opts));
    ASSERT_OK(cluster_->Start());
    KuduClientBuilder builder;
    ASSERT_OK(cluster_->CreateClient(builder, &client_));
  }

  virtual void TearDown() OVERRIDE {
    if (HasFatalFailure()) {
      for (int i = 0; i < 3; i++) {
        WARN_NOT_OK(PstackWatcher::DumpPidStacks(cluster_->tablet_server(i)->pid()),
            "Couldn't dump stacks");
      }
    }
    cluster_->Shutdown();
    KuduTest::TearDown();
  }

 protected:
  int CountFilesInDir(const string& path);

  int CountReplicasInMetadataDirs();
  Status CheckNoData();
  void WaitForNoData();
  void WaitForReplicaCount(int expected);
  void WaitForAllTSToCrash();

  // Delete the given table. If the operation times out, dumps the master stacks
  // to help debug master-side deadlocks.
  void DeleteTable(const string& table_name);

  gscoped_ptr<ExternalMiniCluster> cluster_;
  shared_ptr<KuduClient> client_;
};

int DeleteTableTest::CountFilesInDir(const string& path) {
  int count = 0;
  vector<string> entries;
  CHECK_OK(env_->GetChildren(path, &entries));
  BOOST_FOREACH(const string& e, entries) {
    if (e != "." && e != "..") {
      count++;
    }
  }
  return count;
}

int DeleteTableTest::CountReplicasInMetadataDirs() {
  // Rather than using FsManager's functionality for listing blocks, we just manually
  // list the contents of the metadata directory. This is because we're using an
  // external minicluster, and initializing a new FsManager to point at the running
  // tablet servers isn't easy.
  int count = 0;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    string data_dir = cluster_->tablet_server(i)->data_dir();
    count += CountFilesInDir(JoinPathSegments(data_dir, FsManager::kTabletMetadataDirName));
  }
  return count;
}

Status DeleteTableTest::CheckNoData() {
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    string data_dir = cluster_->tablet_server(i)->data_dir();
    if (CountFilesInDir(JoinPathSegments(data_dir, FsManager::kTabletMetadataDirName)) > 0) {
      return Status::IllegalState("tablet metadata blocks still exist", data_dir);
    }
    if (CountFilesInDir(JoinPathSegments(data_dir, FsManager::kWalDirName)) > 0) {
      return Status::IllegalState("wals still exist", data_dir);
    }
    if (CountFilesInDir(JoinPathSegments(data_dir, FsManager::kConsensusMetadataDirName)) > 0) {
      return Status::IllegalState("consensus metadata still exists", data_dir);
    }
  }
  return Status::OK();;
}

void DeleteTableTest::WaitForNoData() {
  Status s;
  for (int i = 0; i < 1000; i++) {
    s = CheckNoData();
    if (s.ok()) return;
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  ASSERT_OK(s);
}

void DeleteTableTest::WaitForReplicaCount(int expected) {
  Status s;
  for (int i = 0; i < 1000; i++) {
    if (CountReplicasInMetadataDirs() == expected) return;
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  ASSERT_EQ(expected, CountReplicasInMetadataDirs());
}

void DeleteTableTest::DeleteTable(const string& table_name) {
  Status s = client_->DeleteTable(table_name);
  if (s.IsTimedOut()) {
    WARN_NOT_OK(PstackWatcher::DumpPidStacks(cluster_->master()->pid()),
                        "Couldn't dump stacks");
  }
  ASSERT_OK(s);
}

void DeleteTableTest::WaitForAllTSToCrash() {
  for (int i = 0; i < 1000; i++) {
    bool any_alive = false;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      if (cluster_->tablet_server(i)->IsProcessAlive()) {
        any_alive = true;
      }
    }
    if (!any_alive) return;
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  FAIL() << "Not all tablet servers crashed!";
}

TEST_F(DeleteTableTest, TestDeleteEmptyTable) {
  // Create a table on the cluster. We're just using TestWorkload
  // as a convenient way to create it.
  TestWorkload(cluster_.get()).Setup();

  // The table should have replicas on all three tservers.
  WaitForReplicaCount(3);

  // Delete it and wait for the replicas to get deleted.
  NO_FATALS(DeleteTable(TestWorkload::kDefaultTableName));
  NO_FATALS(WaitForNoData());
}

TEST_F(DeleteTableTest, TestDeleteTableWithConcurrentWrites) {
  int n_iters = AllowSlowTests() ? 20 : 1;
  for (int i = 0; i < n_iters; i++) {
    TestWorkload workload(cluster_.get());
    workload.set_table_name(Substitute("table-$0", i));

    // We'll delete the table underneath the writers, so we expcted
    // a NotFound error during the writes.
    workload.set_not_found_allowed(true);
    workload.Setup();

    // Start the workload, and wait to see some rows actually inserted
    workload.Start();
    while (workload.rows_inserted() < 100) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }

    // Delete it and wait for the replicas to get deleted.
    NO_FATALS(DeleteTable(workload.table_name()));
    NO_FATALS(WaitForNoData());

    // Sleep just a little longer to make sure client threads send
    // requests to the missing tablets.
    SleepFor(MonoDelta::FromMilliseconds(50));

    workload.StopAndJoin();
    cluster_->AssertNoCrashes();
  }
}

class DeleteTableParameterizedTest : public DeleteTableTest,
                                     public ::testing::WithParamInterface<const char*> {
};

// Test that if a server crashes mid-delete that the delete will be rolled
// forward on startup. Parameterized by different fault flags that cause a
// crash at various points.
TEST_P(DeleteTableParameterizedTest, TestRollForwardDelete) {
  const string fault_flag = GetParam();
  LOG(INFO) << "Running with fault flag: " << fault_flag;

  // Dynamically set the fault flag so they crash when DeleteTablet() is called
  // by the Master.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(i), fault_flag, "1.0"));
  }

  // Create a table on the cluster. We're just using TestWorkload
  // as a convenient way to create it.
  TestWorkload(cluster_.get()).Setup();

  // The table should have replicas on all three tservers.
  WaitForReplicaCount(3);

  // Delete it and wait for the tablet servers to crash.
  NO_FATALS(DeleteTable(TestWorkload::kDefaultTableName));
  NO_FATALS(WaitForAllTSToCrash());

  // There should still be data left on disk.
  Status s = CheckNoData();
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();

  // Now restart the tablet servers. They should roll forward their deletes.
  // We don't have to reset the fault flag here because it was set dynamically.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    cluster_->tablet_server(i)->Shutdown();
    ASSERT_OK(cluster_->tablet_server(i)->Restart());
  }
  NO_FATALS(WaitForNoData());
}

const char* flags[] = {"fault_crash_after_blocks_deleted",
                       "fault_crash_after_wal_deleted",
                       "fault_crash_after_cmeta_deleted"};

INSTANTIATE_TEST_CASE_P(FaultFlags, DeleteTableParameterizedTest, ::testing::ValuesIn(flags));

} // namespace kudu
