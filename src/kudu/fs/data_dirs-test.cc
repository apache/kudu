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

#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <gflags/gflags_declare.h>

#include "kudu/fs/fs.pb.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/gutil/map-util.h"

using std::set;
using std::string;
using std::vector;
using strings::Substitute;

DECLARE_int32(fs_data_dirs_full_disk_cache_seconds);
DECLARE_int64(disk_reserved_bytes_free_for_testing);
DECLARE_int64(fs_data_dirs_reserved_bytes);
DECLARE_int32(fs_target_data_dirs_per_tablet);

METRIC_DECLARE_gauge_uint64(data_dirs_failed);

namespace kudu {
namespace fs {

using internal::DataDirGroup;

static const char* kDirNamePrefix = "test_data_dir";
static const int kNumDirs = 10;

class DataDirGroupTest : public KuduTest {
 public:
  DataDirGroupTest() :
      test_tablet_name_("test_tablet"),
      test_block_opts_(CreateBlockOptions({ test_tablet_name_ })),
      entity_(METRIC_ENTITY_server.Instantiate(&registry_, "test")) {}

  virtual void SetUp() override {
    KuduTest::SetUp();
    FLAGS_fs_target_data_dirs_per_tablet = kNumDirs / 2 + 1;
    DataDirManagerOptions opts;
    opts.metric_entity = entity_;
    ASSERT_OK(DataDirManager::CreateNew(env_, GetDirNames(kNumDirs), opts, &dd_manager_));
  }

 protected:
  vector<string> GetDirNames(int num_dirs) {
    vector<string> ret;
    for (int i = 0; i < num_dirs; i++) {
      string dir_name = Substitute("$0-$1", kDirNamePrefix, i);
      ret.push_back(GetTestPath(dir_name));
      CHECK_OK(env_->CreateDir(ret[i]));
    }
    return ret;
  }

  const string test_tablet_name_;
  const CreateBlockOptions test_block_opts_;
  MetricRegistry registry_;
  scoped_refptr<MetricEntity> entity_;
  std::unique_ptr<DataDirManager> dd_manager_;
};

TEST_F(DataDirGroupTest, TestCreateGroup) {
  // Test that the DataDirManager doesn't know about the tablets we're about
  // to insert.
  DataDir* dd = nullptr;
  Status s = dd_manager_->GetNextDataDir(test_block_opts_, &dd);
  ASSERT_EQ(nullptr, dd);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Tried to get directory but no directory group "
                                    "registered for tablet");

  DataDirGroupPB orig_pb;
  ASSERT_OK(dd_manager_->CreateDataDirGroup(test_tablet_name_));
  ASSERT_TRUE(dd_manager_->GetDataDirGroupPB(test_tablet_name_, &orig_pb));

  // Ensure that the DataDirManager will not create a group for a tablet that
  // it already knows about.
  s = dd_manager_->CreateDataDirGroup(test_tablet_name_);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Tried to create directory group for tablet "
                                    "but one is already registered");
  DataDirGroupPB pb;
  ASSERT_TRUE(dd_manager_->GetDataDirGroupPB(test_tablet_name_, &pb));

  // Verify that the data directory is unchanged after failing to create an
  // existing tablet.
  for (int i = 0; i < pb.uuids().size(); i++) {
    ASSERT_EQ(orig_pb.uuids(i), pb.uuids(i));
  }
  ASSERT_EQ(orig_pb.uuids().size(), pb.uuids().size());

  // Check that the tablet's DataDirGroup spans the right number of dirs.
  int num_dirs_with_tablets = 0;
  for (const auto& e: dd_manager_->tablets_by_uuid_idx_map_) {
    if (!e.second.empty()) {
      ASSERT_EQ(1, e.second.size());
      num_dirs_with_tablets++;
    }
  }
  ASSERT_EQ(FLAGS_fs_target_data_dirs_per_tablet, num_dirs_with_tablets);

  // Try to use the group.
  ASSERT_OK(dd_manager_->GetNextDataDir(test_block_opts_, &dd));
  ASSERT_FALSE(dd->is_full());
}

TEST_F(DataDirGroupTest, TestLoadFromPB) {
  // Create a PB, delete the group, then load the group from the PB.
  DataDirGroupPB orig_pb;
  ASSERT_OK(dd_manager_->CreateDataDirGroup(test_tablet_name_));
  ASSERT_TRUE(dd_manager_->GetDataDirGroupPB(test_tablet_name_, &orig_pb));
  dd_manager_->DeleteDataDirGroup(test_tablet_name_);
  ASSERT_OK(dd_manager_->LoadDataDirGroupFromPB(test_tablet_name_, orig_pb));

  // Check that the tablet's DataDirGroup spans the right number of dirs.
  int num_dirs_with_tablets = 0;
  for (const auto& e: dd_manager_->tablets_by_uuid_idx_map_) {
    if (!e.second.empty()) {
      ASSERT_EQ(1, e.second.size());
      num_dirs_with_tablets++;
    }
  }
  ASSERT_EQ(FLAGS_fs_target_data_dirs_per_tablet, num_dirs_with_tablets);

  // Ensure that loading from a PB will fail if the DataDirManager already
  // knows about the tablet.
  Status s = dd_manager_->LoadDataDirGroupFromPB(test_tablet_name_, orig_pb);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Tried to load directory group for tablet but "
                                    "one is already registered");
}

TEST_F(DataDirGroupTest, TestDeleteDataDirGroup) {
  ASSERT_OK(dd_manager_->CreateDataDirGroup(test_tablet_name_));
  DataDir* dd;
  ASSERT_OK(dd_manager_->GetNextDataDir(test_block_opts_, &dd));
  ASSERT_FALSE(dd->is_full());
  dd_manager_->DeleteDataDirGroup(test_tablet_name_);
  Status s = dd_manager_->GetNextDataDir(test_block_opts_, &dd);
  ASSERT_FALSE(dd->is_full());
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Tried to get directory but no directory group "
                                    "registered for tablet");
}

TEST_F(DataDirGroupTest, TestFullDisk) {
  FLAGS_fs_data_dirs_full_disk_cache_seconds = 0;       // Don't cache device fullness.
  FLAGS_fs_data_dirs_reserved_bytes = 1;                // Reserved space.
  FLAGS_disk_reserved_bytes_free_for_testing = 0;       // Free space.

  Status s = dd_manager_->CreateDataDirGroup(test_tablet_name_);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "All healthy data directories are full");
}

TEST_F(DataDirGroupTest, TestFailedDirNotReturned) {
  FLAGS_fs_target_data_dirs_per_tablet = 2;
  ASSERT_OK(dd_manager_->CreateDataDirGroup(test_tablet_name_));
  DataDir* dd;
  DataDir* failed_dd;
  uint16_t uuid_idx;
  // Fail one of the directories in the group and verify that it is not used.
  ASSERT_OK(dd_manager_->GetNextDataDir(test_block_opts_, &failed_dd));
  ASSERT_TRUE(dd_manager_->FindUuidIndexByDataDir(failed_dd, &uuid_idx));
  // These calls are idempotent.
  dd_manager_->MarkDataDirFailed(uuid_idx);
  dd_manager_->MarkDataDirFailed(uuid_idx);
  dd_manager_->MarkDataDirFailed(uuid_idx);
  ASSERT_EQ(1, down_cast<AtomicGauge<uint64_t>*>(
        entity_->FindOrNull(METRIC_data_dirs_failed).get())->value());
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(dd_manager_->GetNextDataDir(test_block_opts_, &dd));
    ASSERT_NE(dd, failed_dd);
  }

  // Fail the other directory and verify that neither will be used.
  ASSERT_TRUE(dd_manager_->FindUuidIndexByDataDir(dd, &uuid_idx));
  dd_manager_->MarkDataDirFailed(uuid_idx);
  ASSERT_EQ(2, down_cast<AtomicGauge<uint64_t>*>(
        entity_->FindOrNull(METRIC_data_dirs_failed).get())->value());
  Status s = dd_manager_->GetNextDataDir(test_block_opts_, &dd);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STR_CONTAINS(s.ToString(), "No healthy directories exist in tablet's directory group");
}

TEST_F(DataDirGroupTest, TestFailedDirNotAddedToGroup) {
  // Fail one dir and create a group with all directories. The failed directory
  // shouldn't be in the group.
  FLAGS_fs_target_data_dirs_per_tablet = kNumDirs;
  dd_manager_->MarkDataDirFailed(0);
  ASSERT_EQ(1, down_cast<AtomicGauge<uint64_t>*>(
        entity_->FindOrNull(METRIC_data_dirs_failed).get())->value());
  ASSERT_OK(dd_manager_->CreateDataDirGroup(test_tablet_name_));
  DataDirGroupPB pb;
  ASSERT_TRUE(dd_manager_->GetDataDirGroupPB(test_tablet_name_, &pb));
  ASSERT_EQ(kNumDirs - 1, pb.uuids_size());

  // Check that all uuid_indices are valid and are not in the failed directory
  // (uuid_idx 0).
  for (const string& uuid : pb.uuids()) {
    uint16_t* uuid_idx = FindOrNull(dd_manager_->idx_by_uuid_, uuid);
    ASSERT_NE(nullptr, uuid_idx);
    ASSERT_NE(0, *uuid_idx);
  }
  dd_manager_->DeleteDataDirGroup(test_tablet_name_);

  for (uint16_t i = 1; i < kNumDirs; i++) {
    dd_manager_->MarkDataDirFailed(i);
  }
  ASSERT_EQ(kNumDirs, down_cast<AtomicGauge<uint64_t>*>(
        entity_->FindOrNull(METRIC_data_dirs_failed).get())->value());
  Status s = dd_manager_->CreateDataDirGroup(test_tablet_name_);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STR_CONTAINS(s.ToString(), "No healthy data directories available");
}

TEST_F(DataDirGroupTest, TestLoadBalancingDistribution) {
  FLAGS_fs_target_data_dirs_per_tablet = 3;
  const double kNumTablets = 20;

  // Add 'kNumTablets' tablets, each with groups of size
  // 'FLAGS_fs_target_data_dirs_per_tablet'.
  for (int tablet_idx = 0; tablet_idx < kNumTablets; tablet_idx++) {
    ASSERT_OK(dd_manager_->CreateDataDirGroup(Substitute("$0-$1", test_tablet_name_, tablet_idx)));
  }
  const double kMeanTabletsPerDir = kNumTablets * FLAGS_fs_target_data_dirs_per_tablet / kNumDirs;

  // Calculate the standard deviation of the number of tablets per disk.
  // If tablets are evenly spread across directories, this should be small.
  double sum_squared_dev = 0;
  for (const auto& e : dd_manager_->tablets_by_uuid_idx_map_) {
    LOG(INFO) << Substitute("$0 is storing data from $1 tablets.",
        dd_manager_->data_dir_by_uuid_idx_[e.first]->dir(), e.second.size());
    double deviation = static_cast<double>(e.second.size()) - kMeanTabletsPerDir;
    sum_squared_dev += deviation * deviation;
  }
  double stddev = sqrt(sum_squared_dev / kNumDirs);
  LOG(INFO) << Substitute("$0 tablets stored across $1 directories.", kNumTablets, kNumDirs);

  // Looping this 1000 times yielded a couple stddev values over 2.0. A high
  // standard deviation does not necessarily reveal an error, but does indicate
  // a relatively unlikely distribution of data directory groups.
  LOG(INFO) << "Standard deviation: " << stddev;

}

TEST_F(DataDirGroupTest, TestLoadBalancingBias) {
  // Shows that block placement will tend to favor directories with less load.
  // First add a set of tablets for skew. Then add more tablets and check that
  // there's still roughly a uniform distribution.
  FLAGS_fs_target_data_dirs_per_tablet = 5;

  // Start with one data directory group that has some tablets.
  const double kTabletsPerSkewedDir = 10;
  const int kNumSkewedDirs = FLAGS_fs_target_data_dirs_per_tablet;

  // Number of tablets (pre-replication) added after the skew tablets.
  // This configuration will proceed with 10 directories, and a tablet load
  // of 20 * 5, for an mean of 10 tablets associated with each dir.
  const double kNumAdditionalTablets = 10;
  const string kSkewTabletPrefix = "skew_tablet";

  // Manually create a group for the skewed directories.
  //
  // Note: this should not happen in the wild and is used here as a way to
  // introduce some initial skew to the distribution.
  auto uuid_idx_iter = dd_manager_->tablets_by_uuid_idx_map_.begin();
  vector<uint16_t> skewed_dir_indices;
  for (int i = 0; i < kNumSkewedDirs; i++) {
    uint16_t uuid_idx = uuid_idx_iter->first;
    skewed_dir_indices.push_back(uuid_idx);
    uuid_idx_iter++;
  }

  // Add tablets to each skewed directory.
  for (int skew_tablet_idx = 0; skew_tablet_idx < kTabletsPerSkewedDir; skew_tablet_idx++) {
    string skew_tablet = Substitute("$0-$1", kSkewTabletPrefix, skew_tablet_idx);
    InsertOrDie(&dd_manager_->group_by_tablet_map_, skew_tablet, DataDirGroup(skewed_dir_indices));
    for (uint16_t uuid_idx : skewed_dir_indices) {
      InsertOrDie(&FindOrDie(dd_manager_->tablets_by_uuid_idx_map_, uuid_idx), skew_tablet);
    }
  }

  // Add the additional tablets.
  for (int tablet_idx = 0; tablet_idx < kNumAdditionalTablets; tablet_idx++) {
    ASSERT_OK(dd_manager_->CreateDataDirGroup(Substitute("$0-$1", test_tablet_name_, tablet_idx)));
  }

  // Calculate the standard deviation of the number of tablets per disk.
  double sum_squared_dev = 0;
  const double kMeanTabletsPerDir = (kTabletsPerSkewedDir * kNumSkewedDirs +
      kNumAdditionalTablets * FLAGS_fs_target_data_dirs_per_tablet) / kNumDirs;
  for (const auto& e : dd_manager_->tablets_by_uuid_idx_map_) {
    LOG(INFO) << Substitute("$0 is storing data from $1 tablets.",
        dd_manager_->data_dir_by_uuid_idx_[e.first]->dir(), e.second.size());
    double deviation = static_cast<double>(e.second.size()) - kMeanTabletsPerDir;
    sum_squared_dev += deviation * deviation;
  }
  double stddev = sqrt(sum_squared_dev / kNumDirs);

  // Looping this 5000 times yielded no stddev values higher than 5.0.
  LOG(INFO) << "Standard deviation: " << stddev;

  // Since the skewed directories start out with 10 tablets, a block-placement
  // heuristic that only takes into account tablet load would fail to add more
  // tablets to these skewed directories, as it would lead to all directories
  // having the mean, 10, tablets. Instead, the block-placement heuristic should
  // not completely ignore the initially skewed dirs.
  bool some_added_to_skewed_dirs = false;
  for (uint16_t skewed_uuid_index : skewed_dir_indices) {
    set<string>* tablets = FindOrNull(dd_manager_->tablets_by_uuid_idx_map_, skewed_uuid_index);
    ASSERT_NE(nullptr, tablets);
    if (tablets->size() > kTabletsPerSkewedDir) {
      some_added_to_skewed_dirs = true;
    }
  }
  ASSERT_TRUE(some_added_to_skewed_dirs);
}

} // namespace fs
} //namespace kudu
