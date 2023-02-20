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

#include "kudu/fs/log_block_manager.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <initializer_list>
#include <memory>
#include <optional>
#include <ostream>
#include <random>
#include <set>
#include <string>
#include <type_traits>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/dir_manager.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_report.h"
#include "kudu/fs/log_block_manager-test-util.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/atomic.h"
#include "kudu/util/env.h"
#include "kudu/util/file_cache.h"
#include "kudu/util/metrics.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h" // IWYU pragma: keep
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/threadpool.h"

using kudu::pb_util::ReadablePBContainerFile;
using std::set;
using std::string;
using std::shared_ptr;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::SkipEmpty;
using strings::Split;
using strings::Substitute;

DECLARE_bool(cache_force_single_shard);
DECLARE_bool(crash_on_eio);
DECLARE_double(env_inject_eio);
DECLARE_double(log_container_excess_space_before_cleanup_fraction);
DECLARE_double(log_container_live_metadata_before_compact_ratio);
DECLARE_int32(fs_target_data_dirs_per_tablet);
DECLARE_int64(log_container_max_blocks);
DECLARE_string(block_manager);
DECLARE_string(block_manager_preflush_control);
DECLARE_string(env_inject_eio_globs);
DECLARE_uint64(log_container_preallocate_bytes);
DECLARE_uint64(log_container_max_size);
DECLARE_uint64(log_container_metadata_max_size);
DECLARE_bool(log_container_metadata_runtime_compact);
DECLARE_double(log_container_metadata_size_before_compact_ratio);
DEFINE_int32(startup_benchmark_batch_count_for_testing, 1000,
             "Container count to do startup benchmark.");
DEFINE_int32(startup_benchmark_block_count_per_batch_for_testing, 1000,
             "Block count to do startup benchmark.");
DEFINE_int32(startup_benchmark_data_dir_count_for_testing, 8,
             "Data directories to do startup benchmark.");
DEFINE_int32(startup_benchmark_reopen_times, 10,
             "Block manager reopen times.");
DEFINE_double(startup_benchmark_deleted_block_percentage, 90.0,
              "Percentage of deleted blocks in containers.");
DEFINE_validator(startup_benchmark_deleted_block_percentage,
                 [](const char* /*n*/, double v) { return 0 <= v && v <= 100; });
DECLARE_bool(encrypt_data_at_rest);
DECLARE_uint64(fs_max_thread_count_per_data_dir);

// Block manager metrics.
METRIC_DECLARE_counter(block_manager_total_blocks_deleted);

// Log block manager metrics.
METRIC_DECLARE_gauge_uint64(log_block_manager_bytes_under_management);
METRIC_DECLARE_gauge_uint64(log_block_manager_blocks_under_management);
METRIC_DECLARE_counter(log_block_manager_holes_punched);
METRIC_DECLARE_gauge_uint64(log_block_manager_containers);
METRIC_DECLARE_gauge_uint64(log_block_manager_full_containers);
METRIC_DECLARE_gauge_uint64(log_block_manager_dead_containers_deleted);

namespace kudu {
namespace fs {

namespace internal {
class LogBlockContainer;
} // namespace internal

class LogBlockManagerTest : public KuduTest,
                            public ::testing::WithParamInterface<
                                std::tuple<bool, string>> {
 public:
  LogBlockManagerTest() :
      test_tablet_name_("test_tablet"),
      test_block_opts_({ test_tablet_name_ }),
      // Use a small file cache (smaller than the number of containers).
      //
      // Not strictly necessary except for TestDeleteFromContainerAfterMetadataCompaction.
      file_cache_("test_cache", env_, 50, scoped_refptr<MetricEntity>()) {
    SetEncryptionFlags(std::get<0>(GetParam()));
    FLAGS_block_manager = std::get<1>(GetParam());
    CHECK_OK(file_cache_.Init());
  }

  void SetUp() override {
    bm_.reset(CreateBlockManager(scoped_refptr<MetricEntity>()));

    // Pass in a report to prevent the block manager from logging unnecessarily.
    FsReport report;
    ASSERT_OK(bm_->Open(&report, nullptr, nullptr));
    ASSERT_OK(dd_manager_->CreateDataDirGroup(test_tablet_name_));
    ASSERT_OK(dd_manager_->GetDataDirGroupPB(test_tablet_name_, &test_group_pb_));
  }

  Status DeleteMetadataFromRdb(const string& dir, const string& container_name) {
    Dir* pdir = dd_manager_->FindDirByRoot(dir);
    CHECK(pdir);

    rocksdb::Slice begin_key = container_name;
    string next_container_name = ObjectIdGenerator::NextOf(container_name);
    rocksdb::Slice end_key = next_container_name;
    auto s = pdir->rdb()->DeleteRange(
        rocksdb::WriteOptions(), pdir->rdb()->DefaultColumnFamily(), begin_key, end_key);
    CHECK_OK(FromRdbStatus(s));
    return Status::OK();
  }

  int MetadataEntriesCount(const string& dir, const string& container_name) {
    Dir* pdir = dd_manager_->FindDirByRoot(dir);
    CHECK(pdir);

    rocksdb::Slice begin_key = container_name;
    int count = 0;
    unique_ptr<rocksdb::Iterator> it(
        pdir->rdb()->NewIterator(rocksdb::ReadOptions(), pdir->rdb()->DefaultColumnFamily()));
    it->Seek(begin_key);
    while (it->Valid() && it->key().starts_with(begin_key)) {
      count++;
      it->Next();
    }
    CHECK_OK(FromRdbStatus(it->status()));
    return count;
  }

 protected:
  LogBlockManager* CreateBlockManager(const scoped_refptr<MetricEntity>& metric_entity,
                                      vector<string> test_data_dirs = {}) {
    PrepareDataDirs(&test_data_dirs);
    if (!dd_manager_) {
      // Ensure the directory manager is initialized.
      CHECK_OK(DataDirManager::CreateNewForTests(env_, test_data_dirs,
          DataDirManagerOptions(), &dd_manager_));
    }

    BlockManagerOptions opts;
    opts.metric_entity = metric_entity;
    if (FLAGS_block_manager == "log") {
      return new LogBlockManagerNativeMeta(
          env_, dd_manager_.get(), &error_manager_, &file_cache_, std::move(opts));
    } else {
      CHECK_EQ(FLAGS_block_manager, "logr");
      return new LogrBlockManager(
          env_, dd_manager_.get(), &error_manager_, &file_cache_, std::move(opts));
    }
  }

  Status ReopenBlockManager(const scoped_refptr<MetricEntity>& metric_entity = nullptr,
                            FsReport* report = nullptr,
                            vector<string> test_data_dirs = {},
                            bool force = false) {
    PrepareDataDirs(&test_data_dirs);

    // The directory manager must outlive the block manager. Destroy the block
    // manager first to enforce this.
    bm_.reset();

    if (force) {
      // Ensure the directory manager is initialized.
      CHECK_OK(DataDirManager::CreateNewForTests(env_, test_data_dirs,
          DataDirManagerOptions(), &dd_manager_));
      RETURN_NOT_OK(dd_manager_->CreateDataDirGroup(test_tablet_name_));
      RETURN_NOT_OK(dd_manager_->GetDataDirGroupPB(test_tablet_name_, &test_group_pb_));
    } else {
      // Re-open the directory manager first to clear any in-memory maps.
      RETURN_NOT_OK(DataDirManager::OpenExistingForTests(env_, test_data_dirs,
                                                         DataDirManagerOptions(), &dd_manager_));
      RETURN_NOT_OK(dd_manager_->LoadDataDirGroupFromPB(test_tablet_name_, test_group_pb_));
    }

    bm_.reset(CreateBlockManager(metric_entity, test_data_dirs));
    RETURN_NOT_OK(bm_->Open(report, nullptr, nullptr));
    return Status::OK();
  }

  // Returns the only container data file in the test directory. Yields an
  // assert failure if more than one is found.
  void GetOnlyContainerDataFile(string* data_file) {
    vector<string> data_files;
    DoGetContainers(DATA_FILES, &data_files);
    ASSERT_EQ(1, data_files.size());
    *data_file = data_files[0];
  }

  // Returns the only container metadata file in the test directory. Yields an
  // assert failure if more than one is found.
  void GetOnlyContainerMetadataFile(string* metadata_file) {
    vector<string> metadata_files;
    DoGetContainers(METADATA_FILES, &metadata_files);
    ASSERT_EQ(1, metadata_files.size());
    *metadata_file = metadata_files[0];
  }

  void GetContainerMetadataFiles(vector<string>* metadata_files) {
    DoGetContainers(METADATA_FILES, metadata_files);
  }

  // Like GetOnlyContainerDataFile(), but returns a container name (i.e. data
  // or metadata file with the file suffix removed).
  void GetOnlyContainer(string* container) {
    vector<string> containers;
    DoGetContainers(CONTAINER_NAMES, &containers);
    ASSERT_EQ(1, containers.size());
    *container = containers[0];
  }

  // Returns the names of all of the containers found in the test directory.
  void GetContainerNames(vector<string>* container_names) {
    DoGetContainers(CONTAINER_NAMES, container_names);
  }

  // Asserts that 'expected_num_containers' are found in the test directory.
  void AssertNumContainers(int expected_num_containers) {
    vector<string> containers;
    DoGetContainers(CONTAINER_NAMES, &containers);
    ASSERT_EQ(expected_num_containers, containers.size());
  }

  // Asserts that 'report' contains no inconsistencies.
  void AssertEmptyReport(const FsReport& report) {
    ASSERT_TRUE(report.full_container_space_check->entries.empty());
    ASSERT_TRUE(report.incomplete_container_check->entries.empty());
    ASSERT_TRUE(report.malformed_record_check->entries.empty());
    ASSERT_TRUE(report.misaligned_block_check->entries.empty());
    ASSERT_TRUE(report.partial_record_check->entries.empty());
    ASSERT_TRUE(report.partial_rdb_record_check->entries.empty());
  }

  DataDirGroupPB test_group_pb_;
  string test_tablet_name_;
  CreateBlockOptions test_block_opts_;

  unique_ptr<DataDirManager> dd_manager_;
  FsErrorManager error_manager_;
  FileCache file_cache_;
  unique_ptr<LogBlockManager> bm_;

 private:
  enum GetMode {
    DATA_FILES,
    METADATA_FILES,
    CONTAINER_NAMES,
  };
  void DoGetContainers(GetMode mode, vector<string>* out) {
    // Populate 'data_files' and 'metadata_files'.
    vector<string> data_files;
    vector<string> metadata_files;
    for (const string& data_dir : dd_manager_->GetDirs()) {
      vector<string> children;
      ASSERT_OK(env_->GetChildren(data_dir, &children));
      for (const string& child : children) {
        if (HasSuffixString(child, LogBlockManager::kContainerDataFileSuffix)) {
          data_files.push_back(JoinPathSegments(data_dir, child));
        } else if (HasSuffixString(child, LogBlockManager::kContainerMetadataFileSuffix)) {
          metadata_files.push_back(JoinPathSegments(data_dir, child));
        }
      }
    }

    switch (mode) {
      case DATA_FILES:
        *out = std::move(data_files);
        break;
      case METADATA_FILES:
        *out = std::move(metadata_files);
        break;
      case CONTAINER_NAMES:
        // Build the union of 'data_files' and 'metadata_files' with suffixes
        // stripped.
        unordered_set<string> container_names;
        for (const auto& df : data_files) {
          string c;
          ASSERT_TRUE(TryStripSuffixString(
              df, LogBlockManager::kContainerDataFileSuffix, &c));
          container_names.emplace(std::move(c));
        }
        for (const auto& mdf : metadata_files) {
          string c;
          ASSERT_TRUE(TryStripSuffixString(
              mdf, LogBlockManager::kContainerMetadataFileSuffix, &c));
          container_names.emplace(std::move(c));
        }
        out->assign(container_names.begin(), container_names.end());
        break;
    }
  }

  void PrepareDataDirs(vector<string>* test_data_dirs) {
    if (test_data_dirs->empty()) {
      *test_data_dirs = { test_dir_ };
    }
    for (const auto& test_data_dir : *test_data_dirs) {
      Status s = Env::Default()->CreateDir(test_data_dir);
      CHECK(s.IsAlreadyPresent() || s.ok())
          << "Could not create directory " << test_data_dir << ": " << s.ToString();
    }
  }
};

static void CheckGaugeMetric(const scoped_refptr<MetricEntity>& entity,
                             int expected_value, const MetricPrototype* prototype) {
  AtomicGauge<uint64_t>* gauge = down_cast<AtomicGauge<uint64_t>*>(
      entity->FindOrNull(*prototype).get());
  DCHECK(gauge);
  ASSERT_EQ(expected_value, gauge->value()) << prototype->name();
}

static void CheckCounterMetric(const scoped_refptr<MetricEntity>& entity,
                               int expected_value, const MetricPrototype* prototype) {
  Counter* counter = down_cast<Counter*>(entity->FindOrNull(*prototype).get());
  DCHECK(counter);
  ASSERT_EQ(expected_value, counter->value()) << prototype->name();
}

static void CheckLogMetrics(const scoped_refptr<MetricEntity>& entity,
                            const vector<std::pair<int, const MetricPrototype*>> gauge_values,
                            const vector<std::pair<int, const MetricPrototype*>> counter_values) {
  for (const auto& gauge_value : gauge_values) {
    NO_FATALS(CheckGaugeMetric(entity, gauge_value.first, gauge_value.second));
  }
  for (const auto& counter_value: counter_values) {
    NO_FATALS(CheckCounterMetric(entity, counter_value.first, counter_value.second));
  }
}

INSTANTIATE_TEST_SUITE_P(EncryptionEnabled, LogBlockManagerTest,
                         ::testing::Combine(
                             ::testing::Values(false, true),
                             ::testing::Values("log", "logr")));

TEST_P(LogBlockManagerTest, MetricsTest) {
  MetricRegistry registry;
  scoped_refptr<MetricEntity> entity = METRIC_ENTITY_server.Instantiate(&registry, "test");
  ASSERT_OK(ReopenBlockManager(entity));
  NO_FATALS(CheckLogMetrics(entity,
      { {0, &METRIC_log_block_manager_bytes_under_management},
        {0, &METRIC_log_block_manager_blocks_under_management},
        {0, &METRIC_log_block_manager_containers},
        {0, &METRIC_log_block_manager_full_containers} },
      { {0, &METRIC_log_block_manager_holes_punched},
        {0, &METRIC_block_manager_total_blocks_deleted},
        {0, &METRIC_log_block_manager_dead_containers_deleted} }));

  // Lower the max container size so that we can more easily test full
  // container metrics.
  // TODO(abukor): If this is 1024, this becomes full when writing the first
  // block because of alignments. If it is over 4k, it fails with encryption
  // disabled due to having only 5 containers instead of 10. Investigate this.
  FLAGS_log_container_max_size = std::get<0>(GetParam()) ? 8192 : 1024;

  // One block --> one container.
  unique_ptr<WritableBlock> writer;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
  NO_FATALS(CheckLogMetrics(entity,
      { {0, &METRIC_log_block_manager_bytes_under_management},
        {0, &METRIC_log_block_manager_blocks_under_management},
        {1, &METRIC_log_block_manager_containers},
        {0, &METRIC_log_block_manager_full_containers} },
      { {0, &METRIC_log_block_manager_holes_punched},
        {0, &METRIC_block_manager_total_blocks_deleted},
        {0, &METRIC_log_block_manager_dead_containers_deleted} }));

  // And when the block is closed, it becomes "under management".
  ASSERT_OK(writer->Close());
  NO_FATALS(CheckLogMetrics(entity,
      { {0, &METRIC_log_block_manager_bytes_under_management},
        {1, &METRIC_log_block_manager_blocks_under_management},
        {1, &METRIC_log_block_manager_containers},
        {0, &METRIC_log_block_manager_full_containers} },
      { {0, &METRIC_log_block_manager_holes_punched},
        {0, &METRIC_block_manager_total_blocks_deleted},
        {0, &METRIC_log_block_manager_dead_containers_deleted} }));

  // Create 10 blocks concurrently. We reuse the existing container and
  // create 9 new ones. All of them get filled.
  BlockId saved_id;
  {
    uint8_t data[1024];
    Random rand(SeedRandom());
    unique_ptr<BlockCreationTransaction> transaction = bm_->NewCreationTransaction();
    for (int i = 0; i < 10; i++) {
      unique_ptr<WritableBlock> b;
      ASSERT_OK(bm_->CreateBlock(test_block_opts_, &b));
      if (saved_id.IsNull()) {
        saved_id = b->id();
      }
      for (int j = 0; j < sizeof(data); j += sizeof(uint32_t)) {
        data[j] = rand.Next();
      }
      b->Append(Slice(data, sizeof(data)));
      ASSERT_OK(b->Finalize());
      transaction->AddCreatedBlock(std::move(b));
    }
    // Metrics for full containers are updated after Finalize().
    NO_FATALS(CheckLogMetrics(entity,
        { {0, &METRIC_log_block_manager_bytes_under_management},
          {1, &METRIC_log_block_manager_blocks_under_management},
          {10, &METRIC_log_block_manager_containers},
          {10, &METRIC_log_block_manager_full_containers} },
        { {0, &METRIC_log_block_manager_holes_punched},
          {0, &METRIC_block_manager_total_blocks_deleted},
          {0, &METRIC_log_block_manager_dead_containers_deleted} }));

    ASSERT_OK(transaction->CommitCreatedBlocks());
    NO_FATALS(CheckLogMetrics(entity,
        { {10 * 1024, &METRIC_log_block_manager_bytes_under_management},
          {11, &METRIC_log_block_manager_blocks_under_management},
          {10, &METRIC_log_block_manager_containers},
          {10, &METRIC_log_block_manager_full_containers} },
        { {0, &METRIC_log_block_manager_holes_punched},
          {0, &METRIC_block_manager_total_blocks_deleted},
          {0, &METRIC_log_block_manager_dead_containers_deleted} }));
  }

  // Reopen the block manager and test the metrics. They're all based on
  // persistent information so they should be the same.
  MetricRegistry new_registry;
  scoped_refptr<MetricEntity> new_entity = METRIC_ENTITY_server.Instantiate(&new_registry, "test");
  ASSERT_OK(ReopenBlockManager(new_entity));
  NO_FATALS(CheckLogMetrics(new_entity,
      { {10 * 1024, &METRIC_log_block_manager_bytes_under_management},
        {11, &METRIC_log_block_manager_blocks_under_management},
        {10, &METRIC_log_block_manager_containers},
        {10, &METRIC_log_block_manager_full_containers} },
      { {0, &METRIC_log_block_manager_holes_punched},
        {0, &METRIC_block_manager_total_blocks_deleted},
        {0, &METRIC_log_block_manager_dead_containers_deleted} }));

  // Delete a block. Its contents should no longer be under management.
  {
    shared_ptr<BlockDeletionTransaction> deletion_transaction =
        bm_->NewDeletionTransaction();
    deletion_transaction->AddDeletedBlock(saved_id);
    ASSERT_OK(deletion_transaction->CommitDeletedBlocks(nullptr));
    NO_FATALS(CheckLogMetrics(new_entity,
        { {9 * 1024, &METRIC_log_block_manager_bytes_under_management},
          {10, &METRIC_log_block_manager_blocks_under_management},
          {10, &METRIC_log_block_manager_containers},
          {10, &METRIC_log_block_manager_full_containers} },
        { {0, &METRIC_log_block_manager_holes_punched},
          {1, &METRIC_block_manager_total_blocks_deleted},
          {0, &METRIC_log_block_manager_dead_containers_deleted} }));
  }
  dd_manager_->WaitOnClosures();
  NO_FATALS(CheckLogMetrics(new_entity,
      { {9 * 1024, &METRIC_log_block_manager_bytes_under_management},
        {10, &METRIC_log_block_manager_blocks_under_management},
        {10, &METRIC_log_block_manager_containers},
        {10, &METRIC_log_block_manager_full_containers} },
      { {1, &METRIC_log_block_manager_holes_punched},
        {1, &METRIC_block_manager_total_blocks_deleted},
        {0, &METRIC_log_block_manager_dead_containers_deleted} }));

  // Set the max container size to default so that we can create a bunch of blocks
  // in the same container. Delete those created blocks afterwards to verify only
  // one hole punch operation is executed since the blocks are contiguous.
  FLAGS_log_container_max_size = 10LU * 1024 * 1024 * 1024;
  {
    vector<BlockId> blocks;
    unique_ptr<BlockCreationTransaction> transaction = bm_->NewCreationTransaction();
    for (int i = 0; i < 10; i++) {
      unique_ptr<WritableBlock> b;
      ASSERT_OK(bm_->CreateBlock(test_block_opts_, &b));
      blocks.emplace_back(b->id());
      b->Append("test data");
      ASSERT_OK(b->Finalize());
      transaction->AddCreatedBlock(std::move(b));
    }
    ASSERT_OK(transaction->CommitCreatedBlocks());

    shared_ptr<BlockDeletionTransaction> deletion_transaction =
        bm_->NewDeletionTransaction();
    for (const auto& block : blocks) {
      deletion_transaction->AddDeletedBlock(block);
    }
    vector<BlockId> deleted;
    ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted));
    ASSERT_EQ(blocks.size(), deleted.size());
    NO_FATALS(CheckLogMetrics(new_entity,
        { {9 * 1024, &METRIC_log_block_manager_bytes_under_management},
          {10, &METRIC_log_block_manager_blocks_under_management},
          {11, &METRIC_log_block_manager_containers},
          {10, &METRIC_log_block_manager_full_containers} },
        { {1, &METRIC_log_block_manager_holes_punched},
          {11, &METRIC_block_manager_total_blocks_deleted},
          {0, &METRIC_log_block_manager_dead_containers_deleted} }));
  }
  // Wait for the actual hole punching to take place.
  dd_manager_->WaitOnClosures();
  NO_FATALS(CheckLogMetrics(new_entity,
      { {9 * 1024, &METRIC_log_block_manager_bytes_under_management},
        {10, &METRIC_log_block_manager_blocks_under_management},
        {11, &METRIC_log_block_manager_containers},
        {10, &METRIC_log_block_manager_full_containers} },
      { {2, &METRIC_log_block_manager_holes_punched},
        {11, &METRIC_block_manager_total_blocks_deleted},
        {0, &METRIC_log_block_manager_dead_containers_deleted} }));
}

TEST_P(LogBlockManagerTest, ContainerPreallocationTest) {
  string kTestData = "test data";

  // For this test to work properly, the preallocation window has to be at
  // least three times the size of the test data.
  ASSERT_GE(FLAGS_log_container_preallocate_bytes, kTestData.size() * 3);

  // Create a block with some test data. This should also trigger
  // preallocation of the container, provided it's supported by the kernel.
  unique_ptr<WritableBlock> written_block;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &written_block));
  ASSERT_OK(written_block->Append(kTestData));
  ASSERT_OK(written_block->Close());

  // We expect the container size to be equal to the preallocation amount,
  // which we know is greater than the test data size.
  string container_data_filename;
  NO_FATALS(GetOnlyContainerDataFile(&container_data_filename));
  uint64_t size;
  ASSERT_OK(env_->GetFileSizeOnDisk(container_data_filename, &size));
  ASSERT_EQ(FLAGS_log_container_preallocate_bytes, size);

  // Upon writing a second block, we'd expect the container to remain the same
  // size.
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &written_block));
  ASSERT_OK(written_block->Append(kTestData));
  ASSERT_OK(written_block->Close());
  NO_FATALS(GetOnlyContainerDataFile(&container_data_filename));
  ASSERT_OK(env_->GetFileSizeOnDisk(container_data_filename, &size));
  ASSERT_EQ(FLAGS_log_container_preallocate_bytes, size);

  // Now reopen the block manager and create another block. The block manager
  // should be smart enough to reuse the previously preallocated amount.
  ASSERT_OK(ReopenBlockManager());
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &written_block));
  ASSERT_OK(written_block->Append(kTestData));
  ASSERT_OK(written_block->Close());
  NO_FATALS(GetOnlyContainerDataFile(&container_data_filename));
  ASSERT_OK(env_->GetFileSizeOnDisk(container_data_filename, &size));
  ASSERT_EQ(FLAGS_log_container_preallocate_bytes, size);
}

// Test for KUDU-2202 to ensure that once the block manager has been notified
// of a block ID, it will not reuse it.
TEST_P(LogBlockManagerTest, TestBumpBlockIds) {
  const int kNumBlocks = 10;
  vector<BlockId> block_ids;
  unique_ptr<WritableBlock> writer;
  for (int i = 0; i < kNumBlocks; i++) {
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
    block_ids.push_back(writer->id());
  }
  BlockId max_so_far = *std::max_element(block_ids.begin(), block_ids.end());

  // Simulate a complete reset of the block manager's block ID record, e.g.
  // from restarting but with all the blocks gone.
  bm_->next_block_id_.Store(1);

  // Now simulate being notified by some other component (e.g. tablet metadata)
  // of the presence of a block ID.
  bm_->NotifyBlockId(BlockId(max_so_far));

  // Once notified, new blocks should be assigned higher IDs.
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
  ASSERT_LT(max_so_far, writer->id());
  max_so_far = writer->id();

  // Notifications of lower or invalid block IDs should not disrupt ordering.
  bm_->NotifyBlockId(BlockId(1));
  bm_->NotifyBlockId(BlockId());
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
  ASSERT_LT(max_so_far, writer->id());
}

// Regression test for KUDU-1190, a crash at startup when a block ID has been
// reused.
TEST_P(LogBlockManagerTest, TestReuseBlockIds) {
  // Typically, the LBM starts with a random block ID when running as a
  // gtest. In this test, we want to control the block IDs.
  bm_->next_block_id_.Store(1);

  vector<BlockId> block_ids;

  // Create 4 containers, with the first four block IDs in the sequence.
  {
    unique_ptr<BlockCreationTransaction> transaction = bm_->NewCreationTransaction();
    for (int i = 0; i < 4; i++) {
      unique_ptr<WritableBlock> writer;
      ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
      block_ids.push_back(writer->id());
      transaction->AddCreatedBlock(std::move(writer));
    }
    ASSERT_OK(transaction->CommitCreatedBlocks());
  }

  // Create one more block, which should reuse the first container.
  {
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
    ASSERT_OK(writer->Close());
  }

  ASSERT_EQ(4, bm_->all_containers_by_name_.size());

  // Delete the original blocks.
  {
    shared_ptr<BlockDeletionTransaction> deletion_transaction =
        bm_->NewDeletionTransaction();
    for (const BlockId& b : block_ids) {
      deletion_transaction->AddDeletedBlock(b);
    }
    ASSERT_OK(deletion_transaction->CommitDeletedBlocks(nullptr));
  }

  // Reset the block ID sequence and re-create new blocks which should reuse the same
  // block IDs. This isn't allowed in current versions of Kudu, but older versions
  // could produce this situation, and we still need to handle it on startup.
  bm_->next_block_id_.Store(1);
  for (int i = 0; i < 4; i++) {
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
    ASSERT_EQ(writer->id(), block_ids[i]);
    ASSERT_OK(writer->Close());
  }

  // Now we have 4 containers with the following metadata:
  //   1: CREATE(1) CREATE (5) DELETE(1) CREATE(4)
  //   2: CREATE(2) DELETE(2) CREATE(1)
  //   3: CREATE(3) DELETE(3) CREATE(2)
  //   4: CREATE(4) DELETE(4) CREATE(3)

  // Re-open the block manager and make sure it can deal with this case where
  // block IDs have been reused.
  ASSERT_OK(ReopenBlockManager());
}

// Test partial record at end of metadata file. See KUDU-1377.
// The idea behind this test is that we should tolerate one partial record at
// the end of a given container metadata file, since we actively append a
// record to a container metadata file when a new block is created or deleted.
// A system crash or disk-full event can result in a partially-written metadata
// record. Ignoring a trailing, partial (not corrupt) record is safe, so long
// as we only consider a container valid if there is at most one trailing
// partial record. If any other metadata record is somehow incomplete or
// corrupt, we consider that an error and the entire container is considered
// corrupted.
//
// Note that we rely on filesystem integrity to ensure that we do not lose
// trailing, fsync()ed metadata.
TEST_P(LogBlockManagerTest, TestMetadataTruncation) {
  if (FLAGS_block_manager != "log") {
    return;
  }

  // Create several blocks.
  vector<BlockId> created_blocks;
  BlockId last_block_id;
  for (int i = 0; i < 4; i++) {
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
    last_block_id = writer->id();
    created_blocks.push_back(last_block_id);
    ASSERT_OK(writer->Close());
  }
  vector<BlockId> block_ids;
  ASSERT_OK(bm_->GetAllBlockIds(&block_ids));
  ASSERT_EQ(4, block_ids.size());
  unique_ptr<ReadableBlock> block;
  ASSERT_OK(bm_->OpenBlock(last_block_id, &block));
  ASSERT_OK(block->Close());

  // Start corrupting the metadata file in different ways.

  string path = LogBlockManager::ContainerPathForTests(
      bm_->all_containers_by_name_.begin()->second.get());
  string metadata_path = path + LogBlockManager::kContainerMetadataFileSuffix;
  string data_path = path + LogBlockManager::kContainerDataFileSuffix;

  uint64_t good_meta_size;
  ASSERT_OK(env_->GetFileSize(metadata_path, &good_meta_size));

  // First, add extra null bytes to the end of the metadata file. This makes
  // the trailing "record" of the metadata file corrupt, but doesn't cause data
  // loss. The result is that the container will automatically truncate the
  // metadata file back to its correct size.
  // We'll do this with 1, 8, and 128 extra bytes-- the first case is too few
  // bytes to be a valid record, while the second is too few but is enough for
  // a data length and its checksum, and the third is too long for a record.
  // The 8- and 128-byte cases are regression tests for KUDU-2260.
  uint64_t cur_meta_size;
  for (const auto num_bytes : {1, 8, 128}) {
    {
      RWFileOptions opts;
      opts.mode = Env::MUST_EXIST;
      opts.is_sensitive = true;
      unique_ptr<RWFile> file;
      ASSERT_OK(env_->NewRWFile(opts, metadata_path, &file));
      ASSERT_OK(file->Truncate(good_meta_size + num_bytes));
    }

    ASSERT_OK(env_->GetFileSize(metadata_path, &cur_meta_size));
    ASSERT_EQ(good_meta_size + num_bytes, cur_meta_size);

    // Reopen the metadata file. We will still see all of our blocks. The size of
    // the metadata file will be restored back to its previous value.
    ASSERT_OK(ReopenBlockManager());
    ASSERT_OK(bm_->GetAllBlockIds(&block_ids));
    ASSERT_EQ(4, block_ids.size());
    ASSERT_OK(bm_->OpenBlock(last_block_id, &block));
    ASSERT_OK(block->Close());

    // Check that the file was truncated back to its previous size by the system.
    ASSERT_OK(env_->GetFileSize(metadata_path, &cur_meta_size));
    ASSERT_EQ(good_meta_size, cur_meta_size);
  }

  // Delete the first block we created. This necessitates writing to the
  // metadata file of the originally-written container, since we append a
  // delete record to the metadata.
  {
    shared_ptr<BlockDeletionTransaction> deletion_transaction =
        bm_->NewDeletionTransaction();
    deletion_transaction->AddDeletedBlock(created_blocks[0]);
    ASSERT_OK(deletion_transaction->CommitDeletedBlocks(nullptr));
  }
  ASSERT_OK(bm_->GetAllBlockIds(&block_ids));
  ASSERT_EQ(3, block_ids.size());

  ASSERT_OK(env_->GetFileSize(metadata_path, &cur_meta_size));
  good_meta_size = cur_meta_size;

  // Add a new block, increasing the size of the container metadata file.
  {
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
    last_block_id = writer->id();
    created_blocks.push_back(last_block_id);
    ASSERT_OK(writer->Close());
  }
  ASSERT_OK(bm_->GetAllBlockIds(&block_ids));
  ASSERT_EQ(4, block_ids.size());
  ASSERT_OK(env_->GetFileSize(metadata_path, &cur_meta_size));
  ASSERT_GT(cur_meta_size, good_meta_size);
  uint64_t prev_good_meta_size = good_meta_size; // Store previous size.
  good_meta_size = cur_meta_size;

  // Now, truncate the metadata file so that we lose the last valid record.
  // This will result in the loss of a block record, therefore we will observe
  // data loss, however it will look like a failed partial write.
  {
    RWFileOptions opts;
    opts.mode = Env::MUST_EXIST;
    opts.is_sensitive = true;
    unique_ptr<RWFile> file;
    ASSERT_OK(env_->NewRWFile(opts, metadata_path, &file));
    ASSERT_OK(file->Truncate(good_meta_size - 1));
  }

  // Reopen the truncated metadata file. We will not find all of our blocks.
  ASSERT_OK(ReopenBlockManager());

  // Because the last record was a partial record on disk, the system should
  // have assumed that it was an incomplete write and truncated the metadata
  // file back to the previous valid record. Let's verify that that's the case.
  good_meta_size = prev_good_meta_size;
  ASSERT_OK(env_->GetFileSize(metadata_path, &cur_meta_size));
  ASSERT_EQ(good_meta_size, cur_meta_size);

  ASSERT_OK(bm_->GetAllBlockIds(&block_ids));
  ASSERT_EQ(3, block_ids.size());
  Status s = bm_->OpenBlock(last_block_id, &block);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Can't find block");

  // Add a new block, increasing the size of the container metadata file.
  {
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
    last_block_id = writer->id();
    created_blocks.push_back(last_block_id);
    ASSERT_OK(writer->Close());
  }

  ASSERT_OK(bm_->GetAllBlockIds(&block_ids));
  ASSERT_EQ(4, block_ids.size());
  ASSERT_OK(bm_->OpenBlock(last_block_id, &block));
  ASSERT_OK(block->Close());

  ASSERT_OK(env_->GetFileSize(metadata_path, &cur_meta_size));
  ASSERT_GT(cur_meta_size, good_meta_size);
  good_meta_size = cur_meta_size;

  // Ensure that we only ever created a single container.
  ASSERT_EQ(1, bm_->all_containers_by_name_.size());
  ASSERT_EQ(1, bm_->available_containers_by_data_dir_.size());
  ASSERT_EQ(1, bm_->available_containers_by_data_dir_.begin()->second.size());

  // Find location of 2nd record in metadata file and corrupt it.
  // This is an unrecoverable error because it's in the middle of the file.
  unique_ptr<RandomAccessFile> meta_file;
  RandomAccessFileOptions raf_opts;
  raf_opts.is_sensitive = true;
  ASSERT_OK(env_->NewRandomAccessFile(raf_opts, metadata_path, &meta_file));
  ReadablePBContainerFile pb_reader(std::move(meta_file));
  ASSERT_OK(pb_reader.Open());
  BlockRecordPB record;
  ASSERT_OK(pb_reader.ReadNextPB(&record));
  uint64_t offset = pb_reader.offset();

  uint64_t latest_meta_size;
  ASSERT_OK(env_->GetFileSize(metadata_path, &latest_meta_size));
  ASSERT_OK(env_->NewRandomAccessFile(raf_opts, metadata_path, &meta_file));
  latest_meta_size -= meta_file->GetEncryptionHeaderSize();
  unique_ptr<uint8_t[]> scratch(new uint8_t[latest_meta_size]);
  Slice result(scratch.get(), latest_meta_size);
  ASSERT_OK(meta_file->Read(meta_file->GetEncryptionHeaderSize(), result));
  string data = result.ToString();
  // Flip the high bit of the length field, which is a 4-byte little endian
  // unsigned integer. This will cause the length field to represent a large
  // value and also cause the length checksum not to validate.
  data[offset + 3] ^= 1 << 7;
  unique_ptr<WritableFile> writable_file;
  WritableFileOptions wf_opts;
  wf_opts.is_sensitive = true;
  ASSERT_OK(env_->NewWritableFile(wf_opts, metadata_path, &writable_file));
  ASSERT_OK(writable_file->Append(data));
  ASSERT_OK(writable_file->Close());

  // Now try to reopen the container.
  // This should look like a bad checksum, and it's not recoverable.
  s = ReopenBlockManager();
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Incorrect checksum");

  // Now truncate both the data and metadata files.
  // This should be recoverable. See KUDU-668.
  ASSERT_OK(env_->NewWritableFile(wf_opts, metadata_path, &writable_file));
  ASSERT_OK(writable_file->Close());
  ASSERT_OK(env_->NewWritableFile(wf_opts, data_path, &writable_file));
  ASSERT_OK(writable_file->Close());

  ASSERT_OK(ReopenBlockManager());
}

// Regression test for a crash when a container's append offset exceeded its
// preallocation offset.
TEST_P(LogBlockManagerTest, TestAppendExceedsPreallocation) {
  FLAGS_log_container_preallocate_bytes = 1;

  // Create a container, preallocate it by one byte, and append more than one.
  unique_ptr<WritableBlock> writer;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
  ASSERT_OK(writer->Append("hello world"));
  ASSERT_OK(writer->Close());

  // On second append, don't crash just because the append offset is ahead of
  // the preallocation offset!
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
  ASSERT_OK(writer->Append("hello world"));
}

TEST_P(LogBlockManagerTest, TestPreallocationAndTruncation) {
  // Ensure preallocation window is greater than the container size itself.
  FLAGS_log_container_max_size = 1024 * 1024;
  FLAGS_log_container_preallocate_bytes = 32 * 1024 * 1024;

  // Fill up one container.
  unique_ptr<WritableBlock> writer;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
  unique_ptr<uint8_t[]> data(new uint8_t[FLAGS_log_container_max_size]);
  memset(data.get(), 0, FLAGS_log_container_max_size);
  ASSERT_OK(writer->Append({ data.get(), FLAGS_log_container_max_size } ));
  string fname;
  NO_FATALS(GetOnlyContainerDataFile(&fname));
  uint64_t size_after_append;
  ASSERT_OK(env_->GetFileSizeOnDisk(fname, &size_after_append));
  ASSERT_EQ(FLAGS_log_container_preallocate_bytes, size_after_append);

  // Close it. The extra preallocated space should be truncated off the file.
  ASSERT_OK(writer->Close());
  uint64_t size_after_close;
  ASSERT_OK(env_->GetFileSizeOnDisk(fname, &size_after_close));
  ASSERT_GE(size_after_close, FLAGS_log_container_max_size);
  ASSERT_LT(size_after_close, size_after_append);

  // Now test the same startup behavior by artificially growing the file
  // and reopening the block manager.
  //
  // Try preallocating in two ways: once with a change to the file size and
  // once without. The second way serves as a proxy for XFS's speculative
  // preallocation behavior, described in KUDU-1856.
  for (RWFile::PreAllocateMode mode : {RWFile::CHANGE_FILE_SIZE,
                                       RWFile::DONT_CHANGE_FILE_SIZE}) {
    LOG(INFO) << "Pass " << mode;
    unique_ptr<RWFile> data_file;
    RWFileOptions opts;
    opts.mode = Env::MUST_EXIST;
    ASSERT_OK(env_->NewRWFile(opts, fname, &data_file));
    opts.is_sensitive = true;
    ASSERT_OK(data_file->PreAllocate(size_after_close, size_after_close, mode));
    uint64_t size_after_preallocate;
    ASSERT_OK(env_->GetFileSizeOnDisk(fname, &size_after_preallocate));
    ASSERT_EQ(size_after_close * 2, size_after_preallocate);

    if (mode == RWFile::DONT_CHANGE_FILE_SIZE) {
      // Some older versions of ext4 (such as on el6) do not appear to truncate
      // unwritten preallocated space that extends beyond the file size. Let's
      // coax them by writing a single byte into that space.
      //
      // Note: this doesn't invalidate the usefulness of this test, as it's
      // quite possible for us to have written a little bit of data into XFS's
      // speculative preallocated area.
      ASSERT_OK(data_file->Write(size_after_close, "a"));
    }

    // Now reopen the block manager. It should notice that the container grew
    // and truncate the extra preallocated space off again.
    ASSERT_OK(ReopenBlockManager());
    uint64_t size_after_reopen;
    ASSERT_OK(env_->GetFileSizeOnDisk(fname, &size_after_reopen));
    ASSERT_EQ(size_after_close, size_after_reopen);
  }
}

TEST_P(LogBlockManagerTest, TestContainerWithManyHoles) {
  // This is a regression test of sorts for KUDU-1508, though it doesn't
  // actually fail if the fix is missing; it just corrupts the filesystem.

  static unordered_map<int, int> block_size_to_last_interior_node_block_number =
     {{1024, 168},
      {2048, 338},
      {4096, 680}};

  const int kNumBlocks = 16 * 1024;

  uint64_t fs_block_size;
  ASSERT_OK(env_->GetBlockSize(test_dir_, &fs_block_size));
  if (!ContainsKey(block_size_to_last_interior_node_block_number,
                   fs_block_size)) {
    LOG(INFO) << Substitute("Filesystem block size is $0, skipping test",
                            fs_block_size);
    return;
  }
  int last_interior_node_block_number = FindOrDie(
      block_size_to_last_interior_node_block_number, fs_block_size);

  ASSERT_GE(kNumBlocks, last_interior_node_block_number);

  // Create a bunch of blocks. They should all go in one container (unless
  // the container becomes full).
  LOG(INFO) << Substitute("Creating $0 blocks", kNumBlocks);
  vector<BlockId> ids;
  for (int i = 0; i < kNumBlocks; i++) {
    unique_ptr<WritableBlock> block;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
    ASSERT_OK(block->Append("aaaa"));
    ASSERT_OK(block->Close());
    ids.push_back(block->id());
  }

  // Delete every other block. In effect, this maximizes the number of extents
  // in the container by forcing the filesystem to alternate every hole with
  // a live extent.
  LOG(INFO) << "Deleting every other block";
  shared_ptr<BlockDeletionTransaction> deletion_transaction =
      this->bm_->NewDeletionTransaction();
  for (int i = 0; i < ids.size(); i += 2) {
    deletion_transaction->AddDeletedBlock(ids[i]);
  }
  ASSERT_OK(deletion_transaction->CommitDeletedBlocks(nullptr));

  // Delete all of the blocks belonging to the interior node. If KUDU-1508
  // applies, this should corrupt the filesystem.
  LOG(INFO) << Substitute("Deleting remaining blocks up to block number $0",
                          last_interior_node_block_number);
  for (int i = 1; i < last_interior_node_block_number; i += 2) {
    deletion_transaction->AddDeletedBlock(ids[i]);
  }
  ASSERT_OK(deletion_transaction->CommitDeletedBlocks(nullptr));
}

TEST_P(LogBlockManagerTest, TestParseKernelRelease) {
  ASSERT_TRUE(LogBlockManager::IsBuggyEl6Kernel("1.7.0.0.el6.x86_64"));

  // no el6 infix
  ASSERT_FALSE(LogBlockManager::IsBuggyEl6Kernel("2.6.32"));

  ASSERT_TRUE(LogBlockManager::IsBuggyEl6Kernel("2.6.32-1.0.0.el6.x86_64"));
  ASSERT_FALSE(LogBlockManager::IsBuggyEl6Kernel("2.6.33-1.0.0.el6.x86_64"));

  // Make sure it's a numeric sort, not a lexicographic one.
  ASSERT_FALSE(LogBlockManager::IsBuggyEl6Kernel("2.6.32-1000.0.0.el6.x86_64"));
  ASSERT_FALSE(LogBlockManager::IsBuggyEl6Kernel("2.6.100-1.0.0.el6.x86_64"));
  ASSERT_FALSE(LogBlockManager::IsBuggyEl6Kernel("2.10.0-1.0.0.el6.x86_64"));
  ASSERT_FALSE(LogBlockManager::IsBuggyEl6Kernel("10.0.0-1.0.0.el6.x86_64"));

  // Kernels from el6.6, el6.7: buggy
  ASSERT_TRUE(LogBlockManager::IsBuggyEl6Kernel("2.6.32-504.30.3.el6.x86_64"));
  ASSERT_TRUE(LogBlockManager::IsBuggyEl6Kernel("2.6.32-573.el6.x86_64"));
  ASSERT_TRUE(LogBlockManager::IsBuggyEl6Kernel("2.6.32-573.1.1.el6.x86_64"));

  // Kernel from el6.8: buggy
  ASSERT_TRUE(LogBlockManager::IsBuggyEl6Kernel("2.6.32-642.el6.x86_64"));

  // Kernels from el6.8 update stream before a fix was applied: buggy.
  ASSERT_TRUE(LogBlockManager::IsBuggyEl6Kernel("2.6.32-642.11.1.el6.x86_64"));
  ASSERT_TRUE(LogBlockManager::IsBuggyEl6Kernel("2.6.32-642.14.1.el6.x86_64"));
  ASSERT_TRUE(LogBlockManager::IsBuggyEl6Kernel("2.6.32-642.14.2.el6.x86_64"));

  // Kernels from el6.8 update stream after a fix was applied: not buggy.
  ASSERT_FALSE(LogBlockManager::IsBuggyEl6Kernel("2.6.32-642.15.1.el6.x86_64"));
  ASSERT_FALSE(LogBlockManager::IsBuggyEl6Kernel("2.6.32-642.18.1.el6.x86_64"));

  // Kernel from el6.9 development prior to fix: buggy.
  ASSERT_TRUE(LogBlockManager::IsBuggyEl6Kernel("2.6.32-673.0.0.el6.x86_64"));

  // Kernel from el6.9 development post-fix: not buggy.
  ASSERT_FALSE(LogBlockManager::IsBuggyEl6Kernel("2.6.32-674.0.0.el6.x86_64"));
}

#ifdef NDEBUG

// Simple micro-benchmark which creates a large number of blocks and then
// times the startup of the LBM.
//
// This is simplistic in several ways compared to two typical workloads:
// 1. minimal number of containers, each of which is entirely full
//    without any deleted blocks.
//   (typical workloads end up writing to several containers at once
//    due to concurrent write operations such as multiple MM threads
//    flushing)
// 2. minimal number of containers, each of which is entirely full
//    with about --startup_benchmark_deleted_block_percentage percent
//    deleted blocks.
//   (typical workloads of write, alter operations, and background MM
//    threads running a long time since last bootstrap)
//
// However it still can be used to micro-optimize the startup process.
TEST_P(LogBlockManagerTest, StartupBenchmark) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Disable preflushing since this can slow down our writes. In particular,
  // since we write such small blocks in this test, each block will likely
  // begin on the same 4KB page as the prior one we wrote, and due to the
  // "stable page writes" feature, each block will thus end up waiting
  // on the writeback of the prior one.
  //
  // See http://yoshinorimatsunobu.blogspot.com/2014/03/how-syncfilerange-really-works.html
  // for details.
  FLAGS_block_manager_preflush_control = "never";
  vector<string> test_dirs;
  {
    SCOPED_LOG_TIMING(INFO, "init environment");
    for (int i = 0; i < FLAGS_startup_benchmark_data_dir_count_for_testing; ++i) {
      test_dirs.emplace_back(test_dir_ + "/" + std::to_string(i));
    }
    // Re-open block manager to place data on multiple data directories.
    ASSERT_OK(ReopenBlockManager(nullptr, nullptr, test_dirs, /* force= */ true));
  }

  // Creates FLAGS_startup_benchmark_batch_count_for_testing *
  //     FLAGS_startup_benchmark_block_count_per_batch_for_testing blocks with minimal data.
  vector<BlockId> block_ids;
  block_ids.reserve(FLAGS_startup_benchmark_batch_count_for_testing *
                    FLAGS_startup_benchmark_block_count_per_batch_for_testing);
  {
    SCOPED_LOG_TIMING(INFO, "create blocks");
    for (int i = 0; i < FLAGS_startup_benchmark_batch_count_for_testing; i++) {
      unique_ptr<BlockCreationTransaction> transaction = bm_->NewCreationTransaction();
      for (int j = 0; j < FLAGS_startup_benchmark_block_count_per_batch_for_testing; j++) {
        unique_ptr<WritableBlock> block;
        ASSERT_OK_FAST(bm_->CreateBlock(test_block_opts_, &block));
        ASSERT_OK_FAST(block->Append("x"));
        ASSERT_OK_FAST(block->Finalize());
        block_ids.emplace_back(block->id());
        transaction->AddCreatedBlock(std::move(block));
      }
      ASSERT_OK(transaction->CommitCreatedBlocks());
    }
  }

  int to_delete_count = block_ids.size() * FLAGS_startup_benchmark_deleted_block_percentage / 100;
  if (to_delete_count > 0) {
    std::mt19937 gen(SeedRandom());
    std::shuffle(block_ids.begin(), block_ids.end(), gen);

    SCOPED_LOG_TIMING(INFO, "delete blocks");
    int j = 0;
    for (int i = 0; i < FLAGS_startup_benchmark_batch_count_for_testing; i++) {
      int to_delete_count_per_batch =
          to_delete_count / FLAGS_startup_benchmark_batch_count_for_testing;
      shared_ptr<BlockDeletionTransaction> deletion_transaction =
          this->bm_->NewDeletionTransaction();
      for (; j < block_ids.size(); j++) {
        deletion_transaction->AddDeletedBlock(block_ids[j]);
        if (--to_delete_count_per_batch <= 0) {
          break;
        }
      }
      ASSERT_OK(deletion_transaction->CommitDeletedBlocks(nullptr));
    }
  }

  // The deleted blocks need to to be hole punched when shutdown block manager, this procedure may
  // cost a very long time. We shutdown the block manager manually before restart it, then we can
  // get a more accurate startup time.
  {
    SCOPED_LOG_TIMING(INFO, "shutdown block manager");
    bm_.reset();
  }

  for (int i = 0; i < FLAGS_startup_benchmark_reopen_times; i++) {
    SCOPED_LOG_TIMING(INFO, "reopening block manager");
    ASSERT_OK(ReopenBlockManager(nullptr, nullptr, test_dirs));
  }
  LOG(INFO) << "Test on --block_manager=" << FLAGS_block_manager;
}
#endif

TEST_P(LogBlockManagerTest, TestFailMultipleTransactionsPerContainer) {
  // Create multiple transactions that will share a container.
  const int kNumTransactions = 3;
  vector<unique_ptr<BlockCreationTransaction>> block_transactions;
  for (int i = 0; i < kNumTransactions; i++) {
    block_transactions.emplace_back(bm_->NewCreationTransaction());
  }

  // Repeatedly add new blocks for the transactions. Finalizing each block
  // makes the block's container available, allowing the same container to be
  // reused by the next block.
  const int kNumBlocks = 10;
  for (int i = 0; i < kNumBlocks; i++) {
    unique_ptr<WritableBlock> block;
    ASSERT_OK_FAST(bm_->CreateBlock(test_block_opts_, &block));
    ASSERT_OK_FAST(block->Append("x"));
    ASSERT_OK_FAST(block->Finalize());
    block_transactions[i % kNumTransactions]->AddCreatedBlock(std::move(block));
  }
  ASSERT_EQ(1, bm_->all_containers_by_name_.size());

  // Briefly inject an error while committing one of the transactions. This
  // should make the container read-only, preventing the remaining transactions
  // from proceeding.
  {
    google::FlagSaver saver;
    FLAGS_crash_on_eio = false;
    FLAGS_env_inject_eio = 1.0;
    Status s = block_transactions[0]->CommitCreatedBlocks();
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
  }

  // Now try to add some more blocks.
  for (int i = 0; i < kNumTransactions; i++) {
    unique_ptr<WritableBlock> block;
    ASSERT_OK_FAST(bm_->CreateBlock(test_block_opts_, &block));

    // The first write will fail, as the container has been marked read-only.
    // This will leave the container unavailable and force the creation of a
    // new container.
    Status s = block->Append("x");
    if (i == 0) {
      ASSERT_TRUE(s.IsIOError()) << s.ToString();
    } else {
      ASSERT_OK_FAST(s);
      ASSERT_OK_FAST(block->Finalize());
    }
    block_transactions[i]->AddCreatedBlock(std::move(block));
  }
  ASSERT_EQ(2, bm_->all_containers_by_name_.size());

  // At this point, all of the transactions have blocks in read-only containers
  // and, thus, will be unable to commit.
  for (const auto& block_transaction : block_transactions) {
    ASSERT_TRUE(block_transaction->CommitCreatedBlocks().IsIOError());
  }
}

TEST_P(LogBlockManagerTest, TestLookupBlockLimit) {
  int64_t limit_1024 = LogBlockManager::LookupBlockLimit(1024);
  int64_t limit_2048 = LogBlockManager::LookupBlockLimit(2048);
  int64_t limit_4096 = LogBlockManager::LookupBlockLimit(4096);

  // Test the floor behavior in LookupBlockLimit().
  for (int i = 0; i < 16384; i++) {
    if (i < 2048) {
      ASSERT_EQ(limit_1024, LogBlockManager::LookupBlockLimit(i));
    } else if (i < 4096) {
      ASSERT_EQ(limit_2048, LogBlockManager::LookupBlockLimit(i));
    } else {
      ASSERT_EQ(limit_4096, LogBlockManager::LookupBlockLimit(i));
    }
  }
}

TEST_P(LogBlockManagerTest, TestContainerBlockLimitingByBlockNum) {
  const int kNumBlocks = 1000;

  // Creates 'kNumBlocks' blocks with minimal data.
  auto create_some_blocks = [&]() {
    for (int i = 0; i < kNumBlocks; i++) {
      unique_ptr<WritableBlock> block;
      RETURN_NOT_OK(bm_->CreateBlock(test_block_opts_, &block));
      RETURN_NOT_OK(block->Append("aaaa"));
      RETURN_NOT_OK(block->Close());
    }
    return Status::OK();
  };

  // All of these blocks should fit into one container.
  ASSERT_OK(create_some_blocks());
  NO_FATALS(AssertNumContainers(1));

  // With a limit imposed, the existing container is immediately full, and we
  // need a few more to satisfy another 'kNumBlocks' blocks.
  FLAGS_log_container_max_blocks = 400;
  ASSERT_OK(ReopenBlockManager());
  ASSERT_OK(create_some_blocks());
  NO_FATALS(AssertNumContainers(4));

  // Now remove the limit and create more blocks. They should go into existing
  // containers, which are now no longer full.
  FLAGS_log_container_max_blocks = -1;
  ASSERT_OK(ReopenBlockManager());

  ASSERT_OK(create_some_blocks());
  NO_FATALS(AssertNumContainers(4));
}

TEST_P(LogBlockManagerTest, TestContainerBlockLimitingByMetadataSize) {
  if (FLAGS_block_manager != "log") {
    return;
  }

  const int kNumBlocks = 1000;

  // Creates 'kNumBlocks' blocks with minimal data.
  auto create_some_blocks = [&]() {
    for (int i = 0; i < kNumBlocks; i++) {
      unique_ptr<WritableBlock> block;
      RETURN_NOT_OK(bm_->CreateBlock(test_block_opts_, &block));
      RETURN_NOT_OK(block->Append("aaaa"));
      RETURN_NOT_OK(block->Close());
    }
    return Status::OK();
  };

  // All of these blocks should fit into one container.
  ASSERT_OK(create_some_blocks());
  NO_FATALS(AssertNumContainers(1));

  // With a limit imposed, the existing container is immediately full, and we
  // need a few more to satisfy another metadata file size.
  // Each CREATE type entry in metadata protobuf file is 39 bytes, so 400 of
  // such entries added by 'create_some_blocks' will make the container full.
  FLAGS_log_container_metadata_max_size = 400 * 39;
  ASSERT_OK(ReopenBlockManager());
  ASSERT_OK(create_some_blocks());
  NO_FATALS(AssertNumContainers(4));

  // Now remove the limit and create more blocks. They should go into existing
  // containers, which are now no longer full.
  FLAGS_log_container_metadata_max_size = 0;
  ASSERT_OK(ReopenBlockManager());

  ASSERT_OK(create_some_blocks());
  NO_FATALS(AssertNumContainers(4));
}

TEST_P(LogBlockManagerTest, TestContainerBlockLimitingByMetadataSizeWithCompaction) {
  if (FLAGS_block_manager != "log") {
    return;
  }

  const int kNumBlocks = 2000;
  const int kNumThreads = 10;
  const double kLiveBlockRatio = 0.1;

  // Creates and deletes some blocks.
  auto create_and_delete_blocks = [&]() {
    vector<BlockId> ids;
    // Creates 'kNumBlocks' blocks.
    for (int i = 0; i < kNumBlocks; i++) {
      unique_ptr<WritableBlock> block;
      RETURN_NOT_OK(bm_->CreateBlock(test_block_opts_, &block));
      RETURN_NOT_OK(block->Append("aaaa"));
      RETURN_NOT_OK(block->Close());
      ids.push_back(block->id());
    }

    // Deletes 'kNumBlocks * (1 - kLiveBlockRatio)' blocks.
    shared_ptr<BlockDeletionTransaction> deletion_transaction =
        bm_->NewDeletionTransaction();
    for (const auto& id : ids) {
      if (rand() % 100 < 100 * kLiveBlockRatio) {
        continue;
      }
      deletion_transaction->AddDeletedBlock(id);
    }
    RETURN_NOT_OK(deletion_transaction->CommitDeletedBlocks(nullptr));

    return Status::OK();
  };

  // Create a thread pool to create and delete blocks.
  unique_ptr<ThreadPool> pool;
  ASSERT_OK(ThreadPoolBuilder("test-metadata-compact-pool")
                .set_max_threads(kNumThreads)
                .Build(&pool));
  auto mt_create_and_delete_blocks = [&]() {
    for (int i = 0; i < kNumThreads; ++i) {
      ASSERT_OK(pool->Submit(create_and_delete_blocks));
    }
    pool->Wait();
    dd_manager_->WaitOnClosures();
  };

  FLAGS_log_container_metadata_runtime_compact = true;
  // Define a small value to make metadata easy to be full.
  FLAGS_log_container_metadata_max_size = 32 * 1024;
  NO_FATALS(mt_create_and_delete_blocks());
  vector<string> metadata_files;
  NO_FATALS(GetContainerMetadataFiles(&metadata_files));
  for (const auto& metadata_file : metadata_files) {
    ASSERT_EVENTUALLY([&] {
      uint64_t file_size;
      NO_FATALS(env_->GetFileSize(metadata_file, &file_size));
      ASSERT_GE(FLAGS_log_container_metadata_max_size *
                    FLAGS_log_container_metadata_size_before_compact_ratio,
                file_size);
    });
  }

  // Reopen and test again.
  ASSERT_OK(ReopenBlockManager());
  NO_FATALS(mt_create_and_delete_blocks());
  NO_FATALS(GetContainerMetadataFiles(&metadata_files));
  for (const auto& metadata_file : metadata_files) {
    ASSERT_EVENTUALLY([&] {
      uint64_t file_size;
      NO_FATALS(env_->GetFileSize(metadata_file, &file_size));
      ASSERT_GE(FLAGS_log_container_metadata_max_size *
                    FLAGS_log_container_metadata_size_before_compact_ratio,
                file_size);
    });
  }

  // Now remove the limit and create more blocks. They should go into existing
  // containers, which are now no longer full.
  FLAGS_log_container_metadata_runtime_compact = false;
  ASSERT_OK(ReopenBlockManager());
  NO_FATALS(mt_create_and_delete_blocks());
  NO_FATALS(GetContainerMetadataFiles(&metadata_files));
  bool exist_larger_one = false;
  for (const auto& metadata_file : metadata_files) {
    uint64_t file_size;
    NO_FATALS(env_->GetFileSize(metadata_file, &file_size));
    if (file_size > FLAGS_log_container_metadata_max_size *
                         FLAGS_log_container_metadata_size_before_compact_ratio) {
      exist_larger_one = true;
      break;
    }
  }
  ASSERT_TRUE(exist_larger_one);
}

TEST_P(LogBlockManagerTest, TestMisalignedBlocksFuzz) {
  FLAGS_log_container_preallocate_bytes = 0;
  const int kNumBlocks = 100;

  // Create one container.
  unique_ptr<WritableBlock> block;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
  ASSERT_OK(block->Close());
  string container_name;
  NO_FATALS(GetOnlyContainer(&container_name));

  // Add a mixture of regular and misaligned blocks to it.
  LBMCorruptor corruptor(env_, dd_manager_.get(), SeedRandom());
  ASSERT_OK(corruptor.Init());
  int num_misaligned_blocks = 0;
  for (int i = 0; i < kNumBlocks; i++) {
    if (rand() % 2) {
      ASSERT_OK(corruptor.AddMisalignedBlockToContainer());

      // Need to reopen the block manager after each corruption because the
      // container metadata writers do not expect the metadata files to have
      // been changed underneath them.
      FsReport report;
      ASSERT_OK(ReopenBlockManager(nullptr, &report));
      corruptor.ResetDataDirManager(dd_manager_.get());
      ASSERT_FALSE(report.HasFatalErrors());
      num_misaligned_blocks++;
    } else {
      unique_ptr<WritableBlock> block;
      ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));

      // Append at least once to ensure that the data file grows.
      //
      // The LBM considers the last record of a container to be malformed if
      // it's zero-length and if the file hasn't grown enough to catch up it.
      // This combination (zero-length block at the end of a full container
      // without any remaining preallocated space) is nearly impossible in real
      // life, so we avoid it in testing too.
      int num_appends = (rand() % 8) + 1;
      uint64_t raw_block_id = block->id().id();
      Slice s(reinterpret_cast<const uint8_t*>(&raw_block_id),
              sizeof(raw_block_id));
      for (int j = 0; j < num_appends; j++) {
        // The corruptor writes the block ID repeatedly into each misaligned
        // block, so we'll make our regular blocks do the same thing.
        ASSERT_OK(block->Append(s));
      }
      ASSERT_OK(block->Close());
    }
  }
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
  corruptor.ResetDataDirManager(dd_manager_.get());
  ASSERT_FALSE(report.HasFatalErrors()) << report.ToString();
  ASSERT_EQ(num_misaligned_blocks, report.misaligned_block_check->entries.size());
  for (const auto& mb : report.misaligned_block_check->entries) {
    ASSERT_EQ(container_name, mb.container);
  }

  // Delete about half of them, chosen randomly.
  vector<BlockId> block_ids;
  {
    shared_ptr<BlockDeletionTransaction> deletion_transaction =
        this->bm_->NewDeletionTransaction();
    ASSERT_OK(bm_->GetAllBlockIds(&block_ids));
    for (const auto& id : block_ids) {
      if (rand() % 2) {
        deletion_transaction->AddDeletedBlock(id);
      }
    }
    ASSERT_OK(deletion_transaction->CommitDeletedBlocks(nullptr));
  }

  // Wait for the block manager to punch out all of the holes. It's easiest to
  // do this by reopening it; shutdown will wait for outstanding hole punches.
  //
  // On reopen, some misaligned blocks should be gone from the report.
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
  corruptor.ResetDataDirManager(dd_manager_.get());
  ASSERT_FALSE(report.HasFatalErrors());
  ASSERT_GT(report.misaligned_block_check->entries.size(), 0);
  ASSERT_LT(report.misaligned_block_check->entries.size(), num_misaligned_blocks);
  for (const auto& mb : report.misaligned_block_check->entries) {
    ASSERT_EQ(container_name, mb.container);
  }

  // Read and verify the contents of each remaining block.
  ASSERT_OK(bm_->GetAllBlockIds(&block_ids));
  for (const auto& id : block_ids) {
    uint64_t raw_block_id = id.id();
    unique_ptr<ReadableBlock> b;
    ASSERT_OK(bm_->OpenBlock(id, &b));
    uint64_t size;
    ASSERT_OK(b->Size(&size));
    ASSERT_EQ(0, size % sizeof(raw_block_id));
    uint8_t buf[size];
    ASSERT_OK(b->Read(0, Slice(buf, size)));
    for (int i = 0; i < size; i += sizeof(raw_block_id)) {
      ASSERT_EQ(raw_block_id, *reinterpret_cast<uint64_t*>(buf + i));
    }
    ASSERT_OK(b->Close());
  }
}

TEST_P(LogBlockManagerTest, TestRepairPreallocateExcessSpace) {
  // Enforce that the container's actual size is strictly upper-bounded by the
  // calculated size so we can more easily trigger repairs.
  FLAGS_log_container_excess_space_before_cleanup_fraction = 0.0;

  // Disable preallocation so we can more easily control it.
  FLAGS_log_container_preallocate_bytes = 0;

  // Make it easy to create a full container.
  FLAGS_log_container_max_size = 1;

  const int kNumContainers = 10;

  // Create several full containers.
  {
    unique_ptr<BlockCreationTransaction> transaction = bm_->NewCreationTransaction();
    for (int i = 0; i < kNumContainers; i++) {
      unique_ptr<WritableBlock> block;
      ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
      ASSERT_OK(block->Append("a"));
      transaction->AddCreatedBlock(std::move(block));
    }
    ASSERT_OK(transaction->CommitCreatedBlocks());
  }
  vector<string> container_names;
  NO_FATALS(GetContainerNames(&container_names));

  // Corrupt one container.
  LBMCorruptor corruptor(env_, dd_manager_.get(), SeedRandom());
  ASSERT_OK(corruptor.Init());
  ASSERT_OK(corruptor.PreallocateFullContainer());

  // Check the report.
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
  corruptor.ResetDataDirManager(dd_manager_.get());
  ASSERT_FALSE(report.HasFatalErrors());
  ASSERT_EQ(1, report.full_container_space_check->entries.size());
  const LBMFullContainerSpaceCheck::Entry& fcs =
      report.full_container_space_check->entries[0];
  unordered_set<string> container_name_set(container_names.begin(),
                                           container_names.end());
  ASSERT_TRUE(ContainsKey(container_name_set, fcs.container));
  ASSERT_GT(fcs.excess_bytes, 0);
  ASSERT_TRUE(fcs.repaired);
  report.full_container_space_check->entries.clear();
  NO_FATALS(AssertEmptyReport(report));
}

TEST_P(LogBlockManagerTest, TestRepairUnpunchedBlocks) {
  const int kNumBlocks = 100;

  // Enforce that the container's actual size is strictly upper-bounded by the
  // calculated size so we can more easily trigger repairs.
  FLAGS_log_container_excess_space_before_cleanup_fraction = 0.0;

  // Force our single container to become full once created.
  FLAGS_log_container_max_size = std::get<0>(GetParam()) ? 4096 : 0;

  // Force the test to measure extra space in unpunched holes, not in the
  // preallocation buffer.
  FLAGS_log_container_preallocate_bytes = 0;

  // Create one container.
  unique_ptr<WritableBlock> block;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
  ASSERT_OK(block->Append("a"));
  ASSERT_OK(block->Close());
  string data_file;
  NO_FATALS(GetOnlyContainerDataFile(&data_file));
  uint64_t initial_file_size_on_disk;
  ASSERT_OK(env_->GetFileSizeOnDisk(data_file, &initial_file_size_on_disk));

  // Add some "unpunched blocks" to the container.
  LBMCorruptor corruptor(env_, dd_manager_.get(), SeedRandom());
  ASSERT_OK(corruptor.Init());
  for (int i = 0; i < kNumBlocks; i++) {
    ASSERT_OK(corruptor.AddUnpunchedBlockToFullContainer());
  }

  uint64_t file_size_on_disk;
  ASSERT_OK(env_->GetFileSizeOnDisk(data_file, &file_size_on_disk));
  ASSERT_GT(file_size_on_disk, initial_file_size_on_disk);

  // Check the report.
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
  corruptor.ResetDataDirManager(dd_manager_.get());
  ASSERT_FALSE(report.HasFatalErrors());
  ASSERT_EQ(1, report.full_container_space_check->entries.size());
  const LBMFullContainerSpaceCheck::Entry& fcs =
      report.full_container_space_check->entries[0];
  string container;
  NO_FATALS(GetOnlyContainer(&container));
  ASSERT_EQ(container, fcs.container);
  ASSERT_EQ(file_size_on_disk, fcs.excess_bytes + initial_file_size_on_disk);
  ASSERT_TRUE(fcs.repaired);
  report.full_container_space_check->entries.clear();
  NO_FATALS(AssertEmptyReport(report));

  // Wait for the block manager to punch out all of the holes (done as part of
  // repair at startup). It's easiest to do this by reopening it; shutdown will
  // wait for outstanding hole punches.
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
  NO_FATALS(AssertEmptyReport(report));

  // File size should be 0 post-repair.
  ASSERT_OK(env_->GetFileSizeOnDisk(data_file, &file_size_on_disk));
  ASSERT_EQ(initial_file_size_on_disk, file_size_on_disk);
}

TEST_P(LogBlockManagerTest, TestRepairIncompleteContainer) {
  const int kNumContainers = 20;

  // Create some incomplete containers. The corruptor will select between
  // several variants of "incompleteness" at random (see
  // LBMCorruptor::CreateIncompleteContainer() for details).
  LBMCorruptor corruptor(env_, dd_manager_.get(), SeedRandom());
  ASSERT_OK(corruptor.Init());
  for (int i = 0; i < kNumContainers; i++) {
    ASSERT_OK(corruptor.CreateIncompleteContainer());
  }
  vector<string> container_names;
  NO_FATALS(GetContainerNames(&container_names));
  ASSERT_EQ(kNumContainers, container_names.size());

  // Check the report.
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
  corruptor.ResetDataDirManager(dd_manager_.get());
  ASSERT_FALSE(report.HasFatalErrors());
  ASSERT_EQ(kNumContainers, report.incomplete_container_check->entries.size());
  unordered_set<string> container_name_set(container_names.begin(),
                                           container_names.end());
  for (const auto& ic : report.incomplete_container_check->entries) {
    ASSERT_TRUE(ContainsKey(container_name_set, ic.container));
    ASSERT_TRUE(ic.repaired);
  }
  report.incomplete_container_check->entries.clear();
  NO_FATALS(AssertEmptyReport(report));
}

TEST_P(LogBlockManagerTest, TestDetectMalformedRecords) {
  const int kNumRecords = 50;

  // Create one container.
  unique_ptr<WritableBlock> block;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
  ASSERT_OK(block->Append("a"));
  ASSERT_OK(block->Close());
  string container_name;
  NO_FATALS(GetOnlyContainer(&container_name));

  // Add some malformed records. The corruptor will select between
  // several variants of "malformedness" at random (see
  // LBMCorruptor::AddMalformedRecordToContainer for details).
  LBMCorruptor corruptor(env_, dd_manager_.get(), SeedRandom());
  ASSERT_OK(corruptor.Init());
  for (int i = 0; i < kNumRecords; i++) {
    ASSERT_OK(corruptor.AddMalformedRecordToContainer());
  }

  // Check the report.
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
  corruptor.ResetDataDirManager(dd_manager_.get());
  ASSERT_TRUE(report.HasFatalErrors());
  ASSERT_EQ(kNumRecords, report.malformed_record_check->entries.size());
  for (const auto& mr : report.malformed_record_check->entries) {
    ASSERT_EQ(container_name, mr.container);
  }
  report.malformed_record_check->entries.clear();
  NO_FATALS(AssertEmptyReport(report));
}

TEST_P(LogBlockManagerTest, TestDetectMisalignedBlocks) {
  const int kNumBlocks = 50;

  // Create one container.
  unique_ptr<WritableBlock> block;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
  ASSERT_OK(block->Append("a"));
  ASSERT_OK(block->Close());
  string container_name;
  NO_FATALS(GetOnlyContainer(&container_name));

  // Add some misaligned blocks.
  LBMCorruptor corruptor(env_, dd_manager_.get(), SeedRandom());
  ASSERT_OK(corruptor.Init());
  for (int i = 0; i < kNumBlocks; i++) {
    ASSERT_OK(corruptor.AddMisalignedBlockToContainer());
  }

  // Check the report.
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
  corruptor.ResetDataDirManager(dd_manager_.get());
  ASSERT_FALSE(report.HasFatalErrors());
  ASSERT_EQ(kNumBlocks, report.misaligned_block_check->entries.size());
  uint64_t fs_block_size;
  ASSERT_OK(env_->GetBlockSize(test_dir_, &fs_block_size));
  for (const auto& mb : report.misaligned_block_check->entries) {
    ASSERT_EQ(container_name, mb.container);
  }
  report.misaligned_block_check->entries.clear();
  NO_FATALS(AssertEmptyReport(report));
}

TEST_P(LogBlockManagerTest, TestRepairPartialRecords) {
  if (FLAGS_block_manager != "log") {
    return;
  }

  const int kNumContainers = 50;
  const int kNumRecords = 10;

  // Create some containers.
  {
    unique_ptr<BlockCreationTransaction> transaction = bm_->NewCreationTransaction();
    for (int i = 0; i < kNumContainers; i++) {
      unique_ptr<WritableBlock> block;
      ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
      ASSERT_OK(block->Append("a"));
      transaction->AddCreatedBlock(std::move(block));
    }
  }
  vector<string> container_names;
  NO_FATALS(GetContainerNames(&container_names));
  ASSERT_EQ(kNumContainers, container_names.size());

  // Add some partial records.
  LBMCorruptor corruptor(env_, dd_manager_.get(), SeedRandom());
  ASSERT_OK(corruptor.Init());
  for (int i = 0; i < kNumRecords; i++) {
    ASSERT_OK(corruptor.AddPartialRecordToContainer());
  }

  // Check the report.
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
  corruptor.ResetDataDirManager(dd_manager_.get());
  ASSERT_FALSE(report.HasFatalErrors());
  ASSERT_EQ(kNumRecords, report.partial_record_check->entries.size());
  unordered_set<string> container_name_set(container_names.begin(),
                                           container_names.end());
  for (const auto& pr : report.partial_record_check->entries) {
    ASSERT_TRUE(ContainsKey(container_name_set, pr.container));
    ASSERT_GT(pr.offset, 0);
    ASSERT_TRUE(pr.repaired);
  }
  report.partial_record_check->entries.clear();
  NO_FATALS(AssertEmptyReport(report));
}

TEST_P(LogBlockManagerTest, TestDeleteDeadContainersAtStartup) {
  // Force our single container to become full once created.
  FLAGS_log_container_max_size = 0;

  // Create one container.
  BlockId block_id;
  {
    unique_ptr<WritableBlock> block;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
    ASSERT_OK(block->Append("a"));
    ASSERT_OK(block->Close());
    block_id = block->id();
  }
  string data_file_name;
  string metadata_file_name;
  NO_FATALS(GetOnlyContainerDataFile(&data_file_name));
  NO_FATALS(GetOnlyContainerDataFile(&metadata_file_name));

  // Reopen the block manager. The container files should still be there.
  ASSERT_OK(ReopenBlockManager());
  ASSERT_TRUE(env_->FileExists(data_file_name));
  ASSERT_TRUE(env_->FileExists(metadata_file_name));

  // Delete the one block and reopen it again. The container files should have
  // been deleted.
  {
    shared_ptr<BlockDeletionTransaction> deletion_transaction =
        this->bm_->NewDeletionTransaction();
    deletion_transaction->AddDeletedBlock(block_id);
    ASSERT_OK(deletion_transaction->CommitDeletedBlocks(nullptr));
  }
  ASSERT_OK(ReopenBlockManager());
  ASSERT_FALSE(env_->FileExists(data_file_name));
  ASSERT_FALSE(env_->FileExists(metadata_file_name));
}

TEST_P(LogBlockManagerTest, TestCompactFullContainerMetadataAtStartup) {
  if (FLAGS_block_manager != "log") {
    return;
  }

  // With this ratio, the metadata of a full container comprised of half dead
  // blocks will be compacted at startup.
  FLAGS_log_container_live_metadata_before_compact_ratio = 0.50;

  // Set an easy-to-test upper bound on container size.
  FLAGS_log_container_max_blocks = 10;

  // Create one full container and store the initial size of its metadata file.
  vector<BlockId> block_ids;
  for (int i = 0; i < FLAGS_log_container_max_blocks; i++) {
    unique_ptr<WritableBlock> block;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
    ASSERT_OK(block->Append("a"));
    ASSERT_OK(block->Close());
    block_ids.emplace_back(block->id());
  }
  string metadata_file_name;
  NO_FATALS(GetOnlyContainerMetadataFile(&metadata_file_name));
  uint64_t pre_compaction_file_size;
  ASSERT_OK(env_->GetFileSize(metadata_file_name, &pre_compaction_file_size));

  // Delete a block and reopen the block manager. Eventually, the container's
  // metadata file should get compacted at startup (we look for this by testing
  // its file size).
  uint64_t post_compaction_file_size;
  int64_t last_live_aligned_bytes;
  int num_blocks_deleted = 0;
  for (const auto& id : block_ids) {
    {
      shared_ptr<BlockDeletionTransaction> deletion_transaction =
          bm_->NewDeletionTransaction();
      deletion_transaction->AddDeletedBlock(id);
      ASSERT_OK(deletion_transaction->CommitDeletedBlocks(nullptr));
    }
    num_blocks_deleted++;
    FsReport report;
    ASSERT_OK(ReopenBlockManager(nullptr, &report));
    last_live_aligned_bytes = report.stats.live_block_bytes_aligned;

    ASSERT_OK(env_->GetFileSize(metadata_file_name, &post_compaction_file_size));
    if (post_compaction_file_size < pre_compaction_file_size) {
      break;
    }
  }

  // We should be able to anticipate precisely when the compaction occurred.
  ASSERT_EQ(FLAGS_log_container_max_blocks *
              FLAGS_log_container_live_metadata_before_compact_ratio,
            num_blocks_deleted);

  // The "gap" in the compacted container's block records (corresponding to
  // dead blocks that were removed) shouldn't affect the number of live bytes
  // post-alignment.
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
  ASSERT_EQ(last_live_aligned_bytes, report.stats.live_block_bytes_aligned);
}

// Regression test for a bug in which, after a metadata file was compacted,
// we would not properly handle appending to the new (post-compaction) metadata.
//
// The bug was related to a stale file descriptor left in the file_cache, so
// this test explicitly targets that scenario.
TEST_P(LogBlockManagerTest, TestDeleteFromContainerAfterMetadataCompaction) {
  if (FLAGS_block_manager != "log") {
    return;
  }

  // Compact aggressively.
  FLAGS_log_container_live_metadata_before_compact_ratio = 0.99;
  // Use a single shard so that we have an accurate max cache capacity
  // regardless of the number of cores on the machine.
  FLAGS_cache_force_single_shard = true;
  // Use very small containers, so that we generate a lot of them (and thus
  // consume a lot of file descriptors).
  FLAGS_log_container_max_blocks = 4;
  // Reopen so the flags take effect.
  ASSERT_OK(ReopenBlockManager());

  // Create many container with a bunch of blocks, half of which are deleted.
  vector<BlockId> block_ids;
  {
    shared_ptr<BlockDeletionTransaction> deletion_transaction =
        this->bm_->NewDeletionTransaction();
    for (int i = 0; i < 1000; i++) {
      unique_ptr<WritableBlock> block;
      ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
      ASSERT_OK(block->Close());
      if (i % 2 == 1) {
        deletion_transaction->AddDeletedBlock(block->id());
      } else {
        block_ids.emplace_back(block->id());
      }
    }
    ASSERT_OK(deletion_transaction->CommitDeletedBlocks(nullptr));
  }

  // Reopen the block manager. This will cause it to compact all of the metadata
  // files, since we've deleted half the blocks in every container and the
  // threshold is set high above.
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));

  // Delete the remaining blocks in a random order. This will append to metadata
  // files which have just been compacted. Since we have more metadata files than
  // we have file_cache capacity, this will also generate a mix of cache hits,
  // misses, and re-insertions.
  std::mt19937 gen(SeedRandom());
  std::shuffle(block_ids.begin(), block_ids.end(), gen);
  {
    shared_ptr<BlockDeletionTransaction> deletion_transaction =
        this->bm_->NewDeletionTransaction();
    for (const BlockId &b : block_ids) {
      deletion_transaction->AddDeletedBlock(b);
    }
    ASSERT_OK(deletion_transaction->CommitDeletedBlocks(nullptr));
  }

  // Reopen to make sure that the metadata can be properly loaded and
  // that the resulting block manager is empty.
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
  ASSERT_EQ(0, report.stats.live_block_count);
  ASSERT_EQ(0, report.stats.live_block_bytes_aligned);
}

// Test to ensure that if a directory cannot be read from, its startup process
// will run smoothly. The directory manager will note the failed directories
// and only healthy ones are reported.
TEST_P(LogBlockManagerTest, TestOpenWithFailedDirectories) {
  // Initialize a new directory manager with multiple directories.
  bm_.reset();
  vector<string> test_dirs;
  const int kNumDirs = 5;
  for (int i = 0; i < kNumDirs; i++) {
    string dir = GetTestPath(Substitute("test_dir_$0", i));
    ASSERT_OK(env_->CreateDir(dir));
    test_dirs.emplace_back(std::move(dir));
  }
  ASSERT_OK(DataDirManager::CreateNewForTests(env_, test_dirs,
      DataDirManagerOptions(), &dd_manager_));

  // Open the directory manager successfully.
  ASSERT_OK(DataDirManager::OpenExistingForTests(env_, test_dirs,
      DataDirManagerOptions(), &dd_manager_));

  // Wire in a callback to fail data directories.
  error_manager_.SetErrorNotificationCb(
      ErrorHandlerType::DISK_ERROR, [this](const string& uuid) {
        this->dd_manager_->MarkDirFailedByUuid(uuid);
      });
  bm_.reset(CreateBlockManager(nullptr));

  // Fail one of the directories, chosen randomly.
  FLAGS_crash_on_eio = false;
  FLAGS_env_inject_eio = 1;
  int failed_idx = Random(SeedRandom()).Next() % kNumDirs;
  FLAGS_env_inject_eio_globs = JoinPathSegments(test_dirs[failed_idx], "**");

  // Check the report, ensuring the correct directory has failed.
  FsReport report;
  ASSERT_OK(bm_->Open(&report, nullptr, nullptr));
  ASSERT_EQ(kNumDirs - 1, report.data_dirs.size());
  for (const string& data_dir : report.data_dirs) {
    ASSERT_NE(data_dir, test_dirs[failed_idx]);
  }
  const set<int>& failed_dirs = dd_manager_->GetFailedDirs();
  ASSERT_EQ(1, failed_dirs.size());

  int uuid_idx;
  dd_manager_->FindUuidIndexByRoot(test_dirs[failed_idx], &uuid_idx);
  ASSERT_TRUE(ContainsKey(failed_dirs, uuid_idx));
}

// Test Close() a FINALIZED block. Including,
// 1) a container can be reused when the block is finalized.
// 2) the block cannot be opened/found until close it.
// 3) the same container is not marked as available twice.
TEST_P(LogBlockManagerTest, TestFinalizeBlock) {
  // Create 4 blocks.
  vector<unique_ptr<WritableBlock>> blocks;
  for (int i = 0; i < 4; i++) {
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
    ASSERT_OK(writer->Append("test data"));
    ASSERT_OK(writer->Finalize());
    blocks.emplace_back(std::move(writer));
  }
  ASSERT_EQ(1, bm_->all_containers_by_name_.size());

  for (const auto& block : blocks) {
    // Open the block and verify they cannot be found.
    ASSERT_TRUE(bm_->OpenBlock(block->id(), nullptr).IsNotFound());
    ASSERT_OK(block->Close());
  }

  ASSERT_EQ(1, bm_->all_containers_by_name_.size());
  // Ensure the same container has not been marked as available twice.
  ASSERT_EQ(1, bm_->available_containers_by_data_dir_.begin()->second.size());
}

// Test available log container selection is LIFO.
TEST_P(LogBlockManagerTest, TestLIFOContainerSelection) {
  // Create 4 blocks and 4 opened containers that are not full.
  vector<unique_ptr<WritableBlock>> blocks;
  for (int i = 0; i < 4; i++) {
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
    writer->Append("test data");
    blocks.emplace_back(std::move(writer));
  }
  for (const auto& block : blocks) {
    ASSERT_OK(block->Close());
  }
  ASSERT_EQ(4, bm_->all_containers_by_name_.size());

  blocks.clear();
  // Create some other blocks, and finalize each block after write.
  // The first available container in the queue will be reused every time.
  internal::LogBlockContainer* container =
      bm_->available_containers_by_data_dir_.begin()->second.front().get();
  for (int i = 0; i < 4; i++) {
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
    writer->Append("test data");
    ASSERT_OK(writer->Finalize());
    // After finalizing the written block, the used container will be
    // available again and can be reused for the following created block.
    ASSERT_EQ(container,
              bm_->available_containers_by_data_dir_.begin()->second.front().get());
    blocks.emplace_back(std::move(writer));
  }
  for (const auto& block : blocks) {
    ASSERT_OK(block->Close());
  }
  ASSERT_EQ(4, bm_->all_containers_by_name_.size());
}

TEST_P(LogBlockManagerTest, TestAbortBlock) {
  unique_ptr<WritableBlock> writer;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
  ASSERT_OK(writer->Append("test data"));
  ASSERT_OK(writer->Abort());
  // Ensures the container is available after block's Abort().
  ASSERT_EQ(1, bm_->available_containers_by_data_dir_.begin()->second.size());

  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
  ASSERT_OK(writer->Append("test data"));
  ASSERT_OK(writer->Finalize());
  ASSERT_OK(writer->Abort());
  // Ensures the container is available after block's Abort().
  ASSERT_EQ(1, bm_->available_containers_by_data_dir_.begin()->second.size());
}

TEST_P(LogBlockManagerTest, TestDeleteDeadContainersByDeletionTransaction) {
  const auto TestProcess = [&] (int block_num) {
    ASSERT_GT(block_num, 0);
    MetricRegistry registry;
    scoped_refptr<MetricEntity> entity = METRIC_ENTITY_server.Instantiate(
        &registry, Substitute("test-$0", block_num));

    ASSERT_OK(ReopenBlockManager(entity));
    NO_FATALS(CheckLogMetrics(entity,
        { {0, &METRIC_log_block_manager_bytes_under_management},
          {0, &METRIC_log_block_manager_blocks_under_management},
          {0, &METRIC_log_block_manager_containers},
          {0, &METRIC_log_block_manager_full_containers} },
        { {0, &METRIC_log_block_manager_holes_punched},
          {0, &METRIC_block_manager_total_blocks_deleted},
          {0, &METRIC_log_block_manager_dead_containers_deleted} }));

    // Create a bunch of blocks -> one container.
    vector<BlockId> blocks;
    for (int i = 0; i < block_num - 1; ++i) {
      unique_ptr<WritableBlock> writer;
      ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
      blocks.emplace_back(writer->id());
      ASSERT_OK(writer->Finalize());
      ASSERT_OK(writer->Close());
      NO_FATALS(CheckLogMetrics(entity,
          { {0, &METRIC_log_block_manager_bytes_under_management},
            {i + 1, &METRIC_log_block_manager_blocks_under_management},
            {1, &METRIC_log_block_manager_containers},
            {0, &METRIC_log_block_manager_full_containers} },
          { {0, &METRIC_log_block_manager_holes_punched},
            {0, &METRIC_block_manager_total_blocks_deleted},
            {0, &METRIC_log_block_manager_dead_containers_deleted} }));
    }
    {
      // The last block makes a full container.
      FLAGS_log_container_max_size = std::get<0>(GetParam()) ? 4097 : 1;
      unique_ptr<WritableBlock> writer;
      ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
      blocks.emplace_back(writer->id());
      ASSERT_OK(writer->Append("a"));
      ASSERT_OK(writer->Finalize());
      ASSERT_OK(writer->Close());
      NO_FATALS(CheckLogMetrics(entity,
          { {1, &METRIC_log_block_manager_bytes_under_management},
            {block_num, &METRIC_log_block_manager_blocks_under_management},
            {1, &METRIC_log_block_manager_containers},
            {1, &METRIC_log_block_manager_full_containers} },
          { {0, &METRIC_log_block_manager_holes_punched},
            {0, &METRIC_block_manager_total_blocks_deleted},
            {0, &METRIC_log_block_manager_dead_containers_deleted} }));
    }
    ASSERT_EQ(block_num, blocks.size());

    // Check the container files.
    string data_file_name;
    NO_FATALS(GetOnlyContainerDataFile(&data_file_name));
    string metadata_file_name;
    if (FLAGS_block_manager == "log") {
      NO_FATALS(GetOnlyContainerMetadataFile(&metadata_file_name));
    }
    // Open the last block for reading.
    unique_ptr<ReadableBlock> reader;
    ASSERT_OK(bm_->OpenBlock(blocks[block_num-1], &reader));
    uint64_t size;
    ASSERT_OK(reader->Size(&size));
    ASSERT_EQ(1, size);

    // Delete all of the blocks, which makes a dead container.
    {
      vector<BlockId> deleted;
      shared_ptr<BlockDeletionTransaction> deletion_transaction =
        this->bm_->NewDeletionTransaction();
      for (const auto& block : blocks) {
        deletion_transaction->AddDeletedBlock(block);
      }
      ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted));
      ASSERT_EQ(block_num, deleted.size());
      NO_FATALS(CheckLogMetrics(entity,
          { {0, &METRIC_log_block_manager_bytes_under_management},
            {0, &METRIC_log_block_manager_blocks_under_management},
            {1, &METRIC_log_block_manager_containers},
            {1, &METRIC_log_block_manager_full_containers} },
          { {0, &METRIC_log_block_manager_holes_punched},
            {block_num, &METRIC_block_manager_total_blocks_deleted},
            {0, &METRIC_log_block_manager_dead_containers_deleted} }));
    }
    // The container is still alive, because there is a opened block previously.
    NO_FATALS(CheckLogMetrics(entity,
        { {0, &METRIC_log_block_manager_bytes_under_management},
          {0, &METRIC_log_block_manager_blocks_under_management},
          {1, &METRIC_log_block_manager_containers},
          {1, &METRIC_log_block_manager_full_containers} },
        { {0, &METRIC_log_block_manager_holes_punched},
          {block_num, &METRIC_block_manager_total_blocks_deleted},
          {0, &METRIC_log_block_manager_dead_containers_deleted} }));

    // After the reader is closed, the container is actually deleted.
    reader->Close();
    NO_FATALS(CheckLogMetrics(entity,
        { {0, &METRIC_log_block_manager_bytes_under_management},
          {0, &METRIC_log_block_manager_blocks_under_management},
          {0, &METRIC_log_block_manager_containers},
          {0, &METRIC_log_block_manager_full_containers} },
        { {0, &METRIC_log_block_manager_holes_punched},
          {block_num, &METRIC_block_manager_total_blocks_deleted},
          {1, &METRIC_log_block_manager_dead_containers_deleted} }));

    // The container files should have been deleted.
    ASSERT_FALSE(env_->FileExists(data_file_name));
    if (FLAGS_block_manager == "log") {
      ASSERT_FALSE(env_->FileExists(metadata_file_name));
    }
  };

  for (int i = 1; i < 4; ++i) {
    NO_FATALS(TestProcess(i));
  }
}

// Test for KUDU-2665 to ensure that once the container is full and has no live
// blocks but with a reference by WritableBlock, it will not be deleted.
TEST_P(LogBlockManagerTest, TestDoNotDeleteFakeDeadContainer) {
  // Lower the max container size.
  FLAGS_log_container_max_size = 64 * 1024;

  const auto Process = [&] (bool close_block) {
    // Create a bunch of blocks on the same container.
    vector<BlockId> blocks;
    for (int i = 0; i < 10; ++i) {
      unique_ptr<BlockCreationTransaction> transaction = bm_->NewCreationTransaction();
      unique_ptr<WritableBlock> writer;
      ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
      blocks.emplace_back(writer->id());
      ASSERT_OK(writer->Append("a"));
      ASSERT_OK(writer->Finalize());
      transaction->AddCreatedBlock(std::move(writer));
      ASSERT_OK(transaction->CommitCreatedBlocks());
    }

    // Create a special block.
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
    BlockId block_id = writer->id();
    unique_ptr<uint8_t[]> data(new uint8_t[FLAGS_log_container_max_size]);
    ASSERT_OK(writer->Append({ data.get(), FLAGS_log_container_max_size }));
    ASSERT_OK(writer->Finalize());
    // Do not close and reset the writer.
    // Now the container is full and has no live blocks.

    // Delete the bunch of blocks.
    {
      shared_ptr<BlockDeletionTransaction> transaction = bm_->NewDeletionTransaction();
      for (const auto& e : blocks) {
        transaction->AddDeletedBlock(e);
      }
      ASSERT_OK(transaction->CommitDeletedBlocks(nullptr));
      transaction.reset();
      dd_manager_->WaitOnClosures();
    }

    // Close and reset the writer.
    // It's going to test Abort() when 'close_block' is false.
    if (close_block) {
      ASSERT_OK(writer->Close());
    }
    writer.reset();

    // Open the special block after restart.
    ASSERT_OK(ReopenBlockManager());
    unique_ptr<ReadableBlock> block;
    if (close_block) {
      ASSERT_OK(bm_->OpenBlock(block_id, &block));
    } else {
      ASSERT_TRUE(bm_->OpenBlock(block_id, &block).IsNotFound());
    }
  };

  Process(true);
  Process(false);
}

TEST_P(LogBlockManagerTest, TestHalfPresentContainer) {
  if (FLAGS_block_manager != "log") {
    return;
  }

  BlockId block_id;
  string data_file_name;
  string metadata_file_name;
  MetricRegistry registry;
  scoped_refptr<MetricEntity> entity = METRIC_ENTITY_server.Instantiate(&registry, "test");

  const auto CreateContainer = [&] (bool create_block = false) {
    ASSERT_OK(ReopenBlockManager(entity));
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
    block_id = writer->id();
    if (create_block) {
      ASSERT_OK(writer->Append("a"));
    }
    ASSERT_OK(writer->Finalize());
    ASSERT_OK(writer->Close());
    NO_FATALS(GetOnlyContainerDataFile(&data_file_name));
    NO_FATALS(GetOnlyContainerMetadataFile(&metadata_file_name));
  };

  const auto CreateMetadataFile = [&] () {
    // We're often recreating an existing file, so we must invalidate any
    // entry in the file cache first.
    file_cache_.Invalidate(metadata_file_name);

    unique_ptr<WritableFile> metadata_file_writer;
    WritableFileOptions opts;
    opts.is_sensitive = true;
    ASSERT_OK(env_->NewWritableFile(opts, metadata_file_name, &metadata_file_writer));
    ASSERT_OK(metadata_file_writer->Append(Slice("a")));
    metadata_file_writer->Close();
  };

  const auto CreateDataFile = [&] () {
    // We're often recreating an existing file, so we must invalidate any
    // entry in the file cache first.
    file_cache_.Invalidate(data_file_name);

    unique_ptr<WritableFile> data_file_writer;
    WritableFileOptions opts;
    opts.is_sensitive = true;
    ASSERT_OK(env_->NewWritableFile(opts, data_file_name, &data_file_writer));
    data_file_writer->Close();
  };

  const auto DeleteBlock = [&] () {
    shared_ptr<BlockDeletionTransaction> transaction = bm_->NewDeletionTransaction();
    transaction->AddDeletedBlock(block_id);
    ASSERT_OK(transaction->CommitDeletedBlocks(nullptr));
    transaction.reset();
    dd_manager_->WaitOnClosures();
  };

  const auto CheckOK = [&] () {
    FsReport report;
    ASSERT_OK(ReopenBlockManager(entity, &report));
    ASSERT_FALSE(report.HasFatalErrors());
    NO_FATALS(AssertEmptyReport(report));
  };

  const auto CheckFailed = [&] (const Status& expect) {
    Status s = ReopenBlockManager(entity);
    ASSERT_EQ(s.CodeAsString(), expect.CodeAsString());
  };

  const auto CheckRepaired = [&] () {
    FsReport report;
    ASSERT_OK(ReopenBlockManager(entity, &report));
    ASSERT_FALSE(report.HasFatalErrors());
    ASSERT_EQ(1, report.incomplete_container_check->entries.size());
    report.incomplete_container_check->entries.clear();
    NO_FATALS(AssertEmptyReport(report));
  };

  // Case1: the metadata file has gone missing and
  //        the size of the existing data file is 0.
  {
    // Create a container.
    NO_FATALS(CreateContainer());

    // Delete the metadata file.
    ASSERT_OK(env_->DeleteFile(metadata_file_name));

    // The container has been repaired.
    NO_FATALS(CheckRepaired());
  }

  // Case2: the metadata file has gone missing and
  //        the size of the existing data file is >0.
  {
    // Create a container.
    NO_FATALS(CreateContainer(true));

    // Delete the metadata file.
    ASSERT_OK(env_->DeleteFile(metadata_file_name));

    // The metadata file has gone missing.
    NO_FATALS(CheckFailed(Status::NotFound("")));

    // Delete the data file to keep path clean.
    ASSERT_OK(env_->DeleteFile(data_file_name));
  }

  // Case3: the size of the existing metadata file is <MIN and
  //        the data file has gone missing.
  {
    // Create a container.
    NO_FATALS(CreateContainer());

    // Delete the data file&metadata file, and keep the path.
    ASSERT_OK(env_->DeleteFile(data_file_name));
    ASSERT_OK(env_->DeleteFile(metadata_file_name));

    // Create a metadata file whose size is <MIN.
    NO_FATALS(CreateMetadataFile());

    // The container has been repaired.
    NO_FATALS(CheckRepaired());
  }

  // Case4: the size of the existing metadata file is <MIN and
  //        the size of the existing data file is 0.
  {
    // Create a container.
    NO_FATALS(CreateContainer());

    // Delete the metadata file.
    ASSERT_OK(env_->DeleteFile(metadata_file_name));

    // Create a metadata file whose size is <MIN.
    NO_FATALS(CreateMetadataFile());

    // The container has been repaired.
    NO_FATALS(CheckRepaired());
  }

  // Case5: the size of the existing metadata file is <MIN and
  //        the size of the existing data file is >0.
  {
    // Create a container.
    NO_FATALS(CreateContainer(true));

    // Delete the metadata file.
    ASSERT_OK(env_->DeleteFile(metadata_file_name));

    // Create a metadata file whose size is <MIN.
    NO_FATALS(CreateMetadataFile());

    // Check passed, but open metadata file failed at last.
    NO_FATALS(CheckFailed(Status::Incomplete("")));

    // Delete the data file and metadata file to keep path clean.
    ASSERT_OK(env_->DeleteFile(data_file_name));
    ASSERT_OK(env_->DeleteFile(metadata_file_name));
  }

  // Case6: the existing metadata file has no live blocks and
  //        the data file has gone missing.
  {
    NO_FATALS(CreateContainer(true));

    // Delete the only block.
    NO_FATALS(DeleteBlock());

    // Delete the data file.
    ASSERT_OK(env_->DeleteFile(data_file_name));

    // The container has been repaired.
    NO_FATALS(CheckRepaired());
  }

  // Case7: the existing metadata file has no live blocks and
  //        the size of the existing data file is 0.
  {
    NO_FATALS(CreateContainer(true));

    // Delete the only block.
    NO_FATALS(DeleteBlock());

    // Delete the data file.
    ASSERT_OK(env_->DeleteFile(data_file_name));

    // Create an empty data file.
    NO_FATALS(CreateDataFile());

    // Check passed, but verify records failed at last(malformed records).
    NO_FATALS(CheckFailed(Status::Corruption("")));

    // Delete the data file and metadata file to keep path clean.
    ASSERT_OK(env_->DeleteFile(data_file_name));
    ASSERT_OK(env_->DeleteFile(metadata_file_name));
  }

  // Case8: the existing metadata file has no live blocks and
  //        the size of the existing data file is >0.
  {
    NO_FATALS(CreateContainer(true));

    // Delete the only block.
    NO_FATALS(DeleteBlock());

    // The container is ok.
    NO_FATALS(CheckOK());
  }

  // Case9: the existing metadata file has live blocks and
  //        the data file has gone missing.
  {
    // Create a container.
    NO_FATALS(CreateContainer(true));

    // Delete the data file.
    ASSERT_OK(env_->DeleteFile(data_file_name));

    // The data file has gone missing.
    NO_FATALS(CheckFailed(Status::NotFound("")));

    // Delete the metadata file to keep path clean.
    ASSERT_OK(env_->DeleteFile(metadata_file_name));
  }

  // Case10: the existing metadata file has live blocks and
  //         the size of the existing data file is 0.
  {
    // Create a container.
    NO_FATALS(CreateContainer(true));

    // Delete the data file.
    ASSERT_OK(env_->DeleteFile(data_file_name));

    // Create an empty data file.
    NO_FATALS(CreateDataFile());

    // Check passed, but verify records failed at last(malformed records).
    NO_FATALS(CheckFailed(Status::Corruption("")));

    // Delete the data file and metadata file to keep path clean.
    ASSERT_OK(env_->DeleteFile(data_file_name));
    ASSERT_OK(env_->DeleteFile(metadata_file_name));
  }

  // Case11: the existing metadata file has live blocks and
  //         the size of the existing data file is >0.
  {
    // Create a container.
    NO_FATALS(CreateContainer(true));

    // The container is ok.
    NO_FATALS(CheckOK());
  }
}

TEST_P(LogBlockManagerTest, TestLogBlockManagerHalfPresentContainer) {
  if (FLAGS_block_manager != "logr") {
    return;
  }

  BlockId block_id;
  string data_file_name;
  string metadata_file_name;
  string dir;
  string container_name;
  MetricRegistry registry;
  scoped_refptr<MetricEntity> entity = METRIC_ENTITY_server.Instantiate(&registry, "test");

  const auto CreateContainer = [&] (bool create_block = false) {
    ASSERT_OK(ReopenBlockManager(entity));
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
    block_id = writer->id();
    if (create_block) {
      ASSERT_OK(writer->Append("a"));
    }
    ASSERT_OK(writer->Finalize());
    ASSERT_OK(writer->Close());
    NO_FATALS(GetOnlyContainerDataFile(&data_file_name));
    dir = DirName(data_file_name);
    string file_name = BaseName(data_file_name);
    vector<string> tmp = Split(file_name, ".", SkipEmpty());
    CHECK(!tmp.empty());
    container_name = tmp[0];
  };

  const auto CreateDataFile = [&] () {
    // We're often recreating an existing file, so we must invalidate any
    // entry in the file cache first.
    file_cache_.Invalidate(data_file_name);

    unique_ptr<WritableFile> data_file_writer;
    WritableFileOptions opts;
    opts.is_sensitive = true;
    ASSERT_OK(env_->NewWritableFile(opts, data_file_name, &data_file_writer));
    data_file_writer->Close();
  };

  const auto DeleteBlock = [&] () {
    shared_ptr<BlockDeletionTransaction> transaction = bm_->NewDeletionTransaction();
    transaction->AddDeletedBlock(block_id);
    ASSERT_OK(transaction->CommitDeletedBlocks(nullptr));
    transaction.reset();
    dd_manager_->WaitOnClosures();
  };

  const auto CheckOK = [&] () {
    FsReport report;
    ASSERT_OK(ReopenBlockManager(entity, &report));
    ASSERT_FALSE(report.HasFatalErrors());
    NO_FATALS(AssertEmptyReport(report));
  };

  const auto CheckRepaired = [&] (const string& type) {
    FsReport report;
    ASSERT_OK(ReopenBlockManager(entity, &report));
    ASSERT_FALSE(report.HasFatalErrors());
    if (type == "incomplete_container_check") {
      ASSERT_EQ(1, report.incomplete_container_check->entries.size());
      report.incomplete_container_check->entries.clear();
    } else if (type == "partial_rdb_record_check") {
      ASSERT_EQ(1, report.partial_rdb_record_check->entries.size());
      report.partial_rdb_record_check->entries.clear();
    }
    NO_FATALS(AssertEmptyReport(report));
  };

  const auto WriteBadMetadataToRdb = [&] (const string& dir,
                                          const string& container_name,
                                          const BlockId& block_id) {
    Dir* pdir = dd_manager_->FindDirByRoot(dir);
    CHECK(pdir);

    string tmp_key = container_name + "." + block_id.ToString();
    rocksdb::Slice key(tmp_key);
    string value = "bad_value";
    rocksdb::WriteOptions wopt;
    rocksdb::Status s = pdir->rdb()->Put(wopt, key, rocksdb::Slice(value));
    CHECK_OK(FromRdbStatus(s));
  };

  // Case1: the metadata in rdb is empty and
  //        the size of the existing data file is 0.
  {
    // Create a container.
    NO_FATALS(CreateContainer());

    // Delete the metadata file.
    ASSERT_OK(DeleteMetadataFromRdb(dir, container_name));

    // The container has been repaired.
    NO_FATALS(CheckRepaired("incomplete_container_check"));

    // Data file has been removed, metadata is empty.
    ASSERT_FALSE(env_->FileExists(data_file_name));
    ASSERT_EQ(0, MetadataEntriesCount(dir, container_name));
  }

  // Case2: the metadata in rdb is empty and
  //        the size of the existing data file is >0.
  {
    // Create a container.
    NO_FATALS(CreateContainer(true));

    // Delete the metadata file.
    ASSERT_OK(DeleteMetadataFromRdb(dir, container_name));

    // The container has been repaired.
    NO_FATALS(CheckOK());

    // Data file exists, but metadata is empty.
    ASSERT_TRUE(env_->FileExists(data_file_name));
    ASSERT_EQ(0, MetadataEntriesCount(dir, container_name));
  }

  // Case3: the metadata in rdb is empty and
  //        the data file has gone missing.
  {
    // Create a container.
    NO_FATALS(CreateContainer());

    // Delete the data file&metadata file, and keep the path.
    ASSERT_OK(env_->DeleteFile(data_file_name));
    ASSERT_OK(DeleteMetadataFromRdb(dir, container_name));

    // The container is not exist actually.
    NO_FATALS(CheckOK());

    // Data file has been removed, metadata is empty.
    ASSERT_FALSE(env_->FileExists(data_file_name));
    ASSERT_EQ(0, MetadataEntriesCount(dir, container_name));
  }

  // Case4: the metadata in rdb has bad value and
  //        the size of the existing data file is 0.
  {
    // Create a container.
    NO_FATALS(CreateContainer());

    // Delete the metadata file.
    ASSERT_OK(DeleteMetadataFromRdb(dir, container_name));

    // Create a metadata record with bad value.
    WriteBadMetadataToRdb(dir, container_name, block_id);

    // The container has been repaired.
    NO_FATALS(CheckRepaired("incomplete_container_check"));

    // Data file has been removed, metadata is empty.
    ASSERT_FALSE(env_->FileExists(data_file_name));
    ASSERT_EQ(0, MetadataEntriesCount(dir, container_name));
  }

  // Case5: the metadata in rdb has bad value and
  //        the size of the existing data file is >0.
  {
    // Create a container.
    NO_FATALS(CreateContainer(true));

    // Delete the metadata file.
    ASSERT_OK(DeleteMetadataFromRdb(dir, container_name));

    // Create a metadata record with bad value.
    WriteBadMetadataToRdb(dir, container_name, block_id);

    // The container has been repaired.
    NO_FATALS(CheckRepaired("partial_rdb_record_check"));

    // Data file has been removed, metadata is empty.
    ASSERT_TRUE(env_->FileExists(data_file_name));
    ASSERT_EQ(0, MetadataEntriesCount(dir, container_name));
  }

  // Case6: the existing metadata file has no live blocks and
  //        the data file has gone missing.
  {
    NO_FATALS(CreateContainer(true));

    // Delete the only block.
    NO_FATALS(DeleteBlock());

    // Delete the data file.
    ASSERT_OK(env_->DeleteFile(data_file_name));

    // The container is not exist actually.
    NO_FATALS(CheckOK());

    // Data file has been removed, metadata is empty.
    ASSERT_FALSE(env_->FileExists(data_file_name));
    ASSERT_EQ(0, MetadataEntriesCount(dir, container_name));
  }

  // Case7: the existing metadata file has no live blocks and
  //        the size of the existing data file is 0.
  {
    NO_FATALS(CreateContainer(true));

    // Delete the only block.
    NO_FATALS(DeleteBlock());

    // Delete the data file.
    ASSERT_OK(env_->DeleteFile(data_file_name));

    // Create an empty data file.
    NO_FATALS(CreateDataFile());

    // The container has been repaired.
    NO_FATALS(CheckRepaired("incomplete_container_check"));

    // Data file has been removed, metadata is empty.
    ASSERT_FALSE(env_->FileExists(data_file_name));
    ASSERT_EQ(0, MetadataEntriesCount(dir, container_name));
  }

  // Case8: the existing metadata file has no live blocks and
  //        the size of the existing data file is >0.
  {
    NO_FATALS(CreateContainer(true));

    // Delete the only block.
    NO_FATALS(DeleteBlock());

    // The container is ok.
    NO_FATALS(CheckOK());

    // Data file exists, but metadata is empty.
    ASSERT_TRUE(env_->FileExists(data_file_name));
    ASSERT_EQ(0, MetadataEntriesCount(dir, container_name));

    ASSERT_OK(env_->DeleteFile(data_file_name));
  }

  // TODO: need to clear up dead metadata
  // Case9: the existing metadata file has live blocks and
  //        the data file has gone missing.
  {
//    // Create a container.
//    NO_FATALS(CreateContainer(true));
//
//    // Delete the data file.
//    ASSERT_OK(env_->DeleteFile(data_file_name));
//
//    // The container has been repaired.
//    NO_FATALS(CheckRepaired("incomplete_container_check"));
//
//    // Data file has been removed, metadata is empty.
//    ASSERT_FALSE(env_->FileExists(data_file_name));
//    ASSERT_EQ(0, MetadataEntriesCount(dir, container_name));
  }

  // Case10: the existing metadata file has live blocks and
  //         the size of the existing data file is 0.
  {
    // Create a container.
    NO_FATALS(CreateContainer(true));

    // Delete the data file.
    ASSERT_OK(env_->DeleteFile(data_file_name));

    // Create an empty data file.
    NO_FATALS(CreateDataFile());

    // The container has been repaired.
    NO_FATALS(CheckRepaired("incomplete_container_check"));

    // Delete the data file and metadata file to keep path clean.
    ASSERT_FALSE(env_->FileExists(data_file_name));
    ASSERT_EQ(0, MetadataEntriesCount(dir, container_name));
  }

  // Case11: the existing metadata file has live blocks and
  //         the size of the existing data file is >0.
  {
    // Create a container.
    NO_FATALS(CreateContainer(true));

    // The container is ok.
    NO_FATALS(CheckOK());

    ASSERT_TRUE(env_->FileExists(data_file_name));
    ASSERT_EQ(1, MetadataEntriesCount(dir, container_name));
  }
}

} // namespace fs
} // namespace kudu
