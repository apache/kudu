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
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_report.h"
#include "kudu/fs/log_block_manager-test-util.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/atomic.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h" // IWYU pragma: keep
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::pb_util::ReadablePBContainerFile;
using std::set;
using std::string;
using std::shared_ptr;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

DECLARE_bool(cache_force_single_shard);
DECLARE_bool(crash_on_eio);
DECLARE_double(env_inject_eio);
DECLARE_double(log_container_excess_space_before_cleanup_fraction);
DECLARE_double(log_container_live_metadata_before_compact_ratio);
DECLARE_int64(block_manager_max_open_files);
DECLARE_int64(log_container_max_blocks);
DECLARE_string(block_manager_preflush_control);
DECLARE_string(env_inject_eio_globs);
DECLARE_uint64(log_container_preallocate_bytes);
DECLARE_uint64(log_container_max_size);

// Log block manager metrics.
METRIC_DECLARE_gauge_uint64(log_block_manager_bytes_under_management);
METRIC_DECLARE_gauge_uint64(log_block_manager_blocks_under_management);
METRIC_DECLARE_gauge_uint64(log_block_manager_containers);
METRIC_DECLARE_gauge_uint64(log_block_manager_full_containers);

namespace kudu {
namespace fs {

namespace internal {
class LogBlockContainer;
} // namespace internal

class LogBlockManagerTest : public KuduTest {
 public:
  LogBlockManagerTest() :
    test_tablet_name_("test_tablet"),
    test_block_opts_({ test_tablet_name_ }),
    test_error_manager_(new FsErrorManager()),
    bm_(CreateBlockManager(scoped_refptr<MetricEntity>())) {
  }

  void SetUp() override {
    // Pass in a report to prevent the block manager from logging unnecessarily.
    FsReport report;
    ASSERT_OK(bm_->Open(&report));
    ASSERT_OK(dd_manager_->CreateDataDirGroup(test_tablet_name_));
    ASSERT_TRUE(dd_manager_->GetDataDirGroupPB(test_tablet_name_, &test_group_pb_));
  }

 protected:
  LogBlockManager* CreateBlockManager(const scoped_refptr<MetricEntity>& metric_entity) {
    if (!dd_manager_) {
      // Ensure the directory manager is initialized.
      CHECK_OK(DataDirManager::CreateNewForTests(env_, { test_dir_ },
          DataDirManagerOptions(), &dd_manager_));
    }
    BlockManagerOptions opts;
    opts.metric_entity = metric_entity;
    return new LogBlockManager(env_, dd_manager_.get(), test_error_manager_.get(), opts);
  }

  Status ReopenBlockManager(const scoped_refptr<MetricEntity>& metric_entity = nullptr,
                            FsReport* report = nullptr) {
    // The directory manager must outlive the block manager. Destroy the block
    // manager first to enforce this.
    bm_.reset();

    // Re-open the directory manager first to clear any in-memory maps.
    RETURN_NOT_OK(DataDirManager::OpenExistingForTests(env_, { test_dir_ },
        DataDirManagerOptions(), &dd_manager_));

    bm_.reset(CreateBlockManager(metric_entity));
    RETURN_NOT_OK(bm_->Open(report));
    RETURN_NOT_OK(dd_manager_->LoadDataDirGroupFromPB(test_tablet_name_, test_group_pb_));
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
  }

  DataDirGroupPB test_group_pb_;
  string test_tablet_name_;
  CreateBlockOptions test_block_opts_;

  unique_ptr<DataDirManager> dd_manager_;
  unique_ptr<FsErrorManager> test_error_manager_;
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
    for (const string& data_dir : dd_manager_->GetDataDirs()) {
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
};

static void CheckLogMetrics(const scoped_refptr<MetricEntity>& entity,
                            int bytes_under_management, int blocks_under_management,
                            int containers, int full_containers) {
  ASSERT_EQ(bytes_under_management, down_cast<AtomicGauge<uint64_t>*>(
                entity->FindOrNull(METRIC_log_block_manager_bytes_under_management)
                .get())->value());
  ASSERT_EQ(blocks_under_management, down_cast<AtomicGauge<uint64_t>*>(
                entity->FindOrNull(METRIC_log_block_manager_blocks_under_management)
                .get())->value());
  ASSERT_EQ(containers, down_cast<AtomicGauge<uint64_t>*>(
                entity->FindOrNull(METRIC_log_block_manager_containers)
                .get())->value());
  ASSERT_EQ(full_containers, down_cast<AtomicGauge<uint64_t>*>(
                entity->FindOrNull(METRIC_log_block_manager_full_containers)
                .get())->value());
}

TEST_F(LogBlockManagerTest, MetricsTest) {
  MetricRegistry registry;
  scoped_refptr<MetricEntity> entity = METRIC_ENTITY_server.Instantiate(&registry, "test");
  ASSERT_OK(ReopenBlockManager(entity));
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 0, 0, 0, 0));

  // Lower the max container size so that we can more easily test full
  // container metrics.
  FLAGS_log_container_max_size = 1024;

  // One block --> one container.
  unique_ptr<WritableBlock> writer;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 0, 0, 1, 0));

  // And when the block is closed, it becomes "under management".
  ASSERT_OK(writer->Close());
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 0, 1, 1, 0));

  // Create 10 blocks concurrently. We reuse the existing container and
  // create 9 new ones. All of them get filled.
  BlockId saved_id;
  {
    Random rand(SeedRandom());
    unique_ptr<BlockCreationTransaction> transaction = bm_->NewCreationTransaction();
    for (int i = 0; i < 10; i++) {
      unique_ptr<WritableBlock> b;
      ASSERT_OK(bm_->CreateBlock(test_block_opts_, &b));
      if (saved_id.IsNull()) {
        saved_id = b->id();
      }
      uint8_t data[1024];
      for (int i = 0; i < sizeof(data); i += sizeof(uint32_t)) {
        data[i] = rand.Next();
      }
      b->Append(Slice(data, sizeof(data)));
      ASSERT_OK(b->Finalize());
      transaction->AddCreatedBlock(std::move(b));
    }
    // Metrics for full containers are updated after Finalize().
    ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 0, 1, 10, 10));

    ASSERT_OK(transaction->CommitCreatedBlocks());
    ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 10 * 1024, 11, 10, 10));
  }

  // Reopen the block manager and test the metrics. They're all based on
  // persistent information so they should be the same.
  MetricRegistry new_registry;
  scoped_refptr<MetricEntity> new_entity = METRIC_ENTITY_server.Instantiate(&new_registry, "test");
  ASSERT_OK(ReopenBlockManager(new_entity));
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(new_entity, 10 * 1024, 11, 10, 10));

  // Delete a block. Its contents should no longer be under management.
  shared_ptr<BlockDeletionTransaction> deletion_transaction =
      bm_->NewDeletionTransaction();
  deletion_transaction->AddDeletedBlock(saved_id);
  vector<BlockId> deleted;
  ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted));
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(new_entity, 9 * 1024, 10, 10, 10));
}

TEST_F(LogBlockManagerTest, ContainerPreallocationTest) {
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

// Regression test for KUDU-1190, a crash at startup when a block ID has been
// reused.
TEST_F(LogBlockManagerTest, TestReuseBlockIds) {
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
  for (const BlockId& b : block_ids) {
    ASSERT_OK(bm_->DeleteBlock(b));
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
TEST_F(LogBlockManagerTest, TestMetadataTruncation) {
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
      bm_->all_containers_by_name_.begin()->second);
  string metadata_path = path + LogBlockManager::kContainerMetadataFileSuffix;
  string data_path = path + LogBlockManager::kContainerDataFileSuffix;

  uint64_t good_meta_size;
  ASSERT_OK(env_->GetFileSize(metadata_path, &good_meta_size));

  // First, add an extra byte to the end of the metadata file. This makes the
  // trailing "record" of the metadata file corrupt, but doesn't cause data
  // loss. The result is that the container will automatically truncate the
  // metadata file back to its correct size.
  {
    RWFileOptions opts;
    opts.mode = Env::OPEN_EXISTING;
    unique_ptr<RWFile> file;
    ASSERT_OK(env_->NewRWFile(opts, metadata_path, &file));
    ASSERT_OK(file->Truncate(good_meta_size + 1));
  }

  uint64_t cur_meta_size;
  ASSERT_OK(env_->GetFileSize(metadata_path, &cur_meta_size));
  ASSERT_EQ(good_meta_size + 1, cur_meta_size);

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

  // Delete the first block we created. This necessitates writing to the
  // metadata file of the originally-written container, since we append a
  // delete record to the metadata.
  ASSERT_OK(bm_->DeleteBlock(created_blocks[0]));
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
    opts.mode = Env::OPEN_EXISTING;
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
  ASSERT_OK(env_->NewRandomAccessFile(metadata_path, &meta_file));
  ReadablePBContainerFile pb_reader(std::move(meta_file));
  ASSERT_OK(pb_reader.Open());
  BlockRecordPB record;
  ASSERT_OK(pb_reader.ReadNextPB(&record));
  uint64_t offset = pb_reader.offset();

  uint64_t latest_meta_size;
  ASSERT_OK(env_->GetFileSize(metadata_path, &latest_meta_size));
  ASSERT_OK(env_->NewRandomAccessFile(metadata_path, &meta_file));
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[latest_meta_size]);
  Slice result(scratch.get(), latest_meta_size);
  ASSERT_OK(meta_file->Read(0, result));
  string data = result.ToString();
  // Flip the high bit of the length field, which is a 4-byte little endian
  // unsigned integer. This will cause the length field to represent a large
  // value and also cause the length checksum not to validate.
  data[offset + 3] ^= 1 << 7;
  unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->NewWritableFile(metadata_path, &writable_file));
  ASSERT_OK(writable_file->Append(data));
  ASSERT_OK(writable_file->Close());

  // Now try to reopen the container.
  // This should look like a bad checksum, and it's not recoverable.
  s = ReopenBlockManager();
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_STR_CONTAINS(s.ToString(), "Incorrect checksum");

  // Now truncate both the data and metadata files.
  // This should be recoverable. See KUDU-668.
  ASSERT_OK(env_->NewWritableFile(metadata_path, &writable_file));
  ASSERT_OK(writable_file->Close());
  ASSERT_OK(env_->NewWritableFile(data_path, &writable_file));
  ASSERT_OK(writable_file->Close());

  ASSERT_OK(ReopenBlockManager());
}

// Regression test for a crash when a container's append offset exceeded its
// preallocation offset.
TEST_F(LogBlockManagerTest, TestAppendExceedsPreallocation) {
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

TEST_F(LogBlockManagerTest, TestPreallocationAndTruncation) {
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
  ASSERT_EQ(FLAGS_log_container_max_size, size_after_close);

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
    opts.mode = Env::OPEN_EXISTING;
    ASSERT_OK(env_->NewRWFile(opts, fname, &data_file));
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
    ASSERT_EQ(FLAGS_log_container_max_size, size_after_reopen);
  }
}

TEST_F(LogBlockManagerTest, TestContainerWithManyHoles) {
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
  vector<BlockId> deleted;
  ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted));

  // Delete all of the blocks belonging to the interior node. If KUDU-1508
  // applies, this should corrupt the filesystem.
  LOG(INFO) << Substitute("Deleting remaining blocks up to block number $0",
                          last_interior_node_block_number);
  for (int i = 1; i < last_interior_node_block_number; i += 2) {
    deletion_transaction->AddDeletedBlock(ids[i]);
  }
  ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted));
}

TEST_F(LogBlockManagerTest, TestParseKernelRelease) {
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
// This is simplistic in several ways compared to a typical workload:
// - only one data directory (typical servers have several)
// - minimal number of containers, each of which is entirely full
//   (typical workloads end up writing to several containers at once
//    due to concurrent write operations such as multiple MM threads
//    flushing)
// - no deleted blocks to process
//
// However it still can be used to micro-optimize the startup process.
TEST_F(LogBlockManagerTest, StartupBenchmark) {
  // Disable preflushing since this can slow down our writes. In particular,
  // since we write such small blocks in this test, each block will likely
  // begin on the same 4KB page as the prior one we wrote, and due to the
  // "stable page writes" feature, each block will thus end up waiting
  // on the writeback of the prior one.
  //
  // See http://yoshinorimatsunobu.blogspot.com/2014/03/how-syncfilerange-really-works.html
  // for details.
  FLAGS_block_manager_preflush_control = "never";
  const int kNumBlocks = AllowSlowTests() ? 1000000 : 1000;
  // Creates 'kNumBlocks' blocks with minimal data.
  {
    unique_ptr<BlockCreationTransaction> transaction = bm_->NewCreationTransaction();
    for (int i = 0; i < kNumBlocks; i++) {
      unique_ptr<WritableBlock> block;
      ASSERT_OK_FAST(bm_->CreateBlock(test_block_opts_, &block));
      ASSERT_OK_FAST(block->Append("x"));
      ASSERT_OK_FAST(block->Finalize());
      transaction->AddCreatedBlock(std::move(block));
    }
    ASSERT_OK(transaction->CommitCreatedBlocks());
  }
  for (int i = 0; i < 10; i++) {
    LOG_TIMING(INFO, "reopening block manager") {
      ASSERT_OK(ReopenBlockManager());
    }
  }
}
#endif

TEST_F(LogBlockManagerTest, TestLookupBlockLimit) {
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

TEST_F(LogBlockManagerTest, TestContainerBlockLimiting) {
  const int kNumBlocks = 1000;

  // Creates 'kNumBlocks' blocks with minimal data.
  auto create_some_blocks = [&]() -> Status {
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

TEST_F(LogBlockManagerTest, TestMisalignedBlocksFuzz) {
  FLAGS_log_container_preallocate_bytes = 0;
  const int kNumBlocks = 100;

  // Create one container.
  unique_ptr<WritableBlock> block;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
  ASSERT_OK(block->Close());
  string container_name;
  NO_FATALS(GetOnlyContainer(&container_name));

  // Add a mixture of regular and misaligned blocks to it.
  LBMCorruptor corruptor(env_, dd_manager_->GetDataDirs(), SeedRandom());
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
  ASSERT_FALSE(report.HasFatalErrors()) << report.ToString();
  ASSERT_EQ(num_misaligned_blocks, report.misaligned_block_check->entries.size());
  for (const auto& mb : report.misaligned_block_check->entries) {
    ASSERT_EQ(container_name, mb.container);
  }

  shared_ptr<BlockDeletionTransaction> deletion_transaction =
      this->bm_->NewDeletionTransaction();
  // Delete about half of them, chosen randomly.
  vector<BlockId> block_ids;
  ASSERT_OK(bm_->GetAllBlockIds(&block_ids));
  for (const auto& id : block_ids) {
    if (rand() % 2) {
      deletion_transaction->AddDeletedBlock(id);
    }
  }
  vector<BlockId> deleted;
  ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted));

  // Wait for the block manager to punch out all of the holes. It's easiest to
  // do this by reopening it; shutdown will wait for outstanding hole punches.
  //
  // On reopen, some misaligned blocks should be gone from the report.
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
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

TEST_F(LogBlockManagerTest, TestRepairPreallocateExcessSpace) {
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
  LBMCorruptor corruptor(env_, dd_manager_->GetDataDirs(), SeedRandom());
  ASSERT_OK(corruptor.Init());
  ASSERT_OK(corruptor.PreallocateFullContainer());

  // Check the report.
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
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

TEST_F(LogBlockManagerTest, TestRepairUnpunchedBlocks) {
  const int kNumBlocks = 100;

  // Enforce that the container's actual size is strictly upper-bounded by the
  // calculated size so we can more easily trigger repairs.
  FLAGS_log_container_excess_space_before_cleanup_fraction = 0.0;

  // Force our single container to become full once created.
  FLAGS_log_container_max_size = 0;

  // Force the test to measure extra space in unpunched holes, not in the
  // preallocation buffer.
  FLAGS_log_container_preallocate_bytes = 0;

  // Create one container.
  unique_ptr<WritableBlock> block;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
  ASSERT_OK(block->Close());
  string data_file;
  NO_FATALS(GetOnlyContainerDataFile(&data_file));
  uint64_t file_size_on_disk;
  ASSERT_OK(env_->GetFileSizeOnDisk(data_file, &file_size_on_disk));
  ASSERT_EQ(0, file_size_on_disk);

  // Add some "unpunched blocks" to the container.
  LBMCorruptor corruptor(env_, dd_manager_->GetDataDirs(), SeedRandom());
  ASSERT_OK(corruptor.Init());
  for (int i = 0; i < kNumBlocks; i++) {
    ASSERT_OK(corruptor.AddUnpunchedBlockToFullContainer());
  }

  ASSERT_OK(env_->GetFileSizeOnDisk(data_file, &file_size_on_disk));
  ASSERT_GT(file_size_on_disk, 0);

  // Check the report.
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
  ASSERT_FALSE(report.HasFatalErrors());
  ASSERT_EQ(1, report.full_container_space_check->entries.size());
  const LBMFullContainerSpaceCheck::Entry& fcs =
      report.full_container_space_check->entries[0];
  string container;
  NO_FATALS(GetOnlyContainer(&container));
  ASSERT_EQ(container, fcs.container);
  ASSERT_EQ(file_size_on_disk, fcs.excess_bytes);
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
  ASSERT_EQ(0, file_size_on_disk);
}

TEST_F(LogBlockManagerTest, TestRepairIncompleteContainer) {
  const int kNumContainers = 20;

  // Create some incomplete containers. The corruptor will select between
  // several variants of "incompleteness" at random (see
  // LBMCorruptor::CreateIncompleteContainer() for details).
  LBMCorruptor corruptor(env_, dd_manager_->GetDataDirs(), SeedRandom());
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

TEST_F(LogBlockManagerTest, TestDetectMalformedRecords) {
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
  LBMCorruptor corruptor(env_, dd_manager_->GetDataDirs(), SeedRandom());
  ASSERT_OK(corruptor.Init());
  for (int i = 0; i < kNumRecords; i++) {
    ASSERT_OK(corruptor.AddMalformedRecordToContainer());
  }

  // Check the report.
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
  ASSERT_TRUE(report.HasFatalErrors());
  ASSERT_EQ(kNumRecords, report.malformed_record_check->entries.size());
  for (const auto& mr : report.malformed_record_check->entries) {
    ASSERT_EQ(container_name, mr.container);
  }
  report.malformed_record_check->entries.clear();
  NO_FATALS(AssertEmptyReport(report));
}

TEST_F(LogBlockManagerTest, TestDetectMisalignedBlocks) {
  const int kNumBlocks = 50;

  // Create one container.
  unique_ptr<WritableBlock> block;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
  ASSERT_OK(block->Append("a"));
  ASSERT_OK(block->Close());
  string container_name;
  NO_FATALS(GetOnlyContainer(&container_name));

  // Add some misaligned blocks.
  LBMCorruptor corruptor(env_, dd_manager_->GetDataDirs(), SeedRandom());
  ASSERT_OK(corruptor.Init());
  for (int i = 0; i < kNumBlocks; i++) {
    ASSERT_OK(corruptor.AddMisalignedBlockToContainer());
  }

  // Check the report.
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
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

TEST_F(LogBlockManagerTest, TestRepairPartialRecords) {
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
  LBMCorruptor corruptor(env_, dd_manager_->GetDataDirs(), SeedRandom());
  ASSERT_OK(corruptor.Init());
  for (int i = 0; i < kNumRecords; i++) {
    ASSERT_OK(corruptor.AddPartialRecordToContainer());
  }

  // Check the report.
  FsReport report;
  ASSERT_OK(ReopenBlockManager(nullptr, &report));
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

TEST_F(LogBlockManagerTest, TestDeleteDeadContainersAtStartup) {
  // Force our single container to become full once created.
  FLAGS_log_container_max_size = 0;

  // Create one container.
  unique_ptr<WritableBlock> block;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &block));
  ASSERT_OK(block->Append("a"));
  ASSERT_OK(block->Close());
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
  shared_ptr<BlockDeletionTransaction> deletion_transaction =
      this->bm_->NewDeletionTransaction();
  deletion_transaction->AddDeletedBlock(block->id());
  vector<BlockId> deleted;
  ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted));
  ASSERT_OK(ReopenBlockManager());
  ASSERT_FALSE(env_->FileExists(data_file_name));
  ASSERT_FALSE(env_->FileExists(metadata_file_name));
}

TEST_F(LogBlockManagerTest, TestCompactFullContainerMetadataAtStartup) {
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
    ASSERT_OK(bm_->DeleteBlock(id));
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
TEST_F(LogBlockManagerTest, TestDeleteFromContainerAfterMetadataCompaction) {
  // Compact aggressively.
  FLAGS_log_container_live_metadata_before_compact_ratio = 0.99;
  // Use a small file cache (smaller than the number of containers).
  FLAGS_block_manager_max_open_files = 50;
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
    vector<BlockId> deleted;
    ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted));
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
  std::random_shuffle(block_ids.begin(), block_ids.end());
  {
    shared_ptr<BlockDeletionTransaction> deletion_transaction =
        this->bm_->NewDeletionTransaction();
    for (const BlockId &b : block_ids) {
      deletion_transaction->AddDeletedBlock(b);
    }
    vector<BlockId> deleted;
    ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted));
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
TEST_F(LogBlockManagerTest, TestOpenWithFailedDirectories) {
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
  test_error_manager_->SetErrorNotificationCb(Bind(&DataDirManager::MarkDataDirFailedByUuid,
                                                   Unretained(dd_manager_.get())));
  bm_.reset(CreateBlockManager(nullptr));

  // Fail one of the directories, chosen randomly.
  FLAGS_crash_on_eio = false;
  FLAGS_env_inject_eio = 1;
  int failed_idx = Random(SeedRandom()).Next() % kNumDirs;
  FLAGS_env_inject_eio_globs = JoinPathSegments(test_dirs[failed_idx], "**");

  // Check the report, ensuring the correct directory has failed.
  FsReport report;
  ASSERT_OK(bm_->Open(&report));
  ASSERT_EQ(kNumDirs - 1, report.data_dirs.size());
  for (const string& data_dir : report.data_dirs) {
    ASSERT_NE(data_dir, test_dirs[failed_idx]);
  }
  const set<uint16_t>& failed_dirs = dd_manager_->GetFailedDataDirs();
  ASSERT_EQ(1, failed_dirs.size());

  uint16_t uuid_idx;
  dd_manager_->FindUuidIndexByRoot(test_dirs[failed_idx], &uuid_idx);
  ASSERT_TRUE(ContainsKey(failed_dirs, uuid_idx));
  FLAGS_env_inject_eio = 0;
}

// Test Close() a FINALIZED block. Including,
// 1) a container can be reused when the block is finalized.
// 2) the block cannot be opened/found until close it.
// 3) the same container is not marked as available twice.
TEST_F(LogBlockManagerTest, TestFinalizeBlock) {
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
TEST_F(LogBlockManagerTest, TestLIFOContainerSelection) {
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
      bm_->available_containers_by_data_dir_.begin()->second.front();
  for (int i = 0; i < 4; i++) {
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
    writer->Append("test data");
    ASSERT_OK(writer->Finalize());
    // After finalizing the written block, the used container will be
    // available again and can be reused for the following created block.
    ASSERT_EQ(container,
              bm_->available_containers_by_data_dir_.begin()->second.front());
    blocks.emplace_back(std::move(writer));
  }
  for (const auto& block : blocks) {
    ASSERT_OK(block->Close());
  }
  ASSERT_EQ(4, bm_->all_containers_by_name_.size());
}

TEST_F(LogBlockManagerTest, TestAbortBlock) {
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

} // namespace fs
} // namespace kudu
