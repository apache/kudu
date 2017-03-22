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
#include <memory>
#include <unordered_map>
#include <string>
#include <vector>

#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_report.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/env_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/test_util.h"

using kudu::env_util::ReadFully;
using kudu::pb_util::ReadablePBContainerFile;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

DECLARE_uint64(log_container_preallocate_bytes);
DECLARE_uint64(log_container_max_size);

DECLARE_int64(log_container_max_blocks);

// Log block manager metrics.
METRIC_DECLARE_gauge_uint64(log_block_manager_bytes_under_management);
METRIC_DECLARE_gauge_uint64(log_block_manager_blocks_under_management);
METRIC_DECLARE_counter(log_block_manager_containers);
METRIC_DECLARE_counter(log_block_manager_full_containers);

namespace kudu {
namespace fs {

class LogBlockManagerTest : public KuduTest {
 public:
  LogBlockManagerTest() :
    bm_(CreateBlockManager(scoped_refptr<MetricEntity>())) {
  }

  void SetUp() override {
    CHECK_OK(bm_->Create());

    // Pass in a report to prevent the block manager from logging unnecessarily.
    FsReport report;
    CHECK_OK(bm_->Open(&report));
  }

 protected:
  LogBlockManager* CreateBlockManager(const scoped_refptr<MetricEntity>& metric_entity) {
    BlockManagerOptions opts;
    opts.metric_entity = metric_entity;
    opts.root_paths = { test_dir_ };
    return new LogBlockManager(env_, opts);
  }

  Status ReopenBlockManager(
      const scoped_refptr<MetricEntity>& metric_entity = scoped_refptr<MetricEntity>()) {
    bm_.reset(CreateBlockManager(metric_entity));
    return bm_->Open(nullptr);
  }

  void GetOnlyContainerDataFile(string* data_file) {
    // The expected directory contents are dot, dotdot, test metadata, instance
    // file, and one container file pair.
    string container_data_filename;
    vector<string> children;
    ASSERT_OK(env_->GetChildren(GetTestDataDirectory(), &children));
    ASSERT_EQ(6, children.size());
    for (const string& child : children) {
      if (HasSuffixString(child, ".data")) {
        ASSERT_TRUE(container_data_filename.empty());
        container_data_filename = JoinPathSegments(GetTestDataDirectory(), child);
        break;
      }
    }
    ASSERT_FALSE(container_data_filename.empty());
    *data_file = container_data_filename;
  }

  void AssertNumContainers(int expected_num_containers) {
    // The expected directory contents are dot, dotdot, test metadata, instance
    // file, and a file pair per container.
    vector<string> children;
    ASSERT_OK(env_->GetChildren(GetTestDataDirectory(), &children));
    ASSERT_EQ(4 + (2 * expected_num_containers), children.size());
  }

  unique_ptr<LogBlockManager> bm_;
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
  ASSERT_EQ(containers, down_cast<Counter*>(
                entity->FindOrNull(METRIC_log_block_manager_containers)
                .get())->value());
  ASSERT_EQ(full_containers, down_cast<Counter*>(
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
  ASSERT_OK(bm_->CreateBlock(&writer));
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 0, 0, 1, 0));

  // And when the block is closed, it becomes "under management".
  ASSERT_OK(writer->Close());
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 0, 1, 1, 0));

  // Create 10 blocks concurrently. We reuse the existing container and
  // create 9 new ones. All of them get filled.
  BlockId saved_id;
  {
    Random rand(SeedRandom());
    ScopedWritableBlockCloser closer;
    for (int i = 0; i < 10; i++) {
      unique_ptr<WritableBlock> b;
      ASSERT_OK(bm_->CreateBlock(&b));
      if (saved_id.IsNull()) {
        saved_id = b->id();
      }
      uint8_t data[1024];
      for (int i = 0; i < sizeof(data); i += sizeof(uint32_t)) {
        data[i] = rand.Next();
      }
      b->Append(Slice(data, sizeof(data)));
      closer.AddBlock(std::move(b));
    }
    ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 0, 1, 10, 0));

    // Only when the blocks are closed are the containers considered full.
    ASSERT_OK(closer.CloseBlocks());
    ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(entity, 10 * 1024, 11, 10, 10));
  }

  // Reopen the block manager and test the metrics. They're all based on
  // persistent information so they should be the same.
  MetricRegistry new_registry;
  scoped_refptr<MetricEntity> new_entity = METRIC_ENTITY_server.Instantiate(&new_registry, "test");
  ASSERT_OK(ReopenBlockManager(new_entity));
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(new_entity, 10 * 1024, 11, 10, 10));

  // Delete a block. Its contents should no longer be under management.
  ASSERT_OK(bm_->DeleteBlock(saved_id));
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
  ASSERT_OK(bm_->CreateBlock(&written_block));
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
  ASSERT_OK(bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->Append(kTestData));
  ASSERT_OK(written_block->Close());
  NO_FATALS(GetOnlyContainerDataFile(&container_data_filename));
  ASSERT_OK(env_->GetFileSizeOnDisk(container_data_filename, &size));
  ASSERT_EQ(FLAGS_log_container_preallocate_bytes, size);

  // Now reopen the block manager and create another block. The block manager
  // should be smart enough to reuse the previously preallocated amount.
  ASSERT_OK(ReopenBlockManager());
  ASSERT_OK(bm_->CreateBlock(&written_block));
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
    ScopedWritableBlockCloser closer;
    for (int i = 0; i < 4; i++) {
      unique_ptr<WritableBlock> writer;
      ASSERT_OK(bm_->CreateBlock(&writer));
      block_ids.push_back(writer->id());
      closer.AddBlock(std::move(writer));
    }
    ASSERT_OK(closer.CloseBlocks());
  }

  // Create one more block, which should reuse the first container.
  {
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(bm_->CreateBlock(&writer));
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
    ASSERT_OK(bm_->CreateBlock(&writer));
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
    ASSERT_OK(bm_->CreateBlock(&writer));
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
    ASSERT_OK(bm_->CreateBlock(&writer));
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
    ASSERT_OK(bm_->CreateBlock(&writer));
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
  Slice result;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[latest_meta_size]);
  ASSERT_OK(ReadFully(meta_file.get(), 0, latest_meta_size, &result, scratch.get()));
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
  ASSERT_OK(bm_->CreateBlock(&writer));
  ASSERT_OK(writer->Append("hello world"));
  ASSERT_OK(writer->Close());

  // On second append, don't crash just because the append offset is ahead of
  // the preallocation offset!
  ASSERT_OK(bm_->CreateBlock(&writer));
  ASSERT_OK(writer->Append("hello world"));
}

TEST_F(LogBlockManagerTest, TestPreallocationAndTruncation) {
  // Ensure preallocation window is greater than the container size itself.
  FLAGS_log_container_max_size = 1024 * 1024;
  FLAGS_log_container_preallocate_bytes = 32 * 1024 * 1024;

  // Fill up one container.
  unique_ptr<WritableBlock> writer;
  ASSERT_OK(bm_->CreateBlock(&writer));
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
    ASSERT_OK(bm_->CreateBlock(&block));
    ASSERT_OK(block->Append("aaaa"));
    ASSERT_OK(block->Close());
    ids.push_back(block->id());
  }

  // Delete every other block. In effect, this maximizes the number of extents
  // in the container by forcing the filesystem to alternate every hole with
  // a live extent.
  LOG(INFO) << "Deleting every other block";
  for (int i = 0; i < ids.size(); i += 2) {
    ASSERT_OK(bm_->DeleteBlock(ids[i]));
  }

  // Delete all of the blocks belonging to the interior node. If KUDU-1508
  // applies, this should corrupt the filesystem.
  LOG(INFO) << Substitute("Deleting remaining blocks up to block number $0",
                          last_interior_node_block_number);
  for (int i = 1; i < last_interior_node_block_number; i += 2) {
    ASSERT_OK(bm_->DeleteBlock(ids[i]));
  }
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
      RETURN_NOT_OK(bm_->CreateBlock(&block));
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

} // namespace fs
} // namespace kudu
