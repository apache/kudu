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
#include <string>
#include <vector>

#include <google/protobuf/util/message_differencer.h>

#include "kudu/fs/data_dirs.h"
#include "kudu/fs/file_block_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_report.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

using google::protobuf::util::MessageDifferencer;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DECLARE_double(log_container_live_metadata_before_compact_ratio);
DECLARE_uint64(log_container_preallocate_bytes);
DECLARE_uint64(log_container_max_size);
DECLARE_int64(fs_data_dirs_reserved_bytes);
DECLARE_int64(disk_reserved_bytes_free_for_testing);
DECLARE_int32(fs_data_dirs_full_disk_cache_seconds);
DECLARE_uint32(fs_target_data_dirs_per_tablet);
DECLARE_string(block_manager);
DECLARE_double(env_inject_eio);
DECLARE_bool(suicide_on_eio);

// Generic block manager metrics.
METRIC_DECLARE_gauge_uint64(block_manager_blocks_open_reading);
METRIC_DECLARE_gauge_uint64(block_manager_blocks_open_writing);
METRIC_DECLARE_counter(block_manager_total_writable_blocks);
METRIC_DECLARE_counter(block_manager_total_readable_blocks);
METRIC_DECLARE_counter(block_manager_total_bytes_written);
METRIC_DECLARE_counter(block_manager_total_bytes_read);

// Data directory metrics.
METRIC_DECLARE_gauge_uint64(data_dirs_full);

// The LogBlockManager is only supported on Linux, since it requires hole punching.
#define RETURN_NOT_LOG_BLOCK_MANAGER() \
  do { \
    if (FLAGS_block_manager != "log") { \
      LOG(INFO) << "This platform does not use the log block manager by default. Skipping test."; \
      return; \
    } \
  } while (false)

namespace kudu {
namespace fs {

static const char* kTestData = "test data";

template <typename T>
class BlockManagerTest : public KuduTest {
 public:
  BlockManagerTest() :
    test_tablet_name_("test_tablet"),
    test_block_opts_(CreateBlockOptions({ test_tablet_name_ })),
    bm_(CreateBlockManager(scoped_refptr<MetricEntity>(),
                           shared_ptr<MemTracker>(),
                           { test_dir_ })) {
  }

  virtual void SetUp() OVERRIDE {
    CHECK_OK(bm_->Create());
    // Pass in a report to prevent the block manager from logging
    // unnecessarily.
    FsReport report;
    CHECK_OK(bm_->Open(&report));
    CHECK_OK(bm_->dd_manager()->CreateDataDirGroup(test_tablet_name_));
    CHECK(bm_->dd_manager()->GetDataDirGroupPB(test_tablet_name_, &test_group_pb_));
  }

  void DistributeBlocksAcrossDirs(int num_dirs, int num_blocks_per_dir) {
    // Create a data directory group that contains 'num_dirs' data directories.
    string tablet_name = Substitute("$0_disks", num_dirs);
    CreateBlockOptions opts({ tablet_name });
    FLAGS_fs_target_data_dirs_per_tablet = num_dirs;
    ASSERT_OK(bm_->dd_manager()->CreateDataDirGroup(tablet_name));
    int num_blocks = num_dirs * num_blocks_per_dir;

    // Write 'num_blocks' blocks to this data dir group.
    ScopedWritableBlockCloser closer;
    for (int i = 0; i < num_blocks; i++) {
      unique_ptr<WritableBlock> written_block;
      ASSERT_OK(bm_->CreateBlock(opts, &written_block));
      ASSERT_OK(written_block->Append(kTestData));
      closer.AddBlock(std::move(written_block));
    }
    ASSERT_OK(closer.CloseBlocks());
  }

 protected:
  T* CreateBlockManager(const scoped_refptr<MetricEntity>& metric_entity,
                        const shared_ptr<MemTracker>& parent_mem_tracker,
                        const vector<string>& paths) {
    BlockManagerOptions opts;
    opts.metric_entity = metric_entity;
    opts.parent_mem_tracker = parent_mem_tracker;
    opts.root_paths = paths;
    return new T(env_, opts);
  }

  Status ReopenBlockManager(const scoped_refptr<MetricEntity>& metric_entity,
                            const shared_ptr<MemTracker>& parent_mem_tracker,
                            const vector<string>& paths,
                            bool create,
                            bool load_test_group = true) {
    bm_.reset(CreateBlockManager(metric_entity, parent_mem_tracker, paths));
    if (create) {
      RETURN_NOT_OK(bm_->Create());
    }
    RETURN_NOT_OK(bm_->Open(nullptr));

    // Certain tests may maintain their own directory groups, in which case
    // the default test group should not be used.
    if (load_test_group) {
      RETURN_NOT_OK(bm_->dd_manager()->LoadDataDirGroupFromPB(test_tablet_name_, test_group_pb_));
    }
    return Status::OK();
  }

  void RunMultipathTest(const vector<string>& paths);

  void RunMemTrackerTest();

  void RunBlockDistributionTest(const vector<string>& paths);

  static Status CountFilesCb(int* num_files, Env::FileType type,
                      const string& /*dirname*/,
                      const string& basename) {
    if (basename == kInstanceMetadataFileName) {
      return Status::OK();
    }
    if (type == Env::FILE_TYPE) {
      *num_files += 1;
    }
    return Status::OK();
  }

  // Utility function that counts the number of files within a directory
  // hierarchy, ignoring '.', '..', and file 'kInstanceMetadataFileName'.
  Status CountFiles(const string& root, int* num_files) {
    *num_files = 0;
    RETURN_NOT_OK(env_->Walk(root, Env::PRE_ORDER,
                             Bind(BlockManagerTest::CountFilesCb, num_files)));
    return Status::OK();
  }

  // Keep an internal copy of the data dir group to act as metadata.
  DataDirGroupPB test_group_pb_;
  string test_tablet_name_;
  CreateBlockOptions test_block_opts_;
  gscoped_ptr<T> bm_;
};

template <>
void BlockManagerTest<LogBlockManager>::SetUp() {
  RETURN_NOT_LOG_BLOCK_MANAGER();
  CHECK_OK(bm_->Create());
  // Pass in a report to prevent the block manager from logging
  // unnecessarily.
  FsReport report;
  CHECK_OK(bm_->Open(&report));
  CHECK_OK(bm_->dd_manager()->CreateDataDirGroup(test_tablet_name_));

  // Store the DataDirGroupPB for tests that reopen the block manager.
  CHECK(bm_->dd_manager()->GetDataDirGroupPB(test_tablet_name_, &test_group_pb_));
}

template <>
void BlockManagerTest<FileBlockManager>::RunBlockDistributionTest(const vector<string>& paths) {
  vector<int> blocks_in_each_path(paths.size());
  // Writes 'num_blocks_per_dir' per directory. This will not necessarily write exactly
  // 'num_blocks_per_dir' in each directory, but will ensure blocks are written to all directories
  // with a high probability.
  // Running this 5000 times yielded no failures.
  int num_blocks_per_dir = 30;

  // Spread across 1, then 3, then 5 data directories.
  for (int d: { 1, 3, 5 }) {
    DistributeBlocksAcrossDirs(d, num_blocks_per_dir);
    // Iterate through the data directories, counting the number of blocks in each directory, and
    // comparing against the number of blocks in that data directory in the previous iteration. A
    // difference indicates that new blocks have been written to the directory.
    int num_paths_added_to = 0;
    int total_blocks_across_paths = 0;
    for (int path_idx = 0; path_idx < paths.size(); path_idx++) {
      int num_blocks = 0;
      ASSERT_OK(CountFiles(paths[path_idx], &num_blocks));
      int new_blocks = num_blocks - blocks_in_each_path[path_idx];
      if (new_blocks > 0) {
        num_paths_added_to++;
        total_blocks_across_paths += new_blocks;
        blocks_in_each_path[path_idx] = num_blocks;
      }
    }
    ASSERT_EQ(d * num_blocks_per_dir, total_blocks_across_paths);
    ASSERT_EQ(d, num_paths_added_to);
  }
}

template <>
void BlockManagerTest<LogBlockManager>::RunBlockDistributionTest(const vector<string>& paths) {
  vector<int> files_in_each_path(paths.size());
  int num_blocks_per_dir = 30;
  // Spread across 1, then 3, then 5 data directories.
  for (int d: { 1, 3, 5 }) {
    DistributeBlocksAcrossDirs(d, num_blocks_per_dir);

    // Check that upon each addition of new paths to data dir groups, new files are being created.
    // Since log blocks are placed and used randomly within a data dir group, the only expected
    // behavior is that the total number of files and the number of paths with files will increase.
    bool some_new_files = false;
    bool some_new_paths = false;
    for (int path_idx = 0; path_idx < paths.size(); path_idx++) {
      int num_files = 0;
      ASSERT_OK(CountFiles(paths[path_idx], &num_files));
      int new_files = num_files - files_in_each_path[path_idx];
      if (new_files > 0) {
        some_new_files = true;
        if (files_in_each_path[path_idx] == 0) {
          some_new_paths = true;
        }
        files_in_each_path[path_idx] = num_files;
      }
    }
    ASSERT_TRUE(some_new_paths);
    ASSERT_TRUE(some_new_files);
  }
}

template <>
void BlockManagerTest<FileBlockManager>::RunMultipathTest(const vector<string>& paths) {
  // Ensure that each path has an instance file and that it's well-formed.
  for (const string& path : paths) {
    vector<string> children;
    ASSERT_OK(env_->GetChildren(path, &children));
    ASSERT_EQ(3, children.size());
    for (const string& child : children) {
      if (child == "." || child == "..") {
        continue;
      }
      PathInstanceMetadataPB instance;
      ASSERT_OK(pb_util::ReadPBContainerFromPath(env_,
                                                 JoinPathSegments(path, child),
                                                 &instance));
    }
  }
  // Create a DataDirGroup for the data that's about to be inserted.
  // Spread the data across all 3 paths. Writing twenty blocks randomly within
  // this group should result in blocks being placed in every directory with a
  // high probability.
  CreateBlockOptions opts({ "multipath_test" });
  FLAGS_fs_target_data_dirs_per_tablet = 3;
  ASSERT_OK(bm_->dd_manager()->CreateDataDirGroup("multipath_test"));

  // Write twenty blocks.
  const char* kTestData = "test data";
  for (int i = 0; i < 20; i++) {
    unique_ptr<WritableBlock> written_block;
    ASSERT_OK(bm_->CreateBlock(opts, &written_block));
    ASSERT_OK(written_block->Append(kTestData));
    ASSERT_OK(written_block->Close());
  }

  // Each path should now have some additional block subdirectories. We
  // can't know for sure exactly how many (depends on the block IDs
  // generated), but this ensures that at least some change were made.
  int num_blocks = 0;
  for (const string& path : paths) {
    vector<string> children;
    ASSERT_OK(env_->GetChildren(path, &children));
    int blocks_in_path = 0;
    ASSERT_OK(CountFiles(path, &blocks_in_path));
    num_blocks += blocks_in_path;
  }
  ASSERT_EQ(20, num_blocks);
}

template <>
void BlockManagerTest<LogBlockManager>::RunMultipathTest(const vector<string>& paths) {
  // Write (3 * numPaths * 2) blocks, in groups of (numPaths * 2). That should
  // yield two containers per path.
  CreateBlockOptions opts({ "multipath_test" });
  FLAGS_fs_target_data_dirs_per_tablet = 3;
  ASSERT_OK(bm_->dd_manager()->CreateDataDirGroup("multipath_test"));

  const char* kTestData = "test data";
  ScopedWritableBlockCloser closer;
  // Creates (numPaths * 2) containers.
  for (int j = 0; j < paths.size() * 2; j++) {
    unique_ptr<WritableBlock> block;
    ASSERT_OK(bm_->CreateBlock(opts, &block));
    ASSERT_OK(block->Append(kTestData));
    closer.AddBlock(std::move(block));
  }
  ASSERT_OK(closer.CloseBlocks());

  // Verify the results. Each path has dot, dotdot, instance file.
  // (numPaths * 2) containers were created, each consisting of 2 files.
  // Thus, there should be a total of (numPaths * (3 + 4)) files.
  int sum = 0;
  for (const string& path : paths) {
    vector<string> children;
    ASSERT_OK(env_->GetChildren(path, &children));
    sum += children.size();
  }
  ASSERT_EQ(paths.size() * 7, sum);
}

template <>
void BlockManagerTest<FileBlockManager>::RunMemTrackerTest() {
  shared_ptr<MemTracker> tracker = MemTracker::CreateTracker(-1, "test tracker");
  ASSERT_OK(ReopenBlockManager(scoped_refptr<MetricEntity>(),
                               tracker,
                               { test_dir_ },
                               false /* create */));

  // The file block manager does not allocate memory for persistent data.
  int64_t initial_mem = tracker->consumption();
  ASSERT_EQ(initial_mem, 0);
  unique_ptr<WritableBlock> writer;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
  ASSERT_OK(writer->Close());
  ASSERT_EQ(tracker->consumption(), initial_mem);
}

template <>
void BlockManagerTest<LogBlockManager>::RunMemTrackerTest() {
  shared_ptr<MemTracker> tracker = MemTracker::CreateTracker(-1, "test tracker");
  ASSERT_OK(ReopenBlockManager(scoped_refptr<MetricEntity>(),
                               tracker,
                               { test_dir_ },
                               false /* create */));

  // The initial consumption should be non-zero due to the block map.
  int64_t initial_mem = tracker->consumption();
  ASSERT_GT(initial_mem, 0);

  // Allocating a persistent block should increase the consumption.
  unique_ptr<WritableBlock> writer;
  ASSERT_OK(bm_->CreateBlock(test_block_opts_, &writer));
  ASSERT_OK(writer->Close());
  ASSERT_GT(tracker->consumption(), initial_mem);
}

// What kinds of BlockManagers are supported?
#if defined(__linux__)
typedef ::testing::Types<FileBlockManager, LogBlockManager> BlockManagers;
#else
typedef ::testing::Types<FileBlockManager> BlockManagers;
#endif
TYPED_TEST_CASE(BlockManagerTest, BlockManagers);

// Test the entire lifecycle of a block.
TYPED_TEST(BlockManagerTest, EndToEndTest) {
  // Create a block.
  unique_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));

  // Write some data to it.
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Close());

  // Read the data back.
  unique_ptr<ReadableBlock> read_block;
  ASSERT_OK(this->bm_->OpenBlock(written_block->id(), &read_block));
  uint64_t sz;
  ASSERT_OK(read_block->Size(&sz));
  ASSERT_EQ(test_data.length(), sz);
  uint8_t scratch[test_data.length()];
  Slice data(scratch, test_data.length());
  ASSERT_OK(read_block->Read(0, &data));
  ASSERT_EQ(test_data, data);

  // Read the data back into multiple slices
  size_t size1 = 5;
  uint8_t scratch1[size1];
  Slice data1(scratch1, size1);
  size_t size2 = 4;
  uint8_t scratch2[size2];
  Slice data2(scratch2, size2);
  vector<Slice> results = { data1, data2 };
  ASSERT_OK(read_block->ReadV(0, &results));
  ASSERT_EQ(test_data.substr(0, size1), data1);
  ASSERT_EQ(test_data.substr(size1, size2), data2);

  // We don't actually do anything with the result of this call; we just want
  // to make sure it doesn't trigger a crash (see KUDU-1931).
  LOG(INFO) << "Block memory footprint: " << read_block->memory_footprint();

  // Delete the block.
  ASSERT_OK(this->bm_->DeleteBlock(written_block->id()));
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), nullptr)
              .IsNotFound());
}

TYPED_TEST(BlockManagerTest, CreateBlocksInDataDirs) {
  // Create a block before creating a data dir group.
  CreateBlockOptions fake_block_opts({ "fake_tablet_name" });
  Status s = this->bm_->CreateBlock(fake_block_opts, nullptr);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Tried to get directory but no "
                                    "DataDirGroup registered for tablet");

  // Ensure the data dir groups can only be created once.
  s = this->bm_->dd_manager()->CreateDataDirGroup(this->test_tablet_name_);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Tried to create DataDirGroup for tablet "
                                    "but one is already registered");

  DataDirGroupPB test_group_pb;
  // Check that the in-memory DataDirGroup did not change.
  ASSERT_TRUE(this->bm_->dd_manager()->GetDataDirGroupPB(
      this->test_tablet_name_, &test_group_pb));
  ASSERT_TRUE(MessageDifferencer::Equals(test_group_pb, this->test_group_pb_));
}

// Test that we can still read from an opened block after deleting it
// (even if we can't open it again).
TYPED_TEST(BlockManagerTest, ReadAfterDeleteTest) {
  // Write a new block.
  unique_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Close());

  // Open it for reading, then delete it. Subsequent opens should fail.
  unique_ptr<ReadableBlock> read_block;
  ASSERT_OK(this->bm_->OpenBlock(written_block->id(), &read_block));
  ASSERT_OK(this->bm_->DeleteBlock(written_block->id()));
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), nullptr)
              .IsNotFound());

  // But we should still be able to read from the opened block.
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  Slice data(scratch.get(), test_data.length());
  ASSERT_OK(read_block->Read(0, &data));
  ASSERT_EQ(test_data, data);
}

TYPED_TEST(BlockManagerTest, CloseTwiceTest) {
  // Create a new block and close it repeatedly.
  unique_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  ASSERT_OK(written_block->Close());
  ASSERT_OK(written_block->Close());

  // Open it for reading and close it repeatedly.
  unique_ptr<ReadableBlock> read_block;
  ASSERT_OK(this->bm_->OpenBlock(written_block->id(), &read_block));
  ASSERT_OK(read_block->Close());
  ASSERT_OK(read_block->Close());
}

TYPED_TEST(BlockManagerTest, CloseManyBlocksTest) {
  const int kNumBlocks = 1000;

  if (!AllowSlowTests()) {
    LOG(INFO) << "Not running in slow-tests mode";
    return;
  }

  // Disable preallocation for this test as it creates many containers.
  FLAGS_log_container_preallocate_bytes = 0;

  Random rand(SeedRandom());
  vector<WritableBlock*> dirty_blocks;
  ElementDeleter deleter(&dirty_blocks);
  LOG_TIMING(INFO, Substitute("creating $0 blocks", kNumBlocks)) {
    for (int i = 0; i < kNumBlocks; i++) {
      // Create a block.
      unique_ptr<WritableBlock> written_block;
      ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));

      // Write 64k bytes of random data into it.
      uint8_t data[65536];
      for (int i = 0; i < sizeof(data); i += sizeof(uint32_t)) {
        data[i] = rand.Next();
      }
      written_block->Append(Slice(data, sizeof(data)));

      dirty_blocks.push_back(written_block.release());
    }
  }

  LOG_TIMING(INFO, Substitute("closing $0 blocks", kNumBlocks)) {
    ASSERT_OK(this->bm_->CloseBlocks(dirty_blocks));
  }
}

// We can't really test that FlushDataAsync() "works", but we can test that
// it doesn't break anything.
TYPED_TEST(BlockManagerTest, FlushDataAsyncTest) {
  unique_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->FlushDataAsync());
}

TYPED_TEST(BlockManagerTest, WritableBlockStateTest) {
  unique_ptr<WritableBlock> written_block;

  // Common flow: CLEAN->DIRTY->CLOSED.
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  ASSERT_EQ(WritableBlock::CLEAN, written_block->state());
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_EQ(WritableBlock::DIRTY, written_block->state());
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_EQ(WritableBlock::DIRTY, written_block->state());
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test FLUSHING->CLOSED transition.
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->FlushDataAsync());
  ASSERT_EQ(WritableBlock::FLUSHING, written_block->state());
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test CLEAN->CLOSED transition.
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test FlushDataAsync() no-op.
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  ASSERT_OK(written_block->FlushDataAsync());
  ASSERT_EQ(WritableBlock::FLUSHING, written_block->state());

  // Test DIRTY->CLOSED transition.
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());
}

static void CheckMetrics(const scoped_refptr<MetricEntity>& metrics,
                         int blocks_open_reading, int blocks_open_writing,
                         int total_readable_blocks, int total_writable_blocks,
                         int total_bytes_read, int total_bytes_written) {
  ASSERT_EQ(blocks_open_reading, down_cast<AtomicGauge<uint64_t>*>(
                metrics->FindOrNull(METRIC_block_manager_blocks_open_reading).get())->value());
  ASSERT_EQ(blocks_open_writing, down_cast<AtomicGauge<uint64_t>*>(
                metrics->FindOrNull(METRIC_block_manager_blocks_open_writing).get())->value());
  ASSERT_EQ(total_readable_blocks, down_cast<Counter*>(
                metrics->FindOrNull(METRIC_block_manager_total_readable_blocks).get())->value());
  ASSERT_EQ(total_writable_blocks, down_cast<Counter*>(
                metrics->FindOrNull(METRIC_block_manager_total_writable_blocks).get())->value());
  ASSERT_EQ(total_bytes_read, down_cast<Counter*>(
                metrics->FindOrNull(METRIC_block_manager_total_bytes_read).get())->value());
  ASSERT_EQ(total_bytes_written, down_cast<Counter*>(
                metrics->FindOrNull(METRIC_block_manager_total_bytes_written).get())->value());
}

TYPED_TEST(BlockManagerTest, AbortTest) {
  MetricRegistry registry;
  scoped_refptr<MetricEntity> entity = METRIC_ENTITY_server.Instantiate(&registry, "test");
  ASSERT_OK(this->ReopenBlockManager(entity,
                                     shared_ptr<MemTracker>(),
                                     { this->test_dir_ },
                                     false /* create */));

  unique_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Abort());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), nullptr)
              .IsNotFound());

  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->FlushDataAsync());
  ASSERT_OK(written_block->Abort());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), nullptr)
              .IsNotFound());

  ASSERT_NO_FATAL_FAILURE(CheckMetrics(entity, 0, 0, 0, 2, 0, test_data.size() * 2));
}

TYPED_TEST(BlockManagerTest, PersistenceTest) {
  // Create three blocks:
  // 1. Empty.
  // 2. Non-empty.
  // 3. Deleted.
  unique_ptr<WritableBlock> written_block1;
  unique_ptr<WritableBlock> written_block2;
  unique_ptr<WritableBlock> written_block3;
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block1));
  ASSERT_OK(written_block1->Close());
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block2));
  string test_data = "test data";
  ASSERT_OK(written_block2->Append(test_data));
  ASSERT_OK(written_block2->Close());
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block3));
  ASSERT_OK(written_block3->Append(test_data));
  ASSERT_OK(written_block3->Close());
  ASSERT_OK(this->bm_->DeleteBlock(written_block3->id()));

  // Reopen the block manager. This may read block metadata from disk.
  //
  // The existing block manager is left open, which proxies for the process
  // having crashed without cleanly shutting down the block manager. The
  // on-disk metadata should still be clean.
  gscoped_ptr<BlockManager> new_bm(this->CreateBlockManager(
      scoped_refptr<MetricEntity>(),
      MemTracker::CreateTracker(-1, "other tracker"),
      { this->test_dir_ }));
  ASSERT_OK(new_bm->Open(nullptr));

  // Test that the state of all three blocks is properly reflected.
  unique_ptr<ReadableBlock> read_block;
  ASSERT_OK(new_bm->OpenBlock(written_block1->id(), &read_block));
  uint64_t sz;
  ASSERT_OK(read_block->Size(&sz));
  ASSERT_EQ(0, sz);
  ASSERT_OK(read_block->Close());
  ASSERT_OK(new_bm->OpenBlock(written_block2->id(), &read_block));
  ASSERT_OK(read_block->Size(&sz));
  ASSERT_EQ(test_data.length(), sz);
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  Slice data(scratch.get(), test_data.length());
  ASSERT_OK(read_block->Read(0, &data));
  ASSERT_EQ(test_data, data);
  ASSERT_OK(read_block->Close());
  ASSERT_TRUE(new_bm->OpenBlock(written_block3->id(), nullptr)
              .IsNotFound());
}

TYPED_TEST(BlockManagerTest, BlockDistributionTest) {
  vector<string> paths;
  for (int i = 0; i < 5; i++) {
    paths.push_back(this->GetTestPath(Substitute("block_dist_path$0", i)));
  }
  ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                     shared_ptr<MemTracker>(),
                                     paths,
                                     true /* create */,
                                     false /* load_test_group */));
  ASSERT_NO_FATAL_FAILURE(this->RunBlockDistributionTest(paths));
}

TYPED_TEST(BlockManagerTest, MultiPathTest) {
  // Recreate the block manager with three paths.
  vector<string> paths;
  for (int i = 0; i < 3; i++) {
    paths.push_back(this->GetTestPath(Substitute("path$0", i)));
  }
  ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                     shared_ptr<MemTracker>(),
                                     paths,
                                     true /* create */,
                                     false /* load_test_group */));

  ASSERT_NO_FATAL_FAILURE(this->RunMultipathTest(paths));
}

static void CloseHelper(ReadableBlock* block) {
  CHECK_OK(block->Close());
}

// Tests that ReadableBlock::Close() is thread-safe and idempotent.
TYPED_TEST(BlockManagerTest, ConcurrentCloseReadableBlockTest) {
  unique_ptr<WritableBlock> writer;
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &writer));
  ASSERT_OK(writer->Close());

  unique_ptr<ReadableBlock> reader;
  ASSERT_OK(this->bm_->OpenBlock(writer->id(), &reader));

  vector<scoped_refptr<Thread> > threads;
  for (int i = 0; i < 100; i++) {
    scoped_refptr<Thread> t;
    ASSERT_OK(Thread::Create("test", Substitute("t$0", i),
                             &CloseHelper, reader.get(), &t));
    threads.push_back(t);
  }
  for (const scoped_refptr<Thread>& t : threads) {
    t->Join();
  }
}

TYPED_TEST(BlockManagerTest, MetricsTest) {
  const string kTestData = "test data";
  MetricRegistry registry;
  scoped_refptr<MetricEntity> entity = METRIC_ENTITY_server.Instantiate(&registry, "test");
  ASSERT_OK(this->ReopenBlockManager(entity,
                                     shared_ptr<MemTracker>(),
                                     { this->test_dir_ },
                                     false));
  ASSERT_NO_FATAL_FAILURE(CheckMetrics(entity, 0, 0, 0, 0, 0, 0));

  for (int i = 0; i < 3; i++) {
    unique_ptr<WritableBlock> writer;
    unique_ptr<ReadableBlock> reader;

    // An open writer. Also reflected in total_writable_blocks.
    ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &writer));
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        entity, 0, 1, i, i + 1,
        i * kTestData.length(), i * kTestData.length()));

    // Block is no longer opened for writing, but its data
    // is now reflected in total_bytes_written.
    ASSERT_OK(writer->Append(kTestData));
    ASSERT_OK(writer->Close());
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        entity, 0, 0, i, i + 1,
        i * kTestData.length(), (i + 1) * kTestData.length()));

    // An open reader.
    ASSERT_OK(this->bm_->OpenBlock(writer->id(), &reader));
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        entity, 1, 0, i + 1, i + 1,
        i * kTestData.length(), (i + 1) * kTestData.length()));

    // The read is reflected in total_bytes_read.
    gscoped_ptr<uint8_t[]> scratch(new uint8_t[kTestData.length()]);
    Slice data(scratch.get(), kTestData.length());
    ASSERT_OK(reader->Read(0, &data));
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        entity, 1, 0, i + 1, i + 1,
        (i + 1) * kTestData.length(), (i + 1) * kTestData.length()));

    // The reader is now gone.
    ASSERT_OK(reader->Close());
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        entity, 0, 0, i + 1, i + 1,
        (i + 1) * kTestData.length(), (i + 1) * kTestData.length()));
  }
}

TYPED_TEST(BlockManagerTest, MemTrackerTest) {
  ASSERT_NO_FATAL_FAILURE(this->RunMemTrackerTest());
}

TYPED_TEST(BlockManagerTest, TestDiskSpaceCheck) {
  // Reopen the block manager with metrics enabled.
  MetricRegistry registry;
  scoped_refptr<MetricEntity> entity = METRIC_ENTITY_server.Instantiate(&registry, "test");
  ASSERT_OK(this->ReopenBlockManager(entity,
                                     shared_ptr<MemTracker>(),
                                     { GetTestDataDirectory() },
                                     false /* create */));

  FLAGS_fs_data_dirs_full_disk_cache_seconds = 0; // Don't cache device fullness.
  FLAGS_fs_data_dirs_reserved_bytes = 1; // Keep at least 1 byte reserved in the FS.
  FLAGS_log_container_preallocate_bytes = 0; // Disable preallocation.
  FLAGS_fs_target_data_dirs_per_tablet = 3; // Use a subset of directories instead of all.

  // Normally, a data dir is checked for fullness only after a block is closed;
  // if it's now full, the next attempt at block creation will fail. Only when
  // a data dir was last observed as full is it also checked before block creation.
  //
  // This behavior enforces a "soft" limit on disk space consumption but
  // complicates testing somewhat.
  bool data_dir_observed_full = false;

  int i = 0;
  for (int free_space : { 0, 2, 0 }) {
    FLAGS_disk_reserved_bytes_free_for_testing = free_space;

    for (int attempt = 0; attempt < 3; attempt++) {
      unique_ptr<WritableBlock> writer;
      LOG(INFO) << "Attempt #" << ++i;
      Status s = this->bm_->CreateBlock(this->test_block_opts_, &writer);
      if (FLAGS_disk_reserved_bytes_free_for_testing < FLAGS_fs_data_dirs_reserved_bytes) {
        if (data_dir_observed_full) {
          // The dir was previously observed as full, so CreateBlock() checked
          // fullness again and failed.
          ASSERT_TRUE(s.IsIOError()) << s.ToString();
          ASSERT_STR_CONTAINS(s.ToString(), "All data directories are full");
        } else {
          ASSERT_OK(s);
          ASSERT_OK(writer->Append("test data"));
          ASSERT_OK(writer->Close());

          // The dir was not previously full so CreateBlock() did not check for
          // fullness, but given the parameters of the test, we know that the
          // dir was observed as full at Close().
          data_dir_observed_full = true;
        }
        ASSERT_EQ(1, down_cast<AtomicGauge<uint64_t>*>(
            entity->FindOrNull(METRIC_data_dirs_full).get())->value());
      } else {
        // CreateBlock() succeeded regardless of the previously fullness state,
        // and the new state is definitely not full.
        ASSERT_OK(s);
        ASSERT_OK(writer->Append("test data"));
        ASSERT_OK(writer->Close());
        data_dir_observed_full = false;
        ASSERT_EQ(0, down_cast<AtomicGauge<uint64_t>*>(
            entity->FindOrNull(METRIC_data_dirs_full).get())->value());
      }
    }
  }
}

// Regression test for KUDU-1793.
TYPED_TEST(BlockManagerTest, TestMetadataOkayDespiteFailedWrites) {
  const int kNumTries = 3;
  const int kNumBlockTries = 1000;
  const int kNumAppends = 4;
  const string kTestData = "asdf";

  // Since we're appending so little data, reconfigure these to ensure quite a
  // few containers and a good amount of preallocating.
  FLAGS_log_container_max_size = 256 * 1024;
  FLAGS_log_container_preallocate_bytes = 8 * 1024;

  // Force some file operations to fail.
  FLAGS_suicide_on_eio = false;
  FLAGS_env_inject_eio = 0.1;

  // Compact log block manager metadata aggressively at startup; injected
  // errors may also crop up here.
  FLAGS_log_container_live_metadata_before_compact_ratio = 0.5;

  // Creates a block, writing the result to 'out' on success.
  auto create_a_block = [&](BlockId* out) -> Status {
    unique_ptr<WritableBlock> block;
    RETURN_NOT_OK(this->bm_->CreateBlock(this->test_block_opts_, &block));
    for (int i = 0; i < kNumAppends; i++) {
      RETURN_NOT_OK(block->Append(kTestData));
    }
    RETURN_NOT_OK(block->Close());
    *out = block->id();
    return Status::OK();
  };

  // Reads a block given by 'id', comparing its contents to kTestData.
  auto read_a_block = [&](const BlockId& id) -> Status {
    unique_ptr<ReadableBlock> block;
    RETURN_NOT_OK(this->bm_->OpenBlock(id, &block));
    uint64_t size;
    RETURN_NOT_OK(block->Size(&size));
    CHECK_EQ(kNumAppends * kTestData.size(), size);

    for (int i = 0; i < kNumAppends; i++) {
      uint8_t buf[kTestData.size()];
      Slice s(buf, kTestData.size());
      RETURN_NOT_OK(block->Read(i * kNumAppends, &s));
      CHECK_EQ(kTestData, s);
    }
    return Status::OK();
  };

  // For each iteration:
  // 1. Try to create kNumTries new blocks.
  // 2. Try to delete every other block.
  // 3. Read and test every block.
  // 4. Restart the block manager, forcing the on-disk metadata to be reloaded.
  BlockIdSet ids;
  for (int attempt = 0; attempt < kNumTries; attempt++) {
    int num_created = 0;
    for (int i = 0; i < kNumBlockTries; i++) {
      BlockId id;
      Status s = create_a_block(&id);
      if (s.ok()) {
        InsertOrDie(&ids, id);
        num_created++;
      }
    }
    LOG(INFO) << Substitute("Successfully created $0 blocks on $1 attempts",
                            num_created, kNumBlockTries);

    int num_deleted = 0;
    int num_deleted_attempts = 0;
    for (auto it = ids.begin(); it != ids.end();) {
      // TODO(adar): the lbm removes a block from its block map even if the
      // on-disk deletion fails. When that's fixed, update this code to
      // erase() only if s.ok().
      Status s = this->bm_->DeleteBlock(*it);
      it = ids.erase(it);
      if (s.ok()) {
        num_deleted++;
      }
      num_deleted_attempts++;

      // Skip every other block.
      if (it != ids.end()) {
        it++;
      }
    }
    LOG(INFO) << Substitute("Successfully deleted $0 blocks on $1 attempts",
                            num_deleted, num_deleted_attempts);

    {
      // Since this test is for failed writes, don't inject faults during reads
      // or while reopening the block manager.
      google::FlagSaver saver;
      FLAGS_env_inject_eio = 0;
      for (const auto& id : ids) {
        ASSERT_OK(read_a_block(id));
      }
      ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                         shared_ptr<MemTracker>(),
                                         { GetTestDataDirectory() },
                                         false /* create */));
    }
  }
  FLAGS_env_inject_eio = 0;
}

TYPED_TEST(BlockManagerTest, TestGetAllBlockIds) {
  vector<BlockId> ids;
  for (int i = 0; i < 100; i++) {
    unique_ptr<WritableBlock> block;
    ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &block));
    ASSERT_OK(block->Close());
    ids.push_back(block->id());
  }

  // The file block manager should skip these; they shouldn't appear in
  // 'retrieved_ids' below.
  for (const auto& s : { string("abcde"), // not numeric
                         string("12345"), // not a real block ID
                         ids.begin()->ToString() }) { // not in a block directory
    unique_ptr<WritableFile> writer;
    ASSERT_OK(this->env_->NewWritableFile(
        JoinPathSegments(this->test_dir_, s), &writer));
    ASSERT_OK(writer->Close());
  }

  vector<BlockId> retrieved_ids;
  ASSERT_OK(this->bm_->GetAllBlockIds(&retrieved_ids));

  // Sort the two collections before the comparison as GetAllBlockIds() does
  // not guarantee a deterministic order.
  std::sort(ids.begin(), ids.end(), BlockIdCompare());
  std::sort(retrieved_ids.begin(), retrieved_ids.end(), BlockIdCompare());

  ASSERT_EQ(ids, retrieved_ids);
}

} // namespace fs
} // namespace kudu
