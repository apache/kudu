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

#include "kudu/fs/block_manager.h"

#include <stdlib.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "kudu/fs/block_id.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/file_block_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_report.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/array_view.h" // IWYU pragma: keep
#include "kudu/util/env.h"
#include "kudu/util/file_cache.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using google::protobuf::util::MessageDifferencer;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DECLARE_double(log_container_live_metadata_before_compact_ratio);
DECLARE_uint64(log_container_preallocate_bytes);
DECLARE_uint64(log_container_max_size);
DECLARE_int64(fs_data_dirs_reserved_bytes);
DECLARE_int64(disk_reserved_bytes_free_for_testing);
DECLARE_int32(fs_data_dirs_available_space_cache_seconds);
DECLARE_int32(fs_target_data_dirs_per_tablet);
DECLARE_string(block_manager);
DECLARE_double(env_inject_eio);
DECLARE_bool(crash_on_eio);

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

template<typename T>
string block_manager_type();

template<>
string block_manager_type<FileBlockManager>() { return "file"; }

template<>
string block_manager_type<LogBlockManager>() { return "log"; }

template <typename T>
class BlockManagerTest : public KuduTest {
 public:
  BlockManagerTest() :
      test_tablet_name_("test_tablet"),
      test_block_opts_(CreateBlockOptions({ test_tablet_name_ })),
      file_cache_("test_cache", env_, 1, scoped_refptr<MetricEntity>()),
      bm_(CreateBlockManager(scoped_refptr<MetricEntity>(),
                             shared_ptr<MemTracker>())) {
    CHECK_OK(file_cache_.Init());
  }

  virtual void SetUp() override {
    // Pass in a report to prevent the block manager from logging unnecessarily.
    FsReport report;
    ASSERT_OK(bm_->Open(&report));
    ASSERT_OK(dd_manager_->CreateDataDirGroup(test_tablet_name_));
    ASSERT_OK(dd_manager_->GetDataDirGroupPB(test_tablet_name_, &test_group_pb_));
  }

  void DistributeBlocksAcrossDirs(int num_dirs, int num_blocks_per_dir) {
    // Create a data directory group that contains 'num_dirs' data directories.
    string tablet_name = Substitute("$0_disks", num_dirs);
    CreateBlockOptions opts({ tablet_name });
    FLAGS_fs_target_data_dirs_per_tablet = num_dirs;
    ASSERT_OK(dd_manager_->CreateDataDirGroup(tablet_name));
    int num_blocks = num_dirs * num_blocks_per_dir;

    // Write 'num_blocks' blocks to this data dir group.
    unique_ptr<BlockCreationTransaction> transaction = bm_->NewCreationTransaction();
    for (int i = 0; i < num_blocks; i++) {
      unique_ptr<WritableBlock> written_block;
      ASSERT_OK(bm_->CreateBlock(opts, &written_block));
      ASSERT_OK(written_block->Append(kTestData));
      transaction->AddCreatedBlock(std::move(written_block));
    }
    ASSERT_OK(transaction->CommitCreatedBlocks());
  }

 protected:

  T* CreateBlockManager(const scoped_refptr<MetricEntity>& metric_entity,
                        const shared_ptr<MemTracker>& parent_mem_tracker) {
    if (!dd_manager_) {
      DataDirManagerOptions opts;
      opts.dir_type = block_manager_type<T>();
      // Create a new directory manager if necessary.
      CHECK_OK(DataDirManager::CreateNewForTests(env_, { test_dir_ },
          opts, &dd_manager_));
    }
    BlockManagerOptions opts;
    opts.metric_entity = metric_entity;
    opts.parent_mem_tracker = parent_mem_tracker;
    return new T(env_, this->dd_manager_.get(), &error_manager_,
                 &file_cache_, std::move(opts));
  }

  Status ReopenBlockManager(const scoped_refptr<MetricEntity>& metric_entity,
                            const shared_ptr<MemTracker>& parent_mem_tracker,
                            const vector<string>& paths,
                            bool create,
                            bool load_test_group = true) {
    // The directory manager must outlive the block manager. Destroy the block
    // manager first to enforce this.
    bm_.reset();
    DataDirManagerOptions opts;
    opts.dir_type = block_manager_type<T>();
    opts.metric_entity = metric_entity;
    if (create) {
      RETURN_NOT_OK(DataDirManager::CreateNewForTests(
          env_, paths, opts, &dd_manager_));
    } else {
      RETURN_NOT_OK(DataDirManager::OpenExistingForTests(
          env_, paths, opts, &dd_manager_));
    }
    bm_.reset(CreateBlockManager(metric_entity, parent_mem_tracker));
    RETURN_NOT_OK(bm_->Open(nullptr));

    // Certain tests may maintain their own directory groups, in which case
    // the default test group should not be used.
    if (load_test_group) {
      RETURN_NOT_OK(dd_manager_->LoadDataDirGroupFromPB(test_tablet_name_, test_group_pb_));
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
    return env_->Walk(
        root, Env::PRE_ORDER,
        [num_files](Env::FileType type, const string& dirname, const string& basename) {
          return CountFilesCb(num_files, type, dirname, basename);
        });
  }

  // Keep an internal copy of the data dir group to act as metadata.
  DataDirGroupPB test_group_pb_;
  string test_tablet_name_;
  CreateBlockOptions test_block_opts_;
  FsErrorManager error_manager_;
  unique_ptr<DataDirManager> dd_manager_;
  FileCache file_cache_;
  unique_ptr<T> bm_;
};

template <>
void BlockManagerTest<LogBlockManager>::SetUp() {
  RETURN_NOT_LOG_BLOCK_MANAGER();
  // Pass in a report to prevent the block manager from logging unnecessarily.
  FsReport report;
  ASSERT_OK(bm_->Open(&report));
  ASSERT_OK(dd_manager_->CreateDataDirGroup(test_tablet_name_));

  // Store the DataDirGroupPB for tests that reopen the block manager.
  ASSERT_OK(dd_manager_->GetDataDirGroupPB(test_tablet_name_, &test_group_pb_));
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
  for (const string& path : dd_manager_->GetDirs()) {
    vector<string> children;
    ASSERT_OK(env_->GetChildren(path, &children));
    ASSERT_EQ(3, children.size());
    for (const string& child : children) {
      if (child == "." || child == "..") {
        continue;
      }
      DirInstanceMetadataPB instance;
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
  ASSERT_OK(dd_manager_->CreateDataDirGroup("multipath_test"));

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
  ASSERT_OK(dd_manager_->CreateDataDirGroup("multipath_test"));

  const char* kTestData = "test data";
  unique_ptr<BlockCreationTransaction> transaction = bm_->NewCreationTransaction();
  // Creates (numPaths * 2) containers.
  for (int j = 0; j < paths.size() * 2; j++) {
    unique_ptr<WritableBlock> block;
    ASSERT_OK(bm_->CreateBlock(opts, &block));
    ASSERT_OK(block->Append(kTestData));
    transaction->AddCreatedBlock(std::move(block));
  }
  ASSERT_OK(transaction->CommitCreatedBlocks());

  // Verify the results. (numPaths * 2) containers were created, each
  // consisting of 2 files. Thus, there should be a total of
  // (numPaths * 4) files, ignoring '.', '..', and instance files.
  int sum = 0;
  for (const string& path : paths) {
    vector<string> children;
    ASSERT_OK(env_->GetChildren(path, &children));
    int files_in_path = 0;
    ASSERT_OK(CountFiles(path, &files_in_path));
    sum += files_in_path;
  }
  ASSERT_EQ(paths.size() * 4, sum);
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

// Test to make sure that we don't break the file block manager, which depends
// on a static set of directories to function properly. Internally, the
// DataDirManager of a file block manager must use a specific ordering for its
// directory UUID indexes which is persisted with the PIMFs.
TYPED_TEST(BlockManagerTest, TestOpenWithDifferentDirOrder) {
  const string path1 = this->GetTestPath("path1");
  const string path2 = this->GetTestPath("path2");
  vector<string> paths = { path1, path2 };
  ASSERT_OK(this->env_->CreateDir(path1));
  ASSERT_OK(this->env_->CreateDir(path2));
  ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                     shared_ptr<MemTracker>(),
                                     paths,
                                     true /* create */,
                                     false /* load_test_group */));

  const string kTablet = "tablet";
  CreateBlockOptions opts({ kTablet });
  FLAGS_fs_target_data_dirs_per_tablet = 2;
  ASSERT_OK(this->dd_manager_->CreateDataDirGroup(kTablet));

  // Create a block and keep track of its block id.
  unique_ptr<BlockCreationTransaction> transaction = this->bm_->NewCreationTransaction();
  unique_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(opts, &written_block));
  const auto block_id = written_block->id();
  ASSERT_OK(written_block->Append(kTestData));
  transaction->AddCreatedBlock(std::move(written_block));
  ASSERT_OK(transaction->CommitCreatedBlocks());

  // Now reopen the block manager with a different ordering for the data
  // directories.
  paths = { path2, path1 };
  ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                     shared_ptr<MemTracker>(),
                                     paths,
                                     false /* create */,
                                     false /* load_test_group */));

  // We should have no trouble reading the block back.
  unique_ptr<ReadableBlock> read_block;
  ASSERT_OK(this->bm_->OpenBlock(block_id, &read_block));
}

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
  ASSERT_OK(read_block->Read(0, data));
  ASSERT_EQ(test_data, data);

  // Read the data back into multiple slices
  size_t size1 = 5;
  uint8_t scratch1[size1];
  Slice data1(scratch1, size1);
  size_t size2 = 4;
  uint8_t scratch2[size2];
  Slice data2(scratch2, size2);
  vector<Slice> results = { data1, data2 };
  ASSERT_OK(read_block->ReadV(0, results));
  ASSERT_EQ(test_data.substr(0, size1), data1);
  ASSERT_EQ(test_data.substr(size1, size2), data2);

  // We don't actually do anything with the result of this call; we just want
  // to make sure it doesn't trigger a crash (see KUDU-1931).
  LOG(INFO) << "Block memory footprint: " << read_block->memory_footprint();

  // Delete the block.
  shared_ptr<BlockDeletionTransaction> deletion_transaction =
      this->bm_->NewDeletionTransaction();
  deletion_transaction->AddDeletedBlock(written_block->id());
  vector<BlockId> deleted;
  ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted));
  ASSERT_EQ(1, deleted.size());
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), nullptr)
              .IsNotFound());
}

TYPED_TEST(BlockManagerTest, CreateBlocksInDataDirs) {
  // Create a block before creating a data dir group.
  CreateBlockOptions fake_block_opts({ "fake_tablet_name" });
  Status s = this->bm_->CreateBlock(fake_block_opts, nullptr);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Tried to get directory but no "
                                    "directory group registered for tablet");

  // Ensure the data dir groups can only be created once.
  s = this->dd_manager_->CreateDataDirGroup(this->test_tablet_name_);
  ASSERT_TRUE(s.IsAlreadyPresent()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Tried to create directory group for tablet "
                                    "but one is already registered");

  DataDirGroupPB test_group_pb;
  // Check that the in-memory DataDirGroup did not change.
  ASSERT_OK(this->dd_manager_->GetDataDirGroupPB(
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
  shared_ptr<BlockDeletionTransaction> deletion_transaction =
      this->bm_->NewDeletionTransaction();
  deletion_transaction->AddDeletedBlock(written_block->id());
  vector<BlockId> deleted;
  ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted));
  ASSERT_EQ(1, deleted.size());
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), nullptr)
              .IsNotFound());

  // But we should still be able to read from the opened block.
  unique_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  Slice data(scratch.get(), test_data.length());
  ASSERT_OK(read_block->Read(0, data));
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

  Random rand(SeedRandom());
  unique_ptr<BlockCreationTransaction> creation_transaction =
      this->bm_->NewCreationTransaction();
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
      written_block->Finalize();
      creation_transaction->AddCreatedBlock(std::move(written_block));
    }
  }

  LOG_TIMING(INFO, Substitute("closing $0 blocks", kNumBlocks)) {
    ASSERT_OK(creation_transaction->CommitCreatedBlocks());
  }
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

  // Test FINALIZED->CLOSED transition.
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Finalize());
  ASSERT_EQ(WritableBlock::FINALIZED, written_block->state());
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test CLEAN->CLOSED transition.
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test Finalize() no-op.
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  ASSERT_OK(written_block->Finalize());
  ASSERT_EQ(WritableBlock::FINALIZED, written_block->state());

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
  ASSERT_OK(written_block->Finalize());
  ASSERT_OK(written_block->Abort());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), nullptr)
              .IsNotFound());

  NO_FATALS(CheckMetrics(entity, 0, 0, 0, 2, 0, test_data.size() * 2));
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
  {
    shared_ptr<BlockDeletionTransaction> deletion_transaction =
        this->bm_->NewDeletionTransaction();
    deletion_transaction->AddDeletedBlock(written_block3->id());
    vector<BlockId> deleted;
    ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted));
    ASSERT_EQ(1, deleted.size());
  }

  // Reopen the block manager. This may read block metadata from disk.
  //
  // The existing block manager is left open, which proxies for the process
  // having crashed without cleanly shutting down the block manager. The
  // on-disk metadata should still be clean.
  unique_ptr<BlockManager> new_bm(this->CreateBlockManager(
      scoped_refptr<MetricEntity>(),
      MemTracker::CreateTracker(-1, "other tracker")));
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
  unique_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  Slice data(scratch.get(), test_data.length());
  ASSERT_OK(read_block->Read(0, data));
  ASSERT_EQ(test_data, data);
  ASSERT_OK(read_block->Close());
  ASSERT_TRUE(new_bm->OpenBlock(written_block3->id(), nullptr)
              .IsNotFound());
}

TYPED_TEST(BlockManagerTest, BlockDistributionTest) {
  vector<string> paths;
  for (int i = 0; i < 5; i++) {
    paths.push_back(this->GetTestPath(Substitute("block_dist_path$0", i)));
    ASSERT_OK(this->env_->CreateDir(paths[i]));
  }
  ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                     shared_ptr<MemTracker>(),
                                     paths,
                                     true /* create */,
                                     false /* load_test_group */));
  NO_FATALS(this->RunBlockDistributionTest(paths));
}

TYPED_TEST(BlockManagerTest, MultiPathTest) {
  // Recreate the block manager with three paths.
  vector<string> paths;
  for (int i = 0; i < 3; i++) {
    paths.push_back(this->GetTestPath(Substitute("path$0", i)));
    ASSERT_OK(this->env_->CreateDir(paths[i]));
  }
  ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                     shared_ptr<MemTracker>(),
                                     paths,
                                     true /* create */,
                                     false /* load_test_group */));

  NO_FATALS(this->RunMultipathTest(paths));
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

  constexpr int kNumThreads = 100;
  vector<thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&reader]() { CloseHelper(reader.get()); });
  }
  for (auto& t : threads) {
    t.join();
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
  NO_FATALS(CheckMetrics(entity, 0, 0, 0, 0, 0, 0));

  for (int i = 0; i < 3; i++) {
    unique_ptr<WritableBlock> writer;
    unique_ptr<ReadableBlock> reader;

    // An open writer. Also reflected in total_writable_blocks.
    ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &writer));
    NO_FATALS(CheckMetrics(
        entity, 0, 1, i, i + 1,
        i * kTestData.length(), i * kTestData.length()));

    // Block is no longer opened for writing, but its data
    // is now reflected in total_bytes_written.
    ASSERT_OK(writer->Append(kTestData));
    ASSERT_OK(writer->Close());
    NO_FATALS(CheckMetrics(
        entity, 0, 0, i, i + 1,
        i * kTestData.length(), (i + 1) * kTestData.length()));

    // An open reader.
    ASSERT_OK(this->bm_->OpenBlock(writer->id(), &reader));
    NO_FATALS(CheckMetrics(
        entity, 1, 0, i + 1, i + 1,
        i * kTestData.length(), (i + 1) * kTestData.length()));

    // The read is reflected in total_bytes_read.
    unique_ptr<uint8_t[]> scratch(new uint8_t[kTestData.length()]);
    Slice data(scratch.get(), kTestData.length());
    ASSERT_OK(reader->Read(0, data));
    NO_FATALS(CheckMetrics(
        entity, 1, 0, i + 1, i + 1,
        (i + 1) * kTestData.length(), (i + 1) * kTestData.length()));

    // The reader is now gone.
    ASSERT_OK(reader->Close());
    NO_FATALS(CheckMetrics(
        entity, 0, 0, i + 1, i + 1,
        (i + 1) * kTestData.length(), (i + 1) * kTestData.length()));
  }
}

TYPED_TEST(BlockManagerTest, MemTrackerTest) {
  NO_FATALS(this->RunMemTrackerTest());
}

TYPED_TEST(BlockManagerTest, TestDiskSpaceCheck) {
  // Reopen the block manager with metrics enabled.
  MetricRegistry registry;
  scoped_refptr<MetricEntity> entity = METRIC_ENTITY_server.Instantiate(&registry, "test");
  ASSERT_OK(this->ReopenBlockManager(entity,
                                     shared_ptr<MemTracker>(),
                                     { GetTestDataDirectory() },
                                     false /* create */));

  FLAGS_fs_data_dirs_available_space_cache_seconds = 0; // Don't cache device available space.
  FLAGS_fs_data_dirs_reserved_bytes = 1; // Keep at least 1 byte reserved in the FS.
  FLAGS_log_container_preallocate_bytes = 0; // Disable preallocation.
  FLAGS_fs_target_data_dirs_per_tablet = 3; // Use a subset of directories instead of all.

  int i = 0;
  for (int free_space : { 0, 2 }) {
    FLAGS_disk_reserved_bytes_free_for_testing = free_space;

    for (int attempt = 0; attempt < 2; attempt++) {
      unique_ptr<WritableBlock> writer;
      LOG(INFO) << "Attempt #" << ++i;
      Status s = this->bm_->CreateBlock(this->test_block_opts_, &writer);
      if (FLAGS_disk_reserved_bytes_free_for_testing < FLAGS_fs_data_dirs_reserved_bytes) {
        // The dir was previously observed as full, so CreateBlock() checked
        // fullness again and failed.
        ASSERT_TRUE(s.IsIOError()) << s.ToString();
        ASSERT_STR_CONTAINS(
            s.ToString(), "No directories available in test_tablet's directory group");
      } else {
        // CreateBlock() succeeded regardless of the previously fullness state,
        // and the new state is definitely not full.
        ASSERT_OK(s);
        ASSERT_OK(writer->Append("test data"));
        ASSERT_OK(writer->Close());
        ASSERT_EQ(0, down_cast<AtomicGauge<uint64_t>*>(
            entity->FindOrNull(METRIC_data_dirs_full).get())->value());
      }
    }
  }
}

// Regression test for KUDU-1793.
TYPED_TEST(BlockManagerTest, TestMetadataOkayDespiteFailure) {
  const int kNumTries = 3;
  const int kNumBlockTries = 500;
  const int kNumAppends = 4;
  const string kLongTestData = string(3000, 'L');
  const string kShortTestData = string(40, 's');

  // Since we're appending so little data, reconfigure these to ensure quite a
  // few containers and a good amount of preallocating.
  FLAGS_log_container_max_size = 256 * 1024;
  FLAGS_log_container_preallocate_bytes = 8 * 1024;

  // Force some file operations to fail.
  FLAGS_crash_on_eio = false;
  FLAGS_env_inject_eio = 0.1;

  // Compact log block manager metadata aggressively at startup; injected
  // errors may also crop up here.
  FLAGS_log_container_live_metadata_before_compact_ratio = 0.5;

  // Creates a block with the given 'test_data', writing the result
  // to 'out' on success.
  auto create_a_block = [&](BlockId* out, const string& test_data) -> Status {
    unique_ptr<WritableBlock> block;
    RETURN_NOT_OK(this->bm_->CreateBlock(this->test_block_opts_, &block));
    for (int i = 0; i < kNumAppends; i++) {
      RETURN_NOT_OK(block->Append(test_data));
    }

    RETURN_NOT_OK(block->Finalize());
    RETURN_NOT_OK(block->Close());
    *out = block->id();
    return Status::OK();
  };

  // Reads a block given by 'id', comparing its contents. Note that
  // we need to compare with both kLongTestData and kShortTestData as we
  // do not know the blocks' content ahead.
  auto read_a_block = [&](const BlockId& id) -> Status {
    unique_ptr<ReadableBlock> block;
    RETURN_NOT_OK(this->bm_->OpenBlock(id, &block));
    uint64_t size;
    RETURN_NOT_OK(block->Size(&size));
    uint8_t buf[size];
    Slice slice(buf, size);
    RETURN_NOT_OK(block->Read(0, slice));

    string data = slice.ToString();
    const string* string_to_check =
        kNumAppends * kShortTestData.size() == size ? &kShortTestData : &kLongTestData;
    for (int i = 0; i < kNumAppends; i++) {
      CHECK_EQ(*string_to_check,
               data.substr(i * string_to_check->size(), string_to_check->size()));
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
      // Inject different size of data to better simulate
      // real world case.
      Status s = create_a_block(&id, i % 2 ? kShortTestData : kLongTestData);

      if (s.ok()) {
        InsertOrDie(&ids, id);
        num_created++;
      }
    }
    LOG(INFO) << Substitute("Successfully created $0 blocks on $1 attempts",
                            num_created, kNumBlockTries);

    int num_deleted = 0;
    int num_deleted_attempts = 0;
    vector<BlockId> deleted;
    {
      shared_ptr<BlockDeletionTransaction> deletion_transaction =
          this->bm_->NewDeletionTransaction();
      for (auto it = ids.begin(); it != ids.end();) {
        // TODO(adar): the lbm removes a block from its block map even if the
        // on-disk deletion fails. When that's fixed, update this code to
        // erase() only if s.ok().
        deletion_transaction->AddDeletedBlock(*it);
        it = ids.erase(it);
        num_deleted_attempts++;

        // Skip every other block.
        if (it != ids.end()) {
          it++;
        }
      }
      ignore_result(deletion_transaction->CommitDeletedBlocks(&deleted));
    }
    num_deleted += deleted.size();
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

TYPED_TEST(BlockManagerTest, FinalizeTest) {
  const string kTestData = "test data";
  unique_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &written_block));
  ASSERT_OK(written_block->Append(kTestData));
  ASSERT_OK(written_block->Finalize());
  ASSERT_OK(written_block->Close());
}

// Tests that WritableBlock::Close() is thread-safe for multiple
// FINALIZED blocks.
TYPED_TEST(BlockManagerTest, ConcurrentCloseFinalizedWritableBlockTest) {
  const string kTestData = "test data";

  // Create several blocks. For each block, Finalize() it after fully
  // written, and then Close() it.
  auto write_data = [&]() {
    for (int i = 0; i < 5; i++) {
      unique_ptr<WritableBlock> writer;
      CHECK_OK(this->bm_->CreateBlock(this->test_block_opts_, &writer));
      CHECK_OK(writer->Append(kTestData));
      CHECK_OK(writer->Finalize());
      SleepFor(MonoDelta::FromMilliseconds(rand() % 100));
      CHECK_OK(writer->Close());
    }
  };

  constexpr int kNumThreads = 100;
  vector<thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back(write_data);
  }
  for (auto& t : threads) {
    t.join();
  }

  vector<BlockId> retrieved_ids;
  ASSERT_OK(this->bm_->GetAllBlockIds(&retrieved_ids));
  ASSERT_EQ(500, retrieved_ids.size());
  for (const auto& id : retrieved_ids) {
    unique_ptr<ReadableBlock> block;
    ASSERT_OK(this->bm_->OpenBlock(id, &block));
    uint64_t size;
    ASSERT_OK(block->Size(&size));
    uint8_t buf[size];
    Slice slice(buf, size);
    ASSERT_OK(block->Read(0, slice));
    ASSERT_EQ(kTestData, slice);
  }

  ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                     shared_ptr<MemTracker>(),
                                     { GetTestDataDirectory() },
                                     false /* create */));
}

TYPED_TEST(BlockManagerTest, TestBlockTransaction) {
  // Create a BlockCreationTransaction. In this transaction,
  // create some blocks and commit the writes all together.
  const string kTestData = "test data";
  unique_ptr<BlockCreationTransaction> creation_transaction =
      this->bm_->NewCreationTransaction();
  vector<BlockId> created_blocks;
  for (int i = 0; i < 20; i++) {
    unique_ptr<WritableBlock> writer;
    ASSERT_OK(this->bm_->CreateBlock(this->test_block_opts_, &writer));

    // Write some data to it.
    ASSERT_OK(writer->Append(kTestData));
    ASSERT_OK(writer->Finalize());
    created_blocks.emplace_back(writer->id());
    creation_transaction->AddCreatedBlock(std::move(writer));
  }
  ASSERT_OK(creation_transaction->CommitCreatedBlocks());

  // Read the blocks and verify the content.
  for (const auto& block : created_blocks) {
    unique_ptr<ReadableBlock> reader;
    ASSERT_OK(this->bm_->OpenBlock(block, &reader));
    uint64_t sz;
    ASSERT_OK(reader->Size(&sz));
    ASSERT_EQ(kTestData.length(), sz);
    uint8_t scratch[kTestData.length()];
    Slice data(scratch, kTestData.length());
    ASSERT_OK(reader->Read(0, data));
    ASSERT_EQ(kTestData, data);
  }

  // Create a BlockDeletionTransaction. In this transaction,
  // randomly delete almost half of the created blocks.
  shared_ptr<BlockDeletionTransaction> deletion_transaction =
      this->bm_->NewDeletionTransaction();
  for (const auto& block : created_blocks) {
    if (rand() % 2) deletion_transaction->AddDeletedBlock(block);
  }
  vector<BlockId> deleted_blocks;
  ASSERT_OK(deletion_transaction->CommitDeletedBlocks(&deleted_blocks));
  for (const auto& block : deleted_blocks) {
    created_blocks.erase(std::remove(created_blocks.begin(), created_blocks.end(), block),
                         created_blocks.end());
    ASSERT_TRUE(this->bm_->OpenBlock(block, nullptr).IsNotFound());
  }

  // Delete the rest of created blocks. But force the operations to fail,
  // in order to test that the first failure properly propagates.
  FLAGS_crash_on_eio = false;
  FLAGS_env_inject_eio = 1.0;
  for (const auto& block : created_blocks) {
    deletion_transaction->AddDeletedBlock(block);
  }
  deleted_blocks.clear();
  Status s = deletion_transaction->CommitDeletedBlocks(&deleted_blocks);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STR_CONTAINS(s.ToString(), Substitute("only deleted $0 blocks, "
                                               "first failure",
                                               deleted_blocks.size()));
}

} // namespace fs
} // namespace kudu
