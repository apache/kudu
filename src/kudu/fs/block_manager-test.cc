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

#include "kudu/fs/file_block_manager.h"
#include "kudu/fs/fs.pb.h"
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

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DECLARE_uint64(log_container_preallocate_bytes);
DECLARE_uint64(log_container_max_size);

DECLARE_int64(fs_data_dirs_reserved_bytes);
DECLARE_int64(disk_reserved_bytes_free_for_testing);

DECLARE_int32(fs_data_dirs_full_disk_cache_seconds);

DECLARE_string(block_manager);

DECLARE_double(env_inject_io_error_on_write_or_preallocate);

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

template <typename T>
class BlockManagerTest : public KuduTest {
 public:
  BlockManagerTest() :
    bm_(CreateBlockManager(scoped_refptr<MetricEntity>(),
                           shared_ptr<MemTracker>(),
                           { test_dir_ })) {
  }

  virtual void SetUp() OVERRIDE {
    CHECK_OK(bm_->Create());
    CHECK_OK(bm_->Open());
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
                            bool create) {
    bm_.reset(CreateBlockManager(metric_entity, parent_mem_tracker, paths));
    if (create) {
      RETURN_NOT_OK(bm_->Create());
    }
    return bm_->Open();
  }

  void RunMultipathTest(const vector<string>& paths);

  void RunMemTrackerTest();

  gscoped_ptr<T> bm_;
};

template <>
void BlockManagerTest<LogBlockManager>::SetUp() {
  RETURN_NOT_LOG_BLOCK_MANAGER();
  CHECK_OK(bm_->Create());
  CHECK_OK(bm_->Open());
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

  // Write ten blocks.
  const char* kTestData = "test data";
  for (int i = 0; i < 10; i++) {
    unique_ptr<WritableBlock> written_block;
    ASSERT_OK(bm_->CreateBlock(&written_block));
    ASSERT_OK(written_block->Append(kTestData));
    ASSERT_OK(written_block->Close());
  }

  // Each path should now have some additional block subdirectories. We
  // can't know for sure exactly how many (depends on the block IDs
  // generated), but this ensures that at least some change were made.
  for (const string& path : paths) {
    vector<string> children;
    ASSERT_OK(env_->GetChildren(path, &children));
    ASSERT_GT(children.size(), 3);
  }
}

template <>
void BlockManagerTest<LogBlockManager>::RunMultipathTest(const vector<string>& paths) {
  // Write (3 * numPaths * 2) blocks, in groups of (numPaths * 2). That should
  // yield two containers per path.
  const char* kTestData = "test data";
  for (int i = 0; i < 3; i++) {
    ScopedWritableBlockCloser closer;
    for (int j = 0; j < paths.size() * 2; j++) {
      unique_ptr<WritableBlock> block;
      ASSERT_OK(bm_->CreateBlock(&block));
      ASSERT_OK(block->Append(kTestData));
      closer.AddBlock(std::move(block));
    }
    ASSERT_OK(closer.CloseBlocks());
  }

  // Verify the results: 7 children = dot, dotdot, instance file, and two
  // containers (two files per container).
  for (const string& path : paths) {
    vector<string> children;
    ASSERT_OK(env_->GetChildren(path, &children));
    ASSERT_EQ(children.size(), 7);
  }
}

template <>
void BlockManagerTest<FileBlockManager>::RunMemTrackerTest() {
  shared_ptr<MemTracker> tracker = MemTracker::CreateTracker(-1, "test tracker");
  ASSERT_OK(ReopenBlockManager(scoped_refptr<MetricEntity>(),
                               tracker,
                               { test_dir_ },
                               false));

  // The file block manager does not allocate memory for persistent data.
  int64_t initial_mem = tracker->consumption();
  ASSERT_EQ(initial_mem, 0);
  unique_ptr<WritableBlock> writer;
  ASSERT_OK(bm_->CreateBlock(&writer));
  ASSERT_OK(writer->Close());
  ASSERT_EQ(tracker->consumption(), initial_mem);
}

template <>
void BlockManagerTest<LogBlockManager>::RunMemTrackerTest() {
  shared_ptr<MemTracker> tracker = MemTracker::CreateTracker(-1, "test tracker");
  ASSERT_OK(ReopenBlockManager(scoped_refptr<MetricEntity>(),
                               tracker,
                               { test_dir_ },
                               false));

  // The initial consumption should be non-zero due to the block map.
  int64_t initial_mem = tracker->consumption();
  ASSERT_GT(initial_mem, 0);

  // Allocating a persistent block should increase the consumption.
  unique_ptr<WritableBlock> writer;
  ASSERT_OK(bm_->CreateBlock(&writer));
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
  ASSERT_OK(this->bm_->CreateBlock(&written_block));

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
  Slice data;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  ASSERT_OK(read_block->Read(0, test_data.length(), &data, scratch.get()));
  ASSERT_EQ(test_data, data);

  // We don't actually do anything with the result of this call; we just want
  // to make sure it doesn't trigger a crash (see KUDU-1931).
  LOG(INFO) << "Block memory footprint: " << read_block->memory_footprint();

  // Delete the block.
  ASSERT_OK(this->bm_->DeleteBlock(written_block->id()));
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), nullptr)
              .IsNotFound());
}

// Test that we can still read from an opened block after deleting it
// (even if we can't open it again).
TYPED_TEST(BlockManagerTest, ReadAfterDeleteTest) {
  // Write a new block.
  unique_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
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
  Slice data;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  ASSERT_OK(read_block->Read(0, test_data.length(), &data, scratch.get()));
  ASSERT_EQ(test_data, data);
}

TYPED_TEST(BlockManagerTest, CloseTwiceTest) {
  // Create a new block and close it repeatedly.
  unique_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
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
      ASSERT_OK(this->bm_->CreateBlock(&written_block));

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
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->FlushDataAsync());
}

TYPED_TEST(BlockManagerTest, WritableBlockStateTest) {
  unique_ptr<WritableBlock> written_block;

  // Common flow: CLEAN->DIRTY->CLOSED.
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_EQ(WritableBlock::CLEAN, written_block->state());
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_EQ(WritableBlock::DIRTY, written_block->state());
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_EQ(WritableBlock::DIRTY, written_block->state());
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test FLUSHING->CLOSED transition.
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->FlushDataAsync());
  ASSERT_EQ(WritableBlock::FLUSHING, written_block->state());
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test CLEAN->CLOSED transition.
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test FlushDataAsync() no-op.
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->FlushDataAsync());
  ASSERT_EQ(WritableBlock::FLUSHING, written_block->state());

  // Test DIRTY->CLOSED transition.
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
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
                                     false));

  unique_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Abort());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), nullptr)
              .IsNotFound());

  ASSERT_OK(this->bm_->CreateBlock(&written_block));
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
  ASSERT_OK(this->bm_->CreateBlock(&written_block1));
  ASSERT_OK(written_block1->Close());
  ASSERT_OK(this->bm_->CreateBlock(&written_block2));
  string test_data = "test data";
  ASSERT_OK(written_block2->Append(test_data));
  ASSERT_OK(written_block2->Close());
  ASSERT_OK(this->bm_->CreateBlock(&written_block3));
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
  ASSERT_OK(new_bm->Open());

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
  Slice data;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  ASSERT_OK(read_block->Read(0, test_data.length(), &data, scratch.get()));
  ASSERT_EQ(test_data, data);
  ASSERT_OK(read_block->Close());
  ASSERT_TRUE(new_bm->OpenBlock(written_block3->id(), nullptr)
              .IsNotFound());
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
                                     true));

  ASSERT_NO_FATAL_FAILURE(this->RunMultipathTest(paths));
}

static void CloseHelper(ReadableBlock* block) {
  CHECK_OK(block->Close());
}

// Tests that ReadableBlock::Close() is thread-safe and idempotent.
TYPED_TEST(BlockManagerTest, ConcurrentCloseReadableBlockTest) {
  unique_ptr<WritableBlock> writer;
  ASSERT_OK(this->bm_->CreateBlock(&writer));
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
    ASSERT_OK(this->bm_->CreateBlock(&writer));
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
    Slice data;
    gscoped_ptr<uint8_t[]> scratch(new uint8_t[kTestData.length()]);
    ASSERT_OK(reader->Read(0, kTestData.length(), &data, scratch.get()));
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
                                     false));

  FLAGS_fs_data_dirs_full_disk_cache_seconds = 0; // Don't cache device fullness.
  FLAGS_fs_data_dirs_reserved_bytes = 1; // Keep at least 1 byte reserved in the FS.
  FLAGS_log_container_preallocate_bytes = 0; // Disable preallocation.

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
      Status s = this->bm_->CreateBlock(&writer);
      if (FLAGS_disk_reserved_bytes_free_for_testing < FLAGS_fs_data_dirs_reserved_bytes) {
        if (data_dir_observed_full) {
          // The dir was previously observed as full, so CreateBlock() checked
          // fullness again and failed.
          ASSERT_TRUE(s.IsIOError());
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
  FLAGS_env_inject_io_error_on_write_or_preallocate = 0.2;

  // Creates a block, writing the result to 'out' on success.
  auto create_a_block = [&](BlockId* out) -> Status {
    unique_ptr<WritableBlock> block;
    RETURN_NOT_OK(this->bm_->CreateBlock(&block));
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
      Slice s;
      RETURN_NOT_OK(block->Read(i * kNumAppends, sizeof(buf), &s, buf));
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

    for (const auto& id : ids) {
      ASSERT_OK(read_a_block(id));
    }

    ASSERT_OK(this->ReopenBlockManager(scoped_refptr<MetricEntity>(),
                                       shared_ptr<MemTracker>(),
                                       { GetTestDataDirectory() },
                                       false));
  }
}

TYPED_TEST(BlockManagerTest, TestGetAllBlockIds) {
  vector<BlockId> ids;
  for (int i = 0; i < 100; i++) {
    unique_ptr<WritableBlock> block;
    ASSERT_OK(this->bm_->CreateBlock(&block));
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
