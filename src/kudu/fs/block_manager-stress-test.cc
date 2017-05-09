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
#include <cmath>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "kudu/fs/file_block_manager.h"
#include "kudu/fs/fs_report.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/fs/log_block_manager-test-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/atomic.h"
#include "kudu/util/file_cache-test-util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/random.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DECLARE_double(log_container_excess_space_before_cleanup_fraction);
DECLARE_double(log_container_live_metadata_before_compact_ratio);
DECLARE_int64(block_manager_max_open_files);
DECLARE_uint64(log_container_max_size);
DECLARE_uint64(log_container_preallocate_bytes);

DEFINE_int32(test_duration_secs, 2, "Number of seconds to run the test");
DEFINE_int32(num_writer_threads, 4, "Number of writer threads to run");
DEFINE_int32(num_reader_threads, 8, "Number of reader threads to run");
DEFINE_int32(num_deleter_threads, 1, "Number of deleter threads to run");
DEFINE_int32(block_group_size, 8, "Number of blocks to write per block "
             "group. Must be power of 2");
DEFINE_int32(block_group_bytes, 32 * 1024,
             "Total amount of data (in bytes) to write per block group");
DEFINE_int32(num_bytes_per_write, 32,
             "Number of bytes to write at a time");
DEFINE_int32(num_inconsistencies, 16,
             "Number of on-disk inconsistencies to inject in between test runs");
DEFINE_string(block_manager_paths, "", "Comma-separated list of paths to "
              "use for block storage. If empty, will use the default unit "
              "test path");

using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace fs {

// This test attempts to simulate how a TS might use the block manager:
//
// writing threads (default 2) that do the following in a tight loop:
// - create a new group of blocks (default 10)
// - write a PRNG seed into each block
// - write a big chunk of data (default 32m) into the block group:
//   - pick the next block to write a piece to at random
//   - write one piece at a time (default 64k) of data generated using
//     that block's PRNG seed
// - close the blocks
// - add the blocks to the block_id vector (write locked)
// reading threads (default 8) that do the following in a tight loop:
// - read one block id at random from block_id vector (read locked)
// - read the block fully into memory, parsing its seed
// - verify that the contents of the block match the PRNG output
// deleting threads (default 1) that do the following every second:
// - drain one group of blocks from the block_id vector(write locked)
// - delete all the blocks drained from the vector
template <typename T>
class BlockManagerStressTest : public KuduTest {
 public:
  BlockManagerStressTest() :
    rand_seed_(SeedRandom()),
    stop_latch_(1),
    total_blocks_written_(0),
    total_bytes_written_(0),
    total_blocks_read_(0),
    total_bytes_read_(0),
    total_blocks_deleted_(0) {

    // Increase the number of containers created.
    FLAGS_log_container_max_size = 1 * 1024 * 1024;
    FLAGS_log_container_preallocate_bytes = 1 * 1024 * 1024;

    // Ensure the file cache is under stress too.
    FLAGS_block_manager_max_open_files = 512;

    // Maximize the amount of cleanup triggered by the extra space heuristic.
    FLAGS_log_container_excess_space_before_cleanup_fraction = 0.0;

    // Compact block manager metadata aggressively.
    FLAGS_log_container_live_metadata_before_compact_ratio = 0.80;

    if (FLAGS_block_manager_paths.empty()) {
      data_dirs_.push_back(test_dir_);
    } else {
      data_dirs_ = strings::Split(FLAGS_block_manager_paths, ",",
                                  strings::SkipEmpty());
    }

    // Defer block manager creation until after the above flags are set.
    bm_.reset(CreateBlockManager());
  }

  virtual void SetUp() OVERRIDE {
    CHECK_OK(bm_->Create());
    CHECK_OK(bm_->Open(nullptr));
  }

  virtual void TearDown() OVERRIDE {
    // If non-standard paths were provided we need to delete them in
    // between test runs.
    if (!FLAGS_block_manager_paths.empty()) {
      for (const auto& dd : data_dirs_) {
        WARN_NOT_OK(env_->DeleteRecursively(dd),
                    Substitute("Couldn't recursively delete $0", dd));
      }
    }
  }

  BlockManager* CreateBlockManager() {
    BlockManagerOptions opts;
    opts.root_paths = data_dirs_;
    return new T(env_, opts);
  }

  void RunTest(int secs) {
    LOG(INFO) << "Starting all threads";
    this->StartThreads();
    SleepFor(MonoDelta::FromSeconds(secs));
    LOG(INFO) << "Stopping all threads";
    this->StopThreads();
    this->JoinThreads();
    this->stop_latch_.Reset(1);
  }

  void StartThreads() {
    scoped_refptr<Thread> new_thread;
    for (int i = 0; i < FLAGS_num_writer_threads; i++) {
      CHECK_OK(Thread::Create("BlockManagerStressTest", Substitute("writer-$0", i),
                              &BlockManagerStressTest::WriterThread, this, &new_thread));
      threads_.push_back(new_thread);
    }
    for (int i = 0; i < FLAGS_num_reader_threads; i++) {
      CHECK_OK(Thread::Create("BlockManagerStressTest", Substitute("reader-$0", i),
                              &BlockManagerStressTest::ReaderThread, this, &new_thread));
      threads_.push_back(new_thread);
    }
    for (int i = 0; i < FLAGS_num_deleter_threads; i++) {
      CHECK_OK(Thread::Create("BlockManagerStressTest", Substitute("deleter-$0", i),
                              &BlockManagerStressTest::DeleterThread, this, &new_thread));
      threads_.push_back(new_thread);
    }
  }

  void StopThreads() {
    stop_latch_.CountDown();
  }

  bool ShouldStop(const MonoDelta& wait_time) {
    return stop_latch_.WaitFor(wait_time);
  }

  void JoinThreads() {
    for (const scoped_refptr<kudu::Thread>& thr : threads_) {
     CHECK_OK(ThreadJoiner(thr.get()).Join());
    }
  }

  void WriterThread();
  void ReaderThread();
  void DeleterThread();

  int GetMaxFdCount() const;

  // Adds FLAGS_num_inconsistencies randomly chosen inconsistencies to the
  // block manager's on-disk representation, assuming the block manager in
  // question supports inconsistency detection and repair.
  //
  // The block manager should be idle while this is called, and it should be
  // restarted afterwards so that detection and repair have a chance to run.
  void InjectNonFatalInconsistencies();

 protected:
  // Directories where blocks will be written.
  vector<string> data_dirs_;

  // Used to generate random data. All PRNG instances are seeded with this
  // value to ensure that the test is reproducible.
  int rand_seed_;

  // Tells the threads to stop running.
  CountDownLatch stop_latch_;

  // Tracks blocks that have been synced and are ready to be read/deleted.
  //
  // Each entry is a block id and the number of in-progress openers. To delete
  // a block, there must be no openers.
  unordered_map<BlockId, int, BlockIdHash, BlockIdEqual> written_blocks_;

  // Protects written_blocks_.
  simple_spinlock lock_;

  // The block manager.
  gscoped_ptr<BlockManager> bm_;

  // The running threads.
  vector<scoped_refptr<Thread> > threads_;

  // Some performance counters.

  AtomicInt<int64_t> total_blocks_written_;
  AtomicInt<int64_t> total_bytes_written_;

  AtomicInt<int64_t> total_blocks_read_;
  AtomicInt<int64_t> total_bytes_read_;

  AtomicInt<int64_t> total_blocks_deleted_;
};

template <typename T>
void BlockManagerStressTest<T>::WriterThread() {
  Random rand(rand_seed_);
  size_t num_blocks_written = 0;
  size_t num_bytes_written = 0;
  MonoDelta tight_loop(MonoDelta::FromSeconds(0));
  while (!ShouldStop(tight_loop)) {
    vector<WritableBlock*> dirty_blocks;
    ElementDeleter deleter(&dirty_blocks);
    vector<Random> dirty_block_rands;

    // Create the blocks and write out the PRNG seeds.
    for (int i = 0; i < FLAGS_block_group_size; i++) {
      unique_ptr<WritableBlock> block;
      CHECK_OK(bm_->CreateBlock(&block));

      const uint32_t seed = rand.Next() + 1;
      Slice seed_slice(reinterpret_cast<const uint8_t*>(&seed), sizeof(seed));
      CHECK_OK(block->Append(seed_slice));

      dirty_blocks.push_back(block.release());
      dirty_block_rands.push_back(Random(seed));
    }

    // Write a large amount of data to the group of blocks.
    //
    // To emulate a real life workload, we pick the next block to write at
    // random, and write a smaller chunk of data to it.
    size_t total_dirty_bytes = 0;
    while (total_dirty_bytes < FLAGS_block_group_bytes) {
      // Pick the next block.
      int next_block_idx = rand.Skewed(log2(dirty_blocks.size()));
      WritableBlock* block = dirty_blocks[next_block_idx];
      Random& rand = dirty_block_rands[next_block_idx];

      // Write a small chunk of data.
      faststring data;
      while (data.length() < FLAGS_num_bytes_per_write) {
        const uint32_t next_int = rand.Next();
        data.append(&next_int, sizeof(next_int));
      }
      CHECK_OK(block->Append(data));
      total_dirty_bytes += data.length();
    }

    // Close all dirty blocks.
    //
    // We could close them implicitly when the blocks are destructed but
    // this way we can check for errors.
    CHECK_OK(bm_->CloseBlocks(dirty_blocks));

    // Publish the now sync'ed blocks to readers and deleters.
    {
      std::lock_guard<simple_spinlock> l(lock_);
      for (WritableBlock* block : dirty_blocks) {
        InsertOrDie(&written_blocks_, block->id(), 0);
      }
    }
    num_blocks_written += dirty_blocks.size();
    num_bytes_written += total_dirty_bytes;
  }

  total_blocks_written_.IncrementBy(num_blocks_written);
  total_bytes_written_.IncrementBy(num_bytes_written);
}

template <typename T>
void BlockManagerStressTest<T>::ReaderThread() {
  Random rand(rand_seed_);
  size_t num_blocks_read = 0;
  size_t num_bytes_read = 0;
  MonoDelta tight_loop(MonoDelta::FromSeconds(0));
  while (!ShouldStop(tight_loop)) {
    BlockId block_id;
    {
      // Grab a block at random.
      std::lock_guard<simple_spinlock> l(lock_);
      if (written_blocks_.empty()) {
        continue;
      }

      auto it = written_blocks_.begin();
      std::advance(it, rand.Uniform(written_blocks_.size()));
      block_id = it->first;
      it->second++;
    }

    unique_ptr<ReadableBlock> block;
    CHECK_OK(bm_->OpenBlock(block_id, &block));

    // Done opening the block, make it available for deleting.
    {
      std::lock_guard<simple_spinlock> l(lock_);
      int& openers = FindOrDie(written_blocks_, block_id);
      openers--;
    }

    // Read it fully into memory.
    uint64_t block_size;
    CHECK_OK(block->Size(&block_size));
    gscoped_ptr<uint8_t[]> scratch(new uint8_t[block_size]);
    Slice data(scratch.get(), block_size);
    CHECK_OK(block->Read(0, &data));

    // The first 4 bytes correspond to the PRNG seed.
    CHECK(data.size() >= 4);
    uint32_t seed;
    memcpy(&seed, data.data(), sizeof(uint32_t));
    Random rand(seed);

    // Verify every subsequent number using the PRNG.
    size_t bytes_processed;
    for (bytes_processed = 4; // start after the PRNG seed
        bytes_processed < data.size();
        bytes_processed += sizeof(uint32_t)) {
      uint32_t expected_num = rand.Next();
      uint32_t actual_num;
      memcpy(&actual_num, data.data() + bytes_processed, sizeof(uint32_t));
      if (expected_num != actual_num) {
        LOG(FATAL) << "Read " << actual_num << " and not " << expected_num
                   << " from position " << bytes_processed << " in block "
                   << block_id;
      }
    }
    CHECK_EQ(bytes_processed, data.size());
    num_blocks_read++;
    num_bytes_read += block_size;
  }

  total_blocks_read_.IncrementBy(num_blocks_read);
  total_bytes_read_.IncrementBy(num_bytes_read);
}

template <typename T>
void BlockManagerStressTest<T>::DeleterThread() {
  Random rand(rand_seed_);
  size_t num_blocks_deleted = 0;
  MonoDelta tight_loop(MonoDelta::FromSeconds(0));
  while (!ShouldStop(tight_loop)) {
    // Grab a block at random.
    BlockId to_delete;
    {
      std::lock_guard<simple_spinlock> l(lock_);
      if (written_blocks_.empty()) {
        continue;
      }

      auto it = written_blocks_.begin();
      std::advance(it, rand.Uniform(written_blocks_.size()));
      if (it->second > 0) {
        continue;
      }

      to_delete = it->first;
      written_blocks_.erase(it);
    }

    // And delete it.
    CHECK_OK(bm_->DeleteBlock(to_delete));
    num_blocks_deleted++;
  }

  total_blocks_deleted_.IncrementBy(num_blocks_deleted);
}

template <>
int BlockManagerStressTest<FileBlockManager>::GetMaxFdCount() const {
  return FLAGS_block_manager_max_open_files +
      // Each open block exists outside the file cache.
      (FLAGS_num_writer_threads * FLAGS_block_group_size) +
      // Each reader thread can open a file outside the cache if its lookup
      // misses. It'll immediately evict an existing fd, but in that brief
      // window of time both fds may be open simultaneously.
      FLAGS_num_reader_threads;
}

template <>
int BlockManagerStressTest<LogBlockManager>::GetMaxFdCount() const {
  return FLAGS_block_manager_max_open_files +
      // If all containers are full, each open block could theoretically
      // result in a new container, which is two files briefly outside the
      // cache (before they are inserted and evict other cached files).
      (FLAGS_num_writer_threads * FLAGS_block_group_size * 2);
}

template <>
void BlockManagerStressTest<FileBlockManager>::InjectNonFatalInconsistencies() {
  // Do nothing; the FBM has no repairable inconsistencies.
}

template <>
void BlockManagerStressTest<LogBlockManager>::InjectNonFatalInconsistencies() {
  LBMCorruptor corruptor(env_, data_dirs_, rand_seed_);
  ASSERT_OK(corruptor.Init());

  for (int i = 0; i < FLAGS_num_inconsistencies; i++) {
    ASSERT_OK(corruptor.InjectRandomNonFatalInconsistency());
  }
}

// What kinds of BlockManagers are supported?
#if defined(__linux__)
typedef ::testing::Types<FileBlockManager, LogBlockManager> BlockManagers;
#else
typedef ::testing::Types<FileBlockManager> BlockManagers;
#endif
TYPED_TEST_CASE(BlockManagerStressTest, BlockManagers);

TYPED_TEST(BlockManagerStressTest, StressTest) {
  OverrideFlagForSlowTests("test_duration_secs", "30");
  OverrideFlagForSlowTests("block_group_size", "16");
  OverrideFlagForSlowTests("num_inconsistencies", "128");

  if ((FLAGS_block_group_size & (FLAGS_block_group_size - 1)) != 0) {
    LOG(FATAL) << "block_group_size " << FLAGS_block_group_size
               << " is not a power of 2";
  }

  PeriodicOpenFdChecker checker(this->env_, this->GetMaxFdCount());

  LOG(INFO) << "Running on fresh block manager";
  checker.Start();
  this->RunTest(FLAGS_test_duration_secs / 2);
  NO_FATALS(this->InjectNonFatalInconsistencies());
  LOG(INFO) << "Running on populated block manager";
  this->bm_.reset(this->CreateBlockManager());
  FsReport report;
  ASSERT_OK(this->bm_->Open(&report));
  ASSERT_OK(report.LogAndCheckForFatalErrors());
  this->RunTest(FLAGS_test_duration_secs / 2);
  checker.Stop();

  LOG(INFO) << "Printing test totals";
  LOG(INFO) << "--------------------";
  LOG(INFO) << Substitute("Wrote $0 blocks ($1 bytes) via $2 threads",
                          this->total_blocks_written_.Load(),
                          this->total_bytes_written_.Load(),
                          FLAGS_num_writer_threads);
  LOG(INFO) << Substitute("Read $0 blocks ($1 bytes) via $2 threads",
                          this->total_blocks_read_.Load(),
                          this->total_bytes_read_.Load(),
                          FLAGS_num_reader_threads);
  LOG(INFO) << Substitute("Deleted $0 blocks via $1 threads",
                          this->total_blocks_deleted_.Load(),
                          FLAGS_num_deleter_threads);
}

} // namespace fs
} // namespace kudu
