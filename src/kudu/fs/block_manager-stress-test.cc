// Copyright (c) 2014, Cloudera, inc.

#include <boost/foreach.hpp>
#include <cmath>
#include <string>
#include <vector>

#include "kudu/fs/file_block_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/util/atomic.h"
#include "kudu/util/random.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DEFINE_int32(test_duration_secs, 30, "Number of seconds to run the test");
DEFINE_int32(num_writer_threads, 4, "Number of writer threads to run");
DEFINE_int32(num_reader_threads, 8, "Number of reader threads to run");
DEFINE_int32(num_deleter_threads, 1, "Number of deleter threads to run");
DEFINE_int32(block_group_size, 16, "Number of blocks to write per block "
             "group. Must be power of 2");
DEFINE_int32(block_group_bytes, 64 * 1024 * 1024,
             "Total amount of data (in bytes) to write per block group");
DEFINE_int32(num_bytes_per_write, 64 * 1024,
             "Number of bytes to write at a time");

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace fs {

// This test attempts to simulate how a TS might use the block manager:
//
// writing threads (default 2) that do the following in a tight loop:
// - create a new group of blocks (default 10)
// - write a PRNG seed into each block
// - write a big chunk of data (default 64m) into the block group:
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
// - drain the block_id vector(write locked)
// - delete all the blocks drained from the vector
//
// TODO: Don't delete all blocks ala "permgen".
class BlockManagerStressTest : public KuduTest {
 public:
  BlockManagerStressTest() :
    rand_(SeedRandom()),
    stop_latch_(1),
    bm_(new FileBlockManager(env_.get(), GetTestPath("bm"))),
    total_blocks_written_(0),
    total_bytes_written_(0),
    total_blocks_read_(0),
    total_bytes_read_(0),
    total_blocks_deleted_(0) {
  }

  virtual void SetUp() OVERRIDE {
    CHECK_OK(bm_->Create());
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
    BOOST_FOREACH(const scoped_refptr<kudu::Thread>& thr, threads_) {
     CHECK_OK(ThreadJoiner(thr.get()).Join());
    }
  }

  void WriterThread();
  void ReaderThread();
  void DeleterThread();

 protected:
  // Used to generate random data.
  Random rand_;

  // Tells the threads to stop running.
  CountDownLatch stop_latch_;

  // Tracks blocks that have been synced and are ready to be read/deleted.
  vector<BlockId> written_blocks_;

  // Protects written_blocks_.
  rw_spinlock lock_;

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

void BlockManagerStressTest::WriterThread() {
  string thread_name = Thread::current_thread()->name();
  LOG(INFO) << "Thread " << thread_name << " starting";

  size_t num_blocks_written = 0;
  size_t num_bytes_written = 0;
  MonoDelta tight_loop(MonoDelta::FromSeconds(0));
  while (!ShouldStop(tight_loop)) {
    vector<WritableBlock*> dirty_blocks;
    ElementDeleter deleter(&dirty_blocks);
    vector<Random> dirty_block_rands;

    // Create the blocks and write out the PRNG seeds.
    for (int i = 0; i < FLAGS_block_group_size; i++) {
      gscoped_ptr<WritableBlock> block;
      CHECK_OK(bm_->CreateAnonymousBlock(&block));

      const uint32_t seed = rand_.Next();
      Slice seed_slice(reinterpret_cast<const uint8_t*>(&seed), sizeof(seed));
      LOG(INFO) << "Creating block " << block->id().ToString() << " with seed " << seed;
      block->Append(seed_slice);

      dirty_blocks.push_back(block.release());
      dirty_block_rands.push_back(Random(seed));
    }

    // Write a large amount of data to the group of blocks.
    //
    // To emulate a real life workload, we pick the next block to write at
    // random, and write a smaller chunk of data to it.
    LOG(INFO) << "Writing " << FLAGS_block_group_bytes << " bytes into new blocks";
    size_t total_dirty_bytes = 0;
    while (total_dirty_bytes < FLAGS_block_group_bytes) {
      // Pick the next block.
      int next_block_idx = rand_.Skewed(log2(dirty_blocks.size()));
      WritableBlock* block = dirty_blocks[next_block_idx];
      Random& rand = dirty_block_rands[next_block_idx];

      // Write a small chunk of data.
      faststring data;
      while (data.length() < FLAGS_num_bytes_per_write) {
        const uint32_t next_int = rand.Next();
        data.append(&next_int, sizeof(next_int));
      }
      block->Append(data);
      total_dirty_bytes += data.length();
    }

    // Close all dirty blocks.
    //
    // We could close them implicitly when the blocks are destructed but
    // this way we can check for errors.
    LOG(INFO) << "Closing new blocks";
    CHECK_OK(bm_->CloseBlocks(dirty_blocks));

    // Publish the now sync'ed blocks to readers and deleters.
    {
      lock_guard<rw_spinlock> l(&lock_);
      BOOST_FOREACH(WritableBlock* block, dirty_blocks) {
        written_blocks_.push_back(block->id());
      }
    }
    num_blocks_written += dirty_blocks.size();
    num_bytes_written += total_dirty_bytes;
  }

  LOG(INFO) << Substitute("Thread $0 stopping. Wrote $1 blocks ($2 bytes)",
                          thread_name, num_blocks_written, num_bytes_written);
  total_blocks_written_.IncrementBy(num_blocks_written);
  total_bytes_written_.IncrementBy(num_bytes_written);
}

void BlockManagerStressTest::ReaderThread() {
  string thread_name = Thread::current_thread()->name();
  LOG(INFO) << "Thread " << thread_name << " starting";

  size_t num_blocks_read = 0;
  size_t num_bytes_read = 0;
  MonoDelta tight_loop(MonoDelta::FromSeconds(0));
  while (!ShouldStop(tight_loop)) {
    gscoped_ptr<ReadableBlock> block;
    {
      // Grab a block at random.
      shared_lock<rw_spinlock> l(&lock_);
      size_t num_blocks = written_blocks_.size();
      if (num_blocks > 0) {
        uint32_t next_id = rand_.Uniform(num_blocks);
        const BlockId& block_id = written_blocks_[next_id];
        CHECK_OK(bm_->OpenBlock(block_id, &block));
      }
    }
    if (!block) {
      continue;
    }

    // Read it fully into memory.
    string block_id = block->id().ToString();
    size_t block_size;
    CHECK_OK(block->Size(&block_size));
    Slice data;
    gscoped_ptr<uint8_t[]> scratch(new uint8_t[block_size]);
    CHECK_OK(block->Read(0, block_size, &data, scratch.get()));
    LOG(INFO) << "Read " << block_size << " bytes from block " << block_id;

    // The first 4 bytes correspond to the PRNG seed.
    CHECK(data.size() >= 4);
    uint32_t seed;
    memcpy(&seed, data.data(), sizeof(uint32_t));
    LOG(INFO) << "Read seed " << seed << " from block " << block_id;
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
    LOG(INFO) << "Finished reading block " << block->id().ToString();
    num_blocks_read++;
    num_bytes_read += block_size;
  }

  LOG(INFO) << Substitute("Thread $0 stopping. Read $1 blocks ($2 bytes)",
                          thread_name, num_blocks_read, num_bytes_read);
  total_blocks_read_.IncrementBy(num_blocks_read);
  total_bytes_read_.IncrementBy(num_bytes_read);
}

void BlockManagerStressTest::DeleterThread() {
  string thread_name = Thread::current_thread()->name();
  LOG(INFO) << "Thread " << thread_name << " starting";

  size_t num_blocks_deleted = 0;
  MonoDelta sleep_time(MonoDelta::FromSeconds(1));
  while (!ShouldStop(sleep_time)) {
    // Grab all the blocks we can.
    vector<BlockId> to_delete;
    {
      lock_guard<rw_spinlock> l(&lock_);
      to_delete.swap(written_blocks_);
    }

    // And delete them.
    BOOST_FOREACH(const BlockId& block_id, to_delete) {
      LOG(INFO) << "Deleting block " << block_id.ToString();
      CHECK_OK(bm_->DeleteBlock(block_id));
    }
    num_blocks_deleted += to_delete.size();
  }

  LOG(INFO) << Substitute("Thread $0 stopping. Deleted $1 blocks",
                          thread_name, num_blocks_deleted);
  total_blocks_deleted_.IncrementBy(num_blocks_deleted);
}

TEST_F(BlockManagerStressTest, StressTest) {
  if ((FLAGS_block_group_size & (FLAGS_block_group_size - 1)) != 0) {
    LOG(FATAL) << "block_group_size " << FLAGS_block_group_size
               << " is not a power of 2";
  }
  if (!AllowSlowTests()) {
    LOG(INFO) << "Not running in slow-tests mode, skipping stress test";
    return;
  }

  LOG(INFO) << "Starting all threads";
  StartThreads();
  usleep(FLAGS_test_duration_secs * 1000000);
  LOG(INFO) << "Stopping all threads";
  StopThreads();
  JoinThreads();

  LOG(INFO) << "Printing test totals";
  LOG(INFO) << "--------------------";
  LOG(INFO) << Substitute("Wrote $0 blocks ($1 bytes) via $2 threads",
                          total_blocks_written_.Load(),
                          total_bytes_written_.Load(),
                          FLAGS_num_writer_threads);
  LOG(INFO) << Substitute("Read $0 blocks ($1 bytes) via $2 threads",
                          total_blocks_read_.Load(),
                          total_bytes_read_.Load(),
                          FLAGS_num_reader_threads);
  LOG(INFO) << Substitute("Deleted $0 blocks via $1 threads",
                          total_blocks_deleted_.Load(),
                          FLAGS_num_deleter_threads);
}

} // namespace fs
} // namespace kudu
