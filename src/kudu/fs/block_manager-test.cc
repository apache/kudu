// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>

#include "kudu/fs/file_block_manager.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

using boost::assign::list_of;
using std::string;
using std::vector;
using strings::Substitute;

// LogBlockManager opens two files per container, and CloseManyBlocksTest
// uses one container for each block. To simplify testing (i.e. no need to
// raise the ulimit on open files), the default is kept low.
DEFINE_int32(num_blocks_close, 500,
             "Number of blocks to simultaneously close in CloseManyBlocksTest");

DECLARE_uint64(log_container_max_size);

namespace kudu {
namespace fs {

template <typename T>
class BlockManagerTest : public KuduTest {
 public:
  BlockManagerTest() :
    bm_(CreateBlockManager(NULL, list_of(GetTestDataDirectory()))) {
  }

  virtual void SetUp() OVERRIDE {
    CHECK_OK(bm_->Create());
    CHECK_OK(bm_->Open());
  }

 protected:
  BlockManager* CreateBlockManager(MetricContext* parent_metric_context,
                                   const vector<string>& paths) {
    return new T(env_.get(), parent_metric_context, paths);
  }

  void RunMultipathTest(const vector<string>& paths);

  void RunLogMetricsTest();

  gscoped_ptr<BlockManager> bm_;
};

template <>
void BlockManagerTest<FileBlockManager>::RunMultipathTest(const vector<string>& paths) {
  // Ensure that each path has an instance file and that it's well-formed.
  BOOST_FOREACH(const string& path, paths) {
    vector<string> children;
    ASSERT_OK(env_->GetChildren(path, &children));
    ASSERT_EQ(3, children.size());
    BOOST_FOREACH(const string& child, children) {
      if (child == "." || child == "..") {
        continue;
      }
      PathInstanceMetadataPB instance;
      ASSERT_OK(pb_util::ReadPBContainerFromPath(env_.get(),
                                                 JoinPathSegments(path, child),
                                                 &instance));
    }
  }

  // Write ten blocks.
  const char* kTestData = "test data";
  for (int i = 0; i < 10; i++) {
    gscoped_ptr<WritableBlock> written_block;
    ASSERT_OK(bm_->CreateBlock(&written_block));
    ASSERT_OK(written_block->Append(kTestData));
    ASSERT_OK(written_block->Close());
  }

  // Each path should now have some additional block subdirectories. We
  // can't know for sure exactly how many (depends on the block IDs
  // generated), but this ensures that at least some change were made.
  BOOST_FOREACH(const string& path, paths) {
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
      gscoped_ptr<WritableBlock> block;
      ASSERT_OK(bm_->CreateBlock(&block));
      ASSERT_OK(block->Append(kTestData));
      closer.AddBlock(block.Pass());
    }
    ASSERT_OK(closer.CloseBlocks());
  }

  // Verify the results: 7 children = dot, dotdot, instance file, and two
  // containers (two files per container).
  BOOST_FOREACH(const string& path, paths) {
    vector<string> children;
    ASSERT_OK(env_->GetChildren(path, &children));
    ASSERT_EQ(children.size(), 7);
  }
}

template <>
void BlockManagerTest<FileBlockManager>::RunLogMetricsTest() {
  LOG(INFO) << "Test skipped; wrong block manager";
}

static void CheckLogMetrics(const MetricRegistry::UnorderedMetricMap& metrics,
                            int bytes_under_management, int blocks_under_management,
                            int total_containers, int total_full_containers) {
    ASSERT_EQ(bytes_under_management, down_cast<AtomicGauge<uint64_t>*>(
        FindOrDie(metrics, "test.block_manager.bytes_under_management"))->value());
    ASSERT_EQ(blocks_under_management, down_cast<AtomicGauge<uint64_t>*>(
        FindOrDie(metrics, "test.block_manager.blocks_under_management"))->value());
    ASSERT_EQ(total_containers, down_cast<Counter*>(
        FindOrDie(metrics, "test.block_manager.total_containers"))->value());
    ASSERT_EQ(total_full_containers, down_cast<Counter*>(
        FindOrDie(metrics, "test.block_manager.total_full_containers"))->value());
}

template <>
void BlockManagerTest<LogBlockManager>::RunLogMetricsTest() {
  MetricRegistry registry;
  MetricContext context(&registry, "test");
  this->bm_.reset(this->CreateBlockManager(&context, list_of(GetTestDataDirectory())));
  ASSERT_OK(this->bm_->Open());
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(
      registry.UnsafeMetricsMapForTests(), 0, 0, 0, 0));

  // Lower the max container size so that we can more easily test full
  // container metrics.
  FLAGS_log_container_max_size = 1024;

  // One block --> one container.
  gscoped_ptr<WritableBlock> writer;
  ASSERT_OK(this->bm_->CreateBlock(&writer));
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(
      registry.UnsafeMetricsMapForTests(), 0, 0, 1, 0));

  // And when the block is closed, it becomes "under management".
  ASSERT_OK(writer->Close());
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(
      registry.UnsafeMetricsMapForTests(), 0, 1, 1, 0));

  // Create 10 blocks concurrently. We reuse the existing container and
  // create 9 new ones. All of them get filled.
  BlockId saved_id;
  {
    Random rand(SeedRandom());
    ScopedWritableBlockCloser closer;
    for (int i = 0; i < 10; i++) {
      gscoped_ptr<WritableBlock> b;
      ASSERT_OK(this->bm_->CreateBlock(&b));
      if (saved_id.IsNull()) {
        saved_id = b->id();
      }
      uint8_t data[1024];
      for (int i = 0; i < sizeof(data); i += sizeof(uint32_t)) {
        data[i] = rand.Next();
      }
      b->Append(Slice(data, sizeof(data)));
      closer.AddBlock(b.Pass());
    }
    ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(
        registry.UnsafeMetricsMapForTests(), 0, 1, 10, 0));

    // Only when the blocks are closed are the containers considered full.
    ASSERT_OK(closer.CloseBlocks());
    ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(
        registry.UnsafeMetricsMapForTests(), 10 * 1024, 11, 10, 10));
  }

  // Reopen the block manager and test the metrics. They're all based on
  // persistent information so they should be the same.
  MetricRegistry new_registry;
  MetricContext new_context(&new_registry, "test");
  this->bm_.reset(this->CreateBlockManager(&new_context, list_of(GetTestDataDirectory())));
  ASSERT_OK(this->bm_->Open());
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(
      new_registry.UnsafeMetricsMapForTests(), 10 * 1024, 11, 10, 10));

  // Delete a block. Its contents should no longer be under management.
  ASSERT_OK(this->bm_->DeleteBlock(saved_id));
  ASSERT_NO_FATAL_FAILURE(CheckLogMetrics(
      new_registry.UnsafeMetricsMapForTests(), 9 * 1024, 10, 10, 10));
}

// What kinds of BlockManagers are supported?
typedef ::testing::Types<FileBlockManager, LogBlockManager> BlockManagers;
TYPED_TEST_CASE(BlockManagerTest, BlockManagers);

// Test the entire lifecycle of a block.
TYPED_TEST(BlockManagerTest, EndToEndTest) {
  // Create a block.
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));

  // Write some data to it.
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Close());

  // Read the data back.
  gscoped_ptr<ReadableBlock> read_block;
  ASSERT_OK(this->bm_->OpenBlock(written_block->id(), &read_block));
  size_t sz;
  ASSERT_OK(read_block->Size(&sz));
  ASSERT_EQ(test_data.length(), sz);
  Slice data;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  ASSERT_OK(read_block->Read(0, test_data.length(), &data, scratch.get()));
  ASSERT_EQ(test_data, data);

  // Delete the block.
  ASSERT_OK(this->bm_->DeleteBlock(written_block->id()));
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), NULL)
              .IsNotFound());
}

// Test that we can still read from an opened block after deleting it
// (even if we can't open it again).
TYPED_TEST(BlockManagerTest, ReadAfterDeleteTest) {
  // Write a new block.
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Close());

  // Open it for reading, then delete it. Subsequent opens should fail.
  gscoped_ptr<ReadableBlock> read_block;
  ASSERT_OK(this->bm_->OpenBlock(written_block->id(), &read_block));
  ASSERT_OK(this->bm_->DeleteBlock(written_block->id()));
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), NULL)
              .IsNotFound());

  // But we should still be able to read from the opened block.
  Slice data;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  ASSERT_OK(read_block->Read(0, test_data.length(), &data, scratch.get()));
  ASSERT_EQ(test_data, data);
}

TYPED_TEST(BlockManagerTest, CloseTwiceTest) {
  // Create a new block and close it repeatedly.
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->Close());
  ASSERT_OK(written_block->Close());

  // Open it for reading and close it repeatedly.
  gscoped_ptr<ReadableBlock> read_block;
  ASSERT_OK(this->bm_->OpenBlock(written_block->id(), &read_block));
  ASSERT_OK(read_block->Close());
  ASSERT_OK(read_block->Close());
}

TYPED_TEST(BlockManagerTest, CloseManyBlocksTest) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Not running in slow-tests mode";
    return;
  }
  Random rand(SeedRandom());
  vector<WritableBlock*> dirty_blocks;
  ElementDeleter deleter(&dirty_blocks);
  LOG(INFO) << "Creating " <<  FLAGS_num_blocks_close << " blocks";
  for (int i = 0; i < FLAGS_num_blocks_close; i++) {
    // Create a block.
    gscoped_ptr<WritableBlock> written_block;
    ASSERT_OK(this->bm_->CreateBlock(&written_block));

    // Write 64k bytes of random data into it.
    uint8_t data[65536];
    for (int i = 0; i < sizeof(data); i += sizeof(uint32_t)) {
      data[i] = rand.Next();
    }
    written_block->Append(Slice(data, sizeof(data)));

    dirty_blocks.push_back(written_block.release());
  }

  LOG_TIMING(INFO, Substitute("closing $0 blocks", FLAGS_num_blocks_close)) {
    ASSERT_OK(this->bm_->CloseBlocks(dirty_blocks));
  }
}

// We can't really test that FlushDataAsync() "works", but we can test that
// it doesn't break anything.
TYPED_TEST(BlockManagerTest, FlushDataAsyncTest) {
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->FlushDataAsync());
}

TYPED_TEST(BlockManagerTest, WritableBlockStateTest) {
  gscoped_ptr<WritableBlock> written_block;

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

TYPED_TEST(BlockManagerTest, AbortTest) {
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Abort());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), NULL)
              .IsNotFound());

  ASSERT_OK(this->bm_->CreateBlock(&written_block));
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->FlushDataAsync());
  ASSERT_OK(written_block->Abort());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), NULL)
              .IsNotFound());
}

TYPED_TEST(BlockManagerTest, PersistenceTest) {
  // Create three blocks:
  // 1. Empty.
  // 2. Non-empty.
  // 3. Deleted.
  gscoped_ptr<WritableBlock> written_block1;
  gscoped_ptr<WritableBlock> written_block2;
  gscoped_ptr<WritableBlock> written_block3;
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
  // Note that some block managers must be closed to fully synchronize
  // their contents to disk (e.g. log_block_manager uses PosixMmapFiles
  // for containers, which must be closed to be trimmed).
  this->bm_.reset(this->CreateBlockManager(NULL, list_of(GetTestDataDirectory())));

  ASSERT_OK(this->bm_->Open());

  // Test that the state of all three blocks is properly reflected.
  gscoped_ptr<ReadableBlock> read_block;
  ASSERT_OK(this->bm_->OpenBlock(written_block1->id(), &read_block));
  size_t sz;
  ASSERT_OK(read_block->Size(&sz));
  ASSERT_EQ(0, sz);
  ASSERT_OK(read_block->Close());
  ASSERT_OK(this->bm_->OpenBlock(written_block2->id(), &read_block));
  ASSERT_OK(read_block->Size(&sz));
  ASSERT_EQ(test_data.length(), sz);
  Slice data;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  ASSERT_OK(read_block->Read(0, test_data.length(), &data, scratch.get()));
  ASSERT_EQ(test_data, data);
  ASSERT_OK(read_block->Close());
  ASSERT_TRUE(this->bm_->OpenBlock(written_block3->id(), NULL)
              .IsNotFound());
}

TYPED_TEST(BlockManagerTest, MultiPathTest) {
  // Recreate the block manager with three paths.
  vector<string> paths;
  for (int i = 0; i < 3; i++) {
    paths.push_back(this->GetTestPath(Substitute("path$0", i)));
  }
  this->bm_.reset(this->CreateBlockManager(NULL, paths));
  ASSERT_OK(this->bm_->Create());
  ASSERT_OK(this->bm_->Open());

  ASSERT_NO_FATAL_FAILURE(this->RunMultipathTest(paths));
}

static void CloseHelper(ReadableBlock* block) {
  CHECK_OK(block->Close());
}

// Tests that ReadableBlock::Close() is thread-safe and idempotent.
TYPED_TEST(BlockManagerTest, ConcurrentCloseReadableBlockTest) {
  gscoped_ptr<WritableBlock> writer;
  ASSERT_OK(this->bm_->CreateBlock(&writer));
  ASSERT_OK(writer->Close());

  gscoped_ptr<ReadableBlock> reader;
  ASSERT_OK(this->bm_->OpenBlock(writer->id(), &reader));

  vector<scoped_refptr<Thread> > threads;
  for (int i = 0; i < 100; i++) {
    scoped_refptr<Thread> t;
    ASSERT_OK(Thread::Create("test", Substitute("t$0", i),
                             &CloseHelper, reader.get(), &t));
    threads.push_back(t);
  }
  BOOST_FOREACH(const scoped_refptr<Thread>& t, threads) {
    t->Join();
  }
}

static void CheckMetrics(const MetricRegistry::UnorderedMetricMap& metrics,
                         int blocks_open_reading, int blocks_open_writing,
                         int total_readable_blocks, int total_writable_blocks,
                         int total_bytes_read, int total_bytes_written) {
  ASSERT_EQ(blocks_open_reading, down_cast<AtomicGauge<uint64_t>*>(
      FindOrDie(metrics, "test.block_manager.blocks_open_reading"))->value());
  ASSERT_EQ(blocks_open_writing, down_cast<AtomicGauge<uint64_t>*>(
      FindOrDie(metrics, "test.block_manager.blocks_open_writing"))->value());
  ASSERT_EQ(total_readable_blocks, down_cast<Counter*>(
      FindOrDie(metrics, "test.block_manager.total_readable_blocks"))->value());
  ASSERT_EQ(total_writable_blocks, down_cast<Counter*>(
      FindOrDie(metrics, "test.block_manager.total_writable_blocks"))->value());
  ASSERT_EQ(total_bytes_read, down_cast<Counter*>(
      FindOrDie(metrics, "test.block_manager.total_bytes_read"))->value());
  ASSERT_EQ(total_bytes_written, down_cast<Counter*>(
      FindOrDie(metrics, "test.block_manager.total_bytes_written"))->value());
}

TYPED_TEST(BlockManagerTest, MetricsTest) {
  const string kTestData = "test data";
  MetricRegistry registry;
  MetricContext context(&registry, "test");
  this->bm_.reset(this->CreateBlockManager(&context, list_of(GetTestDataDirectory())));
  ASSERT_OK(this->bm_->Open());
  ASSERT_NO_FATAL_FAILURE(CheckMetrics(
      registry.UnsafeMetricsMapForTests(), 0, 0, 0, 0, 0, 0));

  for (int i = 0; i < 3; i++) {
    gscoped_ptr<WritableBlock> writer;
    gscoped_ptr<ReadableBlock> reader;

    // An open writer. Also reflected in total_writable_blocks.
    ASSERT_OK(this->bm_->CreateBlock(&writer));
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        registry.UnsafeMetricsMapForTests(), 0, 1, i, i + 1,
        i * kTestData.length(), i * kTestData.length()));

    // Block is no longer opened for writing, but its data
    // is now reflected in total_bytes_written.
    ASSERT_OK(writer->Append(kTestData));
    ASSERT_OK(writer->Close());
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        registry.UnsafeMetricsMapForTests(), 0, 0, i, i + 1,
        i * kTestData.length(), (i + 1) * kTestData.length()));

    // An open reader.
    ASSERT_OK(this->bm_->OpenBlock(writer->id(), &reader));
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        registry.UnsafeMetricsMapForTests(), 1, 0, i + 1, i + 1,
        i * kTestData.length(), (i + 1) * kTestData.length()));

    // The read is reflected in total_bytes_read.
    Slice data;
    gscoped_ptr<uint8_t[]> scratch(new uint8_t[kTestData.length()]);
    ASSERT_OK(reader->Read(0, kTestData.length(), &data, scratch.get()));
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        registry.UnsafeMetricsMapForTests(), 1, 0, i + 1, i + 1,
        (i + 1) * kTestData.length(), (i + 1) * kTestData.length()));

    // The reader is now gone.
    ASSERT_OK(reader->Close());
    ASSERT_NO_FATAL_FAILURE(CheckMetrics(
        registry.UnsafeMetricsMapForTests(), 0, 0, i + 1, i + 1,
        (i + 1) * kTestData.length(), (i + 1) * kTestData.length()));
  }
}

TYPED_TEST(BlockManagerTest, LogMetricsTest) {
  ASSERT_NO_FATAL_FAILURE(this->RunLogMetricsTest());
}

} // namespace fs
} // namespace kudu
