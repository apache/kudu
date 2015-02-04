// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>

#include "kudu/fs/file_block_manager.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

using boost::assign::list_of;
using std::string;
using std::vector;
using strings::Substitute;

// LogBlockManager opens three files per container, and CloseManyBlocksTest
// uses one container for each block. To simplify testing (i.e. no need to
// raise the ulimit on open files), the default is kept low.
DEFINE_int32(num_blocks_close, 333,
             "Number of blocks to simultaneously close in CloseManyBlocksTest");

namespace kudu {
namespace fs {

template <typename T>
class BlockManagerTest : public KuduTest {
 public:
  BlockManagerTest() :
    bm_(CreateBlockManager(list_of(GetTestDataDirectory()))) {
  }

  virtual void SetUp() OVERRIDE {
    CHECK_OK(bm_->Create());
    CHECK_OK(bm_->Open());
  }

 protected:
  BlockManager* CreateBlockManager(const vector<string>& paths) {
    return new T(env_.get(), paths);
  }

  void RunMultipathTest(const vector<string>& paths);

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
  this->bm_.reset(this->CreateBlockManager(list_of(GetTestDataDirectory())));

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
  this->bm_.reset(this->CreateBlockManager(paths));
  ASSERT_OK(this->bm_->Create());
  ASSERT_OK(this->bm_->Open());

  this->RunMultipathTest(paths);
}

} // namespace fs
} // namespace kudu
