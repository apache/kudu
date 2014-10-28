// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/fs/file_block_manager.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/random.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;
using strings::Substitute;

DEFINE_int32(num_blocks_close, 1000,
             "Number of blocks to simultaneously close in CloseManyBlocksTest");

namespace kudu {
namespace fs {

template <typename T>
class BlockManagerTest : public KuduTest {
 public:
  BlockManagerTest() :
    bm_(new T(env_.get(), GetTestPath("bm"))) {
  }

  virtual void SetUp() OVERRIDE {
    CHECK_OK(this->bm_->Create());
  }

 protected:
  gscoped_ptr<BlockManager> bm_;
};

// What kinds of BlockManagers are supported?
typedef ::testing::Types<FileBlockManager> BlockManagers;
TYPED_TEST_CASE(BlockManagerTest, BlockManagers);

// Test the entire lifecycle of a block.
TYPED_TEST(BlockManagerTest, EndToEndTest) {
  // Create a block.
  BlockId b("asdfasdf");
  ASSERT_TRUE(this->bm_->OpenBlock(b, NULL).IsNotFound());
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateNamedBlock(b, &written_block));
  ASSERT_EQ(b, written_block->id());

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

  // Try to create the block again. It should fail.
  ASSERT_TRUE(this->bm_->CreateNamedBlock(written_block->id(), NULL)
              .IsAlreadyPresent());

  // Delete the block.
  ASSERT_OK(this->bm_->DeleteBlock(written_block->id()));
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), NULL)
              .IsNotFound());
}

// Create and delete an anonymous block.
TYPED_TEST(BlockManagerTest, AnonymousBlockTest) {
  gscoped_ptr<WritableBlock> written_block;
  gscoped_ptr<ReadableBlock> read_block;

  ASSERT_OK(this->bm_->CreateAnonymousBlock(&written_block));
  ASSERT_OK(this->bm_->OpenBlock(written_block->id(), &read_block));
  ASSERT_OK(this->bm_->DeleteBlock(written_block->id()));
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), NULL)
              .IsNotFound());
}

// Test that we can still read from an opened block after deleting it
// (even if we can't open it again).
TYPED_TEST(BlockManagerTest, ReadAfterDeleteTest) {
  // Write a new block.
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateAnonymousBlock(&written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));

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
  ASSERT_OK(this->bm_->CreateAnonymousBlock(&written_block));
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
    ASSERT_OK(this->bm_->CreateAnonymousBlock(&written_block));

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
  ASSERT_OK(this->bm_->CreateAnonymousBlock(&written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->FlushDataAsync());
}

TYPED_TEST(BlockManagerTest, WritableBlockStateTest) {
  gscoped_ptr<WritableBlock> written_block;

  // Common flow: CLEAN->DIRTY->CLOSED.
  ASSERT_OK(this->bm_->CreateAnonymousBlock(&written_block));
  ASSERT_EQ(WritableBlock::CLEAN, written_block->state());
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_EQ(WritableBlock::DIRTY, written_block->state());
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_EQ(WritableBlock::DIRTY, written_block->state());
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test FLUSHING->CLOSED transition.
  ASSERT_OK(this->bm_->CreateAnonymousBlock(&written_block));
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->FlushDataAsync());
  ASSERT_EQ(WritableBlock::FLUSHING, written_block->state());
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test CLEAN->CLOSED transition.
  ASSERT_OK(this->bm_->CreateAnonymousBlock(&written_block));
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());

  // Test FlushDataAsync() no-op.
  ASSERT_OK(this->bm_->CreateAnonymousBlock(&written_block));
  ASSERT_OK(written_block->FlushDataAsync());
  ASSERT_EQ(WritableBlock::FLUSHING, written_block->state());

  // Test DIRTY->CLOSED transition.
  ASSERT_OK(this->bm_->CreateAnonymousBlock(&written_block));
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Close());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());
}

TYPED_TEST(BlockManagerTest, AbortTest) {
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(this->bm_->CreateAnonymousBlock(&written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Abort());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), NULL)
              .IsNotFound());

  ASSERT_OK(this->bm_->CreateAnonymousBlock(&written_block));
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->FlushDataAsync());
  ASSERT_OK(written_block->Abort());
  ASSERT_EQ(WritableBlock::CLOSED, written_block->state());
  ASSERT_TRUE(this->bm_->OpenBlock(written_block->id(), NULL)
              .IsNotFound());
}

} // namespace fs
} // namespace kudu
