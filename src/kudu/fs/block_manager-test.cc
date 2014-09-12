// Copyright (c) 2014, Cloudera, inc.

#include "kudu/fs/file_block_manager.h"
#include "kudu/util/test_util.h"

using std::string;

namespace kudu {
namespace fs {

class BlockManagerTest : public KuduTest {
 public:
  BlockManagerTest() {
    CHECK_OK(FileBlockManager::Create(env_.get(), test_dir_, &bm_));
  }

 protected:
  gscoped_ptr<BlockManager> bm_;
};

// Test the entire lifecycle of a block.
TEST_F(BlockManagerTest, EndToEndTest) {
  // Create a block.
  BlockId b("asdfasdf");
  ASSERT_TRUE(bm_->OpenBlock(b, NULL).IsNotFound());
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(bm_->CreateNamedBlock(b, &written_block));
  ASSERT_EQ(b, written_block->id());

  // Write some data to it.
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));
  ASSERT_OK(written_block->Sync());
  ASSERT_OK(written_block->Close());

  // Read the data back.
  gscoped_ptr<ReadableBlock> read_block;
  ASSERT_OK(bm_->OpenBlock(written_block->id(), &read_block));
  size_t sz;
  ASSERT_OK(read_block->Size(&sz));
  ASSERT_EQ(test_data.length(), sz);
  Slice data;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  ASSERT_OK(read_block->Read(0, test_data.length(), &data, scratch.get()));
  ASSERT_EQ(test_data, data);

  // Try to create the block again. It should fail.
  ASSERT_TRUE(bm_->CreateNamedBlock(written_block->id(), NULL)
              .IsAlreadyPresent());

  // Delete the block.
  ASSERT_OK(bm_->DeleteBlock(written_block->id()));
  ASSERT_TRUE(bm_->OpenBlock(written_block->id(), NULL)
              .IsNotFound());
}

// Create and delete an anonymous block.
TEST_F(BlockManagerTest, AnonymousBlockTest) {
  gscoped_ptr<WritableBlock> written_block;
  gscoped_ptr<ReadableBlock> read_block;

  ASSERT_OK(bm_->CreateAnonymousBlock(&written_block));
  ASSERT_OK(bm_->OpenBlock(written_block->id(), &read_block));
  ASSERT_OK(bm_->DeleteBlock(written_block->id()));
  ASSERT_TRUE(bm_->OpenBlock(written_block->id(), NULL)
              .IsNotFound());
}

// Test that sync_on_close=true doesn't cause any problems (we can't
// actually test the durability).
TEST_F(BlockManagerTest, SyncOnCloseTest) {
  gscoped_ptr<WritableBlock> written_block;

  CreateBlockOptions opts;
  opts.sync_on_close = true;
  ASSERT_OK(bm_->CreateAnonymousBlock(&written_block, opts));
  ASSERT_OK(written_block->Append("test data"));
  ASSERT_OK(written_block->Close());
}

// Test that we can still read from an opened block after deleting it
// (even if we can't open it again).
TEST_F(BlockManagerTest, ReadAfterDeleteTest) {
  // Write a new block.
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(bm_->CreateAnonymousBlock(&written_block));
  string test_data = "test data";
  ASSERT_OK(written_block->Append(test_data));

  // Open it for reading, then delete it. Subsequent opens should fail.
  gscoped_ptr<ReadableBlock> read_block;
  ASSERT_OK(bm_->OpenBlock(written_block->id(), &read_block));
  ASSERT_OK(bm_->DeleteBlock(written_block->id()));
  ASSERT_TRUE(bm_->OpenBlock(written_block->id(), NULL)
              .IsNotFound());

  // But we should still be able to read from the opened block.
  Slice data;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[test_data.length()]);
  ASSERT_OK(read_block->Read(0, test_data.length(), &data, scratch.get()));
  ASSERT_EQ(test_data, data);
}

TEST_F(BlockManagerTest, CloseTwiceTest) {
  // Create a new block and close it repeatedly.
  gscoped_ptr<WritableBlock> written_block;
  ASSERT_OK(bm_->CreateAnonymousBlock(&written_block));
  ASSERT_OK(written_block->Close());
  ASSERT_OK(written_block->Close());

  // Open it for reading and close it repeatedly.
  gscoped_ptr<ReadableBlock> read_block;
  ASSERT_OK(bm_->OpenBlock(written_block->id(), &read_block));
  ASSERT_OK(read_block->Close());
  ASSERT_OK(read_block->Close());
}

} // namespace fs
} // namespace kudu
