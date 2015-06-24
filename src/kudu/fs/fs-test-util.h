// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_FS_FS_TEST_UTIL_H
#define KUDU_FS_FS_TEST_UTIL_H

#include "kudu/fs/block_manager.h"
#include "kudu/util/malloc.h"

namespace kudu {
namespace fs {

// ReadableBlock that counts the total number of bytes read.
//
// The counter is kept separate from the class itself because
// ReadableBlocks are often wholly owned by other objects, preventing tests
// from easily snooping on the counter's value.
//
// Sample usage:
//
//   gscoped_ptr<ReadableBlock> block;
//   fs_manager->OpenBlock("some block id", &block);
//   size_t bytes_read = 0;
//   gscoped_ptr<ReadableBlock> tr_block(new CountingReadableBlock(block.Pass(), &bytes_read));
//   tr_block->Read(0, 100, ...);
//   tr_block->Read(0, 200, ...);
//   ASSERT_EQ(300, bytes_read);
//
class CountingReadableBlock : public ReadableBlock {
 public:
  CountingReadableBlock(gscoped_ptr<ReadableBlock> block, size_t* bytes_read)
    : block_(block.Pass()),
      bytes_read_(bytes_read) {
  }

  virtual const BlockId& id() const OVERRIDE {
    return block_->id();
  }

  virtual Status Close() OVERRIDE {
    return block_->Close();
  }

  virtual Status Size(size_t* sz) const OVERRIDE {
    return block_->Size(sz);
  }

  virtual Status Read(uint64_t offset, size_t length,
                      Slice* result, uint8_t* scratch) const OVERRIDE {
    RETURN_NOT_OK(block_->Read(offset, length, result, scratch));
    *bytes_read_ += length;
    return Status::OK();
  }

  virtual size_t memory_footprint() const OVERRIDE {
    return block_->memory_footprint();
  }

 private:
  gscoped_ptr<ReadableBlock> block_;
  size_t* bytes_read_;
};

} // namespace fs
} // namespace kudu

#endif
