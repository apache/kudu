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

#ifndef KUDU_FS_FS_TEST_UTIL_H
#define KUDU_FS_FS_TEST_UTIL_H

#include <memory>
#include <numeric>
#include <vector>

#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/array_view.h"
#include "kudu/util/malloc.h"
#include "kudu/util/slice.h"

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
//   unique_ptr<ReadableBlock> block;
//   fs_manager->OpenBlock("some block id", &block);
//   size_t bytes_read = 0;
//   unique_ptr<ReadableBlock> tr_block(new CountingReadableBlock(std::move(block), &bytes_read));
//   tr_block->Read(0, 100, ...);
//   tr_block->Read(0, 200, ...);
//   ASSERT_EQ(300, bytes_read);
//
class CountingReadableBlock : public ReadableBlock {
 public:
  CountingReadableBlock(std::unique_ptr<ReadableBlock> block, size_t* bytes_read)
    : block_(std::move(block)),
      bytes_read_(bytes_read) {
  }

  virtual const BlockId& id() const OVERRIDE {
    return block_->id();
  }

  virtual Status Close() OVERRIDE {
    return block_->Close();
  }

  virtual Status Size(uint64_t* sz) const OVERRIDE {
    return block_->Size(sz);
  }

  virtual Status Read(uint64_t offset, Slice result) const OVERRIDE {
    return ReadV(offset, ArrayView<Slice>(&result, 1));
  }

  virtual Status ReadV(uint64_t offset, ArrayView<Slice> results) const OVERRIDE {
    RETURN_NOT_OK(block_->ReadV(offset, results));
    // Calculate the read amount of data
    size_t length = std::accumulate(results.begin(), results.end(), static_cast<size_t>(0),
                               [&](int sum, const Slice& curr) {
                                 return sum + curr.size();
                               });
    *bytes_read_ += length;
    return Status::OK();
  }

  virtual size_t memory_footprint() const OVERRIDE {
    return block_->memory_footprint();
  }

 private:
  std::unique_ptr<ReadableBlock> block_;
  size_t* bytes_read_;
};

// Creates a copy of the specified block and corrupts a byte of its data at the
// given 'corrupt_offset' by flipping a bit at offset 'flip_bit'. Returns the
// block id of the corrupted block. Does not change the original block.
inline Status CreateCorruptBlock(FsManager* fs_manager, const BlockId in_id,
    const uint64_t corrupt_offset, uint8_t flip_bit, BlockId* out_id) {
  DCHECK_LT(flip_bit, 8);

  // Read the input block
  std::unique_ptr<ReadableBlock> source;
  RETURN_NOT_OK(fs_manager->OpenBlock(in_id, &source));
  uint64_t file_size;
  RETURN_NOT_OK(source->Size(&file_size));
  uint8_t data_scratch[file_size];
  Slice data(data_scratch, file_size);
  RETURN_NOT_OK(source->Read(0, data));

  // Corrupt the data and write to a new block
  uint8_t orig = data.data()[corrupt_offset];
  uint8_t corrupt = orig ^ (static_cast<uint8_t>(1) << flip_bit);
  data.mutable_data()[corrupt_offset] = corrupt;
  std::unique_ptr<WritableBlock> writer;
  RETURN_NOT_OK(fs_manager->CreateNewBlock({}, &writer));
  RETURN_NOT_OK(writer->Append(data));
  RETURN_NOT_OK(writer->Close());
  *out_id = writer->id();
  return Status::OK();
}

} // namespace fs
} // namespace kudu

#endif
