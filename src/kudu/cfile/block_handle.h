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

#ifndef KUDU_CFILE_BLOCK_HANDLE_H
#define KUDU_CFILE_BLOCK_HANDLE_H

#include <memory>

#include <boost/variant/get.hpp>
#include <boost/variant/variant.hpp>

#include "kudu/cfile/block_cache.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/common/rowblock_memory.h"

namespace kudu {
namespace cfile {

// When blocks are read, they are sometimes resident in the block cache, and sometimes skip the
// block cache. In the case that they came from the cache, we just need to dereference them when
// they stop being used. In the case that they didn't come from cache, we need to actually free
// the underlying data.
//
// NOTE ON REFERENCE COUNTING
// ---------------------------
// Note that the BlockHandle itself may refer to a BlockCacheHandle, which itself is
// reference-counted. When all of the references to a BlockHandle go out of scope, it
// results in decrementing the BlockCacheHandle's reference count.
class BlockHandle : public RefCountedThreadSafe<BlockHandle> {
 public:
  static scoped_refptr<BlockHandle> WithOwnedData(const Slice& data) {
    return { new BlockHandle(data) };
  }

  static scoped_refptr<BlockHandle> WithDataFromCache(BlockCacheHandle handle) {
    return { new BlockHandle(std::move(handle)) };
  }

  Slice data() const { return data_; }

  scoped_refptr<BlockHandle> SubrangeBlock(size_t offset, size_t len) {
    return { new BlockHandle(this, offset, len) };
  }

 protected:
  friend class RefCountedThreadSafe<BlockHandle>;

  // Marker to indicate that this object isn't initialized (owns no data).
  struct Uninitialized {};
  // Marker that the handle directly owns the data in 'data_', rather than referring
  // to data in the block cache or to another BlockHandle.
  struct OwnedData {};

  ~BlockHandle() {
    Reset();
  }
  // Constructor for owned data.
  explicit BlockHandle(Slice data)
      : data_(data),
        ref_(OwnedData{}) {
  }

  explicit BlockHandle(BlockCacheHandle dblk_data)
      : data_(dblk_data.data()),
        ref_(std::move(dblk_data)) {
  }

  BlockHandle(scoped_refptr<BlockHandle> other, size_t offset, size_t len)
      : data_(other->data()),
        ref_(std::move(other)) {
    data_.remove_prefix(offset);
    data_.truncate(len);
  }

  void Reset() {
    if (boost::get<OwnedData>(&ref_) != nullptr) {
      delete [] data_.data();
    }
    data_ = "";
    ref_ = Uninitialized{};
  }

  Slice data_;
  boost::variant<Uninitialized, OwnedData, BlockCacheHandle, scoped_refptr<BlockHandle>> ref_;

  DISALLOW_COPY_AND_ASSIGN(BlockHandle);
};

} // namespace cfile
} // namespace kudu
#endif
