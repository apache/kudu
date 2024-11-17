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

#include "kudu/cfile/cfile_util.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>

#include <glog/logging.h>

#include "kudu/cfile/cfile_reader.h"
#include "kudu/common/column_materialization_context.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/types.h"
#include "kudu/gutil/port.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/mem_tracker.h"

namespace kudu {
namespace cfile {

using std::string;

static const int kBufSize = 1024*1024;

namespace {
constexpr const size_t kIndexBlockSize = 32 * 1024;
constexpr const int kBlockRestartInterval = 16;
constexpr const bool kWritePosIdxEnabled = false;
constexpr const bool kWriteValIdxEnabled = false;
constexpr const bool kOptimizeIndexKeysEnabled = true;
} // anonymous namespace

WriterOptions::WriterOptions()
    : index_block_size(kIndexBlockSize),
      block_restart_interval(kBlockRestartInterval),
      write_posidx(kWritePosIdxEnabled),
      write_validx(kWriteValIdxEnabled),
      optimize_index_keys(kOptimizeIndexKeysEnabled),
      validx_key_encoder(std::nullopt) {
}

WriterOptionsBuilder::WriterOptionsBuilder() noexcept
    : index_block_size_(kIndexBlockSize),
      block_restart_interval_(kBlockRestartInterval),
      write_posidx_(kWritePosIdxEnabled),
      write_validx_(kWriteValIdxEnabled),
      optimize_index_keys_(kOptimizeIndexKeysEnabled),
      validx_key_encoder_(std::nullopt) {
}

WriterOptionsBuilder& WriterOptionsBuilder::index_block_size(size_t size) noexcept {
  index_block_size_ = size;
  return *this;
}

WriterOptionsBuilder& WriterOptionsBuilder::block_restart_interval(int interval) noexcept {
  block_restart_interval_ = interval;
  return *this;
}

WriterOptionsBuilder& WriterOptionsBuilder::write_posidx(bool is_enabled) noexcept {
  write_posidx_ = is_enabled;
  return *this;
}

WriterOptionsBuilder& WriterOptionsBuilder::write_validx(bool is_enabled) noexcept {
  write_validx_ = is_enabled;
  return *this;
}

WriterOptionsBuilder& WriterOptionsBuilder::optimize_index_keys(bool is_enabled) noexcept {
  optimize_index_keys_ = is_enabled;
  return *this;
}

WriterOptionsBuilder& WriterOptionsBuilder::storage_attributes(
    const ColumnStorageAttributes& attrs) noexcept {
  storage_attributes_ = attrs;
  return *this;
}

WriterOptionsBuilder& WriterOptionsBuilder::validx_key_encoder(ValidxKeyEncoder enc) noexcept {
  validx_key_encoder_.emplace(std::move(enc));
  return *this;
}

WriterOptions WriterOptionsBuilder::Build() noexcept {
  WriterOptions opt;
  opt.index_block_size = index_block_size_;
  opt.block_restart_interval = block_restart_interval_;
  opt.write_posidx = write_posidx_;
  opt.write_validx = write_validx_;
  opt.optimize_index_keys = optimize_index_keys_;
  opt.storage_attributes = storage_attributes_;
  opt.validx_key_encoder = validx_key_encoder_;
  return opt;
}

Status DumpIterator(const CFileReader& reader,
                    CFileIterator* it,
                    std::ostream* out,
                    int num_rows,
                    int indent) {
  RowBlockMemory mem(8192);
  uint8_t buf[kBufSize];
  const TypeInfo* type = reader.type_info();
  size_t max_rows = kBufSize/type->size();
  uint8_t non_null_bitmap[BitmapSize(max_rows)];
  ColumnBlock cb(type,
                 reader.is_nullable() ? non_null_bitmap : nullptr,
                 buf,
                 max_rows,
                 &mem);
  SelectionVector sel(max_rows);
  ColumnMaterializationContext ctx(0, nullptr, &cb, &sel);
  string strbuf;
  size_t count = 0;
  while (it->HasNext()) {
    size_t n = num_rows == 0 ? max_rows : std::min(max_rows, num_rows - count);
    if (n == 0) break;

    RETURN_NOT_OK(it->CopyNextValues(&n, &ctx));

    if (reader.is_nullable()) {
      for (size_t i = 0; i < n; i++) {
        strbuf.append(indent, ' ');
        const void* ptr = cb.nullable_cell_ptr(i);
        if (ptr != nullptr) {
          type->AppendDebugStringForValue(ptr, &strbuf);
        } else {
          strbuf.append("NULL");
        }
        strbuf.push_back('\n');
      }
    } else {
      for (size_t i = 0; i < n; i++) {
        strbuf.append(indent, ' ');
        type->AppendDebugStringForValue(cb.cell_ptr(i), &strbuf);
        strbuf.push_back('\n');
      }
    }

    *out << strbuf;
    strbuf.clear();
    mem.Reset();
    count += n;
  }

  VLOG(1) << "Dumped " << count << " rows";

  return Status::OK();
}

ReaderOptions::ReaderOptions()
  : parent_mem_tracker(MemTracker::GetRootTracker()) {
}

size_t CommonPrefixLength(const Slice& slice_a, const Slice& slice_b) {
  // This implementation is modeled after strings::fastmemcmp_inlined().
  int len = std::min(slice_a.size(), slice_b.size());
  const uint8_t* a = slice_a.data();
  const uint8_t* b = slice_b.data();
  const uint8_t* a_limit = a + len;

  const size_t sizeof_uint64 = sizeof(uint64_t);
  // Move forward 8 bytes at a time until finding an unequal portion.
  while (a + sizeof_uint64 <= a_limit &&
         UNALIGNED_LOAD64(a) == UNALIGNED_LOAD64(b)) {
    a += sizeof_uint64;
    b += sizeof_uint64;
  }

  // Same, 4 bytes at a time.
  const size_t sizeof_uint32 = sizeof(uint32_t);
  while (a + sizeof_uint32 <= a_limit &&
         UNALIGNED_LOAD32(a) == UNALIGNED_LOAD32(b)) {
    a += sizeof_uint32;
    b += sizeof_uint32;
  }

  // Now one byte at a time. We could do a 2-bytes-at-a-time loop,
  // but we're following the example of fastmemcmp_inlined(). The benefit of
  // 2-at-a-time likely doesn't outweigh the cost of added code size.
  while (a < a_limit &&
         *a == *b) {
    a++;
    b++;
  }

  return a - slice_a.data();
}

void GetSeparatingKey(const Slice& left, Slice* right) {
  DCHECK_LE(left, *right);
  size_t cpl = CommonPrefixLength(left, *right);
  right->truncate(cpl == right->size() ? cpl : cpl + 1);
}

} // namespace cfile
} // namespace kudu
