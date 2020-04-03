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
#ifndef KUDU_CFILE_BINARY_PREFIX_BLOCK_H
#define KUDU_CFILE_BINARY_PREFIX_BLOCK_H

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <vector>

#include <glog/logging.h>

#include "kudu/cfile/block_encodings.h"
#include "kudu/common/rowid.h"
#include "kudu/gutil/port.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

class Arena;
class ColumnDataView;

namespace cfile {

struct WriterOptions;

// Encoding for data blocks of binary data that have common prefixes.
// This encodes in a manner similar to LevelDB (prefix coding)
class BinaryPrefixBlockBuilder final : public BlockBuilder {
 public:
  explicit BinaryPrefixBlockBuilder(const WriterOptions *options);

  bool IsBlockFull() const override;

  int Add(const uint8_t *vals, size_t count) OVERRIDE;

  void Finish(rowid_t ordinal_pos, std::vector<Slice>* slices) override;

  void Reset() OVERRIDE;

  size_t Count() const OVERRIDE;

  // Return the first added key.
  // key should be a Slice *
  Status GetFirstKey(void *key) const OVERRIDE;

  // Return the last added key.
  // key should be a Slice *
  Status GetLastKey(void *key) const OVERRIDE;

 private:
  faststring header_buf_;
  faststring buffer_;
  faststring last_val_;

  // Restart points, offsets relative to start of block
  std::vector<uint32_t> restarts_;

  int val_count_;
  int vals_since_restart_;
  bool finished_;

  const WriterOptions *options_;
};

// Decoder for BINARY type, PREFIX encoding
class BinaryPrefixBlockDecoder final : public BlockDecoder {
 public:
  explicit BinaryPrefixBlockDecoder(Slice slice);

  virtual Status ParseHeader() OVERRIDE;
  virtual void SeekToPositionInBlock(uint pos) OVERRIDE;
  virtual Status SeekAtOrAfterValue(const void *value,
                                    bool *exact_match) OVERRIDE;
  Status CopyNextValues(size_t *n, ColumnDataView *dst) OVERRIDE;

  virtual bool HasNext() const OVERRIDE {
    DCHECK(parsed_);
    return cur_idx_ < num_elems_;
  }

  virtual size_t Count() const OVERRIDE {
    DCHECK(parsed_);
    return num_elems_;
  }

  virtual size_t GetCurrentIndex() const OVERRIDE {
    DCHECK(parsed_);
    return cur_idx_;
  }

  virtual rowid_t GetFirstRowId() const OVERRIDE {
    DCHECK(parsed_);
    return ordinal_pos_base_;
  }

  // Minimum length of a header.
  // Currently one group of varints for an empty block, so minimum is 5 bytes
  static const size_t kMinHeaderSize = 5;

 private:
  Status SkipForward(int n);
  Status CheckNextPtr();
  Status ParseNextValue();
  Status ParseNextIntoArena(Slice prev_val, Arena *dst, Slice *copied);

  const uint8_t *DecodeEntryLengths(const uint8_t *ptr,
                           uint32_t *shared,
                           uint32_t *non_shared) const;

  const uint8_t *GetRestartPoint(uint32_t idx) const;
  void SeekToRestartPoint(uint32_t idx);

  void SeekToStart();

  Slice data_;

  bool parsed_;

  uint32_t num_elems_;
  rowid_t ordinal_pos_base_;

  uint32_t num_restarts_;
  const uint32_t *restarts_;
  uint32_t restart_interval_;

  const uint8_t *data_start_;

  // Index of the next row to be returned by CopyNextValues, relative to
  // the block's base offset.
  // When the block is exhausted, cur_idx_ == num_elems_
  uint32_t cur_idx_;

  // The first value to be returned by the next CopyNextValues().
  faststring cur_val_;

  // The ptr pointing to the next element to parse. This is for the entry
  // following cur_val_
  // This is advanced by ParseNextValue()
  const uint8_t *next_ptr_;
};

} // namespace cfile
} // namespace kudu

#endif // KUDU_CFILE_BINARY_PREFIX_BLOCK_H
