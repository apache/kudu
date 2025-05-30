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
#pragma once

#include <algorithm>
#include <string>
#include <vector>

#include "kudu/gutil/port.h"
#include "kudu/cfile/block_encodings.h"
#include "kudu/cfile/block_handle.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/common/columnblock.h"
#include "kudu/util/coding.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/bit-stream-utils.inline.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/rle-encoding.h"


namespace kudu {
namespace cfile {

struct WriterOptions;

enum {
  kRleBitmapBlockHeaderSize = 8
};

//
// RLE encoder for the BOOL datatype: uses an RLE-encoded bitmap to
// represent a bool column.
//
class RleBitMapBlockBuilder final : public BlockBuilder {
 public:
  explicit RleBitMapBlockBuilder(const WriterOptions* options)
      : options_(options),
        encoder_(&buf_) {
    Reset();
  }

  int Add(const uint8_t* vals, size_t count) override {
     for (const uint8_t* val = vals;
          val < vals + count;
          ++val) {
       // TODO (perf) : doing this one bit a time is probably
       //               inefficient.
       encoder_.Put(*val, 1);
    }
    count_ += count;
    return count;
  }

  bool IsBlockFull() const override {
    return encoder_.len() > options_->storage_attributes.cfile_block_size;
  }

  void Finish(rowid_t ordinal_pos, std::vector<Slice>* slices) override {
    InlineEncodeFixed32(&buf_[0], count_);
    InlineEncodeFixed32(&buf_[4], ordinal_pos);
    encoder_.Flush();
    *slices = { buf_ };
  }

  void Reset() override {
    count_ = 0;
    encoder_.Clear();
    encoder_.Reserve(kRleBitmapBlockHeaderSize, 0);
  }

  size_t Count() const override {
    return count_;
  }

  // TODO(afeinberg) Implement this method
  Status GetFirstKey(void* key) const override {
    return Status::NotSupported("BOOL keys not supported");
  }

  // TODO(afeinberg) Implement this method
  Status GetLastKey(void* key) const override {
    return Status::NotSupported("BOOL keys not supported");
  }

 private:
  const WriterOptions* const options_;
  faststring buf_;
  RleEncoder<bool, 1> encoder_;
  size_t count_;
};

//
// RLE decoder for bool datatype
//
class RleBitMapBlockDecoder final : public BlockDecoder {
 public:
  explicit RleBitMapBlockDecoder(scoped_refptr<BlockHandle> block)
      : block_(std::move(block)),
        data_(block_->data()),
        parsed_(false),
        num_elems_(0),
        ordinal_pos_base_(0),
        cur_idx_(0) {
  }

  Status ParseHeader() override {
    DCHECK(!parsed_);

    if (PREDICT_FALSE(data_.size() < kRleBitmapBlockHeaderSize)) {
      return Status::Corruption(
          "not enough bytes for header in RleBitMapBlockDecoder");
    }

    num_elems_ = DecodeFixed32(&data_[0]);
    ordinal_pos_base_ = DecodeFixed32(&data_[4]);

    parsed_ = true;

    rle_decoder_ = RleDecoder<bool, 1>(data_.data() + kRleBitmapBlockHeaderSize,
                                       data_.size() - kRleBitmapBlockHeaderSize);

    SeekToPositionInBlock(0);

    return Status::OK();
  }

  void SeekToPositionInBlock(uint pos) override {
    DCHECK(parsed_) << "Must call ParseHeader()";
    DCHECK_LE(pos, num_elems_)
      << "Tried to seek to " << pos << " which is > number of elements ("
      << num_elems_ << ") in the block!";
    // If the block is empty (e.g. the column is filled with nulls), there is no data to seek.
    if (PREDICT_FALSE(num_elems_ == 0)) {
      return;
    }

    if (cur_idx_ == pos) {
      // No need to seek.
      return;
    } else if (cur_idx_ < pos) {
      uint nskip = pos - cur_idx_;
      rle_decoder_.Skip(nskip);
    } else {
      // This approach is also used by CFileReader to
      // seek backwards in an RLE encoded block
      DCHECK_GE(data_.size(), kRleBitmapBlockHeaderSize);
      rle_decoder_ = RleDecoder<bool, 1>(data_.data() + kRleBitmapBlockHeaderSize,
                                         data_.size() - kRleBitmapBlockHeaderSize);
      rle_decoder_.Skip(pos);
    }
    cur_idx_ = pos;
  }

  Status CopyNextValues(size_t* n, ColumnDataView* dst) override {
    DCHECK(parsed_);

    DCHECK_LE(*n, dst->nrows());
    DCHECK_EQ(dst->stride(), sizeof(bool));

    if (PREDICT_FALSE(*n == 0 || cur_idx_ >= num_elems_)) {
      *n = 0;
      return Status::OK();
    }

    size_t bits_to_fetch = std::min(*n, static_cast<size_t>(num_elems_ - cur_idx_));
    size_t remaining = bits_to_fetch;
    uint8_t* data_ptr = dst->data();
    // TODO : do this a word/byte at a time as opposed bit at a time
    while (remaining > 0) {
      bool result = rle_decoder_.Get(reinterpret_cast<bool*>(data_ptr));
      DCHECK(result);
      remaining--;
      data_ptr++;
    }

    cur_idx_ += bits_to_fetch;
    *n = bits_to_fetch;

    return Status::OK();
  }

  Status SeekAtOrAfterValue(const void* /*value*/,
                            bool* /*exact_match*/) override {
    return Status::NotSupported("BOOL keys are not supported!");
  }

  bool HasNext() const override { return cur_idx_ < num_elems_; }

  size_t Count() const override { return num_elems_; }

  size_t GetCurrentIndex() const override { return cur_idx_; }

  rowid_t GetFirstRowId() const override { return ordinal_pos_base_; }

 private:
  scoped_refptr<BlockHandle> block_;
  Slice data_;
  bool parsed_;
  uint32_t num_elems_;
  rowid_t ordinal_pos_base_;
  uint32_t cur_idx_;
  RleDecoder<bool, 1> rle_decoder_;
};

//
// RLE builder for generic integer types. What is missing is some way
// to enforce that this can only be instantiated for INT types.
// TODO : consider if this can also be used for BOOL with only minor
//        alterations
template <DataType IntType>
class RleIntBlockBuilder final : public BlockBuilder {
 public:
  explicit RleIntBlockBuilder(const WriterOptions* options)
      : options_(options),
        rle_encoder_(&buf_) {
    Reset();
  }

  bool IsBlockFull() const override {
    return rle_encoder_.len() > options_->storage_attributes.cfile_block_size;
  }

  int Add(const uint8_t* vals_void, size_t count) override {
    DCHECK_EQ(reinterpret_cast<uintptr_t>(vals_void) & (alignof(CppType) - 1), 0)
        << "Pointer passed to Add() must be naturally-aligned";

    const CppType* vals = reinterpret_cast<const CppType*>(vals_void);
    if (PREDICT_FALSE(count_ == 0)) {
      first_key_ = vals[0];
    }
    for (size_t i = 0; i < count; ++i) {
      rle_encoder_.Put(vals[i], 1);
    }
    count_ += count;
    if (count > 0) {
      last_key_ = vals[count - 1];
    }
    return count;
  }

  void Finish(rowid_t ordinal_pos, std::vector<Slice>* slices) override {
    InlineEncodeFixed32(&buf_[0], count_);
    InlineEncodeFixed32(&buf_[4], ordinal_pos);
    rle_encoder_.Flush();
    *slices = { Slice(buf_) };
  }

  void Reset() override {
    count_ = 0;
    rle_encoder_.Clear();
    rle_encoder_.Reserve(kRleBitmapBlockHeaderSize, 0);
  }

  size_t Count() const override {
    return count_;
  }

  Status GetFirstKey(void* key) const override {
    if (PREDICT_FALSE(count_ == 0)) {
      return Status::NotFound("No keys in the block");
    }
    UnalignedStore<CppType>(key, first_key_);
    return Status::OK();
  }

  Status GetLastKey(void* key) const override {
    if (PREDICT_FALSE(count_ == 0)) {
      return Status::NotFound("No keys in the block");
    }
    UnalignedStore<CppType>(key, last_key_);
    return Status::OK();
  }

 private:
  typedef typename TypeTraits<IntType>::cpp_type CppType;
  enum {
    kCppTypeSize = TypeTraits<IntType>::size
  };

  const WriterOptions* const options_;
  CppType first_key_;
  CppType last_key_;
  faststring buf_;
  size_t count_;
  RleEncoder<CppType, kCppTypeSize * 8> rle_encoder_;
};

//
// RLE decoder for generic integer types.
//
// TODO : as with the matching BlockBuilder above (see comments for
//        that class), it may be be possible to re-use most of the
//        code here for the BOOL type.
//
template <DataType IntType>
class RleIntBlockDecoder final : public BlockDecoder {
 public:
  explicit RleIntBlockDecoder(scoped_refptr<BlockHandle> block)
      : block_(std::move(block)),
        data_(block_->data()),
        parsed_(false),
        num_elems_(0),
        ordinal_pos_base_(0),
        cur_idx_(0) {
  }

  Status ParseHeader() override {
    DCHECK(!parsed_);

    if (PREDICT_FALSE(data_.size() < kRleBitmapBlockHeaderSize)) {
      return Status::Corruption(
          "not enough bytes for header in RleIntBlockDecoder");
    }

    num_elems_ = DecodeFixed32(&data_[0]);
    ordinal_pos_base_ = DecodeFixed32(&data_[4]);

    parsed_ = true;

    rle_decoder_ = RleDecoder<CppType, kCppTypeSize * 8>(
        data_.data() + kRleBitmapBlockHeaderSize,
        data_.size() - kRleBitmapBlockHeaderSize);

    SeekToPositionInBlock(0);

    return Status::OK();
  }

  void SeekToPositionInBlock(uint pos) override {
    DCHECK(parsed_) << "Must call ParseHeader()";
    DCHECK_LE(pos, num_elems_)
        << "Tried to seek to " << pos << " which is > number of elements ("
        << num_elems_ << ") in the block!";
    // If the block is empty (e.g. the column is filled with nulls), there is no data to seek.
    if (PREDICT_FALSE(num_elems_ == 0)) {
      return;
    }

    if (cur_idx_ == pos) {
      // No need to seek.
      return;
    } else if (cur_idx_ < pos) {
      uint nskip = pos - cur_idx_;
      rle_decoder_.Skip(nskip);
    } else {
      rle_decoder_ = RleDecoder<CppType, kCppTypeSize * 8>(
          data_.data() + kRleBitmapBlockHeaderSize,
          data_.size() - kRleBitmapBlockHeaderSize);
      rle_decoder_.Skip(pos);
    }
    cur_idx_ = pos;
  }

  Status SeekAtOrAfterValue(const void* value_void, bool* exact_match) override {
    // Currently using linear search as we do not check whether a
    // mid-point of a buffer will fall on a literal or not.
    //
    // TODO (perf): make this faster by moving forward a 'run at a time'
    //              by perhaps pushing this loop down into RleDecoder itself
    // TODO (perf): investigate placing pointers somewhere in either the
    // header or the tail to speed up search.

    SeekToPositionInBlock(0);

    CppType target = UnalignedLoad<CppType>(value_void);

    while (cur_idx_ < num_elems_) {
      CppType cur_elem = 0;
      if (!rle_decoder_.Get(&cur_elem)) {
        break;
      }
      if (cur_elem == target) {
        rle_decoder_.RewindOne();
        *exact_match = true;
        return Status::OK();
      }
      if (cur_elem > target) {
        rle_decoder_.RewindOne();
        *exact_match = false;
        return Status::OK();
      }
      cur_idx_++;
    }

    return Status::NotFound("not in block");
  }

  Status CopyNextValues(size_t* n, ColumnDataView* dst) override {
    DCHECK(parsed_);

    DCHECK_LE(*n, dst->nrows());
    DCHECK_EQ(dst->stride(), sizeof(CppType));

    if (PREDICT_FALSE(*n == 0 || cur_idx_ >= num_elems_)) {
      *n = 0;
      return Status::OK();
    }

    size_t to_fetch = std::min(*n, static_cast<size_t>(num_elems_ - cur_idx_));
    size_t remaining = to_fetch;
    uint8_t* data_ptr = dst->data();
    while (remaining > 0) {
      bool result = rle_decoder_.Get(reinterpret_cast<CppType*>(data_ptr));
      DCHECK(result);
      remaining--;
      data_ptr += kCppTypeSize;
    }

    cur_idx_ += to_fetch;
    *n = to_fetch;
    return Status::OK();
  }

  Status CopyNextAndEval(size_t* n,
                         ColumnMaterializationContext* ctx,
                         SelectionVectorView* sel,
                         ColumnDataView* dst) override {
    DCHECK(parsed_);

    DCHECK_LE(*n, dst->nrows());
    DCHECK_EQ(dst->stride(), sizeof(CppType));

    ctx->SetDecoderEvalSupported();
    if (PREDICT_FALSE(*n == 0 || cur_idx_ >= num_elems_)) {
      *n = 0;
      return Status::OK();
    }

    const size_t to_fetch = std::min(*n, static_cast<size_t>(num_elems_ - cur_idx_));
    size_t remaining = to_fetch;
    uint8_t* data_ptr = dst->data();
    size_t row_offset = 0;
    while (remaining > 0) {
      CppType val = 0;
      const size_t num_read = rle_decoder_.GetNextRun(&val, remaining);
      DCHECK(num_read > 0);
      DCHECK_LE(num_read, remaining);
      if (ctx->pred()->EvaluateCell<IntType>(static_cast<const void*>(&val))) {
        // Copy data for matching predicate
        for (size_t row_idx = row_offset; row_idx < row_offset + num_read;
             ++row_idx, data_ptr += kCppTypeSize) {
          // Skip copying if the row has already been cleared.
          if (!sel->TestBit(row_idx)) {
            continue;
          }
          *(reinterpret_cast<CppType*>(data_ptr)) = val;
        }
      } else {
        // Mark that the rows will not be returned.
        sel->ClearBits(num_read, row_offset);
        data_ptr += num_read * kCppTypeSize;
      }
      remaining -= num_read;
      row_offset += num_read;
    }

    cur_idx_ += to_fetch;
    *n = to_fetch;

    return Status::OK();
  }

  bool HasNext() const override {
    return cur_idx_ < num_elems_;
  }

  size_t Count() const override {
    return num_elems_;
  }

  size_t GetCurrentIndex() const override {
    return cur_idx_;
  }

  rowid_t GetFirstRowId() const override {
    return ordinal_pos_base_;
  };
 private:
  typedef typename TypeTraits<IntType>::cpp_type CppType;

  enum {
    kCppTypeSize = TypeTraits<IntType>::size
  };

  scoped_refptr<BlockHandle> block_;
  Slice data_;
  bool parsed_;
  uint32_t num_elems_;
  rowid_t ordinal_pos_base_;
  size_t cur_idx_;
  RleDecoder<CppType, kCppTypeSize * 8> rle_decoder_;
};

} // namespace cfile
} // namespace kudu
