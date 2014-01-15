// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CFILE_RLE_BLOCK_H
#define KUDU_CFILE_RLE_BLOCK_H

#include <algorithm>
#include <string>

#include "gutil/port.h"
#include "cfile/block_encodings.h"
#include "common/columnblock.h"
#include "util/coding.h"
#include "util/coding-inl.h"
#include "util/hexdump.h"
#include "util/bit-stream-utils.inline.h"
#include "util/bitmap.h"
#include "util/rle-encoding.h"


namespace kudu {
namespace cfile {

//
// RLE encoder for the BOOL datatype: uses an RLE-encoded bitmap to
// represent a bool column.
//
class RleBitMapBlockBuilder : public BlockBuilder {
 public:
  RleBitMapBlockBuilder()
      : encoder_(&buf_, 1) {
    Reset();
  }

  virtual int Add(const uint8_t* vals, size_t count) {
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

  virtual Slice Finish(rowid_t ordinal_pos) {
    InlineEncodeFixed32(&buf_[0], count_);
    InlineEncodeFixed32(&buf_[4], ordinal_pos);
    encoder_.Flush();
    return Slice(buf_);
  }

  virtual void Reset() {
    count_ = 0;
    encoder_.Clear();
    encoder_.Reserve(kHeaderSize, 0);
  }

  virtual uint64_t EstimateEncodedSize() const {
    return encoder_.len();
  }

  virtual size_t Count() const {
    return count_;
  }

  // TODO Implement this method
  virtual Status GetFirstKey(void* key) const {
    return Status::NotSupported("BOOL keys not supported");
  }

 private:
  faststring buf_;
  RleEncoder<bool> encoder_;
  size_t count_;
};

//
// RLE decoder for bool datatype
//
class RleBitMapBlockDecoder : public BlockDecoder {
 public:
  explicit RleBitMapBlockDecoder(const Slice& slice)
    : data_(slice),
      parsed_(false),
      num_elems_(0),
      ordinal_pos_base_(0),
      cur_idx_(0) {
  }

  virtual Status ParseHeader() {
    CHECK(!parsed_);

     if (data_.size() < kHeaderSize) {
      return Status::Corruption(
          "not enough bytes for header in RleBitMapBlockDecoder");
    }

    num_elems_ = DecodeFixed32(&data_[0]);
    ordinal_pos_base_ = DecodeFixed32(&data_[4]);

    parsed_ = true;

    rle_decoder_ = RleDecoder<bool>(data_.data() + kHeaderSize, data_.size() - kHeaderSize, 1);

    SeekToPositionInBlock(0);

    return Status::OK();
  }

  virtual void SeekToPositionInBlock(uint pos) {
    CHECK(parsed_) << "Must call ParseHeader()";

    if (cur_idx_ == pos) {
      // No need to seek.
      return;
    } else if (cur_idx_ < pos) {
      uint nskip = pos - cur_idx_;
      rle_decoder_.Skip(nskip);
    } else {
      // This approach is also used by CFileReader to
      // seek backwards in an RLE encoded block
      rle_decoder_ = RleDecoder<bool>(data_.data() + kHeaderSize, data_.size() - kHeaderSize, 1);
      rle_decoder_.Skip(pos);
    }
    cur_idx_ = pos;
  }

  virtual Status CopyNextValues(size_t *n, ColumnDataView* dst) {
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

  virtual Status SeekAtOrAfterValue(const void *value,
                                    bool *exact_match) {
    return Status::NotSupported("BOOL keys are not supported!");
  }

  virtual bool HasNext() const { return cur_idx_ < num_elems_; }

  virtual size_t Count() const { return num_elems_; }

  virtual size_t GetCurrentIndex() const { return cur_idx_; }

  virtual rowid_t GetFirstRowId() const { return ordinal_pos_base_; }

 private:
  Slice data_;
  bool parsed_;
  uint32_t num_elems_;
  rowid_t ordinal_pos_base_;
  uint32_t cur_idx_;
  RleDecoder<bool> rle_decoder_;
};


} // namespace cfile
} // namespace kudu

#endif
