// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CFILE_PLAIN_BLOCK_H
#define KUDU_CFILE_PLAIN_BLOCK_H

#include <algorithm>
#include <string>

#include "cfile/block_encodings.h"
#include "common/columnblock.h"
#include "util/coding.h"
#include "util/coding-inl.h"
#include "util/hexdump.h"

namespace kudu {
namespace cfile {

struct WriterOptions;

template<typename DataType>
inline DataType Decode(const uint8_t *ptr) {
  DataType result;
  memcpy(&result, ptr, sizeof(result));
  return result;
}

static const size_t kHeaderSize = sizeof(uint32_t) * 2;

//
// A plain encoder for generic fixed size data types.
//
template<DataType Type>
class PlainBlockBuilder : public BlockBuilder {
 public:
  explicit PlainBlockBuilder(const WriterOptions *options)
      : options_(options) {
    Reset();
  }

  virtual int Add(const uint8_t *vals_void, size_t count) {
    int old_size = buffer_.size();
    buffer_.resize(old_size + count * kCppTypeSize);
    memcpy(&buffer_[old_size], vals_void, count * kCppTypeSize);
    count_ += count;
    return count;
  }

  virtual Slice Finish(rowid_t ordinal_pos) {
    InlineEncodeFixed32(&buffer_[0], count_);
    InlineEncodeFixed32(&buffer_[4], ordinal_pos);
    return Slice(buffer_);
  }

  virtual void Reset() {
    count_ = 0;
    buffer_.clear();
    buffer_.resize(kHeaderSize);
  }

  virtual uint64_t EstimateEncodedSize() const {
    return buffer_.size();
  }

  virtual size_t Count() const {
    return count_;
  }

  virtual Status GetFirstKey(void *key) const {
    DCHECK_GT(count_, 0);
    *reinterpret_cast<CppType *>(key) = Decode<DataType>(&buffer_[kHeaderSize]);
    return Status::OK();
  }

 private:
  faststring buffer_;
  const WriterOptions *options_;
  size_t count_;
  typedef typename TypeTraits<Type>::cpp_type CppType;
  enum {
    kCppTypeSize = TypeTraits<Type>::size
  };

};

//
// A plain decoder for generic fixed size data types.
//
template<DataType Type>
class PlainBlockDecoder : public BlockDecoder {
 public:
  explicit PlainBlockDecoder(const Slice &slice)
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
          "not enough bytes for header in PlainBlockDecoder");
    }

    num_elems_ = DecodeFixed32(&data_[0]);
    ordinal_pos_base_ = DecodeFixed32(&data_[4]);

    if (data_.size() != kHeaderSize + num_elems_ * size_of_type) {
      return Status::Corruption(
          string("unexpected data size. ") + "\nFirst 100 bytes: "
              + HexDump(
                  Slice(data_.data(),
                        (data_.size() < 100 ? data_.size() : 100))));
    }

    parsed_ = true;

    SeekToPositionInBlock(0);

    return Status::OK();
  }

  virtual void SeekToPositionInBlock(uint pos) {
    CHECK(parsed_) << "Must call ParseHeader()";

    if (PREDICT_FALSE(num_elems_ == 0)) {
      DCHECK_EQ(0, pos);
      return;
    }

    DCHECK_LT(pos, num_elems_);
    cur_idx_ = pos;
  }

  virtual Status SeekAtOrAfterValue(const void *value, bool *exact_match) {
    DCHECK(value != NULL);

    const CppType &target = *reinterpret_cast<const CppType *>(value);

    uint32_t left = 0;
    uint32_t right = num_elems_;
    while (left != right) {
      uint32_t mid = (left + right) / 2;
      CppType mid_key = Decode<CppType>(
          &data_[kHeaderSize + mid * size_of_type]);
      // assumes CppType has an implementation of operator<()
      if (mid_key < target) {
        left = mid + 1;
      } else if (mid_key > target) {
        right = mid;
      } else {
        cur_idx_ = mid;
        *exact_match = true;
        return Status::OK();
      }
    }

    *exact_match = false;
    cur_idx_ = left;
    if (cur_idx_ == num_elems_) {
      return Status::NotFound("after last key in block");
    }

    return Status::OK();
  }

  virtual Status CopyNextValues(size_t *n, ColumnDataView *dst) {
    DCHECK(parsed_);
    DCHECK_LE(*n, dst->nrows());
    DCHECK_EQ(dst->stride(), sizeof(CppType));

    if (PREDICT_FALSE(*n == 0 || cur_idx_ >= num_elems_)) {
      *n = 0;
      return Status::OK();
    }

    size_t max_fetch = std::min(*n, static_cast<size_t>(num_elems_ - cur_idx_));
    memcpy(dst->data(), &data_[kHeaderSize + cur_idx_ * size_of_type], max_fetch * size_of_type);
    cur_idx_ += max_fetch;
    *n = max_fetch;
    return Status::OK();
  }

  virtual bool HasNext() const {
    return cur_idx_ < num_elems_;
  }

  virtual size_t Count() const {
    return num_elems_;
  }

  virtual rowid_t ordinal_pos() const {
    return ordinal_pos_base_ + cur_idx_;
  }

 private:

  Slice data_;
  bool parsed_;
  uint32_t num_elems_;
  rowid_t ordinal_pos_base_;
  uint32_t cur_idx_;
  typedef typename TypeTraits<Type>::cpp_type CppType;
  enum {
    size_of_type = TypeTraits<Type>::size
  };

};

}
}

#endif
