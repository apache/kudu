// Copyright (c) 2013, Cloudera, inc.

#include <algorithm>

#include "cfile/cfile.h"
#include "cfile/gvint_block.h"
#include "common/columnblock.h"
#include "util/group_varint-inl.h"

namespace kudu { namespace cfile {

using kudu::coding::AppendGroupVarInt32;
using kudu::coding::CalcRequiredBytes32;
using kudu::coding::DecodeGroupVarInt32;
using kudu::coding::DecodeGroupVarInt32_SSE_Add;
using kudu::coding::AppendGroupVarInt32Sequence;

GVIntBlockBuilder::GVIntBlockBuilder(const WriterOptions *options) :
  estimated_raw_size_(0),
  options_(options)
{}


void GVIntBlockBuilder::Reset() {
  ints_.clear();
  buffer_.clear();
  ints_.reserve(options_->block_size / sizeof(uint32_t));
  estimated_raw_size_ = 0;
}

int GVIntBlockBuilder::Add(const uint8_t *vals_void, size_t count, size_t stride) {
  if (count > 1) {
    DCHECK_GE(stride, sizeof(uint32_t));
  }

  const uint8_t *vals = reinterpret_cast<const uint8_t *>(vals_void);

  int added = 0;
  while (estimated_raw_size_ < options_->block_size &&
         added < count) {
    uint32_t val = *reinterpret_cast<const uint32_t *>(vals);
    vals += stride;
    estimated_raw_size_ += CalcRequiredBytes32(val);
    ints_.push_back(val);
    added++;
  }

  return added;
}

uint64_t GVIntBlockBuilder::EstimateEncodedSize() const {
  // TODO: this currently does not do a good job of estimating
  // when the ints are large but clustered together,
  // since it doesn't take into account the delta coding relative
  // to the min int. We could track the min int along the way
  // but then we have extra branches in the add loop. Come back to this,
  // probably the branches don't matter since this is write-side.
  return estimated_raw_size_ + (ints_.size() + 3) / 4
    + kEstimatedHeaderSizeBytes + kTrailerExtraPaddingBytes;
}

size_t GVIntBlockBuilder::Count() const {
  return ints_.size();
}

Status GVIntBlockBuilder::GetFirstKey(void *key) const {
  if (ints_.empty()) {
    return Status::NotFound("no keys in data block");
  }

  *reinterpret_cast<uint32_t *>(key) = ints_[0];
  return Status::OK();
}

Slice GVIntBlockBuilder::Finish(uint32_t ordinal_pos) {
  int size_estimate = EstimateEncodedSize();
  buffer_.reserve(size_estimate);
  // TODO: negatives and big ints

  IntType min = 0;
  size_t size = ints_.size();

  if (size > 0) {
    min = *std::min_element(ints_.begin(), ints_.end());
  }

  buffer_.clear();
  AppendGroupVarInt32(&buffer_,
                      (uint32_t)size, (uint32_t)min,
                      (uint32_t)ordinal_pos, 0);

  AppendGroupVarInt32Sequence(&buffer_, min, &ints_[0], size);

  // Our estimate should always be an upper bound, or else there's a bunch of
  // extra copies due to resizes here.
  DCHECK_GE(size_estimate, buffer_.size());

  return Slice(buffer_.data(), buffer_.size());
}

////////////////////////////////////////////////////////////
// Decoder
////////////////////////////////////////////////////////////


GVIntBlockDecoder::GVIntBlockDecoder(const Slice &slice) :
  data_(slice),
  parsed_(false)
{
}


Status GVIntBlockDecoder::ParseHeader() {
  // TODO: better range check
  CHECK(data_.size() > 5);

  uint32_t unused;
  ints_start_ = DecodeGroupVarInt32(
    (const uint8_t *)data_.data(), &num_elems_, &min_elem_,
    &ordinal_pos_base_, &unused);

  if (num_elems_ <= 0 ||
      num_elems_ * 5 / 4 > data_.size()) {
    return Status::Corruption("bad number of elems in int block");
  }

  parsed_ = true;
  SeekToStart();

  return Status::OK();
}

class NullSink {
public:
  template <typename T>
  void push_back(T t) {}
};

template<typename T>
class PtrSinkWithStride {
public:
  PtrSinkWithStride(uint8_t *ptr, size_t stride) :
    ptr_(ptr),
    stride_(stride)
  {}

  void push_back(const T &t) {
    *reinterpret_cast<T *>(ptr_) = t;
    ptr_ += stride_;
  }

private:
  uint8_t *ptr_;
  const size_t stride_;
};

void GVIntBlockDecoder::SeekToPositionInBlock(uint pos) {
  CHECK(parsed_) << "Must call ParseHeader()";

  // Reset to start of block
  cur_pos_ = ints_start_;
  cur_idx_ = 0;
  pending_.clear();

  NullSink null;
  // TODO: should this return Status?
  size_t n = pos;
  CHECK_OK(DoGetNextValues(&n, &null));
}

Status GVIntBlockDecoder::SeekAtOrAfterValue(const void *value_void,
                                           bool *exact_match) {
  // for now, use a linear search.
  // TODO: evaluate adding a few pointers at the end of the block back
  // into every 16th group or so, or skipping by just looking at the
  // selector bytes
  SeekToPositionInBlock(0);

  // If it's at or below the first element, stop here.
  uint32_t target = *reinterpret_cast<const uint32_t *>(value_void);

  // Otherwise advance until we find an element >=
  while (cur_idx_ < num_elems_) {
    uint32_t chunk[4];
    PtrSinkWithStride<uint32_t> sink(reinterpret_cast<uint8_t *>(chunk),
                                     sizeof(uint32_t));
    size_t count = 4;
    RETURN_NOT_OK( DoGetNextValues(&count, &sink) );

    uint32_t *chunk_ptr = chunk;
    while (count) {
      if (*chunk_ptr >= target) {
        *exact_match = *chunk_ptr == target;
        // the DoGetNextValues call advanced cur_idx past all 4 values
        // that we read. So, we need to subtract back to point at
        // the matching value.
        cur_idx_ -= count;
        return Status::OK();
      }
      count--;
      chunk_ptr++;
    }
  }

  return Status::NotFound("not in block");
}

Status GVIntBlockDecoder::CopyNextValues(size_t *n, ColumnBlock *dst) {
  DCHECK_EQ(dst->type_info().type(), UINT32);

  PtrSinkWithStride<uint32_t> sink(
    reinterpret_cast<uint8_t *>(dst->data()), dst->stride());
  return DoGetNextValues(n, &sink);
}

template<class IntSink>
Status GVIntBlockDecoder::DoGetNextValues(size_t *n_param, IntSink *sink) {
  size_t n = *n_param;
  int start_idx = cur_idx_;
  size_t rem = num_elems_ - cur_idx_;
  assert(rem >= 0);

  // Only fetch up to remaining amount
  n = std::min(rem, n);

  __m128i min_elem_xmm = (__m128i)_mm_set_ps(
    *reinterpret_cast<float *>(&min_elem_),
    *reinterpret_cast<float *>(&min_elem_),
    *reinterpret_cast<float *>(&min_elem_),
    *reinterpret_cast<float *>(&min_elem_));

  // First drain pending_
  while (n > 0 && !pending_.empty()) {
    sink->push_back(pending_.back());
    pending_.pop_back();
    n--;
    cur_idx_++;
  }
  if (n == 0) goto ret;

  // Now grab groups of 4 and append to vector
  while (n >= 4) {
    uint32_t ints[4];
    cur_pos_ = DecodeGroupVarInt32_SSE_Add(
      cur_pos_, ints, min_elem_xmm);
    cur_idx_ += 4;

    sink->push_back(ints[0]);
    sink->push_back(ints[1]);
    sink->push_back(ints[2]);
    sink->push_back(ints[3]);
    n -= 4;
  }

  if (n == 0) goto ret;

  // Grab next batch into pending_
  // Note that this does _not_ increment cur_idx_
  uint32_t ints[4];
  cur_pos_ = DecodeGroupVarInt32_SSE_Add(
    cur_pos_, ints, min_elem_xmm);
  // pending_ acts like a stack, so push in reverse order.
  pending_.push_back(ints[3]);
  pending_.push_back(ints[2]);
  pending_.push_back(ints[1]);
  pending_.push_back(ints[0]);

  while (n > 0 && !pending_.empty()) {
    sink->push_back(pending_.back());
    pending_.pop_back();
    n--;
    cur_idx_++;
  }

  ret:
  CHECK_EQ(n, 0);
  *n_param = cur_idx_ - start_idx;
  return Status::OK();
}

} // namespace cfile
} // namespace kudu
