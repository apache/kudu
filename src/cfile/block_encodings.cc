// Copyright (c) 2012, Cloudera, inc.

#include <algorithm>
#include <boost/foreach.hpp>
#include <boost/utility/binary.hpp>
#include <glog/logging.h>
#include <stdint.h>

#include "gutil/strings/fastmem.h"
#include "gutil/stl_util.h"
#include "util/coding.h"

#include "block_encodings.h"
// TODO: including this to just get block size from options,
// maybe we should not take a whole options struct
#include "cfile.h"

namespace kudu {
namespace cfile {



////////////////////////////////////////////////////////////
// Encoding
////////////////////////////////////////////////////////////


IntBlockBuilder::IntBlockBuilder(const WriterOptions *options) :
  estimated_raw_size_(0),
  options_(options)
{}

void IntBlockBuilder::AppendShorterInt(
  std::string *s, uint32_t i, size_t bytes) {

  assert(bytes > 0 && bytes <= 4);

#if __BYTE_ORDER == __LITTLE_ENDIAN
  // LSBs come first, so we can just reinterpret-cast
  // and set the right length
  s->append(reinterpret_cast<char *>(&i), bytes);
#else
#error dont support big endian currently
#endif
}

void IntBlockBuilder::Reset() {
  ints_.clear();
  buffer_.clear();
  estimated_raw_size_ = 0;
}

// Calculate the number of bytes to encode the given unsigned int.
static size_t CalcRequiredBytes32(uint32_t i) {
  if (i == 0) return 1;

  return sizeof(long) - __builtin_clzl(i)/8;
}

int IntBlockBuilder::Add(const void *vals_void, int count) {
  const uint32_t *vals = reinterpret_cast<const uint32_t *>(vals_void);

  int added = 0;
  while (estimated_raw_size_ < options_->block_size &&
         added < count) {
    uint32_t val = *vals++;
    estimated_raw_size_ += CalcRequiredBytes32(val);
    ints_.push_back(val);
    added++;
  }

  return added;
}

uint64_t IntBlockBuilder::EstimateEncodedSize() const {
  // TODO: this currently does not do a good job of estimating
  // when the ints are large but clustered together,
  // since it doesn't take into account the delta coding relative
  // to the min int. We could track the min int along the way
  // but then we have extra branches in the add loop. Come back to this,
  // probably the branches don't matter since this is write-side.
  return estimated_raw_size_ + ints_.size() / 4
    + kEstimatedHeaderSizeBytes;
}

size_t IntBlockBuilder::Count() const {
  return ints_.size();
}

void IntBlockBuilder::AppendGroupVarInt32(
  std::string *s,
  uint32_t a, uint32_t b, uint32_t c, uint32_t d) {

  uint8_t a_req = CalcRequiredBytes32(a);
  uint8_t b_req = CalcRequiredBytes32(b);
  uint8_t c_req = CalcRequiredBytes32(c);
  uint8_t d_req = CalcRequiredBytes32(d);

  uint8_t prefix_byte =
    ((a_req - 1) << 6) |
    ((b_req - 1) << 4) |
    ((c_req - 1) << 2) |
    (d_req - 1);

  s->push_back(prefix_byte);
  AppendShorterInt(s, a, a_req);
  AppendShorterInt(s, b, b_req);
  AppendShorterInt(s, c, c_req);
  AppendShorterInt(s, d, d_req);
}

Slice IntBlockBuilder::Finish(uint32_t ordinal_pos) {
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

  IntType *p = &ints_[0];
  while (size >= 4) {
    AppendGroupVarInt32(
      &buffer_,
      p[0] - min, p[1] - min, p[2] - min, p[3] - min);
    size -= 4;
    p += 4;
  }


  IntType trailer[4] = {0, 0, 0, 0};
  IntType *trailer_p = &trailer[0];

  if (size > 0) {
    while (size > 0) {
      *trailer_p++ = *p++ - min;
      size--;
    }

    AppendGroupVarInt32(&buffer_, trailer[0], trailer[1], trailer[2], trailer[3]);
  }
  return Slice(buffer_);
}



////////////////////////////////////////////////////////////
// StringBlockBuilder encoding
////////////////////////////////////////////////////////////

StringBlockBuilder::StringBlockBuilder(const WriterOptions *options) :
  val_count_(0),
  vals_since_restart_(0),
  finished_(false),
  options_(options)
{
  // TODO: move below to method
  STLStringResizeUninitialized(&buffer_, kHeaderReservedLength);
}

void StringBlockBuilder::Reset() {
  finished_ = false;
  val_count_ = 0;
  vals_since_restart_ = 0;

  buffer_.clear();
  STLStringResizeUninitialized(&buffer_, kHeaderReservedLength);

  last_val_.clear();
}

Slice StringBlockBuilder::Finish(uint32_t ordinal_pos) {
  CHECK(!finished_) << "already finished";
  DCHECK_GE(buffer_.size(), kHeaderReservedLength);

  char header_space[kHeaderReservedLength];
  char *header_end = header_space;

  header_end = EncodeVarint32(header_end, val_count_);
  header_end = EncodeVarint32(header_end, ordinal_pos);

  int header_encoded_len = header_end - header_space;

  // Copy the header into the buffer at the right spot.
  // Since the header is likely shorter than the amount of space
  // reserved for it, need to find where it fits:
  int header_offset = kHeaderReservedLength - header_encoded_len;
  DCHECK_GE(header_offset, 0);
  char *header_dst = string_as_array(&buffer_) + header_offset;
  strings::memcpy_inlined(header_dst, header_space, header_encoded_len);

  // Serialize the restart points.
  // Note that the values stored in restarts_ are relative to the
  // start of the *buffer*, which is not the same as the start of
  // the block. So, we must subtract the header offset from each.
  buffer_.reserve(buffer_.size()
                  + restarts_.size() * sizeof(uint32_t) // the data
                  + sizeof(uint32_t)); // the restart count);
  BOOST_FOREACH(uint32_t restart, restarts_) {
    DCHECK_GE((int)restart, header_offset);
    uint32_t relative_to_block = restart - header_offset;
    VLOG(2) << "appending restart " << relative_to_block;
    PutFixed32(&buffer_, relative_to_block);
  }
  PutFixed32(&buffer_, restarts_.size());

  finished_ = true;
  return Slice(&buffer_[header_offset], buffer_.size() - header_offset);
}

int StringBlockBuilder::Add(const void *vals_void, int count) {
  DCHECK_GT(count, 0);
  DCHECK(!finished_);
  DCHECK_LE(vals_since_restart_, options_->block_restart_interval);

  const Slice &val = *reinterpret_cast<const Slice *>(vals_void);

  Slice last_val_piece(last_val_);
  size_t shared = 0;
  if (vals_since_restart_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_val_piece.size(), val.size());
    while ((shared < min_length) && (last_val_piece[shared] == val[shared])) {
      shared++;
    }
  } else {
    // Restart compression
    restarts_.push_back(buffer_.size());
    vals_since_restart_ = 0;
  }
  const size_t non_shared = val.size() - shared;

  // Add "<shared><non_shared>" to buffer_
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);

  // Add string delta to buffer_
  STLAppendToString(&buffer_, val.data() + shared, non_shared);

  // Update state
  STLStringResizeUninitialized(&last_val_, shared);
  STLAppendToString(&last_val_, val.data() + shared, non_shared);
  assert(Slice(last_val_) == val);
  vals_since_restart_++;
  val_count_++;

  return 1;
}

size_t StringBlockBuilder::Count() const {
  return val_count_;
}

uint64_t StringBlockBuilder::EstimateEncodedSize() const {
  // TODO: add restarts size
  return buffer_.size();
}



////////////////////////////////////////////////////////////
// Decoding
////////////////////////////////////////////////////////////


const static uint32_t MASKS[4] = { 0xff, 0xffff, 0xffffff, 0xffffffff };

const uint8_t *IntBlockDecoder::DecodeGroupVarInt32(
  const uint8_t *src,
  uint32_t *a, uint32_t *b, uint32_t *c, uint32_t *d) {

  uint8_t a_sel = (*src & BOOST_BINARY( 11 00 00 00)) >> 6;
  uint8_t b_sel = (*src & BOOST_BINARY( 00 11 00 00)) >> 4;
  uint8_t c_sel = (*src & BOOST_BINARY( 00 00 11 00)) >> 2;
  uint8_t d_sel = (*src & BOOST_BINARY( 00 00 00 11 ));

  src++; // skip past selector byte

  *a = *reinterpret_cast<const uint32_t *>(src) & MASKS[a_sel];
  src += a_sel + 1;

  *b = *reinterpret_cast<const uint32_t *>(src) & MASKS[b_sel];
  src += b_sel + 1;

  *c = *reinterpret_cast<const uint32_t *>(src) & MASKS[c_sel];
  src += c_sel + 1;

  *d = *reinterpret_cast<const uint32_t *>(src) & MASKS[d_sel];
  src += d_sel + 1;

  return src;
}


Status IntBlockDecoder::ParseHeader() {
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
class PtrSink {
public:
  PtrSink(T *ptr) :
    ptr_(ptr)
  {}

  void push_back(const T &t) {
    *ptr_++ = t;
  }

private:
  T *ptr_;
};

void IntBlockDecoder::SeekToPositionInBlock(int pos) {
  CHECK(parsed_) << "Must call ParseHeader()";

  // Reset to start of block
  cur_pos_ = ints_start_;
  cur_idx_ = 0;
  pending_.clear();

  NullSink null;
  DoGetNextValues(pos, &null);
}

int IntBlockDecoder::GetNextValues(int n, void *out) {
  PtrSink<uint32_t> sink(reinterpret_cast<uint32_t *>(out));
  return DoGetNextValues(n, &sink);
}

template<class IntSink>
int IntBlockDecoder::DoGetNextValues(int n, IntSink *sink) {
  int start_idx = cur_idx_;
  int rem = num_elems_ - cur_idx_;
  assert(rem >= 0);

  // Only fetch up to remaining amount
  n = std::min(rem, n);

  // TODO: vec->reserve(vec->size() + n);

  // First drain pending_
  while (n > 0 && !pending_.empty()) {
    sink->push_back(pending_.back());
    pending_.pop_back();
    n--;
    cur_idx_++;
  }
  if (n == 0) return cur_idx_ - start_idx;

  // Now grab groups of 4 and append to vector
  while (n >= 4) {
    uint32_t ints[4];
    cur_pos_ = DecodeGroupVarInt32(
      cur_pos_, &ints[0], &ints[1], &ints[2], &ints[3]);
    cur_idx_ += 4;

    sink->push_back(min_elem_ + ints[0]);
    sink->push_back(min_elem_ + ints[1]);
    sink->push_back(min_elem_ + ints[2]);
    sink->push_back(min_elem_ + ints[3]);

    n -= 4;
  }

  if (n == 0) return cur_idx_ - start_idx;

  // Grab next batch into pending_
  // Note that this does _not_ increment cur_idx_
  uint32_t ints[4];
  cur_pos_ = DecodeGroupVarInt32(
    cur_pos_, &ints[0], &ints[1], &ints[2], &ints[3]);
  // pending_ acts like a stack, so push in reverse order.
  pending_.push_back(min_elem_ + ints[3]);
  pending_.push_back(min_elem_ + ints[2]);
  pending_.push_back(min_elem_ + ints[1]);
  pending_.push_back(min_elem_ + ints[0]);

  while (n > 0 && !pending_.empty()) {
    sink->push_back(pending_.back());
    pending_.pop_back();
    n--;
    cur_idx_++;
  }

  CHECK_EQ(n, 0);
  return cur_idx_ - start_idx;
}





} // namespace cfile
} // namespace kudu
