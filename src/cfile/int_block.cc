// Copyright (c) 2012, Cloudera, inc.

#include <algorithm>
#include <boost/utility/binary.hpp>
#include <stdint.h>

#include "int_block.h"

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

void IntBlockBuilder::Add(IntType val) {
  ints_.push_back(val);
  estimated_raw_size_ += CalcRequiredBytes32(val);
}

uint64_t IntBlockBuilder::EstimateEncodedSize() const {
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

Slice IntBlockBuilder::Finish() {
  // TODO: negatives and big ints

  IntType min = 0;
  size_t size = ints_.size();

  if (size > 0) {
    min = *std::min_element(ints_.begin(), ints_.end());
  }

  buffer_.clear();
  AppendGroupVarInt32(&buffer_,
                      (uint32_t)size, (uint32_t)min, 0, 0);

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
    p = &trailer[0];
    while (size > 0) {
      *trailer_p++ = *p++ - min;
      size--;
    }

    AppendGroupVarInt32(&buffer_, trailer[0], trailer[1], trailer[2], trailer[3]);
  }
  return Slice(buffer_);
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

} // namespace cfile
} // namespace kudu
