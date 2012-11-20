// Copyright (c) 2012, Cloudera, inc.

#include <algorithm>
#include <boost/foreach.hpp>
#include <boost/utility/binary.hpp>
#include <glog/logging.h>
#include <stdint.h>

#include "gutil/once.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/stringpiece.h"
#include "gutil/stringprintf.h"
#include "gutil/stl_util.h"
#include "util/coding.h"
#include "util/coding-inl.h"
#include "util/faststring.h"
#include "util/hexdump.h"
#include "util/memory/arena.h"

#include "block_encodings.h"
// TODO: including this to just get block size from options,
// maybe we should not take a whole options struct
#include "cfile.h"
#include <smmintrin.h>


namespace kudu {
namespace cfile {

using kudu::Arena;


static bool SSE_TABLE_INITTED = false;
static char SSE_TABLE[256 * 16] __attribute__ ((aligned(16)));
static char VARINT_SELECTOR_LENGTHS[256];

const static uint32_t MASKS[4] = { 0xff, 0xffff, 0xffffff, 0xffffffff };

__attribute__((constructor))
static void InitializeSSETables() {
  memset(SSE_TABLE, 0xff, sizeof(SSE_TABLE));

  for (int i = 0; i < 256; i++) {
    uint32_t *entry = reinterpret_cast<uint32_t *>(&SSE_TABLE[i * 16]);

    uint8_t selectors[] = {
      static_cast<uint8_t>((i & BOOST_BINARY( 11 00 00 00)) >> 6),
      static_cast<uint8_t>((i & BOOST_BINARY( 00 11 00 00)) >> 4),
      static_cast<uint8_t>((i & BOOST_BINARY( 00 00 11 00)) >> 2),
      static_cast<uint8_t>((i & BOOST_BINARY( 00 00 00 11 ))) };

    // 00000000 ->
    // 00 ff ff ff  01 ff ff ff  02 ff ff ff  03 ff ff ff

    // 01000100 ->
    // 00 01 ff ff  02 ff ff ff  03 04 ff ff  05 ff ff ff

    uint8_t offset = 0;

    for (int j = 0; j < 4; j++) {
      uint8_t num_bytes = selectors[j] + 1;
      uint8_t *entry_bytes = reinterpret_cast<uint8_t *>(&entry[j]);

      for (int k = 0; k < num_bytes; k++) {
        *entry_bytes++ = offset++;
      }
    }

    VARINT_SELECTOR_LENGTHS[i] = offset;
  }

  SSE_TABLE_INITTED = true;
}

void DumpSSETable() {
  LOG(INFO) << "SSE table:\n"
            << kudu::HexDump(Slice(SSE_TABLE, sizeof(SSE_TABLE)));
}


////////////////////////////////////////////////////////////
// Utility code used by both encoding and decoding
////////////////////////////////////////////////////////////

static const char *DecodeEntryLengths(
  const char *ptr, const char *limit,
  uint32_t *shared, uint32_t *non_shared) {

  if ((ptr = GetVarint32Ptr(ptr, limit, shared)) == NULL) return NULL;
  if ((ptr = GetVarint32Ptr(ptr, limit, non_shared)) == NULL) return NULL;
  if (limit - ptr < *non_shared) {
    return NULL;
  }

  return ptr;
}

// Calculate the number of bytes to encode the given unsigned int.
static size_t CalcRequiredBytes32(uint32_t i) {
  if (i == 0) return 1;

  return sizeof(long) - __builtin_clzl(i)/8;
}

const uint8_t *DecodeGroupVarInt32(
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

const uint8_t *DecodeGroupVarInt32_SSE(
  const uint8_t *src,
  uint32_t *a, uint32_t *b, uint32_t *c, uint32_t *d) {

  DCHECK(SSE_TABLE_INITTED);

  uint8_t sel_byte = *src++;
  __m128i shuffle_mask = _mm_load_si128(
    reinterpret_cast<__m128i *>(&SSE_TABLE[sel_byte * 16]));
  __m128i data = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

  __m128i results = _mm_shuffle_epi8(data, shuffle_mask);


  // It would look like the following would be most efficient,
  // since it turns into a single movdqa instruction:
  //   *reinterpret_cast<__m128i *>(ret) = results;
  // (where ret is an aligned array of ints, which the user must pass)
  // but it is actually slower than the below alternatives by a
  // good amount -- even though these result in more instructions.
  *a = _mm_extract_ps((__v4sf)results, 0);
  *b = _mm_extract_ps((__v4sf)results, 1);
  *c = _mm_extract_ps((__v4sf)results, 2);
  *d = _mm_extract_ps((__v4sf)results, 3);

  // _mm_extract_ps turns into extractps, which is slightly faster
  // than _mm_extract_epi32 (which turns into pextrd)
  // Apparently pextrd involves one more micro-op
  // than extractps
  /*
  *a = _mm_extract_epi32(results, 0);
  *b = _mm_extract_epi32(results, 1);
  *c = _mm_extract_epi32(results, 2);
  *d = _mm_extract_epi32(results, 3);
  */
  src += VARINT_SELECTOR_LENGTHS[sel_byte];
  return src;
}

const uint8_t *DecodeGroupVarInt32_SSE_Add(
  const uint8_t *src,
  uint32_t *ret,
  __m128i add) {

  DCHECK(SSE_TABLE_INITTED);

  uint8_t sel_byte = *src++;
  __m128i shuffle_mask = _mm_load_si128(
    reinterpret_cast<__m128i *>(&SSE_TABLE[sel_byte * 16]));
  __m128i data = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

  __m128i decoded_deltas = _mm_shuffle_epi8(data, shuffle_mask);
  __m128i results = _mm_add_epi32(decoded_deltas, add);

  ret[0] = _mm_extract_ps((__v4sf)results, 0);
  ret[1] = _mm_extract_ps((__v4sf)results, 1);
  ret[2] = _mm_extract_ps((__v4sf)results, 2);
  ret[3] = _mm_extract_ps((__v4sf)results, 3);

  src += VARINT_SELECTOR_LENGTHS[sel_byte];
  return src;
}

void AppendShorterInt(faststring *s, uint32_t i, size_t bytes) {
  DCHECK_GE(bytes, 0);
  DCHECK_LE(bytes, 4);

#if __BYTE_ORDER == __LITTLE_ENDIAN
  // LSBs come first, so we can just reinterpret-cast
  // and set the right length
  s->append(reinterpret_cast<char *>(&i), bytes);
#else
#error dont support big endian currently
#endif
}

void AppendGroupVarInt32(
  faststring *s,
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




////////////////////////////////////////////////////////////
// Encoding
////////////////////////////////////////////////////////////


IntBlockBuilder::IntBlockBuilder(const WriterOptions *options) :
  estimated_raw_size_(0),
  options_(options)
{}


void IntBlockBuilder::Reset() {
  ints_.clear();
  buffer_.clear();
  estimated_raw_size_ = 0;
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

Status IntBlockBuilder::GetFirstKey(void *key) const {
  if (ints_.empty()) {
    return Status::NotFound("no keys in data block");
  }

  *reinterpret_cast<uint32_t *>(key) = ints_[0];
  return Status::OK();
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
  return Slice(buffer_.data(), buffer_.size());
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
  Reset();
}

void StringBlockBuilder::Reset() {
  finished_ = false;
  val_count_ = 0;
  vals_since_restart_ = 0;

  buffer_.clear();
  buffer_.resize(kHeaderReservedLength);

  restarts_.clear();
  last_val_.clear();
}

Slice StringBlockBuilder::Finish(uint32_t ordinal_pos) {
  CHECK(!finished_) << "already finished";
  DCHECK_GE(buffer_.size(), kHeaderReservedLength);

  faststring header(kHeaderReservedLength);

  AppendGroupVarInt32(&header, val_count_, ordinal_pos,
                      options_->block_restart_interval, 0);

  int header_encoded_len = header.size();

  // Copy the header into the buffer at the right spot.
  // Since the header is likely shorter than the amount of space
  // reserved for it, need to find where it fits:
  int header_offset = kHeaderReservedLength - header_encoded_len;
  DCHECK_GE(header_offset, 0);
  char *header_dst = buffer_.data() + header_offset;
  strings::memcpy_inlined(header_dst, header.data(), header_encoded_len);

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
    InlinePutFixed32(&buffer_, relative_to_block);
  }
  InlinePutFixed32(&buffer_, restarts_.size());

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
  InlinePutVarint32(&buffer_, shared);
  InlinePutVarint32(&buffer_, non_shared);

  // Add string delta to buffer_
  buffer_.append(val.data() + shared, non_shared);

  // Update state
  last_val_.resize(shared);
  last_val_.append(val.data() + shared, non_shared);
  DCHECK(Slice(last_val_) == val);
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

Status StringBlockBuilder::GetFirstKey(void *key) const {
  if (val_count_ == 0) {
    return Status::NotFound("no keys in data block");
  }

  const char *p = &buffer_[kHeaderReservedLength];
  uint32_t shared, non_shared;
  p = DecodeEntryLengths(p, &buffer_[buffer_.size()], &shared, &non_shared);
  if (p == NULL) {
    return Status::Corruption("Could not decode first entry in string block");
  }

  CHECK(shared == 0) << "first entry in string block had a non-zero 'shared': "
                     << shared;

  *reinterpret_cast<Slice *>(key) = Slice(p, non_shared);
  return Status::OK();
}


////////////////////////////////////////////////////////////
// Decoding
////////////////////////////////////////////////////////////

IntBlockDecoder::IntBlockDecoder(const Slice &slice) :
  data_(slice),
  parsed_(false)
{
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
class PtrSinkWithStride {
public:
  PtrSinkWithStride(char *ptr, size_t stride) :
    ptr_(ptr),
    stride_(stride)
  {}

  void push_back(const T &t) {
    *reinterpret_cast<T *>(ptr_) = t;
    ptr_ += stride_;
  }

private:
  char *ptr_;
  const size_t stride_;
};

void IntBlockDecoder::SeekToPositionInBlock(uint pos) {
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

Status IntBlockDecoder::SeekAtOrAfterValue(const void *value_void) {
  return Status::NotSupported("TODO: int key search");
}

Status IntBlockDecoder::CopyNextValues(size_t *n, void *out,
                                       size_t stride, Arena *out_arena) {
  CHECK_GE(stride, sizeof(uint32_t));

  PtrSinkWithStride<uint32_t> sink(reinterpret_cast<char *>(out), stride);
  return DoGetNextValues(n, &sink);
}

template<class IntSink>
Status IntBlockDecoder::DoGetNextValues(size_t *n_param, IntSink *sink) {
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

////////////////////////////////////////////////////////////
// StringBlockDecoder
////////////////////////////////////////////////////////////

StringBlockDecoder::StringBlockDecoder(const Slice &slice) :
  data_(slice),
  parsed_(false),
  num_elems_(0),
  ordinal_pos_base_(0),
  num_restarts_(0),
  restarts_(NULL),
  data_start_(0),
  cur_idx_(0),
  next_ptr_(NULL)
{
}

Status StringBlockDecoder::ParseHeader() {
  // First parse the actual header.
  Slice header(data_);

  uint32_t unused;
  data_start_ = reinterpret_cast<const char *>(
    DecodeGroupVarInt32_SSE(
      reinterpret_cast<const uint8_t *>(data_.data()),
      &num_elems_, &ordinal_pos_base_,
      &restart_interval_, &unused));
  if (data_start_ == NULL) {
    return Status::Corruption("couldnt parse string block header");
    // TODO include hexdump
  }

  // Then the footer, which points us to the restarts array
  num_restarts_ = DecodeFixed32(
    data_.data() + data_.size() - sizeof(uint32_t));

  // sanity check the restarts size
  uint32_t restarts_size = num_restarts_ * sizeof(uint32_t);
  if (restarts_size > data_.size()) {
    return Status::Corruption(
      StringPrintf("restart count %d too big to fit in block size %d",
                   num_restarts_, (int)data_.size()));
  }

  // TODO: check relationship between num_elems, num_restarts_,
  // and restart_interval_

  restarts_ = reinterpret_cast<const uint32_t *>(
    data_.data() + data_.size()
    - sizeof(uint32_t) // rewind before the restart length
    - restarts_size);

  SeekToStart();
  parsed_ = true;
  return Status::OK();
}

void StringBlockDecoder::SeekToStart() {
  SeekToRestartPoint(0);
}

void StringBlockDecoder::SeekToPositionInBlock(uint pos) {
  DCHECK_LT(pos, num_elems_);

  int target_restart = pos/restart_interval_;
  SeekToRestartPoint(target_restart);

  // Seek forward to the right index

  // TODO: Seek calls should return a Status
  CHECK(SkipForward(pos - cur_idx_).ok());
  DCHECK_EQ(cur_idx_, pos);
}

// Get the pointer to the entry corresponding to the given restart
// point. Note that the restart points in the file do not include
// the '0' restart point, since that is simply the beginning of
// the data and hence a waste of space. So, 'idx' may range from
// 0 (first record) through num_restarts_ (last recorded restart point)
const char * StringBlockDecoder::GetRestartPoint(uint32_t idx) const {
  DCHECK_LE(idx, num_restarts_);

  if (PREDICT_TRUE(idx > 0)) {
    return data_.data() + restarts_[idx - 1];
  } else {
    return data_start_;
  }
}

// Note: see GetRestartPoint() for 'idx' semantics
void StringBlockDecoder::SeekToRestartPoint(uint32_t idx) {
  next_ptr_ = GetRestartPoint(idx);
  cur_idx_ = idx * restart_interval_;
  ParseNextValue();
}

Status StringBlockDecoder::SeekAtOrAfterValue(const void *value_void) {
  DCHECK(value_void != NULL);

  const Slice &target = *reinterpret_cast<const Slice *>(value_void);

  // Binary search in restart array to find the first restart point
  // with a key >= target
  int32_t left = 0;
  int32_t right = num_restarts_;
  while (left < right) {
    uint32_t mid = (left + right + 1) / 2;
    const char *entry = GetRestartPoint(mid);
    uint32_t shared, non_shared;
    const char *key_ptr = DecodeEntryLengths(entry, &shared, &non_shared);
    if (key_ptr == NULL || (shared != 0)) {
      string err =
        StringPrintf( "bad entry restart=%d shared=%d\n", mid, shared) +
        HexDump(Slice(entry, 16));
      return Status::Corruption(err);
    }
    Slice mid_key(key_ptr, non_shared);
    if (mid_key.compare(target) < 0) {
      // Key at "mid" is smaller than "target".  Therefore all
      // blocks before "mid" are uninteresting.
      left = mid;
    } else {
      // Key at "mid" is >= "target".  Therefore all blocks at or
      // after "mid" are uninteresting.
      right = mid - 1;
    }
  }

  // Linear search (within restart block) for first key >= target
  SeekToRestartPoint(left);

  while (true) {
#ifndef NDEBUG
    VLOG(3) << "loop iter:\n"
            << "cur_idx = " << cur_idx_ << "\n"
            << "target  =" << target.ToString() << "\n"
            << "cur_val_=" << Slice(cur_val_).ToString();
#endif
    if (Slice(cur_val_).compare(target) >= 0) {
      return Status::OK();
    }
    RETURN_NOT_OK(ParseNextValue());
    cur_idx_++;
  }
}

Status StringBlockDecoder::CopyNextValues(size_t *n, void *out_void,
                                          size_t stride, Arena *out_arena) {
  DCHECK(parsed_);
  CHECK_GE(stride, sizeof(Slice));

  char *out = reinterpret_cast<char *>(out_void);

  size_t i = 0;
  for (i = 0; i < *n && cur_idx_ < num_elems_; i++) {
    // Copy the value into the output arena.
    const char *out_data = out_arena->AddStringPieceContent(
      StringPiece(cur_val_.data(), cur_val_.size()));
    if (PREDICT_FALSE(out_data == NULL)) {
      return Status::IOError(
        "Out of memory",
        StringPrintf("Failed to allocate %d bytes in output arena",
                     (int)cur_val_.size()));
    }

    // Put a slice to it in the output array
    *reinterpret_cast<Slice *>(out) = Slice(out_data, cur_val_.size());
    out += stride;

    if (cur_idx_ + 1 < num_elems_) {
      // TODO: Can ParseNextValue take a pointer to the previously
      // parsed value, to avoid having to double-copy?
      RETURN_NOT_OK(ParseNextValue());
    } else {
      // end of block -- postcondition: next_ptr_ NULL
      // since there are no further entries
      next_ptr_ = NULL;
    }
    cur_idx_++;
  }

  *n = i;
  return Status::OK();
}

// Decode the lengths pointed to by 'ptr', doing bounds checking.
//
// Returns a pointer to where the value itself starts.
// Returns NULL if the varints themselves, or the value that
// they prefix extend past the end of the block data.
const char *StringBlockDecoder::DecodeEntryLengths(
  const char *ptr, uint32_t *shared, uint32_t *non_shared) const {

  // data ends where the restart info begins
  const char *limit = reinterpret_cast<const char *>(restarts_);
  return kudu::cfile::DecodeEntryLengths(ptr, limit, shared, non_shared);
}

Status StringBlockDecoder::SkipForward(int n) {
  DCHECK_LT(cur_idx_ + n, num_elems_) <<
    "skip(" << n << ") curidx=" << cur_idx_
            << " num_elems=" << num_elems_;
  // Probably a faster way to implement this using restarts,
  for (int i = 0; i < n; i++) {
    RETURN_NOT_OK(ParseNextValue());
    cur_idx_++;
  }
  return Status::OK();
}

// Parses the data pointed to by next_ptr_ and stores it in cur_val_
// Advances next_ptr_ to point to the following values.
// Does not modify cur_idx_
Status StringBlockDecoder::ParseNextValue() {
  DCHECK(next_ptr_ != NULL);

  if (next_ptr_ == reinterpret_cast<const char *>(restarts_)) {
    DCHECK_EQ(cur_idx_, num_elems_ - 1);
    return Status::NotFound("Trying to parse past end of array");
  }

  uint32_t shared, non_shared;
  const char *val_delta = DecodeEntryLengths(next_ptr_, &shared, &non_shared);
  if (val_delta == NULL) {
    return Status::Corruption(
      StringPrintf("Could not decode value length data at idx %d",
                   cur_idx_));
  }

  // Chop the current key to the length that is shared with the next
  // key, then append the delta portion.
  DCHECK_LE(shared, cur_val_.size())
    << "Specified longer shared amount than previous key length";

  cur_val_.resize(shared);
  cur_val_.append(val_delta, non_shared);

  DCHECK_EQ(cur_val_.size(), shared + non_shared);

  next_ptr_ = val_delta + non_shared;
  return Status::OK();
}

} // namespace cfile
} // namespace kudu

