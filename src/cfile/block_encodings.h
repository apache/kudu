// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_CFILE_BLOCK_ENCODINGS_H
#define KUDU_CFILE_BLOCK_ENCODINGS_H

#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <stdint.h>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "cfile/cfile.pb.h"
#include "cfile/seek_flags.h"
#include "common/columnblock.h"
#include "util/memory/arena.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {
namespace cfile {

struct WriterOptions;

using std::vector;
using std::string;

typedef uint32_t IntType;

// TODO: move to coding-inl.h?
extern const uint8_t *DecodeGroupVarInt32(
  const uint8_t *src,
  uint32_t *a, uint32_t *b, uint32_t *c, uint32_t *d);
extern const uint8_t *DecodeGroupVarInt32_SSE(
  const uint8_t *src,
  uint32_t *a, uint32_t *b, uint32_t *c, uint32_t *d);

extern void AppendShorterInt(faststring *s, uint32_t i, size_t bytes);

extern void AppendGroupVarInt32(
  faststring *s,
  uint32_t a, uint32_t b, uint32_t c, uint32_t d);
//


// Return the default encoding to use for the given data type.
// TODO: this probably won't stay around too long - in a real Flush
// situation, we can look at the content in the memstore and pick the
// most effective coding.
inline EncodingType GetDefaultEncoding(DataType type) {
  switch (type) {
    case STRING:
      return PREFIX;
    case UINT32:
      return GROUP_VARINT;
    default:
      CHECK(0) << "unknown type: " << type;
  }
}


class BlockBuilder : boost::noncopyable {
public:
  // TODO: add a more type-checkable wrapper for void *,
  // like ConstVariantPointer in Supersonic
  virtual int Add(const uint8_t *vals, size_t count, size_t stride) = 0;

  // Return a Slice which represents the encoded data.
  //
  // This Slice points to internal data of this class
  // and becomes invalid after the builder is destroyed
  // or after Finish() is called again.
  virtual Slice Finish(uint32_t ordinal_pos) = 0;

  // Reset the internal state of the encoder.
  //
  // Any data previously returned by Finish or by GetFirstKey
  // may be invalidated by this call.
  //
  // Postcondition: Count() == 0
  virtual void Reset() = 0;

  // Return an estimate of the number of bytes that this block
  // will require once encoded. This is not necessarily
  // an upper or lower bound.
  virtual uint64_t EstimateEncodedSize() const = 0;

  // Return the number of entries that have been added to the
  // block.
  virtual size_t Count() const = 0;

  // Return the key of the first entry in this index block.
  // For pointer-based types (such as strings), the pointed-to
  // data is only valid until the next call to Reset().
  //
  // If no keys have been added, returns Status::NotFound
  virtual Status GetFirstKey(void *key) const = 0;

  virtual ~BlockBuilder() {}
};


// Builder for an encoded block of ints.
// The encoding is group-varint plus frame-of-reference:
//
// Header group (gvint): <num_elements, min_element, [unused], [unused]
// followed by enough group varints to represent the total number of
// elements, including padding 0s at the end. Each element is a delta
// from the min_element frame-of-reference.
//
// See AppendGroupVarInt32(...) for details on the varint
// encoding.
class IntBlockBuilder : public BlockBuilder {
public:
  explicit IntBlockBuilder(const WriterOptions *options);

  int Add(const uint8_t *vals, size_t count, size_t stride);

  Slice Finish(uint32_t ordinal_pos);

  void Reset();

  uint64_t EstimateEncodedSize() const;

  size_t Count() const;

  // Return the first added key.
  // key should be a uint32_t *
  Status GetFirstKey(void *key) const;

private:
  friend class TestEncoding;
  FRIEND_TEST(TestEncoding, TestGroupVarInt);
  FRIEND_TEST(TestEncoding, TestIntBlockEncoder);

  vector<IntType> ints_;
  faststring buffer_;
  uint64_t estimated_raw_size_;

  const WriterOptions *options_;

  enum {
    kEstimatedHeaderSizeBytes = 10
  };
};


// Encoding for data blocks of strings.
// This encodes in a manner similar to LevelDB (prefix coding)
class StringBlockBuilder : public BlockBuilder {
public:
  explicit StringBlockBuilder(const WriterOptions *options);

  int Add(const uint8_t *vals, size_t count, size_t stride);

  // Return a Slice which represents the encoded data.
  //
  // This Slice points to internal data of this class
  // and becomes invalid after the builder is destroyed
  // or after Finish() is called again.
  Slice Finish(uint32_t ordinal_pos);

  void Reset();

  uint64_t EstimateEncodedSize() const;

  size_t Count() const;

  // Return the first added key.
  // key should be a Slice *
  Status GetFirstKey(void *key) const;

private:
  faststring buffer_;
  faststring last_val_;

  // Restart points, offsets relative to start of block
  vector<uint32_t> restarts_;

  int val_count_;
  int vals_since_restart_;
  bool finished_;

  const WriterOptions *options_;

  // Maximum length of a header.
  // We leave this much space at the start of the buffer before
  // accumulating any data, so we can later fill in the variable-length
  // header.
  // Currently two varints, so max 10 bytes
  static const size_t kHeaderReservedLength = 10;
};

////////////////////////////////////////////////////////////
// Decoding
////////////////////////////////////////////////////////////

class BlockDecoder : boost::noncopyable {
public:
  virtual Status ParseHeader() = 0;

  // Seek the decoder to the given positional index of the block.
  // For example, SeekToPositionInBlock(0) seeks to the first
  // stored entry.
  //
  // It is an error to call this with a value larger than Count().
  // Doing so has undefined results.
  //
  // TODO: Since we know the actual file position, maybe we
  // should just take the actual ordinal in the file
  // instead of the position in the block?
  virtual void SeekToPositionInBlock(uint pos) = 0;

  // Seek the decoder to the given value in the block, or the
  // lowest value which is greater than the given value.
  //
  // If the decoder was able to locate an exact match, then
  // sets *exact_match to true. Otherwise sets *exact_match to
  // false, to indicate that the seeked value is _after_ the
  // requested value.
  //
  // If the given value is less than the lowest value in the block,
  // seeks to the start of the block. If it is higher than the highest
  // value in the block, then returns Status::NotFound
  //
  // This will only return valid results when the data block
  // consists of values in sorted order.
  virtual Status SeekAtOrAfterValue(const void *value,
                                    bool *exact_match,
                                    SeekFlags flags = 0) = 0;

  // Fetch the next set of values from the block into 'dst'.
  // The output block must have space for up to n cells.
  //
  // Modifies *n to contain the number of values fetched.
  //
  // In the case that the values are themselves references
  // to other memory (eg Slices), the referred-to memory is
  // allocated in the dst block's arena.
  virtual Status CopyNextValues(size_t *n, ColumnBlock *dst) = 0;

  // Return true if there are more values remaining to be iterated.
  // (i.e that the next call to CopyNextValues will return at least 1
  // element)
  // TODO: change this to a Remaining() call?
  virtual bool HasNext() const = 0;

  // Return the number of elements in this block.
  virtual size_t Count() const = 0;

  // Return the ordinal position in the file of the currently seeked
  // entry (ie the entry that will next be returned by CopyNextValues())
  virtual uint32_t ordinal_pos() const = 0;

  virtual ~BlockDecoder() {}
};

// Decoder for UINT32 type, GROUP_VARINT coding
// TODO: rename?
class IntBlockDecoder : public BlockDecoder {
public:
  explicit IntBlockDecoder(const Slice &slice);

  Status ParseHeader();
  void SeekToStart() {
    SeekToPositionInBlock(0);
  }

  void SeekToPositionInBlock(uint pos);

  Status SeekAtOrAfterValue(const void *value,
                            bool *exact_match,
                            SeekFlags flags = 0);

  Status CopyNextValues(size_t *n, ColumnBlock *dst);

  uint32_t ordinal_pos() const {
    DCHECK(parsed_) << "must parse header first";
    return ordinal_pos_base_ + cur_idx_;
  }

  size_t Count() const {
    return num_elems_;
  }

  bool HasNext() const {
    return (num_elems_ - cur_idx_) > 0;
  }

private:
  friend class TestEncoding;

  template<class IntSink>
  Status DoGetNextValues(size_t *n, IntSink *sink);

  Slice data_;

  bool parsed_;
  const uint8_t *ints_start_;
  uint32_t num_elems_;
  uint32_t min_elem_;
  uint32_t ordinal_pos_base_;

  const uint8_t *cur_pos_;
  size_t cur_idx_;

  // Items that have been decoded but not yet yielded
  // to the user. The next one to be yielded is at the
  // *end* of the vector!
  std::vector<uint32_t> pending_;
};


// Decoder for STRING type, PREFIX encoding
class StringBlockDecoder : public BlockDecoder {
public:
  explicit StringBlockDecoder(const Slice &slice);

  virtual Status ParseHeader();
  virtual void SeekToPositionInBlock(uint pos);
  virtual Status SeekAtOrAfterValue(const void *value,
                                    bool *exact_match,
                                    SeekFlags flags = 0);
  Status CopyNextValues(size_t *n, ColumnBlock *dst);

  virtual bool HasNext() const {
    DCHECK(parsed_);
    return cur_idx_ < num_elems_;
  }

  virtual size_t Count() const {
    DCHECK(parsed_);
    return num_elems_;
  }

  virtual uint32_t ordinal_pos() const {
    DCHECK(parsed_);
    return ordinal_pos_base_ + cur_idx_;
  }

private:
  Status SkipForward(int n);
  Status ParseNextValue();
  const char *DecodeEntryLengths(const char *ptr,
                           uint32_t *shared,
                           uint32_t *non_shared) const;

  const char * GetRestartPoint(uint32_t idx) const;
  void SeekToRestartPoint(uint32_t idx);

  void SeekToStart();

  Slice data_;

  bool parsed_;

  uint32_t num_elems_;
  uint32_t ordinal_pos_base_;

  uint32_t num_restarts_;
  const uint32_t *restarts_;
  uint32_t restart_interval_;

  const char *data_start_;

  // Pointers and data to be returned by the next call to
  // GetNextValues().
  // These are advanced by ParseNextValue()
  uint32_t cur_idx_;
  faststring cur_val_;
  const char *next_ptr_;
};

} // namespace cfile
} // namespace kudu

#endif
