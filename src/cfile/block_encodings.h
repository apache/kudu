// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_CFILE_BLOCK_ENCODINGS_H
#define KUDU_CFILE_BLOCK_ENCODINGS_H

#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <stdint.h>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "util/memory/arena.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {
namespace cfile {

class WriterOptions;

using std::vector;
using std::string;

typedef uint32_t IntType;


class BlockBuilder : boost::noncopyable {
public:
  // TODO: add a more type-checkable wrapper for void *,
  // like ConstVariantPointer in Supersonic
  virtual int Add(const void *vals, int count) = 0;
  virtual Slice Finish(uint32_t ordinal_pos) = 0;
  virtual void Reset() = 0;
  virtual uint64_t EstimateEncodedSize() const = 0;
  virtual size_t Count() const = 0;
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

  int Add(const void *vals, int count);

  // Return a Slice which represents the encoded data.
  //
  // This Slice points to internal data of this class
  // and becomes invalid after the builder is destroyed
  // or after Finish() is called again.
  Slice Finish(uint32_t ordinal_pos);

  void Reset();

  // Return an estimate of the number
  uint64_t EstimateEncodedSize() const;

  size_t Count() const;

private:
  friend class TestEncoding;
  FRIEND_TEST(TestEncoding, TestGroupVarInt);
  FRIEND_TEST(TestEncoding, TestIntBlockEncoder);

  vector<IntType> ints_;
  string buffer_;
  uint64_t estimated_raw_size_;

  const WriterOptions *options_;

  static void AppendShorterInt(std::string *s, uint32_t i, size_t bytes);
  static void AppendGroupVarInt32(
    std::string *s,
    uint32_t a, uint32_t b, uint32_t c, uint32_t d);

  enum {
    kEstimatedHeaderSizeBytes = 6
  };
};


// Encoding for data blocks of strings.
// This encodes in a manner similar to LevelDB (prefix coding)
class StringBlockBuilder : public BlockBuilder {
public:
  explicit StringBlockBuilder(const WriterOptions *options);

  int Add(const void *vals, int count);

  // Return a Slice which represents the encoded data.
  //
  // This Slice points to internal data of this class
  // and becomes invalid after the builder is destroyed
  // or after Finish() is called again.
  Slice Finish(uint32_t ordinal_pos);

  void Reset();

  // Return an estimate of the number
  uint64_t EstimateEncodedSize() const;

  size_t Count() const;
private:
  string buffer_;
  string last_val_;

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

  // Fetch the next set of values from the block into 'out'
  // The output vector must have space for up to 'n' values
  // of this block decoder's type.
  // Returns the number of values fetched.
  //
  // In the case that the values are themselves references
  // to other memory (eg Slices), the referred-to memory is
  // owned by this decoder object, and can be recycled or reused
  // on the next call to GetNextValues or when the decoder is
  // destructed. The caller should deep-copy them if they need to
  // persist beyond that lifetime.
  //
  // TODO: add some way to do a shallow "view" in the future?
  // TODO: add some typesafe wrappers for void pointers
  virtual int GetNextValues(int n, void *out) = 0;

  // Return true if there are more values remaining to be iterated.
  // TODO: change this to a Remaining() call?
  virtual bool HasNext() const = 0;

  // Return the number of elements in this block.
  virtual size_t Count() const = 0;

  // Return the ordinal position in the file of the first entry
  // in this block.
  virtual uint32_t ordinal_pos() const = 0;

  virtual ~BlockDecoder() {}
};

// Decoder for UINT32 type, GROUP_VARINT coding
// TODO: rename?
class IntBlockDecoder : public BlockDecoder {
public:
  explicit IntBlockDecoder(const Slice &slice) :
    data_(slice),
    parsed_(false) {
  }

  Status ParseHeader();
  void SeekToStart() {
    SeekToPositionInBlock(0);
  }

  void SeekToPositionInBlock(uint pos);

  int GetNextValues(int n, void *out);

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

  static const uint8_t *DecodeGroupVarInt32(
    const uint8_t *src,
    uint32_t *a, uint32_t *b, uint32_t *c, uint32_t *d);

  template<class IntSink>
  int DoGetNextValues(int n, IntSink *sink);

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
  virtual int GetNextValues(int n, void *out);

  virtual bool HasNext() const {
    DCHECK(parsed_);
    return cur_idx_ < num_elems_ - 1;
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


  void SeekToStart();

  Slice data_;

  bool parsed_;

  uint32_t num_elems_;
  uint32_t ordinal_pos_base_;

  uint32_t num_restarts_;
  const uint32_t *restarts_;

  const char *data_start_;

  uint32_t cur_idx_;
  const char *cur_ptr_;
  faststring cur_val_;

  // Arena used for output storage for GetNextValues().
  Arena out_arena_;
};

} // namespace cfile
} // namespace kudu

#endif
