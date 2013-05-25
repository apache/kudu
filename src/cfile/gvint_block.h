// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CFILE_GVINT_BLOCK_H
#define KUDU_CFILE_GVINT_BLOCK_H

#include "block_encodings.h"

#include <stdint.h>
#include <vector>

namespace kudu {
namespace cfile {

struct WriterOptions;
typedef uint32_t IntType;

using std::vector;

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
class GVIntBlockBuilder : public BlockBuilder {
public:
  explicit GVIntBlockBuilder(const WriterOptions *options);

  int Add(const uint8_t *vals, size_t count);

  Slice Finish(rowid_t ordinal_pos);

  void Reset();

  uint64_t EstimateEncodedSize() const;

  size_t Count() const;

  // Return the first added key.
  // key should be a uint32_t *
  Status GetFirstKey(void *key) const;

  // Min Length of a header. (prefix + 4 tags)
  static const size_t kMinHeaderSize = 5;

private:
  friend class TestEncoding;
  FRIEND_TEST(TestEncoding, TestGroupVarInt);
  FRIEND_TEST(TestEncoding, TestIntBlockEncoder);

  vector<IntType> ints_;
  faststring buffer_;
  uint64_t estimated_raw_size_;

  const WriterOptions *options_;

  enum {
    kEstimatedHeaderSizeBytes = 10,

    // Up to 3 "0s" can be tacked on the end of the block to round out
    // the groups of 4
    kTrailerExtraPaddingBytes = 3
  };
};

// Decoder for UINT32 type, GROUP_VARINT coding
class GVIntBlockDecoder : public BlockDecoder {
public:
  explicit GVIntBlockDecoder(const Slice &slice);

  Status ParseHeader();
  void SeekToStart() {
    SeekToPositionInBlock(0);
  }

  void SeekToPositionInBlock(uint pos);

  Status SeekAtOrAfterValue(const void *value, bool *exact_match);

  Status CopyNextValues(size_t *n, ColumnDataView *dst);

  rowid_t ordinal_pos() const {
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
  rowid_t ordinal_pos_base_;

  const uint8_t *cur_pos_;
  size_t cur_idx_;

  // Items that have been decoded but not yet yielded
  // to the user. The next one to be yielded is at the
  // *end* of the vector!
  std::vector<uint32_t> pending_;
};


}
}

#endif
