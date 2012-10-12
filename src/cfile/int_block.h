// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_CFILE_INT_BLOCK_H
#define KUDU_CFILE_INT_BLOCK_H

#include <boost/noncopyable.hpp>
#include <stdint.h>
#include <vector>

#include <gtest/gtest.h>

#include "util/slice.h"

namespace kudu {
namespace cfile {

class WriterOptions;

using std::vector;
using std::string;

typedef uint32_t IntType;

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

class IntBlockBuilder : boost::noncopyable {
public:
  explicit IntBlockBuilder(const WriterOptions *options);

  void Add(IntType val);

  // Return a Slice which represents the encoded data.
  //
  // This Slice points to internal data of this class
  // and becomes invalid after the builder is destroyed
  // or after Finish() is called again.
  Slice Finish();

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


class IntBlockDecoder : boost::noncopyable {
public:
  IntBlockDecoder() {}

private:
  friend class TestEncoding;

  static const uint8_t *DecodeGroupVarInt32(
    const uint8_t *src,
    uint32_t *a, uint32_t *b, uint32_t *c, uint32_t *d);

};


} // namespace cfile
} // namespace kudu

#endif
