// Copyright (c) 2012, Cloudera, inc.

#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/utility/binary.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include "gutil/stringprintf.h"
#include "util/test_macros.h"

#include "cfile.h"
#include "block_encodings.h"

namespace kudu { namespace cfile {


class TestEncoding : public ::testing::Test {
protected:
  // Encodes the given four ints as group-varint, then
  // decodes and ensures the result is the same.
  static void DoTestRoundTripGVI32(
    uint32_t a, uint32_t b, uint32_t c, uint32_t d) {

    std::string buf;
    IntBlockBuilder::AppendGroupVarInt32(
      &buf, a, b, c, d);

    uint32_t a_rt, b_rt, c_rt, d_rt;

    const uint8_t *end = IntBlockDecoder::DecodeGroupVarInt32(
      reinterpret_cast<const uint8_t *>(buf.c_str()),
      &a_rt, &b_rt, &c_rt, &d_rt);

    ASSERT_EQ(a, a_rt);
    ASSERT_EQ(b, b_rt);
    ASSERT_EQ(c, c_rt);
    ASSERT_EQ(d, d_rt);
    ASSERT_EQ(reinterpret_cast<const char *>(end),
              buf.c_str() + buf.size());
  }
};


TEST_F(TestEncoding, TestGroupVarInt) {
  std::string buf;
  IntBlockBuilder::AppendGroupVarInt32(
    &buf, 0, 0, 0, 0);
  ASSERT_EQ(5UL, buf.size());
  ASSERT_EQ(0, memcmp("\x00\x00\x00\x00\x00", buf.c_str(), 5));
  buf.clear();

  // All 1-byte
  IntBlockBuilder::AppendGroupVarInt32(
    &buf, 1, 2, 3, 254);
  ASSERT_EQ(5UL, buf.size());
  ASSERT_EQ(0, memcmp("\x00\x01\x02\x03\xfe", buf.c_str(), 5));
  buf.clear();

  // Mixed 1-byte and 2-byte
  IntBlockBuilder::AppendGroupVarInt32(
    &buf, 256, 2, 3, 65535);
  ASSERT_EQ(7UL, buf.size());
  ASSERT_EQ( BOOST_BINARY( 01 00 00 01 ), buf.at(0));
  ASSERT_EQ(256, *reinterpret_cast<uint16_t *>(&buf[1]));
  ASSERT_EQ(2, *reinterpret_cast<uint8_t *>(&buf[3]));
  ASSERT_EQ(3, *reinterpret_cast<uint8_t *>(&buf[4]));
  ASSERT_EQ(65535, *reinterpret_cast<uint16_t *>(&buf[5]));
}


// Round-trip encode/decodes using group varint
TEST_F(TestEncoding, TestGroupVarIntRoundTrip) {
  // A few simple tests.
  DoTestRoundTripGVI32(0, 0, 0, 0);
  DoTestRoundTripGVI32(1, 2, 3, 4);
  DoTestRoundTripGVI32(1, 2000, 3, 200000);

  // Then a randomized test.
  for (int i = 0; i < 10000; i++) {
    DoTestRoundTripGVI32(random(), random(), random(), random());
  }
}

TEST_F(TestEncoding, TestIntBlockEncoder) {
  boost::scoped_ptr<WriterOptions> opts(new WriterOptions());
  IntBlockBuilder ibb(opts.get());

  int *ints = new int[10000];
  for (int i = 0; i < 10000; i++) {
    ints[i] = random();
  }
  ibb.Add(reinterpret_cast<int *>(ints), 10000);
  delete[] ints;

  Slice s = ibb.Finish(12345);
  LOG(INFO) << "Encoded size for 10k ints: " << s.size();

  // Test empty case -- should be 5 bytes for just the
  // header word (all zeros)
  ibb.Reset();
  s = ibb.Finish(0);
  ASSERT_EQ(5UL, s.size());
}

TEST_F(TestEncoding, TestIntBlockRoundTrip) {
  boost::scoped_ptr<WriterOptions> opts(new WriterOptions());
  const uint32_t kOrdinalPosBase = 12345;

  srand(123);

  std::vector<uint32_t> to_insert;
  for (int i = 0; i < 10003; i++) {
    to_insert.push_back(random());
  }

  IntBlockBuilder ibb(opts.get());
  ibb.Add(&to_insert[0], to_insert.size());
  Slice s = ibb.Finish(kOrdinalPosBase);

  IntBlockDecoder ibd(s);
  ibd.ParseHeader();

  ASSERT_EQ(kOrdinalPosBase, ibd.ordinal_pos());

  std::vector<uint32_t> decoded;
  while (decoded.size() < to_insert.size()) {
    EXPECT_EQ((uint32_t)(kOrdinalPosBase + decoded.size()),
              ibd.ordinal_pos());

    int to_decode = (random() % 30) + 1;

    int before_count = decoded.size();
    ibd.GetNextValues(to_decode, &decoded);
    int after_count = decoded.size();
    EXPECT_GE(to_decode, after_count - before_count);
  }

  for (uint i = 0; i < to_insert.size(); i++) {
    if (to_insert[i] != decoded[i]) {
      FAIL() << "Fail at index " << i <<
        " inserted=" << to_insert[i] << " got=" << decoded[i];
    }
  }

  // Test Seek within block
  for (int i = 0; i < 100; i++) {
    int seek_off = random() % decoded.size();
    ibd.SeekToPositionInBlock(seek_off);

    EXPECT_EQ((uint32_t)(kOrdinalPosBase + seek_off),
              ibd.ordinal_pos());
    std::vector<uint32_t> ret;
    ibd.GetNextValues(1, &ret);
    EXPECT_EQ(1u, ret.size());
    EXPECT_EQ(decoded[seek_off], ret[0]);
  }
}

TEST_F(TestEncoding, TestStringBlockBuilderRoundTrip) {
  WriterOptions opts;
  boost::ptr_vector<string> to_insert;
  std::vector<Slice> slices;

  // Prepare 10K items (storage and associated slices)
  for (int i = 0; i < 10000; i++) {
    string *val = new string(StringPrintf("hello%d\n", i));
    to_insert.push_back(val);
    slices.push_back(Slice(*val));
  }

  // Push into a block builder
  StringBlockBuilder sbb(&opts);

  int rem = slices.size();
  Slice *ptr = &slices[0];
  while (rem > 0) {
    int added = sbb.Add(reinterpret_cast<void *>(ptr), rem);
    CHECK(added > 0);
    rem -= added;
    ptr += added;
  }

  ASSERT_EQ(slices.size(), sbb.Count());
  Slice s = sbb.Finish(12345L);

  // the slice should take at least a few bytes per entry
  ASSERT_GT(s.size(), 20000u);

  // TODO: add decoder test here
}


} // namespace cfile
} // namespace kudu

int main(int argc, char **argv) {
  google::InstallFailureSignalHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
