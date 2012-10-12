// Copyright (c) 2012, Cloudera, inc.

#include <boost/scoped_ptr.hpp>
#include <boost/utility/binary.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include "cfile.h"
#include "int_block.h"

namespace kudu { namespace cfile {

TEST(TestCFile, TestGroupVarInt) {
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

TEST(TestCFile, TestIntBlockEncoder) {
  boost::scoped_ptr<WriterOptions> opts(new WriterOptions());
  IntBlockBuilder ibb(opts.get());
  for (int i = 0; i < 10000; i++) {
    ibb.Add(random());
  }
  Slice s = ibb.Finish();
  LOG(INFO) << "Encoded size for 10k ints: " << s.size();

  // Test empty case -- should be 5 bytes for just the
  // header word (all zeros)
  ibb.Reset();
  s = ibb.Finish();
  ASSERT_EQ(5UL, s.size());
}


} // namespace cfile
} // namespace kudu

int main(int argc, char **argv) {
  google::InstallFailureSignalHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
