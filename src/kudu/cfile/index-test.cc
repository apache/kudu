// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cfile/block_pointer.h"
#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/index_block.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"
#include "kudu/gutil/endian.h"
#include "kudu/util/coding.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/faststring.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/protobuf_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::unique_ptr;

namespace kudu {
namespace cfile {

Status SearchInReaderString(const IndexBlockReader& reader,
                            const string& search_key,
                            BlockPointer* ptr,
                            Slice* match) {

  static faststring dst;

  unique_ptr<IndexBlockIterator> iter(reader.NewIterator());
  dst.clear();
  KeyEncoderTraits<BINARY, faststring>::Encode(search_key, &dst);
  RETURN_NOT_OK(iter->SeekAtOrBefore(Slice(dst)));

  *ptr = iter->GetCurrentBlockPointer();
  *match = iter->GetCurrentKey();
  return Status::OK();
}


Status SearchInReaderUint32(const IndexBlockReader& reader,
                            uint32_t search_key,
                            BlockPointer* ptr,
                            Slice* match) {

  static faststring dst;

  unique_ptr<IndexBlockIterator> iter(reader.NewIterator());
  dst.clear();
  KeyEncoderTraits<UINT32, faststring>::Encode(search_key, &dst);
  RETURN_NOT_OK(iter->SeekAtOrBefore(Slice(dst)));

  *ptr = iter->GetCurrentBlockPointer();
  *match = iter->GetCurrentKey();
  return Status::OK();
}

// Expects a Slice containing a big endian encoded int
static uint32_t SliceAsUInt32(const Slice& slice) {
  CHECK_EQ(4, slice.size());
  uint32_t val = 0;
  memcpy(&val, slice.data(), slice.size());
  val = BigEndian::FromHost32(val);
  return val;
}

static void AddToIndex(IndexBlockBuilder* idx,
                       uint32_t val,
                       const BlockPointer& block_pointer) {
  static faststring dst;
  dst.clear();
  KeyEncoderTraits<UINT32, faststring>::Encode(val, &dst);
  idx->Add(Slice(dst), block_pointer);
}

static void AddEmptyKeyToIndex(IndexBlockBuilder* idx,
                               const BlockPointer& block_pointer) {
  idx->Add({}, block_pointer);
}

// Test IndexBlockBuilder and IndexReader with integers
TEST(TestIndexBuilder, TestIndexWithInts) {

  // Encode an index block.
  IndexBlockBuilder idx(true);

  static constexpr int EXPECTED_NUM_ENTRIES = 4;

  uint32_t i;

  i = 10;
  AddToIndex(&idx, i, BlockPointer(90010, 64 * 1024));

  i = 20;
  AddToIndex(&idx, i, BlockPointer(90020, 64 * 1024));

  i = 30;
  AddToIndex(&idx, i, BlockPointer(90030, 64 * 1024));

  i = 40;
  AddToIndex(&idx, i, BlockPointer(90040, 64 * 1024));

  size_t est_size = idx.EstimateEncodedSize();
  Slice s = idx.Finish();

  // Estimated size should be between 75-100%
  // of actual size.
  EXPECT_LT(s.size(), est_size);
  EXPECT_GT(s.size(), est_size * 3 /4);

  // Open the encoded block in a reader.
  IndexBlockReader reader;
  ASSERT_OK(reader.Parse(s));

  // Should have all the entries we inserted.
  ASSERT_EQ(EXPECTED_NUM_ENTRIES, static_cast<int>(reader.Count()));

  // Search for a value prior to first entry
  BlockPointer ptr;
  Slice match;
  Status status = SearchInReaderUint32(reader, 0, &ptr, &match);
  EXPECT_TRUE(status.IsNotFound());

  // Search for a value equal to first entry
  status = SearchInReaderUint32(reader, 10, &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90010, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ(10, SliceAsUInt32(match));

  // Search for a value between 1st and 2nd entries.
  // Should return 1st.
  status = SearchInReaderUint32(reader, 15, &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90010, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ(10, SliceAsUInt32(match));

  // Search for a value equal to 2nd
  // Should return 2nd.
  status = SearchInReaderUint32(reader, 20, &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90020, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ(20, SliceAsUInt32(match));

  // Between 2nd and 3rd.
  // Should return 2nd
  status = SearchInReaderUint32(reader, 25, &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90020, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ(20, SliceAsUInt32(match));

  // Equal 3rd
  status = SearchInReaderUint32(reader, 30, &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90030, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ(30, SliceAsUInt32(match));

  // Between 3rd and 4th
  status = SearchInReaderUint32(reader, 35, &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90030, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ(30, SliceAsUInt32(match));

  // Equal 4th (last)
  status = SearchInReaderUint32(reader, 40, &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90040, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ(40, SliceAsUInt32(match));

  // Greater than 4th (last)
  status = SearchInReaderUint32(reader, 45, &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90040, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ(40, SliceAsUInt32(match));

  idx.Reset();
}

TEST(TestIndexBlock, TestIndexBlockWithStrings) {
  IndexBlockBuilder idx(true);

  // Insert data for "hello-10" through "hello-40" by 10s
  const int EXPECTED_NUM_ENTRIES = 4;
  char data[20];
  for (int i = 1; i <= EXPECTED_NUM_ENTRIES; i++) {
    int len = snprintf(data, sizeof(data), "hello-%d", i * 10);
    Slice s(data, len);

    idx.Add(s, BlockPointer(90000 + i*10, 64 * 1024));
  }
  size_t est_size = idx.EstimateEncodedSize();
  Slice s = idx.Finish();

  // Estimated size should be between 75-100%
  // of actual size.
  EXPECT_LT(s.size(), est_size);
  EXPECT_GT(s.size(), est_size * 3 /4);

  VLOG(1) << kudu::HexDump(s);

  // Open the encoded block in a reader.
  IndexBlockReader reader;
  ASSERT_OK(reader.Parse(s));

  // Should have all the entries we inserted.
  ASSERT_EQ(EXPECTED_NUM_ENTRIES, static_cast<int>(reader.Count()));

  // Search for a value prior to first entry
  BlockPointer ptr;
  Slice match;
  Status status = SearchInReaderString(reader, "hello", &ptr, &match);
  EXPECT_TRUE(status.IsNotFound());

  // Search for a value equal to first entry
  status = SearchInReaderString(reader, "hello-10", &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90010, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ("hello-10", match);

  // Search for a value between 1st and 2nd entries.
  // Should return 1st.
  status = SearchInReaderString(reader, "hello-15", &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90010, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ("hello-10", match);

  // Search for a value equal to 2nd
  // Should return 2nd.
  status = SearchInReaderString(reader, "hello-20", &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90020, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ("hello-20", match);

  // Between 2nd and 3rd.
  // Should return 2nd
  status = SearchInReaderString(reader, "hello-25", &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90020, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ("hello-20", match);

  // Equal 3rd
  status = SearchInReaderString(reader, "hello-30", &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90030, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ("hello-30", match);

  // Between 3rd and 4th
  status = SearchInReaderString(reader, "hello-35", &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90030, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ("hello-30", match);

  // Equal 4th (last)
  status = SearchInReaderString(reader, "hello-40", &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90040, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ("hello-40", match);

  // Greater than 4th (last)
  status = SearchInReaderString(reader, "hello-45", &ptr, &match);
  ASSERT_OK(status);
  EXPECT_EQ(90040, static_cast<int>(ptr.offset()));
  EXPECT_EQ(64 * 1024, static_cast<int>(ptr.size()));
  EXPECT_EQ("hello-40", match);
}

// Test seeking around using the IndexBlockIterator class
TEST(TestIndexBlock, TestIterator) {
  // Encode an index block with 1000 entries.
  IndexBlockBuilder idx(true);

  for (int i = 0; i < 1000; i++) {
    uint32_t key = i * 10;
    AddToIndex(&idx, key, BlockPointer(100000 + i, 64 * 1024));
  }

  Slice s = idx.Finish();

  IndexBlockReader reader;
  ASSERT_OK(reader.Parse(s));
  unique_ptr<IndexBlockIterator> iter(reader.NewIterator());
  ASSERT_OK(iter->SeekToIndex(0));
  ASSERT_EQ(0U, SliceAsUInt32(iter->GetCurrentKey()));
  ASSERT_EQ(100000U, iter->GetCurrentBlockPointer().offset());

  ASSERT_OK(iter->SeekToIndex(50));
  ASSERT_EQ(500U, SliceAsUInt32(iter->GetCurrentKey()));
  ASSERT_EQ(100050U, iter->GetCurrentBlockPointer().offset());

  ASSERT_TRUE(iter->HasNext());
  ASSERT_OK(iter->Next());
  ASSERT_EQ(510U, SliceAsUInt32(iter->GetCurrentKey()));
  ASSERT_EQ(100051U, iter->GetCurrentBlockPointer().offset());

  ASSERT_OK(iter->SeekToIndex(999));
  ASSERT_EQ(9990U, SliceAsUInt32(iter->GetCurrentKey()));
  ASSERT_EQ(100999U, iter->GetCurrentBlockPointer().offset());
  ASSERT_FALSE(iter->HasNext());
  ASSERT_TRUE(iter->Next().IsNotFound());

  ASSERT_OK(iter->SeekToIndex(0));
  ASSERT_EQ(0U, SliceAsUInt32(iter->GetCurrentKey()));
  ASSERT_EQ(100000U, iter->GetCurrentBlockPointer().offset());
  ASSERT_TRUE(iter->HasNext());
}

// Parse an empty index block and make sure the IndexBlockReader's API works
// as expected.
TEST(TestIndexBlock, EmptyBlock) {
  IndexBlockBuilder idx(true);
  ASSERT_TRUE(idx.empty());
  ASSERT_EQ(0, idx.count());
  Slice key;
  const auto s = idx.GetFirstKey(&key);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  const Slice b = idx.Finish();
  IndexBlockReader reader;
  ASSERT_OK(reader.Parse(b));
  ASSERT_TRUE(reader.IsLeaf());
  ASSERT_EQ(0, reader.Count());
  unique_ptr<IndexBlockIterator> it(reader.NewIterator());
  ASSERT_FALSE(it->HasNext());
  {
    const auto s = it->SeekToIndex(0);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }
  {
    const auto s = it->Next();
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }
  {
    const Slice key;
    const auto s = it->SeekAtOrBefore(key);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }
}

// Test how IndexBlockBuilder and IndexBlockReader work with empty keys
// in an index block.
TEST(TestIndexBlock, EmptyKeys) {
  // All the sanity checks in the parser are based on min/max estimates,
  // and there might be variations in the size of the encoded fields because
  // of varint encoding, where higher values consume more space when serialized.
  // That's exercised below by using different number of blocks in the generated
  // index file.

  // One empty key.
  {
    IndexBlockBuilder idx(true);
    AddEmptyKeyToIndex(&idx, BlockPointer(0, 1));
    const Slice b = idx.Finish();

    IndexBlockReader reader;
    ASSERT_OK(reader.Parse(b));
  }

  // Several empty keys.
  {
    IndexBlockBuilder idx(true);
    for (auto i = 0; i < 8; ++i) {
      AddEmptyKeyToIndex(&idx, BlockPointer(i, 1));
    }
    const Slice b = idx.Finish();

    IndexBlockReader reader;
    ASSERT_OK(reader.Parse(b));
  }

  // Many empty keys.
  {
    IndexBlockBuilder idx(true);
    for (auto i = 0; i < 65536; ++i) {
      AddEmptyKeyToIndex(&idx, BlockPointer(i, 1));
    }
    const Slice b = idx.Finish();

    IndexBlockReader reader;
    ASSERT_OK(reader.Parse(b));
  }
}

// Corrupt the trailer or its size in the block and try to parse the block.
TEST(TestIndexBlock, CorruptedTrailer) {
  // Data chunk is too small.
  {
    uint8_t buf[3];
    Slice b(buf, sizeof(buf));
    IndexBlockReader reader;
    const auto s = reader.Parse(b);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "index block too small");
  }

  // The trailer's size is too small.
  {
    IndexBlockBuilder idx(true);
    AddToIndex(&idx, 0, BlockPointer(1, 1024));
    const Slice b = idx.Finish();

    const uint8_t* trailer_size_ptr = b.data() + b.size() - sizeof(uint32_t);

    faststring buf;
    InlinePutFixed32(&buf, 3);
    memmove(const_cast<uint8_t*>(trailer_size_ptr), buf.data(), buf.size());

    IndexBlockReader reader;
    const auto s = reader.Parse(b);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "3: invalid index block trailer size");
  }

  // The trailer's size is too big.
  {
    IndexBlockBuilder idx(true);
    AddToIndex(&idx, 1, BlockPointer(2, 1024));
    const Slice b = idx.Finish();

    const uint8_t* trailer_size_ptr = b.data() + b.size() - sizeof(uint32_t);

    faststring buf;
    InlinePutFixed32(&buf, 1234);
    memmove(const_cast<uint8_t*>(trailer_size_ptr), buf.data(), buf.size());

    IndexBlockReader reader;
    const auto s = reader.Parse(b);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "1234: invalid index block trailer size");
  }

  // The number of key offsets too high for the actual amount of data stored.
  {
    IndexBlockBuilder idx(true);
    AddToIndex(&idx, 2, BlockPointer(3, 1024));
    const Slice b = idx.Finish();

    const uint8_t* trailer_size_ptr = b.data() + b.size() - sizeof(uint32_t);
    const size_t trailer_size = DecodeFixed32(trailer_size_ptr);
    const uint8_t* trailer_ptr = trailer_size_ptr - trailer_size;
    IndexBlockTrailerPB t;
    ASSERT_TRUE(t.ParseFromArray(trailer_ptr, trailer_size));
    t.set_num_entries(5);

    // To make sure writing over the new serialized message doesn't require
    // updating the encoded trailer size, check that the new serialized
    // PB message is the same size as the original message.
    ASSERT_EQ(trailer_size, t.ByteSizeLong());

    faststring buf;
    ASSERT_TRUE(AppendPBToString(t, &buf));
    memmove(const_cast<uint8_t*>(trailer_ptr), buf.data(), buf.size());

    IndexBlockReader reader;
    const auto s = reader.Parse(b);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "5: too many entries in trailer");
  }

  // Negative number of key offsets.
  {
    IndexBlockBuilder idx(true);
    AddToIndex(&idx, 3, BlockPointer(4, 1024));
    const Slice b = idx.Finish();

    IndexBlockTrailerPB t;
    {
      const uint8_t* trailer_size_ptr = b.data() + b.size() - sizeof(uint32_t);
      const size_t trailer_size = DecodeFixed32(trailer_size_ptr);
      const uint8_t* trailer_ptr = trailer_size_ptr - trailer_size;
      ASSERT_TRUE(t.ParseFromArray(trailer_ptr, trailer_size));
    }

    IndexBlockTrailerPB tc(t);
    tc.set_num_entries(-3);
    // The new trailer serialized into a bigger message due to negative value
    // of the int32_t field.
    ASSERT_GT(tc.ByteSizeLong(), t.ByteSizeLong());

    unique_ptr<uint8_t[]> bc_data(
        new uint8_t[b.size() + tc.ByteSizeLong() - t.ByteSizeLong()]);
    Slice bc(b);
    bc.relocate(bc_data.get());

    faststring buf;
    ASSERT_TRUE(AppendPBToString(tc, &buf));
    InlinePutFixed32(&buf, tc.ByteSizeLong());

    // Overwrite the trailer and the trailer size.
    memmove(bc.mutable_data() + bc.size() - tc.ByteSizeLong() - sizeof(uint32_t),
            buf.data(),
            buf.size());

    IndexBlockReader reader;
    const auto s = reader.Parse(bc);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "-3: bad number of entries in trailer");
  }
}

TEST(TestIndexKeys, TestGetSeparatingKey) {
  // Test example cases
  Slice left = "";
  Slice right = "apple";
  GetSeparatingKey(left, &right);
  ASSERT_EQ(right, Slice("a"));

  left = "cardamom";
  right = "carrot";
  GetSeparatingKey(left, &right);
  ASSERT_EQ(right, Slice("carr"));

  left = "fennel";
  right = "fig";
  GetSeparatingKey(left, &right);
  ASSERT_EQ(right, Slice("fi"));
}

} // namespace cfile
} // namespace kudu
