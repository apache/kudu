// Copyright (c) 2012, Cloudera, inc.

#include <gtest/gtest.h>

#include "cfile.h"
#include "index_btree.h"
#include "util/hexdump.h"
#include "util/status.h"
#include "util/test_macros.h"

namespace kudu { namespace cfile {


template<DataType type>
Status SearchInReader(const IndexBlockReader<type> &reader,
                      const typename DataTypeTraits<type>::cpp_type &search_key,
                      BlockPointer *ptr,
                      typename DataTypeTraits<type>::cpp_type *match) {
  gscoped_ptr<IndexBlockIterator<type> > iter(reader.NewIterator());

  RETURN_NOT_OK(iter->SeekAtOrBefore(search_key));

  *ptr = iter->GetCurrentBlockPointer();
  *match = *iter->GetCurrentKey();
  return Status::OK();
}

static uint32_t GetUInt32Key(const gscoped_ptr<IndexBlockIterator<UINT32> > &iter) {
  return *iter->GetCurrentKey();
}


// Test IndexBlockBuilder and IndexReader with integers
TEST(TestIndexBuilder, TestIndexWithInts) {

  // Encode an index block.
  WriterOptions opts;
  IndexBlockBuilder idx(&opts, UINT32, true);

  const int EXPECTED_NUM_ENTRIES = 4;

  uint32_t i;

  i = 10;
  idx.Add(&i, BlockPointer(90010, 64*1024));

  i = 20;
  idx.Add(&i, BlockPointer(90020, 64*1024));

  i = 30;
  idx.Add(&i, BlockPointer(90030, 64*1024));

  i = 40;
  idx.Add(&i, BlockPointer(90040, 64*1024));

  size_t est_size = idx.EstimateEncodedSize();
  Slice s = idx.Finish();

  // Estimated size should be between 75-100%
  // of actual size.
  EXPECT_LT(s.size(), est_size);
  EXPECT_GT(s.size(), est_size * 3 /4);

  // Open the encoded block in a reader.
  IndexBlockReader<UINT32> reader;
  ASSERT_STATUS_OK(reader.Parse(s));

  // Should have all the entries we inserted.
  ASSERT_EQ(EXPECTED_NUM_ENTRIES, (int)reader.Count());

  // Search for a value prior to first entry
  BlockPointer ptr;
  uint32_t match;
  Status status = SearchInReader<UINT32>(reader, 0, &ptr, &match);
  EXPECT_TRUE(status.IsNotFound());

  // Search for a value equal to first entry
  status = SearchInReader<UINT32>(reader, 10, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90010, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(10, (int)match);

  // Search for a value between 1st and 2nd entries.
  // Should return 1st.
  status = SearchInReader<UINT32>(reader, 15, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90010, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(10, (int)match);

  // Search for a value equal to 2nd
  // Should return 2nd.
  status = SearchInReader<UINT32>(reader, 20, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90020, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(20, (int)match);

  // Between 2nd and 3rd.
  // Should return 2nd
  status = SearchInReader<UINT32>(reader, 25, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90020, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(20, (int)match);

  // Equal 3rd
  status = SearchInReader<UINT32>(reader, 30, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90030, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(30, (int)match);

  // Between 3rd and 4th
  status = SearchInReader<UINT32>(reader, 35, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90030, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(30, (int)match);

  // Equal 4th (last)
  status = SearchInReader<UINT32>(reader, 40, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90040, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(40, (int)match);

  // Greater than 4th (last)
  status = SearchInReader<UINT32>(reader, 45, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90040, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(40, (int)match);

  idx.Reset();
}

TEST(TestIndexBlock, TestIndexBlockWithStrings) {
  WriterOptions opts;
  IndexBlockBuilder idx(&opts, STRING, true);

  // Insert data for "hello-10" through "hello-40" by 10s
  const int EXPECTED_NUM_ENTRIES = 4;
  char data[20];
  for (int i = 1; i <= EXPECTED_NUM_ENTRIES; i++) {
    int len = snprintf(data, sizeof(data), "hello-%d", i * 10);
    Slice s(data, len);

    idx.Add(&s, BlockPointer(90000 + i*10, 64 * 1024));
  }
  size_t est_size = idx.EstimateEncodedSize();
  Slice s = idx.Finish();

  // Estimated size should be between 75-100%
  // of actual size.
  EXPECT_LT(s.size(), est_size);
  EXPECT_GT(s.size(), est_size * 3 /4);

  VLOG(1) << kudu::HexDump(s);

  // Open the encoded block in a reader.
  IndexBlockReader<STRING> reader;
  ASSERT_STATUS_OK(reader.Parse(s));

  // Should have all the entries we inserted.
  ASSERT_EQ(EXPECTED_NUM_ENTRIES, (int)reader.Count());

  // Search for a value prior to first entry
  BlockPointer ptr;
  Slice match;
  Status status = SearchInReader(reader, "hello", &ptr, &match);
  EXPECT_TRUE(status.IsNotFound());

  // Search for a value equal to first entry
  status = SearchInReader(reader, "hello-10", &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90010, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ("hello-10", match);

  // Search for a value between 1st and 2nd entries.
  // Should return 1st.
  status = SearchInReader(reader, "hello-15", &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90010, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ("hello-10", match);

  // Search for a value equal to 2nd
  // Should return 2nd.
  status = SearchInReader(reader, "hello-20", &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90020, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ("hello-20", match);

  // Between 2nd and 3rd.
  // Should return 2nd
  status = SearchInReader(reader, "hello-25", &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90020, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ("hello-20", match);

  // Equal 3rd
  status = SearchInReader(reader, "hello-30", &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90030, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ("hello-30", match);

  // Between 3rd and 4th
  status = SearchInReader(reader, "hello-35", &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90030, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ("hello-30", match);

  // Equal 4th (last)
  status = SearchInReader(reader, "hello-40", &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90040, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ("hello-40", match);

  // Greater than 4th (last)
  status = SearchInReader(reader, "hello-45", &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90040, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ("hello-40", match);
}

// Test seeking around using the IndexBlockIterator class
TEST(TestIndexBlock, TestIterator) {
  // Encode an index block with 1000 entries.
  WriterOptions opts;
  IndexBlockBuilder idx(&opts, UINT32, true);

  for (int i = 0; i < 1000; i++) {
    uint32_t key = i * 10;
    idx.Add(&key, BlockPointer(100000 + i, 64 * 1024));
  }
  Slice s = idx.Finish();

  IndexBlockReader<UINT32> reader;
  ASSERT_STATUS_OK(reader.Parse(s));
  gscoped_ptr<IndexBlockIterator<UINT32> > iter(
    reader.NewIterator());
  iter->SeekToIndex(0);
  ASSERT_EQ(0U, GetUInt32Key(iter));
  ASSERT_EQ(100000U, iter->GetCurrentBlockPointer().offset());

  iter->SeekToIndex(50);
  ASSERT_EQ(500U, GetUInt32Key(iter));
  ASSERT_EQ(100050U, iter->GetCurrentBlockPointer().offset());

  ASSERT_TRUE(iter->HasNext());
  ASSERT_STATUS_OK(iter->Next());
  ASSERT_EQ(510U, GetUInt32Key(iter));
  ASSERT_EQ(100051U, iter->GetCurrentBlockPointer().offset());

  iter->SeekToIndex(999);
  ASSERT_EQ(9990U,GetUInt32Key(iter));
  ASSERT_EQ(100999U, iter->GetCurrentBlockPointer().offset());
  ASSERT_FALSE(iter->HasNext());
  ASSERT_TRUE(iter->Next().IsNotFound());

  iter->SeekToIndex(0);
  ASSERT_EQ(0U, GetUInt32Key(iter));
  ASSERT_EQ(100000U, iter->GetCurrentBlockPointer().offset());
  ASSERT_TRUE(iter->HasNext());
}

} // namespace cfile
} // namespace kudu
