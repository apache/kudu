// Copyright (c) 2012, Cloudera, inc.

#include <gtest/gtest.h>

#include "cfile.h"
#include "index_btree.h"
#include "util/status.h"
#include "util/test_macros.h"

namespace kudu { namespace cfile {


Status SearchInReader(const IndexBlockReader<uint32_t > &reader,
                      uint32_t search_key,
                      BlockPointer *ptr,
                      uint32_t *match) {
  scoped_ptr<IndexBlockIterator<uint32_t> > iter(reader.NewIterator());

  RETURN_NOT_OK(iter->SeekAtOrBefore(search_key));

  *ptr = iter->GetCurrentBlockPointer();
  *match = *reinterpret_cast<const uint32_t *>(iter->GetCurrentKey());
  return Status::OK();
}

static uint32_t GetUInt32Key(const scoped_ptr<IndexBlockIterator<uint32_t> > &iter) {
  return *reinterpret_cast<const uint32_t*>(iter->GetCurrentKey());
}


// Test IndexBlockBuilder and IndexReader with integers
TEST(TestIndexBuilder, TestIndexWithInts) {

  // Encode an index block.
  WriterOptions opts;
  IndexBlockBuilder idx(&opts, true);

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
  IndexBlockReader<uint32_t> reader(s);
  ASSERT_STATUS_OK(reader.Parse());

  // Should have all the entries we inserted.
  ASSERT_EQ(EXPECTED_NUM_ENTRIES, (int)reader.Count());

  // Search for a value prior to first entry
  BlockPointer ptr;
  uint32_t match;
  Status status = SearchInReader(reader, 0, &ptr, &match);
  EXPECT_TRUE(status.IsNotFound());

  // Search for a value equal to first entry
  status = SearchInReader(reader, 10, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90010, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(10, (int)match);

  // Search for a value between 1st and 2nd entries.
  // Should return 1st.
  status = SearchInReader(reader, 15, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90010, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(10, (int)match);

  // Search for a value equal to 2nd
  // Should return 2nd.
  status = SearchInReader(reader, 20, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90020, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(20, (int)match);

  // Between 2nd and 3rd.
  // Should return 2nd
  status = SearchInReader(reader, 25, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90020, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(20, (int)match);

  // Equal 3rd
  status = SearchInReader(reader, 30, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90030, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(30, (int)match);

  // Between 3rd and 4th
  status = SearchInReader(reader, 35, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90030, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(30, (int)match);

  // Equal 4th (last)
  status = SearchInReader(reader, 40, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90040, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(40, (int)match);

  // Greater than 4th (last)
  status = SearchInReader(reader, 45, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90040, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(40, (int)match);

  idx.Reset();
}

// Test seeking around using the IndexBlockIterator class
TEST(TestIndexBlock, TestIterator) {
  // Encode an index block with 1000 entries.
  WriterOptions opts;
  IndexBlockBuilder idx(&opts, true);

  for (int i = 0; i < 1000; i++) {
    uint32_t key = i * 10;
    idx.Add(&key, BlockPointer(100000 + i, 64 * 1024));
  }
  Slice s = idx.Finish();

  IndexBlockReader<uint32_t> reader(s);
  ASSERT_STATUS_OK(reader.Parse());
  scoped_ptr<IndexBlockIterator<uint32_t> > iter(
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
