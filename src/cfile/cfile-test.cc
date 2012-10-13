// Copyright (c) 2012, Cloudera, inc

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <stdlib.h>

#include "cfile.h"
#include "cfile_reader.h"
#include "cfile.pb.h"
#include "index_block.h"
#include "index_btree.h"
#include "util/env.h"

namespace kudu { namespace cfile {

#define ASSERT_STATUS_OK(status) do { \
    Status _s = status; \
    if (_s.ok()) { \
      SUCCEED(); \
    } else { \
      FAIL() << "Bad status: " << _s.ToString();  \
    } \
  } while (0);


// Test IndexBlockBuilder and IndexReader with integers
TEST(TestIndexBuilder, TestIndexWithInts) {

  // Encode an index block.
  WriterOptions opts;
  IndexBlockBuilder<uint32_t> idx(&opts, true);

  const int EXPECTED_NUM_ENTRIES = 4;

  idx.Add(10, BlockPointer(90010, 64*1024));
  idx.Add(20, BlockPointer(90020, 64*1024));
  idx.Add(30, BlockPointer(90030, 64*1024));
  idx.Add(40, BlockPointer(90040, 64*1024));

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
  Status status = reader.Search(0, &ptr, &match);
  EXPECT_TRUE(status.IsNotFound());

  // Search for a value equal to first entry
  status = reader.Search(10, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90010, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(10, (int)match);

  // Search for a value between 1st and 2nd entries.
  // Should return 1st.
  status = reader.Search(15, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90010, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(10, (int)match);

  // Search for a value equal to 2nd
  // Should return 2nd.
  status = reader.Search(20, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90020, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(20, (int)match);

  // Between 2nd and 3rd.
  // Should return 2nd
  status = reader.Search(25, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90020, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(20, (int)match);

  // Equal 3rd
  status = reader.Search(30, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90030, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(30, (int)match);

  // Between 3rd and 4th
  status = reader.Search(35, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90030, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(30, (int)match);

  // Equal 4th (last)
  status = reader.Search(40, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90040, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(40, (int)match);

  // Greater than 4th (last)
  status = reader.Search(45, &ptr, &match);
  ASSERT_STATUS_OK(status);
  EXPECT_EQ(90040, (int)ptr.offset());
  EXPECT_EQ(64 * 1024, (int)ptr.size());
  EXPECT_EQ(40, (int)match);

  idx.Reset();
}

TEST(TestIndexBlock, TestIterator) {
  // Encode an index block with 1000 entries.
  WriterOptions opts;
  IndexBlockBuilder<uint32_t> idx(&opts, true);

  for (int i = 0; i < 1000; i++) {
    idx.Add(i * 10, BlockPointer(100000 + i, 64 * 1024));
  }
  Slice s = idx.Finish();

  IndexBlockReader<uint32_t> reader(s);
  ASSERT_STATUS_OK(reader.Parse());
  scoped_ptr<IndexBlockIterator<uint32_t> > iter(
    reader.NewIterator());
  iter->SeekToIndex(0);
  ASSERT_EQ(0U, iter->GetCurrentKey());
  ASSERT_EQ(100000U, iter->GetCurrentBlockPointer().offset());

  iter->SeekToIndex(50);
  ASSERT_EQ(500U, iter->GetCurrentKey());
  ASSERT_EQ(100050U, iter->GetCurrentBlockPointer().offset());

  ASSERT_TRUE(iter->HasNext());
  ASSERT_STATUS_OK(iter->Next());
  ASSERT_EQ(510U, iter->GetCurrentKey());
  ASSERT_EQ(100051U, iter->GetCurrentBlockPointer().offset());

  iter->SeekToIndex(999);
  ASSERT_EQ(9990U, iter->GetCurrentKey());
  ASSERT_EQ(100999U, iter->GetCurrentBlockPointer().offset());
  ASSERT_FALSE(iter->HasNext());
  ASSERT_TRUE(iter->Next().IsNotFound());

  iter->SeekToIndex(0);
  ASSERT_EQ(0U, iter->GetCurrentKey());
  ASSERT_EQ(100000U, iter->GetCurrentBlockPointer().offset());
  ASSERT_TRUE(iter->HasNext());
}

class StringSink: public WritableFile {
 public:
  ~StringSink() { }

  const std::string& contents() const { return contents_; }

  virtual Status Close() { return Status::OK(); }
  virtual Status Flush() { return Status::OK(); }
  virtual Status Sync() { return Status::OK(); }

  virtual Status Append(const Slice& data) {
    contents_.append(data.data(), data.size());
    return Status::OK();
  }

 private:
  std::string contents_;
};

static void WriteTestFile(const string &path,
                          int num_entries) {
  Status s;

  WritableFile *file;
  s = Env::Default()->NewWritableFile(path, &file);

  shared_ptr<WritableFile> sink(file);
  WriterOptions opts;
  // Use a smaller block size to exercise multi-level
  // indexing.
  opts.block_size = 256;
  Writer w(opts, sink);

  ASSERT_STATUS_OK(w.Start());

  BTreeMetaPB meta;
  meta.set_identifier(kPositionalIndexIdentifier);

  shared_ptr<TreeBuilder> tree;
  s = w.AddTree(meta, &tree);
  ASSERT_STATUS_OK(s);

  // Append given number of values to the test tree
  for (int i = 0; i < num_entries; i++) {
    Status s = tree->Append(i * 10);
    // Dont use ASSERT because it accumulates all the logs
    // even for successes
    if (!s.ok()) {
      FAIL() << "Failed Append(" << i << ")";
    }
  }

  ASSERT_STATUS_OK(w.Finish());
}


TEST(TestCFile, TestWrite100MFile) {
  WriteTestFile("/tmp/cfile-Test100M", 100000000);
}

TEST(TestCFile, TestReadWrite) {
  Env *env = Env::Default();

  string path = "/tmp/cfile-TestReadWrite";
  WriteTestFile(path, 10000);

  RandomAccessFile *raf;
  uint64_t size;
  ASSERT_STATUS_OK(env->NewRandomAccessFile(path, &raf));
  ASSERT_STATUS_OK(env->GetFileSize(path, &size));

  shared_ptr<RandomAccessFile> f(raf);

  CFileReader reader(ReaderOptions(), f, size);
  ASSERT_STATUS_OK(reader.Init());

  BlockPointer ptr;


  // TODO: get rid of this SearchPosition stuff, since it is now
  // handled by the positional iterator
  for (int i = 0; i < 10000; i++) {
    // Lookup the data block
    uint32_t match;
    ASSERT_STATUS_OK(reader.SearchPosition(i, &ptr, &match));
    BlockData dblk_data;
    ASSERT_STATUS_OK(reader.ReadBlock(ptr, &dblk_data));
  }

  
  CFileIterator *iter_ptr;
  ASSERT_STATUS_OK( reader.NewIteratorByPos(&iter_ptr) );
  scoped_ptr<CFileIterator> iter(iter_ptr);


  ASSERT_STATUS_OK(iter->SeekToOrdinal(5000));
  ASSERT_EQ(5000u, iter->GetCurrentOrdinal());

  // Seek to last key exactly, should succeed
  ASSERT_STATUS_OK(iter->SeekToOrdinal(9999));
  ASSERT_EQ(9999u, iter->GetCurrentOrdinal());

  // Seek to after last key. Should result in not found.
  ASSERT_TRUE(iter->SeekToOrdinal(10000).IsNotFound());

  // Seek to start of file
  ASSERT_STATUS_OK(iter->SeekToOrdinal(0));
  ASSERT_EQ(0u, iter->GetCurrentOrdinal());
}


} // namespace cfile
} // namespace kudu

int main(int argc, char **argv) {
  google::InstallFailureSignalHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
