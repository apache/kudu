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

#include "kudu/tablet/deltafile.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cfile/cfile_util.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowid.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs-test-util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/mutation.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(deltafile_default_block_size);
DEFINE_int32(first_row_to_update, 10000, "the first row to update");
DEFINE_int32(last_row_to_update, 100000, "the last row to update");
DEFINE_int32(n_verify, 1, "number of times to verify the updates"
             "(useful for benchmarks");

using std::is_sorted;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

using cfile::ReaderOptions;
using fs::CountingReadableBlock;
using fs::ReadableBlock;
using fs::WritableBlock;

class TestDeltaFile : public KuduTest {
 public:
  TestDeltaFile() :
    schema_(CreateSchema()),
    arena_(1024) {
  }

 public:
  void SetUp() OVERRIDE {
    fs_manager_.reset(new FsManager(env_, GetTestPath("fs")));
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());
  }

  static Schema CreateSchema() {
    SchemaBuilder builder;
    CHECK_OK(builder.AddColumn("val", UINT32));
    return builder.Build();
  }

  void WriteTestFile(int min_timestamp = 0, int max_timestamp = 0) {
    unique_ptr<WritableBlock> block;
    ASSERT_OK(fs_manager_->CreateNewBlock({}, &block));
    test_block_ = block->id();
    DeltaFileWriter dfw(std::move(block));
    ASSERT_OK(dfw.Start());

    // Update even numbered rows.
    faststring buf;

    DeltaStats stats;
    for (int i = FLAGS_first_row_to_update; i <= FLAGS_last_row_to_update; i += 2) {
      for (int timestamp = min_timestamp; timestamp <= max_timestamp; timestamp++) {
        buf.clear();
        RowChangeListEncoder update(&buf);
        uint32_t new_val = timestamp + i;
        update.AddColumnUpdate(schema_.column(0), schema_.column_id(0), &new_val);
        DeltaKey key(i, Timestamp(timestamp));
        RowChangeList rcl(buf);
        ASSERT_OK_FAST(dfw.AppendDelta<REDO>(key, rcl));
        ASSERT_OK_FAST(stats.UpdateStats(key.timestamp(), rcl));
      }
    }
    dfw.WriteDeltaStats(stats);
    ASSERT_OK(dfw.Finish());
  }


  void DoTestRoundTrip() {
    // First write the file.
    WriteTestFile();

    // Then iterate back over it, applying deltas to a fake row block.
    for (int i = 0; i < FLAGS_n_verify; i++) {
      VerifyTestFile();
    }
  }

  Status OpenDeltaFileReader(const BlockId& block_id, shared_ptr<DeltaFileReader>* out) {
    unique_ptr<ReadableBlock> block;
    RETURN_NOT_OK(fs_manager_->OpenBlock(block_id, &block));
    return DeltaFileReader::Open(std::move(block), REDO, ReaderOptions(), out);
  }

  Status OpenDeltaFileIterator(const BlockId& block_id, gscoped_ptr<DeltaIterator>* out) {
    shared_ptr<DeltaFileReader> reader;
    RETURN_NOT_OK(OpenDeltaFileReader(block_id, &reader));
    return OpenDeltaFileIteratorFromReader(REDO, reader, out);
  }

  Status OpenDeltaFileIteratorFromReader(DeltaType type,
                                         const shared_ptr<DeltaFileReader>& reader,
                                         gscoped_ptr<DeltaIterator>* out) {
    RowIteratorOptions opts;
    opts.snap_to_include = type == REDO ?
                MvccSnapshot::CreateSnapshotIncludingAllTransactions() :
                MvccSnapshot::CreateSnapshotIncludingNoTransactions();
    opts.projection = &schema_;
    DeltaIterator* raw_iter;
    RETURN_NOT_OK(reader->NewDeltaIterator(opts, &raw_iter));
    out->reset(raw_iter);
    return Status::OK();
  }

  void VerifyTestFile() {
    shared_ptr<DeltaFileReader> reader;
    ASSERT_OK(OpenDeltaFileReader(test_block_, &reader));
    ASSERT_EQ(((FLAGS_last_row_to_update - FLAGS_first_row_to_update) / 2) + 1,
              reader->delta_stats().update_count_for_col_id(schema_.column_id(0)));
    ASSERT_EQ(0, reader->delta_stats().delete_count());
    gscoped_ptr<DeltaIterator> it;
    Status s = OpenDeltaFileIteratorFromReader(REDO, reader, &it);
    if (s.IsNotFound()) {
      FAIL() << "Iterator fell outside of the range of an include-all snapshot";
    }
    ASSERT_OK(s);
    ASSERT_OK(it->Init(nullptr));

    RowBlock block(schema_, 100, &arena_);

    // Iterate through the faked table, starting with batches that
    // come before all of the updates, and extending a bit further
    // past the updates, to ensure that nothing breaks on the boundaries.
    ASSERT_OK(it->SeekToOrdinal(0));

    int start_row = 0;
    while (start_row < FLAGS_last_row_to_update + 10000) {
      block.ZeroMemory();
      arena_.Reset();

      ASSERT_OK_FAST(it->PrepareBatch(block.nrows(), DeltaIterator::PREPARE_FOR_APPLY));
      ColumnBlock dst_col = block.column_block(0);
      ASSERT_OK_FAST(it->ApplyUpdates(0, &dst_col));

      for (int i = 0; i < block.nrows(); i++) {
        uint32_t row = start_row + i;
        bool should_be_updated = (row >= FLAGS_first_row_to_update) &&
          (row <= FLAGS_last_row_to_update) &&
          (row % 2 == 0);

        DCHECK_EQ(block.row(i).cell_ptr(0), dst_col.cell_ptr(i));
        uint32_t updated_val = *schema_.ExtractColumnFromRow<UINT32>(block.row(i), 0);
        VLOG(2) << "row " << row << ": " << updated_val;
        uint32_t expected_val = should_be_updated ? row : 0;
        // Don't use ASSERT_EQ, since it's slow (records positive results, not just negative)
        if (updated_val != expected_val) {
          FAIL() << "failed on row " << row <<
            ": expected " << expected_val << ", got " << updated_val;
        }
      }

      start_row += block.nrows();
    }
  }

 protected:
  gscoped_ptr<FsManager> fs_manager_;
  Schema schema_;
  Arena arena_;
  BlockId test_block_;
};

TEST_F(TestDeltaFile, TestDumpDeltaFileIterator) {
  WriteTestFile();

  gscoped_ptr<DeltaIterator> it;
  Status s = OpenDeltaFileIterator(test_block_, &it);
  if (s.IsNotFound()) {
    FAIL() << "Iterator fell outside of the range of an include-all snapshot";
  }
  ASSERT_OK(s);
  vector<string> it_contents;
  ASSERT_OK(DebugDumpDeltaIterator(REDO,
                                          it.get(),
                                          schema_,
                                          ITERATE_OVER_ALL_ROWS,
                                          &it_contents));
  for (const string& str : it_contents) {
    VLOG(1) << str;
  }
  ASSERT_TRUE(is_sorted(it_contents.begin(), it_contents.end()));
  ASSERT_EQ(it_contents.size(), (FLAGS_last_row_to_update - FLAGS_first_row_to_update) / 2 + 1);
}

TEST_F(TestDeltaFile, TestWriteDeltaFileIteratorToFile) {
  WriteTestFile();
  gscoped_ptr<DeltaIterator> it;
  Status s = OpenDeltaFileIterator(test_block_, &it);
  if (s.IsNotFound()) {
    FAIL() << "Iterator fell outside of the range of an include-all snapshot";
  }
  ASSERT_OK(s);

  unique_ptr<WritableBlock> block;
  ASSERT_OK(fs_manager_->CreateNewBlock({}, &block));
  BlockId block_id(block->id());
  DeltaFileWriter dfw(std::move(block));
  ASSERT_OK(dfw.Start());
  ASSERT_OK(WriteDeltaIteratorToFile<REDO>(it.get(),
                                           ITERATE_OVER_ALL_ROWS,
                                           &dfw));
  ASSERT_OK(dfw.Finish());


  // If delta stats are incorrect, then a Status::NotFound would be
  // returned.

  ASSERT_OK(OpenDeltaFileIterator(block_id, &it));
  vector<string> it_contents;
  ASSERT_OK(DebugDumpDeltaIterator(REDO,
                                          it.get(),
                                          schema_,
                                          ITERATE_OVER_ALL_ROWS,
                                          &it_contents));
  for (const string& str : it_contents) {
    VLOG(1) << str;
  }
  ASSERT_TRUE(is_sorted(it_contents.begin(), it_contents.end()));
  ASSERT_EQ(it_contents.size(), (FLAGS_last_row_to_update - FLAGS_first_row_to_update) / 2 + 1);
}

TEST_F(TestDeltaFile, TestRoundTripTinyDeltaBlocks) {
  // Set block size small, so that we get good coverage
  // of the case where multiple delta blocks correspond to a
  // single underlying data block.
  google::FlagSaver saver;
  FLAGS_deltafile_default_block_size = 256;
  DoTestRoundTrip();
}

TEST_F(TestDeltaFile, TestRoundTrip) {
  DoTestRoundTrip();
}

TEST_F(TestDeltaFile, TestCollectMutations) {
  WriteTestFile();

  {
    gscoped_ptr<DeltaIterator> it;
    Status s = OpenDeltaFileIterator(test_block_, &it);
    if (s.IsNotFound()) {
      FAIL() << "Iterator fell outside of the range of an include-all snapshot";
    }
    ASSERT_OK(s);

    ASSERT_OK(it->Init(nullptr));
    ASSERT_OK(it->SeekToOrdinal(0));

    vector<Mutation *> mutations;
    mutations.resize(100);

    int start_row = 0;
    while (start_row < FLAGS_last_row_to_update + 10000) {
      std::fill(mutations.begin(), mutations.end(), reinterpret_cast<Mutation *>(NULL));

      arena_.Reset();
      ASSERT_OK_FAST(it->PrepareBatch(mutations.size(), DeltaIterator::PREPARE_FOR_COLLECT));
      ASSERT_OK(it->CollectMutations(&mutations, &arena_));

      for (int i = 0; i < mutations.size(); i++) {
        Mutation *mut_head = mutations[i];
        if (mut_head != nullptr) {
          rowid_t row = start_row + i;
          string str = Mutation::StringifyMutationList(schema_, mut_head);
          VLOG(1) << "Mutation on row " << row << ": " << str;
        }
      }

      start_row += mutations.size();
    }
  }

}

TEST_F(TestDeltaFile, TestSkipsDeltasOutOfRange) {
  WriteTestFile(10, 20);
  shared_ptr<DeltaFileReader> reader;
  ASSERT_OK(OpenDeltaFileReader(test_block_, &reader));

  gscoped_ptr<DeltaIterator> iter;

  RowIteratorOptions opts;
  opts.projection = &schema_;

  // should skip
  opts.snap_to_include = MvccSnapshot(Timestamp(9));
  ASSERT_FALSE(opts.snap_to_include.MayHaveCommittedTransactionsAtOrAfter(Timestamp(10)));
  DeltaIterator* raw_iter = nullptr;
  Status s = reader->NewDeltaIterator(opts, &raw_iter);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_TRUE(raw_iter == nullptr);

  // should include
  raw_iter = nullptr;
  opts.snap_to_include = MvccSnapshot(Timestamp(15));
  ASSERT_OK(reader->NewDeltaIterator(opts, &raw_iter));
  ASSERT_TRUE(raw_iter != nullptr);
  iter.reset(raw_iter);

  // should include
  raw_iter = nullptr;
  opts.snap_to_include = MvccSnapshot(Timestamp(21));
  ASSERT_OK(reader->NewDeltaIterator(opts, &raw_iter));
  ASSERT_TRUE(raw_iter != nullptr);
  iter.reset(raw_iter);
}

TEST_F(TestDeltaFile, TestLazyInit) {
  WriteTestFile();

  // Open it using a "counting" readable block.
  unique_ptr<ReadableBlock> block;
  ASSERT_OK(fs_manager_->OpenBlock(test_block_, &block));
  size_t bytes_read = 0;
  unique_ptr<ReadableBlock> count_block(
      new CountingReadableBlock(std::move(block), &bytes_read));

  // Lazily opening the delta file should not trigger any reads.
  shared_ptr<DeltaFileReader> reader;
  ASSERT_OK(DeltaFileReader::OpenNoInit(
      std::move(count_block), REDO, ReaderOptions(), &reader));
  ASSERT_EQ(0, bytes_read);

  // But initializing it should (only the first time).
  ASSERT_OK(reader->Init(nullptr));
  ASSERT_GT(bytes_read, 0);
  size_t bytes_read_after_init = bytes_read;
  ASSERT_OK(reader->Init(nullptr));
  ASSERT_EQ(bytes_read_after_init, bytes_read);

  // And let's test non-lazy open for good measure; it should yield the
  // same number of bytes read.
  ASSERT_OK(fs_manager_->OpenBlock(test_block_, &block));
  bytes_read = 0;
  count_block.reset(new CountingReadableBlock(std::move(block), &bytes_read));
  ASSERT_OK(DeltaFileReader::Open(
      std::move(count_block), REDO, ReaderOptions(), &reader));
  ASSERT_EQ(bytes_read_after_init, bytes_read);
}

// Check that, if a delta file is opened but no deltas are written,
// Finish() will return Status::Aborted().
TEST_F(TestDeltaFile, TestEmptyFileIsAborted) {
  unique_ptr<WritableBlock> block;
  ASSERT_OK(fs_manager_->CreateNewBlock({}, &block));
  test_block_ = block->id();
  {
    DeltaFileWriter dfw(std::move(block));
    ASSERT_OK(dfw.Start());

    // The block is only deleted when the DeltaFileWriter goes out of scope.
    Status s = dfw.Finish();
    ASSERT_TRUE(s.IsAborted());
  }

  // The block should have been deleted as well.
  unique_ptr<ReadableBlock> rb;
  Status s = fs_manager_->OpenBlock(test_block_, &rb);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

template <typename T>
class DeltaTypeTestDeltaFile : public TestDeltaFile {
};

using MyTypes = ::testing::Types<DeltaTypeSelector<REDO>, DeltaTypeSelector<UNDO>>;
TYPED_TEST_CASE(DeltaTypeTestDeltaFile, MyTypes);

// Generates a series of random deltas,  writes them to a DeltaFile, reads them
// back using a DeltaFileIterator, and verifies the results.
TYPED_TEST(DeltaTypeTestDeltaFile, TestFuzz) {
  // Arbitrary constants to control the running time and coverage of the test.
  const int kNumColumns = 100;
  const int kNumRows = 1000;
  const int kNumDeltas = 10000;
  const std::pair<uint64_t, uint64_t> kTimestampRange(0, 100);

  // Build a schema with kNumColumns columns.
  SchemaBuilder sb;
  for (int i = 0; i < kNumColumns; i++) {
    ASSERT_OK(sb.AddColumn(Substitute("col$0", i), UINT32));
  }
  Schema schema(sb.Build());

  Random r(SeedRandom());
  MirroredDeltas<TypeParam> deltas(&schema);

  shared_ptr<DeltaFileReader> reader;
  ASSERT_OK(CreateRandomDeltaFile<TypeParam>(
      schema, this->fs_manager_.get(), &r,
      kNumDeltas, { 0, kNumRows }, kTimestampRange, &deltas, &reader));

  NO_FATALS(RunDeltaFuzzTest<TypeParam>(
      *reader, &r, &deltas, kTimestampRange,
      /*test_filter_column_ids_and_collect_deltas=*/true));
}

} // namespace tablet
} // namespace kudu
