// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <tr1/memory>

#include "common/schema.h"
#include "tablet/deltafile.h"
#include "util/env.h"
#include "util/env_util.h"
#include "util/memenv/memenv.h"
#include "util/test_macros.h"

DECLARE_int32(deltafile_block_size);
DEFINE_int32(first_row_to_update, 10000, "the first row to update");
DEFINE_int32(last_row_to_update, 100000, "the last row to update");
DEFINE_int32(n_verify, 1, "number of times to verify the updates"
             "(useful for benchmarks");

namespace kudu {
namespace tablet {

using std::tr1::shared_ptr;

// Test path to write delta file to (in in-memory environment)
const string kTestPath = "/tmp/test";

class TestDeltaFile : public ::testing::Test {
public:
  TestDeltaFile() :
    env_(NewMemEnv(Env::Default())),
    schema_(boost::assign::list_of
            (ColumnSchema("val", UINT32)),
            1),
    arena_(1024, 1024)
  {}

  void WriteTestFile() {
    shared_ptr<WritableFile> file;
    ASSERT_STATUS_OK( env_util::OpenFileForWrite(env_.get(), kTestPath, &file) );

    DeltaFileWriter dfw(schema_, file);
    ASSERT_STATUS_OK(dfw.Start());

    // Update even numbered rows.
    faststring buf;

    for (int i = FLAGS_first_row_to_update; i <= FLAGS_last_row_to_update; i += 2) {
      buf.clear();
      RowChangeListEncoder update(schema_, &buf);
      uint32_t new_val = i;
      update.AddColumnUpdate(0, &new_val);

      ASSERT_STATUS_OK_FAST(dfw.AppendDelta(i, RowChangeList(buf)));
    }

    ASSERT_STATUS_OK(dfw.Finish());
  }


  void DoTestRoundTrip() {
    // First write the file.
    WriteTestFile();

    // Then iterate back over it, applying deltas to a fake row block.
    for (int i = 0; i < FLAGS_n_verify; i++) {
      VerifyTestFile();
    }
  }

  void VerifyTestFile() {
    gscoped_ptr<DeltaFileReader> reader;
    ASSERT_STATUS_OK(DeltaFileReader::Open(env_.get(), kTestPath, schema_, &reader));

    gscoped_ptr<DeltaIteratorInterface> it(reader->NewDeltaIterator(schema_));
    ASSERT_STATUS_OK(it->Init());

    ScopedRowBlock block(schema_, 100, &arena_);

    // Iterate through the faked table, starting with batches that
    // come before all of the updates, and extending a bit further
    // past the updates, to ensure that nothing breaks on the boundaries.
    ASSERT_STATUS_OK(it->SeekToOrdinal(0));

    int start_row = 0;
    while (start_row < FLAGS_last_row_to_update + 10000) {
      block.ZeroMemory();
      arena_.Reset();

      ASSERT_STATUS_OK_FAST(it->PrepareToApply(&block));
      ASSERT_STATUS_OK_FAST(it->ApplyUpdates(&block, 0));

      for (int i = 0; i < block.nrows(); i++) {
        uint32_t row = start_row + i;
        bool should_be_updated = (row >= FLAGS_first_row_to_update) &&
          (row <= FLAGS_last_row_to_update) &&
          (row % 2 == 0);

        uint32_t updated_val = *schema_.ExtractColumnFromRow<UINT32>(block.row_slice(i), 0);
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
  gscoped_ptr<Env> env_;
  Schema schema_;
  Arena arena_;
};

TEST_F(TestDeltaFile, TestRoundTripTinyDeltaBlocks) {
  // Set block size small, so that we get good coverage
  // of the case where multiple delta blocks correspond to a
  // single underlying data block.
  google::FlagSaver saver;
  FLAGS_deltafile_block_size = 256;
  DoTestRoundTrip();
}

TEST_F(TestDeltaFile, TestRoundTrip) {
  DoTestRoundTrip();
}

} // namespace tablet
} // namespace kudu
