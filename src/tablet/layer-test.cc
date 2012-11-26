// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <tr1/unordered_set>
#include <time.h>

#include "common/row.h"
#include "common/schema.h"
#include "gutil/stringprintf.h"
#include "tablet/layer.h"
#include "util/env.h"
#include "util/status.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"

DEFINE_int32(roundtrip_num_rows, 10000,
             "Number of rows to use for the round-trip test");

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;

static Schema CreateTestSchema() {
  ColumnSchema col1("key", STRING);
  ColumnSchema col2("val", UINT32);

  vector<ColumnSchema> cols = boost::assign::list_of
    (col1)(col2);
  return Schema(cols, 1);
}


class TestLayer : public ::testing::Test {
public:
  TestLayer() :
    ::testing::Test(),
    schema_(CreateTestSchema()),
    env_(Env::Default()),
    n_rows_(FLAGS_roundtrip_num_rows)
  {
    CHECK_GT(n_rows_, 0);
  }

protected:
  virtual void SetUp() {
    const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();

    ASSERT_STATUS_OK(env_->GetTestDirectory(&test_dir_));

    test_dir_ += StringPrintf(
      "/TestLayer.%s.%ld", test_info->name(), time(NULL));

  }

  // Write out a test layer with n_rows_ rows.
  // The data in the layer looks like:
  //   ("hello <00n>", <n>)
  // ... where n is the index of the row in the layer
  // The string values are padded out to 15 digits
  void WriteTestLayer() {
    // Write rows into a new Layer.
    LOG_TIMING(INFO, "Writing layer") {
      LayerWriter lw(env_, schema_, test_dir_);

      ASSERT_STATUS_OK(lw.Open());

      char buf[256];
      RowBuilder rb(schema_);
      for (int i = 0; i < n_rows_; i++) {
        rb.Reset();
        FormatKey(i, buf, sizeof(buf));
        rb.AddString(Slice(buf));
        rb.AddUint32(i);
        ASSERT_STATUS_OK_FAST(lw.WriteRow(rb.data()));
      }
      ASSERT_STATUS_OK(lw.Finish());
    }
  }

  // Picks some number of rows from the given layer and updates
  // them. Stores the indexes of the updated rows in *updated.
  void UpdateExistingRows(Layer *l, float update_ratio,
                          unordered_set<uint32_t> *updated) {
    int to_update = (int)(n_rows_ * update_ratio);
    char buf[256];
    ScopedRowDelta update(schema_);
    for (int i = 0; i < to_update; i++) {
      uint32_t idx_to_update = random() % n_rows_;
      FormatKey(idx_to_update, buf, sizeof(buf));
      Slice key_slice(buf);
      uint32_t new_val = idx_to_update * 5;
      update.get().UpdateColumn(schema_, 1, &new_val);
      ASSERT_STATUS_OK_FAST(l->UpdateRow(
                              &key_slice, update.get()));
      updated->insert(idx_to_update);
    }
  }

  // Verify the contents of the given layer.
  // Updated rows (those whose index is present in 'updated') should have
  // a 'val' column equal to idx*5.
  // Other rows should have val column equal to idx.
  void VerifyUpdates(const Layer &l, const unordered_set<uint32_t> &updated) {
    LOG_TIMING(INFO, "Reading updated rows with row iter") {
      VerifyUpdatesWithRowIter(l, updated);
    }
    LOG_TIMING(INFO, "Reading updated rows with column iter") {
      VerifyUpdatesWithColIter(l, updated);
    }
  }

  void VerifyUpdatesWithRowIter(const Layer &l,
                                const unordered_set<uint32_t> &updated) {
    Schema proj_val(boost::assign::list_of
                    (ColumnSchema("val", UINT32)),
                    1);
    scoped_ptr<Layer::RowIterator> row_iter(l.NewRowIterator(proj_val));
    ASSERT_STATUS_OK(row_iter->Init());
    ASSERT_STATUS_OK(row_iter->SeekToOrdinal(0));
    Arena arena(1024, 1024*1024);
    int batch_size = 10000;
    uint32_t dst[batch_size];

    int i = 0;
    while (row_iter->HasNext()) {
      arena.Reset();
      size_t n = batch_size;
      ASSERT_STATUS_OK_FAST(
        row_iter->CopyNextRows(
          &n, reinterpret_cast<char *>(dst), &arena));
      VerifyUpdatedBlock(reinterpret_cast<uint32_t *>(dst), i, n, updated);
      i += n;
    }
  }

  template<typename Indexable>
  void VerifyUpdatedBlock(const Indexable &from_file, int start_row, size_t n_rows,
                          const unordered_set<uint32_t> &updated) {
      for (int j = 0; j < n_rows; j++) {
        uint32_t idx_in_file = start_row + j;
        int expected;
        if (updated.count(idx_in_file) > 0) {
          expected = idx_in_file * 5;
        } else {
          expected = idx_in_file;
        }

        if (from_file[j] != expected) {
          FAIL() << "Incorrect value at idx " << idx_in_file
                 << ": expected=" << expected << " got=" << from_file[j];
        }
      }
  }

  // Verify the contents of the given layer.
  // Identical to the above, except uses the column iterator instead of
  // row iterator.
  void VerifyUpdatesWithColIter(const Layer &l,
                                const unordered_set<uint32_t> &updated) {
    Schema proj_val(boost::assign::list_of
                    (ColumnSchema("val", UINT32)),
                    1);
    scoped_ptr<Layer::ColumnIterator> col_iter;
    ASSERT_STATUS_OK(l.NewColumnIterator(1, &col_iter));
    ASSERT_STATUS_OK(col_iter->SeekToOrdinal(0));

    int batch_size = 10000;

    ScopedColumnBlock<UINT32> dst(batch_size);

    int i = 0;
    while (col_iter->HasNext()) {
      size_t n = batch_size;
      ASSERT_STATUS_OK_FAST(col_iter->CopyNextValues(&n, &dst));
      VerifyUpdatedBlock(dst, i, n, updated);
      i += n;
    }
  }

  void FormatKey(int i, char *buf, size_t buf_len) {
    snprintf(buf, buf_len, "hello %015d", i);
  }

  Schema schema_;
  string test_dir_;
  Env *env_;
  size_t n_rows_;
};

// Iterate over a Layer, dumping occasional rows to the console,
// using the given schema as a projection.
static void IterateProjection(const Layer &l, const Schema &schema,
                              int expected_rows) {
  scoped_ptr<Layer::RowIterator> row_iter(l.NewRowIterator(schema));
  ASSERT_STATUS_OK(row_iter->Init());
  ASSERT_STATUS_OK(row_iter->SeekToOrdinal(0));

  int batch_size = 100;
  Arena arena(1024, 1024*1024);
  char dst[schema.byte_size() * batch_size];

  int i = 0;
  int log_interval = expected_rows/20 / batch_size;
  while (row_iter->HasNext()) {
    arena.Reset();
    size_t n = batch_size;
    ASSERT_STATUS_OK_FAST(row_iter->CopyNextRows(&n, dst, &arena));
    i += n;

    LOG_EVERY_N(INFO, log_interval) << "Got row: " <<
      schema.DebugRow(dst);
  }

  EXPECT_EQ(expected_rows, i);

}


// TODO: add test which calls CopyNextRows on an iterator with no more
// rows - i think it segfaults!

// Test round-trip writing and reading back a layer with
// multiple columns. Does not test any modifications.
TEST_F(TestLayer, TestLayerRoundTrip) {
  WriteTestLayer();

  // Now open the Layer for read
  Layer l(env_, schema_, test_dir_);
  ASSERT_STATUS_OK(l.Open());

  // First iterate over all columns
  LOG_TIMING(INFO, "Iterating over all columns") {
    IterateProjection(l, schema_, n_rows_);
  }

  // Now iterate only over the key column
  Schema proj_key(boost::assign::list_of
                  (ColumnSchema("key", STRING)),
                  1);

  LOG_TIMING(INFO, "Iterating over only key column") {
    IterateProjection(l, proj_key, n_rows_);
  }


  // Now iterate only over the non-key column
  Schema proj_val(boost::assign::list_of
                  (ColumnSchema("val", UINT32)),
                  1);
  LOG_TIMING(INFO, "Iterating over only val column") {
    IterateProjection(l, proj_val, n_rows_);
  }

  // Test that CheckRowPresent returns correct results

  // 1. Check a key which comes before all keys in layer
  Slice key("h");
  bool present;
  ASSERT_STATUS_OK(l.CheckRowPresent(&key, &present));
  ASSERT_FALSE(present);

  // 2. Check a key which comes after all keys in layer
  key = "z";
  ASSERT_STATUS_OK(l.CheckRowPresent(&key, &present));
  ASSERT_FALSE(present);

  // 3. Check a key which is not present, but comes between present
  // keys
  key = "hello 00000000000049x";
  ASSERT_STATUS_OK(l.CheckRowPresent(&key, &present));
  ASSERT_FALSE(present);

  // 4. Check a key which is present
  char buf[256];
  FormatKey(49, buf, sizeof(buf));
  key = buf;
  ASSERT_STATUS_OK(l.CheckRowPresent(&key, &present));
  ASSERT_TRUE(present);

}

// Test writing a layer, and then updating some rows in it.
TEST_F(TestLayer, TestLayerUpdate) {
  WriteTestLayer();

  // Now open the Layer for read
  Layer l(env_, schema_, test_dir_);
  ASSERT_STATUS_OK(l.Open());

  // Add an update to the delta tracker for a number of keys
  // which exist. These updates will change the value to
  // equal idx*5 (whereas in the original data, value = idx)
  unordered_set<uint32_t> updated;
  UpdateExistingRows(&l, 0.1f, &updated);
  ASSERT_EQ(updated.size(), l.dms_->Count());

  // Try to add an update for a key not in the file (but which falls
  // between two valid keys)
  ScopedRowDelta update(schema_);
  Slice bad_key = Slice("hello 00000000000049x");
  ASSERT_TRUE(l.UpdateRow(&bad_key, update.get()).IsNotFound());

  // Now read back the value column, and verify that the updates
  // are visible.
  VerifyUpdates(l, updated);
}

TEST_F(TestLayer, TestDMSFlush) {
  WriteTestLayer();

  // Now open the Layer for read
  Layer l(env_, schema_, test_dir_);
  ASSERT_STATUS_OK(l.Open());

  // Add an update to the delta tracker for a number of keys
  // which exist. These updates will change the value to
  // equal idx*5 (whereas in the original data, value = idx)
  unordered_set<uint32_t> updated;
  UpdateExistingRows(&l, 0.01f, &updated);
  ASSERT_EQ(updated.size(), l.dms_->Count());

  l.FlushDeltas();

  // Check that the Layer's DMS has now been emptied.
  ASSERT_EQ(0, l.dms_->Count());

  // Now read back the value column, and verify that the updates
  // are visible.
  VerifyUpdates(l, updated);
}

} // namespace tablet
} // namespace kudu

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
