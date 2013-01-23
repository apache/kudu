// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_LAYER_TEST_BASE_H
#define KUDU_TABLET_LAYER_TEST_BASE_H

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <tr1/unordered_set>
#include <unistd.h>

#include "common/iterator.h"
#include "common/rowblock.h"
#include "common/schema.h"
#include "gutil/stringprintf.h"
#include "tablet/layer.h"
#include "util/env.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"

DEFINE_int32(roundtrip_num_rows, 10000,
             "Number of rows to use for the round-trip test");

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;

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
  static Schema CreateTestSchema() {
    ColumnSchema col1("key", STRING);
    ColumnSchema col2("val", UINT32);

    vector<ColumnSchema> cols = boost::assign::list_of
      (col1)(col2);
    return Schema(cols, 1);
  }

  virtual void SetUp() {
    const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();

    ASSERT_STATUS_OK(env_->GetTestDirectory(&test_dir_));

    test_dir_ += StringPrintf(
      "/TestLayer.%s.%d.%ld", test_info->name(), getpid(), time(NULL));

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
  }

  void VerifyUpdatesWithRowIter(const Layer &l,
                                const unordered_set<uint32_t> &updated) {
    Schema proj_val(boost::assign::list_of
                    (ColumnSchema("val", UINT32)),
                    1);
    scoped_ptr<RowIteratorInterface> row_iter(l.NewRowIterator(proj_val));
    ASSERT_STATUS_OK(row_iter->Init());
    ASSERT_STATUS_OK(row_iter->SeekToStart());
    Arena arena(1024, 1024*1024);
    int batch_size = 10000;
    ScopedRowBlock dst(proj_val, batch_size, &arena);


    int i = 0;
    while (row_iter->HasNext()) {
      arena.Reset();
      size_t n = batch_size;
      ASSERT_STATUS_OK_FAST(row_iter->CopyNextRows(&n, &dst));
      VerifyUpdatedBlock(reinterpret_cast<const uint32_t *>(dst.row_ptr(0)),
                         i, n, updated);
      i += n;
    }
  }

  void VerifyUpdatedBlock(const uint32_t *from_file, int start_row, size_t n_rows,
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

  // Iterate over a Layer, dumping occasional rows to the console,
  // using the given schema as a projection.
  static void IterateProjection(const Layer &l, const Schema &schema,
                                int expected_rows) {
    scoped_ptr<RowIteratorInterface> row_iter(l.NewRowIterator(schema));
    ASSERT_STATUS_OK(row_iter->Init());
    ASSERT_STATUS_OK(row_iter->SeekToStart());

    int batch_size = 100;
    Arena arena(1024, 1024*1024);
    ScopedRowBlock dst(schema, batch_size, &arena);

    int i = 0;
    int log_interval = expected_rows/20 / batch_size;
    while (row_iter->HasNext()) {
      arena.Reset();
      size_t n = batch_size;
      ASSERT_STATUS_OK_FAST(row_iter->CopyNextRows(&n, &dst));
      i += n;

      LOG_EVERY_N(INFO, log_interval) << "Got row: " <<
        schema.DebugRow(dst.row_ptr(0));
    }

    EXPECT_EQ(expected_rows, i);
  }

  Status OpenTestLayer(shared_ptr<Layer> *layer) {
    Layer *tmp;
    RETURN_NOT_OK(Layer::Open(env_, schema_, test_dir_, &tmp));
    layer->reset(tmp);
    return Status::OK();
  }



  void FormatKey(int i, char *buf, size_t buf_len) {
    snprintf(buf, buf_len, "hello %015d", i);
  }

  Schema schema_;
  string test_dir_;
  Env *env_;
  size_t n_rows_;
};


} // namespace tablet
} // namespace kudu

#endif
