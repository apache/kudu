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
#include "common/scan_spec.h"
#include "common/schema.h"
#include "gutil/stringprintf.h"
#include "tablet/diskrowset.h"
#include "util/env.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"
#include "util/test_util.h"

DEFINE_int32(roundtrip_num_rows, 10000,
             "Number of rows to use for the round-trip test");
DEFINE_int32(n_read_passes, 10,
             "number of times to read data for perf test");

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;

class TestRowSet : public KuduTest {
public:
  TestRowSet() :
    KuduTest(),
    schema_(CreateTestSchema()),
    n_rows_(FLAGS_roundtrip_num_rows)
  {
    CHECK_GT(n_rows_, 0);
  }

  virtual void SetUp() {
    KuduTest::SetUp();
    rowset_dir_ = GetTestPath("rowset");
  }

protected:
  static Schema CreateTestSchema() {
    ColumnSchema col1("key", STRING);
    ColumnSchema col2("val", UINT32);

    vector<ColumnSchema> cols = boost::assign::list_of
      (col1)(col2);
    return Schema(cols, 1);
  }


  // Write out a test rowset with n_rows_ rows.
  // The data in the rowset looks like:
  //   ("hello <00n>", <n>)
  // ... where n is the index of the row in the rowset
  // The string values are padded out to 15 digits
  void WriteTestRowSet(int n_rows = 0) {
    if (n_rows == 0) {
      n_rows = n_rows_;
    }

    // Write rows into a new DiskRowSet.
    LOG_TIMING(INFO, "Writing rowset") {
      DiskRowSetWriter drsw(env_.get(), schema_, rowset_dir_,
                     BloomFilterSizing::BySizeAndFPRate(32*1024, 0.01f));

      ASSERT_STATUS_OK(drsw.Open());

      char buf[256];
      RowBuilder rb(schema_);
      for (int i = 0; i < n_rows; i++) {
        rb.Reset();
        FormatKey(i, buf, sizeof(buf));
        rb.AddString(Slice(buf));
        rb.AddUint32(i);
        ASSERT_STATUS_OK_FAST(drsw.WriteRow(rb.data()));
      }
      ASSERT_STATUS_OK(drsw.Finish());
    }
  }

  // Picks some number of rows from the given rowset and updates
  // them. Stores the indexes of the updated rows in *updated.
  void UpdateExistingRows(DiskRowSet *rs, float update_ratio,
                          unordered_set<uint32_t> *updated) {
    int to_update = (int)(n_rows_ * update_ratio);
    char buf[256];
    faststring update_buf;
    RowChangeListEncoder update(schema_, &update_buf);
    for (int i = 0; i < to_update; i++) {
      ScopedTransaction tx(&mvcc_);
      uint32_t idx_to_update = random() % n_rows_;
      FormatKey(idx_to_update, buf, sizeof(buf));
      Slice key_slice(buf);
      uint32_t new_val = idx_to_update * 5;
      update.Reset();
      update.AddColumnUpdate(1, &new_val);
      ASSERT_STATUS_OK_FAST(rs->MutateRow(tx.txid(),
                              &key_slice, RowChangeList(update_buf)));
      if (updated != NULL) {
        updated->insert(idx_to_update);
      }
    }
  }

  // Delete the row with the given identifier.
  Status DeleteRow(DiskRowSet *rs, uint32_t row_idx) {
    char buf[256];
    faststring update_buf;
    RowChangeListEncoder update(schema_, &update_buf);

    ScopedTransaction tx(&mvcc_);
    FormatKey(row_idx, buf, sizeof(buf));
    Slice key_slice(buf);
    update.Reset();
    update.SetToDelete();
    return rs->MutateRow(tx.txid(),
                         &key_slice, RowChangeList(update_buf));
  }

  // Verify the contents of the given rowset.
  // Updated rows (those whose index is present in 'updated') should have
  // a 'val' column equal to idx*5.
  // Other rows should have val column equal to idx.
  void VerifyUpdates(const DiskRowSet &rs, const unordered_set<uint32_t> &updated) {
    LOG_TIMING(INFO, "Reading updated rows with row iter") {
      VerifyUpdatesWithRowIter(rs, updated);
    }
  }

  void VerifyUpdatesWithRowIter(const DiskRowSet &rs,
                                const unordered_set<uint32_t> &updated) {
    Schema proj_val(boost::assign::list_of
                    (ColumnSchema("val", UINT32)),
                    1);
    MvccSnapshot snap = MvccSnapshot::CreateSnapshotIncludingAllTransactions();
    gscoped_ptr<RowwiseIterator> row_iter(rs.NewRowIterator(proj_val, snap));
    ASSERT_STATUS_OK(row_iter->Init(NULL));
    Arena arena(1024, 1024*1024);
    int batch_size = 10000;
    RowBlock dst(proj_val, batch_size, &arena);


    int i = 0;
    while (row_iter->HasNext()) {
      arena.Reset();
      size_t n = batch_size;
      ASSERT_STATUS_OK_FAST(row_iter->PrepareBatch(&n));
      ASSERT_STATUS_OK_FAST(row_iter->MaterializeBlock(&dst));
      ASSERT_STATUS_OK_FAST(row_iter->FinishBatch());
      VerifyUpdatedBlock(proj_val.ExtractColumnFromRow<UINT32>(dst.row(0), 0), i, n, updated);
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

  // Iterate over a DiskRowSet, dumping occasional rows to the console,
  // using the given schema as a projection.
  static void IterateProjection(const DiskRowSet &rs, const Schema &schema,
                                int expected_rows, bool do_log = true) {
    MvccSnapshot snap = MvccSnapshot::CreateSnapshotIncludingAllTransactions();
    gscoped_ptr<RowwiseIterator> row_iter(rs.NewRowIterator(schema, snap));
    ASSERT_STATUS_OK(row_iter->Init(NULL));

    int batch_size = 1000;
    Arena arena(1024, 1024*1024);
    RowBlock dst(schema, batch_size, &arena);

    int i = 0;
    int log_interval = expected_rows/20 / batch_size;
    while (row_iter->HasNext()) {
      arena.Reset();
      size_t n = batch_size;
      ASSERT_STATUS_OK_FAST(row_iter->PrepareBatch(&n));
      ASSERT_STATUS_OK_FAST(row_iter->MaterializeBlock(&dst));
      ASSERT_STATUS_OK_FAST(row_iter->FinishBatch());
      i += n;

      if (do_log) {
        LOG_EVERY_N(INFO, log_interval) << "Got row: " << schema.DebugRow(dst.row(0));
      }
    }

    EXPECT_EQ(expected_rows, i);
  }

  void BenchmarkIterationPerformance(const DiskRowSet &rs,
                                     const string &log_message) {
    Schema proj_val(boost::assign::list_of
                    (ColumnSchema("val", UINT32)),
                    1);
    LOG_TIMING(INFO, log_message + " (val column only)") {
      for (int i = 0; i < FLAGS_n_read_passes; i++) {
        IterateProjection(rs, proj_val, n_rows_, false);
      }
    }

    Schema proj_key(boost::assign::list_of
                    (ColumnSchema("key", STRING)),
                    1);
    LOG_TIMING(INFO, log_message + " (key string column only)") {
      for (int i = 0; i < FLAGS_n_read_passes; i++) {
        IterateProjection(rs, proj_key, n_rows_, false);
      }
    }

    LOG_TIMING(INFO, log_message + " (both columns)") {
      for (int i = 0; i < FLAGS_n_read_passes; i++) {
        IterateProjection(rs, schema_, n_rows_, false);
      }
    }
  }

  Status OpenTestRowSet(shared_ptr<DiskRowSet> *rowset) {
    return DiskRowSet::Open(env_.get(), schema_, rowset_dir_, rowset);
  }



  void FormatKey(int i, char *buf, size_t buf_len) {
    snprintf(buf, buf_len, "hello %015d", i);
  }

  Schema schema_;
  size_t n_rows_;
  string rowset_dir_;

  MvccManager mvcc_;
};


} // namespace tablet
} // namespace kudu

#endif
