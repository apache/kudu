// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "common/schema.h"
#include "tablet/deltafile.h"
#include "tablet/delta_compaction.h"
#include "gutil/strings/util.h"
#include "gutil/algorithm.h"
#include "util/test_macros.h"
#include "util/stopwatch.h"
#include "util/test_util.h"
#include "util/env.h"
#include "util/env_util.h"
#include "util/path_util.h"
#include "util/status.h"
#include "util/auto_release_pool.h"

DEFINE_int32(num_rows, 2100, "the first row to update");
DEFINE_int32(num_delta_files, 3, "number of delta files");

namespace kudu {
namespace tablet {

using std::string;
using std::vector;
using util::gtl::is_sorted;

class TestDeltaCompaction : public KuduTest {
 public:
  TestDeltaCompaction()
      : deltafile_idx_(0),
        schema_(CreateSchema()) {
  }

  static Schema CreateSchema() {
    SchemaBuilder builder;
    CHECK_OK(builder.AddColumn("val", UINT32));
    return builder.Build();
  }

  string GetDeltaFilePath(int64_t deltafile_idx) {
    return GetTestPath(StringPrintf("%08ld", deltafile_idx));
  }

  Status GetDeltaFileWriter(string path, const Schema& schema,
                            gscoped_ptr<DeltaFileWriter> *dfw) const {
    shared_ptr<WritableFile> file;
    RETURN_NOT_OK(env_util::OpenFileForWrite(env_.get(), path, &file));
    dfw->reset(new DeltaFileWriter(schema, file));
    RETURN_NOT_OK((*dfw)->Start());
    return Status::OK();
  }

  Status OpenAsCompactionInput(const string& path,
                               const Schema& projection,
                               gscoped_ptr<DeltaCompactionInput> *dci) {
    BlockId block_id(BaseName(path));
    shared_ptr<DeltaFileReader> reader;
    RETURN_NOT_OK(DeltaFileReader::Open(env_.get(), path, block_id, &reader, REDO));
    CHECK_EQ(block_id, reader->block_id());
    RETURN_NOT_OK(DeltaCompactionInput::Open(reader, projection, dci));
    deltafile_idx_++;
    return Status::OK();
  }

  Status FillDeltaFile(rowid_t first_row, int nrows, uint64_t timestamp_min,
                       gscoped_ptr<DeltaCompactionInput> *dci) {
    int limit = first_row + nrows;
    string path = GetDeltaFilePath(deltafile_idx_);
    gscoped_ptr<DeltaFileWriter> dfw;
    RETURN_NOT_OK(GetDeltaFileWriter(path, schema_, &dfw));

    faststring buf;

    int64_t num_updates = 0;
    for (int i = first_row; i < limit; i++) {
      buf.clear();
      RowChangeListEncoder update(&schema_, &buf);
      uint32_t new_val = i;
      update.AddColumnUpdate(0, &new_val);
      int num_txns = random() % 3;
      for (int j = 0, curr_timestamp = timestamp_min; j < num_txns; j++) {
        DeltaKey key(i, Timestamp(curr_timestamp));
        RETURN_NOT_OK(dfw->AppendDelta<REDO>(key, RowChangeList(buf)));
        curr_timestamp++;
        num_updates++;
      }
    }
    DeltaStats stats(schema_.num_columns());
    stats.IncrUpdateCount(0, num_updates);
    RETURN_NOT_OK(dfw->WriteDeltaStats(stats));
    RETURN_NOT_OK(dfw->Finish());
    RETURN_NOT_OK(OpenAsCompactionInput(path, schema_, dci));
    return Status::OK();
  }

  Status CreateMergedDeltaCompactionInput(gscoped_ptr<DeltaCompactionInput> *merged) {
    vector<shared_ptr<DeltaCompactionInput> > inputs;
    int min_timestamp = 0;
    for (int i = 0; i < FLAGS_num_delta_files; i++) {
      gscoped_ptr<DeltaCompactionInput> input;
      RETURN_NOT_OK(FillDeltaFile(0, FLAGS_num_rows, min_timestamp, &input));
      inputs.push_back(shared_ptr<DeltaCompactionInput>(input.release()));
      min_timestamp += 2;
    }
    merged->reset(DeltaCompactionInput::Merge(schema_, inputs));
    return Status::OK();
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    SeedRandom();
  }

 protected:
  int64_t deltafile_idx_;
  Schema schema_;
};

TEST_F(TestDeltaCompaction, TestDeltaFileCompactionInput) {
  gscoped_ptr<DeltaCompactionInput> input;
  ASSERT_STATUS_OK(FillDeltaFile(0, FLAGS_num_rows, 0, &input));
  vector<string> results;
  ASSERT_STATUS_OK(DebugDumpDeltaCompactionInput(input.get(), &results, schema_));
  BOOST_FOREACH(const string &str, results) {
    VLOG(1) << str;
  }
  ASSERT_TRUE(is_sorted(results.begin(), results.end()));
}

TEST_F(TestDeltaCompaction, TestMerge) {
  gscoped_ptr<DeltaCompactionInput> merged;
  ASSERT_STATUS_OK(CreateMergedDeltaCompactionInput(&merged));
  vector<string> results;
  ASSERT_STATUS_OK(DebugDumpDeltaCompactionInput(merged.get(), &results, schema_));
  BOOST_FOREACH(const string &str, results) {
    VLOG(1) << str;
  }
  ASSERT_TRUE(is_sorted(results.begin(), results.end()));
}

TEST_F(TestDeltaCompaction, TestFlushDeltaCompactionInput) {
  gscoped_ptr<DeltaCompactionInput> merged;
  ASSERT_STATUS_OK(CreateMergedDeltaCompactionInput(&merged));
  string path = GetDeltaFilePath(FLAGS_num_delta_files + 1);
  gscoped_ptr<DeltaFileWriter> dfw;
  ASSERT_STATUS_OK(GetDeltaFileWriter(path, schema_, &dfw));
  ASSERT_STATUS_OK(FlushDeltaCompactionInput(merged.get(), dfw.get()));
  ASSERT_STATUS_OK(dfw->Finish());
  gscoped_ptr<DeltaCompactionInput> dci;
  ASSERT_STATUS_OK(OpenAsCompactionInput(path, schema_, &dci));
  vector<string> results;
  ASSERT_STATUS_OK(DebugDumpDeltaCompactionInput(dci.get(), &results, schema_));
  BOOST_FOREACH(const string &str, results) {
    VLOG(1) << str;
  }
  ASSERT_TRUE(is_sorted(results.begin(), results.end()));
}

TEST_F(TestDeltaCompaction, TestMergeMultipleSchemas) {
  vector<Schema> schemas;
  SchemaBuilder builder(schema_);
  schemas.push_back(builder.Build());

  // Add an int column with default
  uint32_t default_c2 = 10;
  ASSERT_STATUS_OK(builder.AddColumn("c2", UINT32, false, &default_c2, &default_c2));
  schemas.push_back(builder.Build());

  // add a string column with default
  Slice default_c3("Hello World");
  ASSERT_STATUS_OK(builder.AddColumn("c3", STRING, false, &default_c3, &default_c3));
  schemas.push_back(builder.Build());

  vector<shared_ptr<DeltaCompactionInput> > inputs;

  faststring buf;
  int row_id = 0;
  int curr_timestamp = 0;
  int deltafile_idx = 0;
  BOOST_FOREACH(const Schema& schema, schemas) {
    // Write the Deltas
    string path = GetDeltaFilePath(deltafile_idx);
    gscoped_ptr<DeltaFileWriter> dfw;
    ASSERT_STATUS_OK(GetDeltaFileWriter(path, schema, &dfw));

    // Generate N updates with the new schema, some of them are on existing
    // rows others are on new rows (see kNumUpdates and kNumMultipleUpdates).
    // Each column will be updated with value composed by delta file id
    // and update number (see update_value assignment).
    size_t kNumUpdates = 10;
    size_t kNumMultipleUpdates = kNumUpdates / 2;
    DeltaStats stats(schema.num_columns());
    for (size_t i = 0; i < kNumUpdates; ++i) {
      buf.clear();
      RowChangeListEncoder update(&schema, &buf);
      for (size_t col_idx = schema.num_key_columns(); col_idx < schema.num_columns(); ++col_idx) {
        stats.IncrUpdateCount(col_idx, 1);
        const ColumnSchema& col_schema = schema.column(col_idx);
        int update_value = deltafile_idx * 100 + i;
        switch (col_schema.type_info()->type()) {
          case UINT32:
            {
              uint32_t u32_val = update_value;
              update.AddColumnUpdate(col_idx, &u32_val);
            }
            break;
          case STRING:
            {
              string s = boost::lexical_cast<string>(update_value);
              Slice str_val(s);
              update.AddColumnUpdate(col_idx, &str_val);
            }
            break;
          default:
            FAIL() << "Type " << DataType_Name(col_schema.type_info()->type()) << " Not Supported";
            break;
        }
      }

      // To simulate multiple updates on the same row, the first N updates
      // of this new schema will always be on rows [0, 1, 2, ...] while the
      // others will be on new rows. (N is tunable by changing kNumMultipleUpdates)
      DeltaKey key((i < kNumMultipleUpdates) ? i : row_id, Timestamp(curr_timestamp));
      ASSERT_STATUS_OK(dfw->AppendDelta<REDO>(key, update.as_changelist()));
      curr_timestamp++;
      row_id++;
    }

    ASSERT_STATUS_OK(dfw->WriteDeltaStats(stats));
    ASSERT_STATUS_OK(dfw->Finish());
    gscoped_ptr<DeltaCompactionInput> dci;
    ASSERT_STATUS_OK(OpenAsCompactionInput(path, schemas.back(), &dci));
    inputs.push_back(shared_ptr<DeltaCompactionInput>(dci.release()));
    deltafile_idx++;
  }

  // Merge
  gscoped_ptr<DeltaCompactionInput> merged(DeltaCompactionInput::Merge(schemas.back(), inputs));
  string path = GetDeltaFilePath(deltafile_idx);

  gscoped_ptr<DeltaFileWriter> dfw;
  ASSERT_STATUS_OK(GetDeltaFileWriter(path, merged->schema(), &dfw));
  ASSERT_STATUS_OK(FlushDeltaCompactionInput(merged.get(), dfw.get()));
  ASSERT_STATUS_OK(dfw->Finish());

  gscoped_ptr<DeltaCompactionInput> dci;
  ASSERT_STATUS_OK(OpenAsCompactionInput(path, merged->schema(), &dci));

  vector<string> results;
  ASSERT_STATUS_OK(DebugDumpDeltaCompactionInput(dci.get(), &results, schemas.back()));
  BOOST_FOREACH(const string &str, results) {
    VLOG(1) << str; // TODO: Verify output
  }
  ASSERT_TRUE(is_sorted(results.begin(), results.end()));
}

} // namespace tablet
} // namespace kudu
