// Copyright (c) 2013, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/delta_compaction.h"
#include "kudu/tablet/delta_iterator_merger.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/algorithm.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/auto_release_pool.h"

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

 virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    SeedRandom();
  }

 protected:
  int64_t deltafile_idx_;
  Schema schema_;
};

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

  vector<shared_ptr<DeltaStore> > inputs;

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
        int col_id = schema.column_id(col_idx);
        DCHECK_GE(col_id, 0);

        stats.IncrUpdateCount(col_idx, 1);
        const ColumnSchema& col_schema = schema.column(col_idx);
        int update_value = deltafile_idx * 100 + i;
        switch (col_schema.type_info()->type()) {
          case UINT32:
            {
              uint32_t u32_val = update_value;
              update.AddColumnUpdate(col_id, &u32_val);
            }
            break;
          case STRING:
            {
              string s = boost::lexical_cast<string>(update_value);
              Slice str_val(s);
              update.AddColumnUpdate(col_id, &str_val);
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
      RowChangeList row_changes = update.as_changelist();
      ASSERT_STATUS_OK(dfw->AppendDelta<REDO>(key, row_changes));
      ASSERT_STATUS_OK(stats.UpdateStats(key.timestamp(), schema, row_changes));
      curr_timestamp++;
      row_id++;
    }

    ASSERT_STATUS_OK(dfw->WriteDeltaStats(stats));
    ASSERT_STATUS_OK(dfw->Finish());
    shared_ptr<RandomAccessFile> reader;
    env_util::OpenFileForRandom(env_.get(), path, &reader);
    shared_ptr<DeltaFileReader> delta_reader;
    BlockId block_id;
    ASSERT_STATUS_OK(DeltaFileReader::Open(reader, block_id,
                                           &delta_reader, REDO));
    inputs.push_back(delta_reader);
    deltafile_idx++;
  }

  // Merge
  MvccSnapshot snap(MvccSnapshot::CreateSnapshotIncludingAllTransactions());
  const Schema& merge_schema = schemas.back();
  shared_ptr<DeltaIterator> merge_iter(DeltaIteratorMerger::Create(inputs,
                                                                   &merge_schema,
                                                                   snap));
  string path = GetDeltaFilePath(deltafile_idx);

  gscoped_ptr<DeltaFileWriter> dfw;
  ASSERT_STATUS_OK(GetDeltaFileWriter(path,merge_schema, &dfw));
  ASSERT_STATUS_OK(WriteDeltaIteratorToFile<REDO>(merge_iter.get(),
                                                  merge_schema,
                                                  ITERATE_OVER_ALL_ROWS,
                                                  dfw.get()));
  ASSERT_STATUS_OK(dfw->Finish());

  shared_ptr<RandomAccessFile> reader;
  env_util::OpenFileForRandom(env_.get(), path, &reader);
  shared_ptr<DeltaFileReader> delta_reader;
  BlockId block_id;
  ASSERT_STATUS_OK(DeltaFileReader::Open(reader, block_id,
                                         &delta_reader, REDO));
  DeltaIterator* raw_iter;
  ASSERT_STATUS_OK(delta_reader->NewDeltaIterator(&merge_schema, snap, &raw_iter));
  gscoped_ptr<DeltaIterator> scoped_iter(raw_iter);

  vector<string> results;
  ASSERT_STATUS_OK(DebugDumpDeltaIterator(REDO, scoped_iter.get(), merge_schema,
                                          ITERATE_OVER_ALL_ROWS, &results));
  BOOST_FOREACH(const string &str, results) {
    VLOG(1) << str;
  }
  ASSERT_TRUE(is_sorted(results.begin(), results.end()));
}

} // namespace tablet
} // namespace kudu
