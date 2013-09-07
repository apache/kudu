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
        schema_(boost::assign::list_of
                (ColumnSchema("val", UINT32)),
                1) {
  }

  string GetNextDeltaFilePath() {
    return GetTestPath(StringPrintf("%d", deltafile_idx_++));
  }

  Status GetDeltaFileWriter(string path,
                            gscoped_ptr<DeltaFileWriter> *dfw) const {
    shared_ptr<WritableFile> file;
    RETURN_NOT_OK(env_util::OpenFileForWrite(env_.get(), path, &file));
    dfw->reset(new DeltaFileWriter(schema_, file));
    RETURN_NOT_OK((*dfw)->Start());
    return Status::OK();
  }

  Status FillDeltaFile(rowid_t first_row, int nrows, uint64_t txid_min,
                       gscoped_ptr<DeltaCompactionInput> *dci) {
    int limit = first_row + nrows;
    string path = GetNextDeltaFilePath();
    gscoped_ptr<DeltaFileWriter> dfw;
    RETURN_NOT_OK(GetDeltaFileWriter(path, &dfw));

    faststring buf;

    for (int i = first_row; i < limit; i++) {
      buf.clear();
      RowChangeListEncoder update(schema_, &buf);
      uint32_t new_val = i;
      update.AddColumnUpdate(0, &new_val);
      int num_txns = random() % 3;
      for (int j = 0, curr_txid = txid_min; j < num_txns; j++) {
        DeltaKey key(i, txid_t(curr_txid));
        RETURN_NOT_OK(dfw->AppendDelta(key, RowChangeList(buf)));
        curr_txid++;
      }
    }
    gscoped_ptr<DeltaFileReader> reader;
    RETURN_NOT_OK(dfw->Finish());
    RETURN_NOT_OK(DeltaFileReader::Open(env_.get(), path, schema_, &reader));
    RETURN_NOT_OK(DeltaCompactionInput::Open(*reader, dci));
    pool_.Add(reader.release());
    return Status::OK();
  }

  Status CreateMergedDeltaCompactionInput(gscoped_ptr<DeltaCompactionInput> *merged) {
    vector<shared_ptr<DeltaCompactionInput> > inputs;
    int min_txid = 0;
    for (int i = 0; i < FLAGS_num_delta_files; i++) {
      gscoped_ptr<DeltaCompactionInput> input;
      RETURN_NOT_OK(FillDeltaFile(0, FLAGS_num_rows, min_txid, &input));
      inputs.push_back(shared_ptr<DeltaCompactionInput>(input.release()));
      min_txid += 2;
    }
    merged->reset(DeltaCompactionInput::Merge(inputs));
    return Status::OK();
  }

  virtual void SetUp() {
    KuduTest::SetUp();
    SeedRandom();
  }

 protected:
  int deltafile_idx_;
  Schema schema_;
  AutoReleasePool pool_;
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
  string path = GetNextDeltaFilePath();
  gscoped_ptr<DeltaFileWriter> dfw;
  ASSERT_STATUS_OK(GetDeltaFileWriter(path, &dfw));
  ASSERT_STATUS_OK(FlushDeltaCompactionInput(merged.get(), dfw.get()));
  ASSERT_STATUS_OK(dfw->Finish());
  gscoped_ptr<DeltaFileReader> reader;
  ASSERT_STATUS_OK(DeltaFileReader::Open(env_.get(), path, schema_, &reader));
  gscoped_ptr<DeltaCompactionInput> dci;
  ASSERT_STATUS_OK(DeltaCompactionInput::Open(*reader, &dci));
  vector<string> results;
  ASSERT_STATUS_OK(DebugDumpDeltaCompactionInput(dci.get(), &results, schema_));
  BOOST_FOREACH(const string &str, results) {
    VLOG(1) << str;
  }
  ASSERT_TRUE(is_sorted(results.begin(), results.end()));
}


} // namespace tablet
} // namespace kudu
