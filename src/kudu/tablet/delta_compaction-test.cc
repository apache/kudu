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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cfile/cfile_util.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/types.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/port.h"
#include "kudu/tablet/delta_iterator_merger.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/rowset.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(num_rows, 2100, "the first row to update");
DEFINE_int32(num_delta_files, 3, "number of delta files");

using std::is_sorted;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tablet {

using cfile::ReaderOptions;
using fs::ReadableBlock;
using fs::WritableBlock;

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

  Status GetDeltaFileWriter(unique_ptr<DeltaFileWriter>* dfw,
                            BlockId* block_id) const {
    unique_ptr<WritableBlock> block;
    RETURN_NOT_OK(fs_manager_->CreateNewBlock({}, &block));
    *block_id = block->id();
    dfw->reset(new DeltaFileWriter(std::move(block)));
    RETURN_NOT_OK((*dfw)->Start());
    return Status::OK();
  }

  Status GetDeltaFileReader(const BlockId& block_id,
                            shared_ptr<DeltaFileReader>* dfr) const {
    unique_ptr<ReadableBlock> block;
    RETURN_NOT_OK(fs_manager_->OpenBlock(block_id, &block));
    shared_ptr<DeltaFileReader> delta_reader;
    return DeltaFileReader::Open(std::move(block), REDO, ReaderOptions(), dfr);
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    SeedRandom();
    fs_manager_.reset(new FsManager(env_, FsManagerOpts(GetTestPath("fs_root"))));
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());
  }

 protected:
  int64_t deltafile_idx_;
  Schema schema_;
  unique_ptr<FsManager> fs_manager_;
};

TEST_F(TestDeltaCompaction, TestMergeMultipleSchemas) {
  vector<Schema> schemas;
  SchemaBuilder builder(schema_);
  schemas.push_back(builder.Build());

  // Add an int column with default
  uint32_t default_c2 = 10;
  ASSERT_OK(builder.AddColumn("c2", UINT32, false, &default_c2, &default_c2));
  schemas.push_back(builder.Build());

  // add a string column with default
  Slice default_c3("Hello World");
  ASSERT_OK(builder.AddColumn("c3", STRING, false, &default_c3, &default_c3));
  schemas.push_back(builder.Build());

  vector<shared_ptr<DeltaStore> > inputs;

  faststring buf;
  int row_id = 0;
  int curr_timestamp = 0;
  int deltafile_idx = 0;
  for (const Schema& schema : schemas) {
    // Write the Deltas
    BlockId block_id;
    unique_ptr<DeltaFileWriter> dfw;
    ASSERT_OK(GetDeltaFileWriter(&dfw, &block_id));

    // Generate N updates with the new schema, some of them are on existing
    // rows others are on new rows (see kNumUpdates and kNumMultipleUpdates).
    // Each column will be updated with value composed by delta file id
    // and update number (see update_value assignment).
    size_t kNumUpdates = 10;
    size_t kNumMultipleUpdates = kNumUpdates / 2;
    unique_ptr<DeltaStats> stats(new DeltaStats);
    for (size_t i = 0; i < kNumUpdates; ++i) {
      buf.clear();
      RowChangeListEncoder update(&buf);
      for (size_t col_idx = schema.num_key_columns(); col_idx < schema.num_columns(); ++col_idx) {
        ColumnId col_id = schema.column_id(col_idx);
        DCHECK_GE(col_id, 0);

        stats->IncrUpdateCount(col_id, 1);
        const ColumnSchema& col_schema = schema.column(col_idx);
        int update_value = deltafile_idx * 100 + i;
        switch (col_schema.type_info()->physical_type()) {
          case UINT32:
            {
              uint32_t u32_val = update_value;
              update.AddColumnUpdate(col_schema, col_id, &u32_val);
            }
            break;
          case BINARY:
            {
              string s = std::to_string(update_value);
              Slice str_val(s);
              update.AddColumnUpdate(col_schema, col_id, &str_val);
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
      ASSERT_OK(dfw->AppendDelta<REDO>(key, row_changes));
      ASSERT_OK(stats->UpdateStats(key.timestamp(), row_changes));
      curr_timestamp++;
      row_id++;
    }

    dfw->WriteDeltaStats(std::move(stats));
    ASSERT_OK(dfw->Finish());
    shared_ptr<DeltaFileReader> dfr;
    ASSERT_OK(GetDeltaFileReader(block_id, &dfr));
    inputs.push_back(dfr);
    deltafile_idx++;
  }

  // Merge
  const Schema& merge_schema = schemas.back();
  RowIteratorOptions opts;
  opts.projection = &merge_schema;
  unique_ptr<DeltaIterator> merge_iter;
  ASSERT_OK(DeltaIteratorMerger::Create(inputs, opts, &merge_iter));
  unique_ptr<DeltaFileWriter> dfw;
  BlockId block_id;
  ASSERT_OK(GetDeltaFileWriter(&dfw, &block_id));
  ASSERT_OK(WriteDeltaIteratorToFile<REDO>(merge_iter.get(),
                                           ITERATE_OVER_ALL_ROWS,
                                           dfw.get()));
  ASSERT_OK(dfw->Finish());

  shared_ptr<DeltaFileReader> dfr;
  ASSERT_OK(GetDeltaFileReader(block_id, &dfr));
  unique_ptr<DeltaIterator> iter;
  ASSERT_OK(dfr->NewDeltaIterator(opts, &iter));

  vector<string> results;
  ASSERT_OK(DebugDumpDeltaIterator(REDO, iter.get(), merge_schema,
                                   ITERATE_OVER_ALL_ROWS, &results));
  for (const string &str : results) {
    VLOG(1) << str;
  }
  ASSERT_TRUE(is_sorted(results.begin(), results.end()));
}

} // namespace tablet
} // namespace kudu
