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

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/optional/optional.hpp>
#include <boost/optional/optional_io.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/key_value_test_schema.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DEFINE_int32(keyspace_size, 3000, "number of unique row keys to insert/mutate");
DEFINE_int32(runtime_seconds, 1, "number of seconds to run the test");
DEFINE_int32(sleep_between_background_ops_ms, 100,
             "number of milliseconds to sleep between flushing or compacting");
DEFINE_int32(update_delete_ratio, 4, "ratio of update:delete when mutating existing rows");

DECLARE_int32(deltafile_default_block_size);

using boost::optional;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tablet {

// Test which does only random operations against a tablet, including update and random
// get (ie scans with equal lower and upper bounds).
//
// The test maintains an in-memory copy of the expected state of the tablet, and uses only
// a single thread, so that it's easy to verify that the tablet always matches the expected
// state.
class TestRandomAccess : public KuduTabletTest {
 public:
  TestRandomAccess()
      : KuduTabletTest(CreateKeyValueTestSchema()),
        done_(1) {
    OverrideFlagForSlowTests("keyspace_size", "30000");
    OverrideFlagForSlowTests("runtime_seconds", "10");
    OverrideFlagForSlowTests("sleep_between_background_ops_ms", "1000");

    // Set a small block size to increase chances that a single update will span
    // multiple delta blocks.
    FLAGS_deltafile_default_block_size = 1024;
    expected_tablet_state_.resize(FLAGS_keyspace_size);
  }

  virtual void SetUp() OVERRIDE {
    KuduTabletTest::SetUp();
    writer_.reset(new LocalTabletWriter(tablet().get(), &client_schema_));
    SeedRandom();
  }

  // Pick a random row of the table, verify its current state, and then
  // modify it in some way. The modifications may include multiple mutations
  // to the same row in a single batch (eg insert/update/delete).
  //
  // The mutations are always valid. For example:
  // - inserting if it doesn't exist yet
  // - perform an update or delete the row if it does exist.
  //
  // TODO: should add a version of this test which also tries invalid operations
  // and validates the correct errors.
  void DoRandomBatch() {
    int key = rand() % expected_tablet_state_.size();
    optional<ExpectedKeyValueRow>& cur_val = expected_tablet_state_[key];

    // Check that a read yields what we expect.
    optional<ExpectedKeyValueRow> val_in_table = GetRow(key);
    ASSERT_EQ(cur_val, val_in_table);

    vector<LocalTabletWriter::Op> pending;
    for (int i = 0; i < 3; i++) {
      int new_val = rand();
      int r = rand() % 3;
      if (cur_val == boost::none) {
        // If there is no row, then randomly insert or upsert.
        switch (r) {
          case 1:
            cur_val = InsertRow(key, new_val, &pending);
            break;
          case 2:
            cur_val = InsertIgnoreRow(key, new_val, &pending);
            break;
          default:
            cur_val = UpsertRow(key, new_val, cur_val, &pending);
        }
      } else {
        if (new_val % (FLAGS_update_delete_ratio + 1) == 0) {
          cur_val = DeleteRow(key, &pending);
        } else {
          // If row already exists, randomly choose between an update,
          // upsert, and insert ignore.
          switch (r) {
            case 1:
              cur_val = MutateRow(key, new_val, cur_val, &pending);
              break;
            case 2:
              InsertIgnoreRow(key, new_val, &pending); // won't change existing value
              break;
            default:
              cur_val = UpsertRow(key, new_val, cur_val, &pending);
          }
        }
      }
    }

    VLOG(1) << "Performing batch:";
    for (const auto& op : pending) {
      VLOG(1) << RowOperationsPB::Type_Name(op.type) << " " << op.row->ToString();
    }

    CHECK_OK(writer_->WriteBatch(pending));
    for (LocalTabletWriter::Op op : pending) {
      delete op.row;
    }
  }

  void DoRandomBatches() {
    int op_count = 0;
    Stopwatch s;
    s.start();
    while (s.elapsed().wall_seconds() < FLAGS_runtime_seconds) {
      for (int i = 0; i < 100; i++) {
        NO_FATALS(DoRandomBatch());
        op_count++;
      }
    }
    LOG(INFO) << "Ran " << op_count << " ops "
              << "(" << (op_count / s.elapsed().wall_seconds()) << " ops/sec)";
  }

  // Wakes up periodically to perform a flush or compaction.
  void BackgroundOpThread() {
    int n_flushes = 0;
    while (!done_.WaitFor(MonoDelta::FromMilliseconds(FLAGS_sleep_between_background_ops_ms))) {
      CHECK_OK(tablet()->Flush());
      ++n_flushes;
      switch (n_flushes % 3) {
        case 0:
          CHECK_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
          break;
        case 1:
          CHECK_OK(tablet()->CompactWorstDeltas(RowSet::MAJOR_DELTA_COMPACTION));
          break;
        case 2:
          CHECK_OK(tablet()->CompactWorstDeltas(RowSet::MINOR_DELTA_COMPACTION));
          break;
      }
    }
  }

  // Adds an insert for the given key/value pair to 'ops', returning the expected value
  optional<ExpectedKeyValueRow> InsertRow(int key, int val, vector<LocalTabletWriter::Op>* ops) {
    return DoRowOp(RowOperationsPB::INSERT, key, val, boost::none, ops);
  }

  optional<ExpectedKeyValueRow> InsertIgnoreRow(int key, int val,
                                                vector<LocalTabletWriter::Op>* ops) {
    return DoRowOp(RowOperationsPB::INSERT_IGNORE, key, val, boost::none, ops);
  }

  optional<ExpectedKeyValueRow> UpsertRow(int key,
                                          int val,
                                          const optional<ExpectedKeyValueRow>& old_row,
                                          vector<LocalTabletWriter::Op>* ops) {
    return DoRowOp(RowOperationsPB::UPSERT, key, val, old_row, ops);
  }

  // Adds an update of the given key/value pair to 'ops', returning the expected value
  optional<ExpectedKeyValueRow> MutateRow(int key,
                                          uint32_t new_val,
                                          const optional<ExpectedKeyValueRow>& old_row,
                                          vector<LocalTabletWriter::Op>* ops) {
    return DoRowOp(RowOperationsPB::UPDATE, key, new_val, old_row, ops);
  }

  optional<ExpectedKeyValueRow> DoRowOp(RowOperationsPB::Type type,
                                        int key,
                                        int val,
                                        const optional<ExpectedKeyValueRow>& old_row,
                                        vector<LocalTabletWriter::Op>* ops) {

    gscoped_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    CHECK_OK(row->SetInt32(0, key));
    optional<ExpectedKeyValueRow> ret = ExpectedKeyValueRow();
    ret->key = key;

    switch (type) {
      case RowOperationsPB::UPSERT:
      case RowOperationsPB::UPDATE:
      case RowOperationsPB::INSERT:
      case RowOperationsPB::INSERT_IGNORE:
        switch (val % 2) {
          case 0:
            CHECK_OK(row->SetNull(1));
            ret->val = boost::none;
            break;
          case 1:
            CHECK_OK(row->SetInt32(1, val));
            ret->val = val;
            break;
        }

        if ((type != RowOperationsPB::UPDATE) && (val % 3 == 1)) {
          // Don't set the value. In the case of an INSERT or an UPSERT with no pre-existing
          // row, this should default to NULL. Otherwise it should remain set to whatever it
          // was previously set to.
          CHECK_OK(row->Unset(1));

          if (type == RowOperationsPB::INSERT || old_row == boost::none) {
            ret->val = boost::none;
          } else {
            ret->val = old_row->val;
          }
        }
        break;
      case RowOperationsPB::DELETE:
        ret = boost::none;
        break;
      default:
        LOG(FATAL) << "Unknown type: " << type;
    }
    ops->push_back(LocalTabletWriter::Op(type, row.release()));
    return ret;
  }


  // Adds a delete of the given row to 'ops', returning an empty string (indicating that
  // the row no longer exists).
  optional<ExpectedKeyValueRow> DeleteRow(int key, vector<LocalTabletWriter::Op>* ops) {
    gscoped_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    CHECK_OK(row->SetInt32(0, key));
    ops->push_back(LocalTabletWriter::Op(RowOperationsPB::DELETE, row.release()));
    return boost::none;
  }

  // Random-read the given row, returning its current value.
  // If the row doesn't exist, returns boost::none.
  optional<ExpectedKeyValueRow> GetRow(int key) {
    ScanSpec spec;
    const Schema& schema = this->client_schema_;
    unique_ptr<RowwiseIterator> iter;
    CHECK_OK(this->tablet()->NewRowIterator(schema, &iter));
    auto pred_one = ColumnPredicate::Equality(schema.column(0), &key);
    spec.AddPredicate(pred_one);
    CHECK_OK(iter->Init(&spec));

    optional<ExpectedKeyValueRow> ret;
    int n_results = 0;

    Arena arena(1024);
    RowBlock block(&schema, 100, &arena);
    while (iter->HasNext()) {
      arena.Reset();
      CHECK_OK(iter->NextBlock(&block));
      for (int i = 0; i < block.nrows(); i++) {
        if (!block.selection_vector()->IsRowSelected(i)) {
          continue;
        }
        // We expect to only get exactly one result per read.
        CHECK_EQ(n_results, 0)
          << "Already got result when looking up row "
          << key << ": " << ret
          << " and now have new matching row: "
          << schema.DebugRow(block.row(i))
          << "  iterator: " << iter->ToString();
        ret = ExpectedKeyValueRow();
        ret->key = *schema.ExtractColumnFromRow<INT32>(block.row(i), 0);
        if (!block.row(i).is_null(1)) {
          ret->val = *schema.ExtractColumnFromRow<INT32>(block.row(i), 1);
        }
        n_results++;
      }
    }
    return ret;
  }

 protected:
  // The current expected state of the tablet.
  vector<optional<ExpectedKeyValueRow>> expected_tablet_state_;

  // Latch triggered when the main thread is finished performing
  // operations. This stops the compact/flush thread.
  CountDownLatch done_;

  gscoped_ptr<LocalTabletWriter> writer_;
};

TEST_F(TestRandomAccess, Test) {
  scoped_refptr<Thread> flush_thread;
  CHECK_OK(Thread::Create("test", "flush",
                          boost::bind(&TestRandomAccess::BackgroundOpThread, this),
                          &flush_thread));

  DoRandomBatches();
  done_.CountDown();
  flush_thread->Join();
}



} // namespace tablet
} // namespace kudu
