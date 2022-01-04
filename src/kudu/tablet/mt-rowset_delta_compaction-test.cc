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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/tablet/diskrowset-test-base.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

enum {
  kDefaultNumSecondsPerThread = 1,
  kDefaultNumFlushThreads = 4,
  kDefaultNumCompactionThreads = 4,
};

DEFINE_int32(num_update_threads, 1, "Number of updater threads");
DEFINE_int32(num_flush_threads, kDefaultNumFlushThreads, "Number of flusher threads");
DEFINE_int32(num_compaction_threads, kDefaultNumCompactionThreads, "Number of compaction threads");
DEFINE_int32(num_seconds_per_thread, kDefaultNumSecondsPerThread,
             "Minimum number of seconds each thread should work");

using std::shared_ptr;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tablet {

using base::subtle::Release_Store;
using base::subtle::Release_Load;
using base::subtle::NoBarrier_Load;

class TestMultiThreadedRowSetDeltaCompaction : public TestRowSet {
 public:

  TestMultiThreadedRowSetDeltaCompaction()
      : TestRowSet(),
        update_counter_(0),
        should_run_(1) {
  }

  // This thread read the value of an atomic integer, updates all rows
  // in 'rs' to the value + 1, and then sets the atomic integer back
  // to value + 1. This is done so that the verifying threads knows the
  // latest expected value of the row (simply calling AtomicIncrement
  // won't work as a thread setting a value n+1 is not guaranteed to finish
  // before a thread setting value n).
  void RowSetUpdateThread(DiskRowSet *rs) {
    while (ShouldRun()) {
      uint32_t val = Release_Load(&update_counter_);
      UpdateRowSet(rs, val + 1);
      if (ShouldRun()) {
        Release_Store(&update_counter_, val + 1);
      }
    }
  }

  void RowSetFlushThread(DiskRowSet *rs) {
    while (ShouldRun()) {
      if (rs->CountDeltaStores() < 5) {
        CHECK_OK(rs->FlushDeltas(nullptr));
      } else {
        SleepFor(MonoDelta::FromMilliseconds(10));
      }
    }
  }

  void RowSetDeltaCompactionThread(DiskRowSet *rs) {
    while (ShouldRun()) {
      CHECK_OK(rs->MinorCompactDeltaStores(nullptr));
    }
  }

  void ReadVerify(DiskRowSet *rs) {
    RowBlockMemory mem(1024);
    RowBlock dst(&schema_, 1000, &mem);
    RowIteratorOptions opts;
    opts.projection = schema_ptr_;
    unique_ptr<RowwiseIterator> iter;
    ASSERT_OK(rs->NewRowIterator(opts, &iter));
    uint32_t expected = NoBarrier_Load(&update_counter_);
    ASSERT_OK(iter->Init(nullptr));
    while (iter->HasNext()) {
      mem.Reset();
      ASSERT_OK_FAST(iter->NextBlock(&dst));
      size_t n = dst.nrows();
      ASSERT_GT(n, 0);
      for (size_t j = 0; j < n; j++) {
        uint32_t val = *schema_.ExtractColumnFromRow<UINT32>(dst.row(j), 1);
        ASSERT_GE(val, expected);
      }
    }
  }

  void StartThreads(DiskRowSet *rs) {
    for (int i = 0; i < FLAGS_num_update_threads; i++) {
      threads_.emplace_back([this, rs]() { this->RowSetUpdateThread(rs); });
    }
    for (int i = 0; i < FLAGS_num_flush_threads; i++) {
      threads_.emplace_back([this, rs]() { this->RowSetFlushThread(rs); });
    }
    for (int i = 0; i < FLAGS_num_compaction_threads; i++) {
      threads_.emplace_back([this, rs]() { this->RowSetDeltaCompactionThread(rs); });
    }
  }

  void JoinThreads() {
    for (auto& t : threads_) {
      t.join();
    }
  }

  void WriteTestRowSetWithZeros() {
    WriteTestRowSet(0, true);
  }

  void UpdateRowSet(DiskRowSet *rs, uint32_t value) {
    for (uint32_t idx = 0; idx < n_rows_ && ShouldRun(); idx++) {
      OperationResultPB result;
      ASSERT_OK_FAST(UpdateRow(rs, idx, value, &result));
    }
  }

  void TestUpdateAndVerify() {
    WriteTestRowSetWithZeros();
    shared_ptr<DiskRowSet> rs;
    ASSERT_OK(OpenTestRowSet(&rs));

    StartThreads(rs.get());
    SleepFor(MonoDelta::FromSeconds(FLAGS_num_seconds_per_thread));
    base::subtle::NoBarrier_Store(&should_run_, 0);
    NO_FATALS(JoinThreads());

    NO_FATALS(ReadVerify(rs.get()));
  }

  bool ShouldRun() const {
    return NoBarrier_Load(&should_run_);
  }

 protected:

  Atomic32 update_counter_;
  Atomic32 should_run_;
  vector<thread> threads_;
};

static void SetupFlagsForSlowTests() {
  if (kDefaultNumSecondsPerThread == FLAGS_num_seconds_per_thread) {
    FLAGS_num_seconds_per_thread = 40;
  }
  if (kDefaultNumFlushThreads == FLAGS_num_flush_threads) {
    FLAGS_num_flush_threads = 8;
  }
  if (kDefaultNumCompactionThreads == FLAGS_num_compaction_threads) {
    FLAGS_num_compaction_threads = 8;
  }
}

TEST_F(TestMultiThreadedRowSetDeltaCompaction, TestMTUpdateAndCompact) {
  if (AllowSlowTests()) {
    SetupFlagsForSlowTests();
  }

  TestUpdateAndVerify();
}

} // namespace tablet
} // namespace kudu
