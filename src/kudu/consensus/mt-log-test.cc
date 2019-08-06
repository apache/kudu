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
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log-test-base.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_index.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

DEFINE_int32(num_writer_threads, 4, "Number of threads writing to the log");
DEFINE_int32(num_reader_threads, 1, "Number of threads accessing the log while writes are ongoing");
DEFINE_int32(num_batches_per_thread, 2000, "Number of batches per thread");
DEFINE_int32(num_ops_per_batch_avg, 5, "Target average number of ops per batch");
DEFINE_bool(verify_log, true, "Whether to verify the log by reading it after the writes complete");

DECLARE_int32(log_thread_idle_threshold_ms);
DECLARE_int32(log_inject_thread_lifecycle_latency_ms);

namespace kudu {
namespace log {

using std::shared_ptr;
using std::vector;
using consensus::OpId;
using consensus::ReplicateRefPtr;
using consensus::ReplicateMsg;
using consensus::WRITE_OP;
using consensus::make_scoped_refptr_replicate;

namespace {

class CustomLatchCallback : public RefCountedThreadSafe<CustomLatchCallback> {
 public:
  CustomLatchCallback(CountDownLatch* latch, vector<Status>* errors)
      : latch_(latch),
        errors_(errors) {
  }

  void StatusCB(const Status& s) {
    if (!s.ok()) {
      errors_->push_back(s);
    }
    latch_->CountDown();
  }

  StatusCallback AsStatusCallback() {
    return Bind(&CustomLatchCallback::StatusCB, this);
  }

 private:
  CountDownLatch* latch_;
  vector<Status>* errors_;
};

} // anonymous namespace

class MultiThreadedLogTest : public LogTestBase {
 public:
  MultiThreadedLogTest()
      : random_(SeedRandom()) {
  }

  virtual void SetUp() OVERRIDE {
    LogTestBase::SetUp();
  }

  vector<consensus::ReplicateRefPtr> CreateRandomBatch() {
    int num_ops = static_cast<int>(random_.Normal(
        static_cast<double>(FLAGS_num_ops_per_batch_avg), 1.0));
    DVLOG(1) << num_ops << " ops in this batch";
    num_ops = std::max(num_ops, 1);
    vector<consensus::ReplicateRefPtr> ret;
    for (int j = 0; j < num_ops; j++) {
      ReplicateRefPtr replicate = make_scoped_refptr_replicate(new ReplicateMsg);
      replicate->get()->set_op_type(WRITE_OP);
      replicate->get()->set_timestamp(clock_->Now().ToUint64());
      tserver::WriteRequestPB* request = replicate->get()->mutable_write_request();
      AddTestRowToPB(RowOperationsPB::INSERT, schema_, 12345, 0,
                     "this is a test insert",
                     request->mutable_row_operations());
      request->set_tablet_id(kTestTablet);
      ret.push_back(replicate);
    }
    return ret;
  }

  void AssignIndexes(vector<consensus::ReplicateRefPtr>* batch) {
    for (auto& rep : *batch) {
      OpId* op_id = rep->get()->mutable_id();
      op_id->set_term(0);
      op_id->set_index(current_index_++);
    }
  }

  void LogWriterThread(int thread_id) {
    CountDownLatch latch(FLAGS_num_batches_per_thread);
    vector<Status> errors;
    for (int i = 0; i < FLAGS_num_batches_per_thread; i++) {
      // Do the expensive allocation outside the lock.
      vector<consensus::ReplicateRefPtr> batch_replicates = CreateRandomBatch();
      auto cb = new CustomLatchCallback(&latch, &errors);
      // Assign indexes and append inside the lock, so that the index order and
      // log order match up.
      {
        std::lock_guard<simple_spinlock> l(lock_);
        AssignIndexes(&batch_replicates);
        ASSERT_OK(log_->AsyncAppendReplicates(batch_replicates, cb->AsStatusCallback()));
      }
      MAYBE_INJECT_RANDOM_LATENCY(FLAGS_log_inject_thread_lifecycle_latency_ms);
    }
    latch.Wait();
    for (const Status& status : errors) {
      WARN_NOT_OK(status, "Unexpected failure during AsyncAppend");
    }
    CHECK_EQ(0, errors.size());
  }

  void Run() {
    for (int i = 0; i < FLAGS_num_writer_threads; i++) {
      scoped_refptr<kudu::Thread> new_thread;
      CHECK_OK(kudu::Thread::Create("test", "inserter",
          &MultiThreadedLogTest::LogWriterThread, this, i, &new_thread));
      threads_.push_back(new_thread);
    }

    // Start a thread which calls some read-only methods on the log
    // to check for races against writers.
    std::atomic<bool> stop_reader(false);
    vector<std::thread> reader_threads;
    for (int i = 0; i < FLAGS_num_reader_threads; i++) {
      reader_threads.emplace_back([&]() {
          std::map<int64_t, int64_t> map;
          while (!stop_reader) {
            log_->GetReplaySizeMap(&map);
            log_->GetGCableDataSize(RetentionIndexes(FLAGS_num_batches_per_thread));
          }
        });
    }

    // Wait for the writers to finish.
    for (scoped_refptr<kudu::Thread>& thread : threads_) {
      ASSERT_OK(ThreadJoiner(thread.get()).Join());
    }

    // Then stop the reader and join on it as well.
    stop_reader = true;
    for (auto& t : reader_threads) {
      t.join();
    }
  }

  void VerifyLog() {
    shared_ptr<LogReader> reader;
    ASSERT_OK(LogReader::Open(fs_manager_.get(), nullptr, kTestTablet, nullptr, &reader));
    SegmentSequence segments;
    ASSERT_OK(reader->GetSegmentsSnapshot(&segments));

    for (const SegmentSequence::value_type& entry : segments) {
      ASSERT_OK(entry->ReadEntries(&entries_));
    }
    vector<uint32_t> ids;
    EntriesToIdList(&ids);
    DVLOG(1) << "Wrote total of " << current_index_ - kStartIndex << " ops";
    ASSERT_EQ(current_index_ - kStartIndex, ids.size());
    ASSERT_TRUE(std::is_sorted(ids.begin(), ids.end()));
  }

 private:
  ThreadSafeRandom random_;
  simple_spinlock lock_;
  vector<scoped_refptr<kudu::Thread> > threads_;
};

TEST_F(MultiThreadedLogTest, TestAppends) {
  // Roll frequently to stress related code paths, unless overridden
  // on the command line.
  if (google::GetCommandLineFlagInfoOrDie("log_segment_size_mb").is_default) {
    options_.segment_size_mb = 1;
  }

  ASSERT_OK(BuildLog());
  LOG_TIMING(INFO, strings::Substitute("inserting $0 batches($1 threads, $2 per-thread)",
                                       FLAGS_num_writer_threads * FLAGS_num_batches_per_thread,
                                       FLAGS_num_writer_threads,
                                       FLAGS_num_batches_per_thread)) {
    NO_FATALS(Run());
  }
  ASSERT_OK(log_->Close());
  if (FLAGS_verify_log) {
    NO_FATALS(VerifyLog());
  }
}

// The lifecycle of the appender task starting and stopping is a bit complicated
// (see Log::AppendThread::GoIdle for details). This injects some latency in key
// points of that lifecycle to ensure that the different potential interleavings
// are triggered. It also injects latency into the writes done by the tests, so
// that sometimes writes are spaced out enough to allow the thread to go idle.
TEST_F(MultiThreadedLogTest, TestAppendThreadStartStopRaces) {
  FLAGS_log_thread_idle_threshold_ms = 1;
  FLAGS_log_inject_thread_lifecycle_latency_ms = 2;
  ASSERT_OK(BuildLog());
  LogWriterThread(1);
  ASSERT_OK(log_->Close());
  NO_FATALS(VerifyLog());
}

} // namespace log
} // namespace kudu
