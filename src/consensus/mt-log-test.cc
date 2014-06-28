// Copyright (c) 2014, Cloudera, inc.

#include "consensus/log-test-base.h"

#include <boost/thread/locks.hpp>
#include <boost/assign/list_of.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include <algorithm>
#include <vector>

#include "gutil/algorithm.h"
#include "gutil/ref_counted.h"
#include "util/random_util.h"
#include "util/thread.h"
#include "util/task_executor.h"
#include "util/locks.h"

DEFINE_int32(num_writer_threads, 4, "Number of threads writing to the log");
DEFINE_int32(num_batches_per_thread, 2000, "Number of batches per thread");
DEFINE_int32(num_ops_per_batch_avg, 5, "Target average number of ops per batch");

namespace kudu {
namespace log {

using std::tr1::shared_ptr;
using std::vector;

namespace {

class CustomLatchCallback : public base::RefCountedThreadSafe<CustomLatchCallback> {
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
    return base::Bind(&CustomLatchCallback::StatusCB, this);
  }

 private:
  CountDownLatch* latch_;
  vector<Status>* errors_;
};

} // anonymous namespace

extern const char *kTestTablet;

class MultiThreadedLogTest : public LogTestBase {
 public:

  virtual void SetUp() OVERRIDE {
    LogTestBase::SetUp();
    SeedRandom();
  }

  void LogWriterThread(int thread_id) {
    vector<const OperationPB*> ops;
    ElementDeleter deleter(&ops);

    CountDownLatch latch(FLAGS_num_batches_per_thread);
    vector<Status> errors;
    for (int i = 0; i < FLAGS_num_batches_per_thread; i++) {
      LogEntryBatch* entry_batch;
      vector<const OperationPB*> batch_ops;
      int num_ops = static_cast<int>(NormalDist(
          static_cast<double>(FLAGS_num_ops_per_batch_avg), 1.0));
      DVLOG(1) << num_ops << " ops in this batch";
      num_ops =  std::max(num_ops, 1);
      {
        boost::lock_guard<simple_spinlock> lock_guard(lock_);
        for (int j = 0; j < num_ops; j++) {
          gscoped_ptr<OperationPB> op(new OperationPB);
          uint32_t index = current_id_++;
          OpId* op_id = op->mutable_id();
          op_id->set_term(0);
          op_id->set_index(index);

          ReplicateMsg* replicate = op->mutable_replicate();
          replicate->set_op_type(WRITE_OP);

          tserver::WriteRequestPB* request = replicate->mutable_write_request();
          AddTestRowToPB(RowOperationsPB::INSERT, schema_,
                         index,
                         0,
                         "this is a test insert",
                         request->mutable_row_operations());
          request->set_tablet_id(kTestTablet);
          batch_ops.push_back(op.release());
        }
        ASSERT_STATUS_OK(log_->Reserve(&batch_ops[0], batch_ops.size(), &entry_batch));
      } // lock_guard scope
      CustomLatchCallback* cb = new CustomLatchCallback(&latch, &errors);
      ASSERT_STATUS_OK(log_->AsyncAppend(entry_batch, cb->AsStatusCallback()));
      // Copy 'batch_ops' to 'ops', so that they can be free upon thread termination.
      std::copy(batch_ops.begin(), batch_ops.end(), std::back_inserter(ops));
    }
    LOG_TIMING(INFO, strings::Substitute("thread $0 waiting to append and sync $1 batches",
                                        thread_id, FLAGS_num_batches_per_thread)) {
      latch.Wait();
    }
    BOOST_FOREACH(const Status& status, errors) {
      WARN_NOT_OK(status, "Unexpected failure during AsyncAppend");
    }
    ASSERT_EQ(0, errors.size());
  }

  void Run() {
    for (int i = 0; i < FLAGS_num_writer_threads; i++) {
      scoped_refptr<kudu::Thread> new_thread;
      CHECK_OK(kudu::Thread::Create("test", "inserter",
          &MultiThreadedLogTest::LogWriterThread, this, i, &new_thread));
      threads_.push_back(new_thread);
    }
    BOOST_FOREACH(scoped_refptr<kudu::Thread>& thread, threads_) {
      ASSERT_STATUS_OK(ThreadJoiner(thread.get()).Join());
    }
  }
 private:
  simple_spinlock lock_;
  vector<scoped_refptr<kudu::Thread> > threads_;
};

TEST_F(MultiThreadedLogTest, TestAppends) {
  BuildLog();
  int start_current_id = current_id_;
  LOG_TIMING(INFO, strings::Substitute("inserting $0 batches($1 threads, $2 per-thread)",
                                      FLAGS_num_writer_threads * FLAGS_num_batches_per_thread,
                                      FLAGS_num_batches_per_thread, FLAGS_num_writer_threads)) {
    ASSERT_NO_FATAL_FAILURE(Run());
  }
  ASSERT_STATUS_OK(log_->Close());
  BuildLogReader();
  BOOST_FOREACH(const ReadableLogSegmentMap::value_type& entry, log_reader_->segments()) {
    ASSERT_STATUS_OK(entry.second->ReadEntries(&entries_));
  }
  vector<uint32_t> ids;
  EntriesToIdList(&ids);
  DVLOG(1) << "Wrote total of " << current_id_ - start_current_id << " ops";
  ASSERT_EQ(current_id_ - start_current_id, ids.size());
  ASSERT_TRUE(util::gtl::is_sorted(ids.begin(), ids.end()));
}

} // namespace log
} // namespace kudu
