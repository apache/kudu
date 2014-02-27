// Copyright (c) 2014, Cloudera, inc.

#include "consensus/log-test-base.h"

#include <boost/thread/locks.hpp>
#include <boost/assign/list_of.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include <vector>

#include "gutil/algorithm.h"
#include "util/task_executor.h"
#include "util/locks.h"
#include "util/thread_util.h"

DEFINE_int32(num_writer_threads, 4, "Number of threads writing to the log");
DEFINE_int32(num_ops_per_thread, 2000, "Number of operations per thread");

namespace kudu {
namespace log {

using std::tr1::shared_ptr;
using std::vector;

namespace {

class CustomLatchCallback : public FutureCallback {
 public:
  CustomLatchCallback(CountDownLatch* latch, vector<Status>* errors)
      : latch_(latch),
        errors_(errors) {
  }

  virtual void OnSuccess() {
    latch_->CountDown();
  }

  virtual void OnFailure(const Status& status) {
    errors_->push_back(status);
    latch_->CountDown();
  }

 private:
  CountDownLatch* latch_;
  vector<Status>* errors_;
};

} // anonymous namespace

extern const char *kTestTablet;

class MultiThreadedLogTest : public LogTestBase {
 public:

  void LogWriterThread(int thread_id) {
    vector<OperationPB*> ops;
    ElementDeleter deleter(&ops);

    CountDownLatch latch(FLAGS_num_ops_per_thread);
    vector<Status> errors;
    for (int i = 0; i < FLAGS_num_ops_per_thread; i++) {
      LogEntryBatch* entry_batch;
      {
        boost::lock_guard<simple_spinlock> lock_guard(lock_);
        gscoped_ptr<OperationPB> op(new OperationPB);
        uint32_t index = current_id_++;
        OpId* op_id = op->mutable_id();
        op_id->set_term(0);
        op_id->set_index(index);

        ReplicateMsg* replicate = op->mutable_replicate();
        replicate->set_op_type(WRITE_OP);

        tserver::WriteRequestPB* request = replicate->mutable_write_request();
        AddTestRowToPB(schema_,
                       index,
                       0,
                       "this is a test insert",
                       request->mutable_to_insert_rows());
        request->set_tablet_id(kTestTablet);
        ASSERT_STATUS_OK(log_->Reserve(boost::assign::list_of(op.get()),
                                       &entry_batch));
        ops.push_back(op.release());
      }
      shared_ptr<CustomLatchCallback> cb(new CustomLatchCallback(&latch, &errors));
      ASSERT_STATUS_OK(log_->AsyncAppend(entry_batch, cb));
    }
    LOG_TIMING(INFO, strings::Substitute("thread $0 waiting to append and sync $1 entries",
                                        thread_id, FLAGS_num_ops_per_thread)) {
      latch.Wait();
    }
    BOOST_FOREACH(const Status& status, errors) {
      WARN_NOT_OK(status, "Unexpected failure during AsyncAppend");
    }
    ASSERT_EQ(0, errors.size());
  }

  void Run() {
    for (int i = 0; i < FLAGS_num_writer_threads; i++) {
      threads_.push_back(shared_ptr<boost::thread>(
          new boost::thread(boost::bind(
              &MultiThreadedLogTest::LogWriterThread, this, i))));
    }
    BOOST_FOREACH(shared_ptr<boost::thread>& thread, threads_) {
      ASSERT_STATUS_OK(ThreadJoiner(thread.get(), "inserter thread").Join());
    }
  }
 private:
  simple_spinlock lock_;
  vector<shared_ptr<boost::thread> > threads_;
};

TEST_F(MultiThreadedLogTest, TestAppends) {
  BuildLog();
  LOG_TIMING(INFO, strings::Substitute("inserting $0 requests($1 threads, $2 per-thread)",
                                      FLAGS_num_writer_threads * FLAGS_num_ops_per_thread,
                                      FLAGS_num_ops_per_thread, FLAGS_num_writer_threads)) {
    ASSERT_NO_FATAL_FAILURE(Run());
  }
  ASSERT_STATUS_OK(log_->Close());
  BuildLogReader();
  for (int i = 0; i < log_reader_->size(); i++) {
    ASSERT_STATUS_OK(LogReader::ReadEntries(log_reader_->segments()[i], &entries_));
  }
  vector<uint32_t> ids;
  EntriesToIdList(&ids);
  ASSERT_EQ(FLAGS_num_writer_threads * FLAGS_num_ops_per_thread,
            ids.size());
  ASSERT_TRUE(util::gtl::is_sorted(ids.begin(), ids.end()));
}

} // namespace log
} // namespace kudu
