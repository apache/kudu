// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_LOG_ASYNC_LOG_READER_H_
#define KUDU_LOG_ASYNC_LOG_READER_H_

#include <vector>

#include "kudu/gutil/callback.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace kudu {
namespace consensus {
class OpId;
class ReplicateMsg;
}

namespace log {
class LogReader;

// An asynchronous reader for the Log.
class AsyncLogReader {
 public:

  // The callback that is called once the messages have been read from disk.
  // AsyncLogReader assumes that the Callback will take ownership of the ReplicateMsgs.
  typedef Callback<
      void(const Status& resulting_status,
           const std::vector<consensus::ReplicateMsg*>& read_replicates)>
      ReadDoneCallback;

  explicit AsyncLogReader(LogReader* log_reader)
      : log_reader_(log_reader),
        is_reading_(false),
        run_latch_(0) {
    ThreadPoolBuilder("async_log_reader").set_max_threads(1).Build(&pool_);
  }

  // Enqueues async reading of ReplicateMsgs from 'starting_at' to 'up_to',
  // both inclusive.
  // Returns Status::OK() if the enqueue was successful or Status::AlreadyPresent()
  // if the reader was already reading. Any other status means the read failed.
  Status EnqueueAsyncRead(int64_t starting_at,
                          int64_t up_to,
                          const ReadDoneCallback& callback);

  // Returns whether a read is happening.
  bool IsReading() const;

  // Waits for ongoing reads to complete, if there is any.
  void Shutdown();

 private:

  // Actually performs the read from disk. Calls the callback when done.
  void ReadEntriesAsync(int64_t starting_at,
                        int64_t up_to,
                        const ReadDoneCallback& callback);

  LogReader* log_reader_;
  bool is_reading_;
  CountDownLatch run_latch_;
  gscoped_ptr<ThreadPool> pool_;
  mutable simple_spinlock lock_;

  DISALLOW_COPY_AND_ASSIGN(AsyncLogReader);
};

}  // namespace log
}  // namespace kudu

#endif /* KUDU_LOG_ASYNC_LOG_READER_H_ */
