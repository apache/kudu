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
#ifndef KUDU_CLIENT_SESSION_INTERNAL_H
#define KUDU_CLIENT_SESSION_INTERNAL_H

#include <memory>

#include "kudu/client/client.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/mutex.h"

namespace kudu {

namespace rpc {
class Messenger;
} // namespace rpc

namespace client {

namespace internal {
class Batcher;
class ErrorCollector;
} // internal

// This class contains the code to do the heavy-lifting for the
// kudu::KuduSession-related operations. Its interface does not assume
// thread-safety in general, but it's thread-safe regarding the following
// concurrent actions:
//
//  * calls to KuduSession::Apply():
//    (there is at most one call at any moment, no concurrency is assumed here).
//
//  * activity of the time-based background flush task
//    (there is at most one task running at any moment).
//
//  * calls from messenger/reactor threads upon completion of the flushed
//    operations to the corresponding tablet server
//    (there can be multiple of those at any moment).
//
class KuduSession::Data {
 public:
  explicit Data(sp::shared_ptr<KuduClient> client,
                std::weak_ptr<rpc::Messenger> messenger);

  void Init(sp::weak_ptr<KuduSession> session);

  // Called by Batcher when a flush has finished.
  void FlushFinished(internal::Batcher* batcher);

  // Returns Status::IllegalState() if 'force' is false and there are still pending
  // operations. If 'force' is true batcher_ is aborted even if there are pending
  // operations.
  Status Close(bool force);

  // Set flush mode for the session.
  Status SetFlushMode(FlushMode mode);

  // Set external consistency mode for the session.
  Status SetExternalConsistencyMode(KuduSession::ExternalConsistencyMode m);

  // Set limit on buffer space consumed by buffered write operations.
  Status SetBufferBytesLimit(size_t size);

  // Set buffer flush watermark (in percentage of the total buffer space).
  Status SetBufferFlushWatermark(int32_t watermark_pct);

  // Set the interval of the background max-wait flushing (in milliseconds).
  Status SetBufferFlushInterval(unsigned int period_ms);

  // Set the limit on maximum number of batchers with pending operations.
  Status SetMaxBatchersNum(unsigned int period_ms);

  // Set timeout for write operations, in milliseconds.
  void SetTimeoutMillis(int timeout_ms);

  // Initiate flushing of the current batcher and invoke the specified callback
  // once the flushing is finished.
  void FlushAsync(KuduStatusCallback* cb);

  // Initiate flushing of the current batcher and await until all batchers
  // complete flushing. The return value is Status::OK() if none of the
  // batchers encountered errors and Status::IOError() otherwise.
  Status Flush();

  // Check whether there are operations not yet sent to tablet servers.
  bool HasPendingOperations() const;
  bool HasPendingOperationsUnlocked() const;

  // Get total number of buffered operations.
  int CountBufferedOperations() const;

  // Initiate flushing of the current batcher if its accumulated operations'
  // on-the-wire size has reached the specified watermark. The result
  // of the asynchronous flushing is reported via the specified callback.
  // Even if the callback is null (nullptr), that does not mean the errors
  // are dropped on the floor -- in case of an error, corresponding information
  // is added into the session's error collector and can be retrieved later.
  void FlushCurrentBatcher(int64_t watermark,
                           KuduStatusCallback* cb);

  // Initiate flushing of the current batcher if it has reached the specified
  // age. If the current batcher is present but it hasn't reached
  // the specified age yet, just return the amount of time left until it reaches
  // the specified age, not flushing the batcher. If the current batcher is
  // of the specified age or older, flush the batcher and return uninitialized
  // MonoDelta object. If there isn't current batcher, return uninitialized
  // MonoDelta object.
  MonoDelta FlushCurrentBatcher(const MonoDelta& max_age);

  // Apply a write operation, i.e. push it through the batcher chain.
  Status ApplyWriteOp(KuduWriteOperation* write_op);

  // Check and start the time-based flush task in background, if necessary.
  void TimeBasedFlushInit();

  // The self-rescheduling task to flush write operations which have been
  // accumulating for too long (controlled by flush_interval_).
  // This does real work only in case of AUTO_FLUSH_BACKGROUND mode.
  // This method is used to initiate/re-initiate the run of the task
  // and re-schedule the task from within. The 'do_startup_check' parameter
  // must be set to 'true' when the method is called not from the task thread.
  static void TimeBasedFlushTask(const Status& status,
                                 std::weak_ptr<rpc::Messenger> weak_messenger,
                                 sp::weak_ptr<KuduSession> weak_session,
                                 bool do_startup_check);

  // Get the total size of pending (i.e. both freshly added and
  // in process of being flushed) operations. This method is used by tests only.
  int64_t GetPendingOperationsSizeForTests() const;

  // Get the total number of batchers in the session.
  // This method is used by tests only.
  size_t GetBatchersCountForTests() const;

  // This constant represents a meaningful name for the first argument in
  // expressions like FlushCurrentBatcher(1, cbk): this is the watermark
  // corresponding to 1 byte of data. This watermark level is the minimum
  // possible for a non-empty batcher, so any non-empty batcher will be flushed
  // if calling FlushCurrentBatcher() using this watermark.
  static const int64_t kWatermarkNonEmptyBatcher = 1;

  // The client that this session is associated with.
  const sp::shared_ptr<KuduClient> client_;

  // Weak reference to the containing session. The reference is weak to
  // avoid circular referencing.  The reference to the KuduSession object
  // is needed by batchers and time-based flush task: being run in independent
  // threads, they need to make sure the object is alive before accessing it.
  sp::weak_ptr<KuduSession> session_;

  // The reference to the client's messenger (keeping the reference instead of
  // declaring friendship to KuduClient and accessing it via the client_).
  std::weak_ptr<rpc::Messenger> messenger_;

  // Buffer for errors.
  scoped_refptr<internal::ErrorCollector> error_collector_;

  kudu::client::KuduSession::ExternalConsistencyMode external_consistency_mode_;

  // Timeout for the next batch.
  MonoDelta timeout_;

  // Interval for the max-wait flush background task.
  MonoDelta flush_interval_;  // protected by mutex_

  // Whether the flush task is active/scheduled.
  bool flush_task_active_; // protected by mutex_

  // Current flush mode for the session's data.
  FlushMode flush_mode_;  // protected by mutex_

  // Mutex for the condition_ member (the condition variable).
  // This lock protects variables from simultaneous access:
  // batcher- and byte-counting members, data flow control and other variables
  // whose modification requires to notify the thread which might be waiting
  // on the 'condition_' variable.
  mutable Mutex mutex_;

  // Condition variable used by the code which allocates next batcher
  // and count bytes used by the buffered write operations.
  // I.e., it used by the data flow control logic while applying/adding
  // new write operations (based on mutex_).
  ConditionVariable condition_;

  // The current batcher being prepared.
  scoped_refptr<internal::Batcher> batcher_;// protected by mutex_

  // Total number of active batchers. Include the current batcher accumulating
  // the newly applied operations, and other batchers with not yet flushed
  // or flushed but not yet finished operations.
  size_t batchers_num_; // protected by mutex_

  // Limit on the number of active batchers.
  size_t batchers_num_limit_;

  // Session-wide limit on total size of buffer used by all batched write
  // operations. The buffer is a virtual entity: there isn't contiguous place
  // in the memory which would contain that 'buffered' data. Instead, buffer's
  // data is spread across all pending operations in all active batchers.
  // Thread-safety note: buffer_bytes_limit_ is not supposed to be modified
  // from any other thread since no thread-safety is advertised for the
  // kudu::KuduSession interface.
  size_t buffer_bytes_limit_;

  // The high-watermark level as the percentage of the buffer space used by
  // freshly added (not-yet-scheduled-for-flush) write operations.
  // Once the level is reached, the BackgroundFlusher triggers flushing
  // of accumulated write operations when running in AUTO_FLUSH_BACKGROUND mode.
  // Thread-safety note: buffer_watermark_pct_ is not supposed to be modified
  // from any other thread since no thread-safety is advertised for the
  // kudu::KuduSession interface.
  int32_t buffer_watermark_pct_;

  // The total number of bytes used by buffered write operations.
  int64_t buffer_bytes_used_;  // protected by mutex_

 private:
  FRIEND_TEST(ClientTest, TestAutoFlushBackgroundApplyBlocks);

  bool buffer_pre_flush_enabled_; // Set to 'false' only in test scenarios.

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
