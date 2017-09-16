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

#include "kudu/client/session-internal.h"

#include <memory>
#include <mutex>
#include <utility>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/function.hpp>
#include <glog/logging.h>

#include "kudu/client/batcher.h"
#include "kudu/client/callbacks.h"
#include "kudu/client/error_collector.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/logging.h"

using std::unique_ptr;

namespace kudu {

using rpc::Messenger;

namespace client {

using internal::Batcher;
using internal::ErrorCollector;

using sp::shared_ptr;
using sp::weak_ptr;


KuduSession::Data::Data(shared_ptr<KuduClient> client,
                        std::weak_ptr<rpc::Messenger> messenger)
    : client_(std::move(client)),
      messenger_(std::move(messenger)),
      error_collector_(new ErrorCollector()),
      external_consistency_mode_(CLIENT_PROPAGATED),
      flush_interval_(MonoDelta::FromMilliseconds(1000)),
      flush_task_active_(false),
      flush_mode_(AUTO_FLUSH_SYNC),
      condition_(&mutex_),
      batchers_num_(0),
      batchers_num_limit_(2),
      buffer_bytes_limit_(7 * 1024 * 1024),
      buffer_watermark_pct_(50),
      buffer_bytes_used_(0),
      buffer_pre_flush_enabled_(true) {
}

void KuduSession::Data::Init(weak_ptr<KuduSession> session) {
  session_.swap(session);
  TimeBasedFlushInit();
}

void KuduSession::Data::FlushFinished(Batcher* batcher) {
  const int64_t bytes_flushed = batcher->buffer_bytes_used();
  {
    std::lock_guard<Mutex> l(mutex_);
    buffer_bytes_used_ -= bytes_flushed;
    --batchers_num_;
    // The logic of KuduSession::ApplyWriteOp() needs to know
    // if total number of batchers or buffer byte count decreases.
    // There can be a thread waiting on the corresponding condition
    // variable: the thread which runs KuduSession::Apply(), and
    // since KuduSession interface does not advertise thread-safety, it's
    // the only thread to notify.
    condition_.Signal();
  }
}

Status KuduSession::Data::Close(bool force) {
  std::lock_guard<Mutex> l(mutex_);
  if (!batcher_) {
      return Status::OK();
  }
  if (batcher_->HasPendingOperations() && !force) {
    return Status::IllegalState("Could not close. There are pending operations.");
  }
  batcher_->Abort();

  return Status::OK();
}

Status KuduSession::Data::SetExternalConsistencyMode(
    KuduSession::ExternalConsistencyMode m) {
  std::lock_guard<Mutex> l(mutex_);
  if (HasPendingOperationsUnlocked()) {
    // NOTE: this is an artificial restriction.
    return Status::IllegalState(
        "Cannot change external consistency mode when writes are buffered");
  }
  // Thread-safety note: the external_consistency_mode_ is not supposed
  // to be accessed or modified from any other thread:
  // no thread-safety is assumed for the kudu::KuduSession interface.
  // However, the lock is needed to check for pending operations because
  // there may be pending RPCs and the background flush task may be running.
  external_consistency_mode_ = m;
  return Status::OK();
}

Status KuduSession::Data::SetFlushMode(FlushMode mode) {
  {
    std::lock_guard<Mutex> l(mutex_);
    if (HasPendingOperationsUnlocked()) {
      // Don't allow to change flush mode otherwise it might lead to
      // unexpected behavior while working with the KuduSession interface.
      // E.g., if changing from MANUAL_FLUSH/AUTO_FLUSH_BACKGROUND to
      // AUTO_FLUSH_SYNC while there are pending operations, on the next call of
      // KuduSession::Apply() the buffered operations will be flushed along
      // with the new one, which is not a good predictable behavior.
      return Status::IllegalState(
          "Cannot change flush mode when writes are buffered.");
    }
    // Thread-safety note: the flush_mode_ is accessed from the background flush
    // thread for reading, so it should be modified under protection.
    // There should not be any threads waiting on conditions
    // which are affected by the setting, so no signalling is necessary here.
    flush_mode_ = mode;
  }

  TimeBasedFlushInit();

  return Status::OK();
}

Status KuduSession::Data::SetBufferBytesLimit(size_t size) {
  std::lock_guard<Mutex> l(mutex_);
  if (HasPendingOperationsUnlocked()) {
    // NOTE: this is an artificial restriction.
    return Status::IllegalState(
        "Cannot change buffer size limit when writes are buffered.");
  }
  // Thread-safety note: the buffer_bytes_limit_ is not supposed to be accessed
  // or modified from any other thread: no thread-safety is assumed
  // for the kudu::KuduSession interface. Due to the latter reason,
  // there should not be any threads waiting on conditions which are affected
  // by the change, so signalling other threads isn't necessary here.
  // However, the lock is needed to check for pending operations because
  // there may be pending RPCs and the background flush task may be running.
  buffer_bytes_limit_ = size;
  return Status::OK();
}

Status KuduSession::Data::SetBufferFlushWatermark(int watermark_pct) {
  if (watermark_pct < 0 || watermark_pct > 100) {
    return Status::InvalidArgument(
        strings::Substitute("$0: watermark must be between 0 and 100 inclusive",
                            watermark_pct));
  }
  std::lock_guard<Mutex> l(mutex_);
  if (HasPendingOperationsUnlocked()) {
    // NOTE: this is an artificial restriction.
    return Status::IllegalState(
        "Cannot change buffer flush watermark when writes are buffered.");
  }
  // Thread-safety note: the buffer_watermark_pct_ is not supposed
  // to be accessed or modified from any other thread:
  // no thread-safety is assumed for the kudu::KuduSession interface.
  // Due to the latter reason, there should not be any threads waiting on
  // conditions which are affected by the setting, so no signalling
  // is necessary here.
  // However, the lock is needed to check for pending operations because
  // there may be pending RPCs and the background flush task may be running.
  buffer_watermark_pct_ = watermark_pct;
  return Status::OK();
}

Status KuduSession::Data::SetBufferFlushInterval(unsigned int millis) {
  std::lock_guard<Mutex> l(mutex_);
  if (HasPendingOperationsUnlocked()) {
    // NOTE: this is an artificial restriction.
    return Status::IllegalState(
        "Cannot change buffer flush interval when writes are buffered.");
  }
  // Thread-safety note: the flush_interval_ is accessed from the background
  // flush thread for reading, so it should be modified under protection.
  flush_interval_ = MonoDelta::FromMilliseconds(millis);
  return Status::OK();
}

Status KuduSession::Data::SetMaxBatchersNum(unsigned int max_num) {
  // 1 is the minimum possible number of batchers per session.
  // 0 means there isn't any limit on the maximum number of batchers.
  std::lock_guard<Mutex> l(mutex_);
  if (HasPendingOperationsUnlocked()) {
    // NOTE: this is an artificial restriction.
    return Status::IllegalState(
        "Cannot change the limit on maximum number of batchers when writes are buffered.");
  }
  // Thread-safety note: the batchers_num_limit_ is not supposed
  // to be accessed or modified from any other thread:
  // no thread-safety is assumed for the kudu::KuduSession interface.
  // Due to the latter reason, there should not be any threads waiting
  // on conditions which are affected by the setting, so no signalling
  // is necessary here.
  // However, the lock is needed to check for pending operations because
  // there may be pending RPCs and the background flush task may be running.
  batchers_num_limit_ = max_num;
  return Status::OK();
}

void KuduSession::Data::SetTimeoutMillis(int timeout_ms) {
  if (timeout_ms < 0) {
    timeout_ms = 0;
  }
  {
    std::lock_guard<Mutex> l(mutex_);
    timeout_ = MonoDelta::FromMilliseconds(timeout_ms);
    if (batcher_) {
      batcher_->SetTimeout(timeout_);
    }
  }
}

void KuduSession::Data::FlushAsync(KuduStatusCallback* cb) {
  // Flush the current batcher if it is non-empty.
  FlushCurrentBatcher(kWatermarkNonEmptyBatcher, cb);
}

Status KuduSession::Data::Flush() {
  // The synchronous flush should initiate flushing of the current batcher,
  // if it exists and has some data, and wait for flush completion of
  // all session's batchers.
  FlushCurrentBatcher(kWatermarkNonEmptyBatcher, nullptr);
  {
    std::lock_guard<Mutex> l(mutex_);
    while (buffer_bytes_used_ > 0) {
      condition_.Wait();
    }
  }
  return error_collector_->CountErrors()
      ? Status::IOError("Some errors occurred") : Status::OK();
}

bool KuduSession::Data::HasPendingOperations() const {
  // Thread-safety note: the buffer_bytes_used_ can be accessed or modified
  // from the threads busy with pending RPCs or from the background flush task.
  std::lock_guard<Mutex> l(mutex_);
  return HasPendingOperationsUnlocked();
}

bool KuduSession::Data::HasPendingOperationsUnlocked() const {
  mutex_.AssertAcquired();
  return buffer_bytes_used_ > 0;
}

int KuduSession::Data::CountBufferedOperations() const {
  std::lock_guard<Mutex> l(mutex_);
  if (batcher_) {
    // Prior batchers (if any) with pending operations are not relevant here:
    // the flushed operations, even if they have not reached the tablet server,
    // are not considered "buffered". Yes, they are "pending",
    // but not "buffered".
    return batcher_->CountBufferedOperations();
  }
  return 0;
}

void KuduSession::Data::FlushCurrentBatcher(int64_t watermark,
                                            KuduStatusCallback* cb) {
  scoped_refptr<Batcher> batcher_to_flush;
  {
    std::lock_guard<Mutex> l(mutex_);
    if (PREDICT_TRUE(batcher_) && batcher_->buffer_bytes_used() >= watermark) {
      batcher_to_flush.swap(batcher_);
    }
  }
  if (batcher_to_flush) {
    // Send off the buffered data. Important to do this outside of the lock
    // since the callback may itself try to take the lock, in the case that
    // the batch fails "inline" on the same thread.
    batcher_to_flush->FlushAsync(cb);
  } else {
    // Nothing to do -- declare a victory.
    if (cb) {
      cb->Run(Status::OK());
    }
  }
}

MonoDelta KuduSession::Data::FlushCurrentBatcher(const MonoDelta& max_age) {
  MonoDelta time_left;
  scoped_refptr<Batcher> batcher_to_flush;
  {
    std::lock_guard<Mutex> l(mutex_);
    if (batcher_) {
      const MonoTime first_op_time = batcher_->first_op_time();
      if (PREDICT_TRUE(first_op_time.Initialized())) {
        const MonoTime now = MonoTime::Now();
        if (first_op_time + max_age <= now) {
          batcher_to_flush.swap(batcher_);
        } else {
          time_left = first_op_time + max_age - now;
        }
      }
    }
  }
  if (batcher_to_flush) {
    // Send off the buffered data. Important to do this outside of the lock
    // since the callback may itself try to take the lock, in the case that
    // the batch fails "inline" on the same thread.
    batcher_to_flush->FlushAsync(nullptr);
  }
  return time_left;
}

// This method takes ownership over the specified write operation. On the return
// from this this method, the operation must end up either in the corresponding
// batcher (success path) or in the error collector (failure path). Otherwise
// it would be a memory leak.
Status KuduSession::Data::ApplyWriteOp(KuduWriteOperation* write_op) {
  if (PREDICT_FALSE(!write_op)) {
    return Status::InvalidArgument("NULL operation");
  }
  if (PREDICT_FALSE(!write_op->row().IsKeySet())) {
    Status status = Status::IllegalState("Key not specified", KUDU_REDACT(write_op->ToString()));
    error_collector_->AddError(unique_ptr<KuduError>(new KuduError(write_op, status)));
    return status;
  }

  // Get 'wire size' of the write operation.
  const int64_t required_size = Batcher::GetOperationSizeInBuffer(write_op);

  const size_t max_size = buffer_bytes_limit_;
  // Thread-safety note: the flush_mode_ is accessed from the background
  // time-based flush task for reading. Practically, it would be possible
  // to get away with not protecting the flush_mode_ since it's read-only
  // access here as well, but TSAN does not like that.
  FlushMode flush_mode;
  {
    std::lock_guard<Mutex> l(mutex_);
    flush_mode = flush_mode_;
  }

  // A sanity check: before trying to validate against any of run-time metrics,
  // verify that the single operation can fit into an empty buffer
  // given the restriction on the buffer size.
  if (PREDICT_FALSE(required_size > max_size)) {
    Status s = Status::Incomplete(strings::Substitute(
          "buffer size limit is too small to fit operation: "
          "required $0, size limit $1",
          required_size, max_size));
    error_collector_->AddError(unique_ptr<KuduError>(new KuduError(write_op, s)));
    return s;
  }

  if (flush_mode == AUTO_FLUSH_BACKGROUND) {
    if (PREDICT_TRUE(buffer_pre_flush_enabled_)) {
      // NOTE: the buffer_pre_flush_enabled_ is set to false only in tests.
      //
      // In need of an extra flush in some cases like shown in the diagram
      // below, otherwise it will require waiting for the time-based flush
      // to happen. Waiting for the time-based flush delays the stream
      // of incoming write operations if such situation happens in the middle.
      // The diagram shows the data layout in the buffer, the flush watermark,
      // and the incoming write operation:
      //                                             +----required_size-----+
      //                                             |                      |
      //                                             | Data of the          |
      //                   +-------max_size-------+  | operation to add.    |
      // flush_watermark-> |                      |  |                      |
      //                   +--buffer_bytes_used---+  +----------0-----------+
      //                   |                      |
      //                   | Data of fresh (newly |
      //                   | added) operations.   |
      //                   |                      |
      //                   +----------------------+
      //                   | Data of operations   |
      //                   | being flushed now.   |
      //                   +----------0-----------+
      FlushCurrentBatcher(max_size - required_size + 1, nullptr);
    }
  }
  {
    std::lock_guard<Mutex> l(mutex_);
    if (flush_mode == AUTO_FLUSH_BACKGROUND) {
      // In AUTO_FLUSH_BACKGROUND mode Apply() blocks if total would-be-used
      // buffer space is over the limit. Once amount of buffered data drops
      // below the limit, a blocking call to Apply() is unblocked.
      while (buffer_bytes_used_ + required_size > max_size) {
        condition_.Wait();
      }
    } else if (PREDICT_FALSE(buffer_bytes_used_ + required_size > max_size)) {
      Status s = Status::Incomplete(strings::Substitute(
          "not enough mutation buffer space remaining for operation: "
          "required additional $0 when $1 of $2 already used",
          required_size, buffer_bytes_used_, max_size));
      error_collector_->AddError(
          unique_ptr<KuduError>(new KuduError(write_op, s)));
      return s;
    }

    // Add the operation to the current batcher. If the current batcher
    // is not there, allocate one and set it to be current.
    if (!batcher_) {
      while (batchers_num_limit_ != 0 &&
             batchers_num_ >= batchers_num_limit_) {
        // Wait until it's possible to add a new batcher given the limit
        // on the maximum outstanding batchers per session.
        condition_.Wait();
      }
      DCHECK(!batcher_);
      // Thread-safety note: the external_consistecy_mode_ and timeout_ms_
      // are not supposed to be accessed or modified from any other thread
      // no thread-safety is advertised for the kudu::KuduSession interface.
      scoped_refptr<Batcher> batcher(
          new Batcher(client_.get(), error_collector_, session_,
                      external_consistency_mode_));
      if (timeout_.Initialized()) {
        batcher->SetTimeout(timeout_);
      }
      batcher.swap(batcher_);
      ++batchers_num_;
    }
    Status op_add_status = batcher_->Add(write_op);
    if (PREDICT_FALSE(!op_add_status.ok())) {
      error_collector_->AddError(
          unique_ptr<KuduError>(new KuduError(write_op, op_add_status)));
      return op_add_status;
    }
    // Finally, update the buffer space usage.
    buffer_bytes_used_ += required_size;
  }

  if (flush_mode == AUTO_FLUSH_BACKGROUND) {
    const size_t flush_watermark =
        buffer_bytes_limit_ * buffer_watermark_pct_ / 100;
    // In AUTO_FLUSH_BACKGROUND mode it's necessary to flush the newly added
    // operations if the flush watermark is reached. The current batcher is
    // the exclusive and the only container for the newly added operations.
    // All other batchers, if any, contain operations which are scheduled
    // to be sent or already on their way to corresponding tablet servers.
    FlushCurrentBatcher(flush_watermark, nullptr);
  }

  return Status::OK();
}

void KuduSession::Data::TimeBasedFlushInit() {
  KuduSession::Data::TimeBasedFlushTask(
      Status::OK(), messenger_, session_, true);
}

void KuduSession::Data::TimeBasedFlushTask(
    const Status& status,
    std::weak_ptr<rpc::Messenger> weak_messenger,
    sp::weak_ptr<KuduSession> weak_session,
    bool do_startup_check) {
  if (PREDICT_FALSE(!status.ok())) {
    return;
  }
  // Check that the session is still alive to access the data safely.
  sp::shared_ptr<KuduSession> session(weak_session.lock());
  if (PREDICT_FALSE(!session)) {
    return;
  }

  KuduSession::Data* data = session->data_;
  MonoDelta max_batcher_age;
  {
    std::lock_guard<Mutex> l(data->mutex_);
    if (do_startup_check && data->flush_task_active_) {
      // The task is already active.
      return;
    }
    if (data->flush_mode_ == AUTO_FLUSH_BACKGROUND) {
      data->flush_task_active_ = true;
    } else {
      // Flush mode could change during the operation. If current mode
      // is no longer AUTO_FLUSH_BACKGROUND, do not re-schedule the task.
      data->flush_task_active_ = false;
      return;
    }
    max_batcher_age = data->flush_interval_;
  }

  // Let's measure the age of a batcher as the time elapsed from the moment
  // of adding the very first operation into the batcher.
  // The idea is to flush the batcher when its age is very close to the
  // flush_interval_: let's call it 'batcher flush age'. If current batcher
  // hasn't reached its flush age yet, just re-schedule the task to
  // re-evaluate the age of then-will-be-current batcher. So, if the current
  // batcher is still current at that time, it will be exactly of its flush age.
  MonoDelta time_left = data->FlushCurrentBatcher(max_batcher_age);
  MonoDelta next_run = time_left.Initialized() ? time_left : max_batcher_age;

  // Re-schedule the task to check and flush the current batcher
  // when its age is closer to the flush_interval_.
  std::shared_ptr<rpc::Messenger> messenger(weak_messenger.lock());
  if (PREDICT_TRUE(messenger)) {
    messenger->ScheduleOnReactor(
        boost::bind(&KuduSession::Data::TimeBasedFlushTask, _1,
                    std::move(weak_messenger), std::move(weak_session), false),
        next_run);
  }
}

int64_t KuduSession::Data::GetPendingOperationsSizeForTests() const {
  std::lock_guard<Mutex> l(mutex_);
  return buffer_bytes_used_;
}

size_t KuduSession::Data::GetBatchersCountForTests() const {
  std::lock_guard<Mutex> l(mutex_);
  return batchers_num_;
}

} // namespace client
} // namespace kudu
