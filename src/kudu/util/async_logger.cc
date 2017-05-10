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

#include "kudu/util/async_logger.h"

#include <algorithm>
#include <string>
#include <thread>

#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"

using std::string;

namespace kudu {

AsyncLogger::AsyncLogger(google::base::Logger* wrapped,
                         int max_buffer_bytes) :
    max_buffer_bytes_(max_buffer_bytes),
    wrapped_(DCHECK_NOTNULL(wrapped)),
    wake_flusher_cond_(&lock_),
    free_buffer_cond_(&lock_),
    flush_complete_cond_(&lock_),
    active_buf_(new Buffer()),
    flushing_buf_(new Buffer()) {
  DCHECK_GT(max_buffer_bytes_, 0);
}

AsyncLogger::~AsyncLogger() {}

void AsyncLogger::Start() {
  CHECK_EQ(state_, INITTED);
  state_ = RUNNING;
  thread_ = std::thread(&AsyncLogger::RunThread, this);
}

void AsyncLogger::Stop() {
  {
    MutexLock l(lock_);
    CHECK_EQ(state_, RUNNING);
    state_ = STOPPED;
    wake_flusher_cond_.Signal();
  }
  thread_.join();
  CHECK(active_buf_->messages.empty());
  CHECK(flushing_buf_->messages.empty());
}

void AsyncLogger::Write(bool force_flush,
                        time_t timestamp,
                        const char* message,
                        int message_len) {
  {
    MutexLock l(lock_);
    DCHECK_EQ(state_, RUNNING);
    while (BufferFull(*active_buf_)) {
      app_threads_blocked_count_for_tests_++;
      free_buffer_cond_.Wait();
    }
    active_buf_->add(Msg(timestamp, string(message, message_len)),
                     force_flush);
    wake_flusher_cond_.Signal();
  }

  // In most cases, we take the 'force_flush' argument to mean that we'll let the logger
  // thread do the flushing for us, but not block the application. However, for the
  // special case of a FATAL log message, we really want to make sure that our message
  // hits the log before we continue, or else it's likely that the application will exit
  // while it's still in our buffer.
  //
  // NOTE: even if the application doesn't wrap the FATAL-level logger, log messages at
  // FATAL are also written to all other log files with lower levels. So, a FATAL message
  // will force a synchronous flush of all lower-level logs before exiting.
  //
  // Unfortunately, the underlying log level isn't passed through to this interface, so we
  // have to use this hack: messages from FATAL errors start with the character 'F'.
  if (message_len > 0 && message[0] == 'F') {
    Flush();
  }
}

void AsyncLogger::Flush() {
  MutexLock l(lock_);
  DCHECK_EQ(state_, RUNNING);

  // Wake up the writer thread at least twice.
  // This ensures that it has completely flushed both buffers.
  uint64_t orig_flush_count = flush_count_;
  while (flush_count_ < orig_flush_count + 2 &&
         state_ == RUNNING) {
    active_buf_->flush = true;
    wake_flusher_cond_.Signal();
    flush_complete_cond_.Wait();
  }
}

uint32 AsyncLogger::LogSize() {
  return wrapped_->LogSize();
}

void AsyncLogger::RunThread() {
  MutexLock l(lock_);
  while (state_ == RUNNING || active_buf_->needs_flush_or_write()) {
    while (!active_buf_->needs_flush_or_write() && state_ == RUNNING) {
      if (!wake_flusher_cond_.TimedWait(MonoDelta::FromSeconds(FLAGS_logbufsecs))) {
        // In case of wait timeout, force it to flush regardless whether there is anything enqueued.
        active_buf_->flush = true;
      }
    }

    active_buf_.swap(flushing_buf_);
    // If the buffer that we are about to flush was full, then
    // we may have other threads which were blocked that we now
    // need to wake up.
    if (BufferFull(*flushing_buf_)) {
      free_buffer_cond_.Broadcast();
    }
    l.Unlock();

    for (const auto& msg : flushing_buf_->messages) {
      wrapped_->Write(false, msg.ts, msg.message.data(), msg.message.size());
    }
    if (flushing_buf_->flush) {
      wrapped_->Flush();
    }
    flushing_buf_->clear();

    l.Lock();
    flush_count_++;
    flush_complete_cond_.Broadcast();
  }
}

bool AsyncLogger::BufferFull(const Buffer& buf) const {
  // We evenly divide our total buffer space between the two buffers.
  return buf.size > (max_buffer_bytes_ / 2);
}

} // namespace kudu
