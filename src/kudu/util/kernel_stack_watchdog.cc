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

#include "kudu/util/kernel_stack_watchdog.h"

#include <boost/bind.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <string>

#include "kudu/util/debug-util.h"
#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/thread.h"
#include "kudu/util/status.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"

DEFINE_int32(hung_task_check_interval_ms, 200,
             "Number of milliseconds in between checks for hung threads");
TAG_FLAG(hung_task_check_interval_ms, hidden);

using std::lock_guard;
using std::string;
using strings::Substitute;

namespace kudu {

DEFINE_STATIC_THREAD_LOCAL(KernelStackWatchdog::TLS,
                           KernelStackWatchdog, tls_);

KernelStackWatchdog::KernelStackWatchdog()
  : log_collector_(nullptr),
    finish_(1) {

  // During creation of the stack watchdog thread, we need to disable using
  // the stack watchdog itself. Otherwise, the 'StartThread' function will
  // try to call back into initializing the stack watchdog, and will self-deadlock.
  CHECK_OK(Thread::CreateWithFlags(
      "kernel-watchdog", "kernel-watcher",
      boost::bind(&KernelStackWatchdog::RunThread, this),
      Thread::NO_STACK_WATCHDOG,
      &thread_));
}

KernelStackWatchdog::~KernelStackWatchdog() {
  finish_.CountDown();
  CHECK_OK(ThreadJoiner(thread_.get()).Join());
}

void KernelStackWatchdog::SaveLogsForTests(bool save_logs) {
  lock_guard<simple_spinlock> l(log_lock_);
  if (save_logs) {
    log_collector_.reset(new std::vector<string>());
  } else {
    log_collector_.reset();
  }
}

std::vector<string> KernelStackWatchdog::LoggedMessagesForTests() const {
  lock_guard<simple_spinlock> l(log_lock_);
  CHECK(log_collector_) << "Must call SaveLogsForTests(true) first";
  return *log_collector_;
}

void KernelStackWatchdog::Register(TLS* tls) {
  int64_t tid = Thread::CurrentThreadId();
  lock_guard<simple_spinlock> l(tls_lock_);
  InsertOrDie(&tls_by_tid_, tid, tls);
}

void KernelStackWatchdog::Unregister() {
  int64_t tid = Thread::CurrentThreadId();
  MutexLock l(unregister_lock_);
  lock_guard<simple_spinlock> l2(tls_lock_);
  CHECK(tls_by_tid_.erase(tid));
}

Status GetKernelStack(pid_t p, string* ret) {
  faststring buf;
  RETURN_NOT_OK(ReadFileToString(Env::Default(), Substitute("/proc/$0/stack", p), &buf));
  *ret = buf.ToString();
  return Status::OK();
}

void KernelStackWatchdog::RunThread() {
  while (true) {
    MonoDelta delta = MonoDelta::FromMilliseconds(FLAGS_hung_task_check_interval_ms);
    if (finish_.WaitFor(delta)) {
      // Watchdog exiting.
      break;
    }

    // Prevent threads from unregistering between the snapshot loop and the sending of
    // signals. This makes it safe for us to access their TLS. We might delay the thread
    // exit a bit, but it would be unusual for any code to block on a thread exit, whereas
    // it's relatively important for threads to _start_ quickly.
    MutexLock l(unregister_lock_);

    // Take the snapshot of the thread information under a short lock.
    //
    // 'lock_' prevents new threads from starting, so we don't want to do any lengthy work
    // (such as gathering stack traces) under this lock.
    TLSMap tls_map_copy;
    {
      lock_guard<simple_spinlock> l(tls_lock_);
      tls_map_copy = tls_by_tid_;
    }

    MicrosecondsInt64 now = GetMonoTimeMicros();
    for (const auto& entry : tls_map_copy) {
      pid_t p = entry.first;
      TLS::Data* tls = &entry.second->data_;
      TLS::Data tls_copy;
      tls->SnapshotCopy(&tls_copy);
      for (int i = 0; i < tls_copy.depth_; i++) {
        const TLS::Frame* frame = &tls_copy.frames_[i];

        int paused_ms = (now - frame->start_time_) / 1000;
        if (paused_ms > frame->threshold_ms_) {
          string kernel_stack;
          Status s = GetKernelStack(p, &kernel_stack);
          if (!s.ok()) {
            // Can't read the kernel stack of the pid, just ignore it.
            kernel_stack = "(could not read kernel stack)";
          }

          string user_stack = DumpThreadStack(p);

          lock_guard<simple_spinlock> l(log_lock_);
          LOG_STRING(WARNING, log_collector_.get())
              << "Thread " << p << " stuck at " << frame->status_
              << " for " << paused_ms << "ms" << ":\n"
              << "Kernel stack:\n" << kernel_stack << "\n"
              << "User stack:\n" << user_stack;
        }
      }
    }
  }
}

KernelStackWatchdog::TLS* KernelStackWatchdog::GetTLS() {
  // Disable leak check. LSAN sometimes gets false positives on thread locals.
  // See: https://github.com/google/sanitizers/issues/757
  debug::ScopedLeakCheckDisabler d;
  INIT_STATIC_THREAD_LOCAL(KernelStackWatchdog::TLS, tls_);
  return tls_;
}

KernelStackWatchdog::TLS::TLS() {
  memset(&data_, 0, sizeof(data_));
  KernelStackWatchdog::GetInstance()->Register(this);
}

KernelStackWatchdog::TLS::~TLS() {
  KernelStackWatchdog::GetInstance()->Unregister();
}

// Optimistic concurrency control approach to snapshot the value of another
// thread's TLS, even though that thread might be changing it.
//
// Called by the watchdog thread to see if a target thread is currently in the
// middle of a watched section.
void KernelStackWatchdog::TLS::Data::SnapshotCopy(Data* copy) const {
  while (true) {
    Atomic32 v_0 = base::subtle::Acquire_Load(&seq_lock_);
    if (v_0 & 1) {
      // If the value is odd, then the thread is in the middle of modifying
      // its TLS, and we have to spin.
      base::subtle::PauseCPU();
      continue;
    }
    ANNOTATE_IGNORE_READS_BEGIN();
    memcpy(copy, this, sizeof(*copy));
    ANNOTATE_IGNORE_READS_END();
    Atomic32 v_1 = base::subtle::Release_Load(&seq_lock_);

    // If the value hasn't changed since we started the copy, then
    // we know that the copy was a consistent snapshot.
    if (v_1 == v_0) break;
  }
}

} // namespace kudu
