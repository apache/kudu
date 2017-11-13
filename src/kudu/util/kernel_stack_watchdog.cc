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

#include <cstdint>
#include <cstring>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include <boost/bind.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

DEFINE_int32(hung_task_check_interval_ms, 200,
             "Number of milliseconds in between checks for hung threads");
TAG_FLAG(hung_task_check_interval_ms, hidden);

DEFINE_int32(inject_latency_on_kernel_stack_lookup_ms, 0,
             "Number of milliseconds of latency to inject when reading a thread's "
             "kernel stack");
TAG_FLAG(inject_latency_on_kernel_stack_lookup_ms, hidden);

using std::lock_guard;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

__thread KernelStackWatchdog::TLS* KernelStackWatchdog::tls_;

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

  std::unique_ptr<TLS> tls(tls_);
  {
    std::unique_lock<Mutex> l(unregister_lock_, std::try_to_lock);
    lock_guard<simple_spinlock> l2(tls_lock_);
    CHECK(tls_by_tid_.erase(tid));
    if (!l.owns_lock()) {
      // The watchdog is in the middle of running and might be accessing
      // 'tls', so just enqueue it for later deletion. Otherwise it
      // will go out of scope at the end of this function and get
      // deleted here.
      pending_delete_.emplace_back(std::move(tls));
    }
  }
  tls_ = nullptr;
}

Status GetKernelStack(pid_t p, string* ret) {
  MAYBE_INJECT_FIXED_LATENCY(FLAGS_inject_latency_on_kernel_stack_lookup_ms);
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

    // Prevent threads from deleting their TLS objects between the snapshot loop and the sending of
    // signals. This makes it safe for us to access their TLS.
    //
    // NOTE: it's still possible that the thread will have exited in between grabbing its pointer
    // and sending a signal, but DumpThreadStack() already is safe about not sending a signal
    // to some other non-Kudu thread.
    MutexLock l(unregister_lock_);

    // Take the snapshot of the thread information under a short lock.
    //
    // 'tls_lock_' prevents new threads from starting, so we don't want to do any lengthy work
    // (such as gathering stack traces) under this lock.
    TLSMap tls_map_copy;
    vector<unique_ptr<TLS>> to_delete;
    {
      lock_guard<simple_spinlock> l(tls_lock_);
      to_delete.swap(pending_delete_);
      tls_map_copy = tls_by_tid_;
    }
    // Actually delete the no-longer-used TLS entries outside of the lock.
    to_delete.clear();

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

          // If the thread exited the frame we're looking at in between when we started
          // grabbing the stack and now, then our stack isn't correct. We shouldn't log it.
          //
          // We just use unprotected reads here since this is a somewhat best-effort
          // check.
          if (ANNOTATE_UNPROTECTED_READ(tls->depth_) < tls_copy.depth_ ||
              ANNOTATE_UNPROTECTED_READ(tls->frames_[i].start_time_) != frame->start_time_) {
            break;
          }

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

void KernelStackWatchdog::ThreadExiting(void* /* unused */) {
  KernelStackWatchdog::GetInstance()->Unregister();
}

void KernelStackWatchdog::CreateAndRegisterTLS() {
  DCHECK(!tls_);
  // Disable leak check. LSAN sometimes gets false positives on thread locals.
  // See: https://github.com/google/sanitizers/issues/757
  debug::ScopedLeakCheckDisabler d;
  auto* tls = new TLS();
  KernelStackWatchdog::GetInstance()->Register(tls);
  tls_ = tls;

  // We manually install a thread-exit function here making use of the internal
  // functionality of the thread-local module, rather than using Thread::CallAtExit().
  // This is because we may use the stack watchdog in contexts such as the client
  // where it's likely that threads aren't associated with a kudu::Thread instance.
  auto* dtor_list = new threadlocal::internal::PerThreadDestructorList();
  dtor_list->destructor = &ThreadExiting;
  dtor_list->arg = nullptr;
  kudu::threadlocal::internal::AddDestructor(dtor_list);
}

KernelStackWatchdog::TLS::TLS() {
  memset(&data_, 0, sizeof(data_));
}

KernelStackWatchdog::TLS::~TLS() {
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
