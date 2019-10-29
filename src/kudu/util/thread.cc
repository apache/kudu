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
//
// Copied from Impala and adapted to Kudu.

#include "kudu/util/thread.h"

#if defined(__linux__)
#include <sys/prctl.h>
#endif // defined(__linux__)
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <memory>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/bind.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/once.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/easy_json.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/kernel_stack_watchdog.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/os-util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/trace.h"
#include "kudu/util/url-coding.h"
#include "kudu/util/web_callback_registry.h"

using boost::bind;
using boost::mem_fn;
using std::ostringstream;
using std::pair;
using std::shared_ptr;
using std::string;
using std::vector;
using std::unordered_map;
using strings::Substitute;

METRIC_DEFINE_gauge_uint64(server, threads_started,
                           "Threads Started",
                           kudu::MetricUnit::kThreads,
                           "Total number of threads started on this server",
                           kudu::EXPOSE_AS_COUNTER);

METRIC_DEFINE_gauge_uint64(server, threads_running,
                           "Threads Running",
                           kudu::MetricUnit::kThreads,
                           "Current number of running threads");

METRIC_DEFINE_gauge_uint64(server, cpu_utime,
                           "User CPU Time",
                           kudu::MetricUnit::kMilliseconds,
                           "Total user CPU time of the process",
                           kudu::EXPOSE_AS_COUNTER);

METRIC_DEFINE_gauge_uint64(server, cpu_stime,
                           "System CPU Time",
                           kudu::MetricUnit::kMilliseconds,
                           "Total system CPU time of the process",
                           kudu::EXPOSE_AS_COUNTER);

METRIC_DEFINE_gauge_uint64(server, voluntary_context_switches,
                           "Voluntary Context Switches",
                           kudu::MetricUnit::kContextSwitches,
                           "Total voluntary context switches",
                           kudu::EXPOSE_AS_COUNTER);

METRIC_DEFINE_gauge_uint64(server, involuntary_context_switches,
                           "Involuntary Context Switches",
                           kudu::MetricUnit::kContextSwitches,
                           "Total involuntary context switches",
                           kudu::EXPOSE_AS_COUNTER);

DEFINE_int32(thread_inject_start_latency_ms, 0,
             "Number of ms to sleep when starting a new thread. (For tests).");
TAG_FLAG(thread_inject_start_latency_ms, hidden);
TAG_FLAG(thread_inject_start_latency_ms, unsafe);

namespace kudu {

static uint64_t GetCpuUTime() {
  rusage ru;
  CHECK_ERR(getrusage(RUSAGE_SELF, &ru));
  return ru.ru_utime.tv_sec * 1000UL + ru.ru_utime.tv_usec / 1000UL;
}

static uint64_t GetCpuSTime() {
  rusage ru;
  CHECK_ERR(getrusage(RUSAGE_SELF, &ru));
  return ru.ru_stime.tv_sec * 1000UL + ru.ru_stime.tv_usec / 1000UL;
}

static uint64_t GetVoluntaryContextSwitches() {
  rusage ru;
  CHECK_ERR(getrusage(RUSAGE_SELF, &ru));
  return ru.ru_nvcsw;
}

static uint64_t GetInVoluntaryContextSwitches() {
  rusage ru;
  CHECK_ERR(getrusage(RUSAGE_SELF, &ru));
  return ru.ru_nivcsw;
}

class ThreadMgr;

__thread Thread* Thread::tls_ = nullptr;

// Singleton instance of ThreadMgr. Only visible in this file, used only by Thread.
// The Thread class adds a reference to thread_manager while it is supervising a thread so
// that a race between the end of the process's main thread (and therefore the destruction
// of thread_manager) and the end of a thread that tries to remove itself from the
// manager after the destruction can be avoided.
static shared_ptr<ThreadMgr> thread_manager;

// Controls the single (lazy) initialization of thread_manager.
static GoogleOnceType once = GOOGLE_ONCE_INIT;

// A singleton class that tracks all live threads, and groups them together for easy
// auditing. Used only by Thread.
class ThreadMgr {
 public:
  ThreadMgr()
      : threads_started_metric_(0),
        threads_running_metric_(0) {
  }

  ~ThreadMgr() {
    thread_categories_.clear();
  }

  static void SetThreadName(const string& name, int64_t tid);

  Status StartInstrumentation(const scoped_refptr<MetricEntity>& metrics,
                              WebCallbackRegistry* web) const;

  // Registers a thread to the supplied category. The key is a pthread_t,
  // not the system TID, since pthread_t is less prone to being recycled.
  void AddThread(const pthread_t& pthread_id, const string& name, const string& category,
      int64_t tid);

  // Removes a thread from the supplied category. If the thread has
  // already been removed, this is a no-op.
  void RemoveThread(const pthread_t& pthread_id, const string& category);

  // Metric callback for number of threads running. Also used for error messages.
  uint64_t ReadThreadsRunning() const;

 private:
  // Container class for any details we want to capture about a thread
  // TODO: Add start-time.
  // TODO: Track fragment ID.
  class ThreadDescriptor {
   public:
    ThreadDescriptor() { }
    ThreadDescriptor(string category, string name, int64_t thread_id)
        : name_(std::move(name)),
          category_(std::move(category)),
          thread_id_(thread_id) {}

    const string& name() const { return name_; }
    const string& category() const { return category_; }
    int64_t thread_id() const { return thread_id_; }

    struct Comparator {
      bool operator()(const ThreadDescriptor& rhs, const ThreadDescriptor& lhs) const {
        return rhs.name() < lhs.name();
      }
    };

   private:
    string name_;
    string category_;
    int64_t thread_id_;
  };

  struct ThreadIdHash {
    size_t operator()(pthread_t thread_id) const noexcept {
      return std::hash<pthread_t>()(thread_id);
    }
  };

  struct ThreadIdEqual {
    bool operator()(pthread_t lhs, pthread_t rhs) const {
      return pthread_equal(lhs, rhs) != 0;
    }
  };

  // A ThreadCategory is a set of threads that are logically related.
  typedef unordered_map<const pthread_t, ThreadDescriptor,
                        ThreadIdHash, ThreadIdEqual> ThreadCategory;

  // All thread categories, keyed on the category name.
  typedef unordered_map<string, ThreadCategory> ThreadCategoryMap;

  // Protects thread_categories_ and thread metrics.
  mutable rw_spinlock lock_;

  // All thread categories that ever contained a thread, even if empty.
  ThreadCategoryMap thread_categories_;

  // Counters to track all-time total number of threads, and the
  // current number of running threads.
  uint64_t threads_started_metric_;
  uint64_t threads_running_metric_;

  // Metric callback for number of threads started.
  uint64_t ReadThreadsStarted() const;

  // Webpage callback; prints all threads by category.
  void ThreadPathHandler(const WebCallbackRegistry::WebRequest& req,
                         WebCallbackRegistry::WebResponse* resp) const;
  void SummarizeThreadDescriptor(const ThreadDescriptor& desc,
                                 EasyJson* output) const;
};

void ThreadMgr::SetThreadName(const string& name, int64_t tid) {
  // On linux we can get the thread names to show up in the debugger by setting
  // the process name for the LWP.  We don't want to do this for the main
  // thread because that would rename the process, causing tools like killall
  // to stop working.
  if (tid == getpid()) {
    return;
  }

#if defined(__linux__)
  // http://0pointer.de/blog/projects/name-your-threads.html
  // Set the name for the LWP (which gets truncated to 15 characters).
  // Note that glibc also has a 'pthread_setname_np' api, but it may not be
  // available everywhere and it's only benefit over using prctl directly is
  // that it can set the name of threads other than the current thread.
  int err = prctl(PR_SET_NAME, name.c_str());
#else
  int err = pthread_setname_np(name.c_str());
#endif // defined(__linux__)
  // We expect EPERM failures in sandboxed processes, just ignore those.
  if (err < 0 && errno != EPERM) {
    PLOG(ERROR) << "SetThreadName";
  }
}

Status ThreadMgr::StartInstrumentation(const scoped_refptr<MetricEntity>& metrics,
                                       WebCallbackRegistry* web) const {
  // Use function gauges here so that we can register a unique copy of these metrics in
  // multiple tservers, even though the ThreadMgr is itself a singleton.
  metrics->NeverRetire(
      METRIC_threads_started.InstantiateFunctionGauge(metrics,
        Bind(&ThreadMgr::ReadThreadsStarted, Unretained(this))));
  metrics->NeverRetire(
      METRIC_threads_running.InstantiateFunctionGauge(metrics,
        Bind(&ThreadMgr::ReadThreadsRunning, Unretained(this))));
  metrics->NeverRetire(
      METRIC_cpu_utime.InstantiateFunctionGauge(metrics,
        Bind(&GetCpuUTime)));
  metrics->NeverRetire(
      METRIC_cpu_stime.InstantiateFunctionGauge(metrics,
        Bind(&GetCpuSTime)));
  metrics->NeverRetire(
      METRIC_voluntary_context_switches.InstantiateFunctionGauge(metrics,
        Bind(&GetVoluntaryContextSwitches)));
  metrics->NeverRetire(
      METRIC_involuntary_context_switches.InstantiateFunctionGauge(metrics,
        Bind(&GetInVoluntaryContextSwitches)));

  if (web) {
    auto thread_callback = bind<void>(mem_fn(&ThreadMgr::ThreadPathHandler),
                                      this, _1, _2);
    DCHECK_NOTNULL(web)->RegisterPathHandler("/threadz", "Threads", thread_callback,
                                             /* is_styled= */ true,
                                             /* is_on_nav_bar= */ true);
  }
  return Status::OK();
}

uint64_t ThreadMgr::ReadThreadsStarted() const {
  shared_lock<decltype(lock_)> l(lock_);
  return threads_started_metric_;
}

uint64_t ThreadMgr::ReadThreadsRunning() const {
  shared_lock<decltype(lock_)> l(lock_);
  return threads_running_metric_;
}

void ThreadMgr::AddThread(const pthread_t& pthread_id, const string& name,
    const string& category, int64_t tid) {
  // These annotations cause TSAN to ignore the synchronization on lock_
  // without causing the subsequent mutations to be treated as data races
  // in and of themselves (that's what IGNORE_READS_AND_WRITES does).
  //
  // Why do we need them here and in SuperviseThread()? TSAN operates by
  // observing synchronization events and using them to establish "happens
  // before" relationships between threads. Where these relationships are
  // not built, shared state access constitutes a data race. The
  // synchronization events here, in RemoveThread(), and in
  // SuperviseThread() may cause TSAN to establish a "happens before"
  // relationship between thread functors, ignoring potential data races.
  // The annotations prevent this from happening.
  ANNOTATE_IGNORE_SYNC_BEGIN();
  ANNOTATE_IGNORE_READS_AND_WRITES_BEGIN();
  {
    // NOTE: Not using EmplaceOrDie() here -- that's because in environments
    //   where fork() is called after some threads have been spawned, child
    //   processes will inadvertently inherit the contents of the thread
    //   registry (i.e. the entries in the thread_categories_ container).
    //   For some platforms, pthread_t handles for threads in different
    //   processes might be the same, so using EmplaceOrDie() would induce
    //   a crash when ThreadMgr::AddThread() is called for a new thread
    //   in the child process.
    //
    // TODO(aserbin): maybe, keep the thread_categories_ registry not in a
    //   global static container, but bind the container with the life cycle
    //   of some top-level object that uses the ThreadMgr as a singleton.
    std::lock_guard<decltype(lock_)> l(lock_);
    thread_categories_[category][pthread_id] =
        ThreadDescriptor(category, name, tid);
    ++threads_running_metric_;
    ++threads_started_metric_;
  }
  ANNOTATE_IGNORE_SYNC_END();
  ANNOTATE_IGNORE_READS_AND_WRITES_END();
}

void ThreadMgr::RemoveThread(const pthread_t& pthread_id, const string& category) {
  ANNOTATE_IGNORE_SYNC_BEGIN();
  ANNOTATE_IGNORE_READS_AND_WRITES_BEGIN();
  {
    std::lock_guard<decltype(lock_)> l(lock_);
    auto& threads = FindOrDie(thread_categories_, category);
    auto num_erased = threads.erase(pthread_id);
    CHECK_EQ(1, num_erased);
    --threads_running_metric_;
  }
  ANNOTATE_IGNORE_SYNC_END();
  ANNOTATE_IGNORE_READS_AND_WRITES_END();
}

void ThreadMgr::SummarizeThreadDescriptor(const ThreadDescriptor& desc,
                                          EasyJson* output) const {
  ThreadStats stats;
  Status status = GetThreadStats(desc.thread_id(), &stats);
  if (!status.ok()) {
    KLOG_EVERY_N(INFO, 100) << "Could not get per-thread statistics: "
                            << status.ToString();
  }
  EasyJson thr = output->PushBack(EasyJson::kObject);
  thr["thread_name"] = desc.name();
  thr["user_sec"] = static_cast<double>(stats.user_ns) / 1e9;
  thr["kernel_sec"] = static_cast<double>(stats.kernel_ns) / 1e9;
  thr["iowait_sec"] = static_cast<double>(stats.iowait_ns) / 1e9;
}

void ThreadMgr::ThreadPathHandler(const WebCallbackRegistry::WebRequest& req,
                                  WebCallbackRegistry::WebResponse* resp) const {
  EasyJson& output = resp->output;
  const auto* category_name = FindOrNull(req.parsed_args, "group");
  if (category_name) {
    // List all threads belonging to the desired thread group.
    bool requested_all = *category_name == "all";
    EasyJson rtg = output.Set("requested_thread_group", EasyJson::kObject);
    rtg["group_name"] = EscapeForHtmlToString(*category_name);
    rtg["requested_all"] = requested_all;

    // The critical section is as short as possible so as to minimize the delay
    // imposed on new threads that acquire the lock in write mode.
    vector<ThreadDescriptor> descriptors_to_print;
    if (!requested_all) {
      shared_lock<decltype(lock_)> l(lock_);
      const auto* category = FindOrNull(thread_categories_, *category_name);
      if (!category) {
        return;
      }
      for (const auto& elem : *category) {
        descriptors_to_print.emplace_back(elem.second);
      }
    } else {
      shared_lock<decltype(lock_)> l(lock_);
      for (const auto& category : thread_categories_) {
        for (const auto& elem : category.second) {
          descriptors_to_print.emplace_back(elem.second);
        }
      }
    }

    EasyJson found = rtg.Set("found", EasyJson::kObject);
    EasyJson threads = found.Set("threads", EasyJson::kArray);
    for (const auto& desc : descriptors_to_print) {
      SummarizeThreadDescriptor(desc, &threads);
    }
  } else {
    // List all thread groups and the number of threads running in each.
    vector<pair<string, uint64_t>> thread_categories_info;
    uint64_t running;
    {
      // See comment above regarding short critical sections.
      shared_lock<decltype(lock_)> l(lock_);
      running = threads_running_metric_;
      thread_categories_info.reserve(thread_categories_.size());
      for (const auto& category : thread_categories_) {
        thread_categories_info.emplace_back(category.first, category.second.size());
      }
    }

    output["total_threads_running"] = running;
    EasyJson groups = output.Set("groups", EasyJson::kArray);
    for (const auto& elem : thread_categories_info) {
      string category_arg;
      if (WebCallbackRegistry::IsProxiedViaKnox(req)) {
        // Knox encodes query parameter values when it rewrites HTTP responses.
        // If we also encoded, we'd end up with broken URLs. For example, we'd
        // encode the query parameter 'group=service pool' to
        // 'group=service%20pool', then Knox would encode it again to
        // 'group=service%2520pool'.
        category_arg = elem.first;
      } else {
        UrlEncode(elem.first, &category_arg);
      }
      EasyJson g = groups.PushBack(EasyJson::kObject);
      g["encoded_group_name"] = category_arg;
      g["group_name"] = elem.first;
      g["threads_running"] = elem.second;
    }
  }
}

static void InitThreading() {
  thread_manager.reset(new ThreadMgr());
}

Status StartThreadInstrumentation(const scoped_refptr<MetricEntity>& server_metrics,
                                  WebCallbackRegistry* web) {
  GoogleOnceInit(&once, &InitThreading);
  return thread_manager->StartInstrumentation(server_metrics, web);
}

ThreadJoiner::ThreadJoiner(Thread* thr)
  : thread_(CHECK_NOTNULL(thr)),
    warn_after_ms_(kDefaultWarnAfterMs),
    warn_every_ms_(kDefaultWarnEveryMs),
    give_up_after_ms_(kDefaultGiveUpAfterMs) {
}

ThreadJoiner& ThreadJoiner::warn_after_ms(int ms) {
  warn_after_ms_ = ms;
  return *this;
}

ThreadJoiner& ThreadJoiner::warn_every_ms(int ms) {
  warn_every_ms_ = ms;
  return *this;
}

ThreadJoiner& ThreadJoiner::give_up_after_ms(int ms) {
  give_up_after_ms_ = ms;
  return *this;
}

Status ThreadJoiner::Join() {
  if (Thread::current_thread() &&
      Thread::current_thread()->tid() == thread_->tid()) {
    return Status::InvalidArgument("Can't join on own thread", thread_->name_);
  }

  // Early exit: double join is a no-op.
  if (!thread_->joinable_) {
    return Status::OK();
  }

  int waited_ms = 0;
  bool keep_trying = true;
  while (keep_trying) {
    if (waited_ms >= warn_after_ms_) {
      LOG(WARNING) << Substitute("Waited for $0ms trying to join with $1 (tid $2)",
                                 waited_ms, thread_->name_, thread_->tid_);
    }

    int remaining_before_giveup = MathLimits<int>::kMax;
    if (give_up_after_ms_ != -1) {
      remaining_before_giveup = give_up_after_ms_ - waited_ms;
    }

    int remaining_before_next_warn = warn_every_ms_;
    if (waited_ms < warn_after_ms_) {
      remaining_before_next_warn = warn_after_ms_ - waited_ms;
    }

    if (remaining_before_giveup < remaining_before_next_warn) {
      keep_trying = false;
    }

    int wait_for = std::min(remaining_before_giveup, remaining_before_next_warn);

    if (thread_->done_.WaitFor(MonoDelta::FromMilliseconds(wait_for))) {
      // Unconditionally join before returning, to guarantee that any TLS
      // has been destroyed (pthread_key_create() destructors only run
      // after a pthread's user method has returned).
      int ret = pthread_join(thread_->thread_, nullptr);
      CHECK_EQ(ret, 0);
      thread_->joinable_ = false;
      return Status::OK();
    }
    waited_ms += wait_for;
  }
  return Status::Aborted(strings::Substitute("Timed out after $0ms joining on $1",
                                             waited_ms, thread_->name_));
}

Thread::~Thread() {
  if (joinable_) {
    int ret = pthread_detach(thread_);
    CHECK_EQ(ret, 0);
  }
}

string Thread::ToString() const {
  return Substitute("Thread $0 (name: \"$1\", category: \"$2\")", tid(), name_, category_);
}

int64_t Thread::WaitForTid() const {
  const string log_prefix = Substitute("$0 ($1) ", name_, category_);
  SCOPED_LOG_SLOW_EXECUTION_PREFIX(WARNING, 500 /* ms */, log_prefix,
                                   "waiting for new thread to publish its TID");
  int loop_count = 0;
  while (true) {
    int64_t t = Acquire_Load(&tid_);
    if (t != PARENT_WAITING_TID) return t;
    boost::detail::yield(loop_count++);
  }
}


Status Thread::StartThread(const string& category, const string& name,
                           const ThreadFunctor& functor, uint64_t flags,
                           scoped_refptr<Thread> *holder) {
  TRACE_COUNTER_INCREMENT("threads_started", 1);
  TRACE_COUNTER_SCOPE_LATENCY_US("thread_start_us");
  GoogleOnceInit(&once, &InitThreading);

  const string log_prefix = Substitute("$0 ($1) ", name, category);
  SCOPED_LOG_SLOW_EXECUTION_PREFIX(WARNING, 500 /* ms */, log_prefix, "starting thread");

  // Temporary reference for the duration of this function.
  scoped_refptr<Thread> t(new Thread(category, name, functor));

  // Optional, and only set if the thread was successfully created.
  //
  // We have to set this before we even start the thread because it's
  // allowed for the thread functor to access 'holder'.
  if (holder) {
    *holder = t;
  }

  t->tid_ = PARENT_WAITING_TID;

  // Add a reference count to the thread since SuperviseThread() needs to
  // access the thread object, and we have no guarantee that our caller
  // won't drop the reference as soon as we return. This is dereferenced
  // in FinishThread().
  t->AddRef();

  auto cleanup = MakeScopedCleanup([&]() {
      // If we failed to create the thread, we need to undo all of our prep work.
      t->tid_ = INVALID_TID;
      t->Release();
    });

  if (PREDICT_FALSE(FLAGS_thread_inject_start_latency_ms > 0)) {
    LOG(INFO) << "Injecting " << FLAGS_thread_inject_start_latency_ms << "ms sleep on thread start";
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_thread_inject_start_latency_ms));
  }

  {
    SCOPED_LOG_SLOW_EXECUTION_PREFIX(WARNING, 500 /* ms */, log_prefix, "creating pthread");
    SCOPED_WATCH_STACK((flags & NO_STACK_WATCHDOG) ? 0 : 250);
    int ret = pthread_create(&t->thread_, nullptr, &Thread::SuperviseThread, t.get());
    if (ret) {
      string msg = "";
      if (ret == EAGAIN) {
        uint64_t rlimit_nproc = Env::Default()->GetResourceLimit(
            Env::ResourceLimitType::RUNNING_THREADS_PER_EUID);
        uint64_t num_threads = thread_manager->ReadThreadsRunning();
        msg = Substitute(" ($0 Kudu-managed threads running in this process, "
                         "$1 max processes allowed for current user)",
                         num_threads, rlimit_nproc);
      }
      return Status::RuntimeError(Substitute("Could not create thread$0", msg), strerror(ret), ret);
    }
  }

  // The thread has been created and is now joinable.
  //
  // Why set this in the parent and not the child? Because only the parent
  // (or someone communicating with the parent) can join, so joinable must
  // be set before the parent returns.
  t->joinable_ = true;
  cleanup.cancel();

  VLOG(2) << "Started thread " << t->tid()<< " - " << category << ":" << name;
  return Status::OK();
}

void* Thread::SuperviseThread(void* arg) {
  Thread* t = static_cast<Thread*>(arg);
  int64_t system_tid = Thread::CurrentThreadId();
  PCHECK(system_tid != -1);

  // Take an additional reference to the thread manager, which we'll need below.
  ANNOTATE_IGNORE_SYNC_BEGIN();
  shared_ptr<ThreadMgr> thread_mgr_ref = thread_manager;
  ANNOTATE_IGNORE_SYNC_END();

  // Set up the TLS.
  //
  // We could store a scoped_refptr in the TLS itself, but as its
  // lifecycle is poorly defined, we'll use a bare pointer. We
  // already incremented the reference count in StartThread.
  Thread::tls_ = t;

  // Publish our tid to 'tid_', which unblocks any callers waiting in
  // WaitForTid().
  Release_Store(&t->tid_, system_tid);

  string name = strings::Substitute("$0-$1", t->name(), system_tid);
  thread_manager->SetThreadName(name, t->tid_);
  thread_manager->AddThread(pthread_self(), name, t->category(), t->tid_);

  // FinishThread() is guaranteed to run (even if functor_ throws an
  // exception) because pthread_cleanup_push() creates a scoped object
  // whose destructor invokes the provided callback.
  pthread_cleanup_push(&Thread::FinishThread, t);
  t->functor_();
  pthread_cleanup_pop(true);

  return nullptr;
}

void Thread::FinishThread(void* arg) {
  Thread* t = static_cast<Thread*>(arg);

  // We're here either because of the explicit pthread_cleanup_pop() in
  // SuperviseThread() or through pthread_exit(). In either case,
  // thread_manager is guaranteed to be live because thread_mgr_ref in
  // SuperviseThread() is still live.
  thread_manager->RemoveThread(pthread_self(), t->category());

  // Signal any Joiner that we're done.
  t->done_.CountDown();

  VLOG(2) << "Ended thread " << t->tid_ << " - " << t->category() << ":" << t->name();
  t->Release();
  // NOTE: the above 'Release' call could be the last reference to 'this',
  // so 'this' could be destructed at this point. Do not add any code
  // following here!
}

} // namespace kudu
