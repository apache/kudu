// Copyright (c) 2014, Cloudera, inc.
//
// Copied from Impala. Changes include:
// - Namespace + imports.
// - Adapted to Kudu metrics library.
// - Removal of ThreadGroups.
// - Switched from promise to spinlock in SuperviseThread to RunThread
//   communication.
// - Fixes for cpplint.
// - Added spinlock for protection against KUDU-11.
// - Replaced boost exception throwing on thread creation with status.
// - Added ThreadJoiner.
// - Added prctl(PR_SET_NAME) to name threads.
// - Added current_thread() abstraction using TLS.
// - Used GoogleOnce to make thread_manager initialization lazy.
// - Switched shared_ptr from boost to tr1.
// - Added ThreadMgr::ScopedRemoveThread for exception safety in Thread::SuperviseThread.

#include "util/thread.h"

#include <algorithm>
#include <boost/foreach.hpp>
#include <boost/system/system_error.hpp>
#include <map>
#include <set>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <tr1/memory>
#include <unistd.h>
#include <vector>

#include "gutil/atomicops.h"
#include "gutil/mathlimits.h"
#include "gutil/once.h"
#include "gutil/strings/substitute.h"
#include "util/debug-util.h"
#include "util/errno.h"
#include "util/metrics.h"
#include "util/url-coding.h"
#include "util/os-util.h"
#include "util/web_callback_registry.h"

using boost::bind;
using boost::lock_guard;
using boost::mem_fn;
using boost::mutex;
using std::tr1::shared_ptr;
using boost::thread;
using boost::thread_resource_error;
using std::endl;
using std::map;
using std::stringstream;

namespace kudu {

class ThreadMgr;

METRIC_DEFINE_gauge_uint64(total_threads, MetricUnit::kThreads,
                           "All time total number of threads");
METRIC_DEFINE_gauge_uint64(current_num_threads, MetricUnit::kThreads,
                           "Current number of running threads");
__thread Thread* Thread::tls_ = NULL;

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
  ThreadMgr() : metrics_enabled_(false) { }

  ~ThreadMgr() {
    lock_guard<mutex> l(lock_);
    thread_categories_.clear();
  }

  Status StartInstrumentation(MetricRegistry* metric, WebCallbackRegistry* web);

  // Registers a thread to the supplied category. The key is a boost::thread::id, used
  // instead of the system TID since boost::thread::id is always available, unlike
  // gettid() which might fail.
  void AddThread(const thread::id& thread, const string& name, const string& category,
      int64_t tid);

  // Removes a thread from the supplied category. If the thread has
  // already been removed, this is a no-op.
  void RemoveThread(const thread::id& boost_id, const string& category);

  // Helper for ensuring exception safety: calls RemoveThread() once
  // we exit the current scope.
  class ScopedRemoveThread {
   public:
    // Note: all parameters passed in to this constructor must remain
    // valid until the object exits the scope.
    ScopedRemoveThread(ThreadMgr* mgr, const thread::id& boost_id, const string& category)
        : mgr_(mgr), boost_id_(boost_id), category_(category) {
    }

    ~ScopedRemoveThread() {
      mgr_->RemoveThread(boost_id_, category_);
    }
   private:
    ThreadMgr* mgr_;
    const thread::id& boost_id_;
    const string& category_;
  };

 private:
  // Container class for any details we want to capture about a thread
  // TODO: Add start-time.
  // TODO: Track fragment ID.
  class ThreadDescriptor {
   public:
    ThreadDescriptor() { }
    ThreadDescriptor(const string& category, const string& name, int64_t thread_id)
        : name_(name), category_(category), thread_id_(thread_id) {
    }

    const string& name() const { return name_; }
    const string& category() const { return category_; }
    int64_t thread_id() const { return thread_id_; }

   private:
    string name_;
    string category_;
    int64_t thread_id_;
  };

  // A ThreadCategory is a set of threads that are logically related.
  // TODO: unordered_map is incompatible with boost::thread::id, but would be more
  // efficient here.
  typedef map<const thread::id, ThreadDescriptor> ThreadCategory;

  // All thread categorys, keyed on the category name.
  typedef map<string, ThreadCategory> ThreadCategoryMap;

  // Protects thread_categories_ and metrics_enabled_
  mutex lock_;

  // All thread categorys that ever contained a thread, even if empty
  ThreadCategoryMap thread_categories_;

  // True after StartInstrumentation(..) returns
  bool metrics_enabled_;

  // Counters to track all-time total number of threads, and the
  // current number of running threads.
  uint64 total_threads_metric_;
  uint64 current_num_threads_metric_;

  // Metric callbacks.
  uint64 ReadNumTotalThreads();
  uint64 ReadNumCurrentThreads();

  // Webpage callback; prints all threads by category
  void ThreadPathHandler(const WebCallbackRegistry::ArgumentMap& args, stringstream* output);
  void PrintThreadCategoryRows(const ThreadCategory& category, stringstream* output);
};

Status ThreadMgr::StartInstrumentation(MetricRegistry* metric, WebCallbackRegistry* web) {
  MetricContext ctx(DCHECK_NOTNULL(metric), "threading");
  lock_guard<mutex> l(lock_);
  metrics_enabled_ = true;

  // TODO: These metrics should be expressed as counters but their lifecycles
  // are tough to define because ThreadMgr is a singleton.
  METRIC_total_threads.InstantiateFunctionGauge(ctx,
      bind(&ThreadMgr::ReadNumTotalThreads, this));
  METRIC_current_num_threads.InstantiateFunctionGauge(ctx,
      bind(&ThreadMgr::ReadNumCurrentThreads, this));

  WebCallbackRegistry::PathHandlerCallback thread_callback =
      bind<void>(mem_fn(&ThreadMgr::ThreadPathHandler), this, _1, _2);
  DCHECK_NOTNULL(web)->RegisterPathHandler("/threadz", thread_callback);
  return Status::OK();
}

uint64 ThreadMgr::ReadNumTotalThreads() {
  lock_guard<mutex> l(lock_);
  return total_threads_metric_;
}

uint64 ThreadMgr::ReadNumCurrentThreads() {
  lock_guard<mutex> l(lock_);
  return current_num_threads_metric_;
}

void ThreadMgr::AddThread(const thread::id& thread, const string& name,
    const string& category, int64_t tid) {
  lock_guard<mutex> l(lock_);
  thread_categories_[category][thread] = ThreadDescriptor(category, name, tid);
  if (metrics_enabled_) {
    current_num_threads_metric_++;
    total_threads_metric_++;
  }
}

void ThreadMgr::RemoveThread(const thread::id& boost_id, const string& category) {
  lock_guard<mutex> l(lock_);
  ThreadCategoryMap::iterator category_it = thread_categories_.find(category);
  DCHECK(category_it != thread_categories_.end());
  category_it->second.erase(boost_id);
  if (metrics_enabled_) {
    current_num_threads_metric_--;
  }
}

void ThreadMgr::PrintThreadCategoryRows(const ThreadCategory& category,
    stringstream* output) {
  BOOST_FOREACH(const ThreadCategory::value_type& thread, category) {
    ThreadStats stats;
    Status status = GetThreadStats(thread.second.thread_id(), &stats);
    if (!status.ok()) {
      LOG_EVERY_N(INFO, 100) << "Could not get per-thread statistics: "
                             << status.ToString();
    }
    (*output) << "<tr><td>" << thread.second.name() << "</td><td>"
              << (static_cast<double>(stats.user_ns) / 1e9) << "</td><td>"
              << (static_cast<double>(stats.kernel_ns) / 1e9) << "</td><td>"
              << (static_cast<double>(stats.iowait_ns) / 1e9) << "</td></tr>";
  }
}

void ThreadMgr::ThreadPathHandler(const WebCallbackRegistry::ArgumentMap& args,
    stringstream* output) {
  lock_guard<mutex> l(lock_);
  vector<const ThreadCategory*> categories_to_print;
  WebCallbackRegistry::ArgumentMap::const_iterator category_name = args.find("group");
  if (category_name != args.end()) {
    string group = EscapeForHtmlToString(category_name->second);
    (*output) << "<h2>Thread Group: " << group << "</h2>" << endl;
    if (group != "all") {
      ThreadCategoryMap::const_iterator category = thread_categories_.find(group);
      if (category == thread_categories_.end()) {
        (*output) << "Thread group '" << group << "' not found" << endl;
        return;
      }
      categories_to_print.push_back(&category->second);
      (*output) << "<h3>" << category->first << " : " << category->second.size()
                << "</h3>";
    } else {
      BOOST_FOREACH(const ThreadCategoryMap::value_type& category, thread_categories_) {
        categories_to_print.push_back(&category.second);
      }
      (*output) << "<h3>All Threads : </h3>";
    }

    (*output) << "<table class='table table-hover table-border'>";
    (*output) << "<tr><th>Thread name</th><th>Cumulative User CPU(s)</th>"
              << "<th>Cumulative Kernel CPU(s)</th>"
              << "<th>Cumulative IO-wait(s)</th></tr>";

    BOOST_FOREACH(const ThreadCategory* category, categories_to_print) {
      PrintThreadCategoryRows(*category, output);
    }
    (*output) << "</table>";
  } else {
    (*output) << "<h2>Thread Groups</h2>";
    if (metrics_enabled_) {
      (*output) << "<h4>" << current_num_threads_metric_ << " thread(s) running";
    }
    (*output) << "<a href='/threadz?group=all'><h3>All Threads</h3>";

    BOOST_FOREACH(const ThreadCategoryMap::value_type& category, thread_categories_) {
      string category_arg;
      UrlEncode(category.first, &category_arg);
      (*output) << "<a href='/threadz?group=" << category_arg << "'><h3>"
                << category.first << " : " << category.second.size() << "</h3></a>";
    }
  }
}

static void InitThreading() {
  thread_manager.reset(new ThreadMgr());
}

Status StartThreadInstrumentation(MetricRegistry* metric, WebCallbackRegistry* web) {
  GoogleOnceInit(&once, &InitThreading);
  return thread_manager->StartInstrumentation(metric, web);
}

ThreadJoiner::ThreadJoiner(const Thread* thr)
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

  int waited_ms = 0;
  bool keep_trying = true;
  while (keep_trying) {
    if (waited_ms >= warn_after_ms_) {
      LOG(WARNING) << "Waited for " << waited_ms << "ms trying to join with "
                   << thread_->name_;
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

    int wait_for = min(remaining_before_giveup, remaining_before_next_warn);
    try {
      if (thread_->thread_->timed_join(boost::posix_time::milliseconds(wait_for))) {
        return Status::OK();
      }
    } catch(boost::system::system_error& e) {
      return Status::RuntimeError(strings::Substitute("Error occured joining on $0",
          thread_->name_), e.what());
    }
    waited_ms += wait_for;
  }
  return Status::Aborted(strings::Substitute("Timed out after $0ms joining on $1",
                                             waited_ms, thread_->name_));
}

static void SetThreadName(const string& name, int64 tid) {
  // On linux we can get the thread names to show up in the debugger by setting
  // the process name for the LWP.  We don't want to do this for the main
  // thread because that would rename the process, causing tools like killall
  // to stop working.
  if (tid == getpid()) {
    return;
  }

  // http://0pointer.de/blog/projects/name-your-threads.html
  // Set the name for the LWP (which gets truncated to 15 characters).
  // Note that glibc also has a 'pthread_setname_np' api, but it may not be
  // available everywhere and it's only benefit over using prctl directly is
  // that it can set the name of threads other than the current thread.
  int err = prctl(PR_SET_NAME, name.c_str());
  // We expect EPERM failures in sandboxed processes, just ignore those.
  if (err < 0 && errno != EPERM) {
    PLOG(ERROR) << "prctl(PR_SET_NAME)";
  }
}

Status Thread::StartThread(const std::string& category, const std::string& name,
                           const ThreadFunctor& functor, scoped_refptr<Thread> *holder) {
  Atomic64 c_p_tid = UNINITIALISED_THREAD_ID;

  // Temporary reference for the duration of this function.
  scoped_refptr<Thread> thr = new Thread(category, name);
  try {
    thr->thread_.reset(
        new thread(&Thread::SuperviseThread, thr.get(), functor, &c_p_tid));
  } catch(thread_resource_error &e) {
    return Status::RuntimeError(e.what());
  }

  // Optional, and only set if the thread was successfully created.
  if (holder) {
    *holder = thr;
  }

  // Wait for the child to discover its tid, then set it. The child will be
  // waiting on tid_; setting it is a signal that all child-visible state
  // has also been set.
  int loop_count = 0;
  while (Acquire_Load(&c_p_tid) == UNINITIALISED_THREAD_ID) {
    boost::detail::yield(loop_count++);
  }
  int64 system_tid = Acquire_Load(&c_p_tid);
  Release_Store(&thr->tid_, system_tid);

  VLOG(2) << "Started thread " << system_tid << " - " << category << ":" << name;
  return Status::OK();
}

void Thread::SuperviseThread(ThreadFunctor functor, Atomic64* c_p_tid) {
  int64_t system_tid = syscall(SYS_gettid);
  if (system_tid == -1) {
    string error_msg = ErrnoToString(errno);
    LOG_EVERY_N(INFO, 100) << "Could not determine thread ID: " << error_msg;
  }
  string name = strings::Substitute("$0-$1", name_, system_tid);

  // Take an additional reference to the thread manager, which we'll need below.
  GoogleOnceInit(&once, &InitThreading);
  shared_ptr<ThreadMgr> thread_mgr_ref = thread_manager;

  // Set up the TLS.
  //
  // We could store a scoped_refptr in the TLS itself, but as its lifecycle is
  // poorly defined, we'll use a bare pointer and take an additional reference on
  // _thread out of band, in thread_ref.
  scoped_refptr<Thread> thread_ref = this;
  tls_ = this;

  // Signal the parent with our tid.
  Release_Store(c_p_tid, system_tid);

  // Any reference to any parameter not copied in by value may no longer be valid after
  // this point, since the caller that is waiting on *c_p_tid != UNINITIALISED_THREAD_ID
  // may wake and return.

  // When tid_ has been set, the parent is done assigning everything and we can proceed to
  // the functor.
  int loop_count = 0;
  while (Acquire_Load(&tid_) == UNINITIALISED_THREAD_ID) {
    boost::detail::yield(loop_count++);
  }

  // Use boost's get_id rather than the system thread ID as the unique key for this thread
  // since the latter is more prone to being recycled.
  thread_mgr_ref->AddThread(boost::this_thread::get_id(), name, category_, system_tid);
  {
    ThreadMgr::ScopedRemoveThread remove_thread(thread_mgr_ref.get(),
                                                boost::this_thread::get_id(), category_);
    SetThreadName(name, system_tid);
    functor();
  }
}

} // namespace kudu
