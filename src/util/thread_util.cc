// Copyright (c) 2013, Cloudera, inc.

#include "util/thread_util.h"

#include <algorithm>
#include <boost/system/system_error.hpp>
#include <glog/logging.h>
#include <string>
#include <sys/prctl.h>

#include "gutil/mathlimits.h"
#include "gutil/linux_syscall_support.h"
#include "gutil/strings/substitute.h"
#include "util/status.h"

using std::string;

namespace kudu {

ThreadJoiner::ThreadJoiner(boost::thread* thr, const string& name)
  : thread_(CHECK_NOTNULL(thr)),
    thread_name_(name),
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
  if (boost::this_thread::get_id() == thread_->get_id()) {
    return Status::InvalidArgument("Can't join on own thread", thread_name_);
  }

  int waited_ms = 0;
  bool keep_trying = true;
  while (keep_trying) {
    if (waited_ms >= warn_after_ms_) {
      LOG(WARNING) << "Waited for " << waited_ms << "ms trying to join with "
                   << thread_name_;
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
    try {
      if (thread_->timed_join(boost::posix_time::milliseconds(wait_for))) {
        return Status::OK();
      }
    } catch(boost::system::system_error& e) {
      return Status::RuntimeError(strings::Substitute("Error occured joining on $0", thread_name_),
                                  e.what());
    }
    waited_ms += wait_for;
  }
  return Status::Aborted(strings::Substitute("Timed out after $0ms joining on $1",
                                             waited_ms, thread_name_));
}

void SetThreadName(const string& name) {
  // On linux we can get the thread names to show up in the debugger by setting
  // the process name for the LWP.  We don't want to do this for the main
  // thread because that would rename the process, causing tools like killall
  // to stop working.
  if (sys_gettid() == getpid()) {
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

} // namespace kudu
