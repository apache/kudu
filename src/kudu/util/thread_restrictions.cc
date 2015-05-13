// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <glog/logging.h>
#include <gperftools/heap-checker.h>

#include "kudu/util/thread.h"
#include "kudu/util/threadlocal.h"
#include "kudu/util/thread_restrictions.h"

#ifdef ENABLE_THREAD_RESTRICTIONS

namespace kudu {

namespace {

struct LocalThreadRestrictions {
  LocalThreadRestrictions()
    : io_allowed(true),
      wait_allowed(true),
      singleton_allowed(true) {
  }

  bool io_allowed;
  bool wait_allowed;
  bool singleton_allowed;
};

LocalThreadRestrictions* LoadTLS() {
  BLOCK_STATIC_THREAD_LOCAL(LocalThreadRestrictions, local_thread_restrictions);
  return local_thread_restrictions;
}

} // anonymous namespace

bool ThreadRestrictions::SetIOAllowed(bool allowed) {
  bool previous_allowed = LoadTLS()->io_allowed;
  LoadTLS()->io_allowed = allowed;
  return previous_allowed;
}

void ThreadRestrictions::AssertIOAllowed() {
  CHECK(LoadTLS()->io_allowed)
    << "Function marked as IO-only was called from a thread that "
    << "disallows IO!  If this thread really should be allowed to "
    << "make IO calls, adjust the call to "
    << "kudu::ThreadRestrictions::SetIOAllowed() in this thread's "
    << "startup. "
    << (Thread::current_thread() ? Thread::current_thread()->ToString() : "(not a kudu::Thread)");
}

bool ThreadRestrictions::SetWaitAllowed(bool allowed) {
  bool previous_allowed = LoadTLS()->wait_allowed;
  LoadTLS()->wait_allowed = allowed;
  return previous_allowed;
}

void ThreadRestrictions::AssertWaitAllowed() {
  CHECK(LoadTLS()->wait_allowed)
    << "Waiting is not allowed to be used on this thread to prevent "
    << "server-wide latency aberrations and deadlocks. "
    << (Thread::current_thread() ? Thread::current_thread()->ToString() : "(not a kudu::Thread)");
}

} // namespace kudu

#endif
