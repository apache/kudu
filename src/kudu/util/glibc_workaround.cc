// Copyright 2015 Cloudera, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <dlfcn.h>
#include <limits.h>
#include <pthread.h>

#include <glog/logging.h>

#include "kudu/gutil/basictypes.h"

// Double the minimum stack size that pthreads thinks we should have.
static const int kMinStackSize = PTHREAD_STACK_MIN * 2;

namespace kudu {
// Dummy function so that we are forced to link this object file
// into binaries.
void ForceLinkingGlibcWorkaround() {}
} // namespace kudu


extern "C" {

// This is a workaround for glibc bug https://sourceware.org/bugzilla/show_bug.cgi?id=13088
// in which, if the total amount of static thread-local-storage in use is relatively
// large, then glibc will fail to create its own internal threads. This can, for example,
// cause timer_create() to fail because it's unable to make its own background thread.
//
// This has been fixed in upstream glibc as of 2.14, but does not appear to be backported
// to el6.
//
// As a workaround, we define our own override of pthread_attr_setstacksize which
// prevents the stack size from being set too low. This overrides the call within
// timer_create() which tries to set a hard-coded 16KB stack, instead passing a
// larger value.
int pthread_attr_setstacksize(pthread_attr_t* attr, size_t size) {
  typedef int (*func_t)(pthread_attr_t*, size_t);
  static func_t next = reinterpret_cast<func_t>(dlsym(RTLD_NEXT, "pthread_attr_setstacksize"));
  CHECK(next) << "unable to find pthread_attr_setstacksize() symbol";

  if (size < kMinStackSize) {
    VLOG(1) << "Detected attempt to set a small thread stack size " << size
            << ": forcing stack size to " << kMinStackSize;
    size = kMinStackSize;
  }
  return next(attr, size);
}
}
