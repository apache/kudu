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

// Override various libdl functions which can race with libunwind.
// The overridden versions set a threadlocal variable and our
// stack-tracing code checks the threadlocal before calling into
// libunwind.
//
// Based on public domain code by Aliaksey Kandratsenka at
// https://github.com/alk/unwind_safeness_helper

#include "kudu/util/debug/unwind_safeness.h"

#include <dlfcn.h>
#include <stddef.h>

#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/port.h"

#define CALL_ORIG(func_name, ...) \
  ((decltype(&func_name))g_orig_ ## func_name)(__VA_ARGS__)

typedef int (*dl_iterate_phdr_cbtype)(struct dl_phdr_info *, size_t, void *);

namespace {

void* g_orig_dlopen;
void* g_orig_dlclose;
#ifndef __APPLE__
void* g_orig_dl_iterate_phdr;
#endif

// The depth of calls into libdl.
__thread int g_unsafeness_depth;

// Scoped helper to track the recursion depth of calls into libdl
struct ScopedBumpDepth {
  ScopedBumpDepth() {
    g_unsafeness_depth++;
  }
  ~ScopedBumpDepth() {
    g_unsafeness_depth--;
  }
};

void *dlsym_or_die(const char *sym) {
  dlerror();
  void* ret = dlsym(RTLD_NEXT, sym);
  char* error = dlerror();
  CHECK(!error) << "failed to find symbol " << sym << ": " << error;
  return ret;
}

__attribute__((constructor))
void Init(void) {
  g_orig_dlopen = dlsym_or_die("dlopen");
  g_orig_dlclose = dlsym_or_die("dlclose");
#ifndef __APPLE__ // This function doesn't exist on macOS.
  g_orig_dl_iterate_phdr = dlsym_or_die("dl_iterate_phdr");
#endif
}

} // anonymous namespace

namespace kudu {
namespace debug {

bool SafeToUnwindStack() {
  return g_unsafeness_depth == 0;
}

} // namespace debug
} // namespace kudu

extern "C" {

void *dlopen(const char *filename, int flag) { // NOLINT
  ScopedBumpDepth d;
  return CALL_ORIG(dlopen, filename, flag);
}

int dlclose(void *handle) { // NOLINT
  ScopedBumpDepth d;
  return CALL_ORIG(dlclose, handle);
}

// Don't hook dl_iterate_phdr in TSAN builds since TSAN already instruments this
// function and blocks signals while calling it.
#if !defined(THREAD_SANITIZER) && !defined(__APPLE__)
int dl_iterate_phdr(dl_iterate_phdr_cbtype callback, void *data) { // NOLINT
  // In ASAN builds, the sanitizer runtime ends up calling dl_iterate_phdr from its
  // initialization, which runs before our own constructor functions. In that case, we
  // need to call Init() from here rather than via the normal path. There's no need to
  // worry about thread-safety since this all happens single-threaded during startup.
  if (PREDICT_FALSE(!g_orig_dl_iterate_phdr)) {
    Init();
  }

  ScopedBumpDepth d;
  return CALL_ORIG(dl_iterate_phdr, callback, data);
}
#endif

}
