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

#include "kudu/util/init.h"

#include <fcntl.h>
#include <unistd.h>

#include <string>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/cpu.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

using std::string;

namespace kudu {

Status BadCPUStatus(const base::CPU& cpu, const char* instruction_set) {
  return Status::NotSupported(strings::Substitute(
      "The CPU on this system ($0) does not support the $1 instruction "
      "set which is required for running Kudu. If you are running inside a VM, "
      "you may need to enable SSE4.2 pass-through.",
      cpu.cpu_brand(), instruction_set));
}

bool IsFdOpen(int fd) {
  return fcntl(fd, F_GETFL) != -1;
}

// Checks that the standard file descriptors are open when the process
// starts.
//
// If these descriptors aren't open, we can run into serious issues:
// we later might open some other files which end up reusing the same
// file descriptor numbers as stderr, and then some library like glog
// may decide to write a log message to what it thinks is stderr. That
// would then overwrite one of our important data files and cause
// corruption!
void CheckStandardFds() {
  if (!IsFdOpen(STDIN_FILENO) ||
      !IsFdOpen(STDOUT_FILENO) ||
      !IsFdOpen(STDERR_FILENO)) {
    // We can't use LOG(FATAL) here because glog isn't initialized yet, and even if it
    // were, it would try to write to stderr, which might end up writing the log message
    // into some unexpected place. This is a rare enough issue that people can deal with
    // the core dump.
    abort();
  }
}

Status CheckCPUFlags() {
  base::CPU cpu;
  if (!cpu.has_sse42()) {
    return BadCPUStatus(cpu, "SSE4.2");
  }

  if (!cpu.has_ssse3()) {
    return BadCPUStatus(cpu, "SSSE3");
  }

  return Status::OK();
}

void InitKuduOrDie() {
  CheckStandardFds();
  CHECK_OK(CheckCPUFlags());
#ifndef THREAD_SANITIZER  // Thread Sanitizer uses its own special atomicops implementation.
  AtomicOps_x86CPUFeaturesInit();
#endif
  // NOTE: this function is called before flags are parsed.
  // Do not add anything in here which is flag-dependent.
}

} // namespace kudu
