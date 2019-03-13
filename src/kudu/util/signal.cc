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

#include "kudu/util/signal.h"

#include <cerrno>

#include <glog/logging.h> // IWYU pragma: keep
#include <glog/raw_logging.h>

namespace kudu {

// Using RAW_LOG() and logging directly into stderr instead of using more
// advanced LOG() to keep all these function as async-signal-safe as possible.
//
// NOTE: RAW_LOG() uses vsnprintf(), and it's not async-signal-safe
//       strictly speaking (in some cases may call malloc(), etc.)

void SetSignalHandler(int signal, SignalHandlerCallback handler) {
  struct sigaction act;
  act.sa_handler = handler;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  if (sigaction(signal, &act, nullptr) != 0) {
    int err = errno;
    RAW_LOG(FATAL, "sigaction() failed: [%d]", err);
  }
}

void IgnoreSigPipe() {
  SetSignalHandler(SIGPIPE, SIG_IGN);
}

void ResetSigPipeHandlerToDefault() {
  SetSignalHandler(SIGPIPE, SIG_DFL);
}

// We unblock all signal masks since they are inherited.
void ResetAllSignalMasksToUnblocked() {
  sigset_t sigset;
  if (sigfillset(&sigset) != 0) {
    int err = errno;
    RAW_LOG(FATAL, "sigfillset() failed: [%d]", err);
  }
  if (sigprocmask(SIG_UNBLOCK, &sigset, nullptr) != 0) {
    int err = errno;
    RAW_LOG(FATAL, "sigprocmask() failed: [%d]", err);
  }
}

} // namespace kudu
