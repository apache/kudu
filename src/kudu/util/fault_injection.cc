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

#include "kudu/util/fault_injection.h"

#include <stdlib.h>
#include <sys/time.h>

#include "kudu/gutil/once.h"
#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/monotime.h"
#include "kudu/util/os-util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"

namespace kudu {
namespace fault_injection {

namespace {
GoogleOnceType g_random_once;
Random* g_random;

void InitRandom() {
  LOG(WARNING) << "FAULT INJECTION ENABLED!";
  LOG(WARNING) << "THIS SERVER MAY CRASH!";

  debug::ScopedLeakCheckDisabler d;
  g_random = new Random(GetRandomSeed32());
  ANNOTATE_BENIGN_RACE_SIZED(g_random, sizeof(Random),
                             "Racy random numbers are OK");
}

} // anonymous namespace

void DoMaybeFault(const char* fault_str, double fraction) {
  GoogleOnceInit(&g_random_once, InitRandom);
  if (PREDICT_TRUE(g_random->NextDoubleFraction() >= fraction)) {
    return;
  }
  LOG(ERROR) << "Injecting fault: " << fault_str << " (process will exit)";
  // _exit will exit the program without running atexit handlers. This more
  // accurately simiulates a crash.
  _exit(kExitStatus);
}

void DoInjectRandomLatency(double max_latency_ms) {
  GoogleOnceInit(&g_random_once, InitRandom);
  SleepFor(MonoDelta::FromMilliseconds(g_random->NextDoubleFraction() * max_latency_ms));
}

bool DoMaybeTrue(double fraction) {
  GoogleOnceInit(&g_random_once, InitRandom);
  return PREDICT_FALSE(g_random->NextDoubleFraction() <= fraction);
}

} // namespace fault_injection
} // namespace kudu
