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

#include <sys/resource.h>

#include <gflags/gflags.h>
#include <gperftools/malloc_extension.h>

#include "kudu/gutil/once.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/random.h"
#include "kudu/util/striped64.h"

DEFINE_int64(memory_limit_hard_bytes, 0,
             "Maximum amount of memory this daemon should use, in bytes. "
             "A value of 0 autosizes based on the total system memory. "
             "A value of -1 disables all memory limiting.");
TAG_FLAG(memory_limit_hard_bytes, stable);

DEFINE_int32(memory_pressure_percentage, 60,
             "Percentage of the hard memory limit that this daemon may "
             "consume before flushing of in-memory data becomes prioritized.");
TAG_FLAG(memory_pressure_percentage, advanced);

DEFINE_int32(memory_limit_soft_percentage, 80,
             "Percentage of the hard memory limit that this daemon may "
             "consume before memory throttling of writes begins. The greater "
             "the excess, the higher the chance of throttling. In general, a "
             "lower soft limit leads to smoother write latencies but "
             "decreased throughput, and vice versa for a higher soft limit.");
TAG_FLAG(memory_limit_soft_percentage, advanced);

DEFINE_int32(memory_limit_warn_threshold_percentage, 98,
             "Percentage of the hard memory limit that this daemon may "
             "consume before WARNING level messages are periodically logged.");
TAG_FLAG(memory_limit_warn_threshold_percentage, advanced);

#ifdef TCMALLOC_ENABLED
DEFINE_int32(tcmalloc_max_free_bytes_percentage, 10,
             "Maximum percentage of the RSS that tcmalloc is allowed to use for "
             "reserved but unallocated memory.");
TAG_FLAG(tcmalloc_max_free_bytes_percentage, advanced);
#endif

using strings::Substitute;

namespace kudu {
namespace process_memory {

namespace {
int64_t g_hard_limit;
int64_t g_soft_limit;
int64_t g_pressure_threshold;

ThreadSafeRandom* g_rand = nullptr;

#ifdef TCMALLOC_ENABLED
// Total amount of memory released since the last GC. If this
// is greater than GC_RELEASE_SIZE, this will trigger a tcmalloc gc.
Atomic64 g_released_memory_since_gc;

// Size, in bytes, that is considered a large value for Release() (or Consume() with
// a negative value). If tcmalloc is used, this can trigger it to GC.
// A higher value will make us call into tcmalloc less often (and therefore more
// efficient). A lower value will mean our memory overhead is lower.
// TODO(todd): this is a stopgap.
const int64_t kGcReleaseSize = 128 * 1024L * 1024L;

#endif // TCMALLOC_ENABLED

} // anonymous namespace


// Flag validation
// ------------------------------------------------------------
// Validate that various flags are percentages.
static bool ValidatePercentage(const char* flagname, int value) {
  if (value >= 0 && value <= 100) {
    return true;
  }
  LOG(ERROR) << Substitute("$0 must be a percentage, value $1 is invalid",
                           flagname, value);
  return false;
}

static bool dummy[] = {
  google::RegisterFlagValidator(&FLAGS_memory_limit_soft_percentage, &ValidatePercentage),
  google::RegisterFlagValidator(&FLAGS_memory_limit_warn_threshold_percentage, &ValidatePercentage)
#ifdef TCMALLOC_ENABLED
  ,google::RegisterFlagValidator(&FLAGS_tcmalloc_max_free_bytes_percentage, &ValidatePercentage)
#endif
};


// Wrappers around tcmalloc functionality
// ------------------------------------------------------------
#ifdef TCMALLOC_ENABLED
static int64_t GetTCMallocProperty(const char* prop) {
  size_t value;
  if (!MallocExtension::instance()->GetNumericProperty(prop, &value)) {
    LOG(DFATAL) << "Failed to get tcmalloc property " << prop;
  }
  return value;
}

int64_t GetTCMallocCurrentAllocatedBytes() {
  return GetTCMallocProperty("generic.current_allocated_bytes");
}

void GcTcmalloc() {
  TRACE_EVENT0("process", "GcTcmalloc");

  // Number of bytes in the 'NORMAL' free list (i.e reserved by tcmalloc but
  // not in use).
  int64_t bytes_overhead = GetTCMallocProperty("tcmalloc.pageheap_free_bytes");
  // Bytes allocated by the application.
  int64_t bytes_used = GetTCMallocCurrentAllocatedBytes();

  int64_t max_overhead = bytes_used * FLAGS_tcmalloc_max_free_bytes_percentage / 100.0;
  if (bytes_overhead > max_overhead) {
    int64_t extra = bytes_overhead - max_overhead;
    while (extra > 0) {
      // Release 1MB at a time, so that tcmalloc releases its page heap lock
      // allowing other threads to make progress. This still disrupts the current
      // thread, but is better than disrupting all.
      MallocExtension::instance()->ReleaseToSystem(1024 * 1024);
      extra -= 1024 * 1024;
    }
  }
}
#endif // TCMALLOC_ENABLED


// Consumption and soft memory limit behavior
// ------------------------------------------------------------
namespace {
void DoInitLimits() {
  int64_t limit = FLAGS_memory_limit_hard_bytes;
  if (limit == 0) {
    // If no limit is provided, we'll use 80% of system RAM.
    int64_t total_ram;
    CHECK_OK(Env::Default()->GetTotalRAMBytes(&total_ram));
    limit = total_ram * 4;
    limit /= 5;
  }
  g_hard_limit = limit;
  g_soft_limit = FLAGS_memory_limit_soft_percentage * g_hard_limit / 100;
  g_pressure_threshold = FLAGS_memory_pressure_percentage * g_hard_limit / 100;

  g_rand = new ThreadSafeRandom(1);

  LOG(INFO) << StringPrintf("Process hard memory limit is %.6f GB",
                            (static_cast<float>(g_hard_limit) / (1024.0 * 1024.0 * 1024.0)));
  LOG(INFO) << StringPrintf("Process soft memory limit is %.6f GB",
                            (static_cast<float>(g_soft_limit) /
                             (1024.0 * 1024.0 * 1024.0)));
  LOG(INFO) << StringPrintf("Process memory pressure threshold is %.6f GB",
                            (static_cast<float>(g_pressure_threshold) /
                             (1024.0 * 1024.0 * 1024.0)));
}

void InitLimits() {
  static GoogleOnceType once;
  GoogleOnceInit(&once, &DoInitLimits);
}

} // anonymous namespace

int64_t CurrentConsumption() {
#ifdef TCMALLOC_ENABLED
  const int64_t kReadIntervalMicros = 50000;
  static Atomic64 last_read_time = 0;
  static simple_spinlock read_lock;
  static Atomic64 consumption = 0;
  uint64_t time = GetMonoTimeMicros();
  if (time > last_read_time + kReadIntervalMicros && read_lock.try_lock()) {
    base::subtle::NoBarrier_Store(&consumption, GetTCMallocCurrentAllocatedBytes());
    // Re-fetch the time after getting the consumption. This way, in case fetching
    // consumption is extremely slow for some reason (eg due to lots of contention
    // in tcmalloc) we at least ensure that we wait at least another full interval
    // before fetching the information again.
    time = GetMonoTimeMicros();
    base::subtle::NoBarrier_Store(&last_read_time, time);
    read_lock.unlock();
  }

  return base::subtle::NoBarrier_Load(&consumption);
#else
  // Without tcmalloc, we have no reliable way of determining our own heap
  // size (e.g. mallinfo doesn't work in ASAN builds). So, we'll fall back
  // to just looking at the sum of our tracked memory.
  return MemTracker::GetRootTracker()->consumption();
#endif
}

int64_t HardLimit() {
  InitLimits();
  return g_hard_limit;
}

bool UnderMemoryPressure(double* current_capacity_pct) {
  InitLimits();
  int64_t consumption = CurrentConsumption();
  if (consumption < g_pressure_threshold) {
    return false;
  }
  if (current_capacity_pct) {
    *current_capacity_pct = static_cast<double>(consumption) / g_hard_limit * 100;
  }
  return true;
}

bool SoftLimitExceeded(double* current_capacity_pct) {
  InitLimits();
  int64_t consumption = CurrentConsumption();
  // Did we exceed the actual limit?
  if (consumption > g_hard_limit) {
    if (current_capacity_pct) {
      *current_capacity_pct = static_cast<double>(consumption) / g_hard_limit * 100;
    }
    return true;
  }

  // No soft limit defined.
  if (g_hard_limit == g_soft_limit) {
    return false;
  }

  // Are we under the soft limit threshold?
  if (consumption < g_soft_limit) {
    return false;
  }

  // We're over the threshold; were we randomly chosen to be over the soft limit?
  if (consumption + g_rand->Uniform64(g_hard_limit - g_soft_limit) > g_hard_limit) {
    if (current_capacity_pct) {
      *current_capacity_pct = static_cast<double>(consumption) / g_hard_limit * 100;
    }
    return true;
  }
  return false;
}

void MaybeGCAfterRelease(int64_t released_bytes) {
#ifdef TCMALLOC_ENABLED
  int64_t now_released = base::subtle::NoBarrier_AtomicIncrement(
      &g_released_memory_since_gc, -released_bytes);
  if (PREDICT_FALSE(now_released > kGcReleaseSize)) {
    base::subtle::NoBarrier_Store(&g_released_memory_since_gc, 0);
    GcTcmalloc();
  }
#endif
}

} // namespace process_memory
} // namespace kudu
