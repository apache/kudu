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

#include <cstdint>

namespace kudu {
namespace process_memory {

// Probabilistically returns true if the process-wide soft memory limit is exceeded.
// The greater the excess, the higher the chance that it returns true.
//
// If the soft limit is exceeded and 'current_capacity_pct' is not NULL, the percentage
// of the hard limit consumed is written to it.
bool SoftLimitExceeded(double* current_capacity_pct);

// Potentially trigger a call to release tcmalloc memory back to the
// OS, after the given amount of memory was released.
void MaybeGCAfterRelease(int64_t released_bytes);

// Return the total current memory consumption of the process.
int64_t CurrentConsumption();

// Return the configured hard limit for the process.
int64_t HardLimit();

#ifdef TCMALLOC_ENABLED
// Get the current amount of allocated memory, according to tcmalloc.
//
// This should be equal to CurrentConsumption(), but is made available so that tests
// can verify the correctness of CurrentConsumption().
int64_t GetTCMallocCurrentAllocatedBytes();
#endif

} // namespace process_memory
} // namespace kudu
