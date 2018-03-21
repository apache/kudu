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

#include "kudu/util/striped64.h"

#include <mm_malloc.h>
#include <unistd.h>

#include <cstdlib>
#include <new>
#include <ostream>
#include <glog/logging.h>

#include "kudu/util/monotime.h"
#include "kudu/util/random.h"

using kudu::striped64::internal::Cell;

namespace kudu {

namespace striped64 {
namespace internal {

//
// Cell
//

Cell::Cell()
    : value_(0) {
}
} // namespace internal
} // namespace striped64

//
// Striped64
//
__thread uint64_t Striped64::tls_hashcode_ = 0;

namespace {
const uint32_t kNumCpus = sysconf(_SC_NPROCESSORS_ONLN);
uint32_t ComputeNumCells() {
  uint32_t n = 1;
  // Calculate the size. Nearest power of two >= NCPU.
  // Also handle a negative NCPU, can happen if sysconf name is unknown
  while (kNumCpus > n) {
    n <<= 1;
  }
  return n;
}
const uint32_t kNumCells = ComputeNumCells();
const uint32_t kCellMask = kNumCells - 1;

striped64::internal::Cell* const kCellsLocked =
      reinterpret_cast<striped64::internal::Cell*>(-1L);

} // anonymous namespace

uint64_t Striped64::get_tls_hashcode() {
  if (PREDICT_FALSE(tls_hashcode_ == 0)) {
    Random r((MonoTime::Now() - MonoTime::Min()).ToNanoseconds());
    const uint64_t hash = r.Next64();
    // Avoid zero to allow xorShift rehash, and because 0 indicates an unset
    // hashcode above.
    tls_hashcode_ = (hash == 0) ? 1 : hash;
  }
  return tls_hashcode_;
}


Striped64::~Striped64() {
  // Cell is a POD, so no need to destruct each one.
  free(cells_);
}

template<class Updater>
void Striped64::RetryUpdate(Rehash to_rehash, Updater updater) {
  uint64_t h = get_tls_hashcode();
  // There are three operations in this loop.
  //
  // 1. Try to add to the Cell hash table entry for the thread if the table exists.
  //    When there's contention, rehash to try a different Cell.
  // 2. Try to initialize the hash table.
  // 3. Try to update the base counter.
  //
  // These are predicated on successful CAS operations, which is why it's all wrapped in an
  // infinite retry loop.
  while (true) {
    Cell* cells = cells_.load(std::memory_order_acquire);
    if (cells && cells != kCellsLocked) {
      if (to_rehash == kRehash) {
        // CAS failed already, rehash before trying to increment.
        to_rehash = kNoRehash;
      } else {
        Cell *cell = &(cells_[h & kCellMask]);
        int64_t v = cell->value_.load(std::memory_order_relaxed);
        if (cell->CompareAndSet(v, updater(v))) {
          // Successfully CAS'd the corresponding cell, done.
          break;
        }
      }
      // Rehash since we failed to CAS, either previously or just now.
      h ^= h << 13;
      h ^= h >> 17;
      h ^= h << 5;
    } else if (cells == nullptr &&
               cells_.compare_exchange_weak(cells, kCellsLocked)) {
      // Allocate cache-aligned memory for use by the cells_ table.
      void* cell_buffer = nullptr;
      int err = posix_memalign(&cell_buffer, CACHELINE_SIZE, sizeof(Cell) * kNumCells);
      CHECK_EQ(0, err) << "error calling posix_memalign" << std::endl;
      // Initialize the table
      cells = new (cell_buffer) Cell[kNumCells];
      cells_.store(cells, std::memory_order_release);
    } else {
      // Fallback to adding to the base value.
      // Means the table wasn't initialized or we failed to init it.
      int64_t v = base_.load(std::memory_order_relaxed);
      if (CasBase(v, updater(v))) {
        break;
      }
    }
  }
  // Record index for next time
  tls_hashcode_ = h;
}

void Striped64::InternalReset(int64_t initial_value) {
  base_.store(initial_value);
  Cell* c;
  do {
    c = cells_.load(std::memory_order_acquire);
  } while (c == kCellsLocked);
  if (c) {
    for (int i = 0; i < kNumCells; i++) {
      c[i].value_.store(initial_value);
    }
  }
}
void LongAdder::IncrementBy(int64_t x) {
  // Use hash table if present. If that fails, call RetryUpdate to rehash and retry.
  // If no hash table, try to CAS the base counter. If that fails, RetryUpdate to init the table.
  Cell* cells = cells_.load(std::memory_order_acquire);
  if (cells && cells != kCellsLocked) {
    Cell *cell = &(cells[get_tls_hashcode() & kCellMask]);
    DCHECK_EQ(0, reinterpret_cast<const uintptr_t>(cell) & (sizeof(Cell) - 1))
        << " unaligned Cell not allowed for Striped64" << std::endl;
    const int64_t old = cell->value_.load(std::memory_order_relaxed);
    if (!cell->CompareAndSet(old, old + x)) {
      // When we hit a hash table contention, signal RetryUpdate to rehash.
      RetryUpdate(kRehash, [x](int64_t old) { return old + x; });
    }
  } else {
    int64_t b = base_.load(std::memory_order_relaxed);
    if (!CasBase(b, b + x)) {
      // Attempt to initialize the table. No need to rehash since the contention was for the
      // base counter, not the hash table.
      RetryUpdate(kNoRehash, [x](int64_t old) { return old + x; });
    }
  }
}

//
// LongAdder
//

int64_t LongAdder::Value() const {
  int64_t sum = base_.load(std::memory_order_relaxed);
  Cell* c = cells_.load(std::memory_order_acquire);
  if (c && c != kCellsLocked) {
    for (int i = 0; i < kNumCells; i++) {
      sum += c[i].value_.load(std::memory_order_relaxed);
    }
  }
  return sum;
}

} // namespace kudu
