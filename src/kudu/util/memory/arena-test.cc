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

#include "kudu/util/memory/arena.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/stringprintf.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/memory/memory.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_util.h"

DEFINE_int32(num_threads, 16, "Number of threads to test");
DEFINE_int32(allocs_per_thread, 10000, "Number of allocations each thread should do");
DEFINE_int32(alloc_size, 4, "number of bytes in each allocation");

namespace kudu {

using std::shared_ptr;
using std::string;
using std::thread;
using std::vector;

// From the "arena" allocate number of bytes required to copy the "to_write" buffer
// and add the allocated buffer to the output "ptrs" vector.
template<class ArenaType>
static void AllocateBytesAndWrite(ArenaType* arena, const Slice& to_write, vector<void*>* ptrs) {
  void *allocated_bytes = arena->AllocateBytes(to_write.size());
  ASSERT_NE(nullptr, allocated_bytes);
  memcpy(allocated_bytes, to_write.data(), to_write.size());
  ptrs->push_back(allocated_bytes);
}

// From the "arena" allocate aligned bytes as specified by "alignment". Number of bytes
// must be at least the size required to copy the "to_write" buffer.
// Add the allocated aligned buffer to the output "ptrs" vector.
template<class ArenaType, class RNG>
static void AllocateAlignedBytesAndWrite(ArenaType* arena, const size_t alignment,
    const Slice& to_write, RNG* r, vector<void*>* ptrs) {
  // To test alignment we allocate random number of bytes within bounds
  // but write to fixed number of bytes "to_write".
  size_t num_bytes = FLAGS_alloc_size + r->Uniform32(128 - FLAGS_alloc_size + 1);
  ASSERT_LE(to_write.size(), num_bytes);
  void* allocated_bytes = arena->AllocateBytesAligned(num_bytes, alignment);
  ASSERT_NE(nullptr, allocated_bytes);
  ASSERT_EQ(0, reinterpret_cast<uintptr_t>(allocated_bytes) % alignment) <<
    "failed to align on " << alignment << "b boundary: " << allocated_bytes;
  memcpy(allocated_bytes, to_write.data(), to_write.size());
  ptrs->push_back(allocated_bytes);
}

// Thread callback function used by bunch of test cases below.
template<class ArenaType, class RNG, bool InvokeAligned = false>
static void AllocateAndTestThreadFunc(ArenaType *arena, uint8_t thread_index, RNG* r) {
  vector<void *> ptrs;
  ptrs.reserve(FLAGS_allocs_per_thread);

  uint8_t buf[FLAGS_alloc_size];
  memset(buf, thread_index, FLAGS_alloc_size);
  Slice data(buf, FLAGS_alloc_size);

  for (int i = 0; i < FLAGS_allocs_per_thread; i++) {
    if (InvokeAligned) {
      // Test alignment up to 64 bytes.
      const size_t alignment = 1 << (i % 7);
      AllocateAlignedBytesAndWrite(arena, alignment, data, r, &ptrs);
    } else {
      AllocateBytesAndWrite(arena, data, &ptrs);
    }
  }

  for (void *p : ptrs) {
    if (memcmp(buf, p, FLAGS_alloc_size) != 0) {
      FAIL() << StringPrintf("overwritten pointer at %p", p);
    }
  }
}

// Non-templated function to forward to above -- simplifies thread creation
static void AllocateAndTestTSArenaFunc(ThreadSafeArena *arena, uint8_t thread_index,
                                       ThreadSafeRandom* r) {
  AllocateAndTestThreadFunc(arena, thread_index, r);
}

template<typename ArenaType, typename RNG>
static void TestArenaAlignmentHelper() {
  RNG r(SeedRandom());

  for (size_t initial_size = 16; initial_size <= 2048; initial_size <<= 1) {
    ArenaType arena(initial_size);
    static constexpr bool kIsMultiThreaded = std::is_same<ThreadSafeArena, ArenaType>::value;
    if (kIsMultiThreaded) {
      vector<thread> threads;
      threads.reserve(FLAGS_num_threads);
      for (auto i = 0; i < FLAGS_num_threads; i++) {
        threads.emplace_back(
            AllocateAndTestThreadFunc<ArenaType, RNG, true /* InvokeAligned */>, &arena, i, &r);
      }
      for (thread& thr : threads) {
        thr.join();
      }
    } else {
      // Invoke the helper method on the same thread avoiding separate single
      // thread creation/join.
      AllocateAndTestThreadFunc<ArenaType, RNG, true /* InvokedAligned */>(&arena, 0, &r);
    }
  }
}

TEST(TestArena, TestSingleThreaded) {
  Arena arena(128);
  Random r(SeedRandom());
  AllocateAndTestThreadFunc(&arena, 0, &r);
}

TEST(TestArena, TestMultiThreaded) {
  CHECK(FLAGS_num_threads < 256);
  ThreadSafeRandom r(SeedRandom());
  ThreadSafeArena arena(1024);

  vector<thread> threads;
  threads.reserve(FLAGS_num_threads);
  for (auto i = 0; i < FLAGS_num_threads; i++) {
    threads.emplace_back(AllocateAndTestTSArenaFunc, &arena, i, &r);
  }

  for (thread& thr : threads) {
    thr.join();
  }
}

TEST(TestArena, TestAlignmentThreadSafe) {
  TestArenaAlignmentHelper<ThreadSafeArena, ThreadSafeRandom>();
}

TEST(TestArena, TestAlignmentNotThreadSafe) {
  TestArenaAlignmentHelper<Arena, Random>();
}

TEST(TestArena, TestAlignmentSmallArena) {
  // Start with small initial size and allocate bytes more than the size of the current
  // component to trigger fallback code path in Arena. Moreover allocate number of bytes
  // with alignment such that "aligned_size" exceeds "next_component_size".
  Arena arena(16);
  constexpr size_t alignment = 32;
  void *ret = arena.AllocateBytesAligned(33, alignment);
  ASSERT_NE(nullptr, ret);
  ASSERT_EQ(0, reinterpret_cast<uintptr_t>(ret) % alignment) <<
    "failed to align on " << alignment << "b boundary: " << ret;
}

TEST(TestArena, TestObjectAlignment) {
  struct MyStruct {
    int64_t v;
  };
  Arena a(256);
  // Allocate a junk byte to ensure that the next allocation isn't "accidentally" aligned.
  a.AllocateBytes(1);
  void* v = a.NewObject<MyStruct>();
  ASSERT_EQ(reinterpret_cast<uintptr_t>(v) % alignof(MyStruct), 0);
}


// MemTrackers update their ancestors when consuming and releasing memory to compute
// usage totals. However, the lifetimes of parent and child trackers can be different.
// Validate that child trackers can still correctly update their parent stats even when
// the parents go out of scope.
TEST(TestArena, TestMemoryTrackerParentReferences) {
  // Set up a parent and child MemTracker.
  const string parent_id = "parent-id";
  const string child_id = "child-id";
  shared_ptr<MemTracker> child_tracker;
  {
    shared_ptr<MemTracker> parent_tracker = MemTracker::CreateTracker(1024, parent_id);
    child_tracker = MemTracker::CreateTracker(-1, child_id, parent_tracker);
    // Parent falls out of scope here. Should still be owned by the child.
  }
  shared_ptr<MemoryTrackingBufferAllocator> allocator(
      new MemoryTrackingBufferAllocator(HeapBufferAllocator::Get(), child_tracker));
  MemoryTrackingArena arena(256, allocator);

  // Try some child operations.
  ASSERT_EQ(256, child_tracker->consumption());
  void *allocated = arena.AllocateBytes(256);
  ASSERT_TRUE(allocated);
  ASSERT_EQ(256, child_tracker->consumption());
  allocated = arena.AllocateBytes(256);
  ASSERT_TRUE(allocated);
  ASSERT_EQ(768, child_tracker->consumption());
}

TEST(TestArena, TestMemoryTrackingDontEnforce) {
  shared_ptr<MemTracker> mem_tracker = MemTracker::CreateTracker(1024, "arena-test-tracker");
  shared_ptr<MemoryTrackingBufferAllocator> allocator(
      new MemoryTrackingBufferAllocator(HeapBufferAllocator::Get(), mem_tracker));
  MemoryTrackingArena arena(256, allocator);
  ASSERT_EQ(256, mem_tracker->consumption());
  void *allocated = arena.AllocateBytes(256);
  ASSERT_TRUE(allocated);
  ASSERT_EQ(256, mem_tracker->consumption());
  allocated = arena.AllocateBytes(256);
  ASSERT_TRUE(allocated);
  ASSERT_EQ(768, mem_tracker->consumption());

  // In DEBUG mode after Reset() the last component of an arena is
  // cleared, but is then created again; in release mode, the last
  // component is not cleared. In either case, after Reset()
  // consumption() should equal the size of the last component which
  // is 512 bytes.
  arena.Reset();
  ASSERT_EQ(512, mem_tracker->consumption());

  // Allocate beyond allowed consumption. This should still go
  // through, since enforce_limit is false.
  allocated = arena.AllocateBytes(1024);
  ASSERT_TRUE(allocated);

  ASSERT_EQ(1536, mem_tracker->consumption());
}

TEST(TestArena, TestMemoryTrackingEnforced) {
  shared_ptr<MemTracker> mem_tracker = MemTracker::CreateTracker(1024, "arena-test-tracker");
  shared_ptr<MemoryTrackingBufferAllocator> allocator(
      new MemoryTrackingBufferAllocator(HeapBufferAllocator::Get(), mem_tracker,
                                        // enforce limit
                                        true));
  MemoryTrackingArena arena(256, allocator);
  ASSERT_EQ(256, mem_tracker->consumption());
  void *allocated = arena.AllocateBytes(256);
  ASSERT_TRUE(allocated);
  ASSERT_EQ(256, mem_tracker->consumption());
  allocated = arena.AllocateBytes(1024);
  ASSERT_FALSE(allocated);
  ASSERT_EQ(256, mem_tracker->consumption());
}

TEST(TestArena, TestSTLAllocator) {
  Arena a(256);
  typedef vector<int, ArenaAllocator<int, false> > ArenaVector;
  ArenaAllocator<int, false> alloc(&a);
  ArenaVector v(alloc);
  for (int i = 0; i < 10000; i++) {
    v.push_back(i);
  }
  for (int i = 0; i < 10000; i++) {
    ASSERT_EQ(i, v[i]);
  }
}

} // namespace kudu
