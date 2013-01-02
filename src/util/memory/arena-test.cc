// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <vector>

#include "gutil/stringprintf.h"
#include "util/memory/arena.h"

DEFINE_int32(num_threads, 16, "Number of threads to test");
DEFINE_int32(allocs_per_thread, 10000, "Number of allocations each thread should do");
DEFINE_int32(alloc_size, 4, "number of bytes in each allocation");

namespace kudu {

static void AllocateThread(Arena *arena, uint8_t thread_index) {
  std::vector<void *> ptrs;
  ptrs.reserve(FLAGS_allocs_per_thread);

  char buf[FLAGS_alloc_size];
  memset(buf, thread_index, FLAGS_alloc_size);

  for (int i = 0; i < FLAGS_allocs_per_thread; i++) {
    void *alloced = arena->AllocateBytes(FLAGS_alloc_size);
    CHECK(alloced);
    memcpy(alloced, buf, FLAGS_alloc_size);
    ptrs.push_back(alloced);
  }

  BOOST_FOREACH(void *p, ptrs) {
    if (memcmp(buf, p, FLAGS_alloc_size) != 0) {
      FAIL() << StringPrintf("overwritten pointer at %p", p);
    }
  }
}


TEST(TestArena, TestSingleThreaded) {
  Arena arena(128, 128);

  AllocateThread(&arena, 0);
}



TEST(TestArena, TestMultiThreaded) {
  CHECK(FLAGS_num_threads < 256);

  Arena arena(1024, 1024);

  boost::ptr_vector<boost::thread> threads;
  for (uint8_t i = 0; i < FLAGS_num_threads; i++) {
    threads.push_back(new boost::thread(AllocateThread, &arena, (uint8_t)i));
  }

  BOOST_FOREACH(boost::thread &thr, threads) {
    thr.join();
  }
}

}
