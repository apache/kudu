// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.

#include "kudu/cfile/bloomfile-test-base.h"

#include <boost/bind.hpp>

#include "kudu/util/thread.h"

DEFINE_int32(benchmark_num_threads, 8, "Number of threads to use for the benchmark");

namespace kudu {
namespace cfile {

class MTBloomFileTest : public BloomFileTestBase {
};

#ifdef NDEBUG
TEST_F(MTBloomFileTest, Benchmark) {
  ASSERT_NO_FATAL_FAILURE(WriteTestBloomFile());
  ASSERT_OK(OpenBloomFile());

  vector<scoped_refptr<kudu::Thread> > threads;

  for (int i = 0; i < FLAGS_benchmark_num_threads; i++) {
    scoped_refptr<kudu::Thread> new_thread;
    CHECK_OK(Thread::Create("test", strings::Substitute("t$0", i),
                            boost::bind(&BloomFileTestBase::ReadBenchmark, this),
                            &new_thread));
    threads.push_back(new_thread);
  }
  BOOST_FOREACH(scoped_refptr<kudu::Thread>& t, threads) {
    t->Join();
  }
}
#endif

} // namespace cfile
} // namespace kudu
