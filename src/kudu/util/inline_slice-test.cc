// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gtest/gtest.h>
#include <vector>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/inline_slice.h"
#include "kudu/util/memory/arena.h"

namespace kudu {

template<size_t N>
static void TestRoundTrip(InlineSlice<N> *slice,
                          Arena *arena,
                          size_t test_size) {
  gscoped_ptr<uint8_t[]> buf(new uint8_t[test_size]);
  for (int i = 0; i < test_size; i++) {
    buf[i] = i & 0xff;
  }

  Slice test_input(buf.get(), test_size);

  slice->set(test_input, arena);
  Slice ret = slice->as_slice();
  ASSERT_TRUE(ret == test_input)
    << "test_size  =" << test_size << "\n"
    << "ret        = " << ret.ToDebugString() << "\n"
    << "test_input = " << test_input.ToDebugString();

  // If the data is small enough to fit inline, then
  // the returned slice should point directly into the
  // InlineSlice object.
  if (test_size < N) {
    ASSERT_EQ(reinterpret_cast<const uint8_t *>(slice) + 1,
              ret.data());
  }
}

// Sweep a variety of inputs for a given size of inline
// data
template<size_t N>
static void DoTest() {
  Arena arena(1024, 4096);

  // Test a range of inputs both growing and shrinking
  InlineSlice<N> my_slice;
  ASSERT_EQ(N, sizeof(my_slice));

  for (size_t to_test = 0; to_test < 1000; to_test++) {
    TestRoundTrip(&my_slice, &arena, to_test);
  }
  for (size_t to_test = 1000; to_test > 0; to_test--) {
    TestRoundTrip(&my_slice, &arena, to_test);
  }
}

TEST(TestInlineSlice, Test8ByteInline) {
  DoTest<8>();
}

TEST(TestInlineSlice, Test12ByteInline) {
  DoTest<12>();
}

TEST(TestInlineSlice, Test16ByteInline) {
  DoTest<16>();
}

} // namespace kudu
