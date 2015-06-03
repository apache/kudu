// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/slice.h"

#include <boost/foreach.hpp>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"

using std::string;

namespace kudu {

typedef SliceMap<int>::type MySliceMap;

TEST(SliceTest, TestSliceMap) {
  MySliceMap my_map;
  Slice a("a");
  Slice b("b");
  Slice c("c");

  // Insertion is deliberately out-of-order; the map should restore order.
  InsertOrDie(&my_map, c, 3);
  InsertOrDie(&my_map, a, 1);
  InsertOrDie(&my_map, b, 2);

  int expectedValue = 0;
  BOOST_FOREACH(const MySliceMap::value_type& pair, my_map) {
    int data = 'a' + expectedValue++;
    ASSERT_EQ(Slice(reinterpret_cast<uint8_t*>(&data), 1), pair.first);
    ASSERT_EQ(expectedValue, pair.second);
  }

  expectedValue = 0;
  for (MySliceMap::iterator iter = my_map.begin(); iter != my_map.end(); iter++) {
    int data = 'a' + expectedValue++;
    ASSERT_EQ(Slice(reinterpret_cast<uint8_t*>(&data), 1), iter->first);
    ASSERT_EQ(expectedValue, iter->second);
  }
}

} // namespace kudu
