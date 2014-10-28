// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

// This unit test belongs in gutil, but it depends on test_main which is
// part of util.
#include "kudu/gutil/map-util.h"

#include <gtest/gtest.h>
#include <map>

using std::map;

namespace kudu {

TEST(FloorTest, TestMapUtil) {
  map<int, int> my_map;

  ASSERT_EQ(NULL, FindFloorOrNull(my_map, 5));

  my_map[5] = 5;
  ASSERT_EQ(5, *FindFloorOrNull(my_map, 6));
  ASSERT_EQ(5, *FindFloorOrNull(my_map, 5));
  ASSERT_EQ(NULL, FindFloorOrNull(my_map, 4));

  my_map[1] = 1;
  ASSERT_EQ(5, *FindFloorOrNull(my_map, 6));
  ASSERT_EQ(5, *FindFloorOrNull(my_map, 5));
  ASSERT_EQ(1, *FindFloorOrNull(my_map, 4));
  ASSERT_EQ(1, *FindFloorOrNull(my_map, 1));
  ASSERT_EQ(NULL, FindFloorOrNull(my_map, 0));

}

} // namespace kudu
