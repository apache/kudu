// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <tr1/unordered_set>

#include "kudu/gutil/map-util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/test_util.h"

DEFINE_int32(flag_with_no_tags, 0, "test flag that has no tags");

DEFINE_int32(flag_with_one_tag, 0, "test flag that has 1 tag");
TAG_FLAG(flag_with_one_tag, stable);

DEFINE_int32(flag_with_two_tags, 0, "test flag that has 2 tags");
TAG_FLAG(flag_with_two_tags, evolving);
TAG_FLAG(flag_with_two_tags, unsafe);

using std::string;
using std::tr1::unordered_set;

namespace kudu {

class FlagTagsTest : public KuduTest {
};

TEST_F(FlagTagsTest, TestTags) {
  unordered_set<string> tags;
  GetFlagTags("flag_with_no_tags", &tags);
  EXPECT_EQ(0, tags.size());

  GetFlagTags("flag_with_one_tag", &tags);
  EXPECT_EQ(1, tags.size());
  EXPECT_TRUE(ContainsKey(tags, "stable"));

  GetFlagTags("flag_with_two_tags", &tags);
  EXPECT_EQ(2, tags.size());
  EXPECT_TRUE(ContainsKey(tags, "evolving"));
  EXPECT_TRUE(ContainsKey(tags, "unsafe"));

  GetFlagTags("missing_flag", &tags);
  EXPECT_EQ(0, tags.size());
}

} // namespace kudu
