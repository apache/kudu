// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include "tablet/compaction_rowset_data.h"

using std::vector;

namespace kudu {
namespace tablet {
namespace compaction_policy {

// For float comparisons.
static const double kEpsilon = 0.01f;

TEST(TestDataSizeCDF, TestStringFractionInRange) {
  // Simple base cases.
  ASSERT_NEAR(DataSizeCDF::StringFractionInRange("a", "z", "a"), 0.0f, kEpsilon);
  ASSERT_NEAR(DataSizeCDF::StringFractionInRange("a", "z", "z"), 1.0f, kEpsilon);
  ASSERT_NEAR(DataSizeCDF::StringFractionInRange("a", "z", "m"), 0.48f, kEpsilon);

  // Different lengths with/without common prefixes
  ASSERT_NEAR(DataSizeCDF::StringFractionInRange("a", "z", "mmmm"), 0.5f, kEpsilon);
  ASSERT_NEAR(DataSizeCDF::StringFractionInRange("a", "zoo", "mmmm"), 0.48f, kEpsilon);
  ASSERT_NEAR(DataSizeCDF::StringFractionInRange("a", "zoo", "z"), 0.98f, kEpsilon);
  ASSERT_NEAR(DataSizeCDF::StringFractionInRange("0000", "9999", "4"), 0.42f, kEpsilon);
  ASSERT_NEAR(DataSizeCDF::StringFractionInRange("0000a", "0000c", "0000b"), 0.5f, kEpsilon);

  // Case where the key falls outside the range
  ASSERT_LE(DataSizeCDF::StringFractionInRange("b", "y", "a"), 0.0f);
  ASSERT_GE(DataSizeCDF::StringFractionInRange("b", "y", "z"), 1.0f);

  // Case for degenerate range.
  ASSERT_GE(DataSizeCDF::StringFractionInRange("b", "b", "b"), 1.0f);
}

} // namespace compaction_policy
} // namespace tablet
} // namespace kudu
