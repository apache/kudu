// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <vector>
#include "tablet/compaction_policy.h"
#include "util/stopwatch.h"
#include "util/test_util.h"

using std::vector;

namespace kudu {
namespace tablet {

// For float comparisons.
static const double kEpsilon = 0.01f;

// Save typing within the tests.
typedef BudgetedCompactionPolicy BCP;

TEST(TestBudgetedCompactionPolicy, TestStringFractionInRange) {
  // Simple base cases.
  ASSERT_NEAR(BCP::StringFractionInRange("a", "z", "a"), 0.0f, kEpsilon);
  ASSERT_NEAR(BCP::StringFractionInRange("a", "z", "z"), 1.0f, kEpsilon);
  ASSERT_NEAR(BCP::StringFractionInRange("a", "z", "m"), 0.48f, kEpsilon);

  // Different lengths with/without common prefixes
  ASSERT_NEAR(BCP::StringFractionInRange("a", "z", "mmmm"), 0.5f, kEpsilon);
  ASSERT_NEAR(BCP::StringFractionInRange("a", "zoo", "mmmm"), 0.48f, kEpsilon);
  ASSERT_NEAR(BCP::StringFractionInRange("a", "zoo", "z"), 0.98f, kEpsilon);
  ASSERT_NEAR(BCP::StringFractionInRange("0000", "9999", "4"), 0.42f, kEpsilon);
  ASSERT_NEAR(BCP::StringFractionInRange("0000a", "0000c", "0000b"), 0.5f, kEpsilon);
}

} // namespace tablet
} // namespace kudu
