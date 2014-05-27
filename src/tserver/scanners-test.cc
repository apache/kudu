// Copyright(c) 2013, Cloudera, inc.
#include "tserver/scanners.h"

#include <gtest/gtest.h>
#include "util/test_util.h"

namespace kudu {
namespace tserver {

TEST(ScannersTest, TestManager) {
  ScannerManager mgr;

  // Create two scanners, make sure their ids are different.
  SharedScanner s1, s2;
  mgr.NewScanner("", "", &s1);
  mgr.NewScanner("", "", &s2);
  ASSERT_NE(s1->id(), s2->id());

  // Check that they're both registered.
  SharedScanner result;
  ASSERT_TRUE(mgr.LookupScanner(s1->id(), &result));
  ASSERT_EQ(result.get(), s1.get());

  ASSERT_TRUE(mgr.LookupScanner(s2->id(), &result));
  ASSERT_EQ(result.get(), s2.get());

  // Check that looking up a bad scanner returns false.
  ASSERT_FALSE(mgr.LookupScanner("xxx", &result));

  // Remove the scanners.
  ASSERT_TRUE(mgr.UnregisterScanner(s1->id()));
  ASSERT_TRUE(mgr.UnregisterScanner(s2->id()));

  // Removing a missing scanner should return false.
  ASSERT_FALSE(mgr.UnregisterScanner("xxx"));
}

} // namespace tserver
} // namespace kudu
