// Copyright(c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/tserver/scanners.h"

#include <vector>

#include <gtest/gtest.h>
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tserver/scanner_metrics.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_util.h"

DECLARE_int32(scanner_ttl_ms);

namespace kudu {

using tablet::TabletPeer;

namespace tserver {

using std::vector;

TEST(ScannersTest, TestManager) {
  scoped_refptr<TabletPeer> null_peer(NULL);
  ScannerManager mgr(NULL);

  // Create two scanners, make sure their ids are different.
  SharedScanner s1, s2;
  mgr.NewScanner(null_peer, "", &s1);
  mgr.NewScanner(null_peer, "", &s2);
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

TEST(ScannerTest, TestExpire) {
  scoped_refptr<TabletPeer> null_peer(NULL);
  FLAGS_scanner_ttl_ms = 100;
  MetricRegistry registry;
  ScannerManager mgr(METRIC_ENTITY_server.Instantiate(&registry, "test"));
  SharedScanner s1, s2;
  mgr.NewScanner(null_peer, "", &s1);
  mgr.NewScanner(null_peer, "", &s2);
  SleepFor(MonoDelta::FromMilliseconds(200));
  s2->UpdateAccessTime();
  mgr.RemoveExpiredScanners();
  ASSERT_EQ(1, mgr.CountActiveScanners());
  ASSERT_EQ(1, mgr.metrics_->scanners_expired->value());
  vector<SharedScanner> active_scanners;
  mgr.ListScanners(&active_scanners);
  ASSERT_EQ(s2->id(), active_scanners[0]->id());
}

} // namespace tserver
} // namespace kudu
