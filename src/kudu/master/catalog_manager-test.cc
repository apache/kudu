// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace master {

using strings::Substitute;

// Test of the tablet assignment algo for splits done at table creation time.
// This tests that when we define a split, the tablet lands on the expected
// side of the split, i.e. it's a closed interval on the start key and an open
// interval on the end key (non-inclusive).
TEST(TableInfoTest, TestAssignmentRanges) {
  const string table_id = CURRENT_TEST_NAME();
  scoped_refptr<TableInfo> table(new TableInfo(table_id));

  // Define & create the splits.
  const int kNumSplits = 3;
  string split_keys[kNumSplits] = { "a", "b", "c" };  // The keys we split on.
  for (int i = 0; i <= kNumSplits; i++) {
    const string& start_key = (i == 0) ? "" : split_keys[i - 1];
    const string& end_key = (i == kNumSplits) ? "" : split_keys[i];
    string tablet_id = Substitute("tablet-$0-$1", start_key, end_key);

    TabletInfo* tablet = new TabletInfo(table, tablet_id);
    TabletMetadataLock meta_lock(tablet, TabletMetadataLock::WRITE);

    PartitionPB* partition = meta_lock.mutable_data()->pb.mutable_partition();
    partition->set_partition_key_start(start_key);
    partition->set_partition_key_end(end_key);
    meta_lock.mutable_data()->pb.set_state(SysTabletsEntryPB::RUNNING);

    table->AddTablet(tablet);
  }

  // Ensure they give us what we are expecting.
  for (int i = 0; i <= kNumSplits; i++) {
    // Calculate the tablet id and start key.
    const string& start_key = (i == 0) ? "" : split_keys[i - 1];
    const string& end_key = (i == kNumSplits) ? "" : split_keys[i];
    string tablet_id = Substitute("tablet-$0-$1", start_key, end_key);

    // Query using the start key.
    GetTableLocationsRequestPB req;
    req.set_max_returned_locations(1);
    req.mutable_table()->mutable_table_name()->assign(table_id);
    req.mutable_partition_key_start()->assign(start_key);
    vector<scoped_refptr<TabletInfo> > tablets_in_range;
    table->GetTabletsInRange(&req, &tablets_in_range);

    // Only one tablet should own this key.
    ASSERT_EQ(1, tablets_in_range.size());
    // The tablet with range start key matching 'start_key' should be the owner.
    ASSERT_EQ(tablet_id, (*tablets_in_range.begin())->tablet_id());
    LOG(INFO) << "Key " << start_key << " found in tablet " << tablet_id;
  }
}

} // namespace master
} // namespace kudu
