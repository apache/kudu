// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/master/catalog_manager.h"

#include <numeric>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(require_new_spec_for_custom_hash_schema_range_bound);

using std::iota;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

// Test of the tablet assignment algo for splits done at table creation time.
// This tests that when we define a split, the tablet lands on the expected
// side of the split, i.e. it's a closed interval on the start key and an open
// interval on the end key (non-inclusive).
TEST(TableInfoTest, TestAssignmentRanges) {
  const string table_id = CURRENT_TEST_NAME();
  scoped_refptr<TableInfo> table(new TableInfo(table_id));
  vector<scoped_refptr<TabletInfo>> tablets;

  // Define & create the splits.
  const int kNumSplits = 3;
  string split_keys[kNumSplits] = { "a", "b", "c" };  // The keys we split on.
  for (int i = 0; i <= kNumSplits; i++) {
    const string& start_key = (i == 0) ? "" : split_keys[i - 1];
    const string& end_key = (i == kNumSplits) ? "" : split_keys[i];
    string tablet_id = Substitute("tablet-$0-$1", start_key, end_key);

    scoped_refptr<TabletInfo> tablet = new TabletInfo(table, tablet_id);
    {
      TabletMetadataLock meta_lock(tablet.get(), LockMode::WRITE);
      PartitionPB* partition = meta_lock.mutable_data()->pb.mutable_partition();
      partition->set_partition_key_start(start_key);
      partition->set_partition_key_end(end_key);
      meta_lock.mutable_data()->pb.set_state(SysTabletsEntryPB::RUNNING);
      meta_lock.Commit();
    }

    TabletMetadataLock meta_lock(tablet.get(), LockMode::READ);
    table->AddRemoveTablets({ tablet }, {});
    tablets.push_back(tablet);
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
    vector<scoped_refptr<TabletInfo>> tablets_in_range;
    ASSERT_OK(table->GetTabletsInRange(&req, &tablets_in_range));

    // Only one tablet should own this key.
    ASSERT_EQ(1, tablets_in_range.size());
    // The tablet with range start key matching 'start_key' should be the owner.
    ASSERT_EQ(tablet_id, (*tablets_in_range.begin())->id());
    LOG(INFO) << "Key " << start_key << " found in tablet " << tablet_id;
  }
}

TEST(TableInfoTest, GetTableLocationsLegacyCustomHashSchemas) {
  const string table_id = CURRENT_TEST_NAME();
  scoped_refptr<TableInfo> table(new TableInfo(table_id));

  {
    TableMetadataLock meta_lock(table.get(), LockMode::WRITE);
    auto* ps = meta_lock.mutable_data()->pb.mutable_partition_schema();
    // It's not really necessary to fill everything in the scope of this test.
    auto* range = ps->add_custom_hash_schema_ranges();
    range->mutable_range_bounds()->set_rows("a");
    auto* hash_dimension = range->add_hash_schema();
    hash_dimension->add_columns()->set_name("b");
    hash_dimension->set_num_buckets(2);
    meta_lock.Commit();
  }

  const string start_key = "a";
  const string end_key = "";
  string tablet_id = Substitute("tablet-$0-$1", start_key, end_key);

  scoped_refptr<TabletInfo> tablet = new TabletInfo(table, tablet_id);
  {
    TabletMetadataLock meta_lock(tablet.get(), LockMode::WRITE);
    PartitionPB* partition = meta_lock.mutable_data()->pb.mutable_partition();
    partition->set_partition_key_start(start_key);
    partition->set_partition_key_end(end_key);
    meta_lock.mutable_data()->pb.set_state(SysTabletsEntryPB::RUNNING);
    meta_lock.Commit();
  }

  TabletMetadataLock meta_lock(tablet.get(), LockMode::READ);
  table->AddRemoveTablets({ tablet }, {});

  // Query by specifying the start of the partition via the partition_key_start
  // field: it should pass even if the table has a range with custom hash schema
  // since as of now all the range partitions must have the number of hash
  // dimenstions fixed across all the ranges in a table.
  {
    GetTableLocationsRequestPB req;
    req.set_max_returned_locations(1);
    req.mutable_table()->mutable_table_name()->assign(table_id);
    req.mutable_partition_key_start()->assign("a");
    vector<scoped_refptr<TabletInfo>> tablets_in_range;
    ASSERT_OK(table->GetTabletsInRange(&req, &tablets_in_range));
    ASSERT_EQ(1, tablets_in_range.size());
  }

  // Query by specifying the start of the partition via the partition_key_start
  // field: it should fail since the table has a range with custom hash schema
  // and --require_new_spec_for_custom_hash_schema_range_bound=true.
  {
    FLAGS_require_new_spec_for_custom_hash_schema_range_bound = true;
    GetTableLocationsRequestPB req;
    req.set_max_returned_locations(1);
    req.mutable_table()->mutable_table_name()->assign(table_id);
    req.mutable_partition_key_start()->assign("a");
    vector<scoped_refptr<TabletInfo>> tablets_in_range;
    auto s = table->GetTabletsInRange(&req, &tablets_in_range);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "for a table with custom per-range hash schemas "
                        "the range must be specified using partition_key_range "
                        "field, not partition_key_{start,end} fields");
  }

  // Query by specifying the start of the partition via the partition_key_range
  // field: it should succeed.
  {
    GetTableLocationsRequestPB req;
    req.set_max_returned_locations(1);
    req.mutable_table()->mutable_table_name()->assign(table_id);
    req.mutable_key_start()->set_hash_key(string("\0\0\0\0", 4));
    req.mutable_key_start()->set_range_key("a");
    vector<scoped_refptr<TabletInfo>> tablets_in_range;
    ASSERT_OK(table->GetTabletsInRange(&req, &tablets_in_range));
    ASSERT_EQ(1, tablets_in_range.size());
  }
}

TEST(TestTSDescriptor, TestReplicaCreationsDecay) {
  TSDescriptor ts("test");
  ASSERT_EQ(0, ts.RecentReplicaCreations());
  ts.IncrementRecentReplicaCreations();

  // The load should start at close to 1.0.
  double val_a = ts.RecentReplicaCreations();
  ASSERT_NEAR(1.0, val_a, 0.05);

  // After 10ms it should have dropped a bit, but still be close to 1.0.
  SleepFor(MonoDelta::FromMilliseconds(10));
  double val_b = ts.RecentReplicaCreations();
  ASSERT_LT(val_b, val_a);
  ASSERT_NEAR(0.99, val_a, 0.05);

  if (AllowSlowTests()) {
    // After 10 seconds, we should have dropped to 0.5^(10/60) = 0.891
    SleepFor(MonoDelta::FromSeconds(10));
    ASSERT_NEAR(0.891, ts.RecentReplicaCreations(), 0.05);
  }
}
TEST(TableInfoTest, MaxReturnedLocationsNotSpecified) {
  const string table_id = CURRENT_TEST_NAME();
  scoped_refptr<TableInfo> table(new TableInfo(table_id));
  vector<scoped_refptr<TabletInfo>> tablets;

  vector<string> ranges(128);
  std::iota(ranges.begin(), ranges.end(), '\0');
  for (auto it = ranges.begin(); it != ranges.end(); ++it) {
    auto next_it = it;
    ++next_it;
    const string& start_key = *it;
    const string& end_key = (next_it == ranges.end()) ? "" : *next_it;

    string tablet_id = Substitute("tablet-$0-$1", start_key, end_key);

    scoped_refptr<TabletInfo> tablet = new TabletInfo(table, tablet_id);
    {
      TabletMetadataLock meta_lock(tablet.get(), LockMode::WRITE);
      PartitionPB* partition = meta_lock.mutable_data()->pb.mutable_partition();
      partition->set_partition_key_start(start_key);
      if (next_it != ranges.end()) {
        partition->set_partition_key_end(end_key);
      }
      meta_lock.mutable_data()->pb.set_state(SysTabletsEntryPB::RUNNING);
      meta_lock.Commit();
    }

    TabletMetadataLock meta_lock(tablet.get(), LockMode::READ);
    table->AddRemoveTablets({ tablet }, {});
    tablets.push_back(tablet);
  }

  // Fetch all the available tablets.
  {
    GetTableLocationsRequestPB req;
    req.clear_max_returned_locations(); // the default is 10 in protobuf
    req.mutable_table()->mutable_table_name()->assign(table_id);
    // Query using the start key of the first tablet's range.
    req.mutable_partition_key_start()->assign("\0");
    vector<scoped_refptr<TabletInfo>> tablets_in_range;
    ASSERT_OK(table->GetTabletsInRange(&req, &tablets_in_range));

    // The response should contain information on every tablet created.
    ASSERT_EQ(ranges.size(), tablets_in_range.size());
  }
}

} // namespace master
} // namespace kudu
