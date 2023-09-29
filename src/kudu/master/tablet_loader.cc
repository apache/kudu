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

#include "kudu/master/tablet_loader.h"

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partition.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.pb.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/pb_util.h"

using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using std::vector;
using std::string;
using strings::Substitute;

namespace kudu {
namespace master {

TabletLoader::TabletLoader(CatalogManager* catalog_manager)
    : catalog_manager_(catalog_manager) {
}

Status TabletLoader::VisitTablet(const string& table_id,
                                 const string& tablet_id,
                                 const SysTabletsEntryPB& metadata) {
  // Lookup the table.
  scoped_refptr<TableInfo> table(FindPtrOrNull(
      catalog_manager_->table_ids_map_, table_id));
  if (table == nullptr) {
    // Tables and tablets are always created/deleted in one operation, so
    // this shouldn't be possible.
    string msg = Substitute("missing table $0 required by tablet $1 (metadata: $2)",
                            table_id, tablet_id, SecureDebugString(metadata));
    LOG(ERROR) << msg;
    return Status::Corruption(msg);
  }

  // Set up the tablet info.
  scoped_refptr<TabletInfo> tablet = new TabletInfo(table, tablet_id);
  TabletMetadataLock l(tablet.get(), LockMode::WRITE);
  l.mutable_data()->pb.CopyFrom(metadata);

  // If the system catalog contains informartion on tablets created with
  // Kudu of versions 1.16.0 and earlier, transform the legacy partition key
  // representation into post-KUDU-2671 form. In essence, after KUDU-2671
  // has been implemented, the hash-related part of the partition key for
  // an unbounded range is built using slightly different rules by the
  // PartitionSchema::UpdatePartitionBoundaries() method. For details, see
  // https://github.com/apache/kudu/commit/8df970f7a652
  if (ConvertFromLegacy(l.mutable_data()->pb.mutable_partition())) {
    LOG(INFO) << Substitute(
        "converted legacy boundary representation for tablet $0 (table $1)",
        tablet_id, table->ToString());
  }

  // Add the tablet to the tablet manager.
  catalog_manager_->tablet_map_[tablet->id()] = tablet;

  // Add the tablet to the table.
  bool is_deleted = l.mutable_data()->is_deleted();
  l.Commit();
  if (!is_deleted) {
    // Need to use a new tablet lock here because AddRemoveTablets() reads
    // from clean state, which is uninitialized for these brand new tablets.
    TabletMetadataLock l(tablet.get(), LockMode::READ);
    table->AddRemoveTablets({ tablet }, {});
    LOG(INFO) << Substitute("loaded metadata for tablet $0 (table $1)",
                            tablet_id, table->ToString());
  }

  VLOG(2) << Substitute("metadata for tablet $0: $1",
                        tablet_id, SecureShortDebugString(metadata));
  return Status::OK();
}

bool TabletLoader::ConvertFromLegacy(PartitionPB* p) {
  if (p->hash_buckets_size() == 0) {
    return false;
  }

  // Build std::vector out of PartitionPB.hash_buckets field.
  vector<int32_t> hash_buckets(p->hash_buckets_size());
  for (size_t i = 0; i < p->hash_buckets_size(); ++i) {
    hash_buckets[i] = p->hash_buckets(i);
  }

  bool converted_lower = false;
  auto* key_start_str = p->mutable_partition_key_start();
  auto lower_bound = Partition::StringToPartitionKey(
      *key_start_str, hash_buckets.size());
  if (PartitionKey::IsLegacy(lower_bound, hash_buckets)) {
    const PartitionKey orig(lower_bound);
    lower_bound = PartitionKey::LowerBoundFromLegacy(
        lower_bound, hash_buckets);
    *key_start_str = lower_bound.ToString();
    LOG(INFO) << Substitute("converted lower range boundary: '$0' --> '$1'",
                            orig.DebugString(), lower_bound.DebugString());
    converted_lower = true;
  }

  bool converted_upper = false;
  auto* key_end_str = p->mutable_partition_key_end();
  auto upper_bound = Partition::StringToPartitionKey(
      *key_end_str, hash_buckets.size());
  if (PartitionKey::IsLegacy(upper_bound, hash_buckets)) {
    const PartitionKey orig(upper_bound);
    upper_bound = PartitionKey::UpperBoundFromLegacy(
        upper_bound, hash_buckets);
    LOG(INFO) << Substitute("converted lower upper boundary: '$0' --> '$1'",
                            orig.DebugString(), upper_bound.DebugString());
    *key_end_str = upper_bound.ToString();
    converted_upper = true;
  }

  return converted_lower || converted_upper;
}

} // namespace master
} // namespace kudu
