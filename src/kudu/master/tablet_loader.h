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
#pragma once

#include <string>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/util/status.h"

namespace kudu {

class PartitionPB;

namespace master {

class CatalogManager;
class SysTabletsEntryPB;

////////////////////////////////////////////////////////////
// Tablet Loader
////////////////////////////////////////////////////////////

class TabletLoader : public TabletVisitor {
 public:
  explicit TabletLoader(CatalogManager* catalog_manager);

  Status VisitTablet(const std::string& table_id,
                     const std::string& tablet_id,
                     const SysTabletsEntryPB& metadata) override;
 private:
  FRIEND_TEST(SysCatalogTest, TabletRangesConversionFromLegacyFormat);

  // Check if the stringified partition keys in the
  // PartitionPB.{partition_key_start, partition_key_end} fields are in
  // pre-KUDU-2671 (i.e. legacy) representation, and if so, then convert
  // them into post-KUDU-2671 representation.
  //
  // For details, see https://github.com/apache/kudu/commit/8df970f7a652
  //
  // This method returns 'true' if lower or upper bound in the in-out parameter
  // 'p' has been converted, 'false' otherwise.
  static bool ConvertFromLegacy(PartitionPB* p);

  CatalogManager* catalog_manager_;

  DISALLOW_COPY_AND_ASSIGN(TabletLoader);
};

} // namespace master
} // namespace kudu
