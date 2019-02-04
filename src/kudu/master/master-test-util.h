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

#ifndef KUDU_MASTER_TEST_UTIL_H_
#define KUDU_MASTER_TEST_UTIL_H_

#include <algorithm>
#include <string>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace master {

Status WaitForRunningTabletCount(MiniMaster* mini_master,
                                 const std::string& table_name,
                                 int expected_count,
                                 GetTableLocationsResponsePB* resp) {
  int wait_time = 1000;

  SCOPED_LOG_TIMING(INFO, strings::Substitute("waiting for tablet count of $0", expected_count));
  while (true) {
    GetTableLocationsRequestPB req;
    resp->Clear();
    req.mutable_table()->set_table_name(table_name);
    req.set_max_returned_locations(expected_count);
    CatalogManager* catalog = mini_master->master()->catalog_manager();
    {
      CatalogManager::ScopedLeaderSharedLock l(catalog);
      RETURN_NOT_OK(l.first_failed_status());
      RETURN_NOT_OK(catalog->GetTableLocations(&req, resp, /*user=*/boost::none));
    }
    if (resp->tablet_locations_size() >= expected_count) {
      return Status::OK();
    }

    LOG(INFO) << "Waiting for " << expected_count << " tablets for table "
              << table_name << ". So far we have " << resp->tablet_locations_size();

    SleepFor(MonoDelta::FromMicroseconds(wait_time));
    wait_time = std::min(wait_time * 5 / 4, 1000000);
  }

  // Unreachable.
  LOG(FATAL) << "Reached unreachable section";
  return Status::RuntimeError("Unreachable statement"); // Suppress compiler warnings.
}

} // namespace master
} // namespace kudu

#endif /* KUDU_MASTER_TEST_UTIL_H_ */
