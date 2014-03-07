// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_MASTER_TEST_UTIL_H_
#define KUDU_MASTER_TEST_UTIL_H_

#include <boost/assign/list_of.hpp>

#include <algorithm>
#include <string>

#include "common/schema.h"
#include "common/wire_protocol.h"
#include "master/catalog_manager.h"
#include "master/mini_master.h"
#include "master/master.h"
#include "master/master.pb.h"
#include "util/test_util.h"

namespace kudu {
namespace master {

Status WaitForRunningTabletCount(MiniMaster* mini_master,
                                 const string& table_name,
                                 int expected_count,
                                 GetTableLocationsResponsePB* resp) {
  int wait_time = 1000;

  for (int i = 0; i < 80; ++i) {
    GetTableLocationsRequestPB req;
    resp->Clear();
    req.mutable_table()->set_table_name(table_name);
    req.set_max_returned_locations(expected_count);
    RETURN_NOT_OK(mini_master->master()->catalog_manager()->GetTableLocations(&req, resp));
    if (resp->tablet_locations_size() >= expected_count) {
      bool is_stale = false;
      BOOST_FOREACH(const TabletLocationsPB& loc, resp->tablet_locations()) {
        is_stale |= loc.stale();
      }

      if (!is_stale) {
        return Status::OK();
      }
    }

    VLOG(1) << "Waiting for " << expected_count << " tablets for table \""
            << table_name << "\". So far have "
            << resp->tablet_locations_size();

    usleep(wait_time);
    wait_time = std::min(wait_time * 5 / 4, 1000000);
  }

  return Status::TimedOut("Timed out waiting for table", table_name);
}

void CreateTabletForTesting(MiniMaster* mini_master,
                            const string& table_name,
                            const Schema& schema,
                            string *tablet_id) {
  {
    CreateTableRequestPB req;
    CreateTableResponsePB resp;

    req.set_name(table_name);
    ASSERT_STATUS_OK(SchemaToPB(schema, req.mutable_schema()));
    ASSERT_STATUS_OK(mini_master->master()->catalog_manager()->CreateTable(&req, &resp, NULL));
  }

  int wait_time = 1000;
  bool is_table_created = false;
  for (int i = 0; i < 80; ++i) {
    IsCreateTableDoneRequestPB req;
    IsCreateTableDoneResponsePB resp;

    req.mutable_table()->set_table_name(table_name);
    ASSERT_STATUS_OK(mini_master->master()->catalog_manager()->IsCreateTableDone(&req, &resp));
    if (resp.done()) {
      is_table_created = true;
      break;
    }

    VLOG(1) << "Waiting for table '" << table_name << "'to be created";

    usleep(wait_time);
    wait_time = std::min(wait_time * 5 / 4, 1000000);
  }
  ASSERT_TRUE(is_table_created);

  GetTableLocationsResponsePB resp;
  ASSERT_STATUS_OK(WaitForRunningTabletCount(mini_master, table_name, 1, &resp));
  *tablet_id = resp.tablet_locations(0).tablet_id();
  LOG(INFO) << "Got tablet " << *tablet_id << " for table " << table_name;
}

} // namespace master
} // namespace kudu

#endif /* KUDU_MASTER_TEST_UTIL_H_ */
