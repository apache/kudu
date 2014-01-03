// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_MASTER_TEST_UTIL_H_
#define KUDU_MASTER_TEST_UTIL_H_

#include <boost/assign/list_of.hpp>

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

  {
    GetTableLocationsRequestPB req;
    GetTableLocationsResponsePB resp;

    int wait_time = 1000;
    for (int i = 0; i < 80; ++i) {
      req.mutable_table()->set_table_name(table_name);
      ASSERT_STATUS_OK(mini_master->master()->catalog_manager()->GetTableLocations(&req, &resp));
      if (resp.tablet_locations_size() > 0) {
        *tablet_id = resp.tablet_locations(0).tablet_id();
        LOG(INFO) << "Got tablet " << *tablet_id << " for table " << table_name;
        return;
      }
      VLOG(1) << "WAITING FOR A TABLET";

      usleep(wait_time);
      wait_time = wait_time * 5 / 4;
    };
  }

  FAIL() << "Unable to get a tablet for table " << table_name;
}

} // namespace master
} // namespace kudu

#endif /* KUDU_MASTER_TEST_UTIL_H_ */
