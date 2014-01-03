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
                            string *tablet_id) {
  CreateTableRequestPB req;
  CreateTableResponsePB resp;

  req.set_name(table_name);

  Schema schema(boost::assign::list_of
                (ColumnSchema("key", UINT32)),
                1);

  ASSERT_STATUS_OK(SchemaToPB(schema, req.mutable_schema()));

  ASSERT_STATUS_OK(mini_master->master()->catalog_manager()->CreateTable(&req, &resp, NULL));
  ASSERT_EQ(1, resp.tablet_ids_size());
  *tablet_id = resp.tablet_ids(0);

  LOG(INFO) << "Created tablet " << *tablet_id << " for table " << table_name;
}

} // namespace master
} // namespace kudu

#endif /* KUDU_MASTER_TEST_UTIL_H_ */
