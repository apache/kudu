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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/regex.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace master {

class RestCatalogTestBase : public KuduTest {
 protected:
  Status CreateTestTable(const std::string& owner = "") {
    client::KuduSchema schema;
    client::KuduSchemaBuilder b;
    b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(client::KuduColumnSchema::INT32)->NotNull();
    RETURN_NOT_OK(b.Build(&schema));
    const std::vector<std::string> column_names{ "key" };

    // Set the schema and range partition columns.
    std::unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
    table_creator->table_name(kTableName).schema(&schema).set_range_partition_columns(column_names);

    // Generate and add the range partition splits for the table.
    constexpr int32_t increment = 1000 / 10;
    for (int32_t i = 1; i < 10; i++) {
      KuduPartialRow* row = schema.NewRow();
      KUDU_CHECK_OK(row->SetInt32(0, i * increment));
      table_creator->add_range_partition_split(row);
    }
    table_creator->num_replicas(1);
    if (!owner.empty()) {
      table_creator->set_owner(owner);
    }
    return table_creator->Create();
  }

  Status GetTableId(const std::string& table_name, std::string* table_id) {
    DCHECK(table_id);
    client::sp::shared_ptr<client::KuduTable> table;
    RETURN_NOT_OK(client_->OpenTable(table_name, &table));
    *table_id = table->id();
    return Status::OK();
  }

  client::sp::shared_ptr<client::KuduClient> client_;
  std::string kTableName = "test_table";
};

}  // namespace master
}  // namespace kudu
