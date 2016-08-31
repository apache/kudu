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

#include "kudu/tools/tool_action.h"

#include <algorithm>
#include <iterator>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tools {

using client::KuduClient;
using client::KuduClientBuilder;
using std::cout;
using std::endl;
using std::string;
using std::unique_ptr;
using std::vector;

namespace {

const char* const kMasterAddressesArg = "master_addresses";
const char* const kTableNameArg = "table_name";

Status DeleteTable(const RunnerContext& context) {
  string master_addresses_str = FindOrDie(context.required_args,
                                          kMasterAddressesArg);
  vector<string> master_addresses = strings::Split(master_addresses_str, ",");
  string table_name = FindOrDie(context.required_args, kTableNameArg);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(KuduClientBuilder()
                .master_server_addrs(master_addresses)
                .Build(&client));
  return client->DeleteTable(table_name);
}

Status ListTables(const RunnerContext& context) {
  string master_addresses_str = FindOrDie(context.required_args,
                                          kMasterAddressesArg);
  vector<string> master_addresses = strings::Split(master_addresses_str, ",");

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(KuduClientBuilder()
                .master_server_addrs(master_addresses)
                .Build(&client));
  vector<string> table_names;
  RETURN_NOT_OK(client->ListTables(&table_names));

  std::copy(table_names.begin(), table_names.end(),
            std::ostream_iterator<string>(cout, "\n"));
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildTableMode() {
  unique_ptr<Action> delete_table =
      ActionBuilder("delete", &DeleteTable)
      .Description("Delete a table")
      .AddRequiredParameter({
        kMasterAddressesArg,
        "Comma-separated list of Kudu Master addresses where each address is "
        "of form 'hostname:port'" })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to delete" })
      .Build();

  unique_ptr<Action> list_tables =
      ActionBuilder("list", &ListTables)
      .Description("List all tables")
      .AddRequiredParameter({
        kMasterAddressesArg,
        "Comma-separated list of Kudu Master addresses where each address is "
        "of form 'hostname:port'" })
      .Build();

  return ModeBuilder("table")
      .Description("Operate on Kudu tables")
      .AddAction(std::move(delete_table))
      .AddAction(std::move(list_tables))
      .Build();
}

} // namespace tools
} // namespace kudu

