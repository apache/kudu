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
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/status.h"

DEFINE_bool(list_tablets, false,
            "Include tablet and replica UUIDs in the output");

namespace kudu {
namespace tools {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduScanToken;
using client::KuduScanTokenBuilder;
using client::KuduTable;
using std::cout;
using std::endl;
using std::string;
using std::unique_ptr;
using std::vector;

namespace {

const char* const kTableNameArg = "table_name";

Status DeleteTable(const RunnerContext& context) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  vector<string> master_addresses = strings::Split(master_addresses_str, ",");
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(KuduClientBuilder()
                .master_server_addrs(master_addresses)
                .Build(&client));
  return client->DeleteTable(table_name);
}

Status ListTables(const RunnerContext& context) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  vector<string> master_addresses = strings::Split(master_addresses_str, ",");

  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(KuduClientBuilder()
                .master_server_addrs(master_addresses)
                .Build(&client));
  vector<string> table_names;
  RETURN_NOT_OK(client->ListTables(&table_names));

  for (const auto& tname : table_names) {
    cout << tname << endl;
    if (!FLAGS_list_tablets) {
      continue;
    }
    client::sp::shared_ptr<KuduTable> client_table;
    RETURN_NOT_OK(client->OpenTable(tname, &client_table));
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(client_table.get());
    RETURN_NOT_OK(builder.Build(&tokens));

    for (const auto* token : tokens) {
      cout << "T " << token->tablet().id() << "\t";
      for (const auto* replica : token->tablet().replicas()) {
        cout << "P" << (replica->is_leader() ? "(L) " : "    ")
             << replica->ts().uuid() << "(" << replica->ts().hostname()
             << ":" << replica->ts().port() << ")" << "    ";
      }
      cout << endl;
    }
    cout << endl;
  }
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildTableMode() {
  unique_ptr<Action> delete_table =
      ActionBuilder("delete", &DeleteTable)
      .Description("Delete a table")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to delete" })
      .Build();

  unique_ptr<Action> list_tables =
      ActionBuilder("list", &ListTables)
      .Description("List all tables")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddOptionalParameter("list_tablets")
      .Build();

  return ModeBuilder("table")
      .Description("Operate on Kudu tables")
      .AddAction(std::move(delete_table))
      .AddAction(std::move(list_tables))
      .Build();
}

} // namespace tools
} // namespace kudu

