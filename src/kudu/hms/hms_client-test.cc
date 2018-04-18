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

#include "kudu/hms/hms_client.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/none_t.hpp>
#include <boost/optional/optional.hpp>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using boost::optional;
using kudu::rpc::SaslProtection;
using std::make_pair;
using std::string;
using std::vector;

namespace kudu {
namespace hms {

class HmsClientTest : public KuduTest,
                      public ::testing::WithParamInterface<optional<SaslProtection::Type>> {
 public:

  Status CreateTable(HmsClient* client,
                     const string& database_name,
                     const string& table_name,
                     const string& table_id) {
    hive::Table table;
    table.dbName = database_name;
    table.tableName = table_name;
    table.tableType = HmsClient::kManagedTable;

    table.__set_parameters({
        make_pair(HmsClient::kKuduTableIdKey, table_id),
        make_pair(HmsClient::kKuduMasterAddrsKey, string("TODO")),
        make_pair(HmsClient::kStorageHandlerKey, HmsClient::kKuduStorageHandler),
    });

    return client->CreateTable(table);
  }

  Status DropTable(HmsClient* client,
                   const string& database_name,
                   const string& table_name,
                   const string& table_id) {
    hive::EnvironmentContext env_ctx;
    env_ctx.__set_properties({ make_pair(HmsClient::kKuduTableIdKey, table_id) });
    return client->DropTableWithContext(database_name, table_name, env_ctx);
  }
};

INSTANTIATE_TEST_CASE_P(ProtectionTypes,
                        HmsClientTest,
                        ::testing::Values(boost::none
                                        , SaslProtection::kIntegrity
// On macos, krb5 has issues repeatedly spinning up new KDCs ('unable to reach
// any KDC in realm KRBTEST.COM, tried 1 KDC'). Integrity protection gives us
// good coverage, so we disable the other variants.
#ifndef __APPLE__
                                        , SaslProtection::kAuthentication
                                        , SaslProtection::kPrivacy
#endif
                                          ));

TEST_P(HmsClientTest, TestHmsOperations) {
  optional<SaslProtection::Type> protection = GetParam();
  MiniKdc kdc;
  MiniHms hms;
  HmsClientOptions hms_client_opts;

  if (protection) {
    ASSERT_OK(kdc.Start());

    string spn = "hive/127.0.0.1";
    string ktpath;
    ASSERT_OK(kdc.CreateServiceKeytab(spn, &ktpath));

    ASSERT_OK(rpc::SaslInit());
    hms.EnableKerberos(kdc.GetEnvVars()["KRB5_CONFIG"],
                       spn,
                       ktpath,
                       *protection);

    ASSERT_OK(kdc.CreateUserPrincipal("alice"));
    ASSERT_OK(kdc.Kinit("alice"));
    ASSERT_OK(kdc.SetKrb5Environment());
    hms_client_opts.enable_kerberos = true;
  }

  ASSERT_OK(hms.Start());

  HmsClient client(hms.address(), hms_client_opts);
  ASSERT_FALSE(client.IsConnected());
  ASSERT_OK(client.Start());
  ASSERT_TRUE(client.IsConnected());

  // Create a database.
  string database_name = "my_db";
  hive::Database db;
  db.name = database_name;
  ASSERT_OK(client.CreateDatabase(db));
  ASSERT_TRUE(client.CreateDatabase(db).IsAlreadyPresent());

  // Get all databases.
  vector<string> databases;
  ASSERT_OK(client.GetAllDatabases(&databases));
  std::sort(databases.begin(), databases.end());
  EXPECT_EQ(vector<string>({ "default", database_name }), databases) << "Databases: " << databases;

  // Get a specific database..
  hive::Database my_db;
  ASSERT_OK(client.GetDatabase(database_name, &my_db));
  EXPECT_EQ(database_name, my_db.name) << "my_db: " << my_db;

  string table_name = "my_table";
  string table_id = "table-id";

  // Check that the HMS rejects Kudu tables without a table ID.
  ASSERT_STR_CONTAINS(CreateTable(&client, database_name, table_name, "").ToString(),
                      "Kudu table entry must contain a table ID");

  // Create a table.
  ASSERT_OK(CreateTable(&client, database_name, table_name, table_id));
  ASSERT_TRUE(CreateTable(&client, database_name, table_name, table_id).IsAlreadyPresent());

  // Retrieve a table.
  hive::Table my_table;
  ASSERT_OK(client.GetTable(database_name, table_name, &my_table));
  EXPECT_EQ(database_name, my_table.dbName) << "my_table: " << my_table;
  EXPECT_EQ(table_name, my_table.tableName);
  EXPECT_EQ(table_id, my_table.parameters[HmsClient::kKuduTableIdKey]);
  EXPECT_EQ(HmsClient::kKuduStorageHandler, my_table.parameters[HmsClient::kStorageHandlerKey]);
  EXPECT_EQ(HmsClient::kManagedTable, my_table.tableType);

  string new_table_name = "my_altered_table";

  // Renaming the table with an incorrect table ID should fail.
  hive::Table altered_table(my_table);
  altered_table.tableName = new_table_name;
  altered_table.parameters[HmsClient::kKuduTableIdKey] = "bogus-table-id";
  ASSERT_TRUE(client.AlterTable(database_name, table_name, altered_table).IsRemoteError());

  // Rename the table.
  altered_table.parameters[HmsClient::kKuduTableIdKey] = table_id;
  ASSERT_OK(client.AlterTable(database_name, table_name, altered_table));

  // Original table is gone.
  ASSERT_TRUE(client.AlterTable(database_name, table_name, altered_table).IsIllegalState());

  // Check that the altered table's properties are intact.
  hive::Table renamed_table;
  ASSERT_OK(client.GetTable(database_name, new_table_name, &renamed_table));
  EXPECT_EQ(database_name, renamed_table.dbName);
  EXPECT_EQ(new_table_name, renamed_table.tableName);
  EXPECT_EQ(table_id, renamed_table.parameters[HmsClient::kKuduTableIdKey]);
  EXPECT_EQ(HmsClient::kKuduStorageHandler,
            renamed_table.parameters[HmsClient::kStorageHandlerKey]);
  EXPECT_EQ(HmsClient::kManagedTable, renamed_table.tableType);

  // Create a table with an uppercase name.
  string uppercase_table_name = "my_UPPERCASE_Table";
  ASSERT_OK(CreateTable(&client, database_name, uppercase_table_name, "uppercase-table-id"));

  // Create a table with an illegal utf-8 name.
  ASSERT_TRUE(CreateTable(&client, database_name, "☃ sculptures 😉", table_id).IsInvalidArgument());

  // Get all tables.
  vector<string> tables;
  ASSERT_OK(client.GetAllTables(database_name, &tables));
  std::sort(tables.begin(), tables.end());
  EXPECT_EQ(vector<string>({ new_table_name, "my_uppercase_table" }), tables)
      << "Tables: " << tables;

  // Check that the HMS rejects Kudu table drops with a bogus table ID.
  ASSERT_TRUE(DropTable(&client, database_name, new_table_name, "bogus-table-id").IsRemoteError());
  // Check that the HMS rejects non-existent table drops.
  ASSERT_TRUE(DropTable(&client, database_name, "foo-bar", "bogus-table-id").IsNotFound());

  // Drop a table.
  ASSERT_OK(DropTable(&client, database_name, new_table_name, table_id));

  // Drop the database.
  ASSERT_TRUE(client.DropDatabase(database_name).IsIllegalState());
  // TODO(HIVE-17008)
  // ASSERT_OK(client.DropDatabase(database_name, Cascade::kTrue));
  // TODO(HIVE-17008)
  // ASSERT_TRUE(client.DropDatabase(database_name).IsNotFound());

  int64_t event_id;
  ASSERT_OK(client.GetCurrentNotificationEventId(&event_id));

  // Retrieve the notification log and spot-check that the results look sensible.
  vector<hive::NotificationEvent> events;
  ASSERT_OK(client.GetNotificationEvents(-1, 100, &events));

  ASSERT_EQ(5, events.size());
  EXPECT_EQ("CREATE_DATABASE", events[0].eventType);
  EXPECT_EQ("CREATE_TABLE", events[1].eventType);
  EXPECT_EQ("ALTER_TABLE", events[2].eventType);
  EXPECT_EQ("CREATE_TABLE", events[3].eventType);
  EXPECT_EQ("DROP_TABLE", events[4].eventType);
  // TODO(HIVE-17008)
  //EXPECT_EQ("DROP_TABLE", events[5].eventType);
  //EXPECT_EQ("DROP_DATABASE", events[6].eventType);

  // Retrieve a specific notification log.
  events.clear();
  ASSERT_OK(client.GetNotificationEvents(2, 1, &events));
  ASSERT_EQ(1, events.size()) << "events: " << events;
  EXPECT_EQ("ALTER_TABLE", events[0].eventType);
  ASSERT_OK(client.Stop());
}

TEST_P(HmsClientTest, TestLargeObjects) {
  optional<SaslProtection::Type> protection = GetParam();

  MiniKdc kdc;
  MiniHms hms;
  HmsClientOptions hms_client_opts;

  if (protection) {
    ASSERT_OK(kdc.Start());

    string spn = "hive/127.0.0.1";
    string ktpath;
    ASSERT_OK(kdc.CreateServiceKeytab(spn, &ktpath));

    ASSERT_OK(rpc::SaslInit());
    hms.EnableKerberos(kdc.GetEnvVars()["KRB5_CONFIG"],
                       spn,
                       ktpath,
                       *protection);

    ASSERT_OK(kdc.CreateUserPrincipal("alice"));
    ASSERT_OK(kdc.Kinit("alice"));
    ASSERT_OK(kdc.SetKrb5Environment());
    hms_client_opts.enable_kerberos = true;
  }

  ASSERT_OK(hms.Start());

  HmsClient client(hms.address(), hms_client_opts);
  ASSERT_OK(client.Start());

  string database_name = "default";
  string table_name = "big_table";

  hive::Table table;
  table.dbName = database_name;
  table.tableName = table_name;
  table.tableType = HmsClient::kManagedTable;
  hive::FieldSchema partition_key;
  partition_key.name = "c1";
  partition_key.type = "int";
  table.partitionKeys.emplace_back(std::move(partition_key));

  ASSERT_OK(client.CreateTable(table));

  // Add a bunch of partitions to the table. This ensures we can send and
  // receive really large thrift objects. We have to add the partitions in small
  // batches, otherwise Derby chokes.
  const int kBatches = 40;
  const int kPartitionsPerBatch = 25;

  for (int batch_idx = 0; batch_idx < kBatches; batch_idx++) {
    vector<hive::Partition> partitions;
    for (int partition_idx = 0; partition_idx < kPartitionsPerBatch; partition_idx++) {
      hive::Partition partition;
      partition.dbName = database_name;
      partition.tableName = table_name;
      partition.values = { std::to_string(batch_idx * kPartitionsPerBatch + partition_idx) };
      partitions.emplace_back(std::move(partition));
    }

    ASSERT_OK(client.AddPartitions(database_name, table_name, std::move(partitions)));
  }

  ASSERT_OK(client.GetTable(database_name, table_name, &table));

  vector<hive::Partition> partitions;
  ASSERT_OK(client.GetPartitions(database_name, table_name, &partitions));
  ASSERT_EQ(kBatches * kPartitionsPerBatch, partitions.size());
}

TEST_F(HmsClientTest, TestHmsFaultHandling) {
  MiniHms hms;
  ASSERT_OK(hms.Start());

  HmsClientOptions options;
  options.recv_timeout = MonoDelta::FromMilliseconds(500),
  options.send_timeout = MonoDelta::FromMilliseconds(500);
  HmsClient client(hms.address(), options);
  ASSERT_OK(client.Start());

  // Get a specific database.
  hive::Database my_db;
  ASSERT_OK(client.GetDatabase("default", &my_db));

  // Shutdown the HMS.
  ASSERT_OK(hms.Stop());
  ASSERT_TRUE(client.GetDatabase("default", &my_db).IsNetworkError());
  ASSERT_OK(client.Stop());

  // Restart the HMS and ensure the client can connect.
  ASSERT_OK(hms.Start());
  ASSERT_OK(client.Start());
  ASSERT_OK(client.GetDatabase("default", &my_db));

  // Pause the HMS and ensure the client times-out appropriately.
  ASSERT_OK(hms.Pause());
  ASSERT_TRUE(client.GetDatabase("default", &my_db).IsTimedOut());

  // Unpause the HMS and ensure the client can continue.
  ASSERT_OK(hms.Resume());
  ASSERT_OK(client.GetDatabase("default", &my_db));
}

// Test connecting the HMS client to TCP sockets in various invalid states.
TEST_F(HmsClientTest, TestHmsConnect) {
  Sockaddr loopback;
  ASSERT_OK(loopback.ParseString("127.0.0.1", 0));

  HmsClientOptions options;
  options.recv_timeout = MonoDelta::FromMilliseconds(100),
  options.send_timeout = MonoDelta::FromMilliseconds(100);
  options.conn_timeout = MonoDelta::FromMilliseconds(100);

  auto start_client = [&options] (Sockaddr addr) -> Status {
    HmsClient client(HostPort(addr), options);
    return client.Start();
  };

  // Listening, but not accepting socket.
  Sockaddr listening;
  Socket listening_socket;
  ASSERT_OK(listening_socket.Init(0));
  ASSERT_OK(listening_socket.BindAndListen(loopback, 1));
  listening_socket.GetSocketAddress(&listening);
  ASSERT_TRUE(start_client(listening).IsTimedOut());

  // Bound, but not listening socket.
  Sockaddr bound;
  Socket bound_socket;
  ASSERT_OK(bound_socket.Init(0));
  ASSERT_OK(bound_socket.Bind(loopback));
  bound_socket.GetSocketAddress(&bound);
  ASSERT_TRUE(start_client(bound).IsNetworkError());

  // Unbound socket.
  Sockaddr unbound;
  Socket unbound_socket;
  ASSERT_OK(unbound_socket.Init(0));
  ASSERT_OK(unbound_socket.Bind(loopback));
  unbound_socket.GetSocketAddress(&unbound);
  ASSERT_OK(unbound_socket.Close());
  ASSERT_TRUE(start_client(unbound).IsNetworkError());
}

TEST_F(HmsClientTest, TestDeserializeJsonTable) {
  string json = R"#({"1":{"str":"table_name"},"2":{"str":"database_name"}})#";
  hive::Table table;
  ASSERT_OK(HmsClient::DeserializeJsonTable(json, &table));
  ASSERT_EQ("table_name", table.tableName);
  ASSERT_EQ("database_name", table.dbName);
}

} // namespace hms
} // namespace kudu
