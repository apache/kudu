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

#include <cstdint>
#include <string>
#include <vector>

#include "kudu/gutil/port.h"
#include "kudu/hms/ThriftHiveMetastore.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace hive {
class Database;
class EnvironmentContext;
class NotificationEvent;
class Partition;
class Table;
}

namespace kudu {

class HostPort;

namespace hms {

// Whether to drop child-objects when dropping an HMS object.
enum class Cascade {
  kTrue,
  kFalse,
};

struct HmsClientOptions {

  // Thrift socket send timeout
  MonoDelta send_timeout = MonoDelta::FromSeconds(60);

  // Thrift socket receive timeout.
  MonoDelta recv_timeout = MonoDelta::FromSeconds(60);

  // Thrift socket connect timeout.
  MonoDelta conn_timeout = MonoDelta::FromSeconds(60);

  // Whether to use SASL Kerberos authentication when connecting to the HMS.
  bool enable_kerberos = false;
};

// A client for the Hive Metastore.
//
// All operations are synchronous, and may block.
//
// HmsClient is not thread safe.
//
// HmsClient wraps a single TCP connection to an HMS, and does not attempt to
// handle or retry on failure. It's expected that a higher-level component will
// wrap HmsClient to provide retry, pooling, and HA deployment features if
// necessary.
//
// Note: Thrift provides a handy TSocketPool class which could be useful in
// allowing the HmsClient to transparently handle connecting to a pool of HA HMS
// instances. However, because TSocketPool handles choosing the instance during
// socket connect, it can't determine if the remote endpoint is actually an HMS,
// or just a random listening TCP socket. Nor can it do application-level checks
// like ensuring that the connected HMS is configured with the Kudu Metastore
// plugin. So, it's better for a higher-level construct to handle connecting to
// HA deployments by wrapping multiple HmsClient instances. HmsClient punts on
// handling connection retries, because the higher-level construct which is
// handling HA deployments will naturally want to retry across HMS instances as
// opposed to retrying repeatedly on a single instance.
class HmsClient {
 public:

  static const char* const kExternalTableKey;
  static const char* const kLegacyKuduStorageHandler;
  static const char* const kLegacyKuduTableNameKey;
  static const char* const kLegacyTablePrefix;
  static const char* const kKuduTableIdKey;
  static const char* const kKuduMasterAddrsKey;
  static const char* const kKuduMasterEventKey;;
  static const char* const kStorageHandlerKey;
  static const char* const kKuduStorageHandler;
  static const char* const kHiveFilterFieldParams;

  static const char* const kTransactionalEventListeners;
  static const char* const kDisallowIncompatibleColTypeChanges;
  static const char* const kDbNotificationListener;
  static const char* const kKuduMetastorePlugin;

  // See org.apache.hadoop.hive.metastore.TableType.
  static const char* const kManagedTable;
  static const char* const kExternalTable;

  static const uint16_t kDefaultHmsPort;

  // Create an HmsClient connection to the proided HMS Thrift RPC address.
  //
  // The individual timeouts may be set to enforce per-operation
  // (read/write/connect) timeouts.
  HmsClient(const HostPort& hms_address, const HmsClientOptions& options);
  ~HmsClient();

  // Starts the HMS client.
  //
  // This method will open a synchronous TCP connection to the HMS. If the HMS
  // can not be reached within the connection timeout interval, an error is
  // returned.
  //
  // Must be called before any subsequent operations using the client.
  Status Start() WARN_UNUSED_RESULT;

  // Stops the HMS client.
  //
  // This is optional; if not called the destructor will stop the client.
  Status Stop() WARN_UNUSED_RESULT;

  // Returns 'true' if the client is connected to the remote server.
  bool IsConnected() WARN_UNUSED_RESULT;

  // Creates a new database in the HMS.
  //
  // If a database already exists by the same name an AlreadyPresent status is
  // returned.
  Status CreateDatabase(const hive::Database& database) WARN_UNUSED_RESULT;

  // Drops a database in the HMS.
  //
  // If 'cascade' is Cascade::kTrue, tables in the database will automatically
  // be dropped (this is the default in HiveQL). Otherwise, the operation will
  // return IllegalState if the database contains tables.
  Status DropDatabase(const std::string& database_name,
                      Cascade cascade = Cascade::kFalse) WARN_UNUSED_RESULT;

  // Returns all HMS databases.
  Status GetAllDatabases(std::vector<std::string>* databases) WARN_UNUSED_RESULT;

  // Retrieves a database from the HMS.
  Status GetDatabase(const std::string& pattern, hive::Database* database) WARN_UNUSED_RESULT;

  // Creates a table in the HMS.
  Status CreateTable(const hive::Table& table,
                     const hive::EnvironmentContext& env_ctx = hive::EnvironmentContext())
    WARN_UNUSED_RESULT;

  // Alter a table in the HMS.
  Status AlterTable(const std::string& database_name,
                    const std::string& table_name,
                    const hive::Table& table,
                    const hive::EnvironmentContext& env_ctx = hive::EnvironmentContext())
    WARN_UNUSED_RESULT;

  // Drops a Kudu table in the HMS.
  Status DropTable(const std::string& database_name,
                   const std::string& table_name,
                   const hive::EnvironmentContext& env_ctx = hive::EnvironmentContext())
    WARN_UNUSED_RESULT;

  // Retrieves an HMS table metadata.
  Status GetTable(const std::string& database_name,
                  const std::string& table_name,
                  hive::Table* table) WARN_UNUSED_RESULT;

  // Retrieves HMS table metadata for all tables in 'table_names'.
  Status GetTables(const std::string& database_name,
                   const std::vector<std::string>& table_names,
                   std::vector<hive::Table>* tables) WARN_UNUSED_RESULT;

  // Retrieves all table names in an HMS database.
  Status GetTableNames(const std::string& database_name,
                       std::vector<std::string>* table_names) WARN_UNUSED_RESULT;

  // Retrieves all table names in an HMS database matching a filter. See the
  // docs for 'get_table_names_by_filter' in hive_metastore.thrift for filter
  // syntax examples.
  Status GetTableNames(const std::string& database_name,
                       const std::string& filter,
                       std::vector<std::string>* table_names) WARN_UNUSED_RESULT;

  // Retrieves a the current HMS notification event ID.
  Status GetCurrentNotificationEventId(int64_t* event_id) WARN_UNUSED_RESULT;

  // Retrieves HMS notification log events, beginning after 'last_event_id'.
  Status GetNotificationEvents(int64_t last_event_id,
                               int32_t max_events,
                               std::vector<hive::NotificationEvent>* events) WARN_UNUSED_RESULT;

  // Adds partitions to an HMS table.
  Status AddPartitions(const std::string& database_name,
                       const std::string& table_name,
                       std::vector<hive::Partition> partitions) WARN_UNUSED_RESULT;

  // Retrieves the partitions of an HMS table.
  Status GetPartitions(const std::string& database_name,
                       const std::string& table_name,
                       std::vector<hive::Partition>* partitions) WARN_UNUSED_RESULT;

  // Deserializes a JSON encoded table.
  //
  // Notification event log messages often include table objects serialized as
  // JSON.
  //
  // See org.apache.hadoop.hive.metastore.messaging.json.JSONMessageFactory for
  // the Java equivalent.
  static Status DeserializeJsonTable(Slice json, hive::Table* table) WARN_UNUSED_RESULT;

 private:
  hive::ThriftHiveMetastoreClient client_;
};

} // namespace hms
} // namespace kudu
