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
#include <exception>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <glog/logging.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TTransport.h>
#include <thrift/transport/TTransportException.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/ThriftHiveMetastore.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/thrift/client.h"
#include "kudu/thrift/sasl_client_transport.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

using apache::thrift::TException;
using apache::thrift::protocol::TJSONProtocol;
using apache::thrift::transport::TMemoryBuffer;
using apache::thrift::transport::TTransportException;
using kudu::thrift::ClientOptions;
using kudu::thrift::CreateClientProtocol;
using kudu::thrift::SaslException;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace hms {

// The entire set of Hive-specific exceptions is defined in
// hive_metastore.thrift. We do not try to handle all of them - TException acts
// as a catch all, as well as default for network errors.
#define HMS_RET_NOT_OK(call, msg) \
  try { \
    (call); \
  } catch (const hive::AlreadyExistsException& e) { \
    return Status::AlreadyPresent((msg), e.what()); \
  } catch (const hive::UnknownDBException& e) { \
    return Status::NotFound((msg), e.what()); \
  } catch (const hive::UnknownTableException& e) { \
    return Status::NotFound((msg), e.what()); \
  } catch (const hive::NoSuchObjectException& e) { \
    return Status::NotFound((msg), e.what()); \
  } catch (const hive::InvalidObjectException& e) { \
    return Status::InvalidArgument((msg), e.what()); \
  } catch (const hive::InvalidOperationException& e) { \
    return Status::IllegalState((msg), e.what()); \
  } catch (const hive::MetaException& e) { \
    return Status::RemoteError((msg), e.what()); \
  } catch (const SaslException& e) { \
    return e.status().CloneAndPrepend((msg)); \
  } catch (const TTransportException& e) { \
    switch (e.getType()) { \
      case TTransportException::TIMED_OUT: return Status::TimedOut((msg), e.what()); \
      case TTransportException::BAD_ARGS: return Status::InvalidArgument((msg), e.what()); \
      case TTransportException::CORRUPTED_DATA: return Status::Corruption((msg), e.what()); \
      default: return Status::NetworkError((msg), e.what()); \
    } \
  } catch (const TException& e) { \
    return Status::IOError((msg), e.what()); \
  } catch (const std::exception& e) { \
    return Status::RuntimeError((msg), e.what()); \
  }

const char* const HmsClient::kLegacyKuduStorageHandler =
  "com.cloudera.kudu.hive.KuduStorageHandler";
const char* const HmsClient::kLegacyTablePrefix = "impala::";
const char* const HmsClient::kKuduTableIdKey = "kudu.table_id";
const char* const HmsClient::kKuduTableNameKey = "kudu.table_name";
const char* const HmsClient::kKuduMasterAddrsKey = "kudu.master_addresses";
const char* const HmsClient::kKuduMasterEventKey = "kudu.master_event";
const char* const HmsClient::kKuduCheckIdKey = "kudu.check_id";
const char* const HmsClient::kKuduStorageHandler =
    "org.apache.hadoop.hive.kudu.KuduStorageHandler";
const char* const HmsClient::kOldKuduStorageHandler = "org.apache.kudu.hive.KuduStorageHandler";

const char* const HmsClient::kTransactionalEventListeners =
  "hive.metastore.transactional.event.listeners";
const char* const HmsClient::kDisallowIncompatibleColTypeChanges =
  "hive.metastore.disallow.incompatible.col.type.changes";
const char* const HmsClient::kDbNotificationListener =
  "org.apache.hive.hcatalog.listener.DbNotificationListener";
const char* const HmsClient::kExternalTableKey = "EXTERNAL";
const char* const HmsClient::kStorageHandlerKey = "storage_handler";
const char* const HmsClient::kKuduMetastorePlugin =
  "org.apache.kudu.hive.metastore.KuduMetastorePlugin";
const char* const HmsClient::kHiveFilterFieldParams = "hive_filter_field_params__";
const char* const HmsClient::kNotificationAddThriftObjects =
  "hive.metastore.notifications.add.thrift.objects";

const char* const HmsClient::kManagedTable = "MANAGED_TABLE";
const char* const HmsClient::kExternalTable = "EXTERNAL_TABLE";

const uint16_t HmsClient::kDefaultHmsPort = 9083;

const int kSlowExecutionWarningThresholdMs = 1000;

const char* const HmsClient::kServiceName = "Hive Metastore";

HmsClient::HmsClient(const HostPort& address, const ClientOptions& options)
      : verify_kudu_sync_config_(options.verify_service_config),
        client_(hive::ThriftHiveMetastoreClient(CreateClientProtocol(address, options))) {
}

HmsClient::~HmsClient() {
  WARN_NOT_OK(Stop(), "failed to shutdown HMS client");
}

Status HmsClient::Start() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "starting HMS client");
  HMS_RET_NOT_OK(client_.getOutputProtocol()->getTransport()->open(),
                 "failed to open Hive Metastore connection");

  if (!verify_kudu_sync_config_) {
    return Status::OK();
  }

  // Immediately after connecting to the HMS, check that it is configured with
  // the required event listeners.
  string event_listener_config;
  HMS_RET_NOT_OK(client_.get_config_value(event_listener_config, kTransactionalEventListeners, ""),
                 Substitute("failed to get Hive Metastore $0 configuration",
                            kTransactionalEventListeners));

  // Parse the set of listeners from the configuration string.
  vector<string> listeners = strings::Split(event_listener_config, ",", strings::SkipWhitespace());
  for (auto& listener : listeners) {
    StripWhiteSpace(&listener);
  }

  for (const auto& required_listener : { kDbNotificationListener, kKuduMetastorePlugin }) {
    if (std::find(listeners.begin(), listeners.end(), required_listener) == listeners.end()) {
      return Status::IllegalState(
          Substitute("Hive Metastore configuration is missing required "
                     "transactional event listener ($0): $1",
                     kTransactionalEventListeners, required_listener));
    }
  }

  // Also check that the HMS is configured to allow changing the type of
  // columns, which is required to support dropping columns. File-based Hive
  // tables handle columns by offset, and removing a column simply shifts all
  // remaining columns down by one. The actual data is not changed, but instead
  // reassigned to the next column. If the HMS is configured to check column
  // type changes it will validate that the existing data matches the shifted
  // column layout. This is overly strict for Kudu, since we handle DDL
  // operations properly.
  //
  // See org.apache.hadoop.hive.metastore.MetaStoreUtils.throwExceptionIfIncompatibleColTypeChange.
  string disallow_incompatible_column_type_changes;
  HMS_RET_NOT_OK(client_.get_config_value(disallow_incompatible_column_type_changes,
                                          kDisallowIncompatibleColTypeChanges,
                                          "false"),
                 Substitute("failed to get Hive Metastore $0 configuration",
                            kDisallowIncompatibleColTypeChanges));

  if (boost::iequals(disallow_incompatible_column_type_changes, "true")) {
    return Status::IllegalState(Substitute(
        "Hive Metastore configuration is invalid: $0 must be set to false",
        kDisallowIncompatibleColTypeChanges));
  }

  // Check that the HMS is configured to add the entire thrift Table/Partition
  // objects to the HMS notifications, which is required to properly parse the
  // HMS notification log. This is specific to the HMS version shipped in
  // Cloudera's CDH.
  string thrift_objects_config;
  HMS_RET_NOT_OK(client_.get_config_value(thrift_objects_config,
                                          kNotificationAddThriftObjects,
                                          "true"),
                 Substitute("failed to get Hive Metastore $0 configuration",
                            kNotificationAddThriftObjects));
  if (boost::iequals(thrift_objects_config, "false")) {
    return Status::IllegalState(Substitute(
        "Hive Metastore configuration is invalid: $0 must be set to true",
        kNotificationAddThriftObjects));
    }

  return Status::OK();
}

Status HmsClient::Stop() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "stopping HMS client");
  HMS_RET_NOT_OK(client_.getInputProtocol()->getTransport()->close(),
                 "failed to close Hive Metastore connection");
  return Status::OK();
}

bool HmsClient::IsConnected() {
  return client_.getInputProtocol()->getTransport()->isOpen();
}

Status HmsClient::CreateDatabase(const hive::Database& database) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "create HMS database");
  HMS_RET_NOT_OK(client_.create_database(database), "failed to create Hive Metastore database");
  return Status::OK();
}

Status HmsClient::DropDatabase(const string& database_name, Cascade cascade) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "drop HMS database");
  HMS_RET_NOT_OK(client_.drop_database(database_name, true, cascade == Cascade::kTrue),
                 "failed to drop Hive Metastore database");
  return Status::OK();
}

Status HmsClient::GetAllDatabases(vector<string>* databases) {
  DCHECK(databases);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get all HMS databases");
  HMS_RET_NOT_OK(client_.get_all_databases(*databases),
                 "failed to get Hive Metastore databases");
  return Status::OK();
}

Status HmsClient::GetDatabase(const string& pattern, hive::Database* database) {
  DCHECK(database);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get HMS database");
  HMS_RET_NOT_OK(client_.get_database(*database, pattern),
                 "failed to get Hive Metastore database");
  return Status::OK();
}

Status HmsClient::CreateTable(const hive::Table& table, const hive::EnvironmentContext& env_ctx) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "create HMS table");
  HMS_RET_NOT_OK(client_.create_table_with_environment_context(table, env_ctx),
                 "failed to create Hive MetaStore table");
  return Status::OK();
}

Status HmsClient::AlterTable(const std::string& database_name,
                             const std::string& table_name,
                             const hive::Table& table,
                             const hive::EnvironmentContext& env_ctx) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "alter HMS table");
  HMS_RET_NOT_OK(client_.alter_table_with_environment_context(database_name, table_name,
                                                              table, env_ctx),
                 "failed to alter Hive MetaStore table");
  return Status::OK();
}

Status HmsClient::DropTable(const string& database_name,
                            const string& table_name,
                            const hive::EnvironmentContext& env_ctx) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "drop HMS table");
  HMS_RET_NOT_OK(client_.drop_table_with_environment_context(database_name, table_name,
                                                             true, env_ctx),
                 "failed to drop Hive Metastore table");
  return Status::OK();
}

Status HmsClient::GetTableNames(const string& database_name,
                                vector<string>* table_names) {
  DCHECK(table_names);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get all HMS tables");
  HMS_RET_NOT_OK(client_.get_all_tables(*table_names, database_name),
                 "failed to get Hive Metastore tables");
  return Status::OK();
}

Status HmsClient::GetTableNames(const std::string& database_name,
                                const std::string& filter,
                                std::vector<std::string>* table_names) {
  DCHECK(table_names);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get filtered HMS tables");
  HMS_RET_NOT_OK(client_.get_table_names_by_filter(*table_names, database_name,
                                                   filter, /*max_tables*/ -1),
                 "failed to get filtered Hive Metastore tables");
  return Status::OK();
}

Status HmsClient::GetTable(const string& database_name,
                           const string& table_name,
                           hive::Table* table) {
  DCHECK(table);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get HMS table");
  HMS_RET_NOT_OK(client_.get_table(*table, database_name, table_name),
                 "failed to get Hive Metastore table");
  return Status::OK();
}

Status HmsClient::GetTables(const string& database_name,
                            const vector<string>& table_names,
                            vector<hive::Table>* tables) {
  DCHECK(tables);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get HMS tables");
  HMS_RET_NOT_OK(client_.get_table_objects_by_name(*tables, database_name, table_names),
                 "failed to get Hive Metastore tables");
  return Status::OK();
}

Status HmsClient::GetCurrentNotificationEventId(int64_t* event_id) {
  DCHECK(event_id);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs,
                            "get HMS current notification event ID");
  hive::CurrentNotificationEventId response;
  HMS_RET_NOT_OK(client_.get_current_notificationEventId(response),
                 "failed to get Hive Metastore current event ID");
  *event_id = response.eventId;
  return Status::OK();
}

Status HmsClient::GetNotificationEvents(int64_t last_event_id,
                                        int32_t max_events,
                                        vector<hive::NotificationEvent>* events) {
  DCHECK(events);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs,
                            "get HMS notification events");
  hive::NotificationEventRequest request;
  request.lastEvent = last_event_id;
  request.__set_maxEvents(max_events);
  hive::NotificationEventResponse response;
  HMS_RET_NOT_OK(client_.get_next_notification(response, request),
                 "failed to get Hive Metastore next notification");
  events->swap(response.events);
  return Status::OK();
}

Status HmsClient::AddPartitions(const string& database_name,
                                const string& table_name,
                                vector<hive::Partition> partitions) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "add HMS table partitions");
  hive::AddPartitionsRequest request;
  hive::AddPartitionsResult response;

  request.dbName = database_name;
  request.tblName = table_name;
  request.parts = std::move(partitions);

  HMS_RET_NOT_OK(client_.add_partitions_req(response, request),
                 "failed to add Hive Metastore table partitions");
  return Status::OK();
}

Status HmsClient::GetPartitions(const string& database_name,
                                const string& table_name,
                                vector<hive::Partition>* partitions) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get HMS table partitions");
  HMS_RET_NOT_OK(client_.get_partitions(*partitions, database_name, table_name, -1),
                 "failed to get Hive Metastore table partitions");
  return Status::OK();
}


Status HmsClient::DeserializeJsonTable(Slice json, hive::Table* table)  {
  shared_ptr<TMemoryBuffer> membuffer(new TMemoryBuffer(json.size()));
  membuffer->write(json.data(), json.size());
  TJSONProtocol protocol(membuffer);
  HMS_RET_NOT_OK(table->read(&protocol), "failed to deserialize JSON table");
  return Status::OK();
}

bool HmsClient::IsKuduTable(const hive::Table& table) {
  const string* storage_handler =
      FindOrNull(table.parameters, hms::HmsClient::kStorageHandlerKey);
  if (!storage_handler) {
    return false;
  }

  // TODO(ghenke): Remove special kOldKuduStorageHandler handling after Impala integration
  //  of the adjusted kKuduStorageHandler.
  return *storage_handler == hms::HmsClient::kKuduStorageHandler ||
         *storage_handler == hms::HmsClient::kOldKuduStorageHandler;
}

} // namespace hms
} // namespace kudu
