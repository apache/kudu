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
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/ThriftHiveMetastore.h"
#include "kudu/hms/hive_metastore_constants.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

using apache::thrift::TException;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TJSONProtocol;
using apache::thrift::transport::TBufferedTransport;
using apache::thrift::transport::TMemoryBuffer;
using apache::thrift::transport::TSocket;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::vector;

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
    return Status::RuntimeError((msg), e.what()); \
  } catch (const TException& e) { \
    return Status::IOError((msg), e.what()); \
  }

const char* const HmsClient::kKuduTableIdKey = "kudu.table_id";
const char* const HmsClient::kKuduMasterAddrsKey = "kudu.master_addresses";
const char* const HmsClient::kKuduStorageHandler = "org.apache.kudu.hive.KuduStorageHandler";

const char* const HmsClient::kTransactionalEventListeners =
  "hive.metastore.transactional.event.listeners";
const char* const HmsClient::kDbNotificationListener =
  "org.apache.hive.hcatalog.listener.DbNotificationListener";
const char* const HmsClient::kKuduMetastorePlugin =
  "org.apache.kudu.hive.metastore.KuduMetastorePlugin";

const int kSlowExecutionWarningThresholdMs = 500;

HmsClient::HmsClient(const HostPort& hms_address)
    : client_(nullptr) {
  auto socket = make_shared<TSocket>(hms_address.host(), hms_address.port());
  auto transport = make_shared<TBufferedTransport>(std::move(socket));
  auto protocol = make_shared<TBinaryProtocol>(std::move(transport));
  client_ = hive::ThriftHiveMetastoreClient(std::move(protocol));
}

HmsClient::~HmsClient() {
  WARN_NOT_OK(Stop(), "failed to shutdown HMS client");
}

Status HmsClient::Start() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 1000 /* ms */, "starting HMS client");
  HMS_RET_NOT_OK(client_.getOutputProtocol()->getTransport()->open(),
                 "failed to open Hive MetaStore connection");

  // Immediately after connecting to the HMS, check that it is configured with
  // the required event listeners.
  string event_listener_config;
  HMS_RET_NOT_OK(client_.get_config_value(event_listener_config, kTransactionalEventListeners, ""),
                 "failed to get Hive MetaStore transactional event listener configuration");

  // Parse the set of listeners from the configuration string.
  vector<string> listeners = strings::Split(event_listener_config, ",", strings::SkipWhitespace());
  for (auto& listener : listeners) {
    StripWhiteSpace(&listener);
  }

  for (const auto& required_listener : { kDbNotificationListener, kKuduMetastorePlugin }) {
    if (std::find(listeners.begin(), listeners.end(), required_listener) == listeners.end()) {
      return Status::IllegalState(
          strings::Substitute("Hive Metastore configuration is missing required "
                              "transactional event listener ($0): $1",
                              kTransactionalEventListeners, required_listener));
    }
  }

  return Status::OK();
}

Status HmsClient::Stop() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "stopping HMS client");
  HMS_RET_NOT_OK(client_.getInputProtocol()->getTransport()->close(),
                 "failed to close Hive MetaStore connection");
  return Status::OK();
}

Status HmsClient::CreateDatabase(const hive::Database& database) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "create HMS database");
  HMS_RET_NOT_OK(client_.create_database(database), "failed to create Hive MetaStore database");
  return Status::OK();
}

Status HmsClient::DropDatabase(const string& database_name, Cascade cascade) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "drop HMS database");
  HMS_RET_NOT_OK(client_.drop_database(database_name, true, cascade == Cascade::kTrue),
                 "failed to drop Hive MetaStore database");
  return Status::OK();
}

Status HmsClient::GetAllDatabases(vector<string>* databases) {
  DCHECK(databases);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get all HMS databases");
  HMS_RET_NOT_OK(client_.get_all_databases(*databases),
                 "failed to get Hive MetaStore databases");
  return Status::OK();
}

Status HmsClient::GetDatabase(const string& pattern, hive::Database* database) {
  DCHECK(database);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get HMS database");
  HMS_RET_NOT_OK(client_.get_database(*database, pattern),
                 "failed to get Hive MetaStore database");
  return Status::OK();
}

Status HmsClient::CreateTable(const hive::Table& table) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "create HMS table");
  HMS_RET_NOT_OK(client_.create_table(table), "failed to create Hive MetaStore table");
  return Status::OK();
}

Status HmsClient::AlterTable(const std::string& database_name,
                             const std::string& table_name,
                             const hive::Table& table) {
  HMS_RET_NOT_OK(client_.alter_table(database_name, table_name, table),
                 "failed to alter Hive MetaStore table");
  return Status::OK();
}

Status HmsClient::DropTableWithContext(const string& database_name,
                                       const string& table_name,
                                       const hive::EnvironmentContext& env_ctx) {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "drop HMS table");
  HMS_RET_NOT_OK(client_.drop_table_with_environment_context(database_name, table_name,
                                                             true, env_ctx),
                 "failed to drop Hive MetaStore table");
  return Status::OK();
}

Status HmsClient::GetAllTables(const string& database_name,
                               vector<string>* tables) {
  DCHECK(tables);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get all HMS tables");
  HMS_RET_NOT_OK(client_.get_all_tables(*tables, database_name),
                 "failed to get Hive MetaStore tables");
  return Status::OK();
}

Status HmsClient::GetTable(const string& database_name,
                           const string& table_name,
                           hive::Table* table) {
  DCHECK(table);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs, "get HMS table");
  HMS_RET_NOT_OK(client_.get_table(*table, database_name, table_name),
                 "failed to get Hive MetaStore table");
  return Status::OK();
}

Status HmsClient::GetCurrentNotificationEventId(int64_t* event_id) {
  DCHECK(event_id);
  SCOPED_LOG_SLOW_EXECUTION(WARNING, kSlowExecutionWarningThresholdMs,
                            "get HMS current notification event ID");
  hive::CurrentNotificationEventId response;
  HMS_RET_NOT_OK(client_.get_current_notificationEventId(response),
                 "failed to get Hive MetaStore current event ID");
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
                 "failed to get Hive MetaStore next notification");
  events->swap(response.events);
  return Status::OK();
}

Status HmsClient::DeserializeJsonTable(Slice json, hive::Table* table)  {
  shared_ptr<TMemoryBuffer> membuffer(new TMemoryBuffer(json.size()));
  membuffer->write(json.data(), json.size());
  TJSONProtocol protocol(membuffer);
  HMS_RET_NOT_OK(table->read(&protocol), "failed to deserialize JSON table");
  return Status::OK();
}

} // namespace hms
} // namespace kudu
