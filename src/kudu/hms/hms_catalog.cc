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

#include "kudu/hms/hms_catalog.h"

#include <algorithm>
#include <functional>
#include <iostream>
#include <map>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_client.h"
#include "kudu/util/async_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/threadpool.h"

using std::move;
using std::string;
using std::vector;
using strings::Substitute;

DEFINE_string(hive_metastore_uris, "",
              "Address of the Hive Metastore instance(s). The provided port must be for the HMS "
              "Thrift service. If a port isn't provided, defaults to 9083. If the HMS is deployed "
              "in an HA configuration, multiple comma-separated addresses can be supplied. If not "
              "set, the Kudu master will not send Kudu table catalog updates to Hive. The "
              "configured value must match the Hive hive.metastore.uris configuration.");
DEFINE_validator(hive_metastore_uris, &kudu::hms::HmsCatalog::ValidateUris);
TAG_FLAG(hive_metastore_uris, experimental);

// Note: the hive_metastore_sasl_enabled and keytab_file combination is validated in master.cc.
DEFINE_bool(hive_metastore_sasl_enabled, false,
            "Configures whether Thrift connections to the Hive Metastore use SASL "
            "(Kerberos) security. Must match the value of the "
            "hive.metastore.sasl.enabled option in the Hive Metastore configuration. "
            "When enabled, the --keytab_file flag must be provided.");
TAG_FLAG(hive_metastore_sasl_enabled, experimental);

DEFINE_int32(hive_metastore_retry_count, 1,
             "The number of times that HMS operations will retry after "
             "encountering retriable failures, such as network errors.");
TAG_FLAG(hive_metastore_retry_count, advanced);
TAG_FLAG(hive_metastore_retry_count, experimental);
TAG_FLAG(hive_metastore_retry_count, runtime);

DEFINE_int32(hive_metastore_send_timeout, 60,
             "Configures the socket send timeout, in seconds, for Thrift "
             "connections to the Hive Metastore.");
TAG_FLAG(hive_metastore_send_timeout, advanced);
TAG_FLAG(hive_metastore_send_timeout, experimental);
TAG_FLAG(hive_metastore_send_timeout, runtime);

DEFINE_int32(hive_metastore_recv_timeout, 60,
             "Configures the socket receive timeout, in seconds, for Thrift "
             "connections to the Hive Metastore.");
TAG_FLAG(hive_metastore_recv_timeout, advanced);
TAG_FLAG(hive_metastore_recv_timeout, experimental);
TAG_FLAG(hive_metastore_recv_timeout, runtime);

DEFINE_int32(hive_metastore_conn_timeout, 60,
             "Configures the socket connect timeout, in seconds, for Thrift "
             "connections to the Hive Metastore.");
TAG_FLAG(hive_metastore_conn_timeout, advanced);
TAG_FLAG(hive_metastore_conn_timeout, experimental);
TAG_FLAG(hive_metastore_conn_timeout, runtime);

namespace kudu {
namespace hms {

const char* const HmsCatalog::kInvalidTableError = "when the Hive Metastore integration "
    "is enabled, Kudu table names must be a period "
    "('.') separated database and table name pair";

HmsCatalog::HmsCatalog(string master_addresses)
    : master_addresses_(std::move(master_addresses)),
      hms_client_(HostPort("", 0), hms_client_options_),
      reconnect_after_(MonoTime::Now()),
      reconnect_failure_(Status::OK()),
      consecutive_reconnect_failures_(0),
      reconnect_idx_(0) {
}

HmsCatalog::~HmsCatalog() {
  Stop();
}

Status HmsCatalog::Start() {
  if (threadpool_) {
    return Status::IllegalState("HMS Catalog is already started");
  }

  RETURN_NOT_OK(ParseUris(FLAGS_hive_metastore_uris, &hms_addresses_));

  // The thread pool must be capped at one thread to ensure serialized access to
  // the fields of HmsCatalog.
  RETURN_NOT_OK(ThreadPoolBuilder("hms-catalog")
      .set_min_threads(1)
      .set_max_threads(1)
      .Build(&threadpool_));

  return Status::OK();
}

void HmsCatalog::Stop() {
  if (threadpool_) {
    threadpool_->Shutdown();
  }
}

Status HmsCatalog::CreateTable(const string& id,
                               const string& name,
                               const Schema& schema) {
  return Execute([&] (HmsClient* client) {
      hive::Table table;
      RETURN_NOT_OK(PopulateTable(id, name, schema, master_addresses_, &table));
      return client->CreateTable(table, EnvironmentContext());
  });
}

Status HmsCatalog::DropTable(const string& id, const string& name) {
  string hms_database;
  string hms_table;
  RETURN_NOT_OK(ParseTableName(name, &hms_database, &hms_table));

  hive::EnvironmentContext env_ctx = EnvironmentContext();
  env_ctx.properties.insert(make_pair(HmsClient::kKuduTableIdKey, id));

  return Execute([&] (HmsClient* client) {
    return client->DropTable(hms_database, hms_table, env_ctx);
  });
}

Status HmsCatalog::UpgradeLegacyImpalaTable(const string& id,
                                            const std::string& db_name,
                                            const std::string& tb_name,
                                            const Schema& schema) {
  return Execute([&] (HmsClient* client) {
    hive::Table table;
    RETURN_NOT_OK(client->GetTable(db_name, tb_name, &table));
    if (table.parameters[HmsClient::kStorageHandlerKey] !=
        HmsClient::kLegacyKuduStorageHandler) {
      return Status::IllegalState("non-legacy table cannot be upgraded");
    }

    RETURN_NOT_OK(PopulateTable(id, Substitute("$0.$1", db_name, tb_name),
                                schema, master_addresses_, &table));
    return client->AlterTable(db_name, tb_name, table, EnvironmentContext());
  });
}

Status HmsCatalog::RetrieveTables(vector<hive::Table>* hms_tables) {
  return Execute([&] (HmsClient* client) {
    vector<string> database_names;
    RETURN_NOT_OK(client->GetAllDatabases(&database_names));
    for (const auto &database_name : database_names) {
      vector<string> table_names;
      RETURN_NOT_OK(client->GetAllTables(database_name, &table_names));
      for (const auto &table_name : table_names) {
        hive::Table hms_table;
        RETURN_NOT_OK(client->GetTable(database_name, table_name, &hms_table));
        hms_tables->emplace_back(move(hms_table));
      }
    }

    return Status::OK();
  });
}

Status HmsCatalog::AlterTable(const string& id,
                              const string& name,
                              const string& new_name,
                              const Schema& schema) {

  return Execute([&] (HmsClient* client) {
      // The HMS does not have a way to alter individual fields of a table
      // entry, so we must request the existing table entry from the HMS, update
      // the fields, and write it back. Otherwise we'd overwrite metadata fields
      // that other tools put into place, such as table statistics. We do
      // overwrite all Kudu-specific entries such as the Kudu master addresses
      // and the full set of columns. This ensures entries are fully 'repaired'
      // during an alter operation.
      //
      // This can go wrong in a number of ways, including:
      //
      // - The original table name isn't a valid Hive database/table pair
      // - The new table name isn't a valid Hive database/table pair
      // - The original table does not exist in the HMS
      // - The original table doesn't match the Kudu table being altered

      string hms_database;
      string hms_table;
      RETURN_NOT_OK(ParseTableName(name, &hms_database, &hms_table));

      hive::Table table;
      RETURN_NOT_OK(client->GetTable(hms_database, hms_table, &table));

      // Check that the HMS entry belongs to the table being altered.
      if (table.parameters[HmsClient::kStorageHandlerKey] != HmsClient::kKuduStorageHandler ||
          table.parameters[HmsClient::kKuduTableIdKey] != id) {
        // The original table isn't a Kudu table, or isn't the same Kudu table.
        return Status::NotFound("the HMS entry for the table being "
                                "altered belongs to another table");
      }

      // Overwrite fields in the table that have changed, including the new name.
      RETURN_NOT_OK(PopulateTable(id, new_name, schema, master_addresses_, &table));
      return client->AlterTable(hms_database, hms_table, table, EnvironmentContext());
  });
}

Status HmsCatalog::GetNotificationEvents(int64_t last_event_id, int max_events,
                                         vector<hive::NotificationEvent>* events) {
  return Execute([&] (HmsClient* client) {
    return client->GetNotificationEvents(last_event_id, max_events, events);
  });
}

template<typename Task>
Status HmsCatalog::Execute(Task task) {
  Synchronizer synchronizer;
  auto callback = synchronizer.AsStdStatusCallback();

  // TODO(todd): wrapping this in a TRACE_EVENT scope and a LOG_IF_SLOW and such
  // would be helpful. Perhaps a TRACE message and/or a TRACE_COUNTER_INCREMENT
  // too to keep track of how much time is spent in calls to HMS for a given
  // CreateTable call. That will also require propagating the current Trace
  // object into the 'Rpc' object. Note that the HmsClient class already has
  // LOG_IF_SLOW calls internally.

  RETURN_NOT_OK(threadpool_->SubmitFunc([=] {
    // The main run routine of the threadpool thread. Runs the task with
    // exclusive access to the HMS client. If the task fails, it will be
    // retried, unless the failure type is non-retriable or the maximum number
    // of retries has been exceeded. Also handles re-connecting the HMS client
    // after a fatal error.
    //
    // Since every task submitted to the (single thread) pool runs this, it's
    // essentially a single iteration of a run loop which handles HMS client
    // reconnection and task processing.
    //
    // Notes on error handling:
    //
    // There are three separate error scenarios below:
    //
    // * Error while (re)connecting the HMS client - This is considered a
    // 'non-recoverable' error. The current task is immediately failed. In order
    // to avoid hot-looping and hammering the HMS with reconnect attempts on
    // every queued task, we set a backoff period. Any tasks which subsequently
    // run during this backoff period are also immediately failed.
    //
    // * Task results in a fatal error - a fatal error is any error caused by a
    // network or IO fault (not an application level failure). The HMS client
    // will attempt to reconnect, and the task will be retried (up to a limit).
    //
    // * Task results in a non-fatal error - a non-fatal error is an application
    // level error, and causes the task to be failed immediately (no retries).

    // Keep track of the first attempt's failure. Typically the first failure is
    // the most informative.
    Status first_failure;

    for (int attempt = 0; attempt <= FLAGS_hive_metastore_retry_count; attempt++) {
      if (!hms_client_.IsConnected()) {
        if (reconnect_after_ > MonoTime::Now()) {
          // Not yet ready to attempt reconnection; fail the task immediately.
          DCHECK(!reconnect_failure_.ok());
          return callback(reconnect_failure_);
        }

        // Attempt to reconnect.
        Status reconnect_status = Reconnect();
        if (!reconnect_status.ok()) {
          // Reconnect failed; retry with exponential backoff capped at 10s and
          // fail the task. We don't bother with jitter here because only the
          // leader master should be attempting this in any given period per
          // cluster.
          consecutive_reconnect_failures_++;
          reconnect_after_ = MonoTime::Now() +
              std::min(MonoDelta::FromMilliseconds(100 << consecutive_reconnect_failures_),
                       MonoDelta::FromSeconds(10));
          reconnect_failure_ = std::move(reconnect_status);
          return callback(reconnect_failure_);
        }

        consecutive_reconnect_failures_ = 0;
      }

      // Execute the task.
      Status task_status = task(&hms_client_);

      // If the task succeeds, or it's a non-retriable error, return the result.
      if (task_status.ok() || !IsFatalError(task_status)) {
        return callback(task_status);
      }

      // A fatal error occurred. Tear down the connection, and try again. We
      // don't log loudly here because odds are the reconnection will fail if
      // it's a true fault, at which point we do log loudly.
      VLOG(1) << "Call to HMS failed: " << task_status.ToString();

      if (attempt == 0) {
        first_failure = std::move(task_status);
      }

      WARN_NOT_OK(hms_client_.Stop(), "Failed to stop Hive Metastore client");
    }

    // We've exhausted the allowed retries.
    DCHECK(!first_failure.ok());
    LOG(WARNING) << "Call to HMS failed after " << FLAGS_hive_metastore_retry_count
                 << " retries: " << first_failure.ToString();

    return callback(first_failure);
  }));

  return synchronizer.Wait();
}

Status HmsCatalog::Reconnect() {
  Status s;

  HmsClientOptions options;
  options.send_timeout = MonoDelta::FromSeconds(FLAGS_hive_metastore_send_timeout);
  options.recv_timeout = MonoDelta::FromSeconds(FLAGS_hive_metastore_recv_timeout);
  options.conn_timeout = MonoDelta::FromSeconds(FLAGS_hive_metastore_conn_timeout);
  options.enable_kerberos = FLAGS_hive_metastore_sasl_enabled;

  // Try reconnecting to each HMS in sequence, returning the first one which
  // succeeds. In order to avoid getting 'stuck' on a partially failed HMS, we
  // remember which we connected to previously and try it last.
  for (int i = 0; i < hms_addresses_.size(); i++) {
    const auto& address = hms_addresses_[reconnect_idx_];
    reconnect_idx_ = (reconnect_idx_ + 1) % hms_addresses_.size();

    hms_client_ = HmsClient(address, options);
    s = hms_client_.Start();
    if (s.ok()) {
      VLOG(1) << "Connected to Hive Metastore " << address.ToString();
      return Status::OK();
    }

    WARN_NOT_OK(s, Substitute("Failed to connect to Hive Metastore ($0)", address.ToString()))
  }

  WARN_NOT_OK(hms_client_.Stop(), "Failed to stop Hive Metastore client");
  return s;
}

bool HmsCatalog::IsFatalError(const Status& status) {
  // Whitelist of errors which are not fatal. This errs on the side of
  // considering an error fatal since the consequences are low; just an
  // unnecessary reconnect. If a fatal error is not recognized it could cause
  // another RPC to fail, since there is no way to check the status of the
  // connection before sending an RPC.
  return !(status.IsAlreadyPresent()
        || status.IsNotFound()
        || status.IsInvalidArgument()
        || status.IsIllegalState()
        || status.IsRemoteError());
}

namespace {

string column_to_field_type(const ColumnSchema& column) {
  // See org.apache.hadoop.hive.serde.serdeConstants.
  switch (column.type_info()->type()) {
    case BOOL: return "boolean";
    case INT8: return "tinyint";
    case INT16: return "smallint";
    case INT32: return "int";
    case INT64: return "bigint";
    case DECIMAL32:
    case DECIMAL64:
    case DECIMAL128: return Substitute("decimal($0,$1)",
                                       column.type_attributes().precision,
                                       column.type_attributes().scale);
    case FLOAT: return "float";
    case DOUBLE: return "double";
    case STRING: return "string";
    case BINARY: return "binary";
    case UNIXTIME_MICROS: return "timestamp";
    default: LOG(FATAL) << "unhandled column type: " << column.TypeToString();
  }
  __builtin_unreachable();
}

hive::FieldSchema column_to_field(const ColumnSchema& column) {
  hive::FieldSchema field;
  field.name = column.name();
  field.type = column_to_field_type(column);
  return field;
}

} // anonymous namespace

Status HmsCatalog::PopulateTable(const string& id,
                                 const string& name,
                                 const Schema& schema,
                                 const string& master_addresses,
                                 hive::Table* table) {
  RETURN_NOT_OK(ParseTableName(name, &table->dbName, &table->tableName));

  // Add the Kudu-specific parameters. This intentionally avoids overwriting
  // other parameters.
  table->parameters[HmsClient::kKuduTableIdKey] = id;
  table->parameters[HmsClient::kKuduMasterAddrsKey] = master_addresses;
  table->parameters[HmsClient::kStorageHandlerKey] = HmsClient::kKuduStorageHandler;
  // Workaround for HIVE-19253.
  table->parameters[HmsClient::kExternalTableKey] = "TRUE";

  // Set the table type to external so that the table's (HD)FS directory will
  // not be deleted when the table is dropped. Deleting the directory is
  // unnecessary, and causes a race in the HMS between concurrent DROP TABLE and
  // CREATE TABLE operations on existing tables.
  table->tableType = HmsClient::kExternalTable;

  // Remove the deprecated Kudu-specific field 'kudu.table_name'.
  EraseKeyReturnValuePtr(&table->parameters, HmsClient::kLegacyKuduTableNameKey);

  // Overwrite the entire set of columns.
  vector<hive::FieldSchema> fields;
  for (const auto& column : schema.columns()) {
    fields.emplace_back(column_to_field(column));
  }
  table->sd.cols = std::move(fields);

  return Status::OK();
}

Status HmsCatalog::ParseTableName(const string& table,
                                  string* hms_database,
                                  string* hms_table) {
  // We do minimal parsing or validating of the identifiers, since Hive has
  // different validation rules based on configuration (and probably version).
  // The only rule we enforce is that there be exactly one period to separate
  // the database and table names, we leave checking of everything else to the
  // HMS.
  //
  // See org.apache.hadoop.hive.metastore.MetaStoreUtils.validateName.

  vector<string> identifiers = strings::Split(table, ".");
  if (identifiers.size() != 2) {
    return Status::InvalidArgument(kInvalidTableError, table);
  }

  *hms_database = std::move(identifiers[0]);
  *hms_table = std::move(identifiers[1]);
  return Status::OK();
}

Status HmsCatalog::ParseUris(const string& metastore_uris, vector<HostPort>* hostports) {
  hostports->clear();

  vector<string> uris = strings::Split(metastore_uris, ",", strings::SkipEmpty());
  const string kSchemeSeparator = "://";

  for (auto& uri : uris) {
    auto scheme_idx = uri.find(kSchemeSeparator, 1);
    if (scheme_idx == string::npos) {
      return Status::InvalidArgument("invalid Hive Metastore URI: missing scheme", uri);
    }
    uri.erase(0, scheme_idx + kSchemeSeparator.size());

    HostPort hp;
    RETURN_NOT_OK(hp.ParseString(uri, HmsClient::kDefaultHmsPort));
    // Note: the Java HMS client canonicalizes the hostname to a FQDN at this
    // point. We skip that because the krb5 library should handle it for us
    // (when rdns = true), whereas the Java GSSAPI implementation apparently
    // never canonicalizes.
    //
    // See org.apache.hadoop.hive.metastore.HiveMetastoreClient.resolveUris.
    hostports->emplace_back(std::move(hp));
  }

  return Status::OK();
}

// Validates the hive_metastore_uris gflag.
bool HmsCatalog::ValidateUris(const char* flag_name, const string& metastore_uris) {
  vector<HostPort> host_ports;
  Status s = HmsCatalog::ParseUris(metastore_uris, &host_ports);
  if (!s.ok()) {
    LOG(ERROR) << "invalid flag " << flag_name << ": " << s.ToString();
  }
  return s.ok();
}

bool HmsCatalog::IsEnabled() {
  return !FLAGS_hive_metastore_uris.empty();
}

hive::EnvironmentContext HmsCatalog::EnvironmentContext() {
  hive::EnvironmentContext env_ctx;
  env_ctx.__set_properties({ std::make_pair(hms::HmsClient::kKuduMasterEventKey, "true") });
  return env_ctx;
}

} // namespace hms
} // namespace kudu
