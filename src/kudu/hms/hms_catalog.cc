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

#include <iostream>
#include <iterator>
#include <map>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/table_util.h"
#include "kudu/common/types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/ascii_ctype.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_client.h"
#include "kudu/thrift/client.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/slice.h"

using boost::none;
using boost::optional;
using std::move;
using std::string;
using std::vector;
using strings::Substitute;

DEFINE_string(hive_metastore_uris, "",
              "Address of the Hive Metastore instance(s). The provided port must be for the HMS "
              "Thrift service. If a port is not provided, defaults to 9083. If the HMS is deployed "
              "in an HA configuration, multiple comma-separated addresses should be supplied. "
              "If not set, the Kudu master will not send Kudu table catalog updates to Hive. The "
              "configured value must match the Hive hive.metastore.uris configuration.");
DEFINE_validator(hive_metastore_uris, &kudu::hms::HmsCatalog::ValidateUris);

// Note: the hive_metastore_sasl_enabled and keytab_file combination is validated in master.cc.
DEFINE_bool(hive_metastore_sasl_enabled, false,
            "Configures whether Thrift connections to the Hive Metastore use SASL "
            "(Kerberos) security. Must match the value of the "
            "hive.metastore.sasl.enabled option in the Hive Metastore configuration. "
            "When enabled, the --keytab_file flag must be provided.");

DEFINE_string(hive_metastore_kerberos_principal, "hive",
              "The service principal of the Hive Metastore server. Must match "
              "the primary (user) portion of hive.metastore.kerberos.principal option "
              "in the Hive Metastore configuration.");

DEFINE_int32(hive_metastore_retry_count, 1,
             "The number of times that HMS operations will retry after "
             "encountering retriable failures, such as network errors.");
TAG_FLAG(hive_metastore_retry_count, advanced);

DEFINE_int32(hive_metastore_send_timeout_seconds, 60,
             "Configures the socket send timeout, in seconds, for Thrift "
             "connections to the Hive Metastore.");
TAG_FLAG(hive_metastore_send_timeout_seconds, advanced);

DEFINE_int32(hive_metastore_recv_timeout_seconds, 60,
             "Configures the socket receive timeout, in seconds, for Thrift "
             "connections to the Hive Metastore.");
TAG_FLAG(hive_metastore_recv_timeout_seconds, advanced);

DEFINE_int32(hive_metastore_conn_timeout_seconds, 60,
             "Configures the socket connect timeout, in seconds, for Thrift "
             "connections to the Hive Metastore.");
TAG_FLAG(hive_metastore_conn_timeout_seconds, advanced);

DEFINE_int32(hive_metastore_max_message_size_bytes, 100 * 1024 * 1024,
             "Maximum size of Hive Metastore objects that can be received by the "
             "HMS client in bytes. Should match the metastore.server.max.message.size "
             "configuration.");
TAG_FLAG(hive_metastore_max_message_size_bytes, advanced);

namespace kudu {
namespace hms {

HmsCatalog::HmsCatalog(string master_addresses)
    : master_addresses_(std::move(master_addresses)) {
}

HmsCatalog::~HmsCatalog() {
  Stop();
}

Status HmsCatalog::Start(HmsClientVerifyKuduSyncConfig verify_service_config) {
  vector<HostPort> addresses;
  RETURN_NOT_OK(ParseUris(FLAGS_hive_metastore_uris, &addresses));

  thrift::ClientOptions options;
  options.send_timeout = MonoDelta::FromSeconds(FLAGS_hive_metastore_send_timeout_seconds);
  options.recv_timeout = MonoDelta::FromSeconds(FLAGS_hive_metastore_recv_timeout_seconds);
  options.conn_timeout = MonoDelta::FromSeconds(FLAGS_hive_metastore_conn_timeout_seconds);
  options.enable_kerberos = FLAGS_hive_metastore_sasl_enabled;
  options.service_principal = FLAGS_hive_metastore_kerberos_principal;
  options.max_buf_size = FLAGS_hive_metastore_max_message_size_bytes;
  options.retry_count = FLAGS_hive_metastore_retry_count;
  options.verify_service_config = verify_service_config == VERIFY;

  RETURN_NOT_OK(ha_client_.Start(std::move(addresses), std::move(options)));

  // Fetch the UUID of the HMS DB, if available.
  string uuid;
  Status uuid_status = ha_client_.Execute([&] (HmsClient* client) {
    return client->GetUuid(&uuid);
  });
  if (uuid_status.ok()) {
    VLOG(1) << "Connected to HMS with uuid " << uuid;
    uuid_ = std::move(uuid);
  } else if (uuid_status.IsNotSupported()) {
    VLOG(1) << "Unable to fetch UUID for HMS: " << uuid_status.ToString();
  } else {
    // If we couldn't connect to the HMS at all, fail.
    return uuid_status;
  }
  return Status::OK();
}

void HmsCatalog::Stop() {
  ha_client_.Stop();
}

Status HmsCatalog::CreateTable(const string& id,
                               const string& name,
                               optional<const string&> owner,
                               const Schema& schema,
                               const string& table_type) {
  hive::Table table;
  RETURN_NOT_OK(PopulateTable(id, name, owner, schema, master_addresses_, table_type, &table));
  return ha_client_.Execute([&] (HmsClient* client) {
      return client->CreateTable(table, EnvironmentContext());
  });
}

Status HmsCatalog::DropTable(const string& id, const string& name) {
  hive::EnvironmentContext env_ctx = EnvironmentContext();
  env_ctx.properties.insert(make_pair(HmsClient::kKuduTableIdKey, id));
  return DropTable(name, env_ctx);
}

Status HmsCatalog::DropLegacyTable(const string& name) {
  return DropTable(name, EnvironmentContext());
}

Status HmsCatalog::DropTable(const string& name, const hive::EnvironmentContext& env_ctx) {
  Slice hms_database;
  Slice hms_table;
  RETURN_NOT_OK(ParseHiveTableIdentifier(name, &hms_database, &hms_table));
  return ha_client_.Execute([&] (HmsClient* client) {
    return client->DropTable(hms_database.ToString(), hms_table.ToString(), env_ctx);
  });
}

Status HmsCatalog::UpgradeLegacyImpalaTable(const string& id,
                                            const string& db_name,
                                            const string& tb_name,
                                            const Schema& schema) {
  return ha_client_.Execute([&] (HmsClient* client) {
    hive::Table table;
    RETURN_NOT_OK(client->GetTable(db_name, tb_name, &table));
    if (table.parameters[HmsClient::kStorageHandlerKey] !=
        HmsClient::kLegacyKuduStorageHandler) {
      return Status::IllegalState("non-legacy table cannot be upgraded");
    }

    // If this is an external table, only upgrade the storage handler.
    if (table.tableType == HmsClient::kExternalTable) {
      table.parameters[HmsClient::kStorageHandlerKey] = HmsClient::kKuduStorageHandler;
    } else if (table.tableType == HmsClient::kManagedTable) {
      RETURN_NOT_OK(PopulateTable(id, Substitute("$0.$1", db_name, tb_name),
                                  table.owner, schema, master_addresses_, table.tableType, &table));
    } else {
      return Status::IllegalState(Substitute("Unsupported table type $0", table.tableType));
    }
    return client->AlterTable(db_name, tb_name, table, EnvironmentContext());
  });
}

Status HmsCatalog::DowngradeToLegacyImpalaTable(const string& name) {
  Slice hms_database;
  Slice hms_table;
  RETURN_NOT_OK(ParseHiveTableIdentifier(name, &hms_database, &hms_table));

  return ha_client_.Execute([&] (HmsClient* client) {
    hive::Table table;
    RETURN_NOT_OK(client->GetTable(hms_database.ToString(), hms_table.ToString(), &table));
    if (!hms::HmsClient::IsKuduTable(table)) {
      return Status::IllegalState("non-Kudu table cannot be downgraded");
    }
    // Downgrade the storage handler.
    table.parameters[HmsClient::kStorageHandlerKey] = HmsClient::kLegacyKuduStorageHandler;
    if (table.tableType == HmsClient::kManagedTable) {
      // Remove the Kudu-specific field 'kudu.table_id'.
      EraseKeyReturnValuePtr(&table.parameters, HmsClient::kKuduTableIdKey);
    }
    return client->AlterTable(table.dbName, table.tableName, table, EnvironmentContext());
  });
}

Status HmsCatalog::GetKuduTables(vector<hive::Table>* kudu_tables) {
  return ha_client_.Execute([&] (HmsClient* client) {
    vector<string> database_names;
    RETURN_NOT_OK(client->GetAllDatabases(&database_names));
    vector<string> table_names;
    vector<hive::Table> tables;

    for (const auto& database_name : database_names) {
      table_names.clear();
      tables.clear();
      RETURN_NOT_OK(client->GetTableNames(
            database_name,
            Substitute("$0$1 = \"$2\" OR $0$1 = \"$3\"",
              HmsClient::kHiveFilterFieldParams,
              HmsClient::kStorageHandlerKey,
              HmsClient::kKuduStorageHandler,
              HmsClient::kLegacyKuduStorageHandler),
            &table_names));

      if (!table_names.empty()) {
        RETURN_NOT_OK(client->GetTables(database_name, table_names, &tables));
        std::move(tables.begin(), tables.end(), std::back_inserter(*kudu_tables));
      }
    }

    return Status::OK();
  });
}

Status HmsCatalog::AlterTable(const string& id,
                              const string& name,
                              const string& new_name,
                              const Schema& schema,
                              const bool& check_id) {
  Slice hms_database;
  Slice hms_table;
  RETURN_NOT_OK(ParseHiveTableIdentifier(name, &hms_database, &hms_table));

  return ha_client_.Execute([&] (HmsClient* client) {
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

      hive::Table table;
      RETURN_NOT_OK(client->GetTable(hms_database.ToString(), hms_table.ToString(), &table));

      // Check that the HMS entry belongs to the table being altered.
      if (!hms::HmsClient::IsKuduTable(table) ||
          (check_id && table.parameters[HmsClient::kKuduTableIdKey] != id)) {
        // The original table isn't a Kudu table, or isn't the same Kudu table.
        return Status::NotFound("the HMS entry for the table being "
                                "altered belongs to another table");
      }

      // Overwrite fields in the table that have changed, including the new name.
      RETURN_NOT_OK(PopulateTable(id, new_name, table.owner, schema, master_addresses_,
          table.tableType, &table));
      return client->AlterTable(hms_database.ToString(), hms_table.ToString(),
                                table, EnvironmentContext(check_id));
  });
}

Status HmsCatalog::GetNotificationEvents(int64_t last_event_id, int max_events,
                                         vector<hive::NotificationEvent>* events) {
  return ha_client_.Execute([&] (HmsClient* client) {
    return client->GetNotificationEvents(last_event_id, max_events, events);
  });
}

Status HmsCatalog::GetUuid(string* uuid) {
  if (!uuid_) {
    return Status::NotSupported("No HMS UUID available");
  }
  *uuid = uuid_.value();
  return Status::OK();
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
  field.comment = column.comment();
  return field;
}

// Convert an ASCII encoded string to lowercase in place.
void ToLowerCase(Slice s) {
  for (int i = 0; i < s.size(); i++) {
    s.mutable_data()[i] = ascii_tolower(s[i]);
  }
}
} // anonymous namespace

Status HmsCatalog::PopulateTable(const string& id,
                                 const string& name,
                                 const optional<const string&>& owner,
                                 const Schema& schema,
                                 const string& master_addresses,
                                 const string& table_type,
                                 hive::Table* table) {
  Slice hms_database_name;
  Slice hms_table_name;
  RETURN_NOT_OK(ParseHiveTableIdentifier(name, &hms_database_name, &hms_table_name));
  table->dbName = hms_database_name.ToString();
  table->tableName = hms_table_name.ToString();
  if (owner) {
    table->owner = *owner;
  }

  // TODO(HIVE-21640): Fix the issue described below and in HIVE-21640
  // Setting the table type to managed means the table's (HD)FS directory will
  // be deleted when the table is dropped. Deleting the directory is
  // unnecessary, and causes a race in the HMS between concurrent DROP TABLE and
  // CREATE TABLE operations on existing tables.
  table->tableType = table_type;

  // TODO(HIVE-19253): Used along with table type to indicate an external table.
  if (table_type == HmsClient::kExternalTable) {
    table->parameters[HmsClient::kExternalTableKey] = "TRUE";
  // Only set the table id on managed tables and ensure the
  // kExternalTableKey property is unset.
  } else if (table_type == HmsClient::kManagedTable) {
    table->parameters[HmsClient::kKuduTableIdKey] = id;
    EraseKeyReturnValuePtr(&table->parameters, HmsClient::kExternalTableKey);
  }

  // Add the Kudu-specific parameters. This intentionally avoids overwriting
  // other parameters.
  table->parameters[HmsClient::kKuduTableNameKey] = name;
  table->parameters[HmsClient::kKuduMasterAddrsKey] = master_addresses;
  table->parameters[HmsClient::kStorageHandlerKey] = HmsClient::kKuduStorageHandler;

  // Overwrite the entire set of columns.
  vector<hive::FieldSchema> fields;
  for (const auto& column : schema.columns()) {
    fields.emplace_back(column_to_field(column));
  }
  table->sd.cols = std::move(fields);

  return Status::OK();
}

Status HmsCatalog::NormalizeTableName(string* table_name) {
  CHECK_NOTNULL(table_name);
  Slice hms_database;
  Slice hms_table;
  RETURN_NOT_OK(ParseHiveTableIdentifier(*table_name, &hms_database, &hms_table));

  ToLowerCase(hms_database);
  ToLowerCase(hms_table);

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

hive::EnvironmentContext HmsCatalog::EnvironmentContext(const bool& check_id) {
  hive::EnvironmentContext env_ctx;
  env_ctx.__set_properties({
    std::make_pair(hms::HmsClient::kKuduMasterEventKey, "true"),
    std::make_pair(hms::HmsClient::kKuduCheckIdKey, check_id ? "true" : "false"),
  });
  return env_ctx;
}

} // namespace hms
} // namespace kudu
