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

#include <boost/optional/optional.hpp>
#include <gtest/gtest_prod.h>

#include "kudu/gutil/port.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_client.h"
#include "kudu/thrift/client.h"
#include "kudu/util/status.h"

namespace kudu {

class HostPort;
class Schema;

namespace hms {

// A high-level API above the HMS which handles converting to and from
// Kudu-specific types, retries, reconnections, HA, error handling, and
// concurrent requests.
//
// This class is thread-safe after Start() is called.
class HmsCatalog {
 public:

  explicit HmsCatalog(std::string master_addresses);
  ~HmsCatalog();

  // Starts the HmsCatalog instance.
  Status Start();

  // Stops the HmsCatalog instance.
  void Stop();

  // Creates a new table entry in the HMS.
  //
  // If 'owner' is omitted the table will be created without an owner. This is
  // useful in circumstances where the owner is not known, for example when
  // creating an HMS table entry for an existing Kudu table.
  //
  // Fails the HMS is unreachable, or a table with the same name is already present.
  Status CreateTable(const std::string& id,
                     const std::string& name,
                     boost::optional<const std::string&> owner,
                     const Schema& schema,
                     const std::string& table_type = hms::HmsClient::kManagedTable)
                     WARN_UNUSED_RESULT;

  // Drops a table entry from the HMS.
  //
  // This method will fail if the HMS is unreachable, if the table does not
  // exist in the HMS, or if the table entry in the HMS doesn't match the
  // specified Kudu table ID.
  Status DropTable(const std::string& id, const std::string& name) WARN_UNUSED_RESULT;

  // Drops a legacy table from the HMS.
  //
  // This method will fail if the HMS is unreachable, or if the table does not
  // exist in the HMS.
  //
  // Note: it's possible to drop a non-legacy table using this method, but that
  // should be avoided, since it will skip the table ID checks in the Kudu HMS
  // plugin.
  Status DropLegacyTable(const std::string& name) WARN_UNUSED_RESULT;

  // Alters a table entry in the HMS.
  //
  // This method will fail if the HMS is unreachable, if the table doesn't exist
  // in the HMS. It will also fail if check_id is true and the table entry in
  // the HMS doesn't match the specified Kudu table ID.
  Status AlterTable(const std::string& id,
                    const std::string& name,
                    const std::string& new_name,
                    const Schema& schema,
                    const bool& check_id = true) WARN_UNUSED_RESULT;

  // Upgrades a legacy Impala table entry in the HMS.
  //
  // This method will fail if the HMS is unreachable, if the table is not a
  // legacy table, or if the table entry in not in the HMS.
  Status UpgradeLegacyImpalaTable(const std::string& id,
                                  const std::string& db_name,
                                  const std::string& tb_name,
                                  const Schema& schema) WARN_UNUSED_RESULT;

  // Downgrades to a legacy Impala table entry in the HMS.
  //
  // This method will fail if the HMS is unreachable, if the table is not a
  // Kudu table, or if the table entry in not in the HMS.
  Status DowngradeToLegacyImpalaTable(const std::string& name) WARN_UNUSED_RESULT;

  // Retrieves all Kudu tables in the HMS.
  //
  // Tables are considered to be Kudu tables if their storage handler matches
  // the legacy Kudu storage handler used by Impala, or the new Kudu storage
  // handler.
  //
  // This method will fail if the HMS is unreachable.
  Status GetKuduTables(std::vector<hive::Table>* kudu_tables) WARN_UNUSED_RESULT;

  // Retrieves notification log events from the HMS.
  //
  // The events will begin at id 'last_event_id + 1', and at most 'max_events'
  // events are returned.
  Status GetNotificationEvents(int64_t last_event_id, int max_events,
                               std::vector<hive::NotificationEvent>* events) WARN_UNUSED_RESULT;

  // Validates the hive_metastore_uris gflag.
  static bool ValidateUris(const char* flag_name, const std::string& metastore_uris);

  // Validates the Hive Metastore SASL gflags.
  static bool ValidateSasl();

  // Returns true if the HMS Catalog should be enabled.
  static bool IsEnabled();

  // Sets the Kudu-specific fields in the table without overwriting unrelated fields.
  //
  // The table owner will not be overwritten if an owner is not provided.
  static Status PopulateTable(const std::string& id,
                              const std::string& name,
                              const boost::optional<const std::string&>& owner,
                              const Schema& schema,
                              const std::string& master_addresses,
                              const std::string& table_type,
                              hive::Table* table) WARN_UNUSED_RESULT;

  // Validates and canonicalizes the provided table name according to HMS rules.
  // If the table name is not valid it will not be modified. If the table name
  // is valid, it will be canonicalized.
  //
  // Valid Kudu/HMS table names consist of a period ('.') separated database and
  // table name pair. The database and table names must contain only the ASCII
  // alphanumeric, '_', and '/' characters.
  //
  // Normalized Kudu/HMS table names are downcased so that they contain no
  // upper-case (A-Z) ASCII characters.
  //
  // Hive handles validating and canonicalizing table names in
  // org.apache.hadoop.hive.metastore.MetaStoreUtils.validateName and
  // org.apache.hadoop.hive.common.util.normalizeIdentifier.
  static Status NormalizeTableName(std::string* table_name) WARN_UNUSED_RESULT;

 private:

  FRIEND_TEST(HmsCatalogStaticTest, TestParseTableName);
  FRIEND_TEST(HmsCatalogStaticTest, TestParseTableNameSlices);
  FRIEND_TEST(HmsCatalogStaticTest, TestParseUris);

  // Drops a table entry from the HMS, supplying the provided environment context.
  Status DropTable(const std::string& name,
                   const hive::EnvironmentContext& env_ctx) WARN_UNUSED_RESULT;

  // Parses a Hive Metastore URI string into a sequence of HostPorts.
  static Status ParseUris(const std::string& metastore_uris, std::vector<HostPort>* hostports);

  // Returns a base environment context for use with calls to the HMS.
  static hive::EnvironmentContext EnvironmentContext(const bool& check_id = true);

  // Kudu master addresses.
  const std::string master_addresses_;

  thrift::HaClient<hms::HmsClient> ha_client_;
};

} // namespace hms
} // namespace kudu
