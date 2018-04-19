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

#include <string>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/hms/hms_client.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace hive {
class EnvironmentContext;
class Table;
}

namespace kudu {

class Schema;
class ThreadPool;

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
  // Fails the HMS is unreachable, or a table with the same name is already present.
  Status CreateTable(const std::string& id,
                     const std::string& name,
                     const Schema& schema) WARN_UNUSED_RESULT;

  // Drops a table entry from the HMS, if it exists.
  //
  // This method will fail if the HMS is unreachable, or if the table entry in
  // the HMS doesn't match the specified Kudu table ID.
  Status DropTable(const std::string& id,
                   const std::string& name) WARN_UNUSED_RESULT;

  // Alters a table entry in the HMS, if it exists. If the table entry does not
  // exist it will be created instead.
  //
  // This method will fail if the HMS is unreachable, or if the table entry in
  // the HMS doesn't match the specified Kudu table ID.
  Status AlterTable(const std::string& id,
                    const std::string& name,
                    const std::string& new_name,
                    const Schema& schema) WARN_UNUSED_RESULT;

  // Validates the hive_metastore_uris gflag.
  static bool ValidateUris(const char* flag_name, const std::string& metastore_uris);

  // Validates the Hive Metastore SASL gflags.
  static bool ValidateSasl();

  // Returns true if the HMS Catalog should be enabled.
  static bool IsEnabled();

 private:

  FRIEND_TEST(HmsCatalogStaticTest, TestParseTableName);
  FRIEND_TEST(HmsCatalogStaticTest, TestParseUris);

  // Synchronously executes a task with exclusive access to the HMS client.
  template<typename Task>
  Status Execute(Task task) WARN_UNUSED_RESULT;

  // Reconnects hms_client_ to an HMS, or returns an error if all HMS instances
  // are unavailable.
  Status Reconnect();

  // Returns true if the RPC status is 'fatal', e.g. the Thrift connection on
  // which it occurred should be shut down.
  static bool IsFatalError(const Status& status);

  // Sets the Kudu-specific fields in the table without overwriting unrelated fields.
  static Status PopulateTable(const std::string& id,
                              const std::string& name,
                              const Schema& schema,
                              const std::string& master_addresses,
                              hive::Table* table) WARN_UNUSED_RESULT;

  // Creates a table entry in the HMS, or updates that existing entry if the
  // table already has an entry.
  //
  // Instead of only attempting to create or alter a table entry, this method should be
  // used to ensure the HMS is kept synchronized in as many edge cases as possible.
  static Status CreateOrUpdateTable(hms::HmsClient* client,
                                    const std::string& id,
                                    const std::string& name,
                                    const Schema& schema,
                                    const std::string& master_addresses) WARN_UNUSED_RESULT;

  // Parses a Kudu table name into a Hive database and table name.
  // Returns an error if the Kudu table name is not correctly formatted.
  static Status ParseTableName(const std::string& table,
                               std::string* hms_database,
                               std::string* hms_table) WARN_UNUSED_RESULT;

  // Parses a Hive Metastore URI string into a sequence of HostPorts.
  static Status ParseUris(const std::string& metastore_uris, std::vector<HostPort>* hostports);

  // Returns a base environment context for use with calls to the HMS.
  static hive::EnvironmentContext EnvironmentContext();

  // Kudu master addresses.
  const std::string master_addresses_;

  // Initialized during Start().
  std::vector<HostPort> hms_addresses_;
  gscoped_ptr<ThreadPool> threadpool_;

  // Fields only used by the threadpool thread:

  // Options to use when creating the HMS client.
  hms::HmsClientOptions hms_client_options_;
  // The HMS client.
  hms::HmsClient hms_client_;

  // Fields which track consecutive reconnection attempts and backoff.
  MonoTime reconnect_after_;
  Status reconnect_failure_;
  int consecutive_reconnect_failures_;
  int reconnect_idx_;
};

} // namespace hms
} // namespace kudu
