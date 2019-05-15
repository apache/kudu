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

#include <memory>
#include <string>

#include <boost/optional/optional.hpp>

#include "kudu/gutil/port.h"
#include "kudu/hms/hms_client.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/util/status.h"

namespace kudu {

class MonoDelta;

class HmsITestBase : public ExternalMiniClusterITestBase {
 public:
  Status StartHms() WARN_UNUSED_RESULT;
  Status StopHms() WARN_UNUSED_RESULT;

  // Creates a database in the HMS catalog.
  Status CreateDatabase(const std::string& database_name) WARN_UNUSED_RESULT;

  // Creates a table in Kudu.
  Status CreateKuduTable(const std::string& database_name,
                         const std::string& table_name,
                         MonoDelta timeout = {}) WARN_UNUSED_RESULT;

  // Creates a table in the HMS catalog.
  Status CreateHmsTable(const std::string& database_name,
                        const std::string& table_name,
                        const std::string& table_type = hms::HmsClient::kManagedTable);

  // Renames a table entry in the HMS catalog.
  Status RenameHmsTable(const std::string& database_name,
                        const std::string& old_table_name,
                        const std::string& new_table_name) WARN_UNUSED_RESULT;

  // Drops all columns from a Kudu HMS table entry.
  Status AlterHmsTableDropColumns(const std::string& database_name,
                                  const std::string& table_name) WARN_UNUSED_RESULT;

  // Checks that the Kudu table schema and the HMS table entry in their
  // respective catalogs are synchronized for a particular table. It also
  // verifies that the table owner is the given user (if not provided,
  // checks against the logged in user).
  void CheckTable(const std::string& database_name,
                  const std::string& table_name,
                  boost::optional<const std::string&> user);

  // Checks that a table does not exist in the Kudu and HMS catalogs.
  void CheckTableDoesNotExist(const std::string& database_name,
                              const std::string& table_name);

 protected:
  std::unique_ptr<hms::HmsClient> hms_client_;
};

} // namespace kudu
