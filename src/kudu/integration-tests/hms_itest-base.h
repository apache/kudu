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
#include "kudu/hms/mini_hms.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/status.h"

namespace kudu {

class MonoDelta;
namespace thrift {
struct ClientOptions;
} // namespace thrift

namespace client {
class KuduClient;
} // namespace client

class HmsITestHarness {
 public:
  Status StartHms(const std::unique_ptr<cluster::ExternalMiniCluster>& cluster)
    WARN_UNUSED_RESULT;
  Status StopHms(const std::unique_ptr<cluster::ExternalMiniCluster>& cluster)
    WARN_UNUSED_RESULT;

  // Creates a database in the HMS catalog.
  Status CreateDatabase(const std::string& database_name) WARN_UNUSED_RESULT;

  // Creates a table in Kudu.
  static Status CreateKuduTable(const std::string& database_name,
                                const std::string& table_name,
                                const client::sp::shared_ptr<client::KuduClient>& client,
                                MonoDelta timeout = {}) WARN_UNUSED_RESULT;

  // Creates a table in the HMS catalog.
  // If supplied, 'kudu_table_name' will be used for the 'kudu.table_name'
  // field of the HMS entry.
  Status CreateHmsTable(const std::string& database_name,
                        const std::string& table_name,
                        const std::string& table_type = hms::HmsClient::kManagedTable,
                        const boost::optional<const std::string&>& kudu_table_name = boost::none);

  // Renames a table entry in the HMS catalog.
  Status RenameHmsTable(const std::string& database_name,
                        const std::string& old_table_name,
                        const std::string& new_table_name) WARN_UNUSED_RESULT;

  // Drops all columns from a Kudu HMS table entry.
  Status AlterHmsTableDropColumns(const std::string& database_name,
                                  const std::string& table_name) WARN_UNUSED_RESULT;

  // Alter the HMS entry to be an external table with `external.table.purge = true`.
  Status AlterHmsTableExternalPurge(const std::string& database_name,
                                    const std::string& table_name) WARN_UNUSED_RESULT;

  // Checks that the Kudu table schema and the HMS table entry in their
  // respective catalogs are synchronized for a particular table. It also
  // verifies that the table owner is the given user (if not provided,
  // checks against the logged in user).
  void CheckTable(const std::string& database_name,
                  const std::string& table_name,
                  const boost::optional<const std::string&>& user,
                  const std::unique_ptr<cluster::ExternalMiniCluster>& cluster,
                  const client::sp::shared_ptr<client::KuduClient>& client,
                  const std::string& table_type = hms::HmsClient::kManagedTable);

  // Checks that a table does not exist in the Kudu and HMS catalogs.
  void CheckTableDoesNotExist(const std::string& database_name,
                              const std::string& table_name,
                              const client::sp::shared_ptr<client::KuduClient>& client);

  hms::HmsClient* hms_client() {
    return hms_client_.get();
  }

  Status RestartHmsClient(const std::unique_ptr<cluster::ExternalMiniCluster>& cluster,
                          const thrift::ClientOptions& hms_opts) {
    hms_client_.reset(new hms::HmsClient(cluster->hms()->address(), hms_opts));
    return hms_client_->Start();
  }

 protected:
  std::unique_ptr<hms::HmsClient> hms_client_;
};

} // namespace kudu
