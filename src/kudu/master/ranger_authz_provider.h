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
#include <unordered_set>

#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/master/authz_provider.h"
#include "kudu/ranger/ranger_client.h"
#include "kudu/util/status.h"

namespace kudu {

class Env;
class MetricEntity;
class SchemaPB;

namespace security {
class TablePrivilegePB;
}  // namespace security

namespace master {

// An implementation of AuthzProvider that connects to Ranger and translates
// authorization requests to Ranger and allows or denies the actions based on
// the received responses.
class RangerAuthzProvider : public AuthzProvider {
 public:

  explicit RangerAuthzProvider(Env* env, const scoped_refptr<MetricEntity>& metric_entity);

  Status Start() override;

  void Stop() override {}

  Status AuthorizeCreateTable(const std::string& table_name,
                              const std::string& user,
                              const std::string& owner) override WARN_UNUSED_RESULT;

  Status AuthorizeDropTable(const std::string& table_name,
                            const std::string& user) override WARN_UNUSED_RESULT;

  Status AuthorizeAlterTable(const std::string& old_table,
                             const std::string& new_table,
                             const std::string& user) override WARN_UNUSED_RESULT;

  Status AuthorizeGetTableMetadata(const std::string& table_name,
                                   const std::string& user) override WARN_UNUSED_RESULT;

  Status AuthorizeListTables(const std::string& user,
                             std::unordered_set<std::string>* table_names,
                             bool* checked_table_names) override WARN_UNUSED_RESULT;

  Status AuthorizeGetTableStatistics(const std::string& table_name,
                                     const std::string& user) override WARN_UNUSED_RESULT;

  Status FillTablePrivilegePB(const std::string& table_name,
                              const std::string& user,
                              const SchemaPB& schema_pb,
                              security::TablePrivilegePB* pb) override WARN_UNUSED_RESULT;

  // Returns true if 'ranger_policy_server' flag is set indicating Ranger
  // authorization is enabled.
  static bool IsEnabled();

 private:
  ranger::RangerClient client_;
};

} // namespace master
} // namespace kudu
