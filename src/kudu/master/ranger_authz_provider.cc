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

#include "kudu/master/ranger_authz_provider.h"

#include <algorithm>
#include <ostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/ranger/ranger.pb.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/status.h"

DECLARE_string(ranger_config_path);

namespace kudu {
class MetricEntity;
namespace master {

using kudu::security::ColumnPrivilegePB;
using kudu::security::TablePrivilegePB;
using kudu::ranger::ActionPB;
using kudu::ranger::ActionHash;
using kudu::ranger::RangerClient;
using std::string;
using std::unordered_set;

RangerAuthzProvider::RangerAuthzProvider(const scoped_refptr<MetricEntity>& metric_entity) :
  client_(metric_entity) {}

Status RangerAuthzProvider::Start() {
  RETURN_NOT_OK(client_.Start());

  return Status::OK();
}

Status RangerAuthzProvider::AuthorizeCreateTable(const string& table_name,
                                                 const string& user,
                                                 const string& /*owner*/) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }
  // Table creation requires 'CREATE ON DATABASE' privilege.
  return client_.AuthorizeAction(user, ActionPB::CREATE, table_name,
                                 RangerClient::Scope::DATABASE);
}

Status RangerAuthzProvider::AuthorizeDropTable(const string& table_name,
                                               const string& user) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }
  // Table deletion requires 'DROP ON TABLE' privilege.
  return client_.AuthorizeAction(user, ActionPB::DROP, table_name);
}

Status RangerAuthzProvider::AuthorizeAlterTable(const string& old_table,
                                                const string& new_table,
                                                const string& user) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }
  // Table alteration (without table rename) requires ALTER ON TABLE.
  if (old_table == new_table) {
    return client_.AuthorizeAction(user, ActionPB::ALTER, old_table);
  }

  // To prevent privilege escalation we require ALL on the old TABLE
  // and CREATE on the new DATABASE for table rename.
  RETURN_NOT_OK(client_.AuthorizeAction(user, ActionPB::ALL, old_table));
  return client_.AuthorizeAction(user, ActionPB::CREATE, new_table,
                                 RangerClient::Scope::DATABASE);
}

Status RangerAuthzProvider::AuthorizeGetTableMetadata(const string& table_name,
                                                      const string& user) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }
  // Get table metadata requires 'METADATA ON TABLE' privilege.
  return client_.AuthorizeAction(user, ActionPB::METADATA, table_name);
}

Status RangerAuthzProvider::AuthorizeListTables(const string& user,
                                                unordered_set<string>* table_names,
                                                bool* checked_table_names) {
  if (IsTrustedUser(user)) {
    *checked_table_names = false;
    return Status::OK();
  }

  *checked_table_names = true;
  // List tables requires 'METADATA ON TABLE' privilege on all tables being listed.
  return client_.AuthorizeActionMultipleTables(user, ActionPB::METADATA, table_names);
}

Status RangerAuthzProvider::AuthorizeGetTableStatistics(const string& table_name,
                                                        const string& user) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }
  // Statistics contain data (e.g. number of rows) that requires the 'SELECT ON TABLE'
  // privilege.
  return client_.AuthorizeAction(user, ActionPB::SELECT, table_name);
}

Status RangerAuthzProvider::FillTablePrivilegePB(const string& table_name,
                                                 const string& user,
                                                 const SchemaPB& schema_pb,
                                                 TablePrivilegePB* pb) {
  DCHECK(pb);
  DCHECK(pb->has_table_id());
  if (IsTrustedUser(user) || client_.AuthorizeAction(user, ActionPB::ALL, table_name).ok()) {
    pb->set_delete_privilege(true);
    pb->set_insert_privilege(true);
    pb->set_scan_privilege(true);
    pb->set_update_privilege(true);
    return Status::OK();
  }

  unordered_set<ActionPB, ActionHash> actions = {
    ActionPB::DELETE,
    ActionPB::INSERT,
    ActionPB::UPDATE,
    ActionPB::SELECT
  };

  // Check if the user has any table-level privileges. If yes, we set them. If
  // select is included, we can also return.
  auto s = client_.AuthorizeActions(user, table_name, &actions);
  if (s.ok()) {
    for (const ActionPB& action : actions) {
      switch (action) {
        case ActionPB::DELETE:
          pb->set_delete_privilege(true);
          break;
        case ActionPB::UPDATE:
          pb->set_update_privilege(true);
          break;
        case ActionPB::INSERT:
          pb->set_insert_privilege(true);
          break;
        case ActionPB::SELECT:
          pb->set_scan_privilege(true);
          break;
        default:
          LOG(WARNING) << "Unexpected action returned by Ranger: " << ActionPB_Name(action);
          break;
      }
      if (pb->scan_privilege()) {
        return Status::OK();
      }
    }
  }

  // If select is not allowed on the table level we need to dig in and set
  // select permissions on the column level.
  static ColumnPrivilegePB scan_col_privilege;
  scan_col_privilege.set_scan_privilege(true);

  unordered_set<string> column_names;
  for (const auto& col : schema_pb.columns()) {
    column_names.emplace(col.name());
  }

  // If AuthorizeAction returns NotAuthorized, that means no column-level select
  // is allowed to the user. In this case we return the previous status.
  // Otherwise we populate schema_pb and return OK.
  //
  // TODO(abukor): revisit if it's worth merge this into the previous request
  if (!client_.AuthorizeActionMultipleColumns(user, ActionPB::SELECT, table_name,
                                              &column_names).ok()) {
    return s;
  }

  for (const auto& col : schema_pb.columns()) {
    if (ContainsKey(column_names, col.name())) {
      InsertOrDie(pb->mutable_column_privileges(), col.id(), scan_col_privilege);
    }
  }


  return Status::OK();
}

bool RangerAuthzProvider::IsEnabled() {
  return !FLAGS_ranger_config_path.empty();
}

} // namespace master
} // namespace kudu
