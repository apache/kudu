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

#include <ostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/table_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/ranger/ranger.pb.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DECLARE_string(ranger_config_path);

using kudu::security::ColumnPrivilegePB;
using kudu::security::TablePrivilegePB;
using kudu::ranger::ActionPB;
using kudu::ranger::ActionHash;
using kudu::ranger::RangerClient;
using std::string;
using std::unordered_set;
using strings::Substitute;

namespace kudu {

class Env;
class MetricEntity;

namespace master {

namespace {

const char* kUnauthorizedAction = "Unauthorized action";
const char* kDenyNonRangerTableTemplate = "Denying action on table with invalid name $0. "
                                          "Use 'kudu table rename_table' to rename it to "
                                          "a Ranger-compatible name.";

Status ParseTableIdentifier(const string& table_name, string* db, string* table) {
  Slice tbl;
  auto s = ParseRangerTableIdentifier(table_name, db, &tbl);
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << Substitute(kDenyNonRangerTableTemplate, table_name);
    return Status::NotAuthorized(kUnauthorizedAction);
  }
  *table = tbl.ToString();
  return Status::OK();
}

} // anonymous namespace

RangerAuthzProvider::RangerAuthzProvider(Env* env,
                                         const scoped_refptr<MetricEntity>& metric_entity) :
  client_(env, metric_entity) {}

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

  string db;
  string tbl;

  RETURN_NOT_OK(ParseTableIdentifier(table_name, &db, &tbl));

  bool authorized;
  // Table creation requires 'CREATE ON DATABASE' privilege.
  RETURN_NOT_OK(client_.AuthorizeAction(user, ActionPB::CREATE, db, tbl, &authorized,
                                        RangerClient::Scope::DATABASE));

  if (PREDICT_FALSE(!authorized)) {
    LOG(WARNING) << Substitute("User $0 is not authorized to CREATE $1", user, table_name);
    return Status::NotAuthorized(kUnauthorizedAction);
  }

  return Status::OK();
}

Status RangerAuthzProvider::AuthorizeDropTable(const string& table_name,
                                               const string& user) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }

  string db;
  string tbl;

  RETURN_NOT_OK(ParseTableIdentifier(table_name, &db, &tbl));
  bool authorized;
  RETURN_NOT_OK(client_.AuthorizeAction(user, ActionPB::DROP, db, tbl, &authorized));

  if (PREDICT_FALSE(!authorized)) {
    LOG(WARNING) << Substitute("User $0 is not authorized to DROP $1", user, table_name);
    return Status::NotAuthorized(kUnauthorizedAction);
  }

  return Status::OK();
}

Status RangerAuthzProvider::AuthorizeAlterTable(const string& old_table,
                                                const string& new_table,
                                                const string& user) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }

  string old_db;
  string old_tbl;

  RETURN_NOT_OK(ParseTableIdentifier(old_table, &old_db, &old_tbl));
  // Table alteration (without table rename) requires ALTER ON TABLE.
  bool authorized;
  if (old_table == new_table) {
    RETURN_NOT_OK(client_.AuthorizeAction(user, ActionPB::ALTER, old_db, old_tbl, &authorized));

    if (PREDICT_FALSE(!authorized)) {
      LOG(WARNING) << Substitute("User $0 is not authorized to ALTER $1", user, old_table);
      return Status::NotAuthorized(kUnauthorizedAction);
    }

    return Status::OK();
  }

  // To prevent privilege escalation we require ALL on the old TABLE
  // and CREATE on the new DATABASE for table rename.
  RETURN_NOT_OK(client_.AuthorizeAction(user, ActionPB::ALL, old_db, old_tbl, &authorized));
  if (PREDICT_FALSE(!authorized)) {
    LOG(WARNING) << Substitute("User $0 is not authorized to perform ALL on $1", user, old_table);
    return Status::NotAuthorized(kUnauthorizedAction);
  }

  string new_db;
  string new_tbl;

  RETURN_NOT_OK(ParseTableIdentifier(new_table, &new_db, &new_tbl));
  RETURN_NOT_OK(client_.AuthorizeAction(user, ActionPB::CREATE, new_db, new_tbl, &authorized,
                                        RangerClient::Scope::DATABASE));

  if (PREDICT_FALSE(!authorized)) {
    LOG(WARNING) << Substitute("User $0 is not authorized to CREATE $1", user, new_table);
    return Status::NotAuthorized(kUnauthorizedAction);
  }

  return Status::OK();
}

Status RangerAuthzProvider::AuthorizeGetTableMetadata(const string& table_name,
                                                      const string& user) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }

  string db;
  string tbl;

  RETURN_NOT_OK(ParseTableIdentifier(table_name, &db, &tbl));
  bool authorized;
  // Get table metadata requires 'METADATA ON TABLE' privilege.
  RETURN_NOT_OK(client_.AuthorizeAction(user, ActionPB::METADATA, db, tbl, &authorized));

  if (PREDICT_FALSE(!authorized)) {
    LOG(WARNING) << Substitute("User $0 is not authorized to access METADATA on $1", user,
        table_name);
    return Status::NotAuthorized(kUnauthorizedAction);
  }

  return Status::OK();
}

Status RangerAuthzProvider::AuthorizeListTables(const string& user,
                                                unordered_set<string>* table_names,
                                                bool* checked_table_names) {
  if (IsTrustedUser(user)) {
    *checked_table_names = false;
    return Status::OK();
  }

  *checked_table_names = true;

  // Return immediately if there is no tables to authorize against.
  if (table_names->empty()) {
    return Status::OK();
  }

  // List tables requires 'METADATA ON TABLE' privilege on all tables being listed.
  return client_.AuthorizeActionMultipleTables(user, ActionPB::METADATA, table_names);
}

Status RangerAuthzProvider::AuthorizeGetTableStatistics(const string& table_name,
                                                        const string& user) {
  if (IsTrustedUser(user)) {
    return Status::OK();
  }
  string db;
  string tbl;

  RETURN_NOT_OK(ParseTableIdentifier(table_name, &db, &tbl));
  bool authorized;
  // Statistics contain data (e.g. number of rows) that requires the 'SELECT ON TABLE'
  // privilege.
  RETURN_NOT_OK(client_.AuthorizeAction(user, ActionPB::SELECT, db, tbl, &authorized));

  if (PREDICT_FALSE(!authorized)) {
    LOG(WARNING) << Substitute("User $0 is not authorized to SELECT on $1", user, table_name);
    return Status::NotAuthorized(kUnauthorizedAction);
  }

  return Status::OK();
}

Status RangerAuthzProvider::FillTablePrivilegePB(const string& table_name,
                                                 const string& user,
                                                 const SchemaPB& schema_pb,
                                                 TablePrivilegePB* pb) {
  DCHECK(pb);
  DCHECK(pb->has_table_id());

  string db;
  string tbl;
  RETURN_NOT_OK(ParseTableIdentifier(table_name, &db, &tbl));

  bool authorized;
  if (IsTrustedUser(user)) {
    authorized = true;
  } else {
    RETURN_NOT_OK(client_.AuthorizeAction(user, ActionPB::ALL, db, tbl, &authorized));
  }
  if (authorized) {
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
  RETURN_NOT_OK(client_.AuthorizeActions(user, db, tbl, &actions));
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

  // If select is not allowed on the table level we need to dig in and set
  // select permissions on the column level.
  static ColumnPrivilegePB scan_col_privilege;
  scan_col_privilege.set_scan_privilege(true);

  unordered_set<string> column_names;
  for (const auto& col : schema_pb.columns()) {
    column_names.emplace(col.name());
  }


  // TODO(abukor): revisit if it's worth merging this into the previous request
  RETURN_NOT_OK(client_.AuthorizeActionMultipleColumns(user, ActionPB::SELECT, db, tbl,
                                                       &column_names));

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
