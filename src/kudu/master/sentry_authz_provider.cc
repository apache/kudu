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

#include "kudu/master/sentry_authz_provider.h"

#include <ostream>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/table_util.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/sentry/sentry_action.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/thrift/client.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/slice.h"

using sentry::TListSentryPrivilegesRequest;
using sentry::TListSentryPrivilegesResponse;
using sentry::TSentryAuthorizable;
using sentry::TSentryGrantOption;
using std::string;
using std::vector;

DEFINE_string(sentry_service_rpc_addresses, "",
              "Comma-separated list of RPC addresses of the Sentry service(s). When "
              "set, Sentry integration is enabled, fine-grained access control is "
              "enforced in the master, and clients are issued authorization tokens. "
              "Must match the value of the sentry.service.client.server.rpc-addresses "
              "option in the Sentry server configuration.");
TAG_FLAG(sentry_service_rpc_addresses, experimental);

DEFINE_string(server_name, "server1",
              "Configures which server namespace the Kudu instance belongs to for defining "
              "server-level privileges in Sentry. Used to distinguish a particular Kudu "
              "cluster in case of a multi-cluster setup. Must match the value of the "
              "hive.sentry.server option in the HiveServer2 configuration, and the value "
              "of the --server_name in Impala configuration.");
TAG_FLAG(server_name, experimental);

DEFINE_string(kudu_service_name, "kudu",
              "The service name of the Kudu server. Must match the service name "
              "used for Kudu server of sentry.service.admin.group option in the "
              "Sentry server configuration.");
TAG_FLAG(kudu_service_name, experimental);

DEFINE_string(sentry_service_kerberos_principal, "sentry",
              "The service principal of the Sentry server. Must match the primary "
              "(user) portion of sentry.service.server.principal option in the "
              "Sentry server configuration.");
TAG_FLAG(sentry_service_kerberos_principal, experimental);

DEFINE_string(sentry_service_security_mode, "kerberos",
              "Configures whether Thrift connections to the Sentry server use "
              "SASL (Kerberos) security. Must match the value of the "
              "‘sentry.service.security.mode’ option in the Sentry server "
              "configuration.");
TAG_FLAG(sentry_service_security_mode, experimental);

DEFINE_int32(sentry_service_retry_count, 1,
             "The number of times that Sentry operations will retry after "
             "encountering retriable failures, such as network errors.");
TAG_FLAG(sentry_service_retry_count, advanced);
TAG_FLAG(sentry_service_retry_count, experimental);

DEFINE_int32(sentry_service_send_timeout_seconds, 60,
             "Configures the socket send timeout, in seconds, for Thrift "
             "connections to the Sentry server.");
TAG_FLAG(sentry_service_send_timeout_seconds, advanced);
TAG_FLAG(sentry_service_send_timeout_seconds, experimental);

DEFINE_int32(sentry_service_recv_timeout_seconds, 60,
             "Configures the socket receive timeout, in seconds, for Thrift "
             "connections to the Sentry server.");
TAG_FLAG(sentry_service_recv_timeout_seconds, advanced);
TAG_FLAG(sentry_service_recv_timeout_seconds, experimental);

DEFINE_int32(sentry_service_conn_timeout_seconds, 60,
             "Configures the socket connect timeout, in seconds, for Thrift "
             "connections to the Sentry server.");
TAG_FLAG(sentry_service_conn_timeout_seconds, advanced);
TAG_FLAG(sentry_service_conn_timeout_seconds, experimental);

DEFINE_int32(sentry_service_max_message_size_bytes, 100 * 1024 * 1024,
             "Maximum size of Sentry objects that can be received by the "
             "Sentry client in bytes. Must match the value of the "
             "sentry.policy.client.thrift.max.message.size option in the "
             "Sentry server configuration.");
TAG_FLAG(sentry_service_max_message_size_bytes, advanced);
TAG_FLAG(sentry_service_max_message_size_bytes, experimental);

using strings::Substitute;

namespace kudu {

using sentry::SentryAction;
using sentry::SentryClient;

namespace master {

// Validates the sentry_service_rpc_addresses gflag.
static bool ValidateAddresses(const char* flag_name, const string& addresses) {
  vector<HostPort> host_ports;
  Status s = HostPort::ParseStringsWithScheme(addresses,
                                              SentryClient::kDefaultSentryPort,
                                              &host_ports);
  if (!s.ok()) {
    LOG(ERROR) << "invalid flag " << flag_name << ": " << s.ToString();
  }
  return s.ok();
}
DEFINE_validator(sentry_service_rpc_addresses, &ValidateAddresses);

SentryAuthzProvider::~SentryAuthzProvider() {
  Stop();
}

Status SentryAuthzProvider::Start() {
  vector<HostPort> addresses;
  RETURN_NOT_OK(HostPort::ParseStringsWithScheme(FLAGS_sentry_service_rpc_addresses,
                                                 SentryClient::kDefaultSentryPort,
                                                 &addresses));

  thrift::ClientOptions options;
  options.enable_kerberos = boost::iequals(FLAGS_sentry_service_security_mode, "kerberos");
  options.service_principal = FLAGS_sentry_service_kerberos_principal;
  options.send_timeout = MonoDelta::FromSeconds(FLAGS_sentry_service_send_timeout_seconds);
  options.recv_timeout = MonoDelta::FromSeconds(FLAGS_sentry_service_recv_timeout_seconds);
  options.conn_timeout = MonoDelta::FromSeconds(FLAGS_sentry_service_conn_timeout_seconds);
  options.max_buf_size = FLAGS_sentry_service_max_message_size_bytes;
  options.retry_count = FLAGS_sentry_service_retry_count;
  return ha_client_.Start(std::move(addresses), std::move(options));
}

void SentryAuthzProvider::Stop() {
  ha_client_.Stop();
}

namespace {

// Returns an authorizable based on the table name and the given scope.
Status GetAuthorizable(const string& table_name,
                       SentryAuthzProvider::AuthorizableScope scope,
                       TSentryAuthorizable* authorizable) {
  Slice database;
  Slice table;
  switch (scope) {
    case SentryAuthzProvider::AuthorizableScope::TABLE:
      RETURN_NOT_OK(ParseHiveTableIdentifier(table_name, &database, &table));
      authorizable->__set_table(table.ToString());
      FALLTHROUGH_INTENDED;
    case SentryAuthzProvider::AuthorizableScope::DATABASE:
      if (database.empty() && table.empty()) {
        RETURN_NOT_OK(ParseHiveTableIdentifier(table_name, &database, &table));
      }
      authorizable->__set_db(database.ToString());
      FALLTHROUGH_INTENDED;
    case SentryAuthzProvider::AuthorizableScope::SERVER:
      authorizable->__set_server(FLAGS_server_name);
      break;
    default:
      LOG(FATAL) << "unsupported authorizable scope";
      break;
  }

  return Status::OK();
}

} // anonymous namespace

Status SentryAuthzProvider::AuthorizeCreateTable(const string& table_name,
                                                 const string& user,
                                                 const string& owner) {
  // If the table is being created with a different owner than the user,
  // then the creating user must have 'ALL ON DATABASE' with grant. See
  // design doc in [SENTRY-2151](https://issues.apache.org/jira/browse/SENTRY-2151).
  //
  // Otherwise, table creation requires 'CREATE ON DATABASE' privilege.
  TSentryAuthorizable authorizable;
  RETURN_NOT_OK(GetAuthorizable(table_name, AuthorizableScope::DATABASE, &authorizable));
  SentryAction action;
  bool grant_option;
  if (user == owner) {
    action = SentryAction(SentryAction::Action::CREATE);
    grant_option = false;
  } else {
    action = SentryAction(SentryAction::Action::ALL);
    grant_option = true;
  }
  return Authorize(authorizable, action, user, grant_option);
}

Status SentryAuthzProvider::AuthorizeDropTable(const string& table_name,
                                               const string& user) {
  // Table deletion requires 'DROP ON TABLE' privilege.
  TSentryAuthorizable authorizable;
  RETURN_NOT_OK(GetAuthorizable(table_name, AuthorizableScope::TABLE, &authorizable));
  SentryAction action = SentryAction(SentryAction::Action::DROP);
  return Authorize(authorizable, action, user);
}

Status SentryAuthzProvider::AuthorizeAlterTable(const string& old_table,
                                                const string& new_table,
                                                const string& user) {
  // For table alteration (without table rename) requires 'ALTER ON TABLE'
  // privilege;
  // For table alteration (with table rename) requires
  //  1. 'ALL ON TABLE <old-table>',
  //  2. 'CREATE ON DATABASE <new-database>'.
  // See [SENTRY-2264](https://issues.apache.org/jira/browse/SENTRY-2264).
  // TODO(hao): add inline hierarchy validation to avoid multiple RPCs.
  TSentryAuthorizable table_authorizable;
  RETURN_NOT_OK(GetAuthorizable(old_table, AuthorizableScope::TABLE, &table_authorizable));
  if (old_table == new_table) {
    SentryAction action = SentryAction(SentryAction::Action::ALTER);
    return Authorize(table_authorizable, action, user);
  }

  SentryAction table_action = SentryAction(SentryAction::Action::ALL);
  RETURN_NOT_OK(Authorize(table_authorizable, table_action, user));
  TSentryAuthorizable db_authorizable;
  RETURN_NOT_OK(GetAuthorizable(new_table, AuthorizableScope::DATABASE, &db_authorizable));
  SentryAction db_action = SentryAction(SentryAction::Action::CREATE);
  return Authorize(db_authorizable, db_action, user);
}

Status SentryAuthzProvider::AuthorizeGetTableMetadata(const std::string& table_name,
                                                      const std::string& user) {
  // Retrieving table metadata requires 'METADATA ON TABLE' privilege.
  TSentryAuthorizable authorizable;
  RETURN_NOT_OK(GetAuthorizable(table_name, AuthorizableScope::TABLE, &authorizable));
  SentryAction action = SentryAction(SentryAction::Action::METADATA);
  return Authorize(authorizable, action, user);
}

Status SentryAuthzProvider::Authorize(const TSentryAuthorizable& authorizable,
                                      const SentryAction& action,
                                      const string& user,
                                      bool grant_option) {

  // In general, a privilege implies another when the authorizable from
  // the former implies the authorizable from the latter, the action
  // from the former implies the action from the latter, and grant option
  // from the former implies the grant option from the latter.
  //
  // ListPrivilegesByUser returns all privileges granted to the user that
  // imply the given authorizable. Therefore, we only need to validate if
  // the granted actions can imply the required one.

  TListSentryPrivilegesRequest request;
  request.__set_requestorUserName(FLAGS_kudu_service_name);
  request.__set_principalName(user);
  request.__set_authorizableHierarchy(authorizable);
  TListSentryPrivilegesResponse response;

  RETURN_NOT_OK(ha_client_.Execute(
      [&] (SentryClient* client) {
        return client->ListPrivilegesByUser(request, &response);
      }));

  for (const auto& privilege : response.privileges) {
    // A grant option cannot imply the other if the former is set
    // but the latter is not.
    if (grant_option && privilege.grantOption != TSentryGrantOption::TRUE) {
      continue;
    }

    SentryAction granted_action;
    Status s = SentryAction::FromString(privilege.action, &granted_action);
    WARN_NOT_OK(s, Substitute("failed to construct sentry action from $0", privilege.action));
    if (s.ok() && granted_action.Implies(action)) {
      return Status::OK();
    }
  }

  // Logs an error if the action is not authorized for debugging purpose, and
  // only returns generic error back to the users to avoid side channel leak,
  // e.g. 'whether table A exists'.
  LOG(ERROR) << "Action <" << action.action() << "> on authorizable <"
             << authorizable << "> is not permitted for user <" << user << ">";
  return Status::NotAuthorized("unauthorized action");
}

} // namespace master
} // namespace kudu
