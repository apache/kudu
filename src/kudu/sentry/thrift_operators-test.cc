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

#include <set>
#include <string>
#include <vector>

#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/util/test_macros.h"

using std::set;
using std::string;
using std::vector;

namespace sentry {

template<typename T>
void AssertCompareRequirements(const T& a, const T& b) {
  // Values must not be less than themselves.
  ASSERT_FALSE(a < a) << a;
  ASSERT_FALSE(b < b) << b;

  // Two values may not be simultaneously less than each other.
  if (a < b) {
    ASSERT_FALSE(b < a);
  }
}

// Asserts the contains orderings are the same.
template<typename T>
void AssertContainersOrdered(const vector<T>& ordered_vec_t, const set<T>& set_t) {
  ASSERT_EQ(ordered_vec_t.size(), set_t.size());
  int i = 0;
  for (const auto& t : set_t) {
    ASSERT_EQ(ordered_vec_t[i++], t);
  }
}

TEST(ThriftOperatorsTest, TestRoleOperatorLt) {
  // TSentryRole::operator<
  TSentryRole role_a;
  role_a.__set_roleName("a");

  TSentryRole role_b;
  role_b.__set_roleName("b");

  TSentryRole role_without_name;
  role_without_name.__set_grantorPrincipal("grantor");

  NO_FATALS(AssertCompareRequirements(role_a, role_b));
  NO_FATALS(AssertCompareRequirements(role_a, role_without_name));
  vector<TSentryRole> ordered_roles { role_without_name, role_a, role_b };
  set<TSentryRole> roles(ordered_roles.begin(), ordered_roles.end());
  NO_FATALS(AssertContainersOrdered(ordered_roles, roles));
}

TEST(ThriftOperatorsTest, TestGroupOperatorLt) {
  // TSentryGroup::operator<
  TSentryGroup group_a;
  group_a.__set_groupName("a");

  TSentryGroup group_b;
  group_b.__set_groupName("b");

  NO_FATALS(AssertCompareRequirements(group_a, group_b));
  vector<TSentryGroup> ordered_groups { group_a, group_b };
  set<TSentryGroup> groups(ordered_groups.begin(), ordered_groups.end());
  NO_FATALS(AssertContainersOrdered(ordered_groups, groups));
}

TEST(ThriftOperatorsTest, TestPrivilegeOperatorLt) {
  // TSentryPrivilege::operator<
  const string kServer = "server1";
  const string kDatabase = "db1";
  const string kTable = "tbl1";

  TSentryPrivilege db_priv;
  db_priv.__set_serverName(kServer);
  db_priv.__set_dbName(kDatabase);

  TSentryPrivilege tbl1_priv;
  tbl1_priv.__set_serverName(kServer);
  tbl1_priv.__set_dbName(kDatabase);
  tbl1_priv.__set_tableName(kTable);

  TSentryPrivilege tbl1_priv_no_db;
  tbl1_priv_no_db.__set_serverName(kServer);
  tbl1_priv_no_db.__set_tableName(kTable);

  TSentryPrivilege tbl2_priv;
  tbl2_priv.__set_serverName(kServer);
  tbl2_priv.__set_dbName(kDatabase);
  tbl2_priv.__set_tableName("tbl2");

  NO_FATALS(AssertCompareRequirements(db_priv, tbl1_priv));
  NO_FATALS(AssertCompareRequirements(db_priv, tbl2_priv));
  NO_FATALS(AssertCompareRequirements(db_priv, tbl1_priv_no_db));
  NO_FATALS(AssertCompareRequirements(tbl1_priv, tbl2_priv));
  vector<TSentryPrivilege> ordered_privileges { tbl1_priv_no_db, db_priv, tbl1_priv, tbl2_priv };
  set<TSentryPrivilege> privileges(ordered_privileges.begin(), ordered_privileges.end());
  NO_FATALS(AssertContainersOrdered(ordered_privileges, privileges));
}

TEST(ThriftOperatorsTest, TestAuthorizableOperatorLt) {
  // TSentryAuthorizable::operator<
  const string kServer = "server1";
  const string kDatabase = "db1";
  TSentryAuthorizable db_authorizable;
  db_authorizable.__set_server(kServer);
  db_authorizable.__set_db(kDatabase);

  TSentryAuthorizable tbl1_authorizable;
  tbl1_authorizable.__set_server(kServer);
  tbl1_authorizable.__set_db(kDatabase);
  tbl1_authorizable.__set_table("tbl1");

  TSentryAuthorizable tbl2_authorizable;
  tbl2_authorizable.__set_server(kServer);
  tbl2_authorizable.__set_db(kDatabase);
  tbl2_authorizable.__set_table("tbl2");

  TSentryAuthorizable server_authorizable;
  server_authorizable.__set_server("server2");

  TSentryAuthorizable uri_authorizable;
  uri_authorizable.__set_server(kServer);
  uri_authorizable.__set_uri("http://uri");

  NO_FATALS(AssertCompareRequirements(server_authorizable, db_authorizable));
  NO_FATALS(AssertCompareRequirements(uri_authorizable, db_authorizable));
  NO_FATALS(AssertCompareRequirements(db_authorizable, tbl1_authorizable));
  NO_FATALS(AssertCompareRequirements(db_authorizable, tbl2_authorizable));
  NO_FATALS(AssertCompareRequirements(tbl1_authorizable, tbl2_authorizable));
  vector<TSentryAuthorizable> ordered_authorizables {
      db_authorizable,
      tbl1_authorizable,
      tbl2_authorizable,
      uri_authorizable,
      server_authorizable,
  };
  set<TSentryAuthorizable> authorizables(
      ordered_authorizables.begin(), ordered_authorizables.end());
  ASSERT_EQ(5, authorizables.size()) << authorizables;
  NO_FATALS(AssertContainersOrdered(ordered_authorizables, authorizables));
}

} // namespace sentry
