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

#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/util/test_macros.h"

using std::set;
using std::string;

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

TEST(ThriftOperatorsTest, TestRoleOperatorLt) {
  // TSentryRole::operator<
  TSentryRole role_a;
  role_a.__set_roleName("a");

  TSentryRole role_b;
  role_b.__set_roleName("b");

  TSentryRole role_c;
  role_c.__set_grantorPrincipal("c");

  NO_FATALS(AssertCompareRequirements(role_a, role_b));
  NO_FATALS(AssertCompareRequirements(role_a, role_c));
  set<TSentryRole> roles { role_a, role_b, role_c};
  ASSERT_EQ(3, roles.size()) << roles;
}

TEST(ThriftOperatorsTest, TestGroupOperatorLt) {
  // TSentryGroup::operator<
  TSentryGroup group_a;
  group_a.__set_groupName("a");

  TSentryGroup group_b;
  group_b.__set_groupName("b");

  NO_FATALS(AssertCompareRequirements(group_a, group_b));
  set<TSentryGroup> groups { group_a, group_b };
  ASSERT_EQ(2, groups.size()) << groups;
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
  set<TSentryPrivilege> privileges { db_priv, tbl1_priv, tbl2_priv, tbl1_priv_no_db };
  ASSERT_EQ(4, privileges.size()) << privileges;
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
  set<TSentryAuthorizable> authorizables {
      server_authorizable,
      uri_authorizable,
      db_authorizable,
      tbl1_authorizable,
      tbl2_authorizable
  };
  ASSERT_EQ(5, authorizables.size()) << authorizables;
}

} // namespace sentry
