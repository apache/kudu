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
#include "kudu/util/test_util.h"

using std::string;
using std::set;

namespace kudu {
namespace sentry {

class ThriftOperatorsTest : public KuduTest {
};

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

TEST_F(ThriftOperatorsTest, TestOperatorLt) {
  // TSentryRole::operator<
  ::sentry::TSentryRole role_a;
  role_a.__set_roleName("a");

  ::sentry::TSentryRole role_b;
  role_b.__set_roleName("b");

  NO_FATALS(AssertCompareRequirements(role_a, role_b));
  set<::sentry::TSentryRole> roles { role_a, role_b };
  ASSERT_EQ(2, roles.size()) << roles;

  // TSentryGroup::operator<
  ::sentry::TSentryGroup group_a;
  group_a.__set_groupName("a");

  ::sentry::TSentryGroup group_b;
  group_b.__set_groupName("b");

  NO_FATALS(AssertCompareRequirements(group_a, group_b));
  set<::sentry::TSentryGroup> groups { group_a, group_b };
  ASSERT_EQ(2, groups.size()) << groups;

  // TSentryPrivilege::operator<
  ::sentry::TSentryPrivilege db_priv;
  db_priv.__set_serverName("server1");
  db_priv.__set_dbName("db1");

  ::sentry::TSentryPrivilege tbl_priv;
  tbl_priv.__set_serverName("server1");
  tbl_priv.__set_dbName("db1");
  tbl_priv.__set_tableName("tbl1");

  NO_FATALS(AssertCompareRequirements(db_priv, tbl_priv));
  set<::sentry::TSentryPrivilege> privileges { db_priv, tbl_priv };
  ASSERT_EQ(2, privileges.size()) << privileges;


  // TSentryAuthorizable::operator<
  ::sentry::TSentryAuthorizable db_authorizable;
  db_authorizable.__set_server("server1");
  db_authorizable.__set_db("db1");

  ::sentry::TSentryAuthorizable tbl_authorizable;
  tbl_authorizable.__set_server("server1");
  tbl_authorizable.__set_db("db1");
  tbl_authorizable.__set_table("tbl1");

  NO_FATALS(AssertCompareRequirements(db_authorizable, tbl_authorizable));
  set<::sentry::TSentryAuthorizable> authorizables { db_authorizable, tbl_authorizable };
  ASSERT_EQ(2, authorizables.size()) << authorizables;
}
} // namespace sentry
} // namespace kudu
