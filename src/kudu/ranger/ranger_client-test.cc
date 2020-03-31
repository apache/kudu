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

#include "kudu/ranger/ranger_client.h"

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include <boost/functional/hash/hash.hpp>
#include <glog/logging.h>
#include <google/protobuf/any.pb.h>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/ranger/ranger.pb.h"
#include "kudu/subprocess/server.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace ranger {

using boost::hash_combine;
using kudu::subprocess::SubprocessMetrics;
using kudu::subprocess::SubprocessRequestPB;
using kudu::subprocess::SubprocessResponsePB;
using kudu::subprocess::SubprocessServer;
using std::move;
using std::string;
using std::unordered_set;
using strings::Substitute;

struct AuthorizedAction {
  string user_name;
  ActionPB action;
  string database_name;
  string table_name;
  string column_name;

  bool operator==(const AuthorizedAction& rhs) const {
    return user_name == rhs.user_name &&
      action == rhs.action &&
      database_name == rhs.database_name &&
      table_name == rhs.table_name &&
      column_name == rhs.column_name;
  }
};

struct AuthorizedActionHash {
 public:
  size_t operator()(const AuthorizedAction& action) const {
    size_t seed = 0;
    hash_combine(seed, action.action);
    hash_combine(seed, action.column_name);
    hash_combine(seed, action.database_name);
    hash_combine(seed, action.table_name);
    hash_combine(seed, action.user_name);

    return seed;
  }
};

class MockSubprocessServer : public SubprocessServer {
 public:
  unordered_set<AuthorizedAction, AuthorizedActionHash> next_response_;

  Status Init() override {
    // don't want to start anything
    return Status::OK();
  }

  ~MockSubprocessServer() override {}

  MockSubprocessServer()
      : SubprocessServer(Env::Default(), "", {"mock"}, SubprocessMetrics()) {}

  Status Execute(SubprocessRequestPB* req,
                 SubprocessResponsePB* resp) override {
    RangerRequestListPB req_list;
    CHECK(req->request().UnpackTo(&req_list));

    RangerResponseListPB resp_list;

    for (const RangerRequestPB& req : req_list.requests()) {
      AuthorizedAction action;
      action.user_name = req_list.user();
      action.action = req.action();
      action.table_name = req.table();
      action.database_name = req.database();
      action.column_name = req.has_column() ? req.column() : "";

      auto r = resp_list.add_responses();
      r->set_allowed(ContainsKey(next_response_, action));
    }

    resp->mutable_response()->PackFrom(resp_list);

    return Status::OK();
  }
};

class RangerClientTest : public KuduTest {
 public:
  RangerClientTest() :
    client_(env_, METRIC_ENTITY_server.Instantiate(&metric_registry_, "ranger_client-test")) {}

  void SetUp() override {
    std::unique_ptr<MockSubprocessServer> server(new MockSubprocessServer());
    next_response_ = &server->next_response_;
    client_.ReplaceServerForTests(std::move(server));
  }

  void Allow(string user_name, ActionPB action, string database_name,
             string table_name, string column_name = "") {
    AuthorizedAction authorized_action;
    authorized_action.user_name = move(user_name);
    authorized_action.action = action;
    authorized_action.database_name = move(database_name);
    authorized_action.table_name = move(table_name);
    authorized_action.column_name = move(column_name);

    next_response_->emplace(authorized_action);
  }

 protected:
  MetricRegistry metric_registry_;
  unordered_set<AuthorizedAction, AuthorizedActionHash>* next_response_;
  RangerClient client_;
};

TEST_F(RangerClientTest, TestAuthorizeCreateTableUnauthorized) {
  auto s = client_.AuthorizeAction("jdoe", ActionPB::CREATE, "bar.baz");
  ASSERT_TRUE(s.IsNotAuthorized());
}

TEST_F(RangerClientTest, TestAuthorizeCreateTableAuthorized) {
  Allow("jdoe", ActionPB::CREATE, "foo", "bar");
  ASSERT_OK(client_.AuthorizeAction("jdoe", ActionPB::CREATE, "foo.bar"));
}

TEST_F(RangerClientTest, TestAuthorizeListNoTables) {
  unordered_set<string> tables;
  ASSERT_OK(client_.AuthorizeActionMultipleTables("jdoe", ActionPB::METADATA, &tables));
  ASSERT_EQ(0, tables.size());
}

TEST_F(RangerClientTest, TestAuthorizeListNoTablesAuthorized) {
  unordered_set<string> tables;
  tables.emplace("foo.bar");
  tables.emplace("foo.baz");
  ASSERT_OK(client_.AuthorizeActionMultipleTables("jdoe", ActionPB::METADATA, &tables));
  ASSERT_EQ(0, tables.size());
}

TEST_F(RangerClientTest, TestAuthorizeMetadataSubsetOfTablesAuthorized) {
  Allow("jdoe", ActionPB::METADATA, "default", "foobar");
  unordered_set<string> tables;
  tables.emplace("default.foobar");
  tables.emplace("barbaz");
  ASSERT_OK(client_.AuthorizeActionMultipleTables("jdoe", ActionPB::METADATA, &tables));
  ASSERT_EQ(1, tables.size());
  ASSERT_EQ("default.foobar", *tables.begin());
}

TEST_F(RangerClientTest, TestAuthorizeMetadataAllAuthorized) {
  Allow("jdoe", ActionPB::METADATA, "default", "foobar");
  Allow("jdoe", ActionPB::METADATA, "default", "barbaz");
  unordered_set<string> tables;
  tables.emplace("default.foobar");
  tables.emplace("barbaz");
  ASSERT_OK(client_.AuthorizeActionMultipleTables("jdoe", ActionPB::METADATA, &tables));
  ASSERT_EQ(2, tables.size());
  ASSERT_TRUE(ContainsKey(tables, "default.foobar"));
  ASSERT_TRUE(ContainsKey(tables, "barbaz"));
}

TEST_F(RangerClientTest, TestAuthorizeMetadataAllNonRanger) {
  unordered_set<string> tables;
  tables.emplace("foo.");
  tables.emplace(".bar");
  ASSERT_OK(client_.AuthorizeActionMultipleTables("jdoe", ActionPB::METADATA, &tables));
  ASSERT_EQ(0, tables.size());
}

TEST_F(RangerClientTest, TestAuthorizeMetadataNoneAuthorizedContainsNonRanger) {
  unordered_set<string> tables;
  tables.emplace("foo.");
  tables.emplace(".bar");
  tables.emplace("foo.bar");
  tables.emplace("foo.baz");
  ASSERT_OK(client_.AuthorizeActionMultipleTables("jdoe", ActionPB::METADATA, &tables));
  ASSERT_EQ(0, tables.size());
}

TEST_F(RangerClientTest, TestAuthorizeMetadataAllAuthorizedContainsNonRanger) {
  Allow("jdoe", ActionPB::METADATA, "default", "foobar");
  Allow("jdoe", ActionPB::METADATA, "default", "barbaz");
  unordered_set<string> tables;
  tables.emplace("default.foobar");
  tables.emplace("barbaz");
  tables.emplace("foo.");
  ASSERT_OK(client_.AuthorizeActionMultipleTables("jdoe", ActionPB::METADATA, &tables));
  ASSERT_EQ(2, tables.size());
  ASSERT_TRUE(ContainsKey(tables, "default.foobar"));
  ASSERT_TRUE(ContainsKey(tables, "barbaz"));
  ASSERT_FALSE(ContainsKey(tables, "foo."));
}

TEST_F(RangerClientTest, TestAuthorizeScanSubsetAuthorized) {
  Allow("jdoe", ActionPB::SELECT, "default", "foobar", "col1");
  Allow("jdoe", ActionPB::SELECT, "default", "foobar", "col3");
  unordered_set<string> columns;
  columns.emplace("col1");
  columns.emplace("col2");
  columns.emplace("col3");
  columns.emplace("col4");
  ASSERT_OK(client_.AuthorizeActionMultipleColumns("jdoe", ActionPB::SELECT,
                                                   "default.foobar", &columns));
  ASSERT_EQ(2, columns.size());
  ASSERT_TRUE(ContainsKey(columns, "col1"));
  ASSERT_TRUE(ContainsKey(columns, "col3"));
  ASSERT_FALSE(ContainsKey(columns, "col2"));
  ASSERT_FALSE(ContainsKey(columns, "col4"));
}

TEST_F(RangerClientTest, TestAuthorizeScanAllColumnsAuthorized) {
  Allow("jdoe", ActionPB::SELECT, "default", "foobar", "col1");
  Allow("jdoe", ActionPB::SELECT, "default", "foobar", "col2");
  Allow("jdoe", ActionPB::SELECT, "default", "foobar", "col3");
  Allow("jdoe", ActionPB::SELECT, "default", "foobar", "col4");
  unordered_set<string> columns;
  columns.emplace("col1");
  columns.emplace("col2");
  columns.emplace("col3");
  columns.emplace("col4");
  ASSERT_OK(client_.AuthorizeActionMultipleColumns("jdoe", ActionPB::SELECT,
                                                   "default.foobar", &columns));
  ASSERT_EQ(4, columns.size());
  ASSERT_TRUE(ContainsKey(columns, "col1"));
  ASSERT_TRUE(ContainsKey(columns, "col2"));
  ASSERT_TRUE(ContainsKey(columns, "col3"));
  ASSERT_TRUE(ContainsKey(columns, "col4"));
}

TEST_F(RangerClientTest, TestAuthorizeScanNoColumnsAuthorized) {
  unordered_set<string> columns;
  for (int i = 0; i < 4; ++i) {
    columns.emplace(Substitute("col$0", i));
  }
  auto s = client_.AuthorizeActionMultipleColumns("jdoe", ActionPB::SELECT,
                                                  "default.foobar", &columns);
  ASSERT_TRUE(s.IsNotAuthorized());
  ASSERT_EQ(4, columns.size());
}

TEST_F(RangerClientTest, TestAuthorizeActionsNoneAuthorized) {
  unordered_set<ActionPB, ActionHash> actions;
  actions.emplace(ActionPB::DROP);
  actions.emplace(ActionPB::SELECT);
  actions.emplace(ActionPB::INSERT);
  auto s = client_.AuthorizeActions("jdoe", "default.foobar", &actions);
  ASSERT_TRUE(s.IsNotAuthorized());
  ASSERT_EQ(3, actions.size());
}

TEST_F(RangerClientTest, TestAuthorizeActionsSomeAuthorized) {
  Allow("jdoe", ActionPB::SELECT, "default", "foobar");
  unordered_set<ActionPB, ActionHash> actions;
  actions.emplace(ActionPB::DROP);
  actions.emplace(ActionPB::SELECT);
  actions.emplace(ActionPB::INSERT);
  ASSERT_OK(client_.AuthorizeActions("jdoe", "default.foobar", &actions));
  ASSERT_EQ(1, actions.size());
  ASSERT_TRUE(ContainsKey(actions, ActionPB::SELECT));
}

TEST_F(RangerClientTest, TestAuthorizeActionsAllAuthorized) {
  Allow("jdoe", ActionPB::DROP, "default", "foobar");
  Allow("jdoe", ActionPB::SELECT, "default", "foobar");
  Allow("jdoe", ActionPB::INSERT, "default", "foobar");
  unordered_set<ActionPB, ActionHash> actions;
  actions.emplace(ActionPB::DROP);
  actions.emplace(ActionPB::SELECT);
  actions.emplace(ActionPB::INSERT);
  ASSERT_OK(client_.AuthorizeActions("jdoe", "default.foobar", &actions));
  ASSERT_EQ(3, actions.size());
}

} // namespace ranger
} // namespace kudu
