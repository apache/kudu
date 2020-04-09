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
#include <vector>

#include <boost/functional/hash/hash.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <google/protobuf/any.pb.h>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/ranger/mini_ranger.h"
#include "kudu/ranger/ranger.pb.h"
#include "kudu/subprocess/server.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_string(log_dir);
DECLARE_string(ranger_config_path);
DECLARE_string(ranger_log_config_dir);
DECLARE_string(ranger_log_level);
DECLARE_bool(ranger_logtostdout);
DECLARE_bool(ranger_overwrite_log_config);

using boost::hash_combine;
using kudu::env_util::ListFilesInDir;
using kudu::subprocess::SubprocessMetrics;
using kudu::subprocess::SubprocessRequestPB;
using kudu::subprocess::SubprocessResponsePB;
using kudu::subprocess::SubprocessServer;
using std::move;
using std::string;
using std::unordered_set;
using std::vector;
using strings::SkipEmpty;
using strings::Split;
using strings::Substitute;

namespace kudu {
namespace ranger {

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
  bool authorized;
  ASSERT_OK(client_.AuthorizeAction("jdoe", ActionPB::CREATE, "bar", "baz", &authorized));
  ASSERT_FALSE(authorized);
}

TEST_F(RangerClientTest, TestAuthorizeCreateTableAuthorized) {
  Allow("jdoe", ActionPB::CREATE, "foo", "bar");
  bool authorized;
  ASSERT_OK(client_.AuthorizeAction("jdoe", ActionPB::CREATE, "foo", "bar", &authorized));
  ASSERT_TRUE(authorized);
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
                                                   "default", "foobar", &columns));
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
                                                   "default", "foobar", &columns));
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
  ASSERT_OK(client_.AuthorizeActionMultipleColumns("jdoe", ActionPB::SELECT,
                                                   "default", "foobar", &columns));
  ASSERT_EQ(0, columns.size());
}

TEST_F(RangerClientTest, TestAuthorizeActionsNoneAuthorized) {
  unordered_set<ActionPB, ActionHash> actions;
  actions.emplace(ActionPB::DROP);
  actions.emplace(ActionPB::SELECT);
  actions.emplace(ActionPB::INSERT);
  ASSERT_OK(client_.AuthorizeActions("jdoe", "default", "foobar", &actions));
  ASSERT_EQ(0, actions.size());
}

TEST_F(RangerClientTest, TestAuthorizeActionsSomeAuthorized) {
  Allow("jdoe", ActionPB::SELECT, "default", "foobar");
  unordered_set<ActionPB, ActionHash> actions;
  actions.emplace(ActionPB::DROP);
  actions.emplace(ActionPB::SELECT);
  actions.emplace(ActionPB::INSERT);
  ASSERT_OK(client_.AuthorizeActions("jdoe", "default", "foobar", &actions));
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
  ASSERT_OK(client_.AuthorizeActions("jdoe", "default", "foobar", &actions));
  ASSERT_EQ(3, actions.size());
}

class RangerClientTestBase : public KuduTest {
 public:
  RangerClientTestBase()
      : test_dir_(GetTestDataDirectory()) {}

  void SetUp() override {
    metric_entity_ = METRIC_ENTITY_server.Instantiate(&metric_registry_, "ranger_client-test");
    FLAGS_ranger_log_level = "debug";
    FLAGS_ranger_logtostdout = true;
    FLAGS_ranger_config_path = test_dir_;
    FLAGS_ranger_log_config_dir = JoinPathSegments(test_dir_, "log_conf");
    FLAGS_log_dir = JoinPathSegments(test_dir_, "logs");
    ASSERT_OK(env_->CreateDir(FLAGS_log_dir));
    ASSERT_OK(InitializeRanger());
  }

  Status InitializeRanger() {
    ranger_.reset(new MiniRanger("127.0.0.1"));
    RETURN_NOT_OK(ranger_->Start());
    RETURN_NOT_OK(ranger_->CreateClientConfig(test_dir_));
    client_.reset(new RangerClient(env_, metric_entity_));
    return client_->Start();
  }

 protected:
  const string test_dir_;
  MetricRegistry metric_registry_;

  scoped_refptr<MetricEntity> metric_entity_;
  std::unique_ptr<MiniRanger> ranger_;
  std::unique_ptr<RangerClient> client_;
};

namespace {

// Looks in --log_dir and returns the full Kudu Ranger subprocess log filename
// and the lines contained therein.
Status GetLinesFromLogFile(Env* env, string* file, vector<string>* lines) {
  vector<string> matches;
  RETURN_NOT_OK(env->Glob(JoinPathSegments(FLAGS_log_dir, "kudu-ranger-subprocess*.log"),
                                           &matches));
  if (matches.size() != 1) {
    return Status::Corruption(Substitute("Expected one file but got $0: $1",
                                         matches.size(), JoinStrings(matches, ", ")));
  }
  faststring contents;
  const string& full_log_file = matches[0];
  RETURN_NOT_OK(ReadFileToString(env, full_log_file, &contents));
  *file = full_log_file;
  *lines = Split(contents.ToString(), "\n", SkipEmpty());
  return Status::OK();
}

} // anonymous namespace

TEST_F(RangerClientTestBase, TestLogging) {
  {
    // Check that a logging configuration was produced by the Ranger client.
    vector<string> files;
    ASSERT_OK(ListFilesInDir(env_, FLAGS_ranger_log_config_dir, &files));
    SCOPED_TRACE(JoinStrings(files, "\n"));
    ASSERT_STRINGS_ANY_MATCH(files, ".*log4j2.properties");
  }
  // Make a request. It doesn't matter whether it succeeds or not -- debug logs
  // should include info about each request.
  bool authorized;
  ASSERT_OK(client_->AuthorizeAction("user", ActionPB::ALL, "db", "table", &authorized));
  ASSERT_FALSE(authorized);
  {
    // Check that the Ranger client logs some DEBUG messages.
    vector<string> lines;
    string log_file;
    ASSERT_OK(GetLinesFromLogFile(env_, &log_file, &lines));
    ASSERT_STRINGS_ANY_MATCH(lines, ".*DEBUG.*");
    // Delete the log file so we can start a fresh log.
    ASSERT_OK(env_->DeleteFile(log_file));
  }
  // Try reconfiguring our logging to spit out info-level logs and start a new
  // log file by restarting the Ranger client. Since we're set up to not
  // overwrite the logging config, this won't be effective -- we should still
  // see DEBUG messages.
  FLAGS_ranger_log_level = "info";
  FLAGS_ranger_overwrite_log_config = false;
  client_.reset(new RangerClient(env_, metric_entity_));
  ASSERT_OK(client_->Start());
  ASSERT_OK(client_->AuthorizeAction("user", ActionPB::ALL, "db", "table", &authorized));
  ASSERT_FALSE(authorized);
  {
    // Our logs should still contain DEBUG messages since we didn't update the
    // logging configuration.
    vector<string> lines;
    string log_file;
    ASSERT_OK(GetLinesFromLogFile(env_, &log_file, &lines));
    ASSERT_STRINGS_ANY_MATCH(lines, ".*DEBUG.*");
    // Delete the log file so we can start a fresh log.
    ASSERT_OK(env_->DeleteFile(log_file));
  }
  // Now try again but this time overwrite the logging configuration.
  FLAGS_ranger_overwrite_log_config = true;
  client_.reset(new RangerClient(env_, metric_entity_));
  ASSERT_OK(client_->Start());
  ASSERT_OK(client_->AuthorizeAction("user", ActionPB::ALL, "db", "table", &authorized));
  ASSERT_FALSE(authorized);
  {
    // We shouldn't see any DEBUG messages since the client is configured to
    // use INFO-level logging.
    vector<string> lines;
    string unused;
    ASSERT_OK(GetLinesFromLogFile(env_, &unused, &lines));
    // We should see no DEBUG messages.
    for (const auto& l : lines) {
      ASSERT_STR_NOT_CONTAINS(l, "DEBUG");
    }
  }
}

} // namespace ranger
} // namespace kudu
