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

#include <cstdint>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/table_util.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/ranger/ranger.pb.h"
#include "kudu/security/init.h"
#include "kudu/subprocess/server.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"

DEFINE_string(ranger_java_path, "java",
              "The path where the Java binary was installed. If "
              "the value isn't an absolute path, it will be evaluated "
              "using the Kudu user's PATH.");
TAG_FLAG(ranger_java_path, experimental);

DEFINE_string(ranger_config_path, "",
              "Path to directory containing Ranger client configuration. "
              "Enables Ranger authorization provider. "
              "sentry_service_rpc_addresses must not be set if this is "
              "enabled.");
TAG_FLAG(ranger_config_path, experimental);

DEFINE_string(ranger_jar_path, "",
              "Path to the JAR file containing the Ranger subprocess. "
              "If not set, the default JAR file path is expected to be"
              "next to the master binary.");
TAG_FLAG(ranger_jar_path, experimental);

DEFINE_string(ranger_receiver_fifo_dir, "",
              "Directory in which to create a fifo used to receive messages "
              "from the Ranger subprocess. Existing fifos at this path will be "
              "overwritten. If not specified, a fifo will be created in the "
              "--ranger_config_path directory.");
TAG_FLAG(ranger_receiver_fifo_dir, advanced);

METRIC_DEFINE_histogram(server, ranger_subprocess_execution_time_ms,
    "Ranger subprocess execution time (ms)",
    kudu::MetricUnit::kMilliseconds,
    "Duration of time in ms spent executing the Ranger subprocess request, excluding "
    "time spent spent in the subprocess queues",
    kudu::MetricLevel::kInfo,
    60000LU, 1);
METRIC_DEFINE_histogram(server, ranger_subprocess_inbound_queue_length,
    "Ranger subprocess inbound queue length",
    kudu::MetricUnit::kMessages,
    "Number of request messages in the Ranger subprocess' inbound request queue",
    kudu::MetricLevel::kInfo,
    1000, 1);
METRIC_DEFINE_histogram(server, ranger_subprocess_inbound_queue_time_ms,
    "Ranger subprocess inbound queue time (ms)",
    kudu::MetricUnit::kMilliseconds,
    "Duration of time in ms spent in the Ranger subprocess' inbound request queue",
    kudu::MetricLevel::kInfo,
    60000LU, 1);
METRIC_DEFINE_histogram(server, ranger_subprocess_outbound_queue_length,
    "Ranger subprocess outbound queue length",
    kudu::MetricUnit::kMessages,
    "Number of request messages in the Ranger subprocess' outbound response queue",
    kudu::MetricLevel::kInfo,
    1000, 1);
METRIC_DEFINE_histogram(server, ranger_subprocess_outbound_queue_time_ms,
    "Ranger subprocess outbound queue time (ms)",
    kudu::MetricUnit::kMilliseconds,
    "Duration of time in ms spent in the Ranger subprocess' outbound response queue",
    kudu::MetricLevel::kInfo,
    60000LU, 1);
METRIC_DEFINE_histogram(server, ranger_server_inbound_queue_size_bytes,
    "Ranger server inbound queue size (bytes)",
    kudu::MetricUnit::kBytes,
    "Number of bytes in the inbound response queue of the Ranger server, recorded "
    "at the time a new response is read from the pipe and added to the inbound queue",
    kudu::MetricLevel::kInfo,
    4 * 1024 * 1024, 1);
METRIC_DEFINE_histogram(server, ranger_server_inbound_queue_time_ms,
    "Ranger server inbound queue time (ms)",
    kudu::MetricUnit::kMilliseconds,
    "Duration of time in ms spent in the Ranger server's inbound response queue",
    kudu::MetricLevel::kInfo,
    60000LU, 1);
METRIC_DEFINE_histogram(server, ranger_server_outbound_queue_size_bytes,
    "Ranger server outbound queue size (bytes)",
    kudu::MetricUnit::kBytes,
    "Number of bytes in the outbound request queue of the Ranger server, recorded "
    "at the time a new request is added to the outbound request queue",
    kudu::MetricLevel::kInfo,
    4 * 1024 * 1024, 1);
METRIC_DEFINE_histogram(server, ranger_server_outbound_queue_time_ms,
    "Ranger server outbound queue time (ms)",
    kudu::MetricUnit::kMilliseconds,
    "Duration of time in ms spent in the Ranger server's outbound request queue",
    kudu::MetricLevel::kInfo,
    60000LU, 1);

DECLARE_string(keytab_file);
DECLARE_string(principal);

namespace kudu {
namespace ranger {

using kudu::subprocess::SubprocessMetrics;
using std::move;
using std::string;
using std::unordered_set;
using std::vector;
using strings::Substitute;

static const char* kUnauthorizedAction = "Unauthorized action";
static const char* kDenyNonRangerTableTemplate = "Denying action on table with invalid name $0. "
                                                 "Use 'kudu table rename_table' to rename it to "
                                                 "a Ranger-compatible name.";
static const char* kMainClass = "org.apache.kudu.subprocess.ranger.RangerSubprocessMain";

// Returns the path to the JAR file containing the Ranger subprocess.
static string GetRangerJarPath() {
  if (FLAGS_ranger_jar_path.empty()) {
    string exe;
    CHECK_OK(Env::Default()->GetExecutablePath(&exe));
    const string bin_dir = DirName(exe);
    return JoinPathSegments(bin_dir, "kudu-subprocess.jar");
  }
  return FLAGS_ranger_jar_path;
}

// Returns the classpath to be used for the Ranger subprocess.
static string GetJavaClasspath() {
  return Substitute("$0:$1", GetRangerJarPath(), FLAGS_ranger_config_path);
}


static string ranger_fifo_base() {
  DCHECK(!FLAGS_ranger_config_path.empty());
  const string& fifo_dir = FLAGS_ranger_receiver_fifo_dir.empty() ?
      FLAGS_ranger_config_path : FLAGS_ranger_receiver_fifo_dir;
  return JoinPathSegments(fifo_dir, "ranger_receiever_fifo");
}

// Builds the arguments to start the Ranger subprocess with the given receiver
// fifo path. Specifically pass the principal and keytab file that the Ranger
// subprocess will log in with if Kerberos is enabled. 'args' has the final
// arguments.  Returns 'OK' if arguments successfully created, error otherwise.
static Status BuildArgv(const string& fifo_path, vector<string>* argv) {
  DCHECK(argv);
  DCHECK(!FLAGS_ranger_config_path.empty());
  // Pass the required arguments to run the Ranger subprocess.
  vector<string> ret = { FLAGS_ranger_java_path, "-cp", GetJavaClasspath(), kMainClass };
  // When Kerberos is enabled in Kudu, pass both Kudu principal and keytab file
  // to the Ranger subprocess.
  if (!FLAGS_keytab_file.empty()) {
    string configured_principal;
    RETURN_NOT_OK_PREPEND(security::GetConfiguredPrincipal(FLAGS_principal, &configured_principal),
                          "unable to get the configured principal from for the Ranger subprocess");
    ret.emplace_back("-i");
    ret.emplace_back(std::move(configured_principal));
    ret.emplace_back("-k");
    ret.emplace_back(FLAGS_keytab_file);
  }
  ret.emplace_back("-o");
  ret.emplace_back(fifo_path);
  *argv = std::move(ret);
  return Status::OK();
}

static bool ValidateRangerConfiguration() {
  if (!FLAGS_ranger_config_path.empty()) {
    // First, check the specified path.
    if (!Env::Default()->FileExists(FLAGS_ranger_java_path)) {
      // Otherwise, since the specified path is not absolute, check if
      // the Java binary is on the PATH.
      string p;
      Status s = Subprocess::Call({ "which", FLAGS_ranger_java_path }, "", &p);
      if (!s.ok()) {
        LOG(ERROR) << Substitute("--ranger_java_path has invalid java binary path: $0",
                                 FLAGS_ranger_java_path);
        return false;
      }
    }
    string ranger_jar_path = GetRangerJarPath();
    if (!Env::Default()->FileExists(ranger_jar_path)) {
      LOG(ERROR) << Substitute("--ranger_jar_path has invalid JAR file path: $0",
                               ranger_jar_path);
      return false;
    }
  }
  return true;
}
GROUP_FLAG_VALIDATOR(ranger_config_flags, ValidateRangerConfiguration);

static const char* ScopeToString(RangerClient::Scope scope) {
  switch (scope) {
    case RangerClient::Scope::DATABASE: return "database";
    case RangerClient::Scope::TABLE: return "table";
  }
  LOG(FATAL) << static_cast<uint16_t>(scope) << ": unknown scope";
  __builtin_unreachable();
}

#define HISTINIT(member, x) member = METRIC_##x.Instantiate(entity)
RangerSubprocessMetrics::RangerSubprocessMetrics(const scoped_refptr<MetricEntity>& entity) {
  HISTINIT(sp_inbound_queue_length, ranger_subprocess_inbound_queue_length);
  HISTINIT(sp_inbound_queue_time_ms, ranger_subprocess_inbound_queue_time_ms);
  HISTINIT(sp_outbound_queue_length, ranger_subprocess_outbound_queue_length);
  HISTINIT(sp_outbound_queue_time_ms, ranger_subprocess_outbound_queue_time_ms);
  HISTINIT(sp_execution_time_ms, ranger_subprocess_execution_time_ms);
  HISTINIT(server_inbound_queue_size_bytes, ranger_server_inbound_queue_size_bytes);
  HISTINIT(server_inbound_queue_time_ms, ranger_server_inbound_queue_time_ms);
  HISTINIT(server_outbound_queue_size_bytes, ranger_server_outbound_queue_size_bytes);
  HISTINIT(server_outbound_queue_time_ms, ranger_server_outbound_queue_time_ms);
}
#undef HISTINIT

RangerClient::RangerClient(Env* env, const scoped_refptr<MetricEntity>& metric_entity)
    : env_(env), metric_entity_(metric_entity) {
  DCHECK(metric_entity);
}

Status RangerClient::Start() {
  VLOG(1) << "Initializing Ranger subprocess server";
  vector<string> argv;
  const string fifo_path = subprocess::SubprocessServer::FifoPath(ranger_fifo_base());
  RETURN_NOT_OK(BuildArgv(fifo_path, &argv));
  subprocess_.reset(new RangerSubprocess(env_, fifo_path, std::move(argv), metric_entity_));
  return subprocess_->Start();
}

// TODO(abukor): refactor to avoid code duplication
Status RangerClient::AuthorizeAction(const string& user_name,
                                     const ActionPB& action,
                                     const string& table_name,
                                     Scope scope) {
  DCHECK(subprocess_);
  string db;
  Slice tbl;

  auto s = ParseRangerTableIdentifier(table_name, &db, &tbl);
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << Substitute(kDenyNonRangerTableTemplate, table_name);
    return Status::NotAuthorized(kUnauthorizedAction);
  }

  RangerRequestListPB req_list;
  RangerResponseListPB resp_list;
  req_list.set_user(user_name);

  RangerRequestPB* req = req_list.add_requests();

  req->set_action(action);
  req->set_database(db);
  // Only pass the table name if this is table level request.
  if (scope == Scope::TABLE) {
    req->set_table(tbl.ToString());
  }

  RETURN_NOT_OK(subprocess_->Execute(req_list, &resp_list));

  CHECK_EQ(1, resp_list.responses_size());
  if (resp_list.responses().begin()->allowed()) {
    return Status::OK();
  }

  LOG(WARNING) << Substitute("User $0 is not authorized to perform $1 on $2 at scope ($3)",
                             user_name, ActionPB_Name(action), table_name, ScopeToString(scope));
  return Status::NotAuthorized(kUnauthorizedAction);
}

Status RangerClient::AuthorizeActionMultipleColumns(const string& user_name,
                                                    const ActionPB& action,
                                                    const string& table_name,
                                                    unordered_set<string>* column_names) {
  DCHECK(subprocess_);
  DCHECK(!column_names->empty());

  string db;
  Slice tbl;

  auto s = ParseRangerTableIdentifier(table_name, &db, &tbl);
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << Substitute(kDenyNonRangerTableTemplate, table_name);
    return Status::NotAuthorized(kUnauthorizedAction);
  }

  RangerRequestListPB req_list;
  RangerResponseListPB resp_list;
  req_list.set_user(user_name);

  for (const auto& col : *column_names) {
    auto req = req_list.add_requests();
    req->set_action(action);
    req->set_database(db);
    req->set_table(tbl.ToString());
    req->set_column(col);
  }

  RETURN_NOT_OK(subprocess_->Execute(req_list, &resp_list));

  DCHECK_EQ(column_names->size(), resp_list.responses_size());

  unordered_set<string> allowed_columns;
  for (auto i = 0; i < req_list.requests_size(); ++i) {
    if (resp_list.responses(i).allowed()) {
      EmplaceOrDie(&allowed_columns, move(req_list.requests(i).column()));
    }
  }

  if (allowed_columns.empty()) {
    LOG(WARNING) << Substitute("User $0 is not authorized to perform $1 on table $2",
                               user_name, ActionPB_Name(action), table_name);
    return Status::NotAuthorized(kUnauthorizedAction);
  }

  *column_names = move(allowed_columns);

  return Status::OK();
}

Status RangerClient::AuthorizeActionMultipleTables(const string& user_name,
                                                   const ActionPB& action,
                                                   unordered_set<string>* table_names) {
  DCHECK(subprocess_);

  RangerRequestListPB req_list;
  RangerResponseListPB resp_list;
  req_list.set_user(user_name);

  vector<string> orig_table_names;

  for (const auto& table : *table_names) {
    string db;
    Slice tbl;

    auto s = ParseRangerTableIdentifier(table, &db, &tbl);
    if (PREDICT_TRUE(s.ok())) {
      orig_table_names.emplace_back(table);

      auto req = req_list.add_requests();
      req->set_action(action);
      req->set_database(db);
      req->set_table(tbl.ToString());
    } else {
      LOG(WARNING) << Substitute(kDenyNonRangerTableTemplate, table);
    }
  }

  RETURN_NOT_OK(subprocess_->Execute(req_list, &resp_list));

  DCHECK_EQ(orig_table_names.size(), resp_list.responses_size());

  unordered_set<string> allowed_tables;
  for (auto i = 0; i < orig_table_names.size(); ++i) {
    if (resp_list.responses(i).allowed()) {
      EmplaceOrDie(&allowed_tables, move(orig_table_names[i]));
    }
  }

  *table_names = move(allowed_tables);

  return Status::OK();
}

Status RangerClient::AuthorizeActions(const string& user_name,
                                      const string& table_name,
                                      unordered_set<ActionPB, ActionHash>* actions) {
  DCHECK(subprocess_);
  DCHECK(!actions->empty());

  string db;
  Slice tbl;

  auto s = ParseRangerTableIdentifier(table_name, &db, &tbl);
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << Substitute(kDenyNonRangerTableTemplate, table_name);
    return Status::NotAuthorized(kUnauthorizedAction);
  }

  RangerRequestListPB req_list;
  RangerResponseListPB resp_list;
  req_list.set_user(user_name);

  for (const auto& action : *actions) {
    auto req = req_list.add_requests();
    req->set_action(action);
    req->set_database(db);
    req->set_table(tbl.ToString());
  }

  RETURN_NOT_OK(subprocess_->Execute(req_list, &resp_list));

  DCHECK_EQ(actions->size(), resp_list.responses_size());

  unordered_set<ActionPB, ActionHash> allowed_actions;
  for (auto i = 0; i < req_list.requests_size(); ++i) {
    if (resp_list.responses(i).allowed()) {
      EmplaceOrDie(&allowed_actions, move(req_list.requests(i).action()));
    }
  }

  if (allowed_actions.empty()) {
    LOG(WARNING) << Substitute("User $0 is not authorized to perform actions $1 on table $2",
                               user_name, JoinMapped(*actions, ActionPB_Name, ", "), table_name);
    return Status::NotAuthorized(kUnauthorizedAction);
  }

  *actions = move(allowed_actions);

  return Status::OK();
}
} // namespace ranger
} // namespace kudu
