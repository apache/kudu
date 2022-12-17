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

#include <sys/stat.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>  // IWYU pragma: keep
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/cfile/cfile-test-base.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/fs_report.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_catalog.h"
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/ts_manager.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/subprocess/subprocess_protocol.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/thrift/client.h"
#include "kudu/tools/tool.pb.h"
#include "kudu/tools/tool_replica_util.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server_options.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/util/async_util.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/url-coding.h"

DECLARE_bool(encrypt_data_at_rest);
DECLARE_bool(fs_data_dirs_consider_available_space);
DECLARE_bool(hive_metastore_sasl_enabled);
DECLARE_bool(show_values);
DECLARE_bool(show_attributes);
DECLARE_int32(catalog_manager_inject_latency_load_ca_info_ms);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_int32(tserver_unresponsive_timeout_ms);
DECLARE_int32(rpc_negotiation_inject_delay_ms);
DECLARE_string(block_manager);
DECLARE_string(hive_metastore_uris);

METRIC_DECLARE_counter(bloom_lookups);
METRIC_DECLARE_entity(tablet);

using kudu::cfile::CFileWriter;
using kudu::cfile::StringDataGenerator;
using kudu::cfile::WriterOptions;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnStorageAttributes;
using kudu::client::KuduInsert;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduTableStatistics;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::ExternalTabletServer;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::consensus::OpId;
using kudu::consensus::RECEIVED_OPID;
using kudu::consensus::ReplicateMsg;
using kudu::consensus::ReplicateRefPtr;
using kudu::fs::BlockDeletionTransaction;
using kudu::fs::FsReport;
using kudu::fs::LogBlockManager;
using kudu::fs::WritableBlock;
using kudu::hms::HmsCatalog;
using kudu::hms::HmsClient;
using kudu::iequals;
using kudu::itest::MiniClusterFsInspector;
using kudu::itest::TServerDetails;
using kudu::log::Log;
using kudu::log::LogOptions;
using kudu::master::MiniMaster;
using kudu::master::TServerStatePB;
using kudu::master::TSManager;
using kudu::rpc::RpcController;
using kudu::subprocess::SubprocessProtocol;
using kudu::tablet::LocalTabletWriter;
using kudu::tablet::Tablet;
using kudu::tablet::TabletDataState;
using kudu::tablet::TabletHarness;
using kudu::tablet::TabletMetadata;
using kudu::tablet::TabletReplica;
using kudu::tablet::TabletSuperBlockPB;
using kudu::tserver::DeleteTabletRequestPB;
using kudu::tserver::DeleteTabletResponsePB;
using kudu::tserver::ListTabletsResponsePB;
using kudu::tserver::MiniTabletServer;
using kudu::tserver::TabletServerErrorPB;
using kudu::tserver::WriteRequestPB;
using std::back_inserter;
using std::copy;
using std::find;
using std::make_pair;
using std::map;
using std::max;
using std::nullopt;
using std::optional;
using std::ostringstream;
using std::pair;
using std::set;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using std::tuple;
using strings::Substitute;

namespace kudu {
namespace tools {

class ToolTest : public KuduTest {
 public:
  ~ToolTest() {
    STLDeleteValues(&ts_map_);
  }

  virtual void TearDown() OVERRIDE {
    if (cluster_) cluster_->Shutdown();
    if (mini_cluster_) mini_cluster_->Shutdown();
    KuduTest::TearDown();
  }

  virtual bool EnableKerberos() {
    return false;
  }

  Status RunTool(const string& arg_str,
                 string* stdout = nullptr,
                 string* stderr = nullptr,
                 vector<string>* stdout_lines = nullptr,
                 vector<string>* stderr_lines = nullptr,
                 const string& in = "") const {
    string out;
    string err;
    Status s = RunKuduTool(strings::Split(arg_str, " ", strings::SkipEmpty()),
                           &out, &err, in);
    if (stdout) {
      *stdout = out;
      StripTrailingNewline(stdout);
    }
    if (stderr) {
      *stderr = err;
      StripTrailingNewline(stderr);
    }
    if (stdout_lines) {
      *stdout_lines = strings::Split(out, "\n");
      while (!stdout_lines->empty() && stdout_lines->back() == "") {
        stdout_lines->pop_back();
      }
    }
    if (stderr_lines) {
      *stderr_lines = strings::Split(err, "\n");
      while (!stderr_lines->empty() && stderr_lines->back() == "") {
        stderr_lines->pop_back();
      }
    }
    return s;
  }

  void RunActionStdoutNone(const string& arg_str) const {
    string stdout;
    string stderr;
    Status s = RunTool(arg_str, &stdout, &stderr, nullptr, nullptr);
    SCOPED_TRACE(stdout);
    SCOPED_TRACE(stderr);
    ASSERT_OK(s);
    ASSERT_TRUE(stdout.empty());
  }

  void RunActionStdoutString(const string& arg_str, string* stdout) const {
    string stderr;
    Status s = RunTool(arg_str, stdout, &stderr, nullptr, nullptr);
    SCOPED_TRACE(*stdout);
    SCOPED_TRACE(stderr);
    ASSERT_TRUE(s.ok()) << arg_str;
  }

  void RunActionStdinStdoutString(const string& arg_str, const string& stdin,
                                  string* stdout) const {
    string stderr;
    Status s = RunTool(arg_str, stdout, &stderr, nullptr, nullptr, stdin);
    SCOPED_TRACE(*stdout);
    SCOPED_TRACE(stderr);
    ASSERT_OK(s);
  }

  Status RunActionStderrString(const string& arg_str, string* stderr) const {
    return RunTool(arg_str, nullptr, stderr, nullptr, nullptr);
  }

  Status RunActionStdoutStderrString(const string& arg_str, string* stdout,
                                     string* stderr) const {
    return RunTool(arg_str, stdout, stderr, nullptr, nullptr);
  }

  string GetEncryptionArgs() {
    return "--encrypt_data_at_rest=true";
  }

  void RunActionStdoutLines(const string& arg_str, vector<string>* stdout_lines) const {
    string stderr;
    Status s = RunTool(arg_str, nullptr, &stderr, stdout_lines, nullptr);
    SCOPED_TRACE(*stdout_lines);
    SCOPED_TRACE(stderr);
    ASSERT_OK(s);
  }

  // Run tool with specified arguments, expecting help output.
  void RunTestHelp(const string& arg_str,
                   const vector<string>& regexes,
                   const Status& expected_status = Status::OK()) const {
    vector<string> stdout;
    vector<string> stderr;
    Status s = RunTool(arg_str, nullptr, nullptr, &stdout, &stderr);
    SCOPED_TRACE(stdout);
    SCOPED_TRACE(stderr);

    // These are always true for showing help.
    ASSERT_TRUE(s.IsRuntimeError());
    ASSERT_TRUE(stdout.empty());
    ASSERT_FALSE(stderr.empty());

    // If it was an invalid command, the usage string is on the second line.
    int usage_idx = 0;
    if (!expected_status.ok()) {
      ASSERT_EQ(expected_status.ToString(), stderr[0]);
      usage_idx = 1;
    }
    ASSERT_EQ(0, stderr[usage_idx].find("Usage: "));

    // Strip away everything up to the usage string to test for regexes.
    const vector<string> remaining_lines(stderr.begin() + usage_idx + 1,
                                         stderr.end());
    for (const auto& r : regexes) {
      ASSERT_STRINGS_ANY_MATCH(remaining_lines, r);
    }
  }

  // Run each of the provided 'arg_str_subcommands' for the specified 'arg_str'
  // command with '--help' command-line flag and make sure the result usage
  // message contains information on command line flags to control RPC and
  // connection negotiation timeouts. This targets various CLI tools issuing
  // RPCs to a Kudu cluster (either to masters or tablet servers).
  void RunTestHelpRpcFlags(const string& arg_str,
                           const vector<string>& arg_str_subcommands) const {
    static const vector<string> kTimeoutRegexes = {
      "-negotiation_timeout_ms \\(Timeout for negotiating an RPC connection",
      "-timeout_ms \\(RPC timeout in milliseconds\\)",
    };
    for (auto& cmd_str : arg_str_subcommands) {
      const string arg = arg_str + " " + cmd_str + " --help";
      NO_FATALS(RunTestHelp(arg, kTimeoutRegexes));
    }
  }

  // Run tool without a required positional argument, expecting error message
  void RunActionMissingRequiredArg(const string& arg_str, const string& required_arg,
                                   bool variadic = false) const {
    const string kPositionalArgumentMessage = "must provide positional argument";
    const string kVariadicArgumentMessage = "must provide variadic positional argument";
    const string& message = variadic ? kVariadicArgumentMessage : kPositionalArgumentMessage;
    Status expected_status = Status::InvalidArgument(Substitute("$0 $1", message, required_arg));

    vector<string> err_lines;
    RunTool(arg_str, nullptr, nullptr, nullptr, /* stderr_lines = */ &err_lines);
    ASSERT_GE(err_lines.size(), 3) << err_lines;
    ASSERT_EQ(expected_status.ToString(), err_lines[0]);
    ASSERT_STR_MATCHES(err_lines[2], "Usage: .*kudu.*");
  }

  void RunFsCheck(const string& arg_str,
                  int expected_num_live,
                  const string& tablet_id,
                  const vector<BlockId>& expected_missing_blocks,
                  int expected_num_orphaned) {
    string stdout;
    string stderr;
    Status s = RunTool(arg_str, &stdout, &stderr, nullptr, nullptr);
    SCOPED_TRACE(stdout);
    SCOPED_TRACE(stderr);
    if (!expected_missing_blocks.empty()) {
      ASSERT_TRUE(s.IsRuntimeError());
      ASSERT_STR_CONTAINS(stderr, "Corruption");
    } else {
      ASSERT_TRUE(s.ok());
    }
    // Some stats aren't gathered for the FBM: see FileBlockManager::Open.
    ASSERT_STR_CONTAINS(
        stdout, Substitute("Total live blocks: $0",
                           FLAGS_block_manager == "file" ? 0 : expected_num_live));
    ASSERT_STR_CONTAINS(
        stdout, Substitute("Total missing blocks: $0", expected_missing_blocks.size()));
    if (!expected_missing_blocks.empty()) {
      ASSERT_STR_CONTAINS(
          stdout, Substitute("Fatal error: tablet $0 missing blocks: ", tablet_id));
      for (const auto& b : expected_missing_blocks) {
        ASSERT_STR_CONTAINS(stdout, b.ToString());
      }
    }
    ASSERT_STR_CONTAINS(
        stdout, Substitute("Total orphaned blocks: $0", expected_num_orphaned));
  }

  Status HasAtLeastOneBackupFile(const string& dir, bool* found) {
    vector<string> children;
    RETURN_NOT_OK(env_->GetChildren(dir, &children));
    *found = false;
    for (const auto& child : children) {
      if (child.find(".bak") != string::npos) {
        *found = true;
        break;
      }
    }
    return Status::OK();
  }

  void RunScanTableCheck(const string& table_name,
                         const string& predicates_json,
                         int64_t min_value,
                         int64_t max_value,
                         const vector<pair<string, string>>& columns = {{"int32", "key"}},
                         const string& action = "table scan") {
    vector<string> col_names;
    col_names.reserve(columns.size());
    for (const auto& column : columns) {
      col_names.push_back(column.second);
    }
    const string projection = JoinStrings(col_names, ",");

    vector<string> lines;
    int64_t total = max<int64_t>(max_value - min_value + 1, 0);
    NO_FATALS(RunActionStdoutLines(
                Substitute("$0 $1 $2 "
                           "-columns=$3 -predicates=$4 -num_threads=1",
                           action,
                           cluster_->master()->bound_rpc_addr().ToString(),
                           table_name, projection, predicates_json),
                &lines));

    vector<pair<string, string>> expected_columns;
    if (columns.empty()) {
      // If we ran with an empty projection, we'll actually get all the columns.
      expected_columns = {{ "int.*", "key" },
                          { "int32", "int_val" },
                          { "string", "string_val" }};
    } else {
      expected_columns = columns;
    }

    size_t line_count = 0;
    int64_t value = min_value;
    for (const auto& line : lines) {
      if (line.find('(') != string::npos) {
        // Check matched rows.
        vector<string> kvs;
        kvs.reserve(expected_columns.size());
        for (const auto& column : expected_columns) {
          // Check projected columns.
          kvs.push_back(Substitute("$0 $1=$2",
              column.first, column.second, column.second == "key" ? to_string(value) : ".*"));
        }
        string line_pattern(R"*(\()*");
        line_pattern += JoinStrings(kvs, ", ");
        line_pattern += (")");
        ASSERT_STR_MATCHES(line, line_pattern);
        ++line_count;
        ++value;
      } else if (line.find("scanned count") != string::npos) {
        // Check tablet scan statistic.
        ASSERT_STR_MATCHES(line, "T .* scanned count .* cost .* seconds");
        ++line_count;
      } else {
        // Check whole table scan statistic.
        ++line_count;
        ASSERT_STR_MATCHES(line, Substitute("Total count $0 cost .* seconds", total));
        // This is the last line of the output.
        ASSERT_EQ(line_count, lines.size());
        break;
      }
    }
    if (action == "table scan") {
      ASSERT_EQ(value, max_value + 1);
    }
  }

  enum class TableCopyMode {
    INSERT_TO_EXIST_TABLE = 0,
    INSERT_TO_NOT_EXIST_TABLE = 1,
    UPSERT_TO_EXIST_TABLE = 2,
    COPY_SCHEMA_ONLY = 3,
  };

  struct RunCopyTableCheckArgs {
    string src_table_name;
    string predicates_json;
    int64_t min_value;
    int64_t max_value;
    string columns;
    TableCopyMode mode;
    int32_t create_table_replication_factor;
    string create_table_hash_bucket_nums;
  };

  void RunCopyTableCheck(const RunCopyTableCheckArgs& args) {
    const string kDstTableName = "kudu.table.copy.to";

    // Prepare command flags, create destination table and write some data if needed.
    string write_type;
    string create_table;
    TestWorkload ww(cluster_.get());
    ww.set_table_name(kDstTableName);
    ww.set_num_replicas(1);
    switch (args.mode) {
      case TableCopyMode::INSERT_TO_EXIST_TABLE:
        write_type = "insert";
        create_table = "false";
        // Create the dst table.
        ww.set_num_write_threads(0);
        ww.Setup();
        break;
      case TableCopyMode::INSERT_TO_NOT_EXIST_TABLE:
        write_type = "insert";
        create_table = "true";
        break;
      case TableCopyMode::UPSERT_TO_EXIST_TABLE:
        write_type = "upsert";
        create_table = "false";
        // Create the dst table and write some data to it.
        ww.set_write_pattern(TestWorkload::INSERT_SEQUENTIAL_ROWS);
        ww.set_num_write_threads(1);
        ww.set_write_batch_size(1);
        ww.Setup();
        ww.Start();
        ASSERT_EVENTUALLY([&]() {
          ASSERT_GE(ww.rows_inserted(), 100);
        });
        ww.StopAndJoin();
        break;
      case TableCopyMode::COPY_SCHEMA_ONLY:
        write_type = "";
        create_table = "true";
        break;
      default:
        ASSERT_TRUE(false);
    }

    // Execute copy command.
    string stdout;
    string stderr;
    Status s = RunActionStdoutStderrString(
                Substitute("table copy $0 $1 $2 -dst_table=$3 -predicates=$4 -write_type=$5 "
                           "-create_table=$6 -create_table_replication_factor=$7 "
                           "-create_table_hash_bucket_nums=$8",
                           cluster_->master()->bound_rpc_addr().ToString(),
                           args.src_table_name,
                           cluster_->master()->bound_rpc_addr().ToString(),
                           kDstTableName,
                           args.predicates_json,
                           write_type,
                           create_table,
                           args.create_table_replication_factor,
                           args.create_table_hash_bucket_nums),
                &stdout, &stderr);
    if (args.create_table_hash_bucket_nums == "10,aa") {
      ASSERT_STR_CONTAINS(stderr, "cannot parse the number of hash buckets.");
      return;
    }
    if (args.create_table_hash_bucket_nums == "10,20,30") {
      ASSERT_STR_CONTAINS(stderr, "The count of hash bucket numbers must be equal to the "
                                  "number of hash schema dimensions.");
      return;
    }
    if (args.create_table_hash_bucket_nums == "10") {
      ASSERT_STR_CONTAINS(stderr, "The count of hash bucket numbers must be equal to the "
                                  "number of hash schema dimensions.");
      return;
    }
    if (args.create_table_hash_bucket_nums == "10,1") {
      ASSERT_STR_CONTAINS(stderr, "The number of hash buckets must not be less than 2.");
      return;
    }
    if (args.create_table_hash_bucket_nums == "10,1") {
      ASSERT_STR_CONTAINS(stderr, "The number of hash buckets must not be less than 2.");
      return;
    }
    if (args.create_table_hash_bucket_nums == "10,50") {
      ASSERT_STR_CONTAINS(stderr, "There are no hash partitions defined in this table.");
      return;
    }
    ASSERT_TRUE(s.ok());

    // Check total count.
    int64_t total = max<int64_t>(args.max_value - args.min_value + 1, 0);
    if (args.mode == TableCopyMode::COPY_SCHEMA_ONLY) {
      ASSERT_STR_NOT_CONTAINS(stdout, "Total count ");
    } else {
      ASSERT_STR_CONTAINS(stdout, Substitute("Total count $0 ", total));
    }

    // Check schema equals when destination table is created automatically.
    if (args.mode == TableCopyMode::INSERT_TO_NOT_EXIST_TABLE ||
        args.mode == TableCopyMode::COPY_SCHEMA_ONLY) {
      vector<string> src_schema;
      NO_FATALS(RunActionStdoutLines(
        Substitute("table describe $0 $1 -show_attributes=true",
                   cluster_->master()->bound_rpc_addr().ToString(),
                   args.src_table_name), &src_schema));

      vector<string> dst_schema;
      NO_FATALS(RunActionStdoutLines(
        Substitute("table describe $0 $1 -show_attributes=true",
                   cluster_->master()->bound_rpc_addr().ToString(),
                   kDstTableName), &dst_schema));

      ASSERT_EQ(src_schema.size(), dst_schema.size());
      for (int i = 0; i < src_schema.size(); ++i) {
        // Table name is different.
        if (HasPrefixString(src_schema[i], "TABLE ")) continue;
        // Replication factor is different when explicitly set it to 3 (default 1).
        if (args.create_table_replication_factor == 3 &&
            HasPrefixString(src_schema[i], "REPLICAS ")) continue;
        vector<string> hash_bucket_nums = Split(args.create_table_hash_bucket_nums,
                                            ",", strings::SkipEmpty());
        if (args.create_table_hash_bucket_nums == "10,20" &&
            HasPrefixString(src_schema[i], "HASH (key_hash0)")) {
          ASSERT_STR_CONTAINS(dst_schema[i], Substitute("PARTITIONS $0",
                                                        hash_bucket_nums[0]));
          continue;
        }
        if (args.create_table_hash_bucket_nums == "10,20" &&
            HasPrefixString(src_schema[i], "HASH (key_hash1, key_hash2)")) {
          ASSERT_STR_CONTAINS(dst_schema[i], Substitute("PARTITIONS $0",
                                                        hash_bucket_nums[1]));
          continue;
        }
        if (args.create_table_hash_bucket_nums == "10,2" &&
            HasPrefixString(src_schema[i], "HASH (key_hash0)")) {
          ASSERT_STR_CONTAINS(dst_schema[i], Substitute("PARTITIONS $0",
                                                        hash_bucket_nums[0]));
          continue;
        }
        if (args.create_table_hash_bucket_nums == "10,2" &&
            HasPrefixString(src_schema[i], "HASH (key_hash1, key_hash2)")) {
          ASSERT_STR_CONTAINS(dst_schema[i], Substitute("PARTITIONS $0",
                                                        hash_bucket_nums[1]));
          continue;
        }
        ASSERT_EQ(src_schema[i], dst_schema[i]);
      }
    }

    // Check all values.
    vector<string> src_lines;
    NO_FATALS(RunActionStdoutLines(
      Substitute("table scan $0 $1 -show_values=true "
                 "-columns=$2 -predicates=$3 -num_threads=1",
                 cluster_->master()->bound_rpc_addr().ToString(),
                 args.src_table_name, args.columns, args.predicates_json), &src_lines));

    vector<string> dst_lines;
    NO_FATALS(RunActionStdoutLines(
      Substitute("table scan $0 $1 -show_values=true "
                 "-columns=$2 -num_threads=1",
                 cluster_->master()->bound_rpc_addr().ToString(),
                 kDstTableName, args.columns), &dst_lines));

    if (args.mode == TableCopyMode::COPY_SCHEMA_ONLY) {
      ASSERT_GE(dst_lines.size(), 1);
      ASSERT_STR_CONTAINS(*dst_lines.rbegin(), "Total count 0 ");
    } else {
      // Rows scanned from source table can be found in destination table.
      set<string> sorted_dst_lines(dst_lines.begin(), dst_lines.end());
      for (auto src_line = src_lines.begin(); src_line != src_lines.end();) {
        if (src_line->find("key") != string::npos) {
          ASSERT_TRUE(ContainsKey(sorted_dst_lines, *src_line));
          sorted_dst_lines.erase(*src_line);
        }
        src_line = src_lines.erase(src_line);
      }

      // Under all modes except UPSERT_TO_EXIST_TABLE, destination table is empty before
      // copying, that means destination table should have no more rows than source table
      // after copying.
      if (args.mode != TableCopyMode::UPSERT_TO_EXIST_TABLE) {
        for (const auto& dst_line : sorted_dst_lines) {
          ASSERT_STR_NOT_CONTAINS(dst_line, "key");
        }
      }
    }

    // Drop dst table.
    ww.Cleanup();
  }

  // Write YAML content to file ${KUDU_CONFIG}/kudurc.
  void PrepareConfigFile(const string& content) {
    string fname = GetTestPath("kudurc");
    ASSERT_OK(WriteStringToFile(env_, content, fname));
  }

  void CheckCorruptClusterInfoConfigFile(const string& content,
                                         const string& expect_err) {
    const string kClusterName = "external_mini_cluster";
    NO_FATALS(PrepareConfigFile(content));
    string stderr;
    Status s = RunActionStderrString(Substitute("master list @$0", kClusterName), &stderr);
    ASSERT_TRUE(s.IsRuntimeError());
    ASSERT_STR_CONTAINS(stderr, expect_err);
  }

  string GetMasterAddrsStr() {
    vector<string> master_addrs;
    for (const auto& hp : mini_cluster_->master_rpc_addrs()) {
      master_addrs.emplace_back(hp.ToString());
    }
    return JoinStrings(master_addrs, ",");
  }

 protected:
  // Note: Each test case must have a single invocation of RunLoadgen() otherwise it leads to
  //       memory leaks.
  void RunLoadgen(int num_tservers = 1,
                  const vector<string>& tool_args = {},
                  const string& table_name = "",
                  string* tool_stdout = nullptr,
                  string* tool_stderr = nullptr);
  void StartExternalMiniCluster(ExternalMiniClusterOptions opts = {});
  void StartMiniCluster(InternalMiniClusterOptions opts = {});
  void StopMiniCluster();
  unique_ptr<ExternalMiniCluster> cluster_;
  unique_ptr<MiniClusterFsInspector> inspect_;
  unordered_map<string, TServerDetails*> ts_map_;
  unique_ptr<InternalMiniCluster> mini_cluster_;
};

// Subclass of ToolTest that allows running individual test cases with Kerberos
// enabled and disabled. Most of the test cases are run only with Kerberos
// disabled, but to get coverage against a Kerberized cluster we run select
// cases in both modes.
class ToolTestKerberosParameterized : public ToolTest, public ::testing::WithParamInterface<bool> {
 public:
  bool EnableKerberos() override {
    return GetParam();
  }
};
INSTANTIATE_TEST_SUITE_P(ToolTestKerberosParameterized, ToolTestKerberosParameterized,
                         ::testing::Values(false, true));

enum RunCopyTableCheckArgsType {
  kTestCopyTableDstTableExist,
  kTestCopyTableDstTableNotExist,
  kTestCopyTableUpsert,
  kTestCopyTableSchemaOnly,
  kTestCopyTableComplexSchema,
  kTestCopyUnpartitionedTable,
  kTestCopyTablePredicates,
  kTestCopyTableWithStringBounds
};
// Subclass of ToolTest that allows running individual test cases with different parameters to run
// 'kudu table copy' CLI tool.
class ToolTestCopyTableParameterized :
    public ToolTest,
    public ::testing::WithParamInterface<int> {
 public:
  void SetUp() override {
    test_case_ = GetParam();
    ExternalMiniClusterOptions opts;
    if (test_case_ == kTestCopyTableSchemaOnly) {
      // In kTestCopyTableSchemaOnly case, we may create table with RF=3,
      // means 3 tservers needed at least.
      opts.num_tablet_servers = 3;
    }
    NO_FATALS(StartExternalMiniCluster(opts));

    // Create the src table and write some data to it.
    TestWorkload ww(cluster_.get());
    ww.set_table_name(kTableName);
    ww.set_num_replicas(1);
    ww.set_write_pattern(TestWorkload::INSERT_SEQUENTIAL_ROWS);
    ww.set_num_write_threads(1);
    // Create a complex schema if needed, or use a default simple schema.
    if (test_case_ == kTestCopyTableComplexSchema) {
      KuduSchema schema;
      ASSERT_OK(CreateComplexSchema(&schema));
      ww.set_schema(schema);
    } else if (test_case_ == kTestCopyUnpartitionedTable) {
      KuduSchema schema;
      ASSERT_OK(CreateUnpartitionedTable(&schema));
      ww.set_schema(schema);
    } else if (test_case_ == kTestCopyTableWithStringBounds) {
      // Regression for KUDU-3306, verify copying a table with string columns in its range key.
      KuduSchema schema;
      ASSERT_OK(CreateTableWithStringBounds(&schema));
      ww.set_schema(schema);
      ww.Setup();
      return;
    }
    ww.Setup();
    ww.Start();
    ASSERT_EVENTUALLY([&]() {
      ASSERT_GE(ww.rows_inserted(), 100);
    });
    ww.StopAndJoin();
    total_rows_ = ww.rows_inserted();

    // Insert one more row with a NULL cell if needed.
    if (test_case_ == kTestCopyTablePredicates) {
      InsertOneRowWithNullCell();
    }
  }

  vector<RunCopyTableCheckArgs> GenerateArgs() {
    RunCopyTableCheckArgs args = { kTableName,
                                   "",
                                   1,
                                   total_rows_,
                                   kSimpleSchemaColumns,
                                   TableCopyMode::INSERT_TO_EXIST_TABLE,
                                   -1 };
    switch (test_case_) {
      case kTestCopyTableDstTableExist:
        return { args };
      case kTestCopyTableDstTableNotExist:
        args.mode = TableCopyMode::INSERT_TO_NOT_EXIST_TABLE;
        return { args };
      case kTestCopyTableUpsert:
        args.mode = TableCopyMode::UPSERT_TO_EXIST_TABLE;
        return { args };
      case kTestCopyTableSchemaOnly: {
        args.mode = TableCopyMode::COPY_SCHEMA_ONLY;
        vector<RunCopyTableCheckArgs> multi_args;
        {
          auto args_temp = args;
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.create_table_replication_factor = 1;
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.create_table_replication_factor = 3;
          multi_args.emplace_back(std::move(args_temp));
        }
        return multi_args;
      }
      case kTestCopyTableComplexSchema: {
        args.columns = kComplexSchemaColumns;
        args.mode = TableCopyMode::INSERT_TO_NOT_EXIST_TABLE;
        vector<RunCopyTableCheckArgs> multi_args;
        {
          auto args_temp = args;
          args.create_table_hash_bucket_nums = "10,20";
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.create_table_hash_bucket_nums = "10,aa";
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.create_table_hash_bucket_nums = "10,20,30";
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.create_table_hash_bucket_nums = "10";
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.create_table_hash_bucket_nums = "10,1";
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.create_table_hash_bucket_nums = "10,2";
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.create_table_hash_bucket_nums = "";
          multi_args.emplace_back(std::move(args_temp));
        }
        return multi_args;
      }
      case kTestCopyUnpartitionedTable: {
        args.mode = TableCopyMode::INSERT_TO_NOT_EXIST_TABLE;
        vector<RunCopyTableCheckArgs> multi_args;
        {
          auto args_temp = args;
          args.create_table_hash_bucket_nums = "10,50";
          multi_args.emplace_back(std::move(args_temp));
        }
        return multi_args;
      }
      case kTestCopyTablePredicates: {
        auto mid = total_rows_ / 2;
        vector<RunCopyTableCheckArgs> multi_args;
        {
          auto args_temp = args;
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.max_value = 1;
          args_temp.predicates_json = R"*(["AND",["=","key",1]])*";
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.min_value = mid + 1;
          args_temp.predicates_json = Substitute(R"*(["AND",[">","key",$0]])*", mid);
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.min_value = mid;
          args_temp.predicates_json = Substitute(R"*(["AND",[">=","key",$0]])*", mid);
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.max_value = mid - 1;
          args_temp.predicates_json = Substitute(R"*(["AND",["<","key",$0]])*", mid);
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.max_value = mid;
          args_temp.predicates_json = Substitute(R"*(["AND",["<=","key",$0]])*", mid);
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.max_value = 5;
          args_temp.predicates_json = R"*(["AND",["IN","key",[1,2,3,4,5]]])*";
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.max_value = total_rows_ - 1;
          args_temp.predicates_json = R"*(["AND",["NOTNULL","string_val"]])*";
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.min_value = total_rows_;
          args_temp.predicates_json = R"*(["AND",["NULL","string_val"]])*";
          multi_args.emplace_back(std::move(args_temp));
        }
        {
          auto args_temp = args;
          args_temp.max_value = 3;
          args_temp.predicates_json = R"*(["AND",["IN","key",[0,1,2,3]],)*"
                                      R"*(["<","key",8],[">=","key",1],["NOTNULL","key"],)*"
                                      R"*(["NOTNULL","string_val"]])*";
          multi_args.emplace_back(std::move(args_temp));
        }
        return multi_args;
      }
      case kTestCopyTableWithStringBounds:
        args.mode = TableCopyMode::COPY_SCHEMA_ONLY;
        args.columns = "";
        return {args};
      default:
        LOG(FATAL) << "Unknown type " << test_case_;
    }
    return {};
  }

 private:
  Status CreateComplexSchema(KuduSchema* schema) {
    shared_ptr<KuduClient> client;
    RETURN_NOT_OK(cluster_->CreateClient(nullptr, &client));
    // Build the schema.
    {
      KuduSchemaBuilder builder;
      builder.AddColumn("key_hash0")->Type(client::KuduColumnSchema::INT32)->NotNull();
      builder.AddColumn("key_hash1")->Type(client::KuduColumnSchema::INT32)->NotNull();
      builder.AddColumn("key_hash2")->Type(client::KuduColumnSchema::INT32)->NotNull();
      builder.AddColumn("key_range")->Type(client::KuduColumnSchema::INT32)->NotNull();
      builder.AddColumn("int8_val")->Type(client::KuduColumnSchema::INT8)
        ->Compression(KuduColumnStorageAttributes::CompressionType::NO_COMPRESSION)
        ->Encoding(KuduColumnStorageAttributes::EncodingType::PLAIN_ENCODING);
      builder.AddColumn("int16_val")->Type(client::KuduColumnSchema::INT16)
        ->Compression(KuduColumnStorageAttributes::CompressionType::SNAPPY)
        ->Encoding(KuduColumnStorageAttributes::EncodingType::RLE);
      builder.AddColumn("int32_val")->Type(client::KuduColumnSchema::INT32)
        ->Compression(KuduColumnStorageAttributes::CompressionType::LZ4)
        ->Encoding(KuduColumnStorageAttributes::EncodingType::BIT_SHUFFLE);
      builder.AddColumn("int64_val")->Type(client::KuduColumnSchema::INT64)
        ->Compression(KuduColumnStorageAttributes::CompressionType::ZLIB)
        ->Default(KuduValue::FromInt(123));
      builder.AddColumn("timestamp_val")->Type(client::KuduColumnSchema::UNIXTIME_MICROS);
      builder.AddColumn("string_val")->Type(client::KuduColumnSchema::STRING)
        ->Encoding(KuduColumnStorageAttributes::EncodingType::PREFIX_ENCODING)
        ->Default(KuduValue::CopyString(Slice("hello")));
      builder.AddColumn("bool_val")->Type(client::KuduColumnSchema::BOOL)
        ->Default(KuduValue::FromBool(false));
      builder.AddColumn("float_val")->Type(client::KuduColumnSchema::FLOAT);
      builder.AddColumn("double_val")->Type(client::KuduColumnSchema::DOUBLE)
        ->Default(KuduValue::FromDouble(123.4));
      builder.AddColumn("binary_val")->Type(client::KuduColumnSchema::BINARY)
        ->Encoding(KuduColumnStorageAttributes::EncodingType::DICT_ENCODING);
      builder.AddColumn("decimal_val")->Type(client::KuduColumnSchema::DECIMAL)
        ->Precision(30)
        ->Scale(4);
      builder.SetPrimaryKey({"key_hash0", "key_hash1", "key_hash2", "key_range"});
      RETURN_NOT_OK(builder.Build(schema));
    }

    // Set up partitioning and create the table.
    {
      unique_ptr<KuduPartialRow> bound0(schema->NewRow());
      RETURN_NOT_OK(bound0->SetInt32("key_range", 0));
      unique_ptr<KuduPartialRow> bound1(schema->NewRow());
      RETURN_NOT_OK(bound1->SetInt32("key_range", 1));
      unique_ptr<KuduPartialRow> bound2(schema->NewRow());
      RETURN_NOT_OK(bound2->SetInt32("key_range", 2));
      unique_ptr<KuduPartialRow> bound3(schema->NewRow());
      RETURN_NOT_OK(bound3->SetInt32("key_range", 3));
      unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
      RETURN_NOT_OK(table_creator->table_name(kTableName)
                    .schema(schema)
                    .add_hash_partitions({"key_hash0"}, 2)
                    .add_hash_partitions({"key_hash1", "key_hash2"}, 3)
                    .set_range_partition_columns({"key_range"})
                    .add_range_partition_split(bound0.release())
                    .add_range_partition_split(bound1.release())
                    .add_range_partition_split(bound2.release())
                    .add_range_partition_split(bound3.release())
                    .num_replicas(1)
                    .Create());
    }

    return Status::OK();
  }

  Status CreateUnpartitionedTable(KuduSchema* schema) {
    shared_ptr<KuduClient> client;
    RETURN_NOT_OK(cluster_->CreateClient(nullptr, &client));
    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    *schema = KuduSchema::FromSchema(GetSimpleTestSchema());
    RETURN_NOT_OK(table_creator->table_name(kTableName)
                      .schema(schema)
                      .set_range_partition_columns({})
                      .num_replicas(1)
                      .Create());
    return Status::OK();
  }

  Status CreateTableWithStringBounds(KuduSchema* schema) {
    shared_ptr<KuduClient> client;
    RETURN_NOT_OK(cluster_->CreateClient(nullptr, &client));
    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    *schema = KuduSchema::FromSchema(
        Schema({ColumnSchema("int_key", INT32), ColumnSchema("string_key", STRING)}, 2));

    unique_ptr<KuduPartialRow> first_lower_bound(schema->NewRow());
    unique_ptr<KuduPartialRow> first_upper_bound(schema->NewRow());
    RETURN_NOT_OK(first_lower_bound->SetStringNoCopy("string_key", "2020-01-01"));
    RETURN_NOT_OK(first_upper_bound->SetStringNoCopy("string_key", "2020-01-01"));

    unique_ptr<KuduPartialRow> second_lower_bound(schema->NewRow());
    unique_ptr<KuduPartialRow> second_upper_bound(schema->NewRow());
    RETURN_NOT_OK(second_lower_bound->SetStringNoCopy("string_key", "2021-01-01"));
    RETURN_NOT_OK(second_upper_bound->SetStringNoCopy("string_key", "2021-01-01"));
    KuduTableCreator::RangePartitionBound bound_type = KuduTableCreator::INCLUSIVE_BOUND;

    return table_creator->table_name(kTableName)
        .schema(schema)
        .set_range_partition_columns({"string_key"})
        .add_range_partition(
            first_lower_bound.release(), first_upper_bound.release(), bound_type, bound_type)
        .add_range_partition(
            second_lower_bound.release(), second_upper_bound.release(), bound_type, bound_type)
        .num_replicas(1)
        .Create();
  }

  void InsertOneRowWithNullCell() {
    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    shared_ptr<KuduSession> session = client->NewSession();
    session->SetTimeoutMillis(20000);
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(kTableName, &table));
    unique_ptr<KuduInsert> insert(table->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetInt32("key", ++total_rows_));
    ASSERT_OK(insert->mutable_row()->SetInt32("int_val", 1));
    ASSERT_OK(session->Apply(insert.release()));
    ASSERT_OK(session->Flush());
  }

  static const char kTableName[];
  static const char kSimpleSchemaColumns[];
  static const char kComplexSchemaColumns[];
  int test_case_ = 0;
  int64_t total_rows_ = 0;
};
const char ToolTestCopyTableParameterized::kTableName[] = "ToolTestCopyTableParameterized";
const char ToolTestCopyTableParameterized::kSimpleSchemaColumns[] = "key,int_val,string_val";
const char ToolTestCopyTableParameterized::kComplexSchemaColumns[]
    = "key_hash0,key_hash1,key_hash2,key_range,int8_val,int16_val,int32_val,int64_val,"
      "timestamp_val,string_val,bool_val,float_val,double_val,binary_val,decimal_val";

INSTANTIATE_TEST_SUITE_P(CopyTableParameterized,
                         ToolTestCopyTableParameterized,
                         ::testing::Values(kTestCopyTableDstTableExist,
                                           kTestCopyTableDstTableNotExist,
                                           kTestCopyTableUpsert,
                                           kTestCopyTableSchemaOnly,
                                           kTestCopyTableComplexSchema,
                                           kTestCopyUnpartitionedTable,
                                           kTestCopyTablePredicates,
                                           kTestCopyTableWithStringBounds));

void ToolTest::StartExternalMiniCluster(ExternalMiniClusterOptions opts) {
  cluster_.reset(new ExternalMiniCluster(std::move(opts)));
  ASSERT_OK(cluster_->Start());
  inspect_.reset(new MiniClusterFsInspector(cluster_.get()));
  STLDeleteValues(&ts_map_);
  ASSERT_OK(CreateTabletServerMap(cluster_->master_proxy(0),
                                  cluster_->messenger(), &ts_map_));
}

void ToolTest::StartMiniCluster(InternalMiniClusterOptions opts) {
  mini_cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
  ASSERT_OK(mini_cluster_->Start());
}

void ToolTest::StopMiniCluster() {
  mini_cluster_->Shutdown();
}

TEST_F(ToolTest, TestHelpXML) {
  string stdout;
  string stderr;
  Status s = RunTool("--helpxml", &stdout, &stderr, nullptr, nullptr);

  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_FALSE(stdout.empty()) << stdout;
  ASSERT_TRUE(stderr.empty()) << stderr;

  // All wrapped in AllModes node
  ASSERT_STR_MATCHES(stdout, "<\\?xml version=\"1.0\"\\?><AllModes>.*</AllModes>");

  // Verify all modes are output
  const vector<string> modes = {
      "cluster",
      "diagnose",
      "fs",
      "local_replica",
      "master",
      "pbc",
      "perf",
      "remote_replica",
      "table",
      "tablet",
      "test",
      "tserver",
      "wal",
      "dump",
      "cmeta",
      "change_config"
  };

  for (const auto& mode : modes) {
    ASSERT_STR_MATCHES(stdout, Substitute(".*<mode><name>$0</name>.*</mode>.*", mode));
  }
}

TEST_F(ToolTest, TestTopLevelHelp) {
  const vector<string> kTopLevelRegexes = {
      "cluster.*Kudu cluster",
      "diagnose.*Diagnostic tools.*",
      "fs.*Kudu filesystem",
      "hms.*Hive Metastores",
      "local_replica.*tablet replicas",
      "master.*Kudu Master",
      "pbc.*protobuf container",
      "perf.*performance of a Kudu cluster",
      "remote_replica.*tablet replicas on a Kudu Tablet Server",
      "table.*Kudu tables",
      "tablet.*Kudu tablets",
      "test.*test actions",
      "tserver.*Kudu Tablet Server",
      "wal.*write-ahead log"
  };
  NO_FATALS(RunTestHelp("", kTopLevelRegexes));
  NO_FATALS(RunTestHelp("--help", kTopLevelRegexes));
  NO_FATALS(RunTestHelp("not_a_mode", kTopLevelRegexes,
                        Status::InvalidArgument("unknown command 'not_a_mode'")));
}

TEST_F(ToolTest, TestModeHelp) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  {
    const string kCmd = "cluster";
    const vector<string> kClusterModeRegexes = {
        "ksck.*Check the health of a Kudu cluster",
        "rebalance.*Move tablet replicas between tablet servers",
    };
    NO_FATALS(RunTestHelp(kCmd, kClusterModeRegexes));
    NO_FATALS(RunTestHelpRpcFlags(kCmd, {"ksck", "rebalance"}));
  }
  {
    const vector<string> kDiagnoseModeRegexes = {
        "parse_stacks.*Parse sampled stack traces",
        "parse_metrics.*Parse metrics out of a diagnostics log",
    };
    NO_FATALS(RunTestHelp("diagnose", kDiagnoseModeRegexes));
  }
  {
    const vector<string> kFsModeRegexes = {
        "check.*Kudu filesystem for inconsistencies",
        "dump.*Dump a Kudu filesystem",
        "format.*new Kudu filesystem",
        "list.*List metadata for on-disk tablets, rowsets, blocks",
        "update_dirs.*Updates the set of data directories",
    };
    NO_FATALS(RunTestHelp("fs", kFsModeRegexes));
    NO_FATALS(RunTestHelp("fs not_a_mode", kFsModeRegexes,
                          Status::InvalidArgument("unknown command 'not_a_mode'")));
  }
  {
    const vector<string> kFsDumpModeRegexes = {
        "block.*binary contents of a data block",
        "cfile.*contents of a CFile",
        "tree.*tree of a Kudu filesystem",
        "uuid.*UUID of a Kudu filesystem",
    };
    NO_FATALS(RunTestHelp("fs dump", kFsDumpModeRegexes));

  }
  {
    const string kCmd = "hms";
    const vector<string> kHmsModeRegexes = {
        "check.*Check metadata consistency",
        "downgrade.*Downgrade the metadata",
        "fix.*Fix automatically-repairable metadata",
        "list.*List the Kudu table HMS entries",
        "precheck.*Check that the Kudu cluster is prepared",
    };
    NO_FATALS(RunTestHelp(kCmd, kHmsModeRegexes));
    NO_FATALS(RunTestHelp(kCmd + " not_a_mode", kHmsModeRegexes,
                          Status::InvalidArgument("unknown command 'not_a_mode'")));
    NO_FATALS(RunTestHelpRpcFlags(
        kCmd, {"check", "downgrade", "fix", "list", "precheck"}));
  }
  {
    const vector<string> kLocalReplicaModeRegexes = {
        "cmeta.*Operate on a local tablet replica's consensus",
        "data_size.*Summarize the data size",
        "dump.*Dump a Kudu filesystem",
        "copy_from_remote.*Copy tablet replicas from a remote server",
        "copy_from_local.*Copy tablet replicas from local filesystem",
        "delete.*Delete tablet replicas from the local filesystem",
        "list.*Show list of tablet replicas",
    };
    NO_FATALS(RunTestHelp("local_replica", kLocalReplicaModeRegexes));
  }
  {
    const vector<string> kLocalReplicaDumpModeRegexes = {
        "block_ids.*Dump the IDs of all blocks",
        "data_dirs.*Dump the data directories",
        "meta.*Dump the metadata",
        "rowset.*Dump the rowset contents",
        "wals.*Dump all WAL",
    };
    NO_FATALS(RunTestHelp("local_replica dump", kLocalReplicaDumpModeRegexes));
  }
  {
    const vector<string> kLocalReplicaCMetaRegexes = {
        "print_replica_uuids.*Print all tablet replica peer UUIDs",
        "rewrite_raft_config.*Rewrite a tablet replica",
        "set_term.*Bump the current term",
    };
    NO_FATALS(RunTestHelp("local_replica cmeta", kLocalReplicaCMetaRegexes));
    // Try with a hyphen instead of an underscore.
    NO_FATALS(RunTestHelp("local-replica cmeta", kLocalReplicaCMetaRegexes));
  }
  {
    const vector<string> kLocalReplicaCopyFromRemoteRegexes = {
        "Copy tablet replicas from a remote server",
    };
    NO_FATALS(RunTestHelp("local_replica copy_from_remote --help",
                          kLocalReplicaCopyFromRemoteRegexes));
    // Try with hyphens instead of underscores.
    NO_FATALS(RunTestHelp("local-replica copy-from-remote --help",
                          kLocalReplicaCopyFromRemoteRegexes));
  }
  {
    const vector<string> kLocalReplicaCopyFromRemoteRegexes = {
        "Copy tablet replicas from local filesystem",
    };
    NO_FATALS(RunTestHelp("local_replica copy_from_local --help",
                          kLocalReplicaCopyFromRemoteRegexes));
    // Try with hyphens instead of underscores.
    NO_FATALS(RunTestHelp("local-replica copy-from-local --help",
                          kLocalReplicaCopyFromRemoteRegexes));
  }
  {
    const string kCmd = "master";
    const vector<string> kMasterModeRegexes = {
        "authz_cache.*Operate on the authz caches of the Kudu Masters",
        "dump_memtrackers.*Dump the memtrackers",
        "get_flags.*Get the gflags",
        "run.*Run a Kudu Master",
        "set_flag.*Change a gflag value",
        "set_flag_for_all.*Change a gflag value",
        "status.*Get the status",
        "timestamp.*Get the current timestamp",
        "list.*List masters in a Kudu cluster",
        "add.*Add a master to the Kudu cluster",
        "remove.*Remove a master from the Kudu cluster",
    };
    NO_FATALS(RunTestHelp(kCmd, kMasterModeRegexes));
    NO_FATALS(RunTestHelpRpcFlags(kCmd,
        { "dump_memtrackers",
          "get_flags",
          "set_flag",
          "set_flag_for_all",
          "status",
          "timestamp",
          "list",
        }));
  }
  {
    const string kSubCmd = "master authz_cache";
    const vector<string> kMasterAuthzCacheModeRegexes = {
        "refresh.*Refresh the authorization policies",
    };
    NO_FATALS(RunTestHelp(kSubCmd, kMasterAuthzCacheModeRegexes));
    NO_FATALS(RunTestHelpRpcFlags(kSubCmd, {"refresh"}));
  }
  {
    NO_FATALS(RunTestHelp("master add --help",
                          {"-wait_secs.*\\(Timeout in seconds to wait while retrying operations",
                           "-kudu_abs_path.*\\(Absolute file path of the 'kudu' executable used"}));
    NO_FATALS(RunTestHelp("master remove --help",
                          {"-master_uuid.*\\(Permanent UUID of the master"}));
  }
  {
    const vector<string> kPbcModeRegexes = {
        "dump.*Dump a PBC",
        "edit.*Edit a PBC \\(protobuf container\\) file",
    };
    NO_FATALS(RunTestHelp("pbc", kPbcModeRegexes));
  }
  {
    const string kCmd = "perf";
    const vector<string> kPerfRegexes = {
        "loadgen.*Run load generation with optional scan afterwards",
        "table_scan.*Show row count and scanning time cost of tablets in a table",
        "tablet_scan.*Show row count of a local tablet",
    };
    NO_FATALS(RunTestHelp(kCmd, kPerfRegexes));
    NO_FATALS(RunTestHelpRpcFlags(kCmd, {"loadgen", "table_scan"}));
  }
  {
    const string kCmd = "remote_replica";
    const vector<string> kRemoteReplicaModeRegexes = {
        "check.*Check if all tablet replicas",
        "copy.*Copy a tablet replica from one Kudu Tablet Server",
        "delete.*Delete a tablet replica",
        "dump.*Dump the data of a tablet replica",
        "list.*List all tablet replicas",
        "unsafe_change_config.*Force the specified replica to adopt",
    };
    NO_FATALS(RunTestHelp(kCmd, kRemoteReplicaModeRegexes));
    NO_FATALS(RunTestHelpRpcFlags(kCmd,
        { "check",
          "copy",
          "delete",
          "dump",
          "list",
          "unsafe_change_config",
        }));
  }
  {
    const string kCmd = "table";
    const vector<string> kTableModeRegexes = {
        "add_range_partition.*Add a range partition for table",
        "clear_comment.*Clear the comment for a table",
        "column_remove_default.*Remove write_default value for a column",
        "column_set_block_size.*Set block size for a column",
        "column_set_compression.*Set compression type for a column",
        "column_set_default.*Set write_default value for a column",
        "column_set_encoding.*Set encoding type for a column",
        "column_set_comment.*Set comment for a column",
        "copy.*Copy table data to another table",
        "create.*Create a new table",
        "add_column.*Add a column",
        "delete_column.*Delete a column",
        "delete.*Delete a table",
        "describe.*Describe a table",
        "drop_range_partition.*Drop a range partition of table",
        "get_extra_configs.*Get the extra configuration properties for a table",
        "list.*List tables",
        "locate_row.*Locate which tablet a row belongs to",
        "recall.*Recall a deleted but still reserved table",
        "rename_column.*Rename a column",
        "rename_table.*Rename a table",
        "scan.*Scan rows from a table",
        "set_comment.*Set the comment for a table",
        "set_extra_config.*Change a extra configuration value on a table",
        "set_limit.*Set the write limit for a table",
        "set_replication_factor.*Change a table's replication factor",
        "statistics.*Get table statistics",
    };
    NO_FATALS(RunTestHelp(kCmd, kTableModeRegexes));
    NO_FATALS(RunTestHelpRpcFlags(kCmd,
        { "add_range_partition",
          "clear_comment",
          "column_remove_default",
          "column_set_block_size",
          "column_set_compression",
          "column_set_default",
          "column_set_encoding",
          "column_set_comment",
          "copy",
          "create",
          "delete_column",
          "delete",
          "describe",
          "drop_range_partition",
          "get_extra_configs",
          "list",
          "locate_row",
          "rename_column",
          "rename_table",
          "scan",
          "set_comment",
          "set_extra_config",
          "set_replication_factor",
          "statistics",
        }));
  }
  {
    const vector<string> kSetLimitModeRegexes = {
        "disk_size.*Set the disk size limit",
        "row_count.*Set the row count limit",
    };
    NO_FATALS(RunTestHelp("table set_limit", kSetLimitModeRegexes));
  }
  {
    const string kCmd = "tablet";
    const vector<string> kTabletModeRegexes = {
        "change_config.*Change.*Raft configuration",
        "leader_step_down.*Change.*tablet's leader",
        "unsafe_replace_tablet.*Replace a tablet with an empty one",
    };
    NO_FATALS(RunTestHelp(kCmd, kTabletModeRegexes));
    NO_FATALS(RunTestHelpRpcFlags(kCmd,
        { "leader_step_down",
          "unsafe_replace_tablet",
        }));
  }
  {
    const string kSubCmd = "tablet change_config";
    const vector<string> kChangeConfigModeRegexes = {
        "add_replica.*Add a new replica",
        "change_replica_type.*Change the type of an existing replica",
        "move_replica.*Move a tablet replica",
        "remove_replica.*Remove an existing replica",
    };
    NO_FATALS(RunTestHelp(kSubCmd, kChangeConfigModeRegexes));
    NO_FATALS(RunTestHelpRpcFlags(kSubCmd,
        { "add_replica",
          "change_replica_type",
          "move_replica",
          "remove_replica",
        }));
  }
  {
    const vector<string> kTestModeRegexes = {
        "mini_cluster.*Spawn a control shell"
    };
    NO_FATALS(RunTestHelp("test", kTestModeRegexes));
  }
  {
    const string kCmd = "tserver";
    const vector<string> kTServerModeRegexes = {
        "dump_memtrackers.*Dump the memtrackers",
        "get_flags.*Get the gflags",
        "set_flag.*Change a gflag value",
        "set_flag_for_all.*Change a gflag value",
        "run.*Run a Kudu Tablet Server",
        "state.*Operate on the state",
        "status.*Get the status",
        "quiesce.*Operate on the quiescing state",
        "timestamp.*Get the current timestamp",
        "list.*List tablet servers",
    };
    NO_FATALS(RunTestHelp(kCmd, kTServerModeRegexes));
    NO_FATALS(RunTestHelpRpcFlags(kCmd,
        { "dump_memtrackers",
          "get_flags",
          "set_flag",
          "set_flag_for_all",
          "status",
          "timestamp",
          "list",
        }));
  }
  {
    const string kSubCmd = "tserver state";
    const vector<string> kTServerSetStateModeRegexes = {
        "enter_maintenance.*Begin maintenance on the Tablet Server",
        "exit_maintenance.*End maintenance of the Tablet Server",
    };
    NO_FATALS(RunTestHelp(kSubCmd, kTServerSetStateModeRegexes));
    NO_FATALS(RunTestHelpRpcFlags(kSubCmd,
        { "enter_maintenance",
          "exit_maintenance",
        }));
  }
  {
    const string kSubCmd = "tserver quiesce";
    const vector<string> kTServerQuiesceModeRegexes = {
        "status.*Output information about the quiescing state",
        "start.*Start quiescing the given Tablet Server",
        "stop.*Stop quiescing a Tablet Server",
    };
    NO_FATALS(RunTestHelp(kSubCmd, kTServerQuiesceModeRegexes));
    NO_FATALS(RunTestHelpRpcFlags(kSubCmd, {"status", "start", "stop"}));
  }
  {
    const string kCmd = "txn";
    const vector<string> kTxnsModeRegexes = {
        "list.*Show details of multi-row transactions",
    };
    NO_FATALS(RunTestHelp(kCmd, kTxnsModeRegexes));
    NO_FATALS(RunTestHelpRpcFlags(kCmd,
        { "list" }));
  }
  {
    const vector<string> kWalModeRegexes = {
        "dump.*Dump a WAL",
    };
    NO_FATALS(RunTestHelp("wal", kWalModeRegexes));
  }
}

TEST_F(ToolTest, TestActionHelp) {
  const vector<string> kFormatActionRegexes = {
      "-fs_wal_dir \\(Directory",
      "-fs_data_dirs \\(Comma-separated list",
      "-uuid \\(The uuid"
  };
  NO_FATALS(RunTestHelp("fs format --help", kFormatActionRegexes));
  NO_FATALS(RunTestHelp("fs format extra_arg", kFormatActionRegexes,
      Status::InvalidArgument("too many arguments: 'extra_arg'")));
}

TEST_F(ToolTest, TestActionMissingRequiredArg) {
  NO_FATALS(RunActionMissingRequiredArg("master list", "master_addresses"));
  NO_FATALS(RunActionMissingRequiredArg("master add", "master_addresses"));
  NO_FATALS(RunActionMissingRequiredArg("master add master.example.com", "master_address"));
  NO_FATALS(RunActionMissingRequiredArg("master remove", "master_addresses"));
  NO_FATALS(RunActionMissingRequiredArg("master remove master.example.com", "master_address"));
  NO_FATALS(RunActionMissingRequiredArg("cluster ksck --master_addresses=master.example.com",
                                        "master_addresses"));
  NO_FATALS(RunActionMissingRequiredArg("local_replica cmeta rewrite_raft_config fake_id",
                                        "peers", /* variadic */ true));
}

TEST_F(ToolTest, TestFsCheck) {
  const string kTestDir = GetTestPath("test");
  const string kTabletId = "ffffffffffffffffffffffffffffffff";
  const Schema kSchema(GetSimpleTestSchema());
  const Schema kSchemaWithIds(SchemaBuilder(kSchema).Build());

  // Create a local replica, flush some rows a few times, and collect all
  // of the created block IDs.
  BlockIdContainer block_ids;
  {
    TabletHarness::Options opts(kTestDir);
    opts.tablet_id = kTabletId;
    TabletHarness harness(kSchemaWithIds, opts);
    ASSERT_OK(harness.Create(true));
    ASSERT_OK(harness.Open());
    LocalTabletWriter writer(harness.tablet().get(), &kSchema);
    KuduPartialRow row(&kSchemaWithIds);

    for (int num_flushes = 0; num_flushes < 10; num_flushes++) {
      for (int i = 0; i < 10; i++) {
        ASSERT_OK(row.SetInt32(0, num_flushes * i));
        ASSERT_OK(row.SetInt32(1, num_flushes * i * 10));
        ASSERT_OK(row.SetStringCopy(2, "HelloWorld"));
        writer.Insert(row);
      }
      harness.tablet()->Flush();
    }
    block_ids = harness.tablet()->metadata()->CollectBlockIds();
    harness.tablet()->Shutdown();
  }

  string encryption_args = env_->IsEncryptionEnabled() ? GetEncryptionArgs() : "";

  // Check the filesystem; all the blocks should be accounted for, and there
  // should be no blocks missing or orphaned.
  NO_FATALS(RunFsCheck(Substitute("fs check --fs_wal_dir=$0 $1", kTestDir, encryption_args),
                       block_ids.size(), kTabletId, {}, 0));

  // Delete half of the blocks. Upon the next check we can only find half, and
  // the other half are deemed missing.
  vector<BlockId> missing_ids;
  {
    FsManager fs(env_, FsManagerOpts(kTestDir));
    FsReport report;
    ASSERT_OK(fs.Open(&report));
    std::shared_ptr<BlockDeletionTransaction> deletion_transaction =
        fs.block_manager()->NewDeletionTransaction();
    int index = 0;
    for (const auto& block_id : block_ids) {
      if (index++ % 2 == 0) {
        deletion_transaction->AddDeletedBlock(block_id);
      }
    }
    deletion_transaction->CommitDeletedBlocks(&missing_ids);
  }
  NO_FATALS(RunFsCheck(Substitute("fs check --fs_wal_dir=$0 $1", kTestDir, encryption_args),
                       block_ids.size() / 2, kTabletId, missing_ids, 0));

  // Delete the tablet superblock. The next check finds half of the blocks,
  // though without the superblock they're all considered to be orphaned.
  //
  // Here we check twice to show that if --repair isn't provided, there should
  // be no effect.
  {
    FsManager fs(env_, FsManagerOpts(kTestDir));
    FsReport report;
    ASSERT_OK(fs.Open(&report));
    ASSERT_OK(env_->DeleteFile(fs.GetTabletMetadataPath(kTabletId)));
  }
  for (int i = 0; i < 2; i++) {
    NO_FATALS(RunFsCheck(Substitute("fs check --fs_wal_dir=$0 $1", kTestDir, encryption_args),
                         block_ids.size() / 2, kTabletId, {}, block_ids.size() / 2));
  }

  // Repair the filesystem. The remaining half of all blocks were found, deemed
  // to be orphaned, and deleted. The next check shows no remaining blocks.
  NO_FATALS(RunFsCheck(Substitute("fs check --fs_wal_dir=$0 $1 --repair", kTestDir,
                                  encryption_args),
                       block_ids.size() / 2, kTabletId, {}, block_ids.size() / 2));
  NO_FATALS(RunFsCheck(Substitute("fs check --fs_wal_dir=$0 $1", kTestDir, encryption_args),
                       0, kTabletId, {}, 0));
}

TEST_F(ToolTest, TestFsCheckLiveServer) {
  string encryption_args = env_->IsEncryptionEnabled() ? GetEncryptionArgs() : "";
  NO_FATALS(StartExternalMiniCluster());
  string args = Substitute("fs check --fs_wal_dir $0 --fs_data_dirs $1 $2",
                           cluster_->GetWalPath("master-0"),
                           JoinStrings(cluster_->GetDataPaths("master-0"), ","),
                           encryption_args);
  NO_FATALS(RunFsCheck(args, 0, "", {}, 0));
  args += " --repair";
  string stdout;
  string stderr;
  Status s = RunTool(args, &stdout, &stderr, nullptr, nullptr);
  SCOPED_TRACE(stdout);
  SCOPED_TRACE(stderr);
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_TRUE(stdout.empty());
  ASSERT_STR_CONTAINS(stderr, "Could not lock");
}

TEST_F(ToolTest, TestFsFormat) {
  const string kTestDir = GetTestPath("test");
  NO_FATALS(RunActionStdoutNone(Substitute("fs format --fs_wal_dir=$0", kTestDir)));
  FsManager fs(env_, FsManagerOpts(kTestDir));
  ASSERT_OK(fs.Open());

  ObjectIdGenerator generator;
  string canonicalized_uuid;
  ASSERT_OK(generator.Canonicalize(fs.uuid(), &canonicalized_uuid));
  ASSERT_EQ(fs.uuid(), canonicalized_uuid);
}

TEST_F(ToolTest, TestFsFormatWithUuid) {
  const string kTestDir = GetTestPath("test");
  ObjectIdGenerator generator;
  string original_uuid = generator.Next();
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs format --fs_wal_dir=$0 --uuid=$1", kTestDir, original_uuid)));
  FsManager fs(env_, FsManagerOpts(kTestDir));
  ASSERT_OK(fs.Open());

  string canonicalized_uuid;
  ASSERT_OK(generator.Canonicalize(fs.uuid(), &canonicalized_uuid));
  ASSERT_EQ(fs.uuid(), canonicalized_uuid);
  ASSERT_EQ(fs.uuid(), original_uuid);
}

TEST_F(ToolTest, TestFsFormatWithServerKey) {
  const string kTestDir = GetTestPath("test");
  ObjectIdGenerator generator;
  string original_uuid = generator.Next();
  string server_key = "00010203040506070809101112131442";
  string server_key_iv = "42141312111009080706050403020100";
  string server_key_version = "kuduclusterkey@0";
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs format --fs_wal_dir=$0 --uuid=$1 --server_key=$2 "
      "--server_key_iv=$3 --server_key_version=$4",
      kTestDir, original_uuid, server_key, server_key_iv, server_key_version)));
  FsManager fs(env_, FsManagerOpts(kTestDir));
  ASSERT_OK(fs.Open());

  string canonicalized_uuid;
  ASSERT_OK(generator.Canonicalize(fs.uuid(), &canonicalized_uuid));
  ASSERT_EQ(canonicalized_uuid, fs.uuid());
  ASSERT_EQ(original_uuid, fs.uuid());
  ASSERT_EQ(server_key, fs.server_key());
  ASSERT_EQ(server_key_iv, fs.server_key_iv());
  ASSERT_EQ(server_key_version, fs.server_key_version());
}

TEST_F(ToolTest, TestFsDumpUuid) {
  const string kTestDir = GetTestPath("test");
  string uuid;
  {
    FsManager fs(env_, FsManagerOpts(kTestDir));
    ASSERT_OK(fs.CreateInitialFileSystemLayout());
    ASSERT_OK(fs.Open());
    uuid = fs.uuid();
  }
  string stdout;
  NO_FATALS(RunActionStdoutString(Substitute(
      "fs dump uuid --fs_wal_dir=$0", kTestDir), &stdout));
  SCOPED_TRACE(stdout);
  ASSERT_EQ(uuid, stdout);
}

TEST_F(ToolTest, TestPbcTools) {
  // It's pointless to run these tests in an encrypted environment, as it uses
  // instance files to test pbc tools, which are not encrypted anyway. The tests
  // also make assumptions about the contents of the instance files, which are
  // different on encrypted servers, as they contain an extra server_key field,
  // which would make these tests break.
  if (FLAGS_encrypt_data_at_rest) {
    GTEST_SKIP();
  }
  const string kTestDir = GetTestPath("test");
  string uuid;
  string instance_path;
  {
    ObjectIdGenerator generator;
    FsManager fs(env_, FsManagerOpts(kTestDir));
    ASSERT_OK(fs.CreateInitialFileSystemLayout(generator.Next()));
    ASSERT_OK(fs.Open());
    uuid = fs.uuid();
    instance_path = fs.GetInstanceMetadataPath(kTestDir);
  }
  {
    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(Substitute(
        "pbc dump $0", instance_path), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_EQ(4, stdout.size());
    ASSERT_EQ("Message 0", stdout[0]);
    ASSERT_EQ("-------", stdout[1]);
    ASSERT_EQ(Substitute("uuid: \"$0\"", uuid), stdout[2]);
    ASSERT_STR_MATCHES(stdout[3], "^format_stamp: \"Formatted at .*\"$");
  }
  // Test dump --debug
  {
    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(Substitute(
        "pbc dump $0 --debug", instance_path), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_EQ(12, stdout.size());
    ASSERT_EQ("File header", stdout[0]);
    ASSERT_EQ("-------", stdout[1]);
    ASSERT_STR_MATCHES(stdout[2], "^Protobuf container version:");
    ASSERT_STR_MATCHES(stdout[3], "^Total container file size:");
    ASSERT_STR_MATCHES(stdout[4], "^Entry PB type:");
    ASSERT_EQ("Message 0", stdout[6]);
    ASSERT_STR_MATCHES(stdout[7], "^offset:");
    ASSERT_STR_MATCHES(stdout[8], "^length:");
    ASSERT_EQ("-------", stdout[9]);
    ASSERT_EQ(Substitute("uuid: \"$0\"", uuid), stdout[10]);
    ASSERT_STR_MATCHES(stdout[11], "^format_stamp: \"Formatted at .*\"$");
  }
  // Test dump --oneline
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(Substitute(
        "pbc dump $0 --oneline", instance_path), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_STR_MATCHES(stdout, Substitute(
        "^0\tuuid: \"$0\" format_stamp: \"Formatted at .*\"$$", uuid));
  }
  // Test dump --json
  {
    // Since the UUID is listed as 'bytes' rather than 'string' in the PB, it dumps
    // base64-encoded.
    string uuid_b64;
    Base64Encode(uuid, &uuid_b64);

    string stdout;
    NO_FATALS(RunActionStdoutString(Substitute(
        "pbc dump $0 --json", instance_path), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_STR_MATCHES(stdout, Substitute(
        R"(^\{"uuid":"$0","formatStamp":"Formatted at .*"\}$$)", uuid_b64));
  }
  // Test dump --json_pretty
  {
    // Since the UUID is listed as 'bytes' rather than 'string' in the PB, it dumps
    // base64-encoded.
    string uuid_b64;
    Base64Encode(uuid, &uuid_b64);

    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(Substitute(
        "pbc dump $0 --json_pretty", instance_path), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_EQ(4, stdout.size());
    ASSERT_EQ("{", stdout[0]);
    ASSERT_EQ(Substitute(R"( "uuid": "$0",)", uuid_b64), stdout[1]);
    ASSERT_STR_MATCHES(stdout[2], R"( "formatStamp": "Formatted at .*")");
    ASSERT_EQ("}", stdout[3]);
  }

  // Utility to set the editor up based on the given shell command.
  auto DoEdit = [&](const string& editor_shell, string* stdout, string* stderr = nullptr,
      const string& extra_flags = "") {
    const string editor_path = GetTestPath("editor");
    CHECK_OK(WriteStringToFile(Env::Default(),
                               StrCat("#!/usr/bin/env bash\n", editor_shell),
                               editor_path));
    chmod(editor_path.c_str(), 0755);
    setenv("EDITOR", editor_path.c_str(), /* overwrite */1);
    return RunTool(Substitute("pbc edit $0 $1", extra_flags, instance_path),
                   stdout, stderr, nullptr, nullptr);
  };

  // Test 'edit' by setting up EDITOR to be a sed script which performs a substitution.
  {
    string stdout;
    ASSERT_OK(DoEdit("exec sed -i -e s/Formatted/Edited/ \"$@\"\n", &stdout, nullptr,
                     "--nobackup"));
    ASSERT_EQ("", stdout);

    // Dump to make sure the edit took place.
    NO_FATALS(RunActionStdoutString(Substitute(
        "pbc dump $0 --oneline", instance_path), &stdout));
    ASSERT_STR_MATCHES(stdout, Substitute(
        "^0\tuuid: \"$0\" format_stamp: \"Edited at .*\"$$", uuid));

    // Make sure no backup file was written.
    bool found_backup;
    ASSERT_OK(HasAtLeastOneBackupFile(kTestDir, &found_backup));
    ASSERT_FALSE(found_backup);
  }

  // Test 'edit' by setting --json_pretty flag.
  {
    string stdout;
    ASSERT_OK(DoEdit("exec sed -i -e s/Edited/Formatted/ \"$@\"\n", &stdout, nullptr,
                     "--nobackup --json_pretty"));
    ASSERT_EQ("", stdout);

    // Dump to make sure the edit took place.
    NO_FATALS(RunActionStdoutString(Substitute(
        "pbc dump $0 --oneline", instance_path), &stdout));
    ASSERT_STR_MATCHES(stdout, Substitute(
        "^0\tuuid: \"$0\" format_stamp: \"Formatted at .*\"$$", uuid));

    // Make sure no backup file was written.
    bool found_backup;
    ASSERT_OK(HasAtLeastOneBackupFile(kTestDir, &found_backup));
    ASSERT_FALSE(found_backup);
  }

  // Test 'edit' with a backup.
  {
    string stdout;
    ASSERT_OK(DoEdit("exec sed -i -e s/Formatted/Edited/ \"$@\"\n", &stdout));
    ASSERT_EQ("", stdout);

    // Make sure a backup file was written.
    bool found_backup;
    ASSERT_OK(HasAtLeastOneBackupFile(kTestDir, &found_backup));
    ASSERT_TRUE(found_backup);
  }

  // Test 'edit' with an unsuccessful edit.
  {
    string stdout, stderr;
    string path;
    ASSERT_OK(FindExecutable("false", {}, &path));
    Status s = DoEdit(path, &stdout, &stderr);
    ASSERT_FALSE(s.ok());
    ASSERT_EQ("", stdout);
    ASSERT_STR_CONTAINS(stderr, "Aborted: editor returned non-zero exit code");
  }

  // Test 'edit' with an edit which tries to write some invalid JSON (missing required fields).
  {
    string stdout, stderr;
    Status s = DoEdit("echo {} > $@\n", &stdout, &stderr);
    ASSERT_EQ("", stdout);
    ASSERT_STR_MATCHES(stderr,
                       "Invalid argument: Unable to parse JSON text: \\{\\}: "
                       ": missing field .*");
    // NOTE: the above extra ':' is due to an apparent bug in protobuf.
  }

  // Test 'edit' with an edit that writes some invalid JSON (bad syntax)
  {
    string stdout, stderr;
    Status s = DoEdit("echo not-a-json-string > $@\n", &stdout, &stderr);
    ASSERT_EQ("", stdout);
    ASSERT_STR_CONTAINS(stderr, "Corruption: JSON text is corrupt: Invalid value.");
  }

  // The file should be unchanged by the unsuccessful edits above.
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(Substitute(
        "pbc dump $0 --oneline", instance_path), &stdout));
    ASSERT_STR_MATCHES(stdout, Substitute(
        "^0\tuuid: \"$0\" format_stamp: \"Edited at .*\"$$", uuid));
  }
}

TEST_F(ToolTest, TestPbcToolsOnMultipleBlocks) {
  const int kNumCFileBlocks = 1024;
  const int kNumEntries = 1;
  const string kTestDir = GetTestPath("test");
  const string kDataDir = JoinPathSegments(kTestDir, "data");

  // Generate a block container metadata file.
  string metadata_path;
  string encryption_args;
  {
    // Open FsManager to write file later.
    FsManager fs(env_, FsManagerOpts(kTestDir));
    ASSERT_OK(fs.CreateInitialFileSystemLayout());
    ASSERT_OK(fs.Open());

    // Write multiple CFile blocks to file.
    for (int i = 0; i < kNumCFileBlocks; ++i) {
      unique_ptr<WritableBlock> block;
      ASSERT_OK(fs.CreateNewBlock({}, &block));
      StringDataGenerator<false> generator("hello %04d");
      WriterOptions opts;
      opts.write_posidx = true;
      CFileWriter writer(
          opts, GetTypeInfo(generator.kDataType), generator.has_nulls(), std::move(block));
      ASSERT_OK(writer.Start());
      generator.Build(kNumEntries);
      ASSERT_OK_FAST(writer.AppendEntries(generator.values(), kNumEntries));
      ASSERT_OK(writer.Finish());
    }

    // Find the CFile metadata file.
    vector<string> children;
    ASSERT_OK(env_->GetChildren(kDataDir, &children));
    vector<string> metadata_files;
    for (const string& child : children) {
      if (HasSuffixString(child, LogBlockManager::kContainerMetadataFileSuffix)) {
        metadata_files.push_back(JoinPathSegments(kDataDir, child));
      }
    }
    ASSERT_EQ(1, metadata_files.size());
    metadata_path = metadata_files[0];

    if (env_->IsEncryptionEnabled()) {
      encryption_args = GetEncryptionArgs() + " --instance_file=" +
          fs.GetInstanceMetadataPath(kTestDir);
      }
  }

  // Test default dump
  {
    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(Substitute(
        "pbc dump $0 $1", metadata_path, encryption_args), &stdout));
    ASSERT_EQ(kNumCFileBlocks * 10 - 1, stdout.size());
    ASSERT_EQ("Message 0", stdout[0]);
    ASSERT_EQ("-------", stdout[1]);
    ASSERT_EQ("block_id {", stdout[2]);
    ASSERT_STR_MATCHES(stdout[3], "^  id: [0-9]+$");
    ASSERT_EQ("}", stdout[4]);
    ASSERT_EQ("op_type: CREATE", stdout[5]);
    ASSERT_STR_MATCHES(stdout[6], "^timestamp_us: [0-9]+$");
    ASSERT_STR_MATCHES(stdout[7], "^offset: [0-9]+$");
    ASSERT_EQ("length: 153", stdout[8]);
    ASSERT_EQ("", stdout[9]);
  }

  // Test dump --debug
  {
    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(Substitute(
        "pbc dump $0 $1 --debug", metadata_path, encryption_args), &stdout));
    ASSERT_EQ(kNumCFileBlocks * 12 + 5, stdout.size());
    // Header
    ASSERT_EQ("File header", stdout[0]);
    ASSERT_EQ("-------", stdout[1]);
    ASSERT_STR_MATCHES(stdout[2], "^Protobuf container version: [0-9]+$");
    ASSERT_STR_MATCHES(stdout[3], "^Total container file size: [0-9]+$");
    ASSERT_EQ("Entry PB type: kudu.BlockRecordPB", stdout[4]);
    ASSERT_EQ("", stdout[5]);
    // The first block
    ASSERT_EQ("Message 0", stdout[6]);
    ASSERT_STR_MATCHES(stdout[7], "^offset: [0-9]+$");
    ASSERT_STR_MATCHES(stdout[8], "^length: [0-9]+$");
    ASSERT_EQ("-------", stdout[9]);
    ASSERT_EQ("block_id {", stdout[10]);
    ASSERT_STR_MATCHES(stdout[11], "^  id: [0-9]+$");
    ASSERT_EQ("}", stdout[12]);
    ASSERT_EQ("op_type: CREATE", stdout[13]);
    ASSERT_STR_MATCHES(stdout[14], "^timestamp_us: [0-9]+$");
    ASSERT_STR_MATCHES(stdout[15], "^offset: [0-9]+$");
    ASSERT_EQ("length: 153", stdout[16]);
    ASSERT_EQ("", stdout[17]);
  }

  // Test dump --oneline
  {
    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(Substitute(
        "pbc dump $0 $1 --oneline", metadata_path, encryption_args), &stdout));
    ASSERT_EQ(kNumCFileBlocks, stdout.size());
    ASSERT_STR_MATCHES(stdout[0],
        "^0\tblock_id \\{ id: [0-9]+ \\} op_type: CREATE "
        "timestamp_us: [0-9]+ offset: [0-9]+ length: 153$");
  }

  // Test dump --json
  {
    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(Substitute(
        "pbc dump $0 $1 --json", metadata_path, encryption_args), &stdout));
    ASSERT_EQ(kNumCFileBlocks, stdout.size());
    ASSERT_STR_MATCHES(stdout[0],
        R"(^\{"blockId":\{"id":"[0-9]+"\},"opType":"CREATE",)"
        R"("timestampUs":"[0-9]+","offset":"[0-9]+","length":"153"\}$$)");
  }

  // Test dump --json_pretty
  {
    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(Substitute(
        "pbc dump $0 $1 --json_pretty", metadata_path, encryption_args), &stdout));
    ASSERT_EQ(kNumCFileBlocks * 10 - 1, stdout.size());
    ASSERT_EQ("{", stdout[0]);
    ASSERT_EQ(R"( "blockId": {)", stdout[1]);
    ASSERT_STR_MATCHES(stdout[2], R"(  "id": "[0-9]+")");
    ASSERT_EQ(" },", stdout[3]);
    ASSERT_EQ(R"( "opType": "CREATE",)", stdout[4]);
    ASSERT_STR_MATCHES(stdout[5], R"( "timestampUs": "[0-9]+",)");
    ASSERT_STR_MATCHES(stdout[6], R"( "offset": "[0-9]+",)");
    ASSERT_EQ(R"( "length": "153")", stdout[7]);
    ASSERT_EQ(R"(})", stdout[8]);
    ASSERT_EQ("", stdout[9]);
  }

  // Utility to set the editor up based on the given shell command.
  auto DoEdit = [&](const string& editor_shell, string* stdout, string* stderr = nullptr,
      const string& extra_flags = "") {
    const string editor_path = GetTestPath("editor");
    CHECK_OK(WriteStringToFile(Env::Default(),
                               StrCat("#!/usr/bin/env bash\n", editor_shell),
                               editor_path));
    chmod(editor_path.c_str(), 0755);
    setenv("EDITOR", editor_path.c_str(), /* overwrite */1);
    return RunTool(Substitute("pbc edit $0 $1 $2", extra_flags, metadata_path, encryption_args),
                   stdout, stderr, nullptr, nullptr);
  };

  // Test 'edit' by setting up EDITOR to be a sed script which performs a substitution.
  {
    string stdout;
    ASSERT_OK(DoEdit("exec sed -i -e s/CREATE/DELETE/ \"$@\"\n", &stdout, nullptr,
                     "--nobackup"));
    ASSERT_EQ("", stdout);

    // Dump to make sure the edit took place.
    vector<string> stdout_lines;
    NO_FATALS(RunActionStdoutLines(Substitute(
        "pbc dump $0 $1 --oneline", metadata_path, encryption_args), &stdout_lines));
    ASSERT_EQ(kNumCFileBlocks, stdout_lines.size());
    ASSERT_STR_MATCHES(stdout_lines[0],
                       "^0\tblock_id \\{ id: [0-9]+ \\} op_type: DELETE "
                       "timestamp_us: [0-9]+ offset: [0-9]+ length: 153$");

    // Make sure no backup file was written.
    bool found_backup;
    ASSERT_OK(HasAtLeastOneBackupFile(kDataDir, &found_backup));
    ASSERT_FALSE(found_backup);
  }

  // Test 'edit' by setting --json_pretty flag.
  {
    string stdout;
    ASSERT_OK(DoEdit("exec sed -i -e s/DELETE/CREATE/ \"$@\"\n", &stdout, nullptr,
                     "--nobackup --json_pretty"));
    ASSERT_EQ("", stdout);

    // Dump to make sure the edit took place.
    vector<string> stdout_lines;
    NO_FATALS(RunActionStdoutLines(Substitute(
        "pbc dump $0 $1 --oneline", metadata_path, encryption_args), &stdout_lines));
    ASSERT_EQ(kNumCFileBlocks, stdout_lines.size());
    ASSERT_STR_MATCHES(stdout_lines[0],
                       "^0\tblock_id \\{ id: [0-9]+ \\} op_type: CREATE "
                       "timestamp_us: [0-9]+ offset: [0-9]+ length: 153$");

    // Make sure no backup file was written.
    bool found_backup;
    ASSERT_OK(HasAtLeastOneBackupFile(kDataDir, &found_backup));
    ASSERT_FALSE(found_backup);
  }

  // Test 'edit' with a backup.
  {
    string stdout;
    ASSERT_OK(DoEdit("exec sed -i -e s/CREATE/DELETE/ \"$@\"\n", &stdout));
    ASSERT_EQ("", stdout);

    // Make sure a backup file was written.
    bool found_backup;
    ASSERT_OK(HasAtLeastOneBackupFile(kDataDir, &found_backup));
    ASSERT_TRUE(found_backup);
  }

  // Test 'edit' with an unsuccessful edit.
  {
    string stdout, stderr;
    string path;
    ASSERT_OK(FindExecutable("false", {}, &path));
    Status s = DoEdit(path, &stdout, &stderr);
    ASSERT_FALSE(s.ok());
    ASSERT_EQ("", stdout);
    ASSERT_STR_CONTAINS(stderr, "Aborted: editor returned non-zero exit code");
  }

  // Test 'edit' with an edit which tries to write some invalid JSON (missing required fields).
  {
    string stdout, stderr;
    Status s = DoEdit("echo {} > $@\n", &stdout, &stderr);
    ASSERT_EQ("", stdout);
    ASSERT_STR_MATCHES(stderr,
                       "Invalid argument: Unable to parse JSON text: \\{\\}: "
                       ": missing field .*");
    // NOTE: the above extra ':' is due to an apparent bug in protobuf.
  }

  // Test 'edit' with an edit that writes some invalid JSON (bad syntax)
  {
    string stdout, stderr;
    Status s = DoEdit("echo not-a-json-string > $@\n", &stdout, &stderr);
    ASSERT_EQ("", stdout);
    ASSERT_STR_CONTAINS(stderr, "Corruption: JSON text is corrupt: Invalid value.");
  }

  // The file should be unchanged by the unsuccessful edits above.
  {
    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(Substitute(
        "pbc dump $0 $1 --oneline", metadata_path, encryption_args), &stdout));
    ASSERT_EQ(kNumCFileBlocks, stdout.size());
    ASSERT_STR_MATCHES(stdout[0],
                       "^0\tblock_id \\{ id: [0-9]+ \\} op_type: DELETE "
                       "timestamp_us: [0-9]+ offset: [0-9]+ length: 153$");
  }
}

TEST_F(ToolTest, TestFsDumpCFile) {
  const int kNumEntries = 8192;
  const string kTestDir = GetTestPath("test");
  FsManager fs(env_, FsManagerOpts(kTestDir));
  ASSERT_OK(fs.CreateInitialFileSystemLayout());
  ASSERT_OK(fs.Open());

  unique_ptr<WritableBlock> block;
  ASSERT_OK(fs.CreateNewBlock({}, &block));
  BlockId block_id = block->id();
  StringDataGenerator<false> generator("hello %04d");
  WriterOptions opts;
  opts.write_posidx = true;
  CFileWriter writer(opts, GetTypeInfo(generator.kDataType),
                     generator.has_nulls(), std::move(block));
  ASSERT_OK(writer.Start());
  generator.Build(kNumEntries);
  ASSERT_OK_FAST(writer.AppendEntries(generator.values(), kNumEntries));
  ASSERT_OK(writer.Finish());

  string encryption_args = env_->IsEncryptionEnabled() ? GetEncryptionArgs() : "";

  {
    NO_FATALS(RunActionStdoutNone(Substitute(
        "fs dump cfile --fs_wal_dir=$0 $1 $2 --noprint_meta --noprint_rows",
        kTestDir, block_id.ToString(), encryption_args)));
  }
  vector<string> stdout;
  {
    NO_FATALS(RunActionStdoutLines(Substitute(
        "fs dump cfile --fs_wal_dir=$0 $1 $2 --noprint_rows",
        kTestDir, block_id.ToString(), encryption_args), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_GE(stdout.size(), 4);
    ASSERT_EQ(stdout[0], "Header:");
    ASSERT_EQ(stdout[2], "Footer:");
  }
  {
    NO_FATALS(RunActionStdoutLines(Substitute(
        "fs dump cfile --fs_wal_dir=$0 $1 $2 --noprint_meta",
        kTestDir, block_id.ToString(), encryption_args), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_EQ(kNumEntries, stdout.size());
  }
  {
    NO_FATALS(RunActionStdoutLines(Substitute(
        "fs dump cfile --fs_wal_dir=$0 $1 $2",
        kTestDir, block_id.ToString(), encryption_args), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_GT(stdout.size(), kNumEntries);
    ASSERT_EQ(stdout[0], "Header:");
    ASSERT_EQ(stdout[2], "Footer:");
  }
}

TEST_F(ToolTest, TestFsDumpBlock) {
  const string kTestDir = GetTestPath("test");
  FsManager fs(env_, FsManagerOpts(kTestDir));
  ASSERT_OK(fs.CreateInitialFileSystemLayout());
  ASSERT_OK(fs.Open());

  unique_ptr<WritableBlock> block;
  ASSERT_OK(fs.CreateNewBlock({}, &block));
  ASSERT_OK(block->Append("hello world"));
  ASSERT_OK(block->Close());
  BlockId block_id = block->id();

  {
    string stdout;
    NO_FATALS(RunActionStdoutString(Substitute(
        "fs dump block --fs_wal_dir=$0 $1 $2",
        kTestDir, block_id.ToString(),
        env_->IsEncryptionEnabled() ? GetEncryptionArgs() : ""), &stdout));
    ASSERT_EQ("hello world", stdout);
  }
}

TEST_F(ToolTest, TestWalDump) {
  const string kTestDir = GetTestPath("test");
  const string kTestTablet = "ffffffffffffffffffffffffffffffff";
  const Schema kSchema(GetSimpleTestSchema());
  const Schema kSchemaWithIds(SchemaBuilder(kSchema).Build());

  FsManager fs(env_, FsManagerOpts(kTestDir));
  ASSERT_OK(fs.CreateInitialFileSystemLayout());
  ASSERT_OK(fs.Open());

  {
    scoped_refptr<Log> log;
    ASSERT_OK(Log::Open(LogOptions(),
                        &fs,
                        /*file_cache*/nullptr,
                        kTestTablet,
                        kSchemaWithIds,
                        0, // schema_version
                        /*metric_entity*/nullptr,
                        &log));

    OpId opid = consensus::MakeOpId(1, 1);
    ReplicateRefPtr replicate =
        consensus::make_scoped_refptr_replicate(new ReplicateMsg());
    replicate->get()->set_op_type(consensus::WRITE_OP);
    replicate->get()->mutable_id()->CopyFrom(opid);
    replicate->get()->set_timestamp(1);
    WriteRequestPB* write = replicate->get()->mutable_write_request();
    ASSERT_OK(SchemaToPB(kSchema, write->mutable_schema()));
    AddTestRowToPB(RowOperationsPB::INSERT, kSchema,
                   opid.index(),
                   0,
                   "this is a test insert",
                   write->mutable_row_operations());
    write->set_tablet_id(kTestTablet);
    Synchronizer s;
    ASSERT_OK(log->AsyncAppendReplicates({ replicate }, s.AsStatusCallback()));
    ASSERT_OK(s.Wait());
  }

  string wal_path = fs.GetWalSegmentFileName(kTestTablet, 1);
  string encryption_args;
  if (env_->IsEncryptionEnabled()) {
    encryption_args = GetEncryptionArgs() + " --instance_file=" +
        fs.GetInstanceMetadataPath(kTestDir);
  }
  string stdout;
  for (const auto& args : { Substitute("wal dump $0 $1", wal_path, encryption_args),
                            Substitute("local_replica dump wals --fs_wal_dir=$0 $1 $2",
                                       kTestDir, kTestTablet, encryption_args)
                           }) {
    SCOPED_TRACE(args);
    for (const auto& print_entries : { "true", "1", "yes", "decoded" }) {
      SCOPED_TRACE(print_entries);
      NO_FATALS(RunActionStdoutString(Substitute("$0 --print_entries=$1",
                                                 args, print_entries), &stdout));
      SCOPED_TRACE(stdout);
      ASSERT_STR_MATCHES(stdout, "Header:");
      ASSERT_STR_MATCHES(stdout, "1\\.1@1");
      ASSERT_STR_MATCHES(stdout, "this is a test insert");
      ASSERT_STR_NOT_MATCHES(stdout, "t<truncated>");
      ASSERT_STR_NOT_MATCHES(stdout, "row_operations \\{");
      ASSERT_STR_MATCHES(stdout, "Footer:");
    }
    for (const auto& print_entries : { "false", "0", "no" }) {
      SCOPED_TRACE(print_entries);
      NO_FATALS(RunActionStdoutString(Substitute("$0 --print_entries=$1",
                                                 args, print_entries), &stdout));
      SCOPED_TRACE(stdout);
      ASSERT_STR_MATCHES(stdout, "Header:");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.1@1");
      ASSERT_STR_NOT_MATCHES(stdout, "this is a test insert");
      ASSERT_STR_NOT_MATCHES(stdout, "t<truncated>");
      ASSERT_STR_NOT_MATCHES(stdout, "row_operations \\{");
      ASSERT_STR_MATCHES(stdout, "Footer:");
    }
    {
      NO_FATALS(RunActionStdoutString(Substitute("$0 --print_entries=pb",
                                                 args), &stdout));
      SCOPED_TRACE(stdout);
      ASSERT_STR_MATCHES(stdout, "Header:");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.1@1");
      ASSERT_STR_MATCHES(stdout, "this is a test insert");
      ASSERT_STR_NOT_MATCHES(stdout, "t<truncated>");
      ASSERT_STR_MATCHES(stdout, "row_operations \\{");
      ASSERT_STR_MATCHES(stdout, "Footer:");
    }
    {
      NO_FATALS(RunActionStdoutString(Substitute(
          "$0 --print_entries=pb --truncate_data=1", args), &stdout));
      SCOPED_TRACE(stdout);
      ASSERT_STR_MATCHES(stdout, "Header:");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.1@1");
      ASSERT_STR_NOT_MATCHES(stdout, "this is a test insert");
      ASSERT_STR_MATCHES(stdout, "t<truncated>");
      ASSERT_STR_MATCHES(stdout, "row_operations \\{");
      ASSERT_STR_MATCHES(stdout, "Footer:");
    }
    {
      NO_FATALS(RunActionStdoutString(Substitute(
          "$0 --print_entries=id", args), &stdout));
      SCOPED_TRACE(stdout);
      ASSERT_STR_MATCHES(stdout, "Header:");
      ASSERT_STR_MATCHES(stdout, "1\\.1@1");
      ASSERT_STR_NOT_MATCHES(stdout, "this is a test insert");
      ASSERT_STR_NOT_MATCHES(stdout, "t<truncated>");
      ASSERT_STR_NOT_MATCHES(stdout, "row_operations \\{");
      ASSERT_STR_MATCHES(stdout, "Footer:");
    }
    {
      NO_FATALS(RunActionStdoutString(Substitute(
          "$0 --print_meta=false", args), &stdout));
      SCOPED_TRACE(stdout);
      ASSERT_STR_NOT_MATCHES(stdout, "Header:");
      ASSERT_STR_MATCHES(stdout, "1\\.1@1");
      ASSERT_STR_MATCHES(stdout, "this is a test insert");
      ASSERT_STR_NOT_MATCHES(stdout, "row_operations \\{");
      ASSERT_STR_NOT_MATCHES(stdout, "Footer:");
    }
  }
}

TEST_F(ToolTest, TestWalDumpWithAlterSchema) {
  const string kTestDir = GetTestPath("test");
  const string kTestTablet = "ffffffffffffffffffffffffffffffff";
  Schema schema(GetSimpleTestSchema());
  Schema schema_with_ids(SchemaBuilder(schema).Build());

  FsManager fs(env_, FsManagerOpts(kTestDir));
  ASSERT_OK(fs.CreateInitialFileSystemLayout());
  ASSERT_OK(fs.Open());

  const std::string kFirstMessage("this is a test insert");
  const std::string kAddColumnName1("addcolumn1_addcolumn1");
  const std::string kAddColumnName2("addcolumn2_addcolumn2");
  const std::string kAddColumnName1Message("insert a record, after alter schema");
  const std::string kAddColumnName2Message("upsert a record, after alter schema");
  {
    scoped_refptr<Log> log;
    ASSERT_OK(Log::Open(LogOptions(),
                        &fs,
                        /*file_cache*/nullptr,
                        kTestTablet,
                        schema_with_ids,
                        0, // schema_version
                        /*metric_entity*/nullptr,
                        &log));

    std::vector<ReplicateRefPtr> replicates;
    {
      OpId opid = consensus::MakeOpId(1, 1);
      ReplicateRefPtr replicate =
          consensus::make_scoped_refptr_replicate(new ReplicateMsg());
      replicate->get()->set_op_type(consensus::WRITE_OP);
      replicate->get()->mutable_id()->CopyFrom(opid);
      replicate->get()->set_timestamp(1);
      WriteRequestPB* write = replicate->get()->mutable_write_request();
      ASSERT_OK(SchemaToPB(schema, write->mutable_schema()));
      AddTestRowToPB(RowOperationsPB::INSERT, schema,
                     opid.index(), 0, kFirstMessage,
                     write->mutable_row_operations());
      write->set_tablet_id(kTestTablet);
      replicates.emplace_back(replicate);
    }
    {
      OpId opid = consensus::MakeOpId(1, 2);
      ReplicateRefPtr replicate =
          consensus::make_scoped_refptr_replicate(new ReplicateMsg());
      replicate->get()->set_op_type(consensus::ALTER_SCHEMA_OP);
      replicate->get()->mutable_id()->CopyFrom(opid);
      replicate->get()->set_timestamp(2);
      tserver::AlterSchemaRequestPB* alter_schema =
          replicate->get()->mutable_alter_schema_request();
      SchemaBuilder schema_builder = SchemaBuilder(schema);
      ASSERT_OK(schema_builder.AddColumn(kAddColumnName1, STRING, true, nullptr, nullptr));
      ASSERT_OK(schema_builder.AddColumn(kAddColumnName2, STRING, true, nullptr, nullptr));
      schema = schema_builder.BuildWithoutIds();
      schema_with_ids = SchemaBuilder(schema).Build();
      ASSERT_OK(SchemaToPB(schema_with_ids, alter_schema->mutable_schema()));
      alter_schema->set_tablet_id(kTestTablet);
      alter_schema->set_schema_version(1);
      replicates.emplace_back(replicate);
    }
    {
      OpId opid = consensus::MakeOpId(1, 3);
      ReplicateRefPtr replicate =
          consensus::make_scoped_refptr_replicate(new ReplicateMsg());
      replicate->get()->set_op_type(consensus::WRITE_OP);
      replicate->get()->mutable_id()->CopyFrom(opid);
      replicate->get()->set_timestamp(3);
      WriteRequestPB* write = replicate->get()->mutable_write_request();
      ASSERT_OK(SchemaToPB(schema, write->mutable_schema()));
      AddTestRowToPBAppendColumns(RowOperationsPB::INSERT, schema,
                                  opid.index(), 2, kFirstMessage,
                                  {{kAddColumnName1, kAddColumnName1Message}},
                                  write->mutable_row_operations());
      write->set_tablet_id(kTestTablet);
      replicates.emplace_back(replicate);
    }
    {
      OpId opid = consensus::MakeOpId(1, 4);
      ReplicateRefPtr replicate =
          consensus::make_scoped_refptr_replicate(new ReplicateMsg());
      replicate->get()->set_op_type(consensus::WRITE_OP);
      replicate->get()->mutable_id()->CopyFrom(opid);
      replicate->get()->set_timestamp(4);
      WriteRequestPB* write = replicate->get()->mutable_write_request();
      ASSERT_OK(SchemaToPB(schema, write->mutable_schema()));
      AddTestRowToPBAppendColumns(RowOperationsPB::UPSERT, schema,
                                  1, 1111, kFirstMessage,
                                  {
                                  {kAddColumnName1, kAddColumnName1Message},
                                  {kAddColumnName2, kAddColumnName2Message},
                                  },
                                  write->mutable_row_operations());
      write->set_tablet_id(kTestTablet);
      replicates.emplace_back(replicate);
    }

    Synchronizer s;
    ASSERT_OK(log->AsyncAppendReplicates(replicates, s.AsStatusCallback()));
    ASSERT_OK(s.Wait());
  }

  string wal_path = fs.GetWalSegmentFileName(kTestTablet, 1);
  string stdout;
  string encryption_args;
  if (env_->IsEncryptionEnabled()) {
    encryption_args = GetEncryptionArgs() + " --instance_file=" +
        fs.GetInstanceMetadataPath(kTestDir);
  }
  for (const auto& args : { Substitute("wal dump $0 $1", encryption_args, wal_path),
                            Substitute("local_replica dump wals --fs_wal_dir=$0 $1 $2",
                                       kTestDir, encryption_args, kTestTablet)
                           }) {
    SCOPED_TRACE(args);
    for (const auto& print_entries : { "true", "1", "yes", "decoded" }) {
      SCOPED_TRACE(print_entries);
      NO_FATALS(RunActionStdoutString(Substitute("$0 --print_entries=$1",
                                                 args, print_entries), &stdout));
      SCOPED_TRACE(stdout);
      ASSERT_STR_MATCHES(stdout, "Header:");
      ASSERT_STR_MATCHES(stdout, "1\\.1@1");
      ASSERT_STR_MATCHES(stdout, "1\\.2@2");
      ASSERT_STR_MATCHES(stdout, "1\\.3@3");
      ASSERT_STR_MATCHES(stdout, "1\\.4@4");
      ASSERT_STR_MATCHES(stdout, kFirstMessage);
      ASSERT_STR_MATCHES(stdout, kAddColumnName1);
      ASSERT_STR_MATCHES(stdout, "ALTER_SCHEMA_OP");
      ASSERT_STR_MATCHES(stdout, kAddColumnName1Message);
      ASSERT_STR_MATCHES(stdout, kAddColumnName2Message);
      ASSERT_STR_NOT_MATCHES(stdout, "t<truncated>");
      ASSERT_STR_NOT_MATCHES(stdout, "row_operations \\{");
      ASSERT_STR_MATCHES(stdout, "Footer:");
    }
    for (const auto& print_entries : { "false", "0", "no" }) {
      SCOPED_TRACE(print_entries);
      NO_FATALS(RunActionStdoutString(Substitute("$0 --print_entries=$1",
                                                 args, print_entries), &stdout));
      SCOPED_TRACE(stdout);
      ASSERT_STR_MATCHES(stdout, "Header:");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.1@1");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.2@2");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.3@3");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.4@4");
      ASSERT_STR_NOT_MATCHES(stdout, kFirstMessage);
      ASSERT_STR_NOT_MATCHES(stdout, kAddColumnName1);
      ASSERT_STR_NOT_MATCHES(stdout, "ALTER_SCHEMA_OP");
      ASSERT_STR_NOT_MATCHES(stdout, kAddColumnName1Message);
      ASSERT_STR_NOT_MATCHES(stdout, kAddColumnName2Message);
      ASSERT_STR_NOT_MATCHES(stdout, "t<truncated>");
      ASSERT_STR_NOT_MATCHES(stdout, "row_operations \\{");
      ASSERT_STR_MATCHES(stdout, "Footer:");
    }
    {
      NO_FATALS(RunActionStdoutString(Substitute("$0 --print_entries=pb",
                                                 args), &stdout));
      SCOPED_TRACE(stdout);
      ASSERT_STR_MATCHES(stdout, "Header:");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.1@1");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.2@2");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.3@3");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.4@4");
      ASSERT_STR_MATCHES(stdout, kFirstMessage);
      ASSERT_STR_MATCHES(stdout, kAddColumnName1);
      ASSERT_STR_MATCHES(stdout, "ALTER_SCHEMA_OP");
      ASSERT_STR_MATCHES(stdout, kAddColumnName1Message);
      ASSERT_STR_MATCHES(stdout, kAddColumnName2Message);
      ASSERT_STR_NOT_MATCHES(stdout, "t<truncated>");
      ASSERT_STR_MATCHES(stdout, "row_operations \\{");
      ASSERT_STR_MATCHES(stdout, "Footer:");
    }
    {
      NO_FATALS(RunActionStdoutString(Substitute(
          "$0 --print_entries=pb --truncate_data=1", args), &stdout));
      SCOPED_TRACE(stdout);
      ASSERT_STR_MATCHES(stdout, "Header:");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.1@1");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.2@2");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.3@3");
      ASSERT_STR_NOT_MATCHES(stdout, "1\\.4@4");
      ASSERT_STR_NOT_MATCHES(stdout, kFirstMessage);
      ASSERT_STR_NOT_MATCHES(stdout, kAddColumnName1);
      ASSERT_STR_NOT_MATCHES(stdout, kAddColumnName1Message);
      ASSERT_STR_NOT_MATCHES(stdout, kAddColumnName2Message);
      ASSERT_STR_MATCHES(stdout, "t<truncated>");
      ASSERT_STR_MATCHES(stdout, "row_operations \\{");
      ASSERT_STR_MATCHES(stdout, "Footer:");
    }
    {
      NO_FATALS(RunActionStdoutString(Substitute(
          "$0 --print_entries=id", args), &stdout));
      SCOPED_TRACE(stdout);
      ASSERT_STR_MATCHES(stdout, "Header:");
      ASSERT_STR_MATCHES(stdout, "1\\.1@1");
      ASSERT_STR_MATCHES(stdout, "1\\.2@2");
      ASSERT_STR_MATCHES(stdout, "1\\.3@3");
      ASSERT_STR_MATCHES(stdout, "1\\.4@4");
      ASSERT_STR_NOT_MATCHES(stdout, kFirstMessage);
      ASSERT_STR_NOT_MATCHES(stdout, kAddColumnName1);
      ASSERT_STR_NOT_MATCHES(stdout, kAddColumnName1Message);
      ASSERT_STR_NOT_MATCHES(stdout, kAddColumnName2Message);
      ASSERT_STR_NOT_MATCHES(stdout, "t<truncated>");
      ASSERT_STR_NOT_MATCHES(stdout, "row_operations \\{");
      ASSERT_STR_MATCHES(stdout, "Footer:");
    }
    {
      NO_FATALS(RunActionStdoutString(Substitute(
          "$0 --print_meta=false", args), &stdout));
      SCOPED_TRACE(stdout);
      ASSERT_STR_NOT_MATCHES(stdout, "Header:");
      ASSERT_STR_MATCHES(stdout, "1\\.1@1");
      ASSERT_STR_MATCHES(stdout, "1\\.2@2");
      ASSERT_STR_MATCHES(stdout, "1\\.3@3");
      ASSERT_STR_MATCHES(stdout, "1\\.4@4");
      ASSERT_STR_MATCHES(stdout, kFirstMessage);
      ASSERT_STR_MATCHES(stdout, kAddColumnName1);
      ASSERT_STR_MATCHES(stdout, kAddColumnName1Message);
      ASSERT_STR_MATCHES(stdout, kAddColumnName2Message);
      ASSERT_STR_NOT_MATCHES(stdout, "row_operations \\{");
      ASSERT_STR_NOT_MATCHES(stdout, "Footer:");
    }
  }
}

TEST_F(ToolTest, TestLocalReplicaDumpDataDirs) {
  constexpr const char* const kTestTablet = "ffffffffffffffffffffffffffffffff";
  constexpr const char* const kTestTableId = "test-table";
  constexpr const char* const kTestTableName = "test-fs-data-dirs-dump-table";
  const Schema kSchema(GetSimpleTestSchema());
  const Schema kSchemaWithIds(SchemaBuilder(kSchema).Build());

  FsManagerOpts opts;
  opts.wal_root = GetTestPath("wal");
  opts.data_roots = {
    GetTestPath("data0"),
    GetTestPath("data1"),
    GetTestPath("data2"),
  };
  FsManager fs(env_, opts);
  ASSERT_OK(fs.CreateInitialFileSystemLayout());
  ASSERT_OK(fs.Open());

  pair<PartitionSchema, Partition> partition = tablet::CreateDefaultPartition(
        kSchemaWithIds);
  scoped_refptr<TabletMetadata> meta;
  ASSERT_OK(TabletMetadata::CreateNew(
      &fs, kTestTablet, kTestTableName, kTestTableId,
      kSchemaWithIds, partition.first, partition.second,
      tablet::TABLET_DATA_READY,
      /*tombstone_last_logged_opid=*/ nullopt,
      /*supports_live_row_count=*/ true,
      /*extra_config=*/ nullopt,
      /*dimension_label=*/ nullopt,
      /*table_type=*/ nullopt,
      &meta));
  string stdout;
  NO_FATALS(RunActionStdoutString(Substitute("local_replica dump data_dirs $0 "
                                             "--fs_wal_dir=$1 "
                                             "--fs_data_dirs=$2 $3",
                                             kTestTablet, opts.wal_root,
                                             JoinStrings(opts.data_roots, ","),
                                             env_->IsEncryptionEnabled()
                                                 ? GetEncryptionArgs()
                                                 : ""),
                                  &stdout));
  vector<string> expected;
  for (const auto& data_root : opts.data_roots) {
    expected.emplace_back(JoinPathSegments(data_root, "data"));
  }
  ASSERT_EQ(JoinStrings(expected, "\n"), stdout);
}

TEST_F(ToolTest, TestLocalReplicaDumpMeta) {
  constexpr const char* const kTestTablet = "ffffffffffffffffffffffffffffffff";
  constexpr const char* const kTestTableId = "test-table";
  constexpr const char* const kTestTableName = "test-fs-meta-dump-table";
  const string kTestDir = GetTestPath("test");
  const Schema kSchema(GetSimpleTestSchema());
  const Schema kSchemaWithIds(SchemaBuilder(kSchema).Build());

  FsManager fs(env_, FsManagerOpts(kTestDir));
  ASSERT_OK(fs.CreateInitialFileSystemLayout());
  ASSERT_OK(fs.Open());

  pair<PartitionSchema, Partition> partition = tablet::CreateDefaultPartition(
        kSchemaWithIds);
  scoped_refptr<TabletMetadata> meta;
  TabletMetadata::CreateNew(&fs, kTestTablet, kTestTableName, kTestTableId,
                  kSchemaWithIds, partition.first, partition.second,
                  tablet::TABLET_DATA_READY,
                  /*tombstone_last_logged_opid=*/ nullopt,
                  /*supports_live_row_count=*/ true,
                  /*extra_config=*/ nullopt,
                  /*dimension_label=*/ nullopt,
                  /*table_type=*/ nullopt,
                  &meta);
  string stdout;
  NO_FATALS(RunActionStdoutString(Substitute("local_replica dump meta $0 "
                                             "--fs_wal_dir=$1 "
                                             "--fs_data_dirs=$2 "
                                             "$3",
                                             kTestTablet, kTestDir,
                                             kTestDir,
                                             env_->IsEncryptionEnabled()
                                                ? GetEncryptionArgs() : ""), &stdout));

  // Verify the contents of the metadata output
  SCOPED_TRACE(stdout);
  string debug_str = meta->partition_schema()
      .PartitionDebugString(meta->partition(), *meta->schema());
  StripWhiteSpace(&debug_str);
  ASSERT_STR_CONTAINS(stdout, debug_str);
  debug_str = Substitute("Table name: $0 Table id: $1",
                         meta->table_name(), meta->table_id());
  ASSERT_STR_CONTAINS(stdout, debug_str);
  debug_str = Substitute("Schema (version=$0):", meta->schema_version());
  ASSERT_STR_CONTAINS(stdout, debug_str);
  debug_str = meta->schema()->ToString();
  StripWhiteSpace(&debug_str);
  ASSERT_STR_CONTAINS(stdout, debug_str);

  TabletSuperBlockPB pb1;
  meta->ToSuperBlock(&pb1);
  debug_str = pb_util::SecureDebugString(pb1);
  StripWhiteSpace(&debug_str);
  ASSERT_STR_CONTAINS(stdout, "Superblock:");
  ASSERT_STR_CONTAINS(stdout, debug_str);
}

TEST_F(ToolTest, TestFsDumpTree) {
  const string kTestDir = GetTestPath("test");
  const Schema kSchema(GetSimpleTestSchema());
  const Schema kSchemaWithIds(SchemaBuilder(kSchema).Build());

  FsManager fs(env_, FsManagerOpts(kTestDir));
  ASSERT_OK(fs.CreateInitialFileSystemLayout());
  ASSERT_OK(fs.Open());

  string stdout;
  NO_FATALS(RunActionStdoutString(Substitute("fs dump tree --fs_wal_dir=$0 "
                                             "--fs_data_dirs=$1",
                                             kTestDir, kTestDir), &stdout));

  // It suffices to verify the contents of the top-level tree structure.
  SCOPED_TRACE(stdout);
  ostringstream tree_out;
  fs.DumpFileSystemTree(tree_out);
  string tree_out_str = tree_out.str();
  StripWhiteSpace(&tree_out_str);
  ASSERT_EQ(stdout, tree_out_str);
}

TEST_F(ToolTest, TestLocalReplicaOps) {
  const string kTestDir = GetTestPath("test");

  ObjectIdGenerator generator;
  const string kTestTablet = "ffffffffffffffffffffffffffffffff";
  const int kRowId = 100;
  const Schema kSchema(GetSimpleTestSchema());
  const Schema kSchemaWithIds(SchemaBuilder(kSchema).Build());

  TabletHarness::Options opts(kTestDir);
  opts.tablet_id = kTestTablet;
  TabletHarness harness(kSchemaWithIds, opts);
  ASSERT_OK(harness.Create(true));
  ASSERT_OK(harness.Open());
  LocalTabletWriter writer(harness.tablet().get(), &kSchema);
  KuduPartialRow row(&kSchemaWithIds);
  for (int num_rowsets = 0; num_rowsets < 3; num_rowsets++) {
    for (int i = 0; i < 10; i++) {
      ASSERT_OK(row.SetInt32(0, num_rowsets * 10 + i));
      ASSERT_OK(row.SetInt32(1, num_rowsets * 10 * 10 + i));
      ASSERT_OK(row.SetStringCopy(2, "HelloWorld"));
      writer.Insert(row);
    }
    harness.tablet()->Flush();
  }
  harness.tablet()->Shutdown();
  string fs_paths = "--fs_wal_dir=" + kTestDir + " "
      "--fs_data_dirs=" + kTestDir;
  string encryption_args = env_->IsEncryptionEnabled() ? GetEncryptionArgs() : "";
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(
        Substitute("local_replica dump block_ids $0 $1 $2",
                   kTestTablet, fs_paths, encryption_args), &stdout));

    SCOPED_TRACE(stdout);
    string tablet_out = "Listing all data blocks in tablet " + kTestTablet;
    ASSERT_STR_CONTAINS(stdout, tablet_out);
    ASSERT_STR_CONTAINS(stdout, "Rowset ");
    ASSERT_STR_MATCHES(stdout, "Column block for column ID .*");
    ASSERT_STR_CONTAINS(stdout, "key INT32 NOT NULL");
    ASSERT_STR_CONTAINS(stdout, "int_val INT32 NOT NULL");
    ASSERT_STR_CONTAINS(stdout, "string_val STRING NULLABLE");
  }
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(
        Substitute("local_replica dump rowset $0 $1 $2", kTestTablet, fs_paths, encryption_args),
        &stdout));

    SCOPED_TRACE(stdout);
    ASSERT_STR_CONTAINS(stdout, "Dumping rowset 0");
    ASSERT_STR_MATCHES(stdout, "RowSet metadata: .*");
    ASSERT_STR_MATCHES(stdout, "last_durable_dms_id: .*");
    ASSERT_STR_CONTAINS(stdout, "columns {");
    ASSERT_STR_CONTAINS(stdout, "block {");
    ASSERT_STR_CONTAINS(stdout, "}");
    ASSERT_STR_MATCHES(stdout, "column_id:.*");
    ASSERT_STR_CONTAINS(stdout, "bloom_block {");
    ASSERT_STR_MATCHES(stdout, "id: .*");
    ASSERT_STR_CONTAINS(stdout, "undo_deltas {");

    ASSERT_STR_CONTAINS(stdout,
                        "RowIdxInBlock: 0; Base: (int32 key=0, int32 int_val=0,"
                        " string string_val=\"HelloWorld\"); "
                        "Undo Mutations: [@1(DELETE)]; Redo Mutations: [];");
    ASSERT_STR_MATCHES(stdout, ".*---------------------.*");
  }
  {
    // This is expected to fail with Invalid argument for kRowId.
    string stdout;
    string stderr;
    Status s = RunTool(Substitute("local_replica dump rowset $0 $1 --rowset_index=$2 $3",
                                  kTestTablet,
                                  fs_paths,
                                  kRowId,
                                  encryption_args),
                       &stdout,
                       &stderr,
                       nullptr,
                       nullptr);
    ASSERT_TRUE(s.IsRuntimeError());
    SCOPED_TRACE(stderr);
    string expected =
        "Could not find rowset " + SimpleItoa(kRowId) + " in tablet id " + kTestTablet;
    ASSERT_STR_CONTAINS(stderr, expected);
  }
  {
    // Dump rowsets' primary keys in comparable format.
    string stdout;
    NO_FATALS(RunActionStdoutString(Substitute("local_replica dump rowset --nodump_all_columns "
                                               "--nodump_metadata --nrows=15 $0 $1 $2",
                                               kTestTablet,
                                               fs_paths,
                                               encryption_args),
                                    &stdout));

    SCOPED_TRACE(stdout);
    ASSERT_STR_CONTAINS(stdout, "Dumping rowset 0");
    ASSERT_STR_CONTAINS(stdout, "Dumping rowset 1");
    ASSERT_STR_CONTAINS(stdout, "Dumping rowset 2");
    ASSERT_STR_NOT_CONTAINS(stdout, "RowSet metadata");
    for (int row_idx = 0; row_idx < 30; row_idx++) {
      string row_key = StringPrintf("800000%02x", row_idx);
      if (row_idx < 15) {
        ASSERT_STR_CONTAINS(stdout, row_key);
      } else {
        ASSERT_STR_NOT_CONTAINS(stdout, row_key);
      }
    }
  }

  {
    // Dump rowsets' primary keys in human readable format.
    string stdout;
    NO_FATALS(
        RunActionStdoutString(Substitute("local_replica dump rowset --nodump_all_columns "
                                         "--nodump_metadata --use_readable_format $0 $1 $2",
                                         kTestTablet,
                                         fs_paths,
                                         encryption_args),
                              &stdout));

    SCOPED_TRACE(stdout);
    ASSERT_STR_CONTAINS(stdout, "Dumping rowset 0");
    ASSERT_STR_CONTAINS(stdout, "Dumping rowset 1");
    ASSERT_STR_CONTAINS(stdout, "Dumping rowset 2");
    ASSERT_STR_NOT_CONTAINS(stdout, "RowSet metadata");
    for (int row_idx = 0; row_idx < 30; row_idx++) {
      ASSERT_STR_CONTAINS(stdout, Substitute("(int32 key=$0)", row_idx));
    }
  }
  {
    // Dump rowsets' primary key bounds only.
    string stdout;
    NO_FATALS(RunActionStdoutString(
        Substitute("local_replica dump rowset --nodump_all_columns "
                   "--nodump_metadata --use_readable_format "
                   "--dump_primary_key_bounds_only $0 $1 $2",
                   kTestTablet, fs_paths, encryption_args), &stdout));

    SCOPED_TRACE(stdout);
    ASSERT_STR_CONTAINS(stdout, "Dumping rowset 0");
    ASSERT_STR_CONTAINS(stdout, "Dumping rowset 1");
    ASSERT_STR_CONTAINS(stdout, "Dumping rowset 2");
    ASSERT_STR_NOT_CONTAINS(stdout, "RowSet metadata");
    for (int row_idx = 0; row_idx < 30; row_idx++) {
      string row_key = Substitute("(int32 key=$0)", row_idx);
      if (row_idx % 10 == 0 || row_idx % 10 == 9) {
        ASSERT_STR_CONTAINS(stdout, row_key);
      } else {
        ASSERT_STR_NOT_CONTAINS(stdout, row_key);
      }
    }
  }
  {
    TabletMetadata* meta = harness.tablet()->metadata();
    string stdout;
    string debug_str;
    NO_FATALS(RunActionStdoutString(
        Substitute("local_replica dump meta $0 $1 $2",
                   kTestTablet, fs_paths, encryption_args), &stdout));

    SCOPED_TRACE(stdout);
    debug_str = meta->partition_schema()
        .PartitionDebugString(meta->partition(), *meta->schema());
    StripWhiteSpace(&debug_str);
    ASSERT_STR_CONTAINS(stdout, debug_str);
    debug_str = Substitute("Table name: $0 Table id: $1",
                           meta->table_name(), meta->table_id());
    ASSERT_STR_CONTAINS(stdout, debug_str);
    debug_str = Substitute("Schema (version=$0):", meta->schema_version());
    StripWhiteSpace(&debug_str);
    ASSERT_STR_CONTAINS(stdout, debug_str);
    debug_str = meta->schema()->ToString();
    StripWhiteSpace(&debug_str);
    ASSERT_STR_CONTAINS(stdout, debug_str);

    TabletSuperBlockPB pb1;
    meta->ToSuperBlock(&pb1);
    debug_str = pb_util::SecureDebugString(pb1);
    StripWhiteSpace(&debug_str);
    ASSERT_STR_CONTAINS(stdout, "Superblock:");
    ASSERT_STR_CONTAINS(stdout, debug_str);
  }
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(
        Substitute("local_replica data_size $0 $1 $2",
                   kTestTablet, fs_paths, encryption_args), &stdout));
    SCOPED_TRACE(stdout);

    string expected = R"(
    table id     |  tablet id                       | rowset id |    block type    | size
-----------------+----------------------------------+-----------+------------------+------
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 0         | c10 (key)        | 164B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 0         | c11 (int_val)    | 113B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 0         | c12 (string_val) | 138B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 0         | REDO             | 0B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 0         | UNDO             | 169B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 0         | BLOOM            | 4.1K
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 0         | PK               | 0B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 0         | *                | 4.6K
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 1         | c10 (key)        | 184B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 1         | c11 (int_val)    | 129B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 1         | c12 (string_val) | 158B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 1         | REDO             | 0B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 1         | UNDO             | 181B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 1         | BLOOM            | 4.1K
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 1         | PK               | 0B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 1         | *                | 4.7K
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 2         | c10 (key)        | 184B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 2         | c11 (int_val)    | 129B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 2         | c12 (string_val) | 158B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 2         | REDO             | 0B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 2         | UNDO             | 181B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 2         | BLOOM            | 4.1K
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 2         | PK               | 0B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | 2         | *                | 4.7K
 KuduTableTestId | ffffffffffffffffffffffffffffffff | *         | c10 (key)        | 543B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | *         | c11 (int_val)    | 364B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | *         | c12 (string_val) | 472B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | *         | REDO             | 0B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | *         | UNDO             | 492B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | *         | BLOOM            | 12.2K
 KuduTableTestId | ffffffffffffffffffffffffffffffff | *         | PK               | 0B
 KuduTableTestId | ffffffffffffffffffffffffffffffff | *         | *                | 14.1K
 KuduTableTestId | *                                | *         | c10 (key)        | 543B
 KuduTableTestId | *                                | *         | c11 (int_val)    | 364B
 KuduTableTestId | *                                | *         | c12 (string_val) | 472B
 KuduTableTestId | *                                | *         | REDO             | 0B
 KuduTableTestId | *                                | *         | UNDO             | 492B
 KuduTableTestId | *                                | *         | BLOOM            | 12.2K
 KuduTableTestId | *                                | *         | PK               | 0B
 KuduTableTestId | *                                | *         | *                | 14.1K
)";
    // Preprocess stdout and our expected table so that we are less
    // sensitive to small variations in encodings, id assignment, etc.
    for (string* p : {&stdout, &expected}) {
      // Replace any string of digits with a single '#'.
      StripString(p, "0123456789.", '#');
      StripDupCharacters(p, '#', 0);
      // Collapse whitespace to a single space.
      StripDupCharacters(p, ' ', 0);
      // Strip the leading and trailing whitespace.
      StripWhiteSpace(p);
      // Collapse '-'s to a single '-' so that different width columns
      // don't change the width of the header line.
      StripDupCharacters(p, '-', 0);
    }

    EXPECT_EQ(stdout, expected);
  }
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(Substitute("local_replica list $0 $1",
                                               fs_paths, encryption_args), &stdout));

    SCOPED_TRACE(stdout);
    ASSERT_STR_MATCHES(stdout, kTestTablet);
  }

  {
    string stdout;
    NO_FATALS(RunActionStdoutString(
        Substitute("local_replica list $0 $1 --list_detail=true",
                   fs_paths, encryption_args), &stdout));

    SCOPED_TRACE(stdout);
    ASSERT_STR_MATCHES(stdout, kTestTablet);
  }

  // Test 'kudu fs list' tablet group.
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(
          Substitute("fs list $0 $1 --columns=table,tablet-id --format=csv",
                     fs_paths, encryption_args),
          &stdout));

    SCOPED_TRACE(stdout);
    EXPECT_EQ(stdout, "KuduTableTest,ffffffffffffffffffffffffffffffff");
  }

  // Test 'kudu fs list' rowset group.
  {
    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(
          Substitute("fs list $0 $1 --columns=table,tablet-id,rowset-id --format=csv",
                     fs_paths, encryption_args),
          &stdout));

    SCOPED_TRACE(stdout);
    ASSERT_EQ(3, stdout.size());
    for (int rowset_idx = 0; rowset_idx < 3; rowset_idx++) {
      EXPECT_EQ(stdout[rowset_idx],
                Substitute("KuduTableTest,ffffffffffffffffffffffffffffffff,$0",
                           rowset_idx));
    }
  }
  // Test 'kudu fs list' block group.
  {
    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(
          Substitute("fs list $0 $1 "
                     "--columns=table,tablet-id,rowset-id,block-kind,column "
                     "--format=csv",
                     fs_paths, encryption_args),
          &stdout));

    SCOPED_TRACE(stdout);
    ASSERT_EQ(15, stdout.size());
    for (int rowset_idx = 0; rowset_idx < 3; rowset_idx++) {
      EXPECT_EQ(stdout[rowset_idx * 5 + 0],
                Substitute("KuduTableTest,$0,$1,column,key", kTestTablet, rowset_idx));
      EXPECT_EQ(stdout[rowset_idx * 5 + 1],
                Substitute("KuduTableTest,$0,$1,column,int_val", kTestTablet, rowset_idx));
      EXPECT_EQ(stdout[rowset_idx * 5 + 2],
                Substitute("KuduTableTest,$0,$1,column,string_val", kTestTablet, rowset_idx));
      EXPECT_EQ(stdout[rowset_idx * 5 + 3],
                Substitute("KuduTableTest,$0,$1,undo,", kTestTablet, rowset_idx));
      EXPECT_EQ(stdout[rowset_idx * 5 + 4],
                Substitute("KuduTableTest,$0,$1,bloom,", kTestTablet, rowset_idx));
    }
  }

  // Test 'kudu fs list' cfile group.
  {
    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(
          Substitute("fs list $0 $1 "
                     "--columns=table,tablet-id,rowset-id,block-kind,"
                               "column,cfile-encoding,cfile-num-values "
                     "--format=csv",
                     fs_paths, encryption_args),
          &stdout));

    SCOPED_TRACE(stdout);
    ASSERT_EQ(15, stdout.size());
    for (int rowset_idx = 0; rowset_idx < 3; rowset_idx++) {
      EXPECT_EQ(stdout[rowset_idx * 5 + 0],
                Substitute("KuduTableTest,$0,$1,column,key,BIT_SHUFFLE,10",
                           kTestTablet, rowset_idx));
      EXPECT_EQ(stdout[rowset_idx * 5 + 1],
                Substitute("KuduTableTest,$0,$1,column,int_val,BIT_SHUFFLE,10",
                           kTestTablet, rowset_idx));
      EXPECT_EQ(stdout[rowset_idx * 5 + 2],
                Substitute("KuduTableTest,$0,$1,column,string_val,DICT_ENCODING,10",
                           kTestTablet, rowset_idx));
      EXPECT_EQ(stdout[rowset_idx * 5 + 3],
                Substitute("KuduTableTest,$0,$1,undo,,PLAIN_ENCODING,10",
                           kTestTablet, rowset_idx));
      EXPECT_EQ(stdout[rowset_idx * 5 + 4],
                Substitute("KuduTableTest,$0,$1,bloom,,PLAIN_ENCODING,0",
                           kTestTablet, rowset_idx));
    }
  }
}

// Create and start Kudu mini cluster, optionally creating a table in the DB,
// and then run 'kudu perf loadgen ...' utility against it.
void ToolTest::RunLoadgen(int num_tservers,
                          const vector<string>& tool_args,
                          const string& table_name,
                          string* tool_stdout,
                          string* tool_stderr) {
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = num_tservers;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));
  if (!table_name.empty()) {
    constexpr const char* const kKeyColumnName = "key";
    static const Schema kSchema = Schema(
      {
        ColumnSchema(kKeyColumnName, INT64),
        ColumnSchema("bool_val", BOOL),
        ColumnSchema("int8_val", INT8),
        ColumnSchema("int16_val", INT16),
        ColumnSchema("int32_val", INT32),
        ColumnSchema("int64_val", INT64),
        ColumnSchema("float_val", FLOAT),
        ColumnSchema("double_val", DOUBLE),
        ColumnSchema("decimal32_val", DECIMAL32, false, false,
                     nullptr, nullptr, ColumnStorageAttributes(),
                     ColumnTypeAttributes(9, 9)),
        ColumnSchema("decimal64_val", DECIMAL64, false, false,
                     nullptr, nullptr, ColumnStorageAttributes(),
                     ColumnTypeAttributes(18, 2)),
        ColumnSchema("decimal128_val", DECIMAL128, false, false,
                     nullptr, nullptr, ColumnStorageAttributes(),
                     ColumnTypeAttributes(38, 0)),
        ColumnSchema("unixtime_micros_val", UNIXTIME_MICROS),
        ColumnSchema("string_val", STRING),
        ColumnSchema("binary_val", BINARY),
      }, 1);

    shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    auto client_schema = KuduSchema::FromSchema(kSchema);
    unique_ptr<client::KuduTableCreator> table_creator(
        client->NewTableCreator());
    ASSERT_OK(table_creator->table_name(table_name)
              .schema(&client_schema)
              .add_hash_partitions({kKeyColumnName}, 2)
              .num_replicas(cluster_->num_tablet_servers())
              .Create());
  }
  vector<string> args = {
    "perf",
    "loadgen",
    cluster_->master()->bound_rpc_addr().ToString(),
  };
  if (!table_name.empty()) {
    args.push_back(Substitute("-table_name=$0", table_name));
  }
  copy(tool_args.begin(), tool_args.end(), back_inserter(args));
  ASSERT_OK(RunKuduTool(args, tool_stdout, tool_stderr));
}

// Run the loadgen benchmark with all optional parameters set to defaults.
TEST_F(ToolTest, TestLoadgenDefaultParameters) {
  NO_FATALS(RunLoadgen());
}

// Verify it's possible to run loadgen to create a table, no records inserted.
// Also verify that --num_rows_per_thread=0 in case of existing table
// results in no rows inserted.
TEST_F(ToolTest, TestLoadgenZeroRowsPerThread) {
  // Run the tool with zer rows per thread against an existing table.
  // The existing table should get no rows inserted.
  {
    string out;
    NO_FATALS(RunLoadgen(1, { "--num_rows_per_thread=0", "--run_scan" },
                         "an_empty_test_table", &out));
    ASSERT_STR_MATCHES(out, "expected rows: 0");
    ASSERT_STR_MATCHES(out, "actual rows  : 0");
  }

  // Request to run with zero rows per thread and with various numbers
  // of generator threads. The latter parameter in such a configuration is
  // irrelevant, and the result table should be empty anyways.
  for (auto num_threads : { 1, 2, 10, 100 }) {
    SCOPED_TRACE(Substitute("num_threads=$0", num_threads));
    const vector<string> args = {
      "perf",
      "loadgen",
      cluster_->master()->bound_rpc_addr().ToString(),
      Substitute("--num_threads=$0", num_threads),
      "--num_rows_per_thread=0",
      "--run_scan",
    };
    string out;
    ASSERT_OK(RunKuduTool(args, &out));
    ASSERT_STR_MATCHES(out, "expected rows: 0");
    ASSERT_STR_MATCHES(out, "actual rows  : 0");
  }
}

// Run the loadgen benchmark in AUTO_FLUSH_BACKGROUND mode, sequential values.
TEST_F(ToolTest, TestLoadgenAutoFlushBackgroundSequential) {
  NO_FATALS(RunLoadgen(3,
      {
        "--buffer_flush_watermark_pct=0.125",
        "--buffer_size_bytes=65536",
        "--buffers_num=8",
        "--num_rows_per_thread=2048",
        "--num_threads=4",
        "--run_scan",
        "--string_fixed=0123456789",
      },
      "bench_auto_flush_background_sequential"));
}

// Run loadgen benchmark in AUTO_FLUSH_BACKGROUND mode with randomized keys and values
// using the deprecated --use_random option.
TEST_F(ToolTest, TestLoadgenAutoFlushBackgroundRandomKeysValuesDeprecated) {
  NO_FATALS(RunLoadgen(
      5,
      {
          // Small number of rows to avoid collisions: it's random generation mode
          "--num_rows_per_thread=16",
          "--num_threads=1",
          "--run_scan",
          "--string_len=8",
          "--use_random",
      },
      "bench_auto_flush_background_random_keys_values_deprecated"));
}

// Run loadgen benchmark in AUTO_FLUSH_BACKGROUND mode with randomized keys
// using the --use_random_pk option.
TEST_F(ToolTest, TestLoadgenAutoFlushBackgroundRandomKeys) {
  NO_FATALS(RunLoadgen(
      5,
      {
          // Small number of rows to avoid collisions: it's random generation mode
          "--num_rows_per_thread=16",
          "--num_threads=1",
          "--run_scan",
          "--string_len=8",
          "--use_random_pk",
      },
      "bench_auto_flush_background_random_keys"));
}

// Run loadgen benchmark in AUTO_FLUSH_BACKGROUND mode with randomized values
// using the --use_random_non_pk option.
TEST_F(ToolTest, TestLoadgenAutoFlushBackgroundRandomValues) {
  NO_FATALS(RunLoadgen(
      5,
      {
          // Large number of rows are okay since only non-pk columns will use random values.
          "--num_rows_per_thread=4096",
          "--num_threads=2",
          "--run_scan",
          "--string_len=8",
          "--use_random_non_pk",
      },
      "bench_auto_flush_background_random_values"));
}

// Run loadgen benchmark in AUTO_FLUSH_BACKGROUND mode with randomized values
// using the --use_random_non_pk option combined with the deprecated --use_random option.
TEST_F(ToolTest, TestLoadgenAutoFlushBackgroundRandomValuesIgnoreDeprecated) {
  // Test to ensure if both '--use_random' and '--use_random_non_pk'/'--use_random_pk' are
  // specified then '--use_random' is ignored.
  NO_FATALS(RunLoadgen(
      5,
      {
          // Large number of rows are okay since only non-pk columns will use random values and
          // the deprecated '--use_random' will be ignored when used with '--use_random_non_pk'.
          "--num_rows_per_thread=4096",
          "--num_threads=2",
          "--run_scan",
          "--string_len=8",
          // Combining deprecated --use_random option with new --use_random_non_pk option.
          "--use_random",
          "--use_random_non_pk",
      },
      "bench_auto_flush_background_random_values_ignore_deprecated"));
}

// Run loadgen benchmark in AUTO_FLUSH_BACKGROUND mode, writing the generated
// data using UPSERT instead of INSERT.
TEST_F(ToolTest, TestLoadgenAutoFlushBackgroundUseUpsert) {
  NO_FATALS(RunLoadgen(
      1 /* num_tservers */,
      {
        "--num_rows_per_thread=4096",
        "--num_threads=8",
        "--run_scan",
        "--string_len=8",
        // Use UPSERT (default is to use INSERT) operations for writing rows.
        "--use_upsert",
        // Use random values: even if there are many threads writing many
        // rows, no errors are expected because of using UPSERT instead of
        // INSERT.
        "--use_random_pk",
        "--use_random_non_pk",
      },
      "bench_auto_flush_background_use_upsert"));
}

// Run the loadgen benchmark in MANUAL_FLUSH mode.
TEST_F(ToolTest, TestLoadgenManualFlush) {
  NO_FATALS(RunLoadgen(3,
      {
        "--buffer_size_bytes=524288",
        "--buffers_num=2",
        "--flush_per_n_rows=1024",
        "--num_rows_per_thread=4096",
        "--num_threads=3",
        "--run_scan",
        "--show_first_n_errors=3",
        "--string_len=16",
      },
      "bench_manual_flush"));
}

TEST_F(ToolTest, TestLoadgenServerSideDefaultNumReplicas) {
  NO_FATALS(RunLoadgen(3, { "--table_num_replicas=0" }));
}

TEST_F(ToolTest, TestLoadgenDatabaseName) {
  NO_FATALS(RunLoadgen(1, { "--auto_database=foo", "--keep_auto_table=true" }));
  string out;
  NO_FATALS(RunActionStdoutString(Substitute("table list $0",
      HostPort::ToCommaSeparatedString(cluster_->master_rpc_addrs())), &out));
  ASSERT_STR_CONTAINS(out, "foo.loadgen_auto_");
}

TEST_F(ToolTest, TestLoadgenKeepAutoTableAndData) {
  // Run 'perf loadgen' and keep data.
  // Create a single tablet by setting table_num_hash_partitions and table_num_range_partitions
  // to 1, then we can easily check sequential rows in the tablet by RunScanTableCheck.
  NO_FATALS(RunLoadgen(1, { "--keep_auto_table=true",
                            "--table_num_hash_partitions=1",
                            "--table_num_range_partitions=1" }));

  string auto_table_name;
  NO_FATALS(RunActionStdoutString(Substitute("table list $0",
      HostPort::ToCommaSeparatedString(cluster_->master_rpc_addrs())), &auto_table_name));

  // Data is kept.
  NO_FATALS(RunScanTableCheck(auto_table_name, "", 0, 1999, {}));

  // Run 'perf loadgen' again with sequential mode and delete new generated data.
  {
    const vector<string> args = {
      "perf",
      "loadgen",
      cluster_->master()->bound_rpc_addr().ToString(),
      Substitute("--table_name=$0", auto_table_name),
      "--seq_start=2000",
      "--use_random=false",
      "--run_cleanup=true",
    };
    ASSERT_OK(RunKuduTool(args));
  }

  // Old data is kept and new data is deleted.
  NO_FATALS(RunScanTableCheck(auto_table_name, "", 0, 1999, {}));

  // Run 'perf loadgen' again with random mode and delete new generated data.
  {
    const vector<string> args = {
      "perf",
      "loadgen",
      cluster_->master()->bound_rpc_addr().ToString(),
      Substitute("--table_name=$0", auto_table_name),
      "--seq_start=2000",
      "--use_random=true",
      "--run_cleanup=true",
    };
    ASSERT_OK(RunKuduTool(args));
  }

  // Old data is kept and new data is deleted.
  NO_FATALS(RunScanTableCheck(auto_table_name, "", 0, 1999, {}));
}

TEST_F(ToolTest, TestLoadgenHmsEnabled) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  ExternalMiniClusterOptions opts;
  opts.hms_mode = HmsMode::ENABLE_HIVE_METASTORE;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));
  string out;
  NO_FATALS(RunActionStdoutString(Substitute("perf loadgen $0",
      HostPort::ToCommaSeparatedString(cluster_->master_rpc_addrs())), &out));
}

// Run the loadgen, generating a few different partition schemas.
TEST_F(ToolTest, TestLoadgenAutoGenTablePartitioning) {
  {
    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 1;
    NO_FATALS(StartExternalMiniCluster(std::move(opts)));
  }
  const vector<string> base_args = {
    "perf", "loadgen",
    cluster_->master()->bound_rpc_addr().ToString(),
    // Use a number of threads that isn't a divisor of the number of partitions
    // so the insertion bounds of the threads don't align with the bounds of
    // the partitions. This isn't necessary for the correctness of this test,
    // but is a bit more unusual and, thus, worth testing. See the comments in
    // tools/tool_action_perf.cc for more details about this partitioning.
    "--num_threads=3",

    // Keep the tables so we can verify the presence of tablets as we go.
    "--keep_auto_table=true",

    // Let's make sure nothing breaks even if we insert across the entire
    // keyspace. If we didn't use `use_random`, the bounds of inserted data
    // would be limited by the number of rows inserted.
    "--use_random",

    // Let's also make sure we get the correct results.
    "--run_scan",
  };

  const MonoDelta kTimeout = MonoDelta::FromMilliseconds(10);
  TServerDetails* ts = ts_map_[cluster_->tablet_server(0)->uuid()];

  // Now let's try running with a single partition.
  vector<string> args(base_args);
  args.emplace_back("--table_num_range_partitions=1");
  args.emplace_back("--table_num_hash_partitions=1");
  int expected_tablets = 1;
  ASSERT_OK(RunKuduTool(args));
  ASSERT_OK(WaitForNumTabletsOnTS(ts, expected_tablets, kTimeout));

  // Now let's try running with a couple range partitions.
  args = base_args;
  args.emplace_back("--table_num_range_partitions=2");
  args.emplace_back("--table_num_hash_partitions=1");
  expected_tablets += 2;
  ASSERT_OK(RunKuduTool(args));
  ASSERT_OK(WaitForNumTabletsOnTS(ts, expected_tablets, kTimeout));

  // Now let's try running with only hash partitions.
  args = base_args;
  args.emplace_back("--table_num_range_partitions=1");
  args.emplace_back("--table_num_hash_partitions=2");
  ASSERT_OK(RunKuduTool(args));
  // Note: we're not deleting the tables as we go so that we can do this check.
  // That also means that we have to take into account the previous tables
  // created during this test.
  expected_tablets += 2;
  ASSERT_OK(WaitForNumTabletsOnTS(ts, expected_tablets, kTimeout));

  // And now with both.
  args = base_args;
  args.emplace_back("--table_num_range_partitions=2");
  args.emplace_back("--table_num_hash_partitions=2");
  expected_tablets += 4;
  ASSERT_OK(RunKuduTool(args));
  ASSERT_OK(WaitForNumTabletsOnTS(ts, expected_tablets, kTimeout));
}

// Run the loadgen with txn-related options.
TEST_F(ToolTest, LoadgenTxnBasics) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  {
    ExternalMiniClusterOptions opts;
    // Prefer lighter cluster to speed up testing.
    opts.num_tablet_servers = 1;
    // Since txn-related functionality is not enabled by default for a while,
    // a couple of flags should be overriden to allow running tests scenarios
    // in txn-enabled environment. Also, set the txn keepalive interval to a
    // higher value: in the end of this scenario it's expected to have a couple
    // of open transactions, but everything might run slower than expected with
    // TSAN-enabled binaries run on inferior hardware or VMs.
    opts.extra_master_flags.emplace_back("--txn_manager_enabled");
    opts.extra_master_flags.emplace_back("--txn_manager_status_table_num_replicas=1");
    opts.extra_tserver_flags.emplace_back("--enable_txn_system_client_init");
    opts.extra_tserver_flags.emplace_back("--txn_keepalive_interval_ms=120000");
    NO_FATALS(StartExternalMiniCluster(std::move(opts)));
  }
  const vector<string> base_args = {
    "perf", "loadgen",
    cluster_->master()->bound_rpc_addr().ToString(),
    // Let's have a control over the number of rows inserted.
    "--num_threads=2",
    "--num_rows_per_thread=111",
  };

  // '--txn_start' works as expected when running against txn-enabled cluster.
  {
    vector<string> args(base_args);
    args.emplace_back("--txn_start");
    ASSERT_OK(RunKuduTool(args));

    // An extra run to check for the number of visible rows: there should be
    // none since the transaction isn't committed.
    args.emplace_back("--run_scan");
    string out;
    ASSERT_OK(RunKuduTool(args, &out));
    ASSERT_STR_MATCHES(out, "expected rows: 0");
    ASSERT_STR_MATCHES(out, "actual rows  : 0");
  }

  // '--txn_start' and '--txn_commit' combination works as expected.
  {
    vector<string> args(base_args);
    args.emplace_back("--txn_start");
    args.emplace_back("--txn_commit");
    ASSERT_OK(RunKuduTool(args));

    // An extra run to check for the number of visible rows: all inserted rows
    // should be visible now since the transaction is to be committed.
    args.emplace_back("--run_scan");
    string out;
    ASSERT_OK(RunKuduTool(args, &out));
    ASSERT_STR_MATCHES(out, "expected rows: 222");
    ASSERT_STR_MATCHES(out, "actual rows  : 222");
  }

  // '--txn_start' and '--txn_rollback' combination works as expected.
  {
    vector<string> args(base_args);
    args.emplace_back("--txn_start");
    args.emplace_back("--txn_rollback");
    ASSERT_OK(RunKuduTool(args));

    // An extra run to check for the number of visible rows: there should be
    // none since the transaction is to be rolled back.
    args.emplace_back("--run_scan");
    string out;
    ASSERT_OK(RunKuduTool(args, &out));
    ASSERT_STR_MATCHES(out, "expected rows: 0");
    ASSERT_STR_MATCHES(out, "actual rows  : 0");
  }

  // Supplying '--txn_rollback' implies '--txn_start'.
  {
    vector<string> args(base_args);
    args.emplace_back("--txn_rollback");
    ASSERT_OK(RunKuduTool(args));

    // An extra run to check for the number of visible rows: there should be
    // none since the transaction is to be rolled back.
    args.emplace_back("--run_scan");
    string out;
    ASSERT_OK(RunKuduTool(args, &out));
    ASSERT_STR_MATCHES(out, "expected rows: 0");
    ASSERT_STR_MATCHES(out, "actual rows  : 0");
  }

  // Supplying '--txn_commit' implies '--txn_start'.
  {
    vector<string> args(base_args);
    args.emplace_back("--txn_commit");
    ASSERT_OK(RunKuduTool(args));

    // An extra run to check for the number of visible rows: all inserted rows
    // should be visible now since the transaction is to be committed.
    args.emplace_back("--run_scan");
    string out;
    ASSERT_OK(RunKuduTool(args, &out));
    ASSERT_STR_MATCHES(out, "expected rows: 222");
    ASSERT_STR_MATCHES(out, "actual rows  : 222");
  }

  // Mixing '--txn_start' and '--use_upsert' isn't possible since only
  // INSERT/INSERT_IGNORE supported as transactional operations for now.
  {
    vector<string> args(base_args);
    args.emplace_back("--use_upsert");
    for (const auto& f : { "--txn_start", "--txn_commit", "--txn_rollback" }) {
      SCOPED_TRACE(Substitute("running with flag: {}", f));
      args.emplace_back(f);
      string err;
      const auto s = RunKuduTool(args, nullptr, &err);
      ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
      ASSERT_STR_CONTAINS(
          err, "only inserts are supported as transactional operations");
      ASSERT_STR_CONTAINS(err, "remove/unset the --use_upsert flag");
    }
  }

  // Mixing '--txn_commit' and '--txn_rollback' doesn't make sense.
  {
    vector<string> args(base_args);
    args.emplace_back("--txn_commit");
    args.emplace_back("--txn_rollback");
    string err;
    const auto s = RunKuduTool(args, nullptr, &err);
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(err, "both --txn_commit and --txn_rollback are set");
    ASSERT_STR_CONTAINS(err, "unset one of those to resolve the conflict");
  }

  // Run 'kudu txn list' to see whether the transactions are in expected
  // state after running the sub-scenarios above.
  {
    const vector<string> args = {
      "txn", "list",
      cluster_->master()->bound_rpc_addr().ToString(),
      "--included_states=*",
      "--columns=state",
    };
    string out;
    ASSERT_OK(RunKuduTool(args, &out));
    ASSERT_STR_MATCHES(out,
R"(state
-----------
 OPEN
 OPEN
 COMMITTED
 COMMITTED
 ABORTED
 ABORTED
 ABORTED
 ABORTED
 COMMITTED
 COMMITTED
)");
  }
}

// Test that a non-random workload results in the behavior we would expect when
// running against an auto-generated range partitioned table.
TEST_F(ToolTest, TestNonRandomWorkloadLoadgen) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  {
    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 1;
    // Flush frequently so there are bloom files to check.
    //
    // Note: we use the number of bloom lookups as a loose indicator of whether
    // writes are sequential or not. If row A is being inserted to a range of
    // the keyspace that has already been inserted to, the interval tree that
    // backs the tablet will be unable to say with certainty that row A does or
    // doesn't already exist, necessitating a bloom lookup. As such, if there
    // are bloom lookups for a tablet for a given workload, we can say that
    // that workload is not sequential.
    opts.extra_tserver_flags = {
      "--flush_threshold_mb=1",
      "--flush_threshold_secs=1",
    };
    NO_FATALS(StartExternalMiniCluster(std::move(opts)));
  }
  const vector<string> base_args = {
    "perf", "loadgen",
    cluster_->master()->bound_rpc_addr().ToString(),
    "--keep_auto_table",

    // Use the same number of threads as partitions so when we range partition,
    // each thread will be writing to a single tablet.
    "--num_threads=4",

    // Insert a bunch of large rows for us to begin flushing so there are bloom
    // files to check.
    "--num_rows_per_thread=10000",
    "--string_len=32768",

    // Since we're using such large payloads, flush more frequently so the
    // client doesn't run out of memory.
    "--flush_per_n_rows=1",
  };

  // Partition the table so each thread inserts to a single range.
  vector<string> args = base_args;
  args.emplace_back("--table_num_range_partitions=4");
  args.emplace_back("--table_num_hash_partitions=1");
  ASSERT_OK(RunKuduTool(args));

  // Check that the insert workload didn't require any bloom lookups.
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  int64_t bloom_lookups = 0;
  ASSERT_OK(itest::GetInt64Metric(ts->bound_http_hostport(),
      &METRIC_ENTITY_tablet, nullptr, &METRIC_bloom_lookups, "value", &bloom_lookups));
  ASSERT_EQ(0, bloom_lookups);
}

TEST_F(ToolTest, TestPerfTableScan) {
  constexpr const char* const kTableName = "perf.table_scan";
  NO_FATALS(RunLoadgen(1, { "--run_scan" }, kTableName));
  NO_FATALS(RunScanTableCheck(kTableName, "", 1, 2000, {}, "perf table_scan"));
}

TEST_F(ToolTest, PerfTableScanCountOnly) {
  constexpr const char* const kTableName = "perf.table_scan.row_count_only";
  // Be specific about the number of threads even if it matches the default
  // value for the --num_threads flag. This is to be explicit about the expected
  // number of rows written into the table.
  NO_FATALS(RunLoadgen(1,
                       {
                         "--num_threads=2",
                         "--num_rows_per_thread=1234",
                       },
                       kTableName));

  // Run table_scan with --row_count_only option
  {
    string out;
    string err;
    vector<string> out_lines;
    const auto s = RunTool(
        Substitute("perf table_scan $0 $1 --row_count_only",
                   cluster_->master()->bound_rpc_addr().ToString(), kTableName),
        &out, &err, &out_lines);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_EQ(1, out_lines.size()) << out;
    ASSERT_STR_CONTAINS(out, "Total count 2468 ");
  }

  // Add the --report_scanner_stats flag as well.
  {
    string out;
    string err;
    const auto s = RunTool(Substitute(
        "perf table_scan $0 $1 --row_count_only --report_scanner_stats",
        cluster_->master()->bound_rpc_addr().ToString(), kTableName),
        &out, &err);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_STR_CONTAINS(out, "bytes_read               0");
    ASSERT_STR_CONTAINS(out, "cfile_cache_hit_bytes               0");
    ASSERT_STR_CONTAINS(out, "cfile_cache_miss_bytes               0");
    ASSERT_STR_CONTAINS(out, "total_duration_nanos ");
    ASSERT_STR_CONTAINS(out, "Total count 2468 ");
  }
}

TEST_F(ToolTest, PerfTableScanReplicaSelection) {
  constexpr const char* const kTableName = "perf.table_scan.replica_selection";
  NO_FATALS(RunLoadgen(1,
                       {
                         "--num_threads=8",
                         "--num_rows_per_thread=1",
                       },
                       kTableName));
  {
    string out;
    string err;
    vector<string> out_lines;
    const auto s = RunTool(
        Substitute("perf table_scan $0 $1 --replica_selection=leader",
                   cluster_->master()->bound_rpc_addr().ToString(), kTableName),
        &out, &err, &out_lines);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_EQ(1, out_lines.size()) << out;
    ASSERT_STR_CONTAINS(out, "Total count 8 ");
  }
  {
    string out;
    string err;
    const auto s = RunTool(Substitute(
        "perf table_scan $0 $1 --replica_selection=CLOSEST",
        cluster_->master()->bound_rpc_addr().ToString(), kTableName),
        &out, &err);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_STR_CONTAINS(out, "Total count 8 ");
  }
}

TEST_F(ToolTest, PerfTableScanFaultTolerant) {
  constexpr const char* const kTableName = "perf.table_scan.fault_tolerant";
  NO_FATALS(RunLoadgen(1,
                       {
                           "--num_threads=8",
                           "--num_rows_per_thread=111",
                       },
                       kTableName));
  for (const auto& flag : {"true", "false"}) {
    string out;
    string err;
    vector<string> out_lines;
    const auto s = RunTool(
        Substitute("perf table_scan $0 $1 --fault_tolerant=$2",
                   cluster_->master()->bound_rpc_addr().ToString(), kTableName, flag),
        &out, &err, &out_lines);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_EQ(1, out_lines.size()) << out;
    ASSERT_STR_CONTAINS(out, "Total count 888 ");
  }
}

TEST_F(ToolTest, PerfTableScanBatchSize) {
  constexpr const char* const kTableName = "perf.table_scan.batch_size";
  NO_FATALS(RunLoadgen(1,
                       {
                         "--num_threads=8",
                         "--num_rows_per_thread=111",
                       },
                       kTableName));
  // Check that the special case of batch size of 0 works as well.
  {
    string out;
    string err;
    vector<string> out_lines;
    const auto s = RunTool(
        Substitute("perf table_scan $0 $1 --scan_batch_size=0",
                   cluster_->master()->bound_rpc_addr().ToString(), kTableName),
        &out, &err, &out_lines);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_EQ(1, out_lines.size()) << out;
    ASSERT_STR_CONTAINS(out, "Total count 888 ");
  }
  // Use default server-side scan batch size.
  {
    string out;
    string err;
    vector<string> out_lines;
    const auto s = RunTool(
        Substitute("perf table_scan $0 $1 --scan_batch_size=-1",
                   cluster_->master()->bound_rpc_addr().ToString(), kTableName),
        &out, &err, &out_lines);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_EQ(1, out_lines.size()) << out;
    ASSERT_STR_CONTAINS(out, "Total count 888 ");
  }
  // Use 2 MiByte scan batch size.
  {
    string out;
    string err;
    vector<string> out_lines;
    const auto s = RunTool(
        Substitute("perf table_scan $0 $1 --scan_batch_size=2097152",
                   cluster_->master()->bound_rpc_addr().ToString(), kTableName),
        &out, &err, &out_lines);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_EQ(1, out_lines.size()) << out;
    ASSERT_STR_CONTAINS(out, "Total count 888 ");
  }
}

TEST_F(ToolTest, TestPerfTabletScan) {
  // Create a table.
  constexpr const char* const kTableName = "perf.tablet_scan";
  NO_FATALS(RunLoadgen(1, {}, kTableName));

  // Get the list of tablets.
  vector<string> tablet_ids;
  TServerDetails* ts = ts_map_[cluster_->tablet_server(0)->uuid()];
  ASSERT_OK(ListRunningTabletIds(ts, MonoDelta::FromSeconds(30), &tablet_ids));

  // Scan the tablets using the local tool.
  cluster_->Shutdown();
  for (const string& tid : tablet_ids) {
    const string args =
        Substitute("perf tablet_scan $0 --fs_wal_dir=$1 --fs_data_dirs=$2 --num_iters=2 $3",
                   tid, cluster_->tablet_server(0)->wal_dir(),
                   JoinStrings(cluster_->tablet_server(0)->data_dirs(), ","),
                   env_->IsEncryptionEnabled() ? GetEncryptionArgs() : "");
    NO_FATALS(RunActionStdoutNone(args));
    NO_FATALS(RunActionStdoutNone(args + " --ordered_scan"));
  }
}

// Test 'kudu remote_replica copy' tool when the destination tablet server is online.
// 1. Test the copy tool when the destination replica is healthy
// 2. Test the copy tool when the destination replica is tombstoned
// 3. Test the copy tool when the destination replica is deleted
TEST_F(ToolTest, TestRemoteReplicaCopy) {
  const string kTestDir = GetTestPath("test");
  MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const int kSrcTsIndex = 0;
  const int kDstTsIndex = 1;
  const int kNumTservers = 3;
  const int kNumTablets = 3;
  // This lets us specify wildcard ip addresses for rpc servers
  // on the tablet servers in the cluster giving us the test coverage
  // for KUDU-1776. With this, 'kudu remote_replica copy' can be used to
  // connect to tablet servers bound to wildcard ip addresses.
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = kNumTservers;
  opts.bind_mode = BindMode::WILDCARD;
  opts.extra_master_flags.emplace_back(
      "--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
  opts.extra_tserver_flags.emplace_back("--enable_leader_failure_detection=false");
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  // TestWorkLoad.Setup() internally generates a table.
  TestWorkload workload(cluster_.get());
  workload.set_num_tablets(kNumTablets);
  workload.set_num_replicas(3);
  workload.Setup();

  // Choose source and destination tablet servers for tablet_copy.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  TServerDetails* src_ts = ts_map_[cluster_->tablet_server(kSrcTsIndex)->uuid()];
  ASSERT_OK(WaitForNumTabletsOnTS(src_ts, kNumTablets, kTimeout, &tablets));
  TServerDetails* dst_ts = ts_map_[cluster_->tablet_server(kDstTsIndex)->uuid()];
  ASSERT_OK(WaitForNumTabletsOnTS(dst_ts, kNumTablets, kTimeout, &tablets));
  const string& healthy_tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until the tablets are RUNNING before we start any copies.
  ASSERT_OK(WaitUntilTabletInState(src_ts, healthy_tablet_id, tablet::RUNNING, kTimeout));
  ASSERT_OK(WaitUntilTabletInState(dst_ts, healthy_tablet_id, tablet::RUNNING, kTimeout));

  // Test 1: Test when the destination replica is healthy with and without --force_copy flag.
  // This is an 'online tablet copy'. i.e, when the tool initiates a copy,
  // the internal machinery of tablet-copy deletes the existing healthy
  // replica on destination and copies the replica if --force_copy is specified.
  // Without --force_copy flag, the test fails to copy since there is a healthy
  // replica already present on the destination tserver.
  string stderr;
  const string& src_ts_addr = cluster_->tablet_server(kSrcTsIndex)->bound_rpc_addr().ToString();
  const string& dst_ts_addr = cluster_->tablet_server(kDstTsIndex)->bound_rpc_addr().ToString();
  string encryption_args = env_->IsEncryptionEnabled() ? GetEncryptionArgs() : "";
  Status s = RunTool(
      Substitute("remote_replica copy $0 $1 $2 $3",
                 healthy_tablet_id, src_ts_addr, dst_ts_addr, encryption_args),
                 nullptr, &stderr, nullptr, nullptr);
  ASSERT_TRUE(s.IsRuntimeError());
  SCOPED_TRACE(stderr);
  ASSERT_STR_CONTAINS(stderr, "Rejecting tablet copy request");

  NO_FATALS(RunActionStdoutNone(Substitute("remote_replica copy $0 $1 $2 $3 --force_copy",
                                           healthy_tablet_id, src_ts_addr, dst_ts_addr,
                                           encryption_args)));
  ASSERT_OK(WaitUntilTabletInState(dst_ts, healthy_tablet_id,
                                   tablet::RUNNING, kTimeout));

  // Test 2 and 3: Test when the destination replica is tombstoned or deleted
  DeleteTabletRequestPB req;
  DeleteTabletResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(kTimeout);
  req.set_dest_uuid(dst_ts->uuid());
  const string& tombstoned_tablet_id = tablets[2].tablet_status().tablet_id();
  req.set_tablet_id(tombstoned_tablet_id);
  req.set_delete_type(TabletDataState::TABLET_DATA_TOMBSTONED);
  ASSERT_OK(dst_ts->tserver_admin_proxy->DeleteTablet(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error());

  // Shut down the destination server and delete one tablet from
  // local fs on destination tserver while it is offline.
  cluster_->tablet_server(kDstTsIndex)->Shutdown();
  const string& deleted_tablet_id = tablets[1].tablet_status().tablet_id();
  NO_FATALS(RunActionStdoutNone(Substitute(
      "local_replica delete $0 --fs_wal_dir=$1 --fs_data_dirs=$2 --clean_unsafe $3",
      deleted_tablet_id, cluster_->tablet_server(kDstTsIndex)->wal_dir(),
      JoinStrings(cluster_->tablet_server(kDstTsIndex)->data_dirs(), ","),
      encryption_args)));

  // At this point, we expect only 2 tablets to show up on destination when
  // we restart the destination tserver. deleted_tablet_id should not be found on
  // destination tserver until we do a copy from the tool again.
  ASSERT_OK(cluster_->tablet_server(kDstTsIndex)->Restart());
  vector<ListTabletsResponsePB::StatusAndSchemaPB> dst_tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(dst_ts, kNumTablets-1, kTimeout, &dst_tablets));
  bool found_tombstoned_tablet = false;
  for (const auto& t : dst_tablets) {
    if (t.tablet_status().tablet_id() == tombstoned_tablet_id) {
      found_tombstoned_tablet = true;
    }
    ASSERT_NE(t.tablet_status().tablet_id(), deleted_tablet_id);
  }
  ASSERT_TRUE(found_tombstoned_tablet);
  // Wait until destination tserver has tombstoned_tablet_id in tombstoned state.
  NO_FATALS(inspect_->WaitForTabletDataStateOnTS(kDstTsIndex, tombstoned_tablet_id,
                                                 { TabletDataState::TABLET_DATA_TOMBSTONED },
                                                 kTimeout));
  // Copy tombstoned_tablet_id from source to destination.
  NO_FATALS(RunActionStdoutNone(Substitute("remote_replica copy $0 $1 $2 $3 --force_copy",
                                           tombstoned_tablet_id, src_ts_addr, dst_ts_addr,
                                           encryption_args)));
  ASSERT_OK(WaitUntilTabletInState(dst_ts, tombstoned_tablet_id,
                                   tablet::RUNNING, kTimeout));
  // Copy deleted_tablet_id from source to destination.
  NO_FATALS(RunActionStdoutNone(Substitute("remote_replica copy $0 $1 $2 $3",
                                           deleted_tablet_id, src_ts_addr, dst_ts_addr,
                                           encryption_args)));
  ASSERT_OK(WaitUntilTabletInState(dst_ts, deleted_tablet_id,
                                   tablet::RUNNING, kTimeout));
}

// Test the 'kudu remote_replica list' tool.
TEST_F(ToolTest, TestRemoteReplicaList) {
  MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const int kNumTservers = 1;
  const int kNumTablets = 1;
  ExternalMiniClusterOptions opts;
  opts.num_data_dirs = 3;
  opts.num_tablet_servers = kNumTservers;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  // TestWorkLoad.Setup() internally generates a table.
  TestWorkload workload(cluster_.get());
  workload.set_num_tablets(kNumTablets);
  workload.set_num_replicas(kNumTservers);
  workload.Setup();

  TServerDetails* ts = ts_map_[cluster_->tablet_server(0)->uuid()];
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts, kNumTablets, kTimeout, &tablets));
  const string& ts_addr = cluster_->tablet_server(0)->bound_rpc_addr().ToString();

  const auto& tablet_status = tablets[0].tablet_status();
  {
    // Test the basic case.
    string stdout;
    NO_FATALS(RunActionStdoutString(
        Substitute("remote_replica list $0", ts_addr), &stdout));

    // Some fields like estimated on disk size may vary. Just check a
    // few whose values we should know exactly.
    ASSERT_STR_CONTAINS(stdout, Substitute("Tablet id: $0", tablet_status.tablet_id()));
    ASSERT_STR_MATCHES(stdout, Substitute("State: (INITIALIZED|BOOTSTRAPPING|RUNNING)"));
    // Check all possible roles regardless of the reality.
    ASSERT_STR_MATCHES(stdout,
                       Substitute("Role: (UNKNOWN_ROLE|FOLLOWER|LEADER|LEARNER|NON_PARTICIPANT)"));
    ASSERT_STR_CONTAINS(stdout, Substitute("Table name: $0", workload.table_name()));
    ASSERT_STR_CONTAINS(stdout, "key INT32 NOT NULL");
    ASSERT_STR_CONTAINS(stdout, "Last status: No bootstrap required, opened a new log");
    ASSERT_STR_CONTAINS(stdout, "Data state: TABLET_DATA_READY");
    ASSERT_STR_CONTAINS(stdout,
        Substitute("Data dirs: $0", JoinStrings(tablet_status.data_dirs(), ", ")));
  }

  {
    // Test we lose the schema with --include_schema=false.
    string stdout;
    NO_FATALS(RunActionStdoutString(
          Substitute("remote_replica list $0 --include_schema=false", ts_addr),
          &stdout));
    ASSERT_STR_NOT_CONTAINS(stdout, "key INT32 NOT NULL");
    ASSERT_STR_CONTAINS(stdout, "Last status: No bootstrap required, opened a new log");
    ASSERT_STR_CONTAINS(stdout, "Data state: TABLET_DATA_READY");
  }

  {
    // Test we see the tablet when matching on wrong tablet id or wrong table
    // name.
    string stdout;
    NO_FATALS(RunActionStdoutString(
          Substitute("remote_replica list $0 --table_name=$1",
                     ts_addr, workload.table_name()),
          &stdout));
    ASSERT_STR_CONTAINS(stdout,
                        Substitute("Tablet id: $0",
                                   tablet_status.tablet_id()));
    stdout.clear();
    NO_FATALS(RunActionStdoutString(
          Substitute("remote_replica list $0 --tablets=$1",
                     ts_addr, tablet_status.tablet_id()),
          &stdout));
    ASSERT_STR_CONTAINS(stdout,
                        Substitute("Tablet id: $0",
                                   tablet_status.tablet_id()));
  }

  {
    // Test we lose the tablet when matching on the wrong tablet id or the wrong
    // table name.
    string stdout;
    NO_FATALS(RunActionStdoutString(
          Substitute("remote_replica list $0 --table_name=foo", ts_addr),
          &stdout));
    ASSERT_STR_NOT_CONTAINS(stdout,
                            Substitute("Tablet id: $0",
                                       tablet_status.tablet_id()));
    stdout.clear();
    NO_FATALS(RunActionStdoutString(
          Substitute("remote_replica list $0 --tablets=foo", ts_addr),
          &stdout));
    ASSERT_STR_NOT_CONTAINS(stdout,
                            Substitute("Tablet id: $0",
                                       tablet_status.tablet_id()));
  }

  {
    // Finally, tombstone the replica and try again.
    string stdout;
    ASSERT_OK(DeleteTablet(ts, tablet_status.tablet_id(),
                           TabletDataState::TABLET_DATA_TOMBSTONED, kTimeout));
    NO_FATALS(RunActionStdoutString(
          Substitute("remote_replica list $0", ts_addr), &stdout));
    ASSERT_STR_CONTAINS(stdout,
                        Substitute("Tablet id: $0", tablet_status.tablet_id()));
    ASSERT_STR_CONTAINS(stdout,
                        Substitute("Table name: $0", workload.table_name()));
    ASSERT_STR_CONTAINS(stdout, "Last status: Deleted tablet blocks from disk");
    ASSERT_STR_CONTAINS(stdout, "Data state: TABLET_DATA_TOMBSTONED");
    ASSERT_STR_CONTAINS(stdout, "Data dirs: <not available>");
  }
}

// Test 'kudu local_replica delete' tool with --clean_unsafe flag for
// deleting the tablet from the tablet server.
TEST_F(ToolTest, TestLocalReplicaDelete) {
  NO_FATALS(StartMiniCluster());

  // TestWorkLoad.Setup() internally generates a table.
  TestWorkload workload(mini_cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup();
  workload.Start();

  // There is only one tserver in the minicluster.
  ASSERT_OK(mini_cluster_->WaitForTabletServerCount(1));
  MiniTabletServer* ts = mini_cluster_->mini_tablet_server(0);

  // Generate some workload followed by a flush to grow the size of the tablet on disk.
  while (workload.rows_inserted() < 10000) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  // Make sure the tablet data is flushed to disk. This is needed
  // so that we can compare the size of the data on disk before and
  // after the deletion of local_replica to check if the size-on-disk
  // is reduced at all.
  string tablet_id;
  {
    vector<scoped_refptr<TabletReplica>> tablet_replicas;
    ts->server()->tablet_manager()->GetTabletReplicas(&tablet_replicas);
    ASSERT_EQ(1, tablet_replicas.size());
    Tablet* tablet = tablet_replicas[0]->tablet();
    ASSERT_OK(tablet->Flush());
    tablet_id = tablet_replicas[0]->tablet_id();
  }
  string encryption_args = env_->IsEncryptionEnabled() ? GetEncryptionArgs() : "";
  const string& tserver_dir = ts->options()->fs_opts.wal_root;
  // Using the delete tool with tablet server running fails.
  string stderr;
  Status s = RunTool(
      Substitute("local_replica delete $0 --fs_wal_dir=$1 --fs_data_dirs=$1 $2 "
                 "--clean_unsafe", tablet_id, tserver_dir, encryption_args),
                 nullptr, &stderr, nullptr, nullptr);
  ASSERT_TRUE(s.IsRuntimeError());
  SCOPED_TRACE(stderr);
  ASSERT_STR_CONTAINS(stderr, "Resource temporarily unavailable");

  // Shut down tablet server and use the delete tool with --clean_unsafe.
  ts->Shutdown();

  const string& data_dir = JoinPathSegments(tserver_dir, "data");
  uint64_t size_before_delete;
  ASSERT_OK(env_->GetFileSizeOnDiskRecursively(data_dir, &size_before_delete));
  NO_FATALS(RunActionStdoutNone(Substitute("local_replica delete $0 --fs_wal_dir=$1 $2 "
                                           "--fs_data_dirs=$1 --clean_unsafe",
                                           tablet_id, tserver_dir, encryption_args)));
  // Verify metadata and WAL segments for the tablet_id are gone.
  const string& wal_dir = JoinPathSegments(tserver_dir,
                                           Substitute("wals/$0", tablet_id));
  ASSERT_FALSE(env_->FileExists(wal_dir));
  const string& meta_dir = JoinPathSegments(tserver_dir,
                                            Substitute("tablet-meta/$0", tablet_id));
  ASSERT_FALSE(env_->FileExists(meta_dir));

  // Try to remove the same tablet replica again.
  s = RunTool(Substitute(
      "local_replica delete $0 --clean_unsafe --fs_wal_dir=$1 --fs_data_dirs=$1 $2",
      tablet_id, tserver_dir, encryption_args),
      nullptr, &stderr, nullptr, nullptr);
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_CONTAINS(stderr, "Not found: Could not load tablet metadata");
  ASSERT_STR_CONTAINS(stderr, "No such file or directory");

  // Try to remove the same tablet replica again if --ignore_nonexistent
  // is specified. The tool should report success and output information on the
  // error ignored.
  ASSERT_OK(RunActionStderrString(
      Substitute("local_replica delete $0 --clean_unsafe --fs_wal_dir=$1 "
                 "--fs_data_dirs=$1 --ignore_nonexistent $2",
                 tablet_id, tserver_dir, encryption_args),
      &stderr));
  ASSERT_STR_CONTAINS(stderr, Substitute("ignoring error for tablet replica $0 "
                                         "because of the --ignore_nonexistent flag",
                                         tablet_id));
  ASSERT_STR_CONTAINS(stderr, "Not found: Could not load tablet metadata");
  ASSERT_STR_CONTAINS(stderr, "No such file or directory");

  // Verify that the total size of the data on disk after 'delete' action
  // is less than before. Although this doesn't necessarily check
  // for orphan data blocks left behind for the given tablet, it certainly
  // indicates that some data has been deleted from disk.
  uint64_t size_after_delete;
  ASSERT_OK(env_->GetFileSizeOnDiskRecursively(data_dir, &size_after_delete));
  ASSERT_LT(size_after_delete, size_before_delete);

  // Since there was only one tablet on the node which was deleted by tool,
  // we can expect the tablet server to have nothing after it comes up.
  ASSERT_OK(ts->Start());
  ASSERT_OK(ts->WaitStarted());
  vector<scoped_refptr<TabletReplica>> tablet_replicas;
  ts->server()->tablet_manager()->GetTabletReplicas(&tablet_replicas);
  ASSERT_EQ(0, tablet_replicas.size());
}

// Test 'kudu local_replica delete' tool with multiple tablet replicas to
// operate at once.
TEST_F(ToolTest, TestLocalReplicaDeleteMultiple) {
  static constexpr int kNumTablets = 10;
  NO_FATALS(StartMiniCluster());

  // TestWorkLoad.Setup() creates a table.
  TestWorkload workload(mini_cluster_.get());
  workload.set_num_replicas(1);
  workload.set_num_tablets(kNumTablets);
  workload.Setup();

  vector<string> tablet_ids;
  MiniTabletServer* ts = mini_cluster_->mini_tablet_server(0);
  {
    // It's important to clear the 'tablet_replicas' container before shutting
    // down the minicluster process. Otherwise CHECK() for the ThreadPool's
    // tokens would trigger.
    vector<scoped_refptr<TabletReplica>> tablet_replicas;
    ts->server()->tablet_manager()->GetTabletReplicas(&tablet_replicas);
    ASSERT_EQ(kNumTablets, tablet_replicas.size());
    for (const auto& replica : tablet_replicas) {
      tablet_ids.emplace_back(replica->tablet_id());
    }
  }
  // Tablet server should be shutdown to release the lock and allow operating
  // on its data.
  ts->Shutdown();
  const string& tserver_dir = ts->options()->fs_opts.wal_root;
  const string tablet_ids_csv_str = JoinStrings(tablet_ids, ",");
  NO_FATALS(RunActionStdoutNone(Substitute(
      "local_replica delete --fs_wal_dir=$0 --fs_data_dirs=$0 "
      "--clean_unsafe $1 $2", tserver_dir, tablet_ids_csv_str,
      env_->IsEncryptionEnabled() ? GetEncryptionArgs() : "")));

  ASSERT_OK(ts->Start());
  ASSERT_OK(ts->WaitStarted());
  {
    vector<scoped_refptr<TabletReplica>> tablet_replicas;
    ts->server()->tablet_manager()->GetTabletReplicas(&tablet_replicas);
    ASSERT_EQ(0, tablet_replicas.size());
  }
}

// Test 'kudu local_replica delete' tool for tombstoning the tablet.
TEST_F(ToolTest, TestLocalReplicaTombstoneDelete) {
  NO_FATALS(StartMiniCluster());

  // TestWorkLoad.Setup() internally generates a table.
  TestWorkload workload(mini_cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup();
  workload.Start();

  // There is only one tserver in the minicluster.
  ASSERT_OK(mini_cluster_->WaitForTabletServerCount(1));
  MiniTabletServer* ts = mini_cluster_->mini_tablet_server(0);

  // Generate some workload followed by a flush to grow the size of the tablet on disk.
  while (workload.rows_inserted() < 10000) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  // Make sure the tablet data is flushed to disk. This is needed
  // so that we can compare the size of the data on disk before and
  // after the deletion of local_replica to verify that the size-on-disk
  // is reduced after the tool operation.
  optional<OpId> last_logged_opid;
  string tablet_id;
  {
    vector<scoped_refptr<TabletReplica>> tablet_replicas;
    ts->server()->tablet_manager()->GetTabletReplicas(&tablet_replicas);
    ASSERT_EQ(1, tablet_replicas.size());
    tablet_id = tablet_replicas[0]->tablet_id();
    last_logged_opid = tablet_replicas[0]->shared_consensus()->GetLastOpId(RECEIVED_OPID);
    Tablet* tablet = tablet_replicas[0]->tablet();
    ASSERT_OK(tablet->Flush());
  }
  const string& tserver_dir = ts->options()->fs_opts.wal_root;

  // Shut down tablet server and use the delete tool.
  ts->Shutdown();
  const string& data_dir = JoinPathSegments(tserver_dir, "data");
  uint64_t size_before_delete;
  ASSERT_OK(env_->GetFileSizeOnDiskRecursively(data_dir, &size_before_delete));
  NO_FATALS(RunActionStdoutNone(Substitute("local_replica delete $0 --fs_wal_dir=$1 "
                                           "--fs_data_dirs=$1 $2",
                                           tablet_id, tserver_dir,
                                           env_->IsEncryptionEnabled()
                                             ? GetEncryptionArgs()
                                             : "")));
  // Verify WAL segments for the tablet_id are gone and
  // the data_dir size on tserver is reduced.
  const string& wal_dir = JoinPathSegments(tserver_dir,
                                           Substitute("wals/$0", tablet_id));
  ASSERT_FALSE(env_->FileExists(wal_dir));
  uint64_t size_after_delete;
  ASSERT_OK(env_->GetFileSizeOnDiskRecursively(data_dir, &size_after_delete));
  ASSERT_LT(size_after_delete, size_before_delete);

  // Bring up the tablet server and verify that tablet is tombstoned and
  // tombstone_last_logged_opid matches with the one test had cached earlier.
  ASSERT_OK(ts->Start());
  ASSERT_OK(ts->WaitStarted());
  {
    vector<scoped_refptr<TabletReplica>> tablet_replicas;
    ts->server()->tablet_manager()->GetTabletReplicas(&tablet_replicas);
    ASSERT_EQ(1, tablet_replicas.size());
    ASSERT_EQ(tablet_id, tablet_replicas[0]->tablet_id());
    ASSERT_EQ(TabletDataState::TABLET_DATA_TOMBSTONED,
              tablet_replicas[0]->tablet_metadata()->tablet_data_state());
    optional<OpId> tombstoned_opid =
        tablet_replicas[0]->tablet_metadata()->tombstone_last_logged_opid();
    ASSERT_NE(nullopt, tombstoned_opid);
    ASSERT_NE(nullopt, last_logged_opid);
    ASSERT_EQ(last_logged_opid->term(), tombstoned_opid->term());
    ASSERT_EQ(last_logged_opid->index(), tombstoned_opid->index());
  }
}

// Test for 'local_replica cmeta' functionality.
TEST_F(ToolTest, TestLocalReplicaCMetaOps) {
  const int kNumTablets = 4;
  const int kNumTabletServers = 3;

  InternalMiniClusterOptions opts;
  opts.num_tablet_servers = kNumTabletServers;
  NO_FATALS(StartMiniCluster(std::move(opts)));

  // TestWorkLoad.Setup() internally generates a table.
  TestWorkload workload(mini_cluster_.get());
  workload.set_num_tablets(kNumTablets);
  workload.set_num_replicas(3);
  workload.Setup();

  vector<string> ts_uuids;
  for (int i = 0; i < kNumTabletServers; ++i) {
    ts_uuids.emplace_back(mini_cluster_->mini_tablet_server(i)->uuid());
  }
  string encryption_args;

  if (env_->IsEncryptionEnabled()) {
    encryption_args = GetEncryptionArgs() + " --instance_file=" +
        JoinPathSegments(mini_cluster_->mini_tablet_server(0)->options()->fs_opts.wal_root,
                         "instance");
  }
  const string& flags =
      Substitute("-fs-wal-dir $0 $1",
                 mini_cluster_->mini_tablet_server(0)->options()->fs_opts.wal_root,
                 encryption_args);
  vector<string> tablet_ids;
  {
    NO_FATALS(RunActionStdoutLines(Substitute("local_replica list $0 $1", flags, encryption_args),
                                   &tablet_ids));
    ASSERT_EQ(kNumTablets, tablet_ids.size());
  }
  vector<string> cmeta_paths;
  for (const auto& tablet_id : tablet_ids) {
    cmeta_paths.emplace_back(mini_cluster_->mini_tablet_server(0)->server()->
        fs_manager()->GetConsensusMetadataPath(tablet_id));
  }
  const string& ts_host_port = mini_cluster_->mini_tablet_server(0)->bound_rpc_addr().ToString();
  for (int i = 0; i < kNumTabletServers; ++i) {
    mini_cluster_->mini_tablet_server(i)->Shutdown();
  }

  // Test print_replica_uuids.
  // We have kNumTablets replicas, so we expect kNumTablets lines, with our 3 servers' UUIDs.
  {
    vector<string> lines;
    NO_FATALS(RunActionStdoutLines(Substitute("local_replica cmeta print_replica_uuids $0 $1 $2",
                                              flags, JoinStrings(tablet_ids, ","), encryption_args),
                                   &lines));
    ASSERT_EQ(kNumTablets, lines.size());
    for (int i = 0; i < lines.size(); ++i) {
      ASSERT_STR_MATCHES(lines[i],
                         Substitute("tablet: $0, peers:( $1| $2| $3){3}",
                                    tablet_ids[i], ts_uuids[0], ts_uuids[1], ts_uuids[2]));
      ASSERT_STR_CONTAINS(lines[i], ts_uuids[0]);
      ASSERT_STR_CONTAINS(lines[i], ts_uuids[1]);
      ASSERT_STR_CONTAINS(lines[i], ts_uuids[2]);
    }
  }

  // Test using set-term to bump the term to 123.
  for (int i = 0; i < tablet_ids.size(); ++i) {
    NO_FATALS(RunActionStdoutNone(Substitute("local_replica cmeta set-term $0 $1 $2 123",
                                             flags, tablet_ids[i], encryption_args)));

    string stdout;
    NO_FATALS(RunActionStdoutString(Substitute("pbc dump $0 $1", cmeta_paths[i], encryption_args),
                                    &stdout));
    ASSERT_STR_CONTAINS(stdout, "current_term: 123");
  }

  // Test that set-term refuses to decrease the term.
  for (const auto& tablet_id : tablet_ids) {
    string stdout, stderr;
    Status s = RunTool(Substitute("local_replica cmeta set-term $0 $1 $2 10",
                                  flags, tablet_id, encryption_args),
                       &stdout, &stderr,
                       /* stdout_lines = */ nullptr,
                       /* stderr_lines = */ nullptr);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ("", stdout);
    EXPECT_THAT(stderr, testing::HasSubstr(
        "specified term 10 must be higher than current term 123"));
  }

  // Test using rewrite_raft_config to set all tablets' raft config with only 1 member.
  {
    NO_FATALS(RunActionStdoutNone(
        Substitute("local_replica cmeta rewrite_raft_config $0 $1 $2:$3 $4",
                   flags,
                   JoinStrings(tablet_ids, ","),
                   ts_uuids[0],
                   ts_host_port,
                   encryption_args)));
  }

  // We have kNumTablets replicas, so we expect kNumTablets lines, with our the
  // first tservers' UUIDs.
  {
    vector<string> lines;
    NO_FATALS(RunActionStdoutLines(Substitute("local_replica cmeta print_replica_uuids $0 $1 $2",
                                              flags, JoinStrings(tablet_ids, ","), encryption_args),
                                   &lines));
    ASSERT_EQ(kNumTablets, lines.size());
    for (int i = 0; i < lines.size(); ++i) {
      ASSERT_STR_MATCHES(lines[i], Substitute("tablet: $0, peers: $1",
                                              tablet_ids[i], ts_uuids[0]));
    }
  }
}

TEST_F(ToolTest, TestServerSetFlag) {
  NO_FATALS(StartExternalMiniCluster());
  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  { // Test set unsafe flag.
    NO_FATALS(RunActionStdoutNone(
        Substitute("master set_flag $0 unlock_unsafe_flags true --force",
                   master_addr)));
    NO_FATALS(RunActionStdoutNone(
        Substitute("master set_flag $0 enable_data_block_fsync true --force",
                   master_addr)));
    // Test invalid unsafe flag.
    string err;
    RunActionStderrString(
        Substitute("master set_flag $0 unlock_unsafe_flags false --force",
                   master_addr), &err);
    ASSERT_STR_CONTAINS(err,
        "Detected inconsistency in command-line flags");
    // Flag value will not be changed.
    string out;
    RunActionStdoutString(
        Substitute("master get_flags $0 --flags=unlock_unsafe_flags --format=json",
                   master_addr), &out);
    rapidjson::Document doc;
    doc.Parse<0>(out.c_str());
    const string flag_value = "true";
    for (int i = 0; i < doc.Size(); i++) {
      const rapidjson::Value& item = doc[i];
      ASSERT_TRUE(item["value"].GetString() == flag_value);
    }
  }
  { // Test set experimental flag.
    NO_FATALS(RunActionStdoutNone(
        Substitute("master set_flag $0 unlock_experimental_flags true --force",
                   master_addr)));
    NO_FATALS(RunActionStdoutNone(
        Substitute("master set_flag $0 auto_rebalancing_enabled true --force",
                   master_addr)));
    // Test invalid experimental flag.
    string err;
    RunActionStderrString(
        Substitute("master set_flag $0 unlock_experimental_flags false --force",
                   master_addr), &err);
    ASSERT_STR_CONTAINS(err,
        "Detected inconsistency in command-line flags");
    // Flag value will not be changed.
    string out;
    RunActionStdoutString(
        Substitute("master get_flags $0 --flags=unlock_experimental_flags --format=json",
                   master_addr), &out);
    rapidjson::Document doc;
    doc.Parse<0>(out.c_str());
    const string flag_value = "true";
    for (int i = 0; i < doc.Size(); i++) {
      const rapidjson::Value& item = doc[i];
      ASSERT_TRUE(item["value"].GetString() == flag_value);
    }
  }
  { // Test set flag using group flag validator.
    NO_FATALS(RunActionStdoutNone(
        Substitute("master set_flag $0 unlock_experimental_flags true --force",
                   master_addr)));
    NO_FATALS(RunActionStdoutNone(
        Substitute("master set_flag $0 raft_prepare_replacement_before_eviction true --force",
                  master_addr)));
    NO_FATALS(RunActionStdoutNone(
        Substitute("master set_flag $0 auto_rebalancing_enabled true --force",
                   master_addr)));
    // Test set flag violating group validator.
    string err;
    RunActionStderrString(
        Substitute("master set_flag $0 raft_prepare_replacement_before_eviction false --force",
                   master_addr), &err);
    NO_FATALS(RunActionStdoutNone(
        Substitute("master set_flag $0 auto_rebalancing_enabled true --force",
                   master_addr)));
    ASSERT_STR_CONTAINS(err, "Detected inconsistency in command-line flags");
    // Flag value will not be changed.
    string out;
    RunActionStdoutString(
        Substitute("master get_flags $0 --flags=raft_prepare_replacement_before_eviction"
                   " --format=json",
                   master_addr), &out);
    rapidjson::Document doc;
    doc.Parse<0>(out.c_str());
    const string flag_value = "true";
    for (int i = 0; i < doc.Size(); i++) {
      const rapidjson::Value& item = doc[i];
      ASSERT_TRUE(item["value"].GetString() == flag_value);
    }
  }
  { // Test set flag using parameter: --run_consistency_check.
    NO_FATALS(RunActionStdoutNone(
        Substitute("master set_flag $0 rpc_certificate_file test "
                   "--force --run_consistency_check=false",
                   master_addr)));
  }
}

TEST_F(ToolTest, TestTserverList) {
  NO_FATALS(StartExternalMiniCluster());

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  const auto& tserver = cluster_->tablet_server(0);

  { // TSV
    string out;
    NO_FATALS(RunActionStdoutString(Substitute("tserver list $0 --columns=uuid --format=tsv",
                                              master_addr),
                                    &out));

    ASSERT_EQ(tserver->uuid(), out);
  }

  { // JSON
    string out;
    NO_FATALS(RunActionStdoutString(
          Substitute("tserver list $0 --columns=uuid,rpc-addresses --format=json", master_addr),
          &out));

    ASSERT_EQ(Substitute("[{\"uuid\":\"$0\",\"rpc-addresses\":\"$1\"}]",
                         tserver->uuid(), tserver->bound_rpc_hostport().ToString()),
              out);
  }

  { // Pretty
    string out;
    NO_FATALS(RunActionStdoutString(
          Substitute("tserver list $0 --columns=uuid,rpc-addresses", master_addr),
          &out));

    ASSERT_STR_CONTAINS(out, tserver->uuid());
    ASSERT_STR_CONTAINS(out, tserver->bound_rpc_hostport().ToString());
  }

  { // Add a tserver
    ASSERT_OK(cluster_->AddTabletServer());

    vector<string> lines;
    NO_FATALS(RunActionStdoutLines(
          Substitute("tserver list $0 --columns=uuid --format=space", master_addr),
          &lines));

    vector<string> expected = {
      tserver->uuid(),
      cluster_->tablet_server(1)->uuid(),
    };

    std::sort(lines.begin(), lines.end());
    std::sort(expected.begin(), expected.end());

    ASSERT_EQ(expected, lines);
  }
}

TEST_F(ToolTest, TestTserverListLocationAssigned) {
  const string kLocationCmdPath = JoinPathSegments(GetTestExecutableDirectory(),
                                                   "testdata/first_argument.sh");
  const string location = "/foo-bar0/BAAZ._9-quux";
  ExternalMiniClusterOptions opts;
  opts.extra_master_flags.emplace_back(
        Substitute("--location_mapping_cmd=$0 $1", kLocationCmdPath, location));

  NO_FATALS(StartExternalMiniCluster(opts));

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();

  string out;
  NO_FATALS(RunActionStdoutString(
        Substitute("tserver list $0 --columns=uuid,location --format=csv", master_addr), &out));

  ASSERT_STR_CONTAINS(out, cluster_->tablet_server(0)->uuid() + "," + location);
}

TEST_F(ToolTest, TestTserverListLocationNotAssigned) {
  NO_FATALS(StartExternalMiniCluster());

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();

  string out;
  NO_FATALS(RunActionStdoutString(
        Substitute("tserver list $0 --columns=uuid,location --format=csv", master_addr), &out));

  ASSERT_STR_CONTAINS(out, cluster_->tablet_server(0)->uuid() + ",<none>");
}

TEST_F(ToolTest, TestTServerListState) {
  NO_FATALS(StartExternalMiniCluster());
  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  const string ts_uuid = cluster_->tablet_server(0)->uuid();

  // First, set some tserver state.
  NO_FATALS(RunActionStdoutNone(Substitute("tserver state enter_maintenance $0 $1",
                                           master_addr, ts_uuid)));

  // If the state isn't requested, we shouldn't see any.
  string out;
  NO_FATALS(RunActionStdoutString(
        Substitute("tserver list $0 --columns=uuid --format=csv", master_addr), &out));
  ASSERT_STR_NOT_CONTAINS(out, Substitute("$0,$1", ts_uuid, "MAINTENANCE_MODE"));

  // If it is requested, we should see the state.
  NO_FATALS(RunActionStdoutString(
        Substitute("tserver list $0 --columns=uuid,state --format=csv", master_addr), &out));
  ASSERT_STR_CONTAINS(out, Substitute("$0,$1", ts_uuid, "MAINTENANCE_MODE"));

  // Ksck should show a table showing the state.
  {
    string out;
    NO_FATALS(RunActionStdoutString(
        Substitute("cluster ksck $0", master_addr), &out));
    ASSERT_STR_CONTAINS(out, Substitute(
         "Tablet Server States\n"
         "              Server              |      State\n"
         "----------------------------------+------------------\n"
         " $0 | MAINTENANCE_MODE", ts_uuid));
  }

  // We should still see the state if the tserver hasn't been registered.
  // "Unregister" the server by restarting the cluster and only bringing up the
  // master, and verify that we still see the tserver in maintenance mode.
  cluster_->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());
  master_addr = cluster_->master()->bound_rpc_addr().ToString();
  NO_FATALS(RunActionStdoutString(
        Substitute("tserver list $0 --columns=uuid,state --format=csv", master_addr), &out));
  ASSERT_STR_CONTAINS(out, Substitute("$0,$1", ts_uuid, "MAINTENANCE_MODE"));
  {
    string out;
    NO_FATALS(RunActionStdoutString(
        Substitute("cluster ksck $0", master_addr), &out));
    ASSERT_STR_CONTAINS(out, Substitute(
         "Tablet Server States\n"
         "              Server              |      State\n"
         "----------------------------------+------------------\n"
         " $0 | MAINTENANCE_MODE", ts_uuid));
  }

  NO_FATALS(RunActionStdoutNone(Substitute("tserver state exit_maintenance $0 $1",
                                           master_addr, ts_uuid)));

  // Once removed, we should see none.
  // Since the tserver hasn't registered, there should be no output at all.
  NO_FATALS(RunActionStdoutString(
        Substitute("tserver list $0 --columns=uuid,state --format=csv", master_addr), &out));
  ASSERT_EQ("", out);

  // And once the tserver has registered, we should see no state. Assert
  // eventually to give the tserver some time to register.
  ASSERT_OK(cluster_->tablet_server(0)->Restart());
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(RunActionStdoutString(
          Substitute("tserver list $0 --columns=uuid,state --format=csv", master_addr), &out));
    ASSERT_STR_CONTAINS(out, Substitute("$0,$1", ts_uuid, "NONE"));
  });
  // Ksck should also report that there aren't special tablet server states.
  NO_FATALS(RunActionStdoutString(
      Substitute("cluster ksck $0", master_addr), &out));
  ASSERT_STR_NOT_CONTAINS(out, "Tablet Server States");
}

TEST_F(ToolTest, TestMasterList) {
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = 0;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  auto* master = cluster_->master();

  string out;
  NO_FATALS(RunActionStdoutString(
        Substitute("master list $0 --columns=uuid,rpc-addresses,role,member_type", master_addr),
        &out));

  ASSERT_STR_CONTAINS(out, master->uuid());
  ASSERT_STR_CONTAINS(out, master->bound_rpc_hostport().ToString());
  ASSERT_STR_MATCHES(out, "LEADER|FOLLOWER|LEARNER|NON_PARTICIPANT");
  // Following check will match both VOTER AND NON_VOTER.
  ASSERT_STR_CONTAINS(out, "VOTER");
}

// Operate on Kudu tables:
// (1)delete a table
// (2)rename a table
// (3)rename a column
// (4)list tables
// (5)scan a table
// (6)copy a table
// (7)alter a column
// (8)delete a column
// (9)set the table limit when kudu-master unsupported and supported it
TEST_F(ToolTest, TestDeleteTable) {
  NO_FATALS(StartExternalMiniCluster());
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));
  string master_addr = cluster_->master()->bound_rpc_addr().ToString();

  constexpr const char* const kTableName = "kudu.table";

  // Create a table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_num_replicas(1);
  workload.Setup();

  // Check that the table exists.
  bool exist = false;
  ASSERT_OK(client->TableExists(kTableName, &exist));
  ASSERT_EQ(exist, true);

  // Delete the table.
  NO_FATALS(RunActionStdoutNone(Substitute(
      "table delete $0 $1 --nomodify_external_catalogs",
      master_addr, kTableName)));

  // Check that the table does not exist.
  ASSERT_OK(client->TableExists(kTableName, &exist));
  ASSERT_EQ(exist, false);
}

TEST_F(ToolTest, TestRenameTable) {
  NO_FATALS(StartExternalMiniCluster());
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));
  string master_addr = cluster_->master()->bound_rpc_addr().ToString();

  constexpr const char* const kTableName = "kudu.table";
  constexpr const char* const kNewTableName = "kudu_table";

  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_num_replicas(1);
  workload.Setup();

  string out;
  NO_FATALS(RunActionStdoutNone(Substitute("table rename_table $0 $1 $2",
                                           master_addr, kTableName,
                                           kNewTableName)));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kNewTableName, &table));

  NO_FATALS(RunActionStdoutNone(
        Substitute("table rename_table $0 $1 $2 --nomodify_external_catalogs",
          master_addr, kNewTableName, kTableName)));
  ASSERT_OK(client->OpenTable(kTableName, &table));
}

TEST_F(ToolTest, TestRecallTable) {
  NO_FATALS(StartExternalMiniCluster());
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));
  string master_addr = cluster_->master()->bound_rpc_addr().ToString();

  constexpr const char* const kTableName = "kudu.table";

  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_num_replicas(1);
  workload.Setup();

  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  string table_id = table->id();

  // Soft-delete the table.
  string out;
  NO_FATALS(RunActionStdoutNone(Substitute(
      "table delete $0 $1 --reserve_seconds=300",
      master_addr, kTableName)));

  // List soft_deleted table.
  vector<string> kudu_tables;
  ASSERT_OK(client->ListSoftDeletedTables(&kudu_tables));
  ASSERT_EQ(1, kudu_tables.size());
  kudu_tables.clear();
  ASSERT_OK(client->ListTables(&kudu_tables));
  ASSERT_EQ(0, kudu_tables.size());

  // Can't create a table with the same name whose state is in soft_deleted.
  workload.set_table_name(kTableName);
  workload.set_num_replicas(1);
  NO_FATALS(workload.Setup());
  ASSERT_OK(client->ListTables(&kudu_tables));
  ASSERT_EQ(0, kudu_tables.size());

  const string kNewTableName = "kudu.table.new";
  // Try to recall the soft_deleted table with new name.
  NO_FATALS(RunActionStdoutNone(Substitute("table recall $0 $1 --new_table_name=$2",
                                           master_addr, table_id, kNewTableName)));

  ASSERT_OK(client->ListTables(&kudu_tables));
  ASSERT_EQ(1, kudu_tables.size());
  ASSERT_TRUE(kNewTableName == kudu_tables[0]);
  kudu_tables.clear();
  ASSERT_OK(client->ListSoftDeletedTables(&kudu_tables));
  ASSERT_EQ(0, kudu_tables.size());
}

TEST_F(ToolTest, TestRenameColumn) {
  NO_FATALS(StartExternalMiniCluster());
  constexpr const char* const kTableName = "table";
  constexpr const char* const kColumnName = "col.0";
  constexpr const char* const kNewColumnName = "col_0";

  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("key")
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull()
      ->PrimaryKey();
  schema_builder.AddColumn(kColumnName)
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull();
  KuduSchema schema;
  ASSERT_OK(schema_builder.Build(&schema));

  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_schema(schema);
  workload.set_num_replicas(1);
  workload.Setup();

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  NO_FATALS(RunActionStdoutNone(Substitute("table rename_column $0 $1 $2 $3",
                                           master_addr, kTableName,
                                           kColumnName, kNewColumnName)));
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
                .add_master_server_addr(master_addr)
                .Build(&client));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_STR_CONTAINS(table->schema().ToString(), kNewColumnName);
}

TEST_F(ToolTest, TestListTables) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  NO_FATALS(StartExternalMiniCluster());
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));
  string master_addr = cluster_->master()->bound_rpc_addr().ToString();

  // Replica's format.
  string ts_uuid = cluster_->tablet_server(0)->uuid();
  string ts_addr = cluster_->tablet_server(0)->bound_rpc_addr().ToString();
  string expect_replica = Substitute("    L $0 $1", ts_uuid, ts_addr);

  // Create some tables.
  constexpr int kNumTables = 10;
  constexpr int kReplicaNum = 1;
  vector<string> table_names;
  for (int i = 0; i < kNumTables; ++i) {
    string table_name = Substitute("kudu.table_$0", i);
    table_names.push_back(table_name);

    TestWorkload workload(cluster_.get());
    workload.set_table_name(table_name);
    workload.set_num_replicas(kReplicaNum);
    workload.Setup();
  }
  std::sort(table_names.begin(), table_names.end());

  const auto ProcessTables = [&] (const int num) {
    ASSERT_GE(num, 1);
    ASSERT_LE(num, kNumTables);

    vector<string> expected;
    expected.insert(expected.end(),
                    table_names.begin(), table_names.begin() + num);

    string filter = "";
    if (kNumTables != num) {
      filter = Substitute("--tables=$0", JoinStrings(expected, ","));
    }

    vector<string> lines;
    NO_FATALS(RunActionStdoutLines(
        Substitute("table list $0 $1", master_addr, filter), &lines));

    for (auto& e : expected) {
      ASSERT_STRINGS_ANY_MATCH(lines, e);
    }
  };

  const auto ProcessTablets = [&] (const int num) {
    ASSERT_GE(num, 1);
    ASSERT_LE(num, kNumTables);

    string filter = "";
    if (kNumTables != num) {
      filter = Substitute("--tables=$0",
        JoinStringsIterator(table_names.begin(), table_names.begin() + num, ","));
    }
    vector<string> lines;
    NO_FATALS(RunActionStdoutLines(
        Substitute("table list $0 $1 --list_tablets", master_addr, filter), &lines));

    map<string, pair<string, string>> output;
    for (int i = 0; i < lines.size(); ++i) {
      if (lines[i].empty()) {
        continue;
      }
      if (MatchPattern(lines[i], "kudu.table_")) {
        continue;
      }
      lines[i].erase(0, lines[i].find_first_not_of(' '));
      ASSERT_LE(i + 2, lines.size());
      output[lines[i]] = pair<string, string>(lines[i + 1], lines[i + 2]);
      i += 2;
    }

    for (const auto& e : output) {
      shared_ptr<KuduTable> table;
      ASSERT_OK(client->OpenTable(e.first, &table));
      vector<KuduScanToken*> tokens;
      ElementDeleter deleter(&tokens);
      KuduScanTokenBuilder builder(table.get());
      ASSERT_OK(builder.Build(&tokens));
      ASSERT_EQ(1, tokens.size()); // Only one partition(tablet) under table.
      // Tablet's format.
      string expect_tablet = Substitute("  T $0", tokens[0]->tablet().id());
      ASSERT_EQ(expect_tablet, e.second.first);
      ASSERT_EQ(expect_replica, e.second.second);
    }
  };

  const auto ProcessTablesStatistics = [&] (const int num) {
    ASSERT_GE(num, 1);
    ASSERT_LE(num, kNumTables);

    vector<string> expected;
    expected.reserve(num);
    for (int i = 0; i < num; i++) {
      expected.emplace_back(
          Substitute("$0 num_tablets:1 num_replicas:$1 live_row_count:0",
                     table_names[i], kReplicaNum));
    }
    vector<string> expected_table;
    expected_table.reserve(num);
    expected_table.insert(expected_table.end(),
                          table_names.begin(), table_names.begin() + num);
    string filter = "";
    if (kNumTables != num) {
      filter = Substitute("--tables=$0", JoinStrings(expected_table, ","));
    }
    vector<string> lines;
    NO_FATALS(RunActionStdoutLines(
        Substitute("table list $0 $1 --show_table_info", master_addr, filter), &lines));

    for (auto& e : expected) {
      ASSERT_STRINGS_ANY_MATCH(lines, e);
    }
  };

  // List the tables and tablets.
  for (int i = 1; i <= kNumTables; ++i) {
    NO_FATALS(ProcessTables(i));
    NO_FATALS(ProcessTablets(i));
    NO_FATALS(ProcessTablesStatistics(i));
  }
}

TEST_F(ToolTest, TestScanTablePredicates) {
  NO_FATALS(StartExternalMiniCluster());
  string master_addr = cluster_->master()->bound_rpc_addr().ToString();

  const string kTableName = "kudu.table.scan.predicates";

  // Create the src table and write some data to it.
  TestWorkload ww(cluster_.get());
  ww.set_table_name(kTableName);
  ww.set_num_replicas(1);
  ww.set_write_pattern(TestWorkload::INSERT_SEQUENTIAL_ROWS);
  ww.set_num_write_threads(1);
  ww.Setup();
  ww.Start();
  ASSERT_EVENTUALLY([&]() {
    ASSERT_GE(ww.rows_inserted(), 10);
  });
  ww.StopAndJoin();
  int64_t total_rows = ww.rows_inserted();

  // Insert one more row with a NULL value column.
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));
  shared_ptr<KuduSession> session = client->NewSession();
  session->SetTimeoutMillis(20000);
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  unique_ptr<KuduInsert> insert(table->NewInsert());
  ASSERT_OK(insert->mutable_row()->SetInt32("key", ++total_rows));
  ASSERT_OK(insert->mutable_row()->SetInt32("int_val", 1));
  ASSERT_OK(session->Apply(insert.release()));
  ASSERT_OK(session->Flush());

  // Check predicates.
  RunScanTableCheck(kTableName, "", 1, total_rows);
  RunScanTableCheck(kTableName, R"*(["AND",["=","key",1]])*", 1, 1);
  int64_t mid = total_rows / 2;
  RunScanTableCheck(kTableName,
                    Substitute(R"*(["AND",[">","key",$0]])*", mid),
                    mid + 1, total_rows);
  RunScanTableCheck(kTableName,
                    Substitute(R"*(["AND",[">=","key",$0]])*", mid),
                    mid, total_rows);
  RunScanTableCheck(kTableName,
                    Substitute(R"*(["AND",["<","key",$0]])*", mid),
                    1, mid - 1);
  RunScanTableCheck(kTableName,
                    Substitute(R"*(["AND",["<=","key",$0]])*", mid),
                    1, mid);
  RunScanTableCheck(kTableName,
                    R"*(["AND",["IN","key",[1,2,3,4,5]]])*",
                    1, 5);
  RunScanTableCheck(kTableName,
                    R"*(["AND",["NOTNULL","string_val"]])*",
                    1, total_rows - 1);
  RunScanTableCheck(kTableName,
                    R"*(["AND",["NULL","string_val"]])*",
                    total_rows, total_rows);
  RunScanTableCheck(kTableName,
                    R"*(["AND",["IN","key",[0,1,2,3]],)*"
                    R"*(["<","key",8],[">=","key",1],["NOTNULL","key"],)*"
                    R"*(["NOTNULL","string_val"]])*",
                    1, 3);
}

TEST_F(ToolTest, TestScanTableProjection) {
  NO_FATALS(StartExternalMiniCluster());
  string master_addr = cluster_->master()->bound_rpc_addr().ToString();

  const string kTableName = "kudu.table.scan.projection";

  // Create the src table and write some data to it.
  TestWorkload ww(cluster_.get());
  ww.set_table_name(kTableName);
  ww.set_num_replicas(1);
  ww.set_write_pattern(TestWorkload::INSERT_SEQUENTIAL_ROWS);
  ww.set_num_write_threads(1);
  ww.Setup();
  ww.Start();
  ASSERT_EVENTUALLY([&]() {
    ASSERT_GE(ww.rows_inserted(), 10);
  });
  ww.StopAndJoin();

  // Check projections.
  string one_row_json = R"*(["AND",["=","key",1]])*";
  RunScanTableCheck(kTableName, one_row_json, 1, 1, {});
  RunScanTableCheck(kTableName, one_row_json, 1, 1, {{"int32", "key"}});
  RunScanTableCheck(kTableName, one_row_json, 1, 1, {{"string", "string_val"}});
  RunScanTableCheck(kTableName, one_row_json, 1, 1, {{"int32", "key"},
                                                     {"string", "string_val"}});
  RunScanTableCheck(kTableName, one_row_json, 1, 1, {{"int32", "key"},
                                                     {"int32", "int_val"},
                                                     {"string", "string_val"}});
}

TEST_F(ToolTest, TestScanTableMultiPredicates) {
  constexpr const char* const kTableName = "kudu.table.scan.multipredicates";

  NO_FATALS(StartExternalMiniCluster());
  string master_addr = cluster_->master()->bound_rpc_addr().ToString();

  // Create the src table and write some data to it.
  TestWorkload ww(cluster_.get());
  ww.set_table_name(kTableName);
  ww.set_num_replicas(1);
  ww.set_write_pattern(TestWorkload::INSERT_SEQUENTIAL_ROWS);
  ww.set_num_write_threads(1);
  ww.Setup();
  ww.Start();
  ASSERT_EVENTUALLY([&]() {
    ASSERT_GE(ww.rows_inserted(), 1000);
  });
  ww.StopAndJoin();
  int64_t total_rows = ww.rows_inserted();
  int64_t mid = total_rows / 2;

  vector<string> lines;
  NO_FATALS(RunActionStdoutLines(
              Substitute("table scan $0 $1 "
                         "-columns=key,string_val -predicates=$2",
                         cluster_->master()->bound_rpc_addr().ToString(),
                         kTableName,
                         Substitute(R"*(["AND",[">","key",$0],)*"
                                    R"*(["<=","key",$1],)*"
                                    R"*([">=","string_val","a"],)*"
                                    R"*(["<","string_val","b"]])*", mid, total_rows)),
                         &lines));
  for (auto line : lines) {
    size_t pos1 = line.find("(int64 key=");
    if (pos1 != string::npos) {
      size_t pos2 = line.find(", string string_val=a", pos1);
      ASSERT_NE(pos2, string::npos);
      int32_t key;
      ASSERT_TRUE(safe_strto32(line.substr(pos1, pos2).c_str(), &key));
      ASSERT_GT(key, mid);
      ASSERT_LE(key, total_rows);
    }
  }
  ASSERT_LE(lines.size(), mid);
}

TEST_F(ToolTest, TableScanRowCountOnly) {
  constexpr const char* const kTableName = "kudu.table.scan.row_count_only";
  // Be specific about the number of threads even if it matches the default
  // value for the --num_threads flag. This is to be explicit about the expected
  // number of rows written into the table.
  NO_FATALS(RunLoadgen(1,
                       {
                         "--num_threads=2",
                         "--num_rows_per_thread=1234",
                       },
                       kTableName));

  // Run table_scan with --row_count_only option
  {
    string out;
    string err;
    vector<string> out_lines;
    const auto s = RunTool(
        Substitute("table scan $0 $1 --row_count_only",
                   cluster_->master()->bound_rpc_addr().ToString(), kTableName),
        &out, &err, &out_lines);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_EQ(1, out_lines.size()) << out;
    ASSERT_STR_CONTAINS(out, "Total count 2468 ");
  }

  // Add the --report_scanner_stats flag as well.
  {
    string out;
    string err;
    const auto s = RunTool(
        Substitute("table scan $0 $1 --row_count_only --report_scanner_stats",
                   cluster_->master()->bound_rpc_addr().ToString(), kTableName),
                   &out, &err);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_STR_CONTAINS(out, "bytes_read               0");
    ASSERT_STR_CONTAINS(out, "cfile_cache_hit_bytes               0");
    ASSERT_STR_CONTAINS(out, "cfile_cache_miss_bytes               0");
    ASSERT_STR_CONTAINS(out, "total_duration_nanos ");
    ASSERT_STR_CONTAINS(out, "Total count 2468 ");
  }
}

TEST_F(ToolTest, TableScanReplicaSelection) {
  constexpr const char* const kTableName = "kudu.table.scan.replica_selection";
  NO_FATALS(RunLoadgen(1,
                       {
                         "--num_threads=2",
                         "--num_rows_per_thread=1",
                       },
                       kTableName));
  {
    string out;
    string err;
    vector<string> out_lines;
    const auto s = RunTool(
        Substitute("table scan $0 $1 --replica_selection=LEADER",
                   cluster_->master()->bound_rpc_addr().ToString(), kTableName),
        &out, &err, &out_lines);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_STR_CONTAINS(out, "Total count 2 ");
  }
  {
    string out;
    string err;
    const auto s = RunTool(
        Substitute("table scan $0 $1 --replica_selection=closest",
                   cluster_->master()->bound_rpc_addr().ToString(), kTableName),
                   &out, &err);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_STR_CONTAINS(out, "Total count 2 ");
  }
}

TEST_F(ToolTest, TableScanCustomBatchSize) {
  constexpr const char* const kTableName = "kudu.table.scan.batch_size";
  NO_FATALS(RunLoadgen(1,
                       {
                         "--num_threads=5",
                         "--num_rows_per_thread=2000",
                       },
                       kTableName));
  // Use 256 KiByte scan batch.
  {
    string out;
    string err;
    vector<string> out_lines;
    const auto s = RunTool(
        Substitute("table scan $0 $1 --scan_batch_size=26214",
                   cluster_->master()->bound_rpc_addr().ToString(), kTableName),
        &out, &err, &out_lines);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_STR_CONTAINS(out, "Total count 10000 ");
  }
}

TEST_F(ToolTest, TableScanFaultTolerant) {
  constexpr const char* const kTableName = "kudu.table.scan.fault_tolerant";
  NO_FATALS(RunLoadgen(1,
                       {
                           "--num_threads=8",
                           "--num_rows_per_thread=111",
                       },
                       kTableName));
  for (const auto& flag : {"true", "false"}) {
    string out;
    string err;
    vector<string> out_lines;
    const auto s = RunTool(
        Substitute("table scan $0 $1 --fault_tolerant=$2",
                   cluster_->master()->bound_rpc_addr().ToString(), kTableName, flag),
        &out, &err, &out_lines);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_STR_CONTAINS(out, "Total count 888 ");
  }
}

TEST_F(ToolTest, TableCopyFaultTolerant) {
  constexpr const char* const kTableName = "kudu.table.copy.fault_tolerant.from";
  constexpr const char* const kNewTableName = "kudu.table.copy.fault_tolerant.to";
  NO_FATALS(RunLoadgen(1,
                       {
                           "--num_threads=8",
                           "--num_rows_per_thread=111",
                       },
                       kTableName));
  for (const auto& flag : {"true", "false"}) {
    string out;
    string err;
    vector<string> out_lines;
    const auto s = RunTool(
        Substitute("table copy $0 $1 $2 --dst_table=$3 --write_type=upsert --fault_tolerant=$4",
                   cluster_->master()->bound_rpc_addr().ToString(), kTableName,
                   cluster_->master()->bound_rpc_addr().ToString(), kNewTableName,
                   flag),
        &out, &err, &out_lines);
    ASSERT_TRUE(s.ok()) << s.ToString() << ": " << err;
    ASSERT_STR_CONTAINS(out, "Total count 888 ");
  }
}

TEST_P(ToolTestCopyTableParameterized, TestCopyTable) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  for (const auto& arg : GenerateArgs()) {
    NO_FATALS(RunCopyTableCheck(arg));
  }
}

TEST_F(ToolTest, TestAlterColumn) {
  NO_FATALS(StartExternalMiniCluster());
  constexpr const char* const kTableName = "kudu.table.alter.column";
  constexpr const char* const kColumnName = "col.0";

  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("key")
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull()
      ->PrimaryKey();
  schema_builder.AddColumn(kColumnName)
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull()
      ->Compression(KuduColumnStorageAttributes::CompressionType::LZ4)
      ->Encoding(KuduColumnStorageAttributes::EncodingType::BIT_SHUFFLE)
      ->BlockSize(40960);
  KuduSchema schema;
  ASSERT_OK(schema_builder.Build(&schema));

  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_schema(schema);
  workload.set_num_replicas(1);
  workload.Setup();

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
                .add_master_server_addr(master_addr)
                .Build(&client));
  shared_ptr<KuduTable> table;
  FLAGS_show_attributes = true;

  // Test a few error cases.
  const auto check_bad_input = [&](const string& alter_type,
                                   const string& alter_value,
                                   const string& err) {
    string stderr;
    Status s = RunActionStderrString(
      Substitute("table $0 $1 $2 $3 $4",
                 alter_type, master_addr, kTableName, kColumnName, alter_value), &stderr);
    ASSERT_TRUE(s.IsRuntimeError());
    ASSERT_STR_CONTAINS(stderr, err);
  };

  // Set write_default value for a column.
  NO_FATALS(RunActionStdoutNone(Substitute("table column_set_default $0 $1 $2 $3",
                                           master_addr, kTableName, kColumnName, "[1024]")));
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_STR_CONTAINS(table->schema().ToString(), "1024");

  // Remove write_default value for a column.
  NO_FATALS(RunActionStdoutNone(Substitute("table column_remove_default $0 $1 $2",
                                           master_addr, kTableName, kColumnName)));
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_STR_NOT_CONTAINS(table->schema().ToString(), "1024");

  // Alter compression type for a column.
  NO_FATALS(RunActionStdoutNone(Substitute("table column_set_compression $0 $1 $2 $3",
                                           master_addr, kTableName, kColumnName,
                                           "DEFAULT_COMPRESSION")));
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_STR_CONTAINS(table->schema().ToString(), "DEFAULT_COMPRESSION");
  ASSERT_STR_NOT_CONTAINS(table->schema().ToString(), "LZ4");

  // Test invalid compression type.
  NO_FATALS(check_bad_input("column_set_compression",
                            "UNKNOWN_COMPRESSION_TYPE",
                            "compression type UNKNOWN_COMPRESSION_TYPE is not supported"));

  // Alter encoding type for a column.
  NO_FATALS(RunActionStdoutNone(Substitute("table column_set_encoding $0 $1 $2 $3",
                                           master_addr, kTableName, kColumnName,
                                           "PLAIN_ENCODING")));
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_STR_CONTAINS(table->schema().ToString(), "PLAIN_ENCODING");
  ASSERT_STR_NOT_CONTAINS(table->schema().ToString(), "BIT_SHUFFLE");

  // Test invalid encoding type.
  NO_FATALS(check_bad_input("column_set_encoding",
                            "UNKNOWN_ENCODING_TYPE",
                            "encoding type UNKNOWN_ENCODING_TYPE is not supported"));

  // Alter block_size for a column.
  NO_FATALS(RunActionStdoutNone(Substitute("table column_set_block_size $0 $1 $2 $3",
                                           master_addr, kTableName, kColumnName, "10240")));
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_STR_CONTAINS(table->schema().ToString(), "10240");
  ASSERT_STR_NOT_CONTAINS(table->schema().ToString(), "40960");

  // Test invalid block_size.
  NO_FATALS(check_bad_input("column_set_block_size", "0", "Invalid block size:"));

  // Alter comment for a column.
  const auto alter_comment = [&] (size_t idx) {
    ObjectIdGenerator generator;
    const string comment = generator.Next();
    ASSERT_EQ(table->schema().Column(idx).comment(), "");
    NO_FATALS(RunActionStdoutNone(Substitute("table column_set_comment $0 $1 $2 $3",
                                             master_addr,
                                             kTableName,
                                             table->schema().Column(idx).name(),
                                             comment)));
    ASSERT_OK(client->OpenTable(kTableName, &table));
    ASSERT_EQ(table->schema().Column(idx).comment(), comment);
  };
  for (int i = 0; i < table->schema().num_columns(); ++i) {
    NO_FATALS(alter_comment(i));
  }
}

TEST_F(ToolTest, TestTableComment) {
  NO_FATALS(StartExternalMiniCluster());
  const string& kTableName = "kudu.test_alter_comment";
  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_num_replicas(1);
  workload.Setup();

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  // Check the table comment starts out empty.
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
                .add_master_server_addr(master_addr)
                .Build(&client));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_TRUE(table->comment().empty()) << table->comment();

  ObjectIdGenerator generator;
  const auto table_comment = generator.Next();
  NO_FATALS(RunActionStdoutNone(Substitute("table set_comment $0 $1 $2",
                                           master_addr,
                                           kTableName,
                                           table_comment)));
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_EQ(table_comment, table->comment());

  // Make attempt to "remove" the comment by replacing it with quotes. This
  // doesn't actually clear the comment.
  NO_FATALS(RunActionStdoutNone(Substitute("table set_comment $0 $1 \"\"",
                                           master_addr,
                                           kTableName)));
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_EQ("\"\"", table->comment());

  // Using the 'clear_comment' command, we can remove it.
  NO_FATALS(RunActionStdoutNone(Substitute("table clear_comment $0 $1",
                                           master_addr,
                                           kTableName)));
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_TRUE(table->comment().empty()) << table->comment();
}

TEST_F(ToolTest, TestColumnSetDefault) {
  NO_FATALS(StartExternalMiniCluster());
  constexpr const char* const kTableName = "kudu.table.set.default";
  constexpr const char* const kIntColumn = "col.int";
  constexpr const char* const kStringColumn = "col.string";
  constexpr const char* const kBoolColumn = "col.bool";
  constexpr const char* const kFloatColumn = "col.float";
  constexpr const char* const kDoubleColumn = "col.double";
  constexpr const char* const kBinaryColumn = "col.binary";
  constexpr const char* const kUnixtimeMicrosColumn = "col.unixtime_micros";
  constexpr const char* const kDecimalColumn = "col.decimal";

  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("key")
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull()
      ->PrimaryKey();
  schema_builder.AddColumn(kIntColumn)
      ->Type(client::KuduColumnSchema::INT64);
  schema_builder.AddColumn(kStringColumn)
      ->Type(client::KuduColumnSchema::STRING);
  schema_builder.AddColumn(kBoolColumn)
      ->Type(client::KuduColumnSchema::BOOL);
  schema_builder.AddColumn(kFloatColumn)
      ->Type(client::KuduColumnSchema::FLOAT);
  schema_builder.AddColumn(kDoubleColumn)
      ->Type(client::KuduColumnSchema::DOUBLE);
  schema_builder.AddColumn(kBinaryColumn)
      ->Type(client::KuduColumnSchema::BINARY);
  schema_builder.AddColumn(kUnixtimeMicrosColumn)
      ->Type(client::KuduColumnSchema::UNIXTIME_MICROS);
  schema_builder.AddColumn(kDecimalColumn)
      ->Type(client::KuduColumnSchema::DECIMAL)
      ->Precision(30)
      ->Scale(4);
  KuduSchema schema;
  ASSERT_OK(schema_builder.Build(&schema));

  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_schema(schema);
  workload.set_num_replicas(1);
  workload.Setup();

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
                .add_master_server_addr(master_addr)
                .Build(&client));
  shared_ptr<KuduTable> table;
  FLAGS_show_attributes = true;

  // Test setting write_default value for a column.
  const auto check_set_defult = [&](const string& col_name,
                                    const string& value,
                                    const string& target_value) {
    RunActionStdoutNone(Substitute("table column_set_default $0 $1 $2 $3",
                                   master_addr, kTableName, col_name, value));
    ASSERT_OK(client->OpenTable(kTableName, &table));
    ASSERT_STR_CONTAINS(table->schema().ToString(), target_value);
  };

  // Test a few error cases.
  const auto check_bad_input = [&](const string& col_name,
                                   const string& value,
                                   const string& err) {
    string stderr;
    Status s = RunActionStderrString(
      Substitute("table column_set_default $0 $1 $2 $3",
                 master_addr, kTableName, col_name, value), &stderr);
    ASSERT_TRUE(s.IsRuntimeError());
    ASSERT_STR_CONTAINS(stderr, err);
  };

  // Set write_default value for a int column.
  NO_FATALS(check_set_defult(kIntColumn, "[-2]", "-2"));
  NO_FATALS(check_bad_input(kIntColumn, "[\"string\"]", "unable to parse"));
  NO_FATALS(check_bad_input(kIntColumn, "[123.4]", "unable to parse"));

  // Set write_default value for a string column.
  NO_FATALS(check_set_defult(kStringColumn, "[\"string_value\"]", "string_value"));
  NO_FATALS(check_bad_input(kStringColumn, "[123]", "unable to parse"));
  // Test empty string is a valid default value for a string column.
  NO_FATALS(check_set_defult(kStringColumn, "[\"\"]", "\"\""));
  NO_FATALS(check_set_defult(kStringColumn, "[null]", "\"\""));
  // Test invalid input of an empty string.
  NO_FATALS(check_bad_input(kStringColumn, "\"\"", "expected object array but got string"));
  NO_FATALS(check_bad_input(kStringColumn, "[]", "you should provide one default value"));

  // Set write_default value for a bool column.
  NO_FATALS(check_set_defult(kBoolColumn, "[true]", "true"));
  NO_FATALS(check_set_defult(kBoolColumn, "[false]", "false"));
  NO_FATALS(check_bad_input(kBoolColumn, "[TRUE]", "JSON text is corrupt: Invalid value."));

  // Set write_default value for a float column.
  NO_FATALS(check_set_defult(kFloatColumn, "[1.23]", "1.23"));

  // Set write_default value for a double column.
  NO_FATALS(check_set_defult(kDoubleColumn, "[-1.2345]", "-1.2345"));

  // Set write_default value for a binary column.
  // Empty string tests is the same with string column.
  NO_FATALS(check_set_defult(kBinaryColumn, "[\"binary_value\"]", "binary_value"));

  // Set write_default value for a unixtime_micro column.
  NO_FATALS(check_set_defult(kUnixtimeMicrosColumn, "[12345]", "12345"));

  // Test setting write_default value for a decimal column.
  NO_FATALS(check_bad_input(
    kDecimalColumn,
    "[123]",
    "DECIMAL columns are not supported for setting default value by this tool"));
}

TEST_F(ToolTest, TestAddColumn) {
  NO_FATALS(StartExternalMiniCluster());
  const string& kTableName = "kudu.table.add.column";
  const string& kColumnName0 = "col.0";
  const string& kColumnName1 = "col.1";
  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("key")
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull()
      ->PrimaryKey();
  schema_builder.AddColumn(kColumnName0)
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull();
  KuduSchema schema;
  ASSERT_OK(schema_builder.Build(&schema));

  // Create the table
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_schema(schema);
  workload.set_num_replicas(1);
  workload.Setup();

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
            .add_master_server_addr(master_addr)
            .Build(&client));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_STR_CONTAINS(table->schema().ToString(), kColumnName0);
  ASSERT_STR_NOT_CONTAINS(table->schema().ToString(), kColumnName1);
  NO_FATALS(RunActionStdoutNone(Substitute("table add_column $0 $1 $2 STRING "
                                           "-encoding_type=DICT_ENCODING "
                                           "-compression_type=LZ4 "
                                           "-default_value=[\"abcd\"] "
                                           "-comment=helloworld",
                                           master_addr, kTableName, kColumnName1)));

  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_STR_CONTAINS(table->schema().ToString(), kColumnName1);

  KuduSchemaBuilder expected_schema_builder;
  expected_schema_builder.AddColumn("key")
      ->Type(kudu::client::KuduColumnSchema::INT32)
      ->NotNull()
      ->PrimaryKey();
  expected_schema_builder.AddColumn(kColumnName1)
      ->Type(client::KuduColumnSchema::STRING)
      ->Compression(client::KuduColumnStorageAttributes::CompressionType::LZ4)
      ->Encoding(client::KuduColumnStorageAttributes::EncodingType::DICT_ENCODING)
      ->Default(client::KuduValue::CopyString(Slice("abcd")))
      ->Comment("helloworld");
  KuduSchema expected_schema;
  ASSERT_OK(expected_schema_builder.Build(&expected_schema));

  ASSERT_EQ(table->schema().Column(2).name(), expected_schema.Column(1).name());
  ASSERT_EQ(table->schema().Column(2).is_nullable(), expected_schema.Column(1).is_nullable());
  ASSERT_EQ(table->schema().Column(2).type(), expected_schema.Column(1).type());
  ASSERT_EQ(table->schema().Column(2).storage_attributes().encoding(),
            expected_schema.Column(1).storage_attributes().encoding());
  ASSERT_EQ(table->schema().Column(2).storage_attributes().compression(),
            expected_schema.Column(1).storage_attributes().compression());
  ASSERT_EQ(table->schema().Column(2).comment(), expected_schema.Column(1).comment());
  ASSERT_EQ(table->schema().Column(2), expected_schema.Column(1));
}

TEST_F(ToolTest, TestDeleteColumn) {
  NO_FATALS(StartExternalMiniCluster());
  constexpr const char* const kTableName = "kudu.table.delete.column";
  constexpr const char* const kColumnName = "col.0";

  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("key")
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull()
      ->PrimaryKey();
  schema_builder.AddColumn(kColumnName)
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull();
  KuduSchema schema;
  ASSERT_OK(schema_builder.Build(&schema));

  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_schema(schema);
  workload.set_num_replicas(1);
  workload.Setup();

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
                .add_master_server_addr(master_addr)
                .Build(&client));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_STR_CONTAINS(table->schema().ToString(), kColumnName);
  NO_FATALS(RunActionStdoutNone(Substitute("table delete_column $0 $1 $2",
                                           master_addr, kTableName, kColumnName)));
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_STR_NOT_CONTAINS(table->schema().ToString(), kColumnName);
}

TEST_F(ToolTest, TestChangeTableLimitNotSupported) {
  constexpr const char* const kTableName = "kudu.table.failtochangelimit";
  // Disable table write limit by default, then set limit will not take effect.
  NO_FATALS(StartExternalMiniCluster());
  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_num_replicas(1);
  workload.Setup();

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  // Report unsupported table limit through kudu CLI.
  string stdout;
  NO_FATALS(RunActionStdoutString(
      Substitute("table statistics $0 $1",
                 master_addr, kTableName),
      &stdout));
  ASSERT_STR_CONTAINS(stdout, "on disk size limit: N/A");
  ASSERT_STR_CONTAINS(stdout, "live row count limit: N/A");

  // Report unsupported table limit through kudu client.
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
                .add_master_server_addr(master_addr)
                .Build(&client));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  unique_ptr<KuduTableStatistics> statistics;
  KuduTableStatistics *table_statistics;
  ASSERT_OK(client->GetTableStatistics(kTableName, &table_statistics));
  statistics.reset(table_statistics);
  ASSERT_EQ(-1, statistics->live_row_count_limit());
  ASSERT_EQ(-1, statistics->on_disk_size_limit());

  // Fail to set table limit if kudu-master does not enable it. Verify the error message.
  string stderr;
  Status s = RunActionStderrString(
      Substitute("table set_limit disk_size $0 $1 1000000",
                 master_addr, kTableName),
      &stderr);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(stderr, "altering table limit is not supported");
  s = RunActionStderrString(
      Substitute("table set_limit row_count $0 $1 1000000",
                 master_addr, kTableName),
      &stderr);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(stderr, "altering table limit is not supported");
}

TEST_F(ToolTest, TestChangeTableLimitSupported) {
  constexpr const char* const kTableName = "kudu.table.changelimit";
  const int64_t kDiskSizeLimit = 999999;
  const int64_t kRowCountLimit = 100000;

  ExternalMiniClusterOptions opts;
  opts.extra_master_flags.emplace_back("--enable_table_write_limit=true");
  opts.extra_master_flags.emplace_back("--superuser_acl=*");

  NO_FATALS(StartExternalMiniCluster(opts));

  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_num_replicas(1);
  workload.Setup();

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
                .add_master_server_addr(master_addr)
                .Build(&client));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  unique_ptr<KuduTableStatistics> statistics;
  KuduTableStatistics *table_statistics;
  ASSERT_OK(client->GetTableStatistics(kTableName, &table_statistics));
  statistics.reset(table_statistics);
  ASSERT_EQ(-1, statistics->live_row_count_limit());
  ASSERT_EQ(-1, statistics->on_disk_size_limit());
  // Check the invalid disk_size_limit.
  string stderr;
  Status s = RunActionStderrString(
      Substitute("table set_limit disk_size $0 $1 abc",
                 master_addr, kTableName),
      &stderr);
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_CONTAINS(stderr, "Could not parse");
  stderr.clear();
  // Check the invalid row_count_limit.
  s = RunActionStderrString(
      Substitute("table set_limit row_count $0 $1 abc",
                 master_addr, kTableName),
      &stderr);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(stderr, "Could not parse");
  stderr.clear();
  // Check the invalid setting parameter: negative value
  s = RunActionStderrString(
      Substitute("table set_limit disk_size $0 $1 -1",
                 master_addr, kTableName),
      &stderr);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(stderr, "ERROR: unknown command line flag");
  stderr.clear();
  s = RunActionStderrString(
      Substitute("table set_limit row_count $0 $1 -1",
                 master_addr, kTableName),
      &stderr);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(stderr, "ERROR: unknown command line flag");
  stderr.clear();
  // Chech the invalid setting parameter: larger than INT64_MAX
  s = RunActionStderrString(
      Substitute("table set_limit disk_size $0 $1 99999999999999999999999999",
                 master_addr, kTableName),
      &stderr);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(stderr, "Could not parse");
  stderr.clear();
  s = RunActionStderrString(
      Substitute("table set_limit row_count $0 $1 99999999999999999999999999",
                 master_addr, kTableName),
      &stderr);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(stderr, "Could not parse");
  stderr.clear();
  // Check the normal limit setting.
  NO_FATALS(RunActionStdoutNone(Substitute("table set_limit disk_size $0 $1 $2",
                                           master_addr, kTableName, kDiskSizeLimit)));
  NO_FATALS(RunActionStdoutNone(Substitute("table set_limit row_count $0 $1 $2",
                                           master_addr, kTableName, kRowCountLimit)));
  ASSERT_OK(client->GetTableStatistics(kTableName, &table_statistics));
  statistics.reset(table_statistics);
  ASSERT_EQ(kRowCountLimit, statistics->live_row_count_limit());
  ASSERT_EQ(kDiskSizeLimit, statistics->on_disk_size_limit());
  // Check unlimited setting.
  NO_FATALS(RunActionStdoutNone(Substitute("table set_limit disk_size $0 $1 unlimited",
                                           master_addr, kTableName)));
  NO_FATALS(RunActionStdoutNone(Substitute("table set_limit row_count $0 $1 unlimited",
                                           master_addr, kTableName)));
  ASSERT_OK(client->GetTableStatistics(kTableName, &table_statistics));
  statistics.reset(table_statistics);
  ASSERT_EQ(-1, statistics->live_row_count_limit());
  ASSERT_EQ(-1, statistics->on_disk_size_limit());
}

TEST_F(ToolTest, TestSetReplicationFactor) {
  constexpr int kNumTservers = 4;
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = kNumTservers;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));
  constexpr const char* const kTableName = "kudu.table.set.replication_factor";

  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("key")
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull()
      ->PrimaryKey();
  schema_builder.AddColumn("value")
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull();

  KuduSchema schema;
  ASSERT_OK(schema_builder.Build(&schema));

  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_schema(schema);
  workload.set_num_replicas(1);
  workload.Setup();

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
                .add_master_server_addr(master_addr)
                .Build(&client));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));

  ASSERT_EQ(1, table->num_replicas());

  NO_FATALS(RunActionStdoutNone(Substitute("table set_replication_factor $0 $1 3",
                                           master_addr, kTableName)));
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_EQ(3, table->num_replicas());

  NO_FATALS(RunActionStdoutNone(Substitute("table set_replication_factor $0 $1 1",
                                           master_addr, kTableName)));
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_EQ(1, table->num_replicas());

  string stderr;
  Status s = RunActionStderrString(
      Substitute("table set_replication_factor $0 $1 3a",
                 master_addr, kTableName), &stderr);
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_CONTAINS(stderr, "Unable to parse replication factor value: 3a.");

  s = RunActionStderrString(
      Substitute("table set_replication_factor $0 $1 0",
                 master_addr, kTableName), &stderr);
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_CONTAINS(stderr, "illegal replication factor 0: minimum allowed replication "
                      "factor is 1 (controlled by --min_num_replicas)");

  s = RunActionStderrString(
      Substitute("table set_replication_factor $0 $1 2",
                 master_addr, kTableName), &stderr);
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_CONTAINS(stderr, "illegal replication factor 2: replication factor must be odd");

  s = RunActionStderrString(
      Substitute("table set_replication_factor $0 $1 5",
                 master_addr, kTableName), &stderr);
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_CONTAINS(stderr, "not enough live tablet servers to alter a table with the"
                              " requested replication factor 5; 4 tablet servers are alive");

  s = RunActionStderrString(
      Substitute("table set_replication_factor $0 $1 9",
                 master_addr, kTableName), &stderr);
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_CONTAINS(stderr, "illegal replication factor 9: maximum allowed replication "
                              "factor is 7 (controlled by --max_num_replicas)");
}

Status CreateLegacyHmsTable(HmsClient* client,
                            const string& hms_database_name,
                            const string& hms_table_name,
                            const string& kudu_table_name,
                            const string& kudu_master_addrs,
                            const string& table_type,
                            const optional<string>& owner) {
  hive::Table table;
  table.dbName = hms_database_name;
  table.tableName = hms_table_name;
  table.tableType = table_type;
  if (owner) {
    table.owner = *owner;
  }

  table.__set_parameters({
      make_pair(HmsClient::kStorageHandlerKey, HmsClient::kLegacyKuduStorageHandler),
      make_pair(HmsClient::kKuduTableNameKey, kudu_table_name),
      make_pair(HmsClient::kKuduMasterAddrsKey, kudu_master_addrs),
  });

  // TODO(HIVE-19253): Used along with table type to indicate an external table.
  if (table_type == HmsClient::kExternalTable) {
    table.parameters[HmsClient::kExternalTableKey] = "TRUE";
  }

  return client->CreateTable(table);
}

Status CreateKuduTable(const shared_ptr<KuduClient>& kudu_client,
                       const string& table_name,
                       const optional<string>& owner = {}) {
  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("foo")
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull()
      ->PrimaryKey();
  KuduSchema schema;
  RETURN_NOT_OK(schema_builder.Build(&schema));
  unique_ptr<client::KuduTableCreator> table_creator(kudu_client->NewTableCreator());
  table_creator->table_name(table_name)
              .schema(&schema)
              .num_replicas(1)
              .set_range_partition_columns({ "foo" });
  if (owner) {
    table_creator->set_owner(*owner);
  }
  return table_creator->Create();
}

bool IsValidTableName(const string& table_name) {
  vector<string> identifiers = strings::Split(table_name, ".");
  return identifiers.size() ==2 &&
         !HasPrefixString(table_name, HmsClient::kLegacyTablePrefix);
}

void ValidateHmsEntries(HmsClient* hms_client,
                        const shared_ptr<KuduClient>& kudu_client,
                        const string& database_name,
                        const string& table_name,
                        const string& master_addr) {
  hive::Table hms_table;
  ASSERT_OK(hms_client->GetTable(database_name, table_name, &hms_table));

  ASSERT_EQ(hms_table.parameters[HmsClient::kStorageHandlerKey], HmsClient::kKuduStorageHandler);
  ASSERT_EQ(hms_table.parameters[HmsClient::kKuduMasterAddrsKey], master_addr);

  if (HmsClient::IsSynchronized(hms_table)) {
    shared_ptr<KuduTable> kudu_table;
    ASSERT_OK(kudu_client->OpenTable(Substitute("$0.$1", database_name, table_name), &kudu_table));
    ASSERT_TRUE(iequals(kudu_table->name(),
                               hms_table.parameters[hms::HmsClient::kKuduTableNameKey]));
    ASSERT_EQ(kudu_table->id(), hms_table.parameters[HmsClient::kKuduTableIdKey]);
    ASSERT_EQ(kudu_table->client()->cluster_id(),
              hms_table.parameters[HmsClient::kKuduClusterIdKey]);
  } else {
    ASSERT_TRUE(ContainsKey(hms_table.parameters, HmsClient::kKuduTableNameKey));
    ASSERT_FALSE(ContainsKey(hms_table.parameters, HmsClient::kKuduTableIdKey));
    ASSERT_FALSE(ContainsKey(hms_table.parameters, HmsClient::kKuduClusterIdKey));
  }
}

TEST_P(ToolTestKerberosParameterized, TestHmsDowngrade) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  ExternalMiniClusterOptions opts;
  opts.hms_mode = HmsMode::ENABLE_METASTORE_INTEGRATION;
  opts.enable_kerberos = EnableKerberos();
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  thrift::ClientOptions hms_opts;
  hms_opts.enable_kerberos = EnableKerberos();
  hms_opts.service_principal = "hive";
  HmsClient hms_client(cluster_->hms()->address(), hms_opts);
  ASSERT_OK(hms_client.Start());
  ASSERT_TRUE(hms_client.IsConnected());
  shared_ptr<KuduClient> kudu_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &kudu_client));

  ASSERT_OK(CreateKuduTable(kudu_client, "default.a"));
  NO_FATALS(ValidateHmsEntries(&hms_client, kudu_client, "default", "a", master_addr));

  // Downgrade to legacy table in both Hive Metastore and Kudu.
  // --hive_metastore_uris and --hive_metastore_sasl_enabled are automatically
  // looked up from the master.
  NO_FATALS(RunActionStdoutNone(Substitute("hms downgrade $0", master_addr)));

  // The check tool should report the legacy table.
  string out;
  string err;
  Status s = RunActionStdoutStderrString(Substitute("hms check $0", master_addr), &out, &err);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(out, hms::HmsClient::kLegacyKuduStorageHandler);
  ASSERT_STR_CONTAINS(out, "default.a");

  // The table should still be accessible in both Kudu and the HMS.
  shared_ptr<KuduTable> kudu_table;
  ASSERT_OK(kudu_client->OpenTable("default.a", &kudu_table));
  hive::Table hms_table;
  ASSERT_OK(hms_client.GetTable("default", "a", &hms_table));

  // Check that re-upgrading works as expected.
  NO_FATALS(RunActionStdoutNone(Substitute("hms fix $0", master_addr)));
  NO_FATALS(RunActionStdoutNone(Substitute("hms check $0", master_addr)));
}

namespace {

// Rewrites the specified HMS table, replacing the given parameters with the
// given value.
Status AlterHmsWithReplacedParam(HmsClient* hms_client,
                                 const string& db, const string& table,
                                 const string& param, const string& val) {
  hive::Table updated_hms_table;
  RETURN_NOT_OK(hms_client->GetTable(db, table, &updated_hms_table));
  updated_hms_table.parameters[param] = val;
  return hms_client->AlterTable(db, table, updated_hms_table);
}

} // anonymous namespace

// Test HMS inconsistencies that can be automatically fixed.
// Kerberos is enabled in order to test the tools work in secure clusters.
TEST_P(ToolTestKerberosParameterized, TestCheckAndAutomaticFixHmsMetadata) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  string kUsername = "alice";
  string kOtherUsername = "bob";
  string kOtherComment = "a table comment";
  ExternalMiniClusterOptions opts;
  opts.hms_mode = HmsMode::DISABLE_HIVE_METASTORE;
  opts.enable_kerberos = EnableKerberos();
  opts.extra_master_flags.emplace_back("--allow_empty_owner=true");
  opts.num_masters = 3;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  vector<string> master_addrs;
  for (const auto& hp : cluster_->master_rpc_addrs()) {
    master_addrs.emplace_back(hp.ToString());
  }
  const string master_addrs_str = JoinStrings(master_addrs, ",");

  thrift::ClientOptions hms_opts;
  hms_opts.enable_kerberos = EnableKerberos();
  hms_opts.service_principal = "hive";
  hms_opts.verify_service_config = false;
  HmsClient hms_client(cluster_->hms()->address(), hms_opts);
  ASSERT_OK(hms_client.Start());
  ASSERT_TRUE(hms_client.IsConnected());

  FLAGS_hive_metastore_uris = cluster_->hms()->uris();
  FLAGS_hive_metastore_sasl_enabled = EnableKerberos();
  HmsCatalog hms_catalog(master_addrs_str);
  ASSERT_OK(hms_catalog.Start());

  shared_ptr<KuduClient> kudu_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &kudu_client));

  // Need to pretend to be the master to allow the purge property change.
  hive::EnvironmentContext master_ctx;
  master_ctx.__set_properties({ std::make_pair(hms::HmsClient::kKuduMasterEventKey, "true") });

  // While the metastore integration is disabled create tables in Kudu and the
  // HMS with inconsistent metadata.

  // Control case: the check tool should not flag this managed table.
  shared_ptr<KuduTable> control;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.control", kUsername));
  ASSERT_OK(kudu_client->OpenTable("default.control", &control));
  ASSERT_OK(hms_catalog.CreateTable(
        control->id(), control->name(), kudu_client->cluster_id(), kUsername,
        KuduSchema::ToSchema(control->schema()), control->comment()));

  // Control case: the check tool should not flag this external synchronized table.
  shared_ptr<KuduTable> control_external;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.control_external", kUsername));
  ASSERT_OK(kudu_client->OpenTable("default.control_external", &control_external));
  ASSERT_OK(hms_catalog.CreateTable(
      control_external->id(), control_external->name(), kudu_client->cluster_id(),
      kUsername, KuduSchema::ToSchema(control_external->schema()), control_external->comment(),
      HmsClient::kExternalTable));
  hive::Table hms_control_external;
  ASSERT_OK(hms_client.GetTable("default", "control_external", &hms_control_external));
  hms_control_external.parameters[HmsClient::kKuduTableIdKey] = control_external->id();
  hms_control_external.parameters[HmsClient::kKuduClusterIdKey] = kudu_client->cluster_id();
  hms_control_external.parameters[HmsClient::kExternalPurgeKey] = "true";
  ASSERT_OK(hms_client.AlterTable("default", "control_external",
      hms_control_external, master_ctx));

  // Test case: Upper-case names are handled specially in a few places.
  shared_ptr<KuduTable> test_uppercase;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.UPPERCASE", kUsername));
  ASSERT_OK(kudu_client->OpenTable("default.UPPERCASE", &test_uppercase));
  ASSERT_OK(hms_catalog.CreateTable(
        test_uppercase->id(), test_uppercase->name(), kudu_client->cluster_id(),
        kUsername, KuduSchema::ToSchema(test_uppercase->schema()), test_uppercase->comment()));

  // Test case: inconsistent schema.
  shared_ptr<KuduTable> inconsistent_schema;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.inconsistent_schema", kUsername));
  ASSERT_OK(kudu_client->OpenTable("default.inconsistent_schema", &inconsistent_schema));
  ASSERT_OK(hms_catalog.CreateTable(
        inconsistent_schema->id(), inconsistent_schema->name(), kudu_client->cluster_id(),
        kUsername, SchemaBuilder().Build(), inconsistent_schema->comment()));

  // Test case: inconsistent name.
  shared_ptr<KuduTable> inconsistent_name;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.inconsistent_name", kUsername));
  ASSERT_OK(kudu_client->OpenTable("default.inconsistent_name", &inconsistent_name));
  ASSERT_OK(hms_catalog.CreateTable(
      inconsistent_name->id(), "default.inconsistent_name_hms", kudu_client->cluster_id(),
      kUsername, KuduSchema::ToSchema(inconsistent_name->schema()), inconsistent_name->comment()));

  // Test case: inconsistent cluster id.
  shared_ptr<KuduTable> inconsistent_cluster;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.inconsistent_cluster", kUsername));
  ASSERT_OK(kudu_client->OpenTable("default.inconsistent_cluster", &inconsistent_cluster));
  ASSERT_OK(hms_catalog.CreateTable(
      inconsistent_cluster->id(), "default.inconsistent_cluster", "inconsistent_cluster",
      kUsername, KuduSchema::ToSchema(inconsistent_cluster->schema()),
      inconsistent_cluster->comment()));

  // Test case: inconsistent master addresses.
  shared_ptr<KuduTable> inconsistent_master_addrs;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.inconsistent_master_addrs", kUsername));
  ASSERT_OK(kudu_client->OpenTable("default.inconsistent_master_addrs",
        &inconsistent_master_addrs));
  HmsCatalog invalid_hms_catalog(Substitute("$0,extra_masters",
      cluster_->master_rpc_addrs()[0].ToString()));
  ASSERT_OK(invalid_hms_catalog.Start());
  ASSERT_OK(invalid_hms_catalog.CreateTable(
        inconsistent_master_addrs->id(), inconsistent_master_addrs->name(),
        kudu_client->cluster_id(), kUsername,
        KuduSchema::ToSchema(inconsistent_master_addrs->schema()),
        inconsistent_master_addrs->comment()));

  // Test case: bad table id.
  shared_ptr<KuduTable> bad_id;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.bad_id", kUsername));
  ASSERT_OK(kudu_client->OpenTable("default.bad_id", &bad_id));
  ASSERT_OK(hms_catalog.CreateTable(
      "not_a_table_id", "default.bad_id", kudu_client->cluster_id(),
      kUsername, KuduSchema::ToSchema(bad_id->schema()), bad_id->comment()));

  // Test case: orphan table in the HMS.
  ASSERT_OK(hms_catalog.CreateTable(
        "orphan-hms-table-id", "default.orphan_hms_table", "orphan-hms-cluster-id",
        kUsername, SchemaBuilder().Build(), ""));

  // Test case: orphan table in the HMS with different, but functionally
  // the same master addresses.
  ASSERT_OK(hms_catalog.CreateTable(
      "orphan-hms-masters-id", "default.orphan_hms_table_masters", "orphan-hms-cluster-id",
      kUsername, SchemaBuilder().Build(), ""));
  // Reverse the address order and duplicate the addresses.
  vector<string> modified_addrs;
  for (const auto& hp : cluster_->master_rpc_addrs()) {
    modified_addrs.emplace_back(hp.ToString());
    modified_addrs.emplace_back(hp.ToString());
  }
  std::reverse(modified_addrs.begin(), modified_addrs.end());
  LOG(INFO) << "Modified Masters: " << JoinStrings(modified_addrs, ",");
  ASSERT_OK(AlterHmsWithReplacedParam(&hms_client, "default", "orphan_hms_table_masters",
      HmsClient::kKuduMasterAddrsKey, JoinStrings(modified_addrs, ",")));

  // Test case: orphan external synchronized table in the HMS.
  ASSERT_OK(hms_catalog.CreateTable(
      "orphan-hms-table-id-external", "default.orphan_hms_table_external",
      "orphan-hms-cluster-id-external", kUsername,
      SchemaBuilder().Build(), "", HmsClient::kExternalTable));
  hive::Table hms_orphan_external;
  ASSERT_OK(hms_client.GetTable("default", "orphan_hms_table_external", &hms_orphan_external));
  hms_orphan_external.parameters[HmsClient::kExternalPurgeKey] = "true";
  ASSERT_OK(hms_client.AlterTable("default", "orphan_hms_table_external",
      hms_orphan_external, master_ctx));

  // Test case: orphan legacy table in the HMS.
  ASSERT_OK(CreateLegacyHmsTable(&hms_client, "default", "orphan_hms_table_legacy_managed",
        "impala::default.orphan_hms_table_legacy_managed",
        master_addrs_str, HmsClient::kManagedTable, kUsername));

  // Test case: orphan table in Kudu.
  ASSERT_OK(CreateKuduTable(kudu_client, "default.kudu_orphan", static_cast<string>("")));

  // Test case: legacy managed table.
  shared_ptr<KuduTable> legacy_managed;
  ASSERT_OK(CreateKuduTable(kudu_client, "impala::default.legacy_managed", kUsername));
  ASSERT_OK(kudu_client->OpenTable("impala::default.legacy_managed", &legacy_managed));
  ASSERT_OK(CreateLegacyHmsTable(&hms_client, "default", "legacy_managed",
      "impala::default.legacy_managed", master_addrs_str, HmsClient::kManagedTable, kUsername));

  // Test case: Legacy external purge table.
  shared_ptr<KuduTable> legacy_purge;
  ASSERT_OK(CreateKuduTable(kudu_client, "impala::default.legacy_purge", kUsername));
  ASSERT_OK(kudu_client->OpenTable("impala::default.legacy_purge", &legacy_purge));
  ASSERT_OK(CreateLegacyHmsTable(&hms_client, "default", "legacy_purge",
      "impala::default.legacy_purge", master_addrs_str, HmsClient::kExternalTable, kUsername));
  hive::Table hms_legacy_purge;
  ASSERT_OK(hms_client.GetTable("default", "legacy_purge", &hms_legacy_purge));
  hms_legacy_purge.parameters[HmsClient::kExternalPurgeKey] = "true";
  ASSERT_OK(hms_client.AlterTable("default", "legacy_purge",
                                  hms_legacy_purge, master_ctx));

  // Test case: legacy external table (pointed at the legacy managed table).
  ASSERT_OK(CreateLegacyHmsTable(&hms_client, "default", "legacy_external",
      "impala::default.legacy_managed", master_addrs_str, HmsClient::kExternalTable, kUsername));

  // Test case: legacy managed table with no owner.
  shared_ptr<KuduTable> legacy_no_owner;
  ASSERT_OK(CreateKuduTable(kudu_client, "impala::default.legacy_no_owner",
                            static_cast<string>("")));
  ASSERT_OK(kudu_client->OpenTable("impala::default.legacy_no_owner", &legacy_no_owner));
  ASSERT_OK(CreateLegacyHmsTable(&hms_client, "default", "legacy_no_owner",
        "impala::default.legacy_no_owner", master_addrs_str, HmsClient::kManagedTable,
        nullopt));

  // Test case: legacy managed table with a Hive-incompatible name (no database).
  shared_ptr<KuduTable> legacy_hive_incompatible_name;
  ASSERT_OK(CreateKuduTable(kudu_client, "legacy_hive_incompatible_name", kUsername));
  ASSERT_OK(kudu_client->OpenTable("legacy_hive_incompatible_name",
        &legacy_hive_incompatible_name));
  ASSERT_OK(CreateLegacyHmsTable(&hms_client, "default", "legacy_hive_incompatible_name",
        "legacy_hive_incompatible_name", master_addrs_str,
        HmsClient::kManagedTable, kUsername));

  // Test case: Kudu table in non-default database.
  hive::Database db;
  db.name = "my_db";
  ASSERT_OK(hms_client.CreateDatabase(db));
  ASSERT_OK(CreateKuduTable(kudu_client, "my_db.table", kUsername));

  // Test case: no owner in HMS
  shared_ptr<KuduTable> no_owner_in_hms;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.no_owner_in_hms", kUsername));
  ASSERT_OK(kudu_client->OpenTable("default.no_owner_in_hms", &no_owner_in_hms));
  ASSERT_OK(hms_catalog.CreateTable(
      no_owner_in_hms->id(), no_owner_in_hms->name(), kudu_client->cluster_id(),
      nullopt, KuduSchema::ToSchema(no_owner_in_hms->schema()), no_owner_in_hms->comment()));

  // Test case: no owner in Kudu
  shared_ptr<KuduTable> no_owner_in_kudu;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.no_owner_in_kudu", static_cast<string>("")));
  ASSERT_OK(kudu_client->OpenTable("default.no_owner_in_kudu", &no_owner_in_kudu));
  ASSERT_OK(hms_catalog.CreateTable(
      no_owner_in_kudu->id(), no_owner_in_kudu->name(), kudu_client->cluster_id(),
      kUsername, KuduSchema::ToSchema(no_owner_in_kudu->schema()), no_owner_in_kudu->comment()));

  // Test case: different owner in Kudu and HMS
  shared_ptr<KuduTable> different_owner;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.different_owner", kUsername));
  ASSERT_OK(kudu_client->OpenTable("default.different_owner", &different_owner));
  ASSERT_OK(hms_catalog.CreateTable(
      different_owner->id(), different_owner->name(), kudu_client->cluster_id(),
      kOtherUsername, KuduSchema::ToSchema(different_owner->schema()),
      different_owner->comment()));

  // Test case: different comment in Kudu and HMS
  shared_ptr<KuduTable> different_comment;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.different_comment", kUsername));
  ASSERT_OK(kudu_client->OpenTable("default.different_comment", &different_comment));
  ASSERT_OK(hms_catalog.CreateTable(
      different_comment->id(), different_comment->name(), kudu_client->cluster_id(),
      different_comment->owner(), KuduSchema::ToSchema(different_comment->schema()),
      kOtherComment));

  unordered_set<string> consistent_tables = {
    "default.control",
    "default.control_external",
  };

  unordered_set<string> inconsistent_tables = {
    "default.UPPERCASE",
    "default.inconsistent_schema",
    "default.inconsistent_name",
    "default.inconsistent_cluster",
    "default.inconsistent_master_addrs",
    "default.bad_id",
    "default.different_comment",
    "default.different_owner",
    "default.no_owner_in_hms",
    "default.no_owner_in_kudu",
    "default.orphan_hms_table",
    "default.orphan_hms_table_masters",
    "default.orphan_hms_table_external",
    "default.orphan_hms_table_legacy_managed",
    "default.kudu_orphan",
    "default.legacy_managed",
    "default.legacy_purge",
    "default.legacy_no_owner",
    "legacy_external",
    "legacy_hive_incompatible_name",
    "my_db.table",
  };

  // Move a list of tables from the inconsistent set to the consistent set.
  auto make_consistent = [&] (const vector<string>& tables) {
    for (const string& table : tables) {
      ASSERT_EQ(inconsistent_tables.erase(table), 1);
    }
    consistent_tables.insert(tables.begin(), tables.end());
  };

  const string hms_flags = Substitute("-hive_metastore_uris=$0 -hive_metastore_sasl_enabled=$1",
                                      FLAGS_hive_metastore_uris, FLAGS_hive_metastore_sasl_enabled);

  // Run the HMS check tool and verify that the consistent tables are not
  // reported, and the inconsistent tables are reported.
  auto check = [&] () {
    string out;
    string err;
    Status s = RunActionStdoutStderrString(
        Substitute("hms check $0 $1", master_addrs_str, hms_flags), &out, &err);
    SCOPED_TRACE(strings::CUnescapeOrDie(out));
    if (inconsistent_tables.empty()) {
      ASSERT_OK(s);
      ASSERT_STR_NOT_CONTAINS(err, "found inconsistencies in the Kudu and HMS catalogs");
    } else {
      ASSERT_FALSE(s.ok());
      ASSERT_STR_CONTAINS(err, "found inconsistencies in the Kudu and HMS catalogs");
    }
    for (const string& table : consistent_tables) {
      ASSERT_STR_NOT_CONTAINS(out, table);
    }
    for (const string& table : inconsistent_tables) {
      ASSERT_STR_CONTAINS(out, table);
    }
  };

  // 'hms check' should point out all of the test-case tables, but not the control tables.
  NO_FATALS(check());

  // 'hms fix --dryrun should not change the output of 'hms check'.
  NO_FATALS(RunActionStdoutNone(
        Substitute("hms fix $0 --dryrun --drop_orphan_hms_tables $1",
            master_addrs_str, hms_flags)));
  NO_FATALS(check());

  // Drop orphan hms tables.
  NO_FATALS(RunActionStdoutNone(
        Substitute("hms fix $0 --drop_orphan_hms_tables --nocreate_missing_hms_tables "
                   "--noupgrade_hms_tables --nofix_inconsistent_tables $1",
                   master_addrs_str, hms_flags)));
  make_consistent({
    "default.orphan_hms_table",
    "default.orphan_hms_table_masters",
    "default.orphan_hms_table_external",
    "default.orphan_hms_table_legacy_managed",
  });
  NO_FATALS(check());

  // Create missing hms tables.
  NO_FATALS(RunActionStdoutNone(
        Substitute("hms fix $0 --noupgrade_hms_tables --nofix_inconsistent_tables $1",
                   master_addrs_str, hms_flags)));
  make_consistent({
    "default.kudu_orphan",
    "my_db.table",
  });
  NO_FATALS(check());

  // Upgrade legacy HMS tables.
  NO_FATALS(RunActionStdoutNone(
        Substitute("hms fix $0 --nofix_inconsistent_tables $1", master_addrs_str, hms_flags)));
  make_consistent({
    "default.legacy_managed",
    "default.legacy_purge",
    "legacy_external",
    "default.legacy_no_owner",
    "legacy_hive_incompatible_name",
  });
  NO_FATALS(check());

  // Refresh stale HMS tables.
  NO_FATALS(RunActionStdoutNone(Substitute("hms fix $0 $1", master_addrs_str, hms_flags)));
  make_consistent({
    "default.UPPERCASE",
    "default.inconsistent_schema",
    "default.inconsistent_name",
    "default.inconsistent_cluster",
    "default.inconsistent_master_addrs",
    "default.bad_id",
    "default.different_comment",
    "default.different_owner",
    "default.no_owner_in_hms",
    "default.no_owner_in_kudu",
  });
  NO_FATALS(check());

  ASSERT_TRUE(inconsistent_tables.empty());

  for (const string& table : {
    "control",
    "control_external",
    "uppercase",
    "inconsistent_schema",
    "inconsistent_name_hms",
    "inconsistent_cluster",
    "inconsistent_master_addrs",
    "bad_id",
    "kudu_orphan",
    "legacy_managed",
    "legacy_purge",
    "legacy_external",
    "legacy_hive_incompatible_name",
  }) {
    NO_FATALS(ValidateHmsEntries(&hms_client, kudu_client, "default", table, master_addrs_str));
  }

  // Validate the tables in the other databases.
  NO_FATALS(ValidateHmsEntries(&hms_client, kudu_client, "my_db", "table", master_addrs_str));

  vector<string> kudu_tables;
  ASSERT_OK(kudu_client->ListTables(&kudu_tables));
  std::sort(kudu_tables.begin(), kudu_tables.end());
  ASSERT_EQ(vector<string>({
    "default.bad_id",
    "default.control",
    "default.control_external",
    "default.different_comment",
    "default.different_owner",
    "default.inconsistent_cluster",
    "default.inconsistent_master_addrs",
    "default.inconsistent_name_hms",
    "default.inconsistent_schema",
    "default.kudu_orphan",
    "default.legacy_hive_incompatible_name",
    "default.legacy_managed",
    "default.legacy_no_owner",
    "default.legacy_purge",
    "default.no_owner_in_hms",
    "default.no_owner_in_kudu",
    "default.uppercase",
    "my_db.table",
  }), kudu_tables);

  // Check that table ownership is preserved in upgraded legacy tables and
  // properly synchronized between Kudu and HMS.
  for (const auto& p : vector<pair<string, string>>({
        make_pair("legacy_managed", kUsername),
        make_pair("legacy_purge", kUsername),
        make_pair("legacy_no_owner", ""),
        make_pair("no_owner_in_hms", kUsername),
        make_pair("different_owner", kUsername),
  })) {
    hive::Table table;
    ASSERT_OK(hms_client.GetTable("default", p.first, &table));
    ASSERT_EQ(p.second, table.owner);
  }
}

// Test HMS inconsistencies that must be manually fixed.
// TODO(ghenke): Add test case for external table using the same name as
//  an existing Kudu table.
TEST_P(ToolTestKerberosParameterized, TestCheckAndManualFixHmsMetadata) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  string kUsername = "alice";
  ExternalMiniClusterOptions opts;
  opts.hms_mode = HmsMode::DISABLE_HIVE_METASTORE;
  opts.enable_kerberos = EnableKerberos();
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  thrift::ClientOptions hms_opts;
  hms_opts.enable_kerberos = EnableKerberos();
  hms_opts.service_principal = "hive";
  hms_opts.verify_service_config = false;
  HmsClient hms_client(cluster_->hms()->address(), hms_opts);
  ASSERT_OK(hms_client.Start());
  ASSERT_TRUE(hms_client.IsConnected());

  FLAGS_hive_metastore_uris = cluster_->hms()->uris();
  FLAGS_hive_metastore_sasl_enabled = EnableKerberos();
  HmsCatalog hms_catalog(master_addr);
  ASSERT_OK(hms_catalog.Start());

  shared_ptr<KuduClient> kudu_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &kudu_client));

  // While the metastore integration is disabled create tables in Kudu and the
  // HMS with inconsistent metadata.

  // Test case: Multiple HMS tables pointing to a single Kudu table.
  shared_ptr<KuduTable> duplicate_hms_tables;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.duplicate_hms_tables"));
  ASSERT_OK(kudu_client->OpenTable("default.duplicate_hms_tables", &duplicate_hms_tables));
  ASSERT_OK(hms_catalog.CreateTable(
        duplicate_hms_tables->id(), "default.duplicate_hms_tables",
        kudu_client->cluster_id(), kUsername,
        KuduSchema::ToSchema(duplicate_hms_tables->schema()), duplicate_hms_tables->comment()));
  ASSERT_OK(hms_catalog.CreateTable(
        duplicate_hms_tables->id(), "default.duplicate_hms_tables_2",
        kudu_client->cluster_id(), kUsername,
        KuduSchema::ToSchema(duplicate_hms_tables->schema()), duplicate_hms_tables->comment()));

  // Test case: Kudu tables Hive-incompatible names.
  ASSERT_OK(CreateKuduTable(kudu_client, "default.hive-incompatible-name"));
  ASSERT_OK(CreateKuduTable(kudu_client, "no_database"));

  // Test case: Kudu table in non-existent database.
  ASSERT_OK(CreateKuduTable(kudu_client, "non_existent_database.table"));

  // Test case: a legacy table with a Hive name which conflicts with another table in Kudu.
  ASSERT_OK(CreateLegacyHmsTable(&hms_client, "default", "conflicting_legacy_table",
        "impala::default.conflicting_legacy_table",
        master_addr, HmsClient::kManagedTable, kUsername));
  ASSERT_OK(CreateKuduTable(kudu_client, "impala::default.conflicting_legacy_table"));
  ASSERT_OK(CreateKuduTable(kudu_client, "default.conflicting_legacy_table"));

  const string hms_flags = Substitute("-hive_metastore_uris=$0 -hive_metastore_sasl_enabled=$1",
                                      FLAGS_hive_metastore_uris, FLAGS_hive_metastore_sasl_enabled);

  // Run the HMS check tool and verify that the inconsistent tables are reported.
  auto check = [&] () {
    string out;
    string err;
    Status s = RunActionStdoutStderrString(Substitute("hms check $0 $1", master_addr, hms_flags),
                                           &out, &err);
    SCOPED_TRACE(strings::CUnescapeOrDie(out));
    for (const string& table : vector<string>({
      "duplicate_hms_tables",
      "duplicate_hms_tables_2",
      "default.hive-incompatible-name",
      "no_database",
      "non_existent_database.table",
      "default.conflicting_legacy_table",
    })) {
      ASSERT_STR_CONTAINS(out, table);
    }
  };

  // Check should recognize this inconsistent tables.
  NO_FATALS(check());

  // Fix should fail, since these are not automatically repairable issues.
  {
    string out;
    string err;
    Status s = RunActionStdoutStderrString(Substitute("hms fix $0 $1", master_addr, hms_flags),
                                           &out, &err);
    SCOPED_TRACE(strings::CUnescapeOrDie(out));
    ASSERT_FALSE(s.ok());
  }

  // Check should still fail.
  NO_FATALS(check());

  // Manually drop the duplicate HMS entries.
  ASSERT_OK(hms_catalog.DropTable(duplicate_hms_tables->id(), "default.duplicate_hms_tables_2"));

  // Rename the incompatible names.
  NO_FATALS(RunActionStdoutNone(Substitute(
          "table rename-table --nomodify-external-catalogs $0 "
          "default.hive-incompatible-name default.hive_compatible_name", master_addr)));
  NO_FATALS(RunActionStdoutNone(Substitute(
          "table rename-table --nomodify-external-catalogs $0 "
          "no_database default.with_database", master_addr)));

  // Create the missing database.
  hive::Database db;
  db.name = "non_existent_database";
  ASSERT_OK(hms_client.CreateDatabase(db));

  // Rename the conflicting table.
  NO_FATALS(RunActionStdoutNone(Substitute(
            "table rename-table --nomodify-external-catalogs $0 "
            "default.conflicting_legacy_table default.non_conflicting_legacy_table", master_addr)));

  // Run the automatic fixer to create missing HMS table entries.
  NO_FATALS(RunActionStdoutNone(Substitute("hms fix $0 $1", master_addr, hms_flags)));

  // Check should now be clean.
  NO_FATALS(RunActionStdoutNone(Substitute("hms check $0 $1", master_addr, hms_flags)));

  // Ensure the tables are available.
  vector<string> kudu_tables;
  ASSERT_OK(kudu_client->ListTables(&kudu_tables));
  std::sort(kudu_tables.begin(), kudu_tables.end());
  ASSERT_EQ(vector<string>({
    "default.conflicting_legacy_table",
    "default.duplicate_hms_tables",
    "default.hive_compatible_name",
    "default.non_conflicting_legacy_table",
    "default.with_database",
    "non_existent_database.table",
  }), kudu_tables);
}

TEST_F(ToolTest, TestHmsIgnoresDifferentMasters) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  ExternalMiniClusterOptions opts;
  opts.num_masters = 2;
  opts.hms_mode = HmsMode::ENABLE_METASTORE_INTEGRATION;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  thrift::ClientOptions hms_opts;
  HmsClient hms_client(cluster_->hms()->address(), hms_opts);
  ASSERT_OK(hms_client.Start());

  shared_ptr<KuduClient> kudu_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &kudu_client));

  // Create a Kudu table.
  ASSERT_OK(CreateKuduTable(kudu_client, "default.table"));
  vector<string> master_addrs;
  for (const auto& hp : cluster_->master_rpc_addrs()) {
    master_addrs.emplace_back(hp.ToString());
  }
  const string master_addrs_str = JoinStrings(master_addrs, ",");
  // Do a sanity check that the tool is fine with the existing table.
  // Note: In the happy case, the tool will not log to stdout or stderr.
  NO_FATALS(RunActionStdoutNone(Substitute("hms check $0", master_addrs_str)));

  // Check that the tool will be OK with the addresses in the HMS being
  // reordered.
  {
    std::reverse(master_addrs.begin(), master_addrs.end());
    hive::Table hms_table_reversed_masters;
    ASSERT_OK(AlterHmsWithReplacedParam(&hms_client, "default", "table",
        HmsClient::kKuduMasterAddrsKey, JoinStrings(master_addrs, ",")));
    NO_FATALS(RunActionStdoutNone(Substitute("hms check $0", master_addrs_str)));
  }

  string out;
  string err;
  // Check that the tool will flag cases where the HMS contains masters that
  // aren't quite right that overlap with the correct set of masters (e.g. in
  // the case of a multi-master migration).
  // Try with an extra master.
  ASSERT_OK(AlterHmsWithReplacedParam(&hms_client, "default", "table",
      HmsClient::kKuduMasterAddrsKey, Substitute("$0,other_master_addr", master_addrs_str)));
  Status s = RunActionStdoutStderrString(
      Substitute("hms check $0", master_addrs_str), &out, &err);
  ASSERT_STR_CONTAINS(out, "default.table");
  NO_FATALS(RunActionStdoutNone(Substitute("hms fix $0", master_addrs_str)));
  NO_FATALS(RunActionStdoutNone(Substitute("hms check $0", master_addrs_str)));

  // And with a missing master.
  ASSERT_OK(AlterHmsWithReplacedParam(&hms_client, "default", "table",
      HmsClient::kKuduMasterAddrsKey, cluster_->master_rpc_addrs()[0].ToString()));
  s = RunActionStdoutStderrString(Substitute("hms check $0", master_addrs_str), &out, &err);
  ASSERT_STR_CONTAINS(out, "default.table");
  NO_FATALS(RunActionStdoutNone(Substitute("hms fix $0", master_addrs_str)));
  NO_FATALS(RunActionStdoutNone(Substitute("hms check $0", master_addrs_str)));

  // Set the masters to point to an entirely different set of masters.
  ASSERT_OK(AlterHmsWithReplacedParam(&hms_client, "default", "table",
      HmsClient::kKuduMasterAddrsKey, "other_master_addrs"));

  // The check tool will ignore the HMS metadata from the other cluster, and
  // view the Kudu table as orphaned.
  s = RunActionStdoutStderrString(
      Substitute("hms check $0", master_addrs_str), &out, &err);
  ASSERT_STR_CONTAINS(out, "default.table");

  // Attempting to fix this orphaned Kudu table will lead the tool to attempt
  // to create an HMS table entry, though one already exists.
  s = RunActionStdoutStderrString(
      Substitute("hms fix $0", master_addrs_str), &out, &err);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(err, "table already exists");

  // And when specifying the right flag, it shouldn't ignore the other masters,
  // and should be able to fix the master addresses.
  s = RunActionStdoutStderrString(
      Substitute("hms check $0 --noignore-other-clusters", master_addrs_str), &out, &err);
  ASSERT_STR_CONTAINS(out, "default.table");
  NO_FATALS(RunActionStdoutNone(
      Substitute("hms fix $0 --noignore-other-clusters", master_addrs_str)));
  NO_FATALS(RunActionStdoutNone(
      Substitute("hms check $0 --noignore-other-clusters", master_addrs_str)));
}

// Make sure that `kudu table delete` works as expected when HMS integration
// is enabled, keeping the behavior of the tool backward-compatible even after
// introducing the "table soft-delete" feature.
TEST_F(ToolTest, DropTableHmsEnabled) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  ExternalMiniClusterOptions opts;
  opts.hms_mode = HmsMode::ENABLE_METASTORE_INTEGRATION;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));
  const auto& master_rpc_addr =
      HostPort::ToCommaSeparatedString(cluster_->master_rpc_addrs());
  string out;
  NO_FATALS(RunActionStdoutString(
      Substitute("perf loadgen --keep_auto_table $0", master_rpc_addr), &out));
  NO_FATALS(RunActionStdoutString(
      Substitute("table list $0", master_rpc_addr), &out));
  const auto table_name = out;
  NO_FATALS(RunActionStdoutNone(
      Substitute("table delete $0 $1", master_rpc_addr, table_name)));
}

TEST_F(ToolTest, TestHmsPrecheck) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  ExternalMiniClusterOptions opts;
  opts.hms_mode = HmsMode::ENABLE_HIVE_METASTORE;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();

  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));

  // Create test tables.
  for (const string& table_name : {
      "a.b",
      "foo.bar",
      "FOO.bar",
      "foo.BAR",
      "fuzz",
      "FUZZ",
      "a.b!",
      "A.B!",
  }) {
      ASSERT_OK(CreateKuduTable(client, table_name));
  }

  // Run the precheck tool. It should complain about the conflicting tables.
  string out;
  string err;
  Status s = RunActionStdoutStderrString(Substitute("hms precheck $0", master_addr), &out, &err);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(err, "found tables in Kudu with case-conflicting names");
  ASSERT_STR_CONTAINS(out, "foo.bar");
  ASSERT_STR_CONTAINS(out, "FOO.bar");
  ASSERT_STR_CONTAINS(out, "foo.BAR");

  // It should not complain about tables which don't have conflicting names.
  ASSERT_STR_NOT_CONTAINS(out, "a.b");

  // It should not complain about tables which have Hive-incompatible names.
  ASSERT_STR_NOT_CONTAINS(out, "fuzz");
  ASSERT_STR_NOT_CONTAINS(out, "FUZZ");
  ASSERT_STR_NOT_CONTAINS(out, "a.b!");
  ASSERT_STR_NOT_CONTAINS(out, "A.B!");

  // Rename the conflicting tables. Use the rename table tool to match the actual workflow.
  NO_FATALS(RunActionStdoutNone(Substitute("table rename_table $0 FOO.bar foo.bar2", master_addr)));
  NO_FATALS(RunActionStdoutNone(Substitute("table rename_table $0 foo.BAR foo.bar3", master_addr)));

  // Precheck should now pass, and the cluster should upgrade succesfully.
  NO_FATALS(RunActionStdoutNone(Substitute("hms precheck $0", master_addr)));

  // Enable the HMS integration.
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);
  cluster_->EnableMetastoreIntegration();
  ASSERT_OK(cluster_->Restart());

  // Sanity-check the tables.
  vector<string> tables;
  ASSERT_OK(client->ListTables(&tables));
  std::sort(tables.begin(), tables.end());
  ASSERT_EQ(vector<string>({
      "A.B!",
      "FUZZ",
      "a.b",
      "a.b!",
      "foo.bar",
      "foo.bar2",
      "foo.bar3",
      "fuzz",
  }), tables);
}

TEST_F(ToolTest, TestHmsList) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  ExternalMiniClusterOptions opts;
  opts.hms_mode = HmsMode::ENABLE_HIVE_METASTORE;
  opts.enable_kerberos = EnableKerberos();
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  thrift::ClientOptions hms_opts;
  hms_opts.enable_kerberos = EnableKerberos();
  hms_opts.service_principal = "hive";
  HmsClient hms_client(cluster_->hms()->address(), hms_opts);
  ASSERT_OK(hms_client.Start());
  ASSERT_TRUE(hms_client.IsConnected());

  FLAGS_hive_metastore_uris = cluster_->hms()->uris();
  FLAGS_hive_metastore_sasl_enabled = EnableKerberos();
  HmsCatalog hms_catalog(master_addr);
  ASSERT_OK(hms_catalog.Start());

  shared_ptr<KuduClient> kudu_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &kudu_client));

  // Create a simple Kudu table so we can use the schema.
  shared_ptr<KuduTable> simple_table;
  ASSERT_OK(CreateKuduTable(kudu_client, "default.simple"));
  ASSERT_OK(kudu_client->OpenTable("default.simple", &simple_table));

  string kUsername = "alice";
  ASSERT_OK(hms_catalog.CreateTable(
      "1", "default.table1", "cluster-id", kUsername,
      KuduSchema::ToSchema(simple_table->schema()),
      "comment 1", hms::HmsClient::kManagedTable));
  ASSERT_OK(hms_catalog.CreateTable(
      "2", "default.table2", "cluster-id", nullopt,
      KuduSchema::ToSchema(simple_table->schema()),
      "", hms::HmsClient::kExternalTable));

  // Test the output when HMS integration is disabled.
  string err;
  RunActionStderrString(Substitute("hms list $0", master_addr), &err);
  ASSERT_STR_CONTAINS(err,
      "Configuration error: Could not fetch the Hive Metastore locations from "
      "the Kudu master since it is not configured with the Hive Metastore "
      "integration. Run the tool with --hive_metastore_uris and pass in the "
      "location(s) of the Hive Metastore.");

  // Enable the HMS integration.
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);
  cluster_->EnableMetastoreIntegration();
  ASSERT_OK(cluster_->Restart());

  // Run the list tool with the defaults. atabase,table,type,$0
  string out;
  RunActionStdoutString(Substitute("hms list $0", master_addr), &out);
  ASSERT_STR_CONTAINS(out,
      "default  | table1 | MANAGED_TABLE  | default.table1");
  ASSERT_STR_CONTAINS(out,
      "default  | table2 | EXTERNAL_TABLE | default.table2");

  // Run the list tool filtering columns and using a different format.
  RunActionStdoutString(
      Substitute("hms list --columns table,owner,comment,kudu.table_id, --format=csv $0",
          master_addr), &out);
  ASSERT_STR_CONTAINS(out, "table1,alice,comment 1,1");
  ASSERT_STR_CONTAINS(out, "table2,,,");
}

TEST_F(ToolTest, TestHMSAddressLog) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  ExternalMiniClusterOptions opts;
  opts.hms_mode = HmsMode::ENABLE_HIVE_METASTORE;
  opts.enable_kerberos = EnableKerberos();
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  // Enable the HMS integration.
  cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY);
  cluster_->EnableMetastoreIntegration();
  ASSERT_OK(cluster_->Restart());

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  thrift::ClientOptions hms_opts;
  hms_opts.enable_kerberos = EnableKerberos();
  hms_opts.service_principal = "hive";
  HmsClient hms_client(cluster_->hms()->address(), hms_opts);
  ASSERT_OK(hms_client.Start());
  ASSERT_TRUE(hms_client.IsConnected());

  FLAGS_hive_metastore_uris = cluster_->hms()->uris();
  FLAGS_hive_metastore_sasl_enabled = EnableKerberos();
  HmsCatalog hms_catalog(master_addr);
  ASSERT_OK(hms_catalog.Start());

  string err;
  RunActionStderrString(Substitute("hms list $0 -v 1", master_addr), &err);
  ASSERT_STR_CONTAINS(err,
      Substitute("Attempting to connect to $0", cluster_->hms()->address().ToString()));
}

// This test is parameterized on the serialization mode and Kerberos.
class ControlShellToolTest :
    public ToolTest,
    public ::testing::WithParamInterface<std::tuple<SubprocessProtocol::SerializationMode,
                                                    bool>> {
 public:
  virtual void SetUp() override {
    ToolTest::SetUp();

    // Start the control shell.
    string mode;
    switch (serde_mode()) {
      case SubprocessProtocol::SerializationMode::JSON: mode = "json"; break;
      case SubprocessProtocol::SerializationMode::PB: mode = "pb"; break;
      default: LOG(FATAL) << "Unknown serialization mode";
    }
    shell_.reset(new Subprocess({
      GetKuduToolAbsolutePath(),
      "test",
      "mini_cluster",
      Substitute("--serialization=$0", mode)
    }));
    shell_->ShareParentStdin(false);
    shell_->ShareParentStdout(false);
    ASSERT_OK(shell_->Start());

    // Start the protocol interface.
    proto_.reset(new SubprocessProtocol(serde_mode(),
                                        SubprocessProtocol::CloseMode::CLOSE_ON_DESTROY,
                                        shell_->ReleaseChildStdoutFd(),
                                        shell_->ReleaseChildStdinFd()));
  }

  virtual void TearDown() override {
    if (proto_) {
      // Stopping the protocol interface will close the fds, causing the shell
      // to exit on its own.
      proto_.reset();
      ASSERT_OK(shell_->Wait());
      int exit_status;
      ASSERT_OK(shell_->GetExitStatus(&exit_status));
      ASSERT_EQ(0, exit_status);
    }
    ToolTest::TearDown();
  }

 protected:
  // Send a control message to the shell and receive its response.
  Status SendReceive(const ControlShellRequestPB& req, ControlShellResponsePB* resp) {
    RETURN_NOT_OK(proto_->SendMessage(req));
    RETURN_NOT_OK(proto_->ReceiveMessage(resp));
    if (resp->has_error()) {
      return StatusFromPB(resp->error());
    }
    return Status::OK();
  }

  SubprocessProtocol::SerializationMode serde_mode() const {
    return ::testing::get<0>(GetParam());
  }

  bool enable_kerberos() const {
    return ::testing::get<1>(GetParam());
  }

  unique_ptr<Subprocess> shell_;
  unique_ptr<SubprocessProtocol> proto_;
};

INSTANTIATE_TEST_SUITE_P(SerializationModes, ControlShellToolTest,
                         ::testing::Combine(::testing::Values(
                             SubprocessProtocol::SerializationMode::PB,
                             SubprocessProtocol::SerializationMode::JSON),
                                            ::testing::Bool()));

TEST_P(ControlShellToolTest, TestControlShell) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  constexpr auto kNumMasters = 1;
  constexpr auto kNumTservers = 3;

  // Create an illegal cluster first, to make sure that the shell continues to
  // work in the event of an error.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    req.mutable_create_cluster()->set_num_masters(4);
    ASSERT_OK(proto_->SendMessage(req));
    ASSERT_OK(proto_->ReceiveMessage(&resp));
    ASSERT_TRUE(resp.has_error());
    Status s = StatusFromPB(resp.error());
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "only one or three masters are supported");
  }

  // Create a cluster.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    req.mutable_create_cluster()->set_cluster_root(JoinPathSegments(
        test_dir_, "minicluster-data"));
    req.mutable_create_cluster()->set_num_masters(kNumMasters);
    req.mutable_create_cluster()->set_num_tservers(kNumTservers);
    req.mutable_create_cluster()->set_enable_kerberos(enable_kerberos());
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Try creating a second cluster. It should fail.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    req.mutable_create_cluster()->set_cluster_root(JoinPathSegments(
        test_dir_, "minicluster-data"));
    ASSERT_OK(proto_->SendMessage(req));
    ASSERT_OK(proto_->ReceiveMessage(&resp));
    ASSERT_TRUE(resp.has_error());
    Status s = StatusFromPB(resp.error());
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "cluster already created");
  }

  // Start it.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    req.mutable_start_cluster();
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Get the masters.
  vector<DaemonInfoPB> masters;
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    req.mutable_get_masters();
    ASSERT_OK(SendReceive(req, &resp));
    ASSERT_TRUE(resp.has_get_masters());
    ASSERT_EQ(kNumMasters, resp.get_masters().masters_size());
    masters.assign(resp.get_masters().masters().begin(),
                   resp.get_masters().masters().end());
  }

  if (enable_kerberos()) {
    // Set up the KDC environment variables so that a client can authenticate
    // to this cluster.
    //
    // Normally this is handled automatically by the cluster's MiniKdc, but
    // since the cluster is running in a subprocess, we have to do it manually.
    unordered_map<string, string> kdc_env_vars;
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    req.mutable_get_kdc_env_vars();
    ASSERT_OK(SendReceive(req, &resp));
    ASSERT_TRUE(resp.has_get_kdc_env_vars());
    for (const auto& e : resp.get_kdc_env_vars().env_vars()) {
      PCHECK(setenv(e.first.c_str(), e.second.c_str(), 1) == 0);
    }
  } else {
    // get_kdc_env_vars should fail on a non-Kerberized cluster.
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    req.mutable_get_kdc_env_vars();
    ASSERT_OK(proto_->SendMessage(req));
    ASSERT_OK(proto_->ReceiveMessage(&resp));
    ASSERT_TRUE(resp.has_error());
    Status s = StatusFromPB(resp.error());
    ASSERT_TRUE(s.IsNotFound());
    ASSERT_STR_CONTAINS(s.ToString(), "kdc not found");
  }

  // Create a table.
  {
    KuduClientBuilder client_builder;
    for (const auto& e : masters) {
      HostPort hp = HostPortFromPB(e.bound_rpc_address());
      client_builder.add_master_server_addr(hp.ToString());
    }
    shared_ptr<KuduClient> client;
    ASSERT_OK(client_builder.Build(&client));
    KuduSchemaBuilder schema_builder;
    schema_builder.AddColumn("foo")
              ->Type(client::KuduColumnSchema::INT32)
              ->NotNull()
              ->PrimaryKey();
    KuduSchema schema;
    ASSERT_OK(schema_builder.Build(&schema));
    unique_ptr<client::KuduTableCreator> table_creator(
        client->NewTableCreator());
    ASSERT_OK(table_creator->table_name("test")
              .schema(&schema)
              .set_range_partition_columns({ "foo" })
              .Create());
  }

  // Get the tservers.
  vector<DaemonInfoPB> tservers;
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    req.mutable_get_tservers();
    ASSERT_OK(SendReceive(req, &resp));
    ASSERT_TRUE(resp.has_get_tservers());
    ASSERT_EQ(kNumTservers, resp.get_tservers().tservers_size());
    tservers.assign(resp.get_tservers().tservers().begin(),
                    resp.get_tservers().tservers().end());
  }

  // Stop a tserver.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_stop_daemon()->mutable_id() = tservers[0].id();
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Try to pause a tserver which isn't running.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_pause_daemon()->mutable_id() = tservers[0].id();
    ASSERT_OK(proto_->SendMessage(req));
    ASSERT_OK(proto_->ReceiveMessage(&resp));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error());
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "the process is not there");
  }

  // Try to resume a tserver which isn't running.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_resume_daemon()->mutable_id() = tservers[0].id();
    ASSERT_OK(proto_->SendMessage(req));
    ASSERT_OK(proto_->ReceiveMessage(&resp));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error());
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "the process is not there");
  }

  // Restart it.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_start_daemon()->mutable_id() = tservers[0].id();
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Pause it.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_pause_daemon()->mutable_id() = tservers[0].id();
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Resume it.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_resume_daemon()->mutable_id() = tservers[0].id();
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Stop a master.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_stop_daemon()->mutable_id() = masters[0].id();
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Try to pause a master which isn't running.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_pause_daemon()->mutable_id() = masters[0].id();
    ASSERT_OK(proto_->SendMessage(req));
    ASSERT_OK(proto_->ReceiveMessage(&resp));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error());
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "the process is not there");
  }

  // Try to resume a master which isn't running.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_resume_daemon()->mutable_id() = masters[0].id();
    ASSERT_OK(proto_->SendMessage(req));
    ASSERT_OK(proto_->ReceiveMessage(&resp));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error());
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "the process is not there");
  }

  // Restart it.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_start_daemon()->mutable_id() = masters[0].id();
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Pause it.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_pause_daemon()->mutable_id() = masters[0].id();
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Resume it.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_resume_daemon()->mutable_id() = masters[0].id();
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Restart some non-existent daemons.
  vector<DaemonIdentifierPB> daemons_to_restart;
  {
    // Unknown daemon type.
    DaemonIdentifierPB id;
    id.set_type(UNKNOWN_DAEMON);
    daemons_to_restart.emplace_back(std::move(id));
  }
  {
    // Tablet server #5.
    DaemonIdentifierPB id;
    id.set_type(TSERVER);
    id.set_index(5);
    daemons_to_restart.emplace_back(std::move(id));
  }
  {
    // Master without an index.
    DaemonIdentifierPB id;
    id.set_type(MASTER);
    daemons_to_restart.emplace_back(std::move(id));
  }
  if (!enable_kerberos()) {
    // KDC for a non-Kerberized cluster.
    DaemonIdentifierPB id;
    id.set_type(KDC);
    daemons_to_restart.emplace_back(std::move(id));
  }
  for (const auto& daemon : daemons_to_restart) {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_start_daemon()->mutable_id() = daemon;
    ASSERT_OK(proto_->SendMessage(req));
    ASSERT_OK(proto_->ReceiveMessage(&resp));
    ASSERT_TRUE(resp.has_error());
  }

  // Attempt to pause/resume some non-existent daemons.
  {
    vector<DaemonIdentifierPB> daemons;
    {
      // Unknown daemon type.
      DaemonIdentifierPB id;
      id.set_type(UNKNOWN_DAEMON);
      daemons.emplace_back(std::move(id));
    }
    {
      // Tablet server #5.
      DaemonIdentifierPB id;
      id.set_type(TSERVER);
      id.set_index(5);
      daemons.emplace_back(std::move(id));
    }
    {
      // Tablet server without an index.
      DaemonIdentifierPB id;
      id.set_type(TSERVER);
      daemons.emplace_back(std::move(id));
    }
    {
      // Master without an index.
      DaemonIdentifierPB id;
      id.set_type(MASTER);
      daemons.emplace_back(std::move(id));
    }
    {
      // Master #100.
      DaemonIdentifierPB id;
      id.set_type(MASTER);
      id.set_index(100);
      daemons.emplace_back(std::move(id));
    }
    for (const auto& daemon : daemons) {
      {
        ControlShellRequestPB req;
        ControlShellResponsePB resp;
        *req.mutable_pause_daemon()->mutable_id() = daemon;
        ASSERT_OK(proto_->SendMessage(req));
        ASSERT_OK(proto_->ReceiveMessage(&resp));
        ASSERT_TRUE(resp.has_error());
      }
      {
        ControlShellRequestPB req;
        ControlShellResponsePB resp;
        *req.mutable_resume_daemon()->mutable_id() = daemon;
        ASSERT_OK(proto_->SendMessage(req));
        ASSERT_OK(proto_->ReceiveMessage(&resp));
        ASSERT_TRUE(resp.has_error());
      }
    }
  }

  // Stop the cluster.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    req.mutable_stop_cluster();
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Restart it.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    req.mutable_start_cluster();
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Set flag.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    auto* r = req.mutable_set_daemon_flag();
    *r->mutable_id() = tservers[0].id();
    r->set_flag("rpc_negotiation_timeout_ms");
    r->set_value("5000");
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Try to set a non-existent flag: this should fail.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    auto* r = req.mutable_set_daemon_flag();
    *r->mutable_id() = masters[0].id();
    r->set_flag("__foo_bar_flag__");
    r->set_value("__value__");
    ASSERT_OK(proto_->SendMessage(req));
    ASSERT_OK(proto_->ReceiveMessage(&resp));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error());
    ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "failed to set flag: result: NO_SUCH_FLAG");
  }

  // Try to set a flag on a non-existent daemon: this should fail.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    auto* r = req.mutable_set_daemon_flag();
    r->mutable_id()->set_index(1000);
    r->mutable_id()->set_type(MASTER);
    r->set_flag("flag");
    r->set_value("value");
    ASSERT_OK(proto_->SendMessage(req));
    ASSERT_OK(proto_->ReceiveMessage(&resp));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error());
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "no master with index 1000");
  }

  // Try to set a flag on a KDC: this should fail since mini-KDC doesn't support
  // SetFlag() call.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    auto* r = req.mutable_set_daemon_flag();
    r->mutable_id()->set_index(0);
    r->mutable_id()->set_type(KDC);
    r->set_flag("flag");
    r->set_value("value");
    ASSERT_OK(proto_->SendMessage(req));
    ASSERT_OK(proto_->ReceiveMessage(&resp));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error());
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "mini-KDC doesn't support SetFlag()");
  }

  // Try pause KDC: this should fail since mini-KDC doesn't support that (yet).
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    auto* r = req.mutable_pause_daemon();
    r->mutable_id()->set_index(0);
    r->mutable_id()->set_type(KDC);
    ASSERT_OK(proto_->SendMessage(req));
    ASSERT_OK(proto_->ReceiveMessage(&resp));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error());
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "mini-KDC doesn't support pausing");
  }

  // Try resume KDC: this should fail since mini-KDC doesn't support that (yet).
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    auto* r = req.mutable_resume_daemon();
    r->mutable_id()->set_index(0);
    r->mutable_id()->set_type(KDC);
    ASSERT_OK(proto_->SendMessage(req));
    ASSERT_OK(proto_->ReceiveMessage(&resp));
    ASSERT_TRUE(resp.has_error());
    auto s = StatusFromPB(resp.error());
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "mini-KDC doesn't support resuming");
  }

  if (enable_kerberos()) {
    // Restart the KDC.
    {
      ControlShellRequestPB req;
      ControlShellResponsePB resp;
      req.mutable_stop_daemon()->mutable_id()->set_type(KDC);
      ASSERT_OK(SendReceive(req, &resp));
    }
    {
      ControlShellRequestPB req;
      ControlShellResponsePB resp;
      req.mutable_start_daemon()->mutable_id()->set_type(KDC);
      ASSERT_OK(SendReceive(req, &resp));
    }
    // Test kinit by deleting the ticket cache, kinitting, and
    // ensuring it is recreated.
    {
      char* ccache_path = getenv("KRB5CCNAME");
      ASSERT_TRUE(ccache_path);
      ASSERT_OK(env_->DeleteFile(ccache_path));
      ControlShellRequestPB req;
      ControlShellResponsePB resp;
      req.mutable_kinit()->set_username("test-user");
      ASSERT_OK(SendReceive(req, &resp));
      ASSERT_TRUE(env_->FileExists(ccache_path));
    }
  }

  // Destroy the cluster.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    req.mutable_destroy_cluster();
    ASSERT_OK(SendReceive(req, &resp));
  }
}

static void CreateTableWithFlushedData(const string& table_name,
                                       InternalMiniCluster* cluster,
                                       int num_tablets = 1,
                                       int num_replicas = 1) {
  // Use a schema with a high number of columns to encourage the creation of
  // many data blocks.
  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("key")
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull()
      ->PrimaryKey();
  for (int i = 0; i < 50; i++) {
    schema_builder.AddColumn(Substitute("col$0", i))
        ->Type(client::KuduColumnSchema::INT32)
        ->NotNull();
  }
  KuduSchema schema;
  ASSERT_OK(schema_builder.Build(&schema));


#if defined(THREAD_SANITIZER)
  constexpr auto kNumRows = 1000;
#else
  constexpr auto kNumRows = 10000;
#endif
  // Create a table and write some data to it.
  TestWorkload workload(cluster);
  workload.set_schema(schema);
  workload.set_table_name(table_name);
  workload.set_num_tablets(num_tablets);
  workload.set_num_replicas(num_replicas);
  workload.Setup();
  workload.Start();
  ASSERT_EVENTUALLY([&]() {
    ASSERT_GE(workload.rows_inserted(), kNumRows);
  });
  workload.StopAndJoin();

  // Flush all tablets belonging to this table.
  for (int i = 0; i < cluster->num_tablet_servers(); i++) {
    vector<scoped_refptr<TabletReplica>> replicas;
    cluster->mini_tablet_server(i)->server()->tablet_manager()->GetTabletReplicas(&replicas);
    for (const auto& r : replicas) {
      if (r->tablet_metadata()->table_name() == table_name) {
        ASSERT_OK(r->tablet()->Flush());
      }
    }
  }
}

// This test simulates a user trying to update from not having data directories
// (i.e. just the wal dir). Any supplied `fs_data_dirs` options thereafter that
// don't include `fs_wal_dir` would effectively lead to an attempt at swapping
// data directories, which is not supported.
TEST_F(ToolTest, TestFsSwappingDirectoriesFailsGracefully) {
  if (FLAGS_block_manager == "file") {
    LOG(INFO) << "Skipping test, only log block manager is supported";
    return;
  }

  // Configure the server to share the data root and wal root.
  NO_FATALS(StartMiniCluster());
  MiniTabletServer* mts = mini_cluster_->mini_tablet_server(0);
  mts->Shutdown();

  // Now try to update to put data exclusively in a different directory.
  const string& wal_root = mts->options()->fs_opts.wal_root;
  const string& new_data_root_no_wal = GetTestPath("foo");
  string stderr;
  Status s = RunTool(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1",
      wal_root, new_data_root_no_wal), nullptr, &stderr);
  ASSERT_STR_CONTAINS(stderr, "no healthy directories found");

  // If we instead try to add the directory to the existing list of
  // directories, Kudu should allow it.
  vector<string> new_data_roots = { new_data_root_no_wal, wal_root };
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1",
      wal_root, JoinStrings(new_data_roots, ","))));
}

TEST_F(ToolTest, TestStartEndMaintenanceMode) {
  NO_FATALS(StartMiniCluster());
  // Perform the steps on a tserver that exists and one that doesn't.
  constexpr const char* const kDummyUuid = "foobaruuid";
  MiniMaster* mini_master = mini_cluster_->mini_master();
  const string& ts_uuid = mini_cluster_->mini_tablet_server(0)->uuid();
  TSManager* ts_manager = mini_master->master()->ts_manager();

  // First, do a sanity check that our tservers have no state at first.
  ASSERT_EQ(TServerStatePB::NONE, ts_manager->GetTServerState(ts_uuid));
  ASSERT_EQ(TServerStatePB::NONE, ts_manager->GetTServerState(kDummyUuid));

  // Enter maintenance mode twice. The second time should no-op.
  for (int i = 0; i < 2; i++) {
    NO_FATALS(RunActionStdoutNone(
        Substitute("tserver state enter_maintenance $0 $1",
                  mini_master->bound_rpc_addr().ToString(), ts_uuid)));
    ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE, ts_manager->GetTServerState(ts_uuid));
    ASSERT_EQ(TServerStatePB::NONE, ts_manager->GetTServerState(kDummyUuid));
  }

  // When entering maintenance mode on a tserver that doesn't exist, we should
  // get an error.
  string stderr;
  ASSERT_TRUE(RunActionStderrString(
      Substitute("tserver state enter_maintenance $0 $1",
                 mini_master->bound_rpc_addr().ToString(), kDummyUuid), &stderr).IsRuntimeError());
  ASSERT_STR_CONTAINS(stderr, "has not been registered");
  ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE, ts_manager->GetTServerState(ts_uuid));
  ASSERT_EQ(TServerStatePB::NONE, ts_manager->GetTServerState(kDummyUuid));

  // But operators are able to specify a flag to force the maintenance mode,
  // despite the tserver not being registered.
  NO_FATALS(RunActionStdoutNone(
      Substitute("tserver state enter_maintenance $0 $1 --allow_missing_tserver",
                 mini_master->bound_rpc_addr().ToString(), kDummyUuid)));
  ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE, ts_manager->GetTServerState(ts_uuid));
  ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE, ts_manager->GetTServerState(kDummyUuid));

  // Exit maintenance mode twice. The second time should no-op.
  for (int i = 0; i < 2; i++) {
    NO_FATALS(RunActionStdoutNone(
        Substitute("tserver state exit_maintenance $0 $1",
                  mini_master->bound_rpc_addr().ToString(), ts_uuid)));
    ASSERT_EQ(TServerStatePB::NONE, ts_manager->GetTServerState(ts_uuid));
    ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE, ts_manager->GetTServerState(kDummyUuid));
  }

  // No flag is necessary to remove the state of a tserver that hasn't been
  // registered. The second time should no-op as well, even though the dummy
  // tserver doesn't exist.
  for (int i = 0; i < 2; i++) {
    NO_FATALS(RunActionStdoutNone(
        Substitute("tserver state exit_maintenance $0 $1",
                  mini_master->bound_rpc_addr().ToString(), kDummyUuid)));
    ASSERT_EQ(TServerStatePB::NONE, ts_manager->GetTServerState(ts_uuid));
    ASSERT_EQ(TServerStatePB::NONE, ts_manager->GetTServerState(kDummyUuid));
  }
}

TEST_F(ToolTest, TestFsRemoveDataDirWithTombstone) {
  if (FLAGS_block_manager == "file") {
    LOG(INFO) << "Skipping test, only log block manager is supported";
    return;
  }

  // Start a cluster whose tserver has multiple data directories and create a
  // tablet on it.
  InternalMiniClusterOptions opts;
  opts.num_data_dirs = 2;
  NO_FATALS(StartMiniCluster(std::move(opts)));
  NO_FATALS(CreateTableWithFlushedData("tablename", mini_cluster_.get()));

  // Tombstone the tablet.
  MiniTabletServer* mts = mini_cluster_->mini_tablet_server(0);
  vector<string> tablet_ids = mts->ListTablets();
  ASSERT_EQ(1, tablet_ids.size());
  TabletServerErrorPB::Code error;
  ASSERT_OK(mts->server()->tablet_manager()->DeleteTablet(
      tablet_ids[0], TabletDataState::TABLET_DATA_TOMBSTONED, nullopt, &error));

  // Set things up so we can restart with one fewer directory.
  string data_root = mts->options()->fs_opts.data_roots[0];
  mts->options()->fs_opts.data_roots = { data_root };
  mts->Shutdown();
  // KUDU-2680: tombstones shouldn't prevent us from removing a directory.
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1 $2",
      mts->options()->fs_opts.wal_root, data_root,
      env_->IsEncryptionEnabled() ? GetEncryptionArgs() : "")));

  ASSERT_OK(mts->Start());
  ASSERT_OK(mts->WaitStarted());
  ASSERT_EQ(0, mts->server()->tablet_manager()->GetNumLiveTablets());
}

TEST_F(ToolTest, TestFsAddRemoveDataDirEndToEnd) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  const string kTableFoo = "foo";
  const string kTableBar = "bar";

  if (FLAGS_block_manager == "file") {
    LOG(INFO) << "Skipping test, only log block manager is supported";
    return;
  }

  // Disable the available space heuristic as it can interfere with the desired
  // test invariant below (that the newly created table write to the newly added
  // data directory).
  FLAGS_fs_data_dirs_consider_available_space = false;

  // Start a cluster whose tserver has multiple data directories.
  InternalMiniClusterOptions opts;
  opts.num_data_dirs = 2;
  NO_FATALS(StartMiniCluster(std::move(opts)));

  // Create a table and flush some test data to it.
  NO_FATALS(CreateTableWithFlushedData(kTableFoo, mini_cluster_.get()));

  // Add a new data directory.
  MiniTabletServer* mts = mini_cluster_->mini_tablet_server(0);
  const string& wal_root = mts->options()->fs_opts.wal_root;
  vector<string> data_roots = mts->options()->fs_opts.data_roots;
  string to_add = JoinPathSegments(DirName(data_roots.back()), "data-new");
  data_roots.emplace_back(to_add);
  mts->Shutdown();
  string encryption_args = env_->IsEncryptionEnabled() ? GetEncryptionArgs() : "";
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1 $2",
      wal_root, JoinStrings(data_roots, ","), encryption_args)));

  // Reconfigure the tserver to use the newly added data directory and restart it.
  //
  // Note: WaitStarted() will return a bad status if any tablets fail to bootstrap.
  mts->options()->fs_opts.data_roots = data_roots;
  ASSERT_OK(mts->Start());
  ASSERT_OK(mts->WaitStarted());

  // Create a second table and flush some data. The flush should write some
  // data to the newly added data directory.
  uint64_t disk_space_used_before;
  ASSERT_OK(env_->GetFileSizeOnDiskRecursively(to_add, &disk_space_used_before));
  NO_FATALS(CreateTableWithFlushedData(kTableBar, mini_cluster_.get()));
  uint64_t disk_space_used_after;
  ASSERT_OK(env_->GetFileSizeOnDiskRecursively(to_add, &disk_space_used_after));
  ASSERT_GT(disk_space_used_after, disk_space_used_before);

  // Try to remove the newly added data directory. This will fail because
  // tablets from the second table are configured to use it.
  mts->Shutdown();
  data_roots.pop_back();
  string stderr;
  ASSERT_TRUE(RunActionStderrString(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1 $2",
      wal_root, JoinStrings(data_roots, ","), encryption_args), &stderr).IsRuntimeError());
  ASSERT_STR_CONTAINS(
      stderr, "Not found: cannot update data directories: at least one "
      "tablet is configured to use removed data directory. Retry with --force "
      "to override this");

  // Make sure the failure really had no effect.
  ASSERT_OK(mts->Start());
  ASSERT_OK(mts->WaitStarted());
  mts->Shutdown();

  // If we force the removal it'll succeed, but the tserver will fail to
  // bootstrap some tablets when restarted.
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1 $2 --force",
      wal_root, JoinStrings(data_roots, ","), encryption_args)));
  mts->options()->fs_opts.data_roots = data_roots;
  ASSERT_OK(mts->Start());
  Status s = mts->WaitStarted();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "one or more data dirs may have been removed");

  // Tablets belonging to the first table should still be OK, but those in the
  // second table should have all failed.
  {
    vector<scoped_refptr<TabletReplica>> replicas;
    mts->server()->tablet_manager()->GetTabletReplicas(&replicas);
    ASSERT_GT(replicas.size(), 0);
    for (const auto& r : replicas) {
      const string& table_name = r->tablet_metadata()->table_name();
      if (table_name == kTableFoo) {
        ASSERT_EQ(tablet::RUNNING, r->state());
      } else {
        ASSERT_EQ(kTableBar, table_name);
        ASSERT_EQ(tablet::FAILED, r->state());
      }
    }
  }

  // Add the removed data directory back. All tablets should successfully
  // bootstrap.
  mts->Shutdown();
  data_roots.emplace_back(to_add);
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1 $2",
      wal_root, JoinStrings(data_roots, ","), encryption_args)));
  mts->options()->fs_opts.data_roots = data_roots;
  ASSERT_OK(mts->Start());
  ASSERT_OK(mts->WaitStarted());
  mts->Shutdown();

  // Remove it again so that the second table's tablets fail once again.
  data_roots.pop_back();
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1 $2 --force",
      wal_root, JoinStrings(data_roots, ","), encryption_args)));
  mts->options()->fs_opts.data_roots = data_roots;
  ASSERT_OK(mts->Start());
  s = mts->WaitStarted();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "one or more data dirs may have been removed");

  // Delete the second table and wait for all of its tablets to be deleted.
  shared_ptr<KuduClient> client;
  ASSERT_OK(mini_cluster_->CreateClient(nullptr, &client));
  ASSERT_OK(client->DeleteTable(kTableBar));
  ASSERT_EVENTUALLY([&]{
    vector<scoped_refptr<TabletReplica>> replicas;
    mts->server()->tablet_manager()->GetTabletReplicas(&replicas);
    for (const auto& r : replicas) {
      ASSERT_NE(kTableBar, r->tablet_metadata()->table_name());
    }
  });

  // Shut down the tserver, add a new data directory, and restart it. There
  // should be no bootstrapping errors because the second table (whose tablets
  // were configured to use the removed data directory) is gone.
  mts->Shutdown();
  data_roots.emplace_back(JoinPathSegments(DirName(data_roots.back()), "data-new2"));
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1 $2",
      wal_root, JoinStrings(data_roots, ","), encryption_args)));
  mts->options()->fs_opts.data_roots = data_roots;
  ASSERT_OK(mts->Start());
  ASSERT_OK(mts->WaitStarted());

  // Shut down again, delete the newly added data directory, and restart. No
  // errors, because the first table's tablets were never configured to use the
  // new data directory.
  mts->Shutdown();
  data_roots.pop_back();
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1 $2",
      wal_root, JoinStrings(data_roots, ","), encryption_args)));
  mts->options()->fs_opts.data_roots = data_roots;
  ASSERT_OK(mts->Start());
  ASSERT_OK(mts->WaitStarted());
}

TEST_F(ToolTest, TestCheckFSWithNonDefaultMetadataDir) {
  const string kTestDir = GetTestPath("test");
  ASSERT_OK(env_->CreateDir(kTestDir));
  string uuid;
  FsManagerOpts opts;
  {
    opts.wal_root = JoinPathSegments(kTestDir, "wal");
    opts.metadata_root = JoinPathSegments(kTestDir, "meta");
    FsManager fs(env_, opts);
    ASSERT_OK(fs.CreateInitialFileSystemLayout());
    ASSERT_OK(fs.Open());
    uuid = fs.uuid();
  }
  // Because a non-default metadata directory was specified, users must provide
  // enough arguments to open the FsManager, or else FS tools will not work.
  // The tool will fail in its own process. Catch its output.
  string stderr;
  Status s = RunTool(Substitute("fs check --fs_wal_dir=$0", opts.wal_root),
                    nullptr, &stderr, {}, {});
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_CONTAINS(s.ToString(), "process exited with non-zero status");
  SCOPED_TRACE(stderr);
  ASSERT_STR_CONTAINS(stderr, "could not verify required directory");

  // Providing the necessary arguments, the tool should work.
  string stdout;
  NO_FATALS(RunActionStdoutString(Substitute(
      "fs check --fs_wal_dir=$0 --fs_metadata_dir=$1",
      opts.wal_root, opts.metadata_root), &stdout));
  SCOPED_TRACE(stdout);
}

TEST_F(ToolTest, TestReplaceTablet) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  constexpr int kNumTservers = 3;
  constexpr int kNumTablets = 3;
  constexpr int kNumRows = 1000;
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = kNumTservers;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  // Setup a table using TestWorkload.
  TestWorkload workload(cluster_.get());
  workload.set_num_tablets(kNumTablets);
  workload.set_num_replicas(kNumTservers);
  workload.Setup();

  // Insert some rows.
  workload.Start();
  while (workload.rows_inserted() < kNumRows) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  // Pick a tablet to break.
  vector<string> tablet_ids;
  TServerDetails* ts = ts_map_[cluster_->tablet_server(0)->uuid()];
  ASSERT_OK(ListRunningTabletIds(ts, kTimeout, &tablet_ids));
  const string old_tablet_id = tablet_ids[Random(SeedRandom()).Uniform(tablet_ids.size())];

  // Break the tablet by doing the following:
  // 1. Tombstone two replicas.
  // 2. Stop the tablet server hosting the third.
  for (int i = 0; i < 2; i ++) {
    ASSERT_OK(DeleteTablet(ts_map_[cluster_->tablet_server(i)->uuid()], old_tablet_id,
                           TabletDataState::TABLET_DATA_TOMBSTONED, kTimeout));
  }
  cluster_->tablet_server(2)->Shutdown();

  // Replace the tablet.
  const string cmd = Substitute("tablet unsafe_replace_tablet $0 $1",
                                cluster_->master()->bound_rpc_addr().ToString(),
                                old_tablet_id);
  string stderr;
  NO_FATALS(RunActionStderrString(cmd, &stderr));
  ASSERT_STR_CONTAINS(stderr, old_tablet_id);

  // Restart the down tablet server.
  ASSERT_OK(cluster_->tablet_server(2)->Restart());

  // After replacement and a short delay, the new tablet should be present
  // and the old tablet should be gone, leaving overall the same number of tablets.
  ASSERT_EVENTUALLY([&]() {
    for (int i = 0; i < kNumTservers; i++) {
      ts = ts_map_[cluster_->tablet_server(i)->uuid()];
      ASSERT_OK(ListRunningTabletIds(ts, kTimeout, &tablet_ids));
      ASSERT_TRUE(std::none_of(tablet_ids.begin(), tablet_ids.end(),
            [&](const string& tablet_id) { return tablet_id == old_tablet_id; }));
    }
  });

  // The replaced tablet can self-heal because of tombstoned voting.
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());

  // Sanity check: there should be no more rows than we inserted before the replace.
  // TODO(wdberkeley): Should also be possible to keep inserting through a replace.
  client::sp::shared_ptr<KuduTable> workload_table;
  ASSERT_OK(workload.client()->OpenTable(workload.table_name(), &workload_table));
  ASSERT_GE(workload.rows_inserted(), CountTableRows(workload_table.get()));
}

TEST_F(ToolTest, TestGetFlags) {
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = 1;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  // Check that we get non-default flags.
  // CSV formatting is easiest to match against.
  // It seems safe to assume that -help and -logemaillevel will not be
  // set, and that fs_wal_dir will be set to a non-default value.
  for (const string& daemon_type : { "master", "tserver" }) {
    const string& daemon_addr = daemon_type == "master" ?
                                cluster_->master()->bound_rpc_addr().ToString() :
                                cluster_->tablet_server(0)->bound_rpc_addr().ToString();
    const string& wal_dir = daemon_type == "master" ?
                            cluster_->master()->wal_dir() :
                            cluster_->tablet_server(0)->wal_dir();
    string out;
    NO_FATALS(RunActionStdoutString(
          Substitute("$0 get_flags $1 -format=csv", daemon_type, daemon_addr),
          &out));
    ASSERT_STR_NOT_MATCHES(out, "help,*");
    ASSERT_STR_NOT_MATCHES(out, "logemaillevel,*");
    ASSERT_STR_CONTAINS(out, Substitute("fs_wal_dir,$0,false", wal_dir));

    // Check that we get all flags with -all_flags.
    out.clear();
    NO_FATALS(RunActionStdoutString(
          Substitute("$0 get_flags $1 -format=csv -all_flags", daemon_type, daemon_addr),
          &out));
    ASSERT_STR_CONTAINS(out, "help,false,true");
    ASSERT_STR_CONTAINS(out, "logemaillevel,999,true");
    ASSERT_STR_CONTAINS(out, Substitute("fs_wal_dir,$0,false", wal_dir));

    // Check that -flag_tags filter to matching tags.
    // -logemaillevel is an unsafe flag.
    out.clear();
    NO_FATALS(RunActionStdoutString(
          Substitute("$0 get_flags $1 -format=csv -all_flags -flag_tags=stable",
                     daemon_type, daemon_addr),
          &out));
    ASSERT_STR_CONTAINS(out, "help,false,true");
    ASSERT_STR_NOT_MATCHES(out, "logemaillevel,*");
    ASSERT_STR_CONTAINS(out, Substitute("fs_wal_dir,$0,false", wal_dir));

    // Check that we get flags with -flags.
    out.clear();
    NO_FATALS(RunActionStdoutString(
        Substitute("$0 get_flags $1 -format=csv -flags=fs_wal_dir,logemaillevel",
                   daemon_type, daemon_addr),
        &out));
    ASSERT_STR_NOT_MATCHES(out, "help*");
    ASSERT_STR_CONTAINS(out, "logemaillevel,999,true");
    ASSERT_STR_CONTAINS(out, Substitute("fs_wal_dir,$0,false", wal_dir));

    // Check -flags will ignore -all_flags.
    out.clear();
    NO_FATALS(RunActionStdoutString(
        Substitute("$0 get_flags $1 -format=csv -all_flags -flags=logemaillevel",
                   daemon_type, daemon_addr),
        &out));
    ASSERT_STR_NOT_MATCHES(out, "help*");
    ASSERT_STR_CONTAINS(out, "logemaillevel,999,true");
    ASSERT_STR_NOT_MATCHES(out, "fs_wal_dir*");

    // Check -flag_tags filter to matching tags with -flags.
    out.clear();
    NO_FATALS(RunActionStdoutString(
        Substitute("$0 get_flags $1 -format=csv -flags=logemaillevel -flag_tags=stable",
                   daemon_type, daemon_addr),
        &out));
    ASSERT_STR_NOT_MATCHES(out, "help*");
    ASSERT_STR_NOT_MATCHES(out, "logemaillevel,*");
    ASSERT_STR_NOT_MATCHES(out, "fs_wal_dir*");
  }
}

// This is a synthetic test to provide coverage for regressions of KUDU-2819.
TEST_F(ToolTest, TabletServersWithUnusualFlags) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  // Run many tablet servers: it helps in detection of races, if any.
#if defined(THREAD_SANITIZER)
  // In case of TSAN builds, it takes too long to wait for the start up of too
  // many tablet servers.
  static constexpr int kNumTabletServers = 32;
#else
  // Run as many tablet servers in external minicluster as possible.
  static constexpr int kNumTabletServers = 62;
#endif
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = kNumTabletServers;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  // Leave only one tablet server running, shutdown all others.
  for (size_t i = 1; i < kNumTabletServers; ++i) {
    cluster_->tablet_server(i)->Shutdown();
  }

  // The 'cluster ksck' tool should report on unavailable tablet servers.
  const string& master_addr = cluster_->master()->bound_rpc_addr().ToString();
  {
    string err;
    Status s = RunActionStderrString(
        Substitute("cluster ksck $0", master_addr), &err);
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(err, "Runtime error: ksck discovered errors");
  }

  // The 'cluster rebalance' tool should bail and report an error due to
  // unavailability of tablet servers in the cluster.
  {
    string err;
    Status s = RunActionStderrString(
        Substitute("cluster rebalance $0", master_addr), &err);
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(err, "unacceptable health status UNAVAILABLE");
  }
}

TEST_F(ToolTest, TestParseStacks) {
  const string kDataPath = JoinPathSegments(GetTestExecutableDirectory(),
                                            "testdata/sample-diagnostics-log.txt");
  const string kBadDataPath = JoinPathSegments(GetTestExecutableDirectory(),
                                               "testdata/bad-diagnostics-log.txt");
  string stdout;
  NO_FATALS(RunActionStdoutString(
      Substitute("diagnose parse_stacks $0", kDataPath),
      &stdout));
  // Spot check a few of the things that should be in the output.
  ASSERT_STR_CONTAINS(stdout, "Stacks at 0314 11:54:20.737790 (periodic):");
  ASSERT_STR_CONTAINS(stdout, "0x1caef51 kudu::StackTraceSnapshot::SnapshotAllStacks()");
  ASSERT_STR_CONTAINS(stdout, "0x3f5ec0f710 <unknown>");

  string stderr;
  Status s = RunActionStderrString(
      Substitute("diagnose parse_stacks $0", kBadDataPath),
      &stderr);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_MATCHES(stderr, "failed to parse stacks from .* line 1.*"
                             "invalid JSON payload.*Missing a closing quotation mark in string");
}

TEST_F(ToolTest, TestParseMetrics) {
  const string kDataPath = JoinPathSegments(GetTestExecutableDirectory(),
                                            "testdata/sample-diagnostics-log.txt");
  const string kBadDataPath = JoinPathSegments(GetTestExecutableDirectory(),
                                               "testdata/bad-diagnostics-log.txt");
  // Check out tablet metrics.
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(
        Substitute("diagnose parse_metrics $0 -simple-metrics=tablet.on_disk_size:size_bytes "
                   "-rate-metrics=tablet.on_disk_size:bytes_rate", kDataPath),
        &stdout));
    // Spot check a few of the things that should be in the output.
    ASSERT_STR_CONTAINS(stdout, "timestamp\tsize_bytes\tbytes_rate");
    ASSERT_STR_CONTAINS(stdout, "1521053715220449\t69705682\t0");
    ASSERT_STR_CONTAINS(stdout, "1521053775220513\t69705682\t0");
    ASSERT_STR_CONTAINS(stdout, "1521053835220611\t69712809\t118.78313932087244");
    ASSERT_STR_CONTAINS(stdout, "1521053895220710\t69712809\t0");
    ASSERT_STR_CONTAINS(stdout, "1661840031227204\t69712809\t0");
  }
  // Check out table metrics.
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(
        Substitute("diagnose parse_metrics $0 "
                   "-simple-metrics=table.on_disk_size:size,table.live_row_count:row_count",
                   kDataPath),
        &stdout));
    // Spot check a few of the things that should be in the output.
    ASSERT_STR_CONTAINS(stdout, "timestamp\trow_count\tsize");
    ASSERT_STR_CONTAINS(stdout, "1521053715220449\t0\t0");
    ASSERT_STR_CONTAINS(stdout, "1521053775220513\t0\t0");
    ASSERT_STR_CONTAINS(stdout, "1521053835220611\t0\t0");
    ASSERT_STR_CONTAINS(stdout, "1521053895220710\t0\t0");
    ASSERT_STR_CONTAINS(stdout, "1661840031227204\t228265\t78540528");
  }
  // Check out table metrics with default metric names.
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(
        Substitute("diagnose parse_metrics $0 "
                   "-simple-metrics=table.on_disk_size,table.live_row_count",
                   kDataPath),
        &stdout));
    // Spot check a few of the things that should be in the output.
    ASSERT_STR_CONTAINS(stdout, "timestamp\tlive_row_count\ton_disk_size");
    ASSERT_STR_CONTAINS(stdout, "1521053715220449\t0\t0");
    ASSERT_STR_CONTAINS(stdout, "1521053775220513\t0\t0");
    ASSERT_STR_CONTAINS(stdout, "1521053835220611\t0\t0");
    ASSERT_STR_CONTAINS(stdout, "1521053895220710\t0\t0");
    ASSERT_STR_CONTAINS(stdout, "1661840031227204\t228265\t78540528");
  }

  string stderr;
  ASSERT_OK(RunActionStderrString(
      Substitute("diagnose parse_metrics $0 -simple-metrics=tablet.on_disk_size:size",
      kBadDataPath), &stderr));
  ASSERT_STR_MATCHES(stderr, "Skipping file.*invalid JSON payload.*Missing "
                             "a closing quotation mark in string");
}

TEST_F(ToolTest, ClusterNameResolverEnvNotSet) {
  const string kClusterName = "external_mini_cluster";
  CHECK_ERR(unsetenv("KUDU_CONFIG"));
  string stderr;
  Status s = RunActionStderrString(
        Substitute("master list @$0", kClusterName),
        &stderr);
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_CONTAINS(
      stderr, "Not found: ${KUDU_CONFIG} is missing");
}

TEST_F(ToolTest, ClusterNameResolverFileNotExist) {
  const string kClusterName = "external_mini_cluster";
  CHECK_ERR(setenv("KUDU_CONFIG", GetTestDataDirectory().c_str(), 1));
  SCOPED_CLEANUP({
    CHECK_ERR(unsetenv("KUDU_CONFIG"));
  });

  string stderr;
  Status s = RunActionStderrString(
        Substitute("master list @$0", kClusterName),
        &stderr);
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_CONTAINS(
      stderr, Substitute("Not found: configuration file $0/kudurc was not found",
              GetTestDataDirectory()));
}

TEST_F(ToolTest, ClusterNameResolverFileCorrupt) {
  ExternalMiniClusterOptions opts;
  opts.num_masters = 3;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));
  auto master_addrs_str = HostPort::ToCommaSeparatedString(cluster_->master_rpc_addrs());

  // Prepare ${KUDU_CONFIG}.
  const string kClusterName = "external_mini_cluster";
  CHECK_ERR(setenv("KUDU_CONFIG", GetTestDataDirectory().c_str(), 1));
  SCOPED_CLEANUP({
    CHECK_ERR(unsetenv("KUDU_CONFIG"));
  });

  // Missing 'clusters_info' section.
  NO_FATALS(CheckCorruptClusterInfoConfigFile(
              Substitute(R"*($0:)*""\n"
                         R"*(  master_addresses: $1)*", kClusterName, master_addrs_str),
              "Not found: parse field clusters_info error: invalid node; "));

  // Missing specified cluster name.
  NO_FATALS(CheckCorruptClusterInfoConfigFile(
              Substitute(R"*(clusters_info:)*""\n"
                         R"*(  $0some_suffix:)*""\n"
                         R"*(    master_addresses: $1)*", kClusterName, master_addrs_str),
              Substitute("Not found: parse field $0 error: invalid node; ",
                         kClusterName)));

  // Missing 'master_addresses' section.
  NO_FATALS(CheckCorruptClusterInfoConfigFile(
              Substitute(R"*(clusters_info:)*""\n"
                         R"*(  $0:)*""\n"
                         R"*(    master_addresses_some_suffix: $1)*", kClusterName, master_addrs_str),
              "Not found: parse field master_addresses error: invalid node; "));

  // Invalid 'master_addresses' value.
  NO_FATALS(CheckCorruptClusterInfoConfigFile(
              Substitute(R"*(clusters_info:)*""\n"
                         R"*(  $0:)*""\n"
                         R"*(    master_addresses: bad,masters,addresses)*", kClusterName),
              "Network error: Could not connect to the cluster: unable to resolve "
              "address for bad: "));
}

TEST_F(ToolTest, ClusterNameResolverNormal) {
  ExternalMiniClusterOptions opts;
  opts.num_masters = 3;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));
  auto master_addrs_str = HostPort::ToCommaSeparatedString(cluster_->master_rpc_addrs());

  // Prepare ${KUDU_CONFIG}.
  const string kClusterName = "external_mini_cluster";
  CHECK_ERR(setenv("KUDU_CONFIG", GetTestDataDirectory().c_str(), 1));
  SCOPED_CLEANUP({
    CHECK_ERR(unsetenv("KUDU_CONFIG"));
  });

  string content = Substitute(
      R"*(# some header comments)*""\n"
      R"*(clusters_info:)*""\n"
      R"*(  $0:  # some section comments)*""\n"
      R"*(    master_addresses: $1  # some key comments)*", kClusterName, master_addrs_str);

  NO_FATALS(PrepareConfigFile(content));

  // Verify output of the two methods.
  string out1;
  NO_FATALS(RunActionStdoutString(
        Substitute("master list $0 --columns=uuid,rpc-addresses", master_addrs_str),
        &out1));
  string out2;
  NO_FATALS(RunActionStdoutString(
        Substitute("master list @$0 --columns=uuid,rpc-addresses", kClusterName),
        &out2));
  ASSERT_EQ(out1, out2);
}

class Is343ReplicaUtilTest :
    public ToolTest,
    public ::testing::WithParamInterface<bool> {
};
INSTANTIATE_TEST_SUITE_P(, Is343ReplicaUtilTest, ::testing::Bool());
TEST_P(Is343ReplicaUtilTest, Is343Cluster) {
  constexpr auto kReplicationFactor = 3;
  const auto is_343_scheme = GetParam();

  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = kReplicationFactor;
  opts.extra_master_flags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
  };
  opts.extra_tserver_flags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
  };
  NO_FATALS(StartExternalMiniCluster(opts));
  const auto& master_addr = cluster_->master()->bound_rpc_addr().ToString();

  {
    const string empty_name = "";
    bool is_343 = false;
    const auto s = Is343SchemeCluster({ master_addr }, empty_name, &is_343);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }

  {
    bool is_343 = false;
    const auto s = Is343SchemeCluster({ master_addr }, nullopt, &is_343);
    ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "not a single table found");
  }

  // Create a table.
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(kReplicationFactor);
  workload.set_table_name("is_343_test_table");
  workload.Setup();

  {
    bool is_343 = false;
    const auto s = Is343SchemeCluster({ master_addr }, nullopt, &is_343);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(is_343_scheme, is_343);
  }
}

class AuthzTServerChecksumTest : public ToolTest {
 public:
  void SetUp() override {
    ExternalMiniClusterOptions opts;
    opts.extra_tserver_flags.emplace_back("--tserver_enforce_access_control=true");
    NO_FATALS(StartExternalMiniCluster(std::move(opts)));
  }
};

// Test the authorization of Checksum scans via the CLI.
TEST_F(AuthzTServerChecksumTest, TestAuthorizeChecksum) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  // First, let's create a table.
  const vector<string> loadgen_args = {
    "perf", "loadgen",
    cluster_->master()->bound_rpc_addr().ToString(),
    "--keep_auto_table",
    "--num_rows_per_thread=0",
  };
  ASSERT_OK(RunKuduTool(loadgen_args));

  // Running a checksum scan should succeed since the tool is run as the OS
  // user, which is the default super-user.
  const vector<string> checksum_args = {
    "cluster", "ksck",
    cluster_->master()->bound_rpc_addr().ToString(),
    "--checksum_scan"
  };
  ASSERT_OK(RunKuduTool(checksum_args));
}

// Regression test for KUDU-2851.
TEST_F(ToolTest, TestFailedTableScan) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  // Create a table using the loadgen tool.
  const string kTableName = "db.table";
  NO_FATALS(RunLoadgen(/*num_tservers*/1, /*tool_args*/{},kTableName));

  // Now shut down the tablet servers so the scans cannot proceed.
  // Upon running the scan tool, we should get a TimedOut status.
  NO_FATALS(cluster_->ShutdownNodes(cluster::ClusterNodes::TS_ONLY));

  // Getting an error when running the scan tool should spit out errors
  // instead of crashing.
  string stdout;
  string stderr;
  Status s = RunTool(Substitute("perf table_scan $0 $1 -num_threads=2",
                                cluster_->master()->bound_rpc_addr().ToString(),
                                kTableName),
                     &stdout, &stderr, nullptr, nullptr);

  ASSERT_TRUE(s.IsRuntimeError());
  SCOPED_TRACE(stderr);
  ASSERT_STR_CONTAINS(stderr, "Timed out");
}

TEST_F(ToolTest, TestFailedTableCopy) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  // Create a table using the loadgen tool.
  const string kTableName = "db.table";
  NO_FATALS(RunLoadgen(/*num_tservers*/1, /*tool_args*/{},kTableName));

  // Create a destination table.
  const string kDstTableName = "kudu.table.copy.to";

  // Now shut down the tablet servers so the scans cannot proceed.
  // Upon running the scan tool, we should get a TimedOut status.
  NO_FATALS(cluster_->ShutdownNodes(cluster::ClusterNodes::TS_ONLY));

  // Getting an error when running the copy tool should spit out errors
  // instead of crashing.
  string stdout;
  string stderr;
  Status s = RunTool(Substitute("table copy $0 $1 $2 -dst_table=$3",
                                cluster_->master()->bound_rpc_addr().ToString(),
                                kTableName,
                                cluster_->master()->bound_rpc_addr().ToString(),
                                kDstTableName),
                     &stdout, &stderr, nullptr, nullptr);

  ASSERT_TRUE(s.IsRuntimeError());
  SCOPED_TRACE(stderr);
  ASSERT_STR_CONTAINS(stderr, "Timed out");
}

TEST_F(ToolTest, TestGetTableStatisticsNotSupported) {
  ExternalMiniClusterOptions opts;
  opts.extra_master_flags = { "--mock_table_metrics_for_testing=true",
                              "--on_disk_size_for_testing=77",
                              "--catalog_manager_support_on_disk_size=false",
                              "--live_row_count_for_testing=99",
                              "--catalog_manager_support_live_row_count=false" };
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  // Create an empty table.
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup();

  string stdout;
  NO_FATALS(RunActionStdoutString(
      Substitute("table statistics $0 $1",
                 cluster_->master()->bound_rpc_addr().ToString(),
                 TestWorkload::kDefaultTableName),
      &stdout));
  ASSERT_STR_CONTAINS(stdout, "on disk size: N/A");
  ASSERT_STR_CONTAINS(stdout, "live row count: N/A");
}

// Run one of the cluster-targeted subcommands verifying the functionality of
// the --connection_negotiation_timeout command-line option (the RPC
// connection negotiation timeout).
TEST_F(ToolTest, ConnectionNegotiationTimeoutOption) {
  static constexpr const char* const kPattern =
      "cluster ksck $0 --negotiation_timeout_ms=$1 --timeout_ms=$2";

  FLAGS_rpc_negotiation_inject_delay_ms = 200;
  NO_FATALS(StartMiniCluster());
  const auto rpc_addr = mini_cluster_->mini_master()->bound_rpc_addr().ToString();

  {
    string msg;
    // Set the RPC timeout to be a bit longer than the connection negotiation
    // timeout, but not too much: otherwise, there would be many RPC retries.
    auto s = RunActionStderrString(Substitute(kPattern, rpc_addr, 10, 11), &msg);
    ASSERT_TRUE(s.IsRuntimeError());
    ASSERT_STR_CONTAINS(msg, Substitute(
        "Timed out: Client connection negotiation failed: "
        "client connection to $0", rpc_addr));
    ASSERT_STR_MATCHES(msg,
        "(Timeout exceeded waiting to connect)|"
        "(received|sent) 0 of .* requested bytes");
  }

  {
    string msg;
    // Set the RPC timeout to be a bit longer than the connection negotiation
    // timeout, but not too much: otherwise, there would be many RPC retries.
    auto s = RunActionStderrString(Substitute(kPattern, rpc_addr, 2, 1), &msg);
    ASSERT_TRUE(s.IsRuntimeError());
    ASSERT_STR_CONTAINS(msg,
        "RPC timeout set by --timeout_ms should be not less than connection "
        "negotiation timeout set by --negotiation_timeout_ms; "
        "current settings are 1 and 2 correspondingly");
  }
}

// Execute a ksck giving the hostname of the local machine twice
// and it should report the duplicates found.
TEST_F(ToolTest, TestDuplicateMastersInKsck) {
  string master_addr;
  if (!GetHostname(&master_addr).ok()) {
    master_addr = "<unknown_host>";
  }
  string out;
  string cmd = Substitute("cluster ksck $0,$0", master_addr);
  Status s = RunActionStderrString(cmd, &out);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(out, "Duplicate master addresses specified");
}

TEST_F(ToolTest, TestNonDefaultPrincipal) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  ExternalMiniClusterOptions opts;
  opts.enable_kerberos = true;
  opts.principal = "oryx";
  NO_FATALS(StartExternalMiniCluster(opts));
  ASSERT_OK(RunKuduTool({"cluster",
                         "ksck",
                         "--sasl_protocol_name=oryx",
                         HostPort::ToCommaSeparatedString(cluster_->master_rpc_addrs())}));
}

class UnregisterTServerTest : public ToolTest, public ::testing::WithParamInterface<bool> {
 public:
  void StartCluster() {
    // Test on a multi-master cluster.
    InternalMiniClusterOptions opts;
    opts.num_masters = 3;
    StartMiniCluster(std::move(opts));
  }
};

INSTANTIATE_TEST_SUITE_P(, UnregisterTServerTest, ::testing::Bool());

TEST_P(UnregisterTServerTest, TestUnregisterTServer) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  bool remove_tserver_state = GetParam();

  // Set a short timeout that masters consider a tserver dead.
  FLAGS_tserver_unresponsive_timeout_ms = 3000;
  NO_FATALS(StartCluster());
  const string master_addrs_str = GetMasterAddrsStr();
  MiniTabletServer* ts = mini_cluster_->mini_tablet_server(0);
  const string ts_uuid = ts->uuid();
  const string ts_hostport = ts->bound_rpc_addr().ToString();

  // Enter maintenance mode on the tserver and shut it down.
  ASSERT_OK(RunKuduTool({"tserver", "state", "enter_maintenance", master_addrs_str, ts_uuid}));
  ts->Shutdown();

  {
    string out;
    string err;
    // Getting an error when running ksck and the output contains the dead tserver.
    Status s =
        RunActionStdoutStderrString(Substitute("cluster ksck $0", master_addrs_str), &out, &err);
    ASSERT_TRUE(s.IsRuntimeError());
    ASSERT_STR_CONTAINS(out, Substitute("$0 | $1 | UNAVAILABLE", ts_uuid, ts_hostport));
  }
  // Wait the tserver become dead.
  ASSERT_EVENTUALLY(
      [&] { ASSERT_EQ(0, mini_cluster_->mini_master(0)->master()->ts_manager()->GetLiveCount()); });

  // Unregister the tserver.
  ASSERT_OK(RunKuduTool({"tserver",
                         "unregister",
                         master_addrs_str,
                         ts_uuid,
                         Substitute("-remove_tserver_state=$0", remove_tserver_state)}));
  {
    // Run ksck and get no error.
    string out;
    NO_FATALS(RunActionStdoutString(Substitute("cluster ksck $0", master_addrs_str), &out));
    if (remove_tserver_state) {
      // Both the persisted state and registration of the tserver was removed.
      ASSERT_STR_NOT_CONTAINS(out, Substitute(" $0 | MAINTENANCE_MODE", ts_uuid));
      ASSERT_STR_NOT_CONTAINS(out, ts_uuid);
    } else {
      // Only the registration of the tserver was removed.
      ASSERT_STR_CONTAINS(out, Substitute(" $0 | MAINTENANCE_MODE", ts_uuid));
      ASSERT_STR_NOT_CONTAINS(out, Substitute("$0 | $1 | UNAVAILABLE", ts_uuid, ts_hostport));
    }
  }

  // Restart the tserver and re-register it on masters.
  ts->Start();
  {
    string out;
    ASSERT_EVENTUALLY([&]() {
      NO_FATALS(RunActionStdoutString(Substitute("cluster ksck $0", master_addrs_str), &out));
    });
    if (remove_tserver_state) {
      // The tserver came back as a brand new tserver.
      ASSERT_STR_NOT_CONTAINS(out, Substitute(" $0 | MAINTENANCE_MODE", ts_uuid));
      ASSERT_STR_CONTAINS(out, ts_uuid);
    } else {
      // The tserver got its original maintenance state.
      ASSERT_STR_CONTAINS(out, Substitute(" $0 | MAINTENANCE_MODE", ts_uuid));
      ASSERT_STR_CONTAINS(out, ts_uuid);
    }
  }
}

TEST_F(UnregisterTServerTest, TestUnregisterTServerNotPresumedDead) {
  // Reduce the TS<->Master heartbeat interval to speed up testing.
  FLAGS_heartbeat_interval_ms = 100;
  NO_FATALS(StartCluster());
  const string master_addrs_str = GetMasterAddrsStr();
  MiniTabletServer* ts = mini_cluster_->mini_tablet_server(0);
  const string ts_uuid = ts->uuid();
  const string ts_hostport = ts->bound_rpc_addr().ToString();

  // Shut down the tserver.
  ts->Shutdown();
  // Get an error because the tserver is not presumed dead by masters.
  {
    string out;
    string err;
    Status s = RunActionStdoutStderrString(
        Substitute("tserver unregister $0 $1", master_addrs_str, ts_uuid), &out, &err);
    ASSERT_TRUE(s.IsRuntimeError());
    ASSERT_STR_CONTAINS(err, ts_uuid);
  }
  // The ksck output contains the dead tserver.
  {
    string out;
    string err;
    Status s =
        RunActionStdoutStderrString(Substitute("cluster ksck $0", master_addrs_str), &out, &err);
    ASSERT_TRUE(s.IsRuntimeError());
    ASSERT_STR_CONTAINS(out, Substitute("$0 | $1 | UNAVAILABLE", ts_uuid, ts_hostport));
  }

  // We could force unregister the tserver.
  ASSERT_OK(RunKuduTool(
      {"tserver", "unregister", master_addrs_str, ts_uuid, "-force_unregister_live_tserver"}));
  {
    string out;
    NO_FATALS(RunActionStdoutString(Substitute("cluster ksck $0", master_addrs_str), &out));
    ASSERT_STR_NOT_CONTAINS(out, ts_uuid);
  }

  // After several hearbeat intervals, the tserver still does not appear in ksck output.
  SleepFor(MonoDelta::FromMilliseconds(3 * FLAGS_heartbeat_interval_ms));
  {
    string out;
    NO_FATALS(RunActionStdoutString(Substitute("cluster ksck $0", master_addrs_str), &out));
    ASSERT_STR_NOT_CONTAINS(out, ts_uuid);
  }
}

TEST_F(ToolTest, TestLocalReplicaCopyLocal) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  // TODO(abukor): Rewrite the test to make sure it works with encryption
  // enabled.
  //
  // Right now, this test would fail with encryption enabled, as the local
  // replica copy shares an Env between the source and the destination and they
  // use different instance files. This shouldn't be a problem in real life, as
  // its meant to copy tablets between disks on the same server, which would
  // share UUIDs and server keys.
  if (FLAGS_encrypt_data_at_rest) {
    GTEST_SKIP();
  }
  // Create replicas and fill some data.
  InternalMiniClusterOptions opts;
  opts.num_data_dirs = 3;
  NO_FATALS(StartMiniCluster(std::move(opts)));
  NO_FATALS(CreateTableWithFlushedData("tablename", mini_cluster_.get()));

  string tablet_id;
  string src_fs_paths_with_prefix;
  string src_fs_paths;
  {
    tablet_id = mini_cluster_->mini_tablet_server(0)->ListTablets()[0];

    // Obtain source filesystem options.
    auto fs_manager = mini_cluster_->mini_tablet_server(0)->server()->fs_manager();
    string src_fs_wal_dir = fs_manager->GetWalsRootDir();
    src_fs_wal_dir = src_fs_wal_dir.substr(0, src_fs_wal_dir.length() - strlen("/wals"));
    string src_fs_data_dirs = JoinMapped(fs_manager->GetDataRootDirs(),
        [] (const string& data_dir) {
          return data_dir.substr(0, data_dir.length() - strlen("/data"));
        }, ",");
    src_fs_paths_with_prefix = "--src_fs_wal_dir=" + src_fs_wal_dir + " "
                               "--src_fs_data_dirs=" + src_fs_data_dirs;
    src_fs_paths = "--fs_wal_dir=" + src_fs_wal_dir + " "
                   "--fs_data_dirs=" + src_fs_data_dirs;
  }

  string dst_fs_paths_with_prefix;
  string dst_fs_paths;
  {
    // Build destination filesystem layout, and obtain filesystem options.
    const string kTestDstDir = GetTestPath("dst_test");
    unique_ptr<FsManager> dst_fs_manager =
        std::make_unique<FsManager>(Env::Default(), FsManagerOpts(kTestDstDir));
    ASSERT_OK(dst_fs_manager->CreateInitialFileSystemLayout());
    ASSERT_OK(dst_fs_manager->Open());
    dst_fs_paths_with_prefix = "--dst_fs_wal_dir=" + kTestDstDir + " "
                               "--dst_fs_data_dirs=" + kTestDstDir;
    dst_fs_paths = "--fs_wal_dir=" + kTestDstDir + " "
                   "--fs_data_dirs=" + kTestDstDir;
  }

  string encryption_args = env_->IsEncryptionEnabled() ? GetEncryptionArgs() : "";

  // Copy replica from local filesystem failed, because the tserver on source filesystem
  // is still running.
  string stdout;
  string stderr;
  Status s = RunActionStdoutStderrString(
      Substitute("local_replica copy_from_local $0 $1 $2 $3",
                 tablet_id, src_fs_paths_with_prefix, dst_fs_paths_with_prefix, encryption_args),
      &stdout, &stderr);
  SCOPED_TRACE(stdout);
  SCOPED_TRACE(stderr);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_MATCHES(stderr,
                     "failed to load instance files: Could not lock instance file. "
                     "Make sure that Kudu is not already running");

  // Shutdown mini cluster and copy replica from local filesystem successfully.
  mini_cluster_->Shutdown();
  NO_FATALS(RunActionStdoutString(
      Substitute("local_replica copy_from_local $0 $1 $2 $3",
                 tablet_id, src_fs_paths_with_prefix, dst_fs_paths_with_prefix, encryption_args),
      &stdout));
  SCOPED_TRACE(stdout);

  // Check the source and destination data is matched.
  string src_stdout;
  NO_FATALS(RunActionStdoutString(
      Substitute("local_replica data_size $0 $1 $2",
                 tablet_id, src_fs_paths, encryption_args), &src_stdout));
  SCOPED_TRACE(src_stdout);

  string dst_stdout;
  NO_FATALS(RunActionStdoutString(
      Substitute("local_replica data_size $0 $1 $2",
                 tablet_id, dst_fs_paths, encryption_args), &dst_stdout));
  SCOPED_TRACE(dst_stdout);

  ASSERT_EQ(src_stdout, dst_stdout);
}

TEST_F(ToolTest, TestLocalReplicaCopyRemote) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  InternalMiniClusterOptions opts;
  opts.num_tablet_servers = 2;
  NO_FATALS(StartMiniCluster(std::move(opts)));
  NO_FATALS(CreateTableWithFlushedData("table1", mini_cluster_.get(), 3, 1));
  NO_FATALS(CreateTableWithFlushedData("table2", mini_cluster_.get(), 3, 1));
  int source_tserver_tablet_count = mini_cluster_->mini_tablet_server(0)->ListTablets().size();
  int target_tserver_tablet_count_before = mini_cluster_->mini_tablet_server(1)
                                                        ->ListTablets().size();
  string tablet_ids_str = JoinStrings(mini_cluster_->mini_tablet_server(0)->ListTablets(), ",");
  string source_tserver_rpc_addr = mini_cluster_->mini_tablet_server(0)
                                                ->bound_rpc_addr().ToString();
  string wal_dir = mini_cluster_->mini_tablet_server(1)->options()->fs_opts.wal_root;
  string data_dirs = JoinStrings(mini_cluster_->mini_tablet_server(1)
                                              ->options()->fs_opts.data_roots, ",");
  NO_FATALS(mini_cluster_->mini_tablet_server(1)->Shutdown());
  // Copy tablet replicas from tserver0 to tserver1.
  NO_FATALS(RunActionStdoutNone(
      Substitute("local_replica copy_from_remote $0 $1 "
                 "-fs_data_dirs=$2 -fs_wal_dir=$3 -num_threads=3",
                 tablet_ids_str,
                 source_tserver_rpc_addr,
                 data_dirs,
                 wal_dir)));
  NO_FATALS(mini_cluster_->mini_tablet_server(1)->Start());
  const vector<string>& target_tablet_ids = mini_cluster_->mini_tablet_server(1)->ListTablets();
  ASSERT_EQ(source_tserver_tablet_count + target_tserver_tablet_count_before,
            target_tablet_ids.size());
  for (string tablet_id : mini_cluster_->mini_tablet_server(0)->ListTablets()) {
    ASSERT_TRUE(find(target_tablet_ids.begin(), target_tablet_ids.end(), tablet_id)
                != target_tablet_ids.end());
  }
}

TEST_F(ToolTest, TestRebuildTserverByLocalReplicaCopy) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  // Local copies are not supported on encrypted severs at this time.
  if (FLAGS_encrypt_data_at_rest) {
    GTEST_SKIP();
  }
  // Create replicas and fill some data.
  const int kNumTserver = 3;
  InternalMiniClusterOptions opts;
  opts.num_tablet_servers = kNumTserver;
  // The source filesystem has only 1 data directory.
  opts.num_data_dirs = 1;
  NO_FATALS(StartMiniCluster(std::move(opts)));
  NO_FATALS(CreateTableWithFlushedData("tablename", mini_cluster_.get(), 8, 3));

  // Obtain source cluster options.
  const int kCopyTserverIndex = 2;
  vector<string> tablet_ids = mini_cluster_->mini_tablet_server(kCopyTserverIndex)->ListTablets();
  vector<HostPort> hps;
  hps.reserve(mini_cluster_->num_tablet_servers());
  for (int i = 0; i < mini_cluster_->num_tablet_servers(); ++i) {
    hps.emplace_back(HostPort(mini_cluster_->mini_tablet_server(i)->bound_rpc_addr()));
  }
  auto fs_manager = mini_cluster_->mini_tablet_server(kCopyTserverIndex)->server()->fs_manager();
  string src_fs_wal_dir = fs_manager->GetWalsRootDir();
  src_fs_wal_dir = src_fs_wal_dir.substr(0, src_fs_wal_dir.length() - strlen("/wals"));
  string src_fs_data_dirs = JoinMapped(fs_manager->GetDataRootDirs(),
      [] (const string& data_dir) {
        return data_dir.substr(0, data_dir.length() - strlen("/data"));
      }, ",");
  string src_fs_paths_with_prefix = "--src_fs_wal_dir=" + src_fs_wal_dir + " "
                                    "--src_fs_data_dirs=" + src_fs_data_dirs;
  string src_fs_paths = "--fs_wal_dir=" + src_fs_wal_dir + " "
                        "--fs_data_dirs=" + src_fs_data_dirs;

  // Build destination filesystem layout with source fs uuid, and obtain filesystem options.
  string dst_fs_paths_with_prefix;
  string src_tserver_fs_root = mini_cluster_->GetTabletServerFsRoot(kCopyTserverIndex);
  string dst_tserver_fs_root = mini_cluster_->GetTabletServerFsRoot(kNumTserver);
  {
    FsManagerOpts fs_opts;
    // The new fs layout has 3 data directories.
    MiniTabletServer::InitFsOpts(3, dst_tserver_fs_root, &fs_opts);
    string dst_fs_wal_dir = fs_opts.wal_root;
    string dst_fs_data_dirs = JoinMapped(fs_opts.data_roots,
        [] (const string& data_root) {
          return data_root;
        }, ",");
    string dst_fs_paths = "--fs_wal_dir=" + dst_fs_wal_dir + " "
                          "--fs_data_dirs=" + dst_fs_data_dirs;
    ASSERT_OK(Env::Default()->CreateDir(dst_tserver_fs_root));
    // Use 'kudu fs format' with '--uuid' to format the new data directories.
    NO_FATALS(RunActionStdoutNone(Substitute("fs format $0 --uuid=$1",
                                             dst_fs_paths, fs_manager->uuid())));
    dst_fs_paths_with_prefix = "--dst_fs_wal_dir=" + dst_fs_wal_dir + " "
                               "--dst_fs_data_dirs=" + dst_fs_data_dirs;
  }

  // Shutdown the cluster before copying.
  NO_FATALS(StopMiniCluster());

  // Copy source tserver's all replicas from local filesystem.
  string stdout;
  NO_FATALS(RunActionStdoutString(Substitute("local_replica copy_from_local $0 $1 $2",
                                             JoinStrings(tablet_ids, ","),
                                             src_fs_paths_with_prefix,
                                             dst_fs_paths_with_prefix),
                                  &stdout));
  SCOPED_TRACE(stdout);

  // Replace the old data/wal dirs with the new ones.
  ASSERT_OK(Env::Default()->RenameFile(src_tserver_fs_root, src_tserver_fs_root + ".bak"));
  ASSERT_OK(Env::Default()->RenameFile(dst_tserver_fs_root, src_tserver_fs_root));

  // Start masters only.
  mini_cluster_->opts_.num_tablet_servers = 0;
  NO_FATALS(mini_cluster_->Start());
  // Then start tserver with old host:port.
  ASSERT_OK(mini_cluster_->AddTabletServer(hps[0]));
  ASSERT_OK(mini_cluster_->AddTabletServer(hps[1]));
  // The new tserver has 3 data directories.
  mini_cluster_->opts_.num_data_dirs = 3;
  ASSERT_OK(mini_cluster_->AddTabletServer(hps[2]));
  // Wait all tserver start normally.
  ASSERT_OK(mini_cluster_->WaitForTabletServerCount(3));
  for (int i = 0; i < mini_cluster_->num_tablet_servers(); ++i) {
    ASSERT_OK(mini_cluster_->mini_tablet_server(i)->WaitStarted());
  }

  // Restart the cluster, and check the cluster state is OK.
  ASSERT_EVENTUALLY([&] {
    string out;
    string err;
    Status s =
        RunActionStdoutStderrString(Substitute("cluster ksck $0", GetMasterAddrsStr()), &out, &err);
    ASSERT_TRUE(s.ok()) << s.ToString() << ", err:\n" << err << ", out:\n" << out;
  });
}

class SetFlagForAllTest :
    public ToolTest,
    public ::testing::WithParamInterface<bool> {
};

INSTANTIATE_TEST_SUITE_P(IsMaster, SetFlagForAllTest, ::testing::Bool());
TEST_P(SetFlagForAllTest, TestSetFlagForAll) {
  const auto is_master = GetParam();

  ExternalMiniClusterOptions opts;
  string role = "master";
  if (is_master) {
    opts.num_masters = 3;
    role = "master";
  } else {
    opts.num_tablet_servers = 3;
    role = "tserver";
  }

  NO_FATALS(StartExternalMiniCluster(std::move(opts)));
  vector<string> master_addresses;
  for (int i = 0; i < opts.num_masters; i++) {
    master_addresses.emplace_back(cluster_->master(i)->bound_rpc_addr().ToString());
  }
  string str_master_addresses = JoinMapped(master_addresses, [](string addr){return addr;}, ",");

  // <flag key, flag value, optional parameter>
  vector<tuple<string , string , string>> command_parameters;
  command_parameters.emplace_back(tuple<string, string, string>("max_log_size", "10" , ""));
  command_parameters.emplace_back(tuple<string, string, string>("log_async", "false" , "--force"));

  for (auto command_parameter : command_parameters) {
    const string flag_key = std::get<0>(command_parameter);
    const string flag_value = std::get<1>(command_parameter);
    const string optional_parameter = std::get<2>(command_parameter);
    NO_FATALS(RunActionStdoutNone(
        Substitute("$0 set_flag_for_all $1 $2 $3 $4",
            role, str_master_addresses, flag_key, flag_value, optional_parameter)));

    int hosts_num = is_master? opts.num_masters : opts.num_tablet_servers;
    string out;
    for (int i = 0; i < hosts_num; i++) {
      if (is_master) {
        NO_FATALS(RunActionStdoutString(
            Substitute("$0 get_flags $1 --format=json", role,
                cluster_->master(i)->bound_rpc_addr().ToString()),
                &out));
      } else {
        NO_FATALS(RunActionStdoutString(
            Substitute("$0 get_flags $1 --format=json", role,
            cluster_->tablet_server(i)->bound_rpc_addr().ToString()),
            &out));
      }
      rapidjson::Document doc;
      doc.Parse<0>(out.c_str());
      for (int i = 0; i < doc.Size(); i++) {
        const rapidjson::Value& item = doc[i];
        ASSERT_TRUE(item["flag"].IsString());
        if (item["flag"].GetString() == flag_key) {
          ASSERT_TRUE(item["value"].IsString());
          ASSERT_TRUE(item["value"].GetString() == flag_value);
          break;
        }
      }
    }
  }

  // A test for setting a non-existing flag.
  string stdout;
  string stderr;
  Status s = RunTool(
      Substitute("$0 set_flag_for_all $1 test_flag test_value",
          role, str_master_addresses),
      &stdout, &stderr, nullptr, nullptr);
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_CONTAINS(stderr, "result: NO_SUCH_FLAG");
}

} // namespace tools
} // namespace kudu
