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
#include <iterator>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <tuple>  // IWYU pragma: keep
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "kudu/cfile/cfile-test-base.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/fs_report.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tools/tool.pb.h"
#include "kudu/tools/tool_action_common.h"
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
#include "kudu/util/int128_util.h" // IWYU pragma: keep
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/url-coding.h"

DECLARE_string(block_manager);

namespace kudu {

namespace tserver {
class TabletServerServiceProxy;
}

namespace tools {

using cfile::CFileWriter;
using cfile::StringDataGenerator;
using cfile::WriterOptions;
using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduSchema;
using client::KuduSchemaBuilder;
using client::KuduTable;
using client::sp::shared_ptr;
using cluster::ExternalMiniCluster;
using cluster::ExternalMiniClusterOptions;
using cluster::InternalMiniCluster;
using cluster::InternalMiniClusterOptions;
using consensus::OpId;
using consensus::RECEIVED_OPID;
using consensus::ReplicateRefPtr;
using consensus::ReplicateMsg;
using fs::BlockDeletionTransaction;
using fs::FsReport;
using fs::WritableBlock;
using hms::HmsClient;
using hms::HmsClientOptions;
using itest::MiniClusterFsInspector;
using itest::TServerDetails;
using log::Log;
using log::LogOptions;
using rpc::RpcController;
using std::back_inserter;
using std::copy;
using std::make_pair;
using std::ostringstream;
using std::pair;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;
using tablet::LocalTabletWriter;
using tablet::Tablet;
using tablet::TabletDataState;
using tablet::TabletHarness;
using tablet::TabletMetadata;
using tablet::TabletReplica;
using tablet::TabletSuperBlockPB;
using tserver::DeleteTabletRequestPB;
using tserver::DeleteTabletResponsePB;
using tserver::MiniTabletServer;
using tserver::WriteRequestPB;
using tserver::ListTabletsRequestPB;
using tserver::ListTabletsResponsePB;
using tserver::TabletServerServiceProxy;

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
    ASSERT_OK(s);
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

    // If it was an invalid command, the usage string is on the third line.
    int usage_idx = 1;
    if (!expected_status.ok()) {
      ASSERT_EQ(expected_status.ToString(), stderr[1]);
      usage_idx = 2;
    }
    ASSERT_EQ(0, stderr[usage_idx].find("Usage: "));

    // Strip away everything up to the usage string to test for regexes.
    const vector<string> remaining_lines(stderr.begin() + usage_idx + 1,
                                         stderr.end());
    for (const auto& r : regexes) {
      ASSERT_STRINGS_ANY_MATCH(remaining_lines, r);
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
    ASSERT_EQ(expected_status.ToString(), err_lines[1]);
    ASSERT_STR_MATCHES(err_lines[3], "Usage: kudu.*");
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

 protected:
  void RunLoadgen(int num_tservers = 1,
                  const vector<string>& tool_args = {},
                  const string& table_name = "");
  void StartExternalMiniCluster(ExternalMiniClusterOptions opts = {});
  void StartMiniCluster(InternalMiniClusterOptions opts = {});
  unique_ptr<ExternalMiniCluster> cluster_;
  unique_ptr<MiniClusterFsInspector> inspect_;
  unordered_map<string, TServerDetails*> ts_map_;
  unique_ptr<InternalMiniCluster> mini_cluster_;
};

void ToolTest::StartExternalMiniCluster(ExternalMiniClusterOptions opts) {
  cluster_.reset(new ExternalMiniCluster(std::move(opts)));
  ASSERT_OK(cluster_->Start());
  inspect_.reset(new MiniClusterFsInspector(cluster_.get()));
  ASSERT_OK(CreateTabletServerMap(cluster_->master_proxy(),
                                  cluster_->messenger(), &ts_map_));
}

void ToolTest::StartMiniCluster(InternalMiniClusterOptions opts) {
  mini_cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
  ASSERT_OK(mini_cluster_->Start());
}

TEST_F(ToolTest, TestHelpXML) {
  string stdout;
  string stderr;
  Status s = RunTool("--helpxml", &stdout, &stderr, nullptr, nullptr);

  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_FALSE(stdout.empty());
  ASSERT_TRUE(stderr.empty());

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
        "uuid.*UUID of a Kudu filesystem"
    };
    NO_FATALS(RunTestHelp("fs dump", kFsDumpModeRegexes));

  }
  {
    const vector<string> kLocalReplicaModeRegexes = {
        "cmeta.*Operate on a local tablet replica's consensus",
        "data_size.*Summarize the data size",
        "dump.*Dump a Kudu filesystem",
        "copy_from_remote.*Copy a tablet replica",
        "delete.*Delete a tablet replica from the local filesystem",
        "list.*Show list of tablet replicas"
    };
    NO_FATALS(RunTestHelp("local_replica", kLocalReplicaModeRegexes));
  }
  {
    const vector<string> kLocalReplicaDumpModeRegexes = {
        "block_ids.*Dump the IDs of all blocks",
        "meta.*Dump the metadata",
        "rowset.*Dump the rowset contents",
        "wals.*Dump all WAL"
    };
    NO_FATALS(RunTestHelp("local_replica dump", kLocalReplicaDumpModeRegexes));
  }
  {
    const vector<string> kLocalReplicaCMetaRegexes = {
        "print_replica_uuids.*Print all tablet replica peer UUIDs",
        "rewrite_raft_config.*Rewrite a tablet replica",
        "set_term.*Bump the current term"
    };
    NO_FATALS(RunTestHelp("local_replica cmeta", kLocalReplicaCMetaRegexes));
    // Try with a hyphen instead of an underscore.
    NO_FATALS(RunTestHelp("local-replica cmeta", kLocalReplicaCMetaRegexes));
  }
  {
    const vector<string> kLocalReplicaCopyFromRemoteRegexes = {
        "Copy a tablet replica from a remote server"
    };
    NO_FATALS(RunTestHelp("local_replica copy_from_remote --help",
                          kLocalReplicaCopyFromRemoteRegexes));
    // Try with hyphens instead of underscores.
    NO_FATALS(RunTestHelp("local-replica copy-from-remote --help",
                          kLocalReplicaCopyFromRemoteRegexes));
  }
  {
    const vector<string> kClusterModeRegexes = {
        "ksck.*Check the health of a Kudu cluster",
    };
    NO_FATALS(RunTestHelp("cluster", kClusterModeRegexes));
  }
  {
    const vector<string> kDiagnoseModeRegexes = {
        "parse_stacks.*Parse sampled stack traces",
    };
    NO_FATALS(RunTestHelp("diagnose", kDiagnoseModeRegexes));
  }
  {
    const vector<string> kMasterModeRegexes = {
        "get_flags.*Get the gflags",
        "set_flag.*Change a gflag value",
        "status.*Get the status",
        "timestamp.*Get the current timestamp"
    };
    NO_FATALS(RunTestHelp("master", kMasterModeRegexes));
  }
  {
    const vector<string> kPbcModeRegexes = {
        "dump.*Dump a PBC",
    };
    NO_FATALS(RunTestHelp("pbc", kPbcModeRegexes));
  }
  {
    const vector<string> kPerfRegexes = {
        "loadgen.*Run load generation with optional scan afterwards",
    };
    NO_FATALS(RunTestHelp("perf", kPerfRegexes));
  }
  {
    const vector<string> kRemoteReplicaModeRegexes = {
        "check.*Check if all tablet replicas",
        "copy.*Copy a tablet replica from one Kudu Tablet Server",
        "delete.*Delete a tablet replica",
        "dump.*Dump the data of a tablet replica",
        "list.*List all tablet replicas",
        "unsafe_change_config.*Force the specified replica to adopt"
    };
    NO_FATALS(RunTestHelp("remote_replica", kRemoteReplicaModeRegexes));
  }
  {
    const vector<string> kTableModeRegexes = {
        "delete.*Delete a table",
        "list.*List all tables",
    };
    NO_FATALS(RunTestHelp("table", kTableModeRegexes));
  }
  {
    const vector<string> kTabletModeRegexes = {
        "change_config.*Change.*Raft configuration",
        "leader_step_down.*Force the tablet's leader replica to step down"
    };
    NO_FATALS(RunTestHelp("tablet", kTabletModeRegexes));
  }
  {
    const vector<string> kTestModeRegexes = {
        "mini_cluster.*Spawn a control shell"
    };
    NO_FATALS(RunTestHelp("test", kTestModeRegexes));
  }
  {
    const vector<string> kChangeConfigModeRegexes = {
        "add_replica.*Add a new replica",
        "change_replica_type.*Change the type of an existing replica",
        "move_replica.*Move a tablet replica",
        "remove_replica.*Remove an existing replica"
    };
    NO_FATALS(RunTestHelp("tablet change_config", kChangeConfigModeRegexes));
  }
  {
    const vector<string> kTServerModeRegexes = {
        "get_flags.*Get the gflags",
        "set_flag.*Change a gflag value",
        "status.*Get the status",
        "timestamp.*Get the current timestamp",
        "list.*List tablet servers"
    };
    NO_FATALS(RunTestHelp("tserver", kTServerModeRegexes));
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
  NO_FATALS(RunActionMissingRequiredArg("cluster ksck --master_addresses=master.example.com",
                                        "master_addresses"));
  NO_FATALS(RunActionMissingRequiredArg("local_replica cmeta rewrite_raft_config fake_id",
                                        "peers", /* variadic */ true));
}

TEST_F(ToolTest, TestFsCheck) {
  const string kTestDir = GetTestPath("test");
  const string kTabletId = "test-tablet";
  const Schema kSchema(GetSimpleTestSchema());
  const Schema kSchemaWithIds(SchemaBuilder(kSchema).Build());

  // Create a local replica, flush some rows a few times, and collect all
  // of the created block IDs.
  vector<BlockId> block_ids;
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

  // Check the filesystem; all the blocks should be accounted for, and there
  // should be no blocks missing or orphaned.
  NO_FATALS(RunFsCheck(Substitute("fs check --fs_wal_dir=$0", kTestDir),
                       block_ids.size(), kTabletId, {}, 0));

  // Delete half of the blocks. Upon the next check we can only find half, and
  // the other half are deemed missing.
  vector<BlockId> missing_ids;
  {
    FsManager fs(env_, kTestDir);
    FsReport report;
    ASSERT_OK(fs.Open(&report));
    std::shared_ptr<BlockDeletionTransaction> deletion_transaction =
        fs.block_manager()->NewDeletionTransaction();
    for (int i = 0; i < block_ids.size(); i += 2) {
      deletion_transaction->AddDeletedBlock(block_ids[i]);
    }
    deletion_transaction->CommitDeletedBlocks(&missing_ids);
  }
  NO_FATALS(RunFsCheck(Substitute("fs check --fs_wal_dir=$0", kTestDir),
                       block_ids.size() / 2, kTabletId, missing_ids, 0));

  // Delete the tablet superblock. The next check finds half of the blocks,
  // though without the superblock they're all considered to be orphaned.
  //
  // Here we check twice to show that if --repair isn't provided, there should
  // be no effect.
  {
    FsManager fs(env_, kTestDir);
    FsReport report;
    ASSERT_OK(fs.Open(&report));
    ASSERT_OK(env_->DeleteFile(fs.GetTabletMetadataPath(kTabletId)));
  }
  for (int i = 0; i < 2; i++) {
    NO_FATALS(RunFsCheck(Substitute("fs check --fs_wal_dir=$0", kTestDir),
                         block_ids.size() / 2, kTabletId, {}, block_ids.size() / 2));
  }

  // Repair the filesystem. The remaining half of all blocks were found, deemed
  // to be orphaned, and deleted. The next check shows no remaining blocks.
  NO_FATALS(RunFsCheck(Substitute("fs check --fs_wal_dir=$0 --repair", kTestDir),
                       block_ids.size() / 2, kTabletId, {}, block_ids.size() / 2));
  NO_FATALS(RunFsCheck(Substitute("fs check --fs_wal_dir=$0", kTestDir),
                       0, kTabletId, {}, 0));
}

TEST_F(ToolTest, TestFsCheckLiveServer) {
  NO_FATALS(StartExternalMiniCluster());
  string args = Substitute("fs check --fs_wal_dir $0 --fs_data_dirs $1",
                           cluster_->GetWalPath("master-0"),
                           JoinStrings(cluster_->GetDataPaths("master-0"), ","));
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
  FsManager fs(env_, kTestDir);
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
  FsManager fs(env_, kTestDir);
  ASSERT_OK(fs.Open());

  string canonicalized_uuid;
  ASSERT_OK(generator.Canonicalize(fs.uuid(), &canonicalized_uuid));
  ASSERT_EQ(fs.uuid(), canonicalized_uuid);
  ASSERT_EQ(fs.uuid(), original_uuid);
}

TEST_F(ToolTest, TestFsDumpUuid) {
  const string kTestDir = GetTestPath("test");
  string uuid;
  {
    FsManager fs(env_, kTestDir);
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
  const string kTestDir = GetTestPath("test");
  string uuid;
  string instance_path;
  {
    ObjectIdGenerator generator;
    FsManager fs(env_, kTestDir);
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
  // Test dump --oneline
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(Substitute(
        "pbc dump $0/instance --oneline", kTestDir), &stdout));
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
        "pbc dump $0/instance --json", kTestDir), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_STR_MATCHES(stdout, Substitute(
        "^\\{\"uuid\":\"$0\",\"formatStamp\":\"Formatted at .*\"\\}$$", uuid_b64));
  }

  // Utility to set the editor up based on the given shell command.
  auto DoEdit = [&](const string& editor_shell, string* stdout, string* stderr = nullptr) {
    const string editor_path = GetTestPath("editor");
    CHECK_OK(WriteStringToFile(Env::Default(),
                               StrCat("#!/usr/bin/env bash\n", editor_shell),
                               editor_path));
    chmod(editor_path.c_str(), 0755);
    setenv("EDITOR", editor_path.c_str(), /* overwrite */1);
    return RunTool(Substitute("pbc edit $0/instance", kTestDir),
                   stdout, stderr, nullptr, nullptr);
  };

  // Test 'edit' by setting up EDITOR to be a sed script which performs a substitution.
  {
    string stdout;
    ASSERT_OK(DoEdit("exec sed -i -e s/Formatted/Edited/ \"$@\"\n", &stdout));
    ASSERT_EQ("", stdout);

    // Dump to make sure the edit took place.
    NO_FATALS(RunActionStdoutString(Substitute(
        "pbc dump $0/instance --oneline", kTestDir), &stdout));
    ASSERT_STR_MATCHES(stdout, Substitute(
        "^0\tuuid: \"$0\" format_stamp: \"Edited at .*\"$$", uuid));
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
                       "Invalid argument: Unable to parse JSON line: \\{\\}: "
                       ": missing field .*");
    // NOTE: the above extra ':' is due to an apparent bug in protobuf.
  }

  // Test 'edit' with an edit that writes some invalid JSON (bad syntax)
  {
    string stdout, stderr;
    Status s = DoEdit("echo not-a-json-string > $@\n", &stdout, &stderr);
    ASSERT_EQ("", stdout);
    ASSERT_STR_CONTAINS(
        stderr,
        "Invalid argument: Unable to parse JSON line: not-a-json-string: Unexpected token.\n"
        "not-a-json-string\n"
        "^");
  }

  // The file should be unchanged by the unsuccessful edits above.
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(Substitute(
        "pbc dump $0/instance --oneline", kTestDir), &stdout));
    ASSERT_STR_MATCHES(stdout, Substitute(
        "^0\tuuid: \"$0\" format_stamp: \"Edited at .*\"$$", uuid));
  }
}

TEST_F(ToolTest, TestFsDumpCFile) {
  const int kNumEntries = 8192;
  const string kTestDir = GetTestPath("test");
  FsManager fs(env_, kTestDir);
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

  {
    NO_FATALS(RunActionStdoutNone(Substitute(
        "fs dump cfile --fs_wal_dir=$0 $1 --noprint_meta --noprint_rows",
        kTestDir, block_id.ToString())));
  }
  vector<string> stdout;
  {
    NO_FATALS(RunActionStdoutLines(Substitute(
        "fs dump cfile --fs_wal_dir=$0 $1 --noprint_rows",
        kTestDir, block_id.ToString()), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_GE(stdout.size(), 4);
    ASSERT_EQ(stdout[0], "Header:");
    ASSERT_EQ(stdout[2], "Footer:");
  }
  {
    NO_FATALS(RunActionStdoutLines(Substitute(
        "fs dump cfile --fs_wal_dir=$0 $1 --noprint_meta",
        kTestDir, block_id.ToString()), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_EQ(kNumEntries, stdout.size());
  }
  {
    NO_FATALS(RunActionStdoutLines(Substitute(
        "fs dump cfile --fs_wal_dir=$0 $1",
        kTestDir, block_id.ToString()), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_GT(stdout.size(), kNumEntries);
    ASSERT_EQ(stdout[0], "Header:");
    ASSERT_EQ(stdout[2], "Footer:");
  }
}

TEST_F(ToolTest, TestFsDumpBlock) {
  const string kTestDir = GetTestPath("test");
  FsManager fs(env_, kTestDir);
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
        "fs dump block --fs_wal_dir=$0 $1",
        kTestDir, block_id.ToString()), &stdout));
    ASSERT_EQ("hello world", stdout);
  }
}

TEST_F(ToolTest, TestWalDump) {
  const string kTestDir = GetTestPath("test");
  const string kTestTablet = "test-tablet";
  const Schema kSchema(GetSimpleTestSchema());
  const Schema kSchemaWithIds(SchemaBuilder(kSchema).Build());

  FsManager fs(env_, kTestDir);
  ASSERT_OK(fs.CreateInitialFileSystemLayout());
  ASSERT_OK(fs.Open());

  {
    scoped_refptr<Log> log;
    ASSERT_OK(Log::Open(LogOptions(),
                        &fs,
                        kTestTablet,
                        kSchemaWithIds,
                        0, // schema_version
                        scoped_refptr<MetricEntity>(),
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
  string stdout;
  for (const auto& args : { Substitute("wal dump $0", wal_path),
                            Substitute("local_replica dump wals --fs_wal_dir=$0 $1",
                                       kTestDir, kTestTablet)
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

TEST_F(ToolTest, TestLocalReplicaDumpMeta) {
  const string kTestDir = GetTestPath("test");
  const string kTestTablet = "test-tablet";
  const string kTestTableId = "test-table";
  const string kTestTableName = "test-fs-meta-dump-table";
  const Schema kSchema(GetSimpleTestSchema());
  const Schema kSchemaWithIds(SchemaBuilder(kSchema).Build());

  FsManager fs(env_, kTestDir);
  ASSERT_OK(fs.CreateInitialFileSystemLayout());
  ASSERT_OK(fs.Open());

  pair<PartitionSchema, Partition> partition = tablet::CreateDefaultPartition(
        kSchemaWithIds);
  scoped_refptr<TabletMetadata> meta;
  TabletMetadata::CreateNew(&fs, kTestTablet, kTestTableName, kTestTableId,
                  kSchemaWithIds, partition.first, partition.second,
                  tablet::TABLET_DATA_READY,
                  /*tombstone_last_logged_opid=*/ boost::none,
                  &meta);
  string stdout;
  NO_FATALS(RunActionStdoutString(Substitute("local_replica dump meta $0 "
                                             "--fs_wal_dir=$1 "
                                             "--fs_data_dirs=$2",
                                             kTestTablet, kTestDir,
                                             kTestDir), &stdout));

  // Verify the contents of the metadata output
  SCOPED_TRACE(stdout);
  string debug_str = meta->partition_schema()
      .PartitionDebugString(meta->partition(), meta->schema());
  StripWhiteSpace(&debug_str);
  ASSERT_STR_CONTAINS(stdout, debug_str);
  debug_str = Substitute("Table name: $0 Table id: $1",
                         meta->table_name(), meta->table_id());
  ASSERT_STR_CONTAINS(stdout, debug_str);
  debug_str = Substitute("Schema (version=$0):", meta->schema_version());
  ASSERT_STR_CONTAINS(stdout, debug_str);
  debug_str = meta->schema().ToString();
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

  FsManager fs(env_, kTestDir);
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
  const string kTestTablet = "test-tablet";
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
  for (int i = 0; i< 10; i++) {
    ASSERT_OK(row.SetInt32(0, i));
    ASSERT_OK(row.SetInt32(1, i*10));
    ASSERT_OK(row.SetStringCopy(2, "HelloWorld"));
    writer.Insert(row);
  }
  harness.tablet()->Flush();
  harness.tablet()->Shutdown();
  string fs_paths = "--fs_wal_dir=" + kTestDir + " "
      "--fs_data_dirs=" + kTestDir;
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(
        Substitute("local_replica dump block_ids $0 $1",
                   kTestTablet, fs_paths), &stdout));

    SCOPED_TRACE(stdout);
    string tablet_out = "Listing all data blocks in tablet " + kTestTablet;
    ASSERT_STR_CONTAINS(stdout, tablet_out);
    ASSERT_STR_CONTAINS(stdout, "Rowset ");
    ASSERT_STR_MATCHES(stdout, "Column block for column ID .*");
    ASSERT_STR_CONTAINS(stdout, "key[int32 NOT NULL]");
    ASSERT_STR_CONTAINS(stdout, "int_val[int32 NOT NULL]");
    ASSERT_STR_CONTAINS(stdout, "string_val[string NULLABLE]");
  }
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(
        Substitute("local_replica dump rowset $0 $1",
                   kTestTablet, fs_paths), &stdout));

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

    // This is expected to fail with Invalid argument for kRowId.
    string stderr;
    Status s = RunTool(
        Substitute("local_replica dump rowset $0 $1 --rowset_index=$2",
                   kTestTablet, fs_paths, kRowId),
                   &stdout, &stderr, nullptr, nullptr);
    ASSERT_TRUE(s.IsRuntimeError());
    SCOPED_TRACE(stderr);
    string expected = "Could not find rowset " + SimpleItoa(kRowId) +
        " in tablet id " + kTestTablet;
    ASSERT_STR_CONTAINS(stderr, expected);
  }
  {
    TabletMetadata* meta = harness.tablet()->metadata();
    string stdout;
    string debug_str;
    NO_FATALS(RunActionStdoutString(
        Substitute("local_replica dump meta $0 $1",
                   kTestTablet, fs_paths), &stdout));

    SCOPED_TRACE(stdout);
    debug_str = meta->partition_schema()
        .PartitionDebugString(meta->partition(), meta->schema());
    StripWhiteSpace(&debug_str);
    ASSERT_STR_CONTAINS(stdout, debug_str);
    debug_str = Substitute("Table name: $0 Table id: $1",
                           meta->table_name(), meta->table_id());
    ASSERT_STR_CONTAINS(stdout, debug_str);
    debug_str = Substitute("Schema (version=$0):", meta->schema_version());
    StripWhiteSpace(&debug_str);
    ASSERT_STR_CONTAINS(stdout, debug_str);
    debug_str = meta->schema().ToString();
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
        Substitute("local_replica data_size $0 $1",
                   kTestTablet, fs_paths), &stdout));
    SCOPED_TRACE(stdout);

    string expected = R"(
    table id     |  tablet id  | rowset id |    block type    | size
-----------------+-------------+-----------+------------------+------
 KuduTableTestId | test-tablet | 0         | c10 (key)        | 164B
 KuduTableTestId | test-tablet | 0         | c11 (int_val)    | 113B
 KuduTableTestId | test-tablet | 0         | c12 (string_val) | 138B
 KuduTableTestId | test-tablet | 0         | REDO             | 0B
 KuduTableTestId | test-tablet | 0         | UNDO             | 169B
 KuduTableTestId | test-tablet | 0         | BLOOM            | 4.1K
 KuduTableTestId | test-tablet | 0         | PK               | 0B
 KuduTableTestId | test-tablet | 0         | *                | 4.6K
 KuduTableTestId | test-tablet | *         | c10 (key)        | 164B
 KuduTableTestId | test-tablet | *         | c11 (int_val)    | 113B
 KuduTableTestId | test-tablet | *         | c12 (string_val) | 138B
 KuduTableTestId | test-tablet | *         | REDO             | 0B
 KuduTableTestId | test-tablet | *         | UNDO             | 169B
 KuduTableTestId | test-tablet | *         | BLOOM            | 4.1K
 KuduTableTestId | test-tablet | *         | PK               | 0B
 KuduTableTestId | test-tablet | *         | *                | 4.6K
 KuduTableTestId | *           | *         | c10 (key)        | 164B
 KuduTableTestId | *           | *         | c11 (int_val)    | 113B
 KuduTableTestId | *           | *         | c12 (string_val) | 138B
 KuduTableTestId | *           | *         | REDO             | 0B
 KuduTableTestId | *           | *         | UNDO             | 169B
 KuduTableTestId | *           | *         | BLOOM            | 4.1K
 KuduTableTestId | *           | *         | PK               | 0B
 KuduTableTestId | *           | *         | *                | 4.6K
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
    NO_FATALS(RunActionStdoutString(Substitute("local_replica list $0",
                                               fs_paths), &stdout));

    SCOPED_TRACE(stdout);
    ASSERT_STR_MATCHES(stdout, kTestTablet);
  }

  // Test 'kudu fs list' tablet group.
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(
          Substitute("fs list $0 --columns=table,tablet-id --format=csv",
                     fs_paths),
          &stdout));

    SCOPED_TRACE(stdout);
    EXPECT_EQ(stdout, "KuduTableTest,test-tablet");
  }

  // Test 'kudu fs list' rowset group.
  {
    string stdout;
    NO_FATALS(RunActionStdoutString(
          Substitute("fs list $0 --columns=table,tablet-id,rowset-id --format=csv",
                     fs_paths),
          &stdout));

    SCOPED_TRACE(stdout);
    EXPECT_EQ(stdout, "KuduTableTest,test-tablet,0");
  }
  // Test 'kudu fs list' block group.
  {
    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(
          Substitute("fs list $0 "
                     "--columns=table,tablet-id,rowset-id,block-kind,column "
                     "--format=csv",
                     fs_paths),
          &stdout));

    SCOPED_TRACE(stdout);
    ASSERT_EQ(5, stdout.size());
    EXPECT_EQ(stdout[0], Substitute("KuduTableTest,$0,0,column,key", kTestTablet));
    EXPECT_EQ(stdout[1], Substitute("KuduTableTest,$0,0,column,int_val", kTestTablet));
    EXPECT_EQ(stdout[2], Substitute("KuduTableTest,$0,0,column,string_val", kTestTablet));
    EXPECT_EQ(stdout[3], Substitute("KuduTableTest,$0,0,undo,", kTestTablet));
    EXPECT_EQ(stdout[4], Substitute("KuduTableTest,$0,0,bloom,", kTestTablet));
  }

  // Test 'kudu fs list' cfile group.
  {
    vector<string> stdout;
    NO_FATALS(RunActionStdoutLines(
          Substitute("fs list $0 "
                     "--columns=table,tablet-id,rowset-id,block-kind,"
                               "column,cfile-encoding,cfile-num-values "
                     "--format=csv",
                     fs_paths),
          &stdout));

    SCOPED_TRACE(stdout);
    ASSERT_EQ(5, stdout.size());
    EXPECT_EQ(stdout[0],
              Substitute("KuduTableTest,$0,0,column,key,BIT_SHUFFLE,10", kTestTablet));
    EXPECT_EQ(stdout[1],
              Substitute("KuduTableTest,$0,0,column,int_val,BIT_SHUFFLE,10", kTestTablet));
    EXPECT_EQ(stdout[2],
              Substitute("KuduTableTest,$0,0,column,string_val,DICT_ENCODING,10", kTestTablet));
    EXPECT_EQ(stdout[3],
              Substitute("KuduTableTest,$0,0,undo,,PLAIN_ENCODING,10", kTestTablet));
    EXPECT_EQ(stdout[4],
              Substitute("KuduTableTest,$0,0,bloom,,PLAIN_ENCODING,0", kTestTablet));
  }
}

// Create and start Kudu mini cluster, optionally creating a table in the DB,
// and then run 'kudu perf loadgen ...' utility against it.
void ToolTest::RunLoadgen(int num_tservers,
                          const vector<string>& tool_args,
                          const string& table_name) {
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = num_tservers;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));
  if (!table_name.empty()) {
    static const string kKeyColumnName = "key";
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
        ColumnSchema("decimal32_val", DECIMAL32, false,
                     NULL, NULL, ColumnStorageAttributes(),
                     ColumnTypeAttributes(9, 9)),
        ColumnSchema("decimal64_val", DECIMAL64, false,
                     NULL, NULL, ColumnStorageAttributes(),
                     ColumnTypeAttributes(18, 2)),
        ColumnSchema("decimal128_val", DECIMAL128, false,
                     NULL, NULL, ColumnStorageAttributes(),
                     ColumnTypeAttributes(38, 0)),
        ColumnSchema("unixtime_micros_val", UNIXTIME_MICROS),
        ColumnSchema("string_val", STRING),
        ColumnSchema("binary_val", BINARY),
      }, 1);

    shared_ptr<client::KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    KuduSchema client_schema(client::KuduSchemaFromSchema(kSchema));
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
  ASSERT_OK(RunKuduTool(args));
}

// Run the loadgen benchmark with all optional parameters set to defaults.
TEST_F(ToolTest, TestLoadgenDefaultParameters) {
  NO_FATALS(RunLoadgen());
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

// Run loadgen benchmark in AUTO_FLUSH_BACKGROUND mode, randomized values.
TEST_F(ToolTest, TestLoadgenAutoFlushBackgroundRandom) {
  NO_FATALS(RunLoadgen(5,
      {
        "--buffer_flush_watermark_pct=0.125",
        "--buffer_size_bytes=65536",
        "--buffers_num=8",
        // small number of rows to avoid collisions: it's random generation mode
        "--num_rows_per_thread=16",
        "--num_threads=1",
        "--run_scan",
        "--string_len=8",
        "--use_random",
      },
      "bench_auto_flush_background_random"));
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
  opts.bind_mode = cluster::MiniCluster::WILDCARD;
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
  Status s = RunTool(
      Substitute("remote_replica copy $0 $1 $2",
                 healthy_tablet_id, src_ts_addr, dst_ts_addr),
                 nullptr, &stderr, nullptr, nullptr);
  ASSERT_TRUE(s.IsRuntimeError());
  SCOPED_TRACE(stderr);
  ASSERT_STR_CONTAINS(stderr, "Rejecting tablet copy request");

  NO_FATALS(RunActionStdoutNone(Substitute("remote_replica copy $0 $1 $2 --force_copy",
                                           healthy_tablet_id, src_ts_addr, dst_ts_addr)));
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
      "local_replica delete $0 --fs_wal_dir=$1 --fs_data_dirs=$2 --clean_unsafe",
      deleted_tablet_id, cluster_->tablet_server(kDstTsIndex)->wal_dir(),
      JoinStrings(cluster_->tablet_server(kDstTsIndex)->data_dirs(), ","))));

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
  NO_FATALS(RunActionStdoutNone(Substitute("remote_replica copy $0 $1 $2 --force_copy",
                                           tombstoned_tablet_id, src_ts_addr, dst_ts_addr)));
  ASSERT_OK(WaitUntilTabletInState(dst_ts, tombstoned_tablet_id,
                                   tablet::RUNNING, kTimeout));
  // Copy deleted_tablet_id from source to destination.
  NO_FATALS(RunActionStdoutNone(Substitute("remote_replica copy $0 $1 $2",
                                           deleted_tablet_id, src_ts_addr, dst_ts_addr)));
  ASSERT_OK(WaitUntilTabletInState(dst_ts, deleted_tablet_id,
                                   tablet::RUNNING, kTimeout));
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
  const string& tserver_dir = ts->options()->fs_opts.wal_root;
  // Using the delete tool with tablet server running fails.
  string stderr;
  Status s = RunTool(
      Substitute("local_replica delete $0 --fs_wal_dir=$1 --fs_data_dirs=$1 "
                 "--clean_unsafe", tablet_id, tserver_dir),
                 nullptr, &stderr, nullptr, nullptr);
  ASSERT_TRUE(s.IsRuntimeError());
  SCOPED_TRACE(stderr);
  ASSERT_STR_CONTAINS(stderr, "Resource temporarily unavailable");

  // Shut down tablet server and use the delete tool with --clean_unsafe.
  ts->Shutdown();

  const string& data_dir = JoinPathSegments(tserver_dir, "data");
  uint64_t size_before_delete;
  ASSERT_OK(env_->GetFileSizeOnDiskRecursively(data_dir, &size_before_delete));
  NO_FATALS(RunActionStdoutNone(Substitute("local_replica delete $0 --fs_wal_dir=$1 "
                                           "--fs_data_dirs=$1 --clean_unsafe",
                                           tablet_id, tserver_dir)));
  // Verify metadata and WAL segments for the tablet_id are gone.
  const string& wal_dir = JoinPathSegments(tserver_dir,
                                           Substitute("wals/$0", tablet_id));
  ASSERT_FALSE(env_->FileExists(wal_dir));
  const string& meta_dir = JoinPathSegments(tserver_dir,
                                            Substitute("tablet-meta/$0", tablet_id));
  ASSERT_FALSE(env_->FileExists(meta_dir));

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
  boost::optional<OpId> last_logged_opid;
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
                                           "--fs_data_dirs=$1",
                                           tablet_id, tserver_dir)));
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
    boost::optional<OpId> tombstoned_opid =
        tablet_replicas[0]->tablet_metadata()->tombstone_last_logged_opid();
    ASSERT_NE(boost::none, tombstoned_opid);
    ASSERT_NE(boost::none, last_logged_opid);
    ASSERT_EQ(last_logged_opid->term(), tombstoned_opid->term());
    ASSERT_EQ(last_logged_opid->index(), tombstoned_opid->index());
  }
}

// Test for 'local_replica cmeta' functionality.
TEST_F(ToolTest, TestLocalReplicaCMetaOps) {
  NO_FATALS(StartMiniCluster());

  // TestWorkLoad.Setup() internally generates a table.
  TestWorkload workload(mini_cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup();
  MiniTabletServer* ts = mini_cluster_->mini_tablet_server(0);
  const string ts_uuid = ts->uuid();
  const string& flags = Substitute("-fs-wal-dir $0", ts->options()->fs_opts.wal_root);
  string tablet_id;
  {
    vector<string> tablets;
    NO_FATALS(RunActionStdoutLines(Substitute("local_replica list $0", flags), &tablets));
    ASSERT_EQ(1, tablets.size());
    tablet_id = tablets[0];
  }
  const auto& cmeta_path = ts->server()->fs_manager()->GetConsensusMetadataPath(tablet_id);

  ts->Shutdown();

  // Test print_replica_uuids.
  // We only have a single replica, so we expect one line, with our server's UUID.
  {
    vector<string> uuids;
    NO_FATALS(RunActionStdoutLines(Substitute("local_replica cmeta print_replica_uuids $0 $1",
                                               flags, tablet_id), &uuids));
    ASSERT_EQ(1, uuids.size());
    EXPECT_EQ(ts_uuid, uuids[0]);
  }

  // Test using set-term to bump the term to 123.
  {
    NO_FATALS(RunActionStdoutNone(Substitute("local_replica cmeta set-term $0 $1 123",
                                             flags, tablet_id)));

    string stdout;
    NO_FATALS(RunActionStdoutString(Substitute("pbc dump $0", cmeta_path),
                                    &stdout));
    ASSERT_STR_CONTAINS(stdout, "current_term: 123");
  }

  // Test that set-term refuses to decrease the term.
  {
    string stdout, stderr;
    Status s = RunTool(Substitute("local_replica cmeta set-term $0 $1 10",
                                  flags, tablet_id),
                       &stdout, &stderr,
                       /* stdout_lines = */ nullptr,
                       /* stderr_lines = */ nullptr);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ("", stdout);
    EXPECT_THAT(stderr, testing::HasSubstr(
        "specified term 10 must be higher than current term 123"));
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

TEST_F(ToolTest, TestMasterList) {
  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = 0;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  auto* master = cluster_->master();

  string out;
  NO_FATALS(RunActionStdoutString(
        Substitute("master list $0 --columns=uuid,rpc-addresses", master_addr),
        &out));

  ASSERT_STR_CONTAINS(out, master->uuid());
  ASSERT_STR_CONTAINS(out, master->bound_rpc_hostport().ToString());
}

TEST_F(ToolTest, TestRenameTable) {
  NO_FATALS(StartExternalMiniCluster());
  const string& kTableName = "kudu.table";
  const string& kNewTableName = "kudu_table";

  // Create the table.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_num_replicas(1);
  workload.Setup();

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  string out;
  NO_FATALS(RunActionStdoutNone(Substitute("table rename_table $0 $1 $2",
                                           master_addr, kTableName,
                                           kNewTableName)));
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
      .add_master_server_addr(master_addr)
      .Build(&client));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kNewTableName, &table));
}

TEST_F(ToolTest, TestRenameColumn) {
  NO_FATALS(StartExternalMiniCluster());
  const string& kTableName = "table";
  const string& kColumnName = "col.0";
  const string& kNewColumnName = "col_0";

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
  string out;
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

Status CreateHmsTable(HmsClient* client,
                      const string& database_name,
                      const string& table_name,
                      const string& table_type) {
  hive::Table table;
  string kudu_table_name(table_name);
  table.dbName = database_name;
  table.tableName = table_name;
  table.tableType = table_type;
  if (table_type == HmsClient::kManagedTable) {
    kudu_table_name = Substitute("$0$1.$2", HmsClient::kLegacyTablePrefix,
                                 database_name, table_name);
  }

  table.__set_parameters({
      make_pair(HmsClient::kStorageHandlerKey,
                HmsClient::kLegacyKuduStorageHandler),
      make_pair(HmsClient::kLegacyKuduTableNameKey,
                kudu_table_name),
      make_pair(HmsClient::kKuduMasterAddrsKey,
                "Master_Addrs"),
  });

  // TODO(Hao): Remove this once HIVE-19253 is fixed.
  if (table_type == HmsClient::kExternalTable) {
    table.parameters[HmsClient::kExternalTableKey] = "TRUE";
  }

  return client->CreateTable(table);
}

Status CreateKuduTable(const shared_ptr<KuduClient>& kudu_client,
                       const string& table_name) {
  KuduSchemaBuilder schema_builder;
  schema_builder.AddColumn("foo")
      ->Type(client::KuduColumnSchema::INT32)
      ->NotNull()
      ->PrimaryKey();
  KuduSchema schema;
  RETURN_NOT_OK(schema_builder.Build(&schema));
  unique_ptr<client::KuduTableCreator> table_creator(kudu_client->NewTableCreator());
  return table_creator->table_name(table_name)
              .schema(&schema)
              .num_replicas(1)
              .set_range_partition_columns({ "foo" })
              .Create();
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
  shared_ptr<KuduTable> kudu_table;
  ASSERT_OK(kudu_client->OpenTable(Substitute("$0.$1", database_name, table_name),
                                   &kudu_table));
  ASSERT_TRUE(hms_table.parameters[HmsClient::kStorageHandlerKey] ==
                  HmsClient::kKuduStorageHandler &&
              hms_table.parameters[HmsClient::kKuduTableIdKey] ==
                  kudu_table->id() &&
              hms_table.parameters[HmsClient::kKuduMasterAddrsKey] == master_addr &&
              !ContainsKey(hms_table.parameters, HmsClient::kLegacyKuduTableNameKey));
}

TEST_F(ToolTest, TestHmsUpgrade) {
  ExternalMiniClusterOptions opts;
  opts.hms_mode = HmsMode::ENABLE_HIVE_METASTORE;
  NO_FATALS(StartExternalMiniCluster(std::move(opts)));

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  HmsClientOptions hms_client_opts;
  HmsClient hms_client(cluster_->hms()->address(), hms_client_opts);
  ASSERT_OK(hms_client.Start());
  ASSERT_TRUE(hms_client.IsConnected());

  const string kDatabaseName = "my_db";
  const string kDefaultDatabaseName = "default";
  const string kManagedTableName = "managed_table";
  const string kExternalTableName = "external_table";
  const string kKuduTableName = "kudu_table";
  shared_ptr<KuduClient> kudu_client;
  ASSERT_OK(KuduClientBuilder()
      .add_master_server_addr(master_addr)
      .Build(&kudu_client));

  // 1. Create a managed impala table in HMS and the corresponding table in Kudu.
  {
    string legacy_managed_table_name = Substitute("$0$1.$2", HmsClient::kLegacyTablePrefix,
                                                  kDatabaseName, kManagedTableName);
    ASSERT_OK(CreateKuduTable(kudu_client, legacy_managed_table_name));
    shared_ptr<KuduTable> table;
    ASSERT_OK(kudu_client->OpenTable(legacy_managed_table_name, &table));
    hive::Database db;
    db.name = kDatabaseName;
    ASSERT_OK(hms_client.CreateDatabase(db));
    ASSERT_OK(CreateHmsTable(&hms_client, kDatabaseName, kManagedTableName,
                             HmsClient::kManagedTable));
    hive::Table hms_table;
    ASSERT_OK(hms_client.GetTable(kDatabaseName, kManagedTableName, &hms_table));
    ASSERT_EQ(HmsClient::kManagedTable, hms_table.tableType);
  }

  // 2. Create an external impala table in HMS and the corresponding table in Kudu.
  {
    ASSERT_OK(CreateKuduTable(kudu_client, kExternalTableName));
    shared_ptr<KuduTable> table;
    ASSERT_OK(kudu_client->OpenTable(kExternalTableName, &table));
    ASSERT_OK(CreateHmsTable(&hms_client, kDatabaseName, kExternalTableName,
                             HmsClient::kExternalTable));
    hive::Table hms_table;
    ASSERT_OK(hms_client.GetTable(kDatabaseName, kExternalTableName, &hms_table));
    ASSERT_EQ(HmsClient::kExternalTable, hms_table.tableType);
  }

  // 3. Create several non-impala Kudu tables. One with hive compatible name, and the
  //    other ones with hive incompatible names.
  {
    ASSERT_OK(CreateKuduTable(kudu_client, kKuduTableName));
    shared_ptr<KuduTable> table;
    ASSERT_OK(kudu_client->OpenTable(kKuduTableName, &table));

    ASSERT_OK(CreateKuduTable(kudu_client, "invalid#table"));
    ASSERT_OK(kudu_client->OpenTable("invalid#table", &table));

    ASSERT_OK(CreateKuduTable(kudu_client, "invalid.@"));
    ASSERT_OK(kudu_client->OpenTable("invalid.@", &table));

    ASSERT_OK(CreateKuduTable(kudu_client, "@.invalid"));
    ASSERT_OK(kudu_client->OpenTable("@.invalid", &table));

    ASSERT_OK(CreateKuduTable(kudu_client, "invalid"));
    ASSERT_OK(kudu_client->OpenTable("invalid", &table));

    ASSERT_OK(CreateKuduTable(kudu_client, "in.val.id"));
    ASSERT_OK(kudu_client->OpenTable("in.val.id", &table));
  }

  {
    vector<string> table_names;
    ASSERT_OK(hms_client.GetAllTables(kDatabaseName, &table_names));
    ASSERT_EQ(2, table_names.size());
  }

  // Upgrade the historical metadata in both Hive Metastore and Kudu.
  string out;
  NO_FATALS(RunActionStdinStdoutString(
      Substitute("hms upgrade $0 $1 --unlock_experimental_flags=true "
                 "--hive_metastore_uris=$2", master_addr,
                 kDefaultDatabaseName, cluster_->hms()->uris()),
      "valid_1\nvalid_2\nvalid_3\nvalid_4\nvalid_5\n", &out));

  // Validate the Kudu table names and metadata format of hms entries.
  {
    vector<string> table_names;
    ASSERT_OK(kudu_client->ListTables(&table_names));
    ASSERT_EQ(8, table_names.size());
    for (const auto& n : table_names) {
      ASSERT_TRUE(IsValidTableName(n));
    }

    NO_FATALS(ValidateHmsEntries(&hms_client, kudu_client, kDatabaseName,
                                 kManagedTableName, master_addr));
    NO_FATALS(ValidateHmsEntries(&hms_client, kudu_client, kDatabaseName,
                                 kExternalTableName, master_addr));
    NO_FATALS(ValidateHmsEntries(&hms_client, kudu_client, kDefaultDatabaseName,
                                 kKuduTableName, master_addr));
    table_names.clear();
    vector<string> db_names;
    ASSERT_OK(hms_client.GetAllDatabases(&db_names));
    ASSERT_EQ(2, db_names.size());
    ASSERT_OK(hms_client.GetAllTables(kDatabaseName, &table_names));
    ASSERT_EQ(2, table_names.size());
    table_names.clear();
    ASSERT_OK(hms_client.GetAllTables(kDefaultDatabaseName, &table_names));
    ASSERT_EQ(6, table_names.size());
  }

  ASSERT_OK(hms_client.Stop());
}

// This test is parameterized on the serialization mode and Kerberos.
class ControlShellToolTest :
    public ToolTest,
    public ::testing::WithParamInterface<std::tuple<ControlShellProtocol::SerializationMode,
                                                    bool>> {
 public:
  virtual void SetUp() override {
    ToolTest::SetUp();

    // Start the control shell.
    string mode;
    switch (serde_mode()) {
      case ControlShellProtocol::SerializationMode::JSON: mode = "json"; break;
      case ControlShellProtocol::SerializationMode::PB: mode = "pb"; break;
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
    proto_.reset(new ControlShellProtocol(serde_mode(),
                                          ControlShellProtocol::CloseMode::CLOSE_ON_DESTROY,
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

  ControlShellProtocol::SerializationMode serde_mode() const {
    return ::testing::get<0>(GetParam());
  }

  bool enable_kerberos() const {
    return ::testing::get<1>(GetParam());
  }

  unique_ptr<Subprocess> shell_;
  unique_ptr<ControlShellProtocol> proto_;
};

INSTANTIATE_TEST_CASE_P(SerializationModes, ControlShellToolTest,
                        ::testing::Combine(::testing::Values(
                            ControlShellProtocol::SerializationMode::PB,
                            ControlShellProtocol::SerializationMode::JSON),
                                           ::testing::Bool()));

TEST_P(ControlShellToolTest, TestControlShell) {
  const int kNumMasters = 1;
  const int kNumTservers = 3;

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
    client::KuduClientBuilder client_builder;
    for (const auto& e : masters) {
      HostPort hp;
      ASSERT_OK(HostPortFromPB(e.bound_rpc_address(), &hp));
      client_builder.add_master_server_addr(hp.ToString());
    }
    shared_ptr<client::KuduClient> client;
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

  // Restart it.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_start_daemon()->mutable_id() = tservers[0].id();
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Stop a master.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_stop_daemon()->mutable_id() = masters[0].id();
    ASSERT_OK(SendReceive(req, &resp));
  }

  // Restart it.
  {
    ControlShellRequestPB req;
    ControlShellResponsePB resp;
    *req.mutable_start_daemon()->mutable_id() = masters[0].id();
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
                                       InternalMiniCluster* cluster) {
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

  // Create a table and write some data to it.
  TestWorkload workload(cluster);
  workload.set_schema(schema);
  workload.set_table_name(table_name);
  workload.set_num_replicas(1);
  workload.Setup();
  workload.Start();
  ASSERT_EVENTUALLY([&](){
    ASSERT_GE(workload.rows_inserted(), 10000);
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
  ASSERT_STR_CONTAINS(stderr, "no healthy data directories found");

  // If we instead try to add the directory to the existing list of
  // directories, Kudu should allow it.
  vector<string> new_data_roots = { new_data_root_no_wal, wal_root };
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1",
      wal_root, JoinStrings(new_data_roots, ","))));
}

TEST_F(ToolTest, TestFsAddRemoveDataDirEndToEnd) {
  const string kTableFoo = "foo";
  const string kTableBar = "bar";

  if (FLAGS_block_manager == "file") {
    LOG(INFO) << "Skipping test, only log block manager is supported";
    return;
  }

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
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1",
      wal_root, JoinStrings(data_roots, ","))));

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
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1",
      wal_root, JoinStrings(data_roots, ",")), &stderr).IsRuntimeError());
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
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1 --force",
      wal_root, JoinStrings(data_roots, ","))));
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
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1",
      wal_root, JoinStrings(data_roots, ","))));
  mts->options()->fs_opts.data_roots = data_roots;
  ASSERT_OK(mts->Start());
  ASSERT_OK(mts->WaitStarted());
  mts->Shutdown();

  // Remove it again so that the second table's tablets fail once again.
  data_roots.pop_back();
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1 --force",
      wal_root, JoinStrings(data_roots, ","))));
  mts->options()->fs_opts.data_roots = data_roots;
  ASSERT_OK(mts->Start());
  s = mts->WaitStarted();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "one or more data dirs may have been removed");

  // Delete the second table and wait for all of its tablets to be deleted.
  shared_ptr<client::KuduClient> client;
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
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1",
      wal_root, JoinStrings(data_roots, ","))));
  mts->options()->fs_opts.data_roots = data_roots;
  ASSERT_OK(mts->Start());
  ASSERT_OK(mts->WaitStarted());

  // Shut down again, delete the newly added data directory, and restart. No
  // errors, because the first table's tablets were never configured to use the
  // new data directory.
  mts->Shutdown();
  data_roots.pop_back();
  NO_FATALS(RunActionStdoutNone(Substitute(
      "fs update_dirs --fs_wal_dir=$0 --fs_data_dirs=$1",
      wal_root, JoinStrings(data_roots, ","))));
  mts->options()->fs_opts.data_roots = data_roots;
  ASSERT_OK(mts->Start());
  ASSERT_OK(mts->WaitStarted());
}

TEST_F(ToolTest, TestDumpFSWithNonDefaultMetadataDir) {
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
  Status s = RunTool(Substitute("fs dump uuid --fs_wal_dir=$0", opts.wal_root),
                    nullptr, &stderr, {}, {});
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_CONTAINS(s.ToString(), "process exited with non-zero status");
  SCOPED_TRACE(stderr);
  ASSERT_STR_CONTAINS(stderr, "could not verify required directory");

  // Providing the necessary arguments, the tool should work.
  string stdout;
  NO_FATALS(RunActionStdoutString(Substitute(
      "fs dump uuid --fs_wal_dir=$0 --fs_metadata_dir=$1",
      opts.wal_root, opts.metadata_root), &stdout));
  SCOPED_TRACE(stdout);
  ASSERT_EQ(uuid, stdout);
}

TEST_F(ToolTest, TestReplaceTablet) {
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
            [&](const string& tablet_id) -> bool { return tablet_id == old_tablet_id; }));
    }
  });

  // The replaced tablet can self-heal because of tombstoned voting.
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());

  // Sanity check: there should be no more rows than we inserted before the replace.
  // TODO(wdberkeley): Should also be possible to keep inserting through a replace.
  client::sp::shared_ptr<client::KuduTable> workload_table;
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
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_MATCHES(stderr, "failed to parse stacks from .*: at line 1: "
                     "invalid JSON payload.*lacks ending quotation");
}

} // namespace tools
} // namespace kudu
