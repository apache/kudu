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
#include <memory>
#include <sstream>
#include <string>
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
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/internal_mini_cluster.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
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
using client::sp::shared_ptr;
using consensus::OpId;
using consensus::RECEIVED_OPID;
using consensus::ReplicateRefPtr;
using consensus::ReplicateMsg;
using fs::FsReport;
using fs::WritableBlock;
using itest::ExternalMiniClusterFsInspector;
using itest::TServerDetails;
using log::Log;
using log::LogOptions;
using rpc::RpcController;
using std::back_inserter;
using std::copy;
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
  ToolTest()
      : tool_path_(GetKuduCtlAbsolutePath()) {
  }

  ~ToolTest() {
    STLDeleteValues(&ts_map_);
  }

  virtual void TearDown() OVERRIDE {
    if (cluster_) cluster_->Shutdown();
    if (mini_cluster_) mini_cluster_->Shutdown();
    KuduTest::TearDown();
  }

  Status RunTool(const string& arg_str,
                 string* stdout,
                 string* stderr,
                 vector<string>* stdout_lines,
                 vector<string>* stderr_lines) const {
    vector<string> args = { tool_path_ };
    vector<string> more_args = strings::Split(arg_str, " ",
                                              strings::SkipEmpty());
    args.insert(args.end(), more_args.begin(), more_args.end());

    string out;
    string err;
    Status s = Subprocess::Call(args, "", &out, &err);
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
    ASSERT_STR_MATCHES(err_lines[2], "Usage: kudu.*");
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
  void StartExternalMiniCluster(const vector<string>& extra_master_flags = {},
                                const vector<string>& extra_tserver_flags = {},
                                int num_tablet_servers = 1);
  void StartMiniCluster(int num_masters = 1,
                        int num_tablet_servers = 1);
  unique_ptr<ExternalMiniCluster> cluster_;
  unique_ptr<ExternalMiniClusterFsInspector> inspect_;
  unordered_map<string, TServerDetails*> ts_map_;
  unique_ptr<InternalMiniCluster> mini_cluster_;
  ExternalMiniClusterOptions cluster_opts_;
  string tool_path_;
};

void ToolTest::StartExternalMiniCluster(const vector<string>& extra_master_flags,
                                        const vector<string>& extra_tserver_flags,
                                        int num_tablet_servers) {
  cluster_opts_.extra_master_flags = extra_master_flags;
  cluster_opts_.extra_tserver_flags = extra_tserver_flags;
  cluster_opts_.num_tablet_servers = num_tablet_servers;
  cluster_.reset(new ExternalMiniCluster(cluster_opts_));
  ASSERT_OK(cluster_->Start());
  inspect_.reset(new ExternalMiniClusterFsInspector(cluster_.get()));
  ASSERT_OK(CreateTabletServerMap(cluster_->master_proxy(),
                                  cluster_->messenger(), &ts_map_));
}

void ToolTest::StartMiniCluster(int num_masters,
                                int num_tablet_servers) {
  InternalMiniClusterOptions opts;
  opts.num_masters = num_masters;
  opts.num_tablet_servers = num_tablet_servers;
  mini_cluster_.reset(new InternalMiniCluster(env_, opts));
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
      "fs",
      "local_replica",
      "master",
      "pbc",
      "perf",
      "remote_replica",
      "table",
      "tablet",
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
      "fs.*Kudu filesystem",
      "local_replica.*tablet replicas",
      "master.*Kudu Master",
      "pbc.*protobuf container",
      "perf.*performance of a Kudu cluster",
      "remote_replica.*tablet replicas on a Kudu Tablet Server",
      "table.*Kudu tables",
      "tablet.*Kudu tablets",
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
        "format.*new Kudu filesystem",
        "dump.*Dump a Kudu filesystem"
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
    const vector<string> kMasterModeRegexes = {
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
    for (int i = 0; i < block_ids.size(); i += 2) {
      ASSERT_OK(fs.DeleteBlock(block_ids[i]));
      missing_ids.push_back(block_ids[i]);
    }
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
  string master_data_dir = cluster_->GetDataPath("master-0");
  string args = Substitute("fs check --fs_wal_dir $0", master_data_dir);
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
    ASSERT_OK(GetExecutablePath("false", {}, &path));
    Status s = DoEdit(path, &stdout, &stderr);
    ASSERT_FALSE(s.ok());
    ASSERT_EQ("", stdout);
    ASSERT_EQ("Aborted: editor returned non-zero exit code", stderr);
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
    ASSERT_EQ("Invalid argument: Unable to parse JSON line: not-a-json-string: Unexpected token.\n"
              "not-a-json-string\n"
              "^", stderr);
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
    ASSERT_STR_MATCHES(stdout, "CFile Header: ");
    ASSERT_STR_MATCHES(stdout, "Delta stats:.*");
    ASSERT_STR_MATCHES(stdout, "ts range=.*");
    ASSERT_STR_MATCHES(stdout, "update_counts_by_col_id=.*");
    ASSERT_STR_MATCHES(stdout, "Dumping column block.*for column id.*");
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
}

// Create and start Kudu mini cluster, optionally creating a table in the DB,
// and then run 'kudu perf loadgen ...' utility against it.
void ToolTest::RunLoadgen(int num_tservers,
                          const vector<string>& tool_args,
                          const string& table_name) {
  NO_FATALS(StartExternalMiniCluster({}, {}, num_tservers));
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
        ColumnSchema("unixtime_micros_val", UNIXTIME_MICROS),
        ColumnSchema("string_val", STRING),
        ColumnSchema("binary_val", BINARY),
      }, 1);

    shared_ptr<client::KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(nullptr, &client));
    client::KuduSchema client_schema(client::KuduSchemaFromSchema(kSchema));
    unique_ptr<client::KuduTableCreator> table_creator(
        client->NewTableCreator());
    ASSERT_OK(table_creator->table_name(table_name)
              .schema(&client_schema)
              .add_hash_partitions({kKeyColumnName}, 2)
              .num_replicas(cluster_->num_tablet_servers())
              .Create());
  }
  vector<string> args = {
    GetKuduCtlAbsolutePath(),
    "perf",
    "loadgen",
    cluster_->master()->bound_rpc_addr().ToString(),
  };
  if (!table_name.empty()) {
    args.push_back(Substitute("-table_name=$0", table_name));
  }
  copy(tool_args.begin(), tool_args.end(), back_inserter(args));
  ASSERT_OK(Subprocess::Call(args));
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
  cluster_opts_.bind_mode = MiniCluster::WILDCARD;
  NO_FATALS(StartExternalMiniCluster(
      {"--catalog_manager_wait_for_new_tablets_to_elect_leader=false"},
      {"--enable_leader_failure_detection=false"}, kNumTservers));

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
  const string& tserver_dir = cluster_->tablet_server(kDstTsIndex)->data_dir();
  const string& deleted_tablet_id = tablets[1].tablet_status().tablet_id();
  NO_FATALS(RunActionStdoutNone(Substitute("local_replica delete $0 --fs_wal_dir=$1 "
                                           "--fs_data_dirs=$1 --clean_unsafe",
                                           deleted_tablet_id, tserver_dir)));

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
  const string& tserver_dir = ts->options()->fs_opts.wal_path;
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
  const string& tserver_dir = ts->options()->fs_opts.wal_path;

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
  const string& flags = Substitute("-fs-wal-dir $0", ts->options()->fs_opts.wal_path);
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
  NO_FATALS(StartExternalMiniCluster({}, {}, 1));

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
  NO_FATALS(StartExternalMiniCluster({}, {}, 0));

  string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  auto* master = cluster_->master();

  string out;
  NO_FATALS(RunActionStdoutString(
        Substitute("master list $0 --columns=uuid,rpc-addresses", master_addr),
        &out));

  ASSERT_STR_CONTAINS(out, master->uuid());
  ASSERT_STR_CONTAINS(out, master->bound_rpc_hostport().ToString());
}

} // namespace tools
} // namespace kudu
