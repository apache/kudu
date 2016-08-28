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

#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <glog/stl_logging.h>

#include "kudu/cfile/cfile-test-base.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace tools {

using cfile::CFileWriter;
using cfile::StringDataGenerator;
using cfile::WriterOptions;
using fs::WritableBlock;
using std::string;
using std::vector;
using strings::Substitute;

class ToolTest : public KuduTest {
 public:
  ToolTest() {
    string exe;
    CHECK_OK(env_->GetExecutablePath(&exe));
    string bin_root = DirName(exe);
    string tool_path = JoinPathSegments(bin_root, "kudu");
    CHECK(env_->FileExists(tool_path)) << "kudu tool not found at " << tool_path;
    tool_path_ = tool_path;
  }

  Status RunTool(const string& arg_str,
                 vector<string>* stdout_lines,
                 vector<string>* stderr_lines) const {
    vector<string> args = { tool_path_ };
    vector<string> more_args = strings::Split(arg_str, " ",
                                              strings::SkipEmpty());
    args.insert(args.end(), more_args.begin(), more_args.end());

    string stdout;
    string stderr;
    Status s = Subprocess::Call(args, &stdout, &stderr);
    StripWhiteSpace(&stdout);
    StripWhiteSpace(&stderr);
    *stdout_lines = strings::Split(stdout, "\n", strings::SkipEmpty());
    *stderr_lines = strings::Split(stderr, "\n", strings::SkipEmpty());
    return s;

  }

  void RunTestActionNoOut(const string& arg_str) const {
    vector<string> stdout;
    RunTestAction(arg_str, &stdout);
    ASSERT_TRUE(stdout.empty());
  }

  void RunTestAction(const string& arg_str, vector<string>* stdout) const {
    vector<string> stderr;
    Status s = RunTool(arg_str, stdout, &stderr);
    SCOPED_TRACE(*stdout);
    SCOPED_TRACE(stderr);
    ASSERT_OK(s);
  }

  void RunTestHelp(const string& arg_str,
                   const vector<string>& regexes,
                   const Status& expected_status = Status::OK()) const {
    vector<string> stdout;
    vector<string> stderr;
    Status s = RunTool(arg_str, &stdout, &stderr);
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
    vector<string> remaining_lines;
    for (int i = usage_idx + 1; i < stderr.size(); i++) {
      remaining_lines.push_back(stderr[i]);
    }
    for (const auto& r : regexes) {
      ASSERT_STRINGS_ANY_MATCH(remaining_lines, r);
    }
  }

 private:
  string tool_path_;
};

TEST_F(ToolTest, TestTopLevelHelp) {
  const vector<string> kTopLevelRegexes = {
      "cluster.*Kudu cluster",
      "fs.*Kudu filesystem",
      "pbc.*protobuf container",
      "tablet.*Kudu replica"
  };
  NO_FATALS(RunTestHelp("", kTopLevelRegexes));
  NO_FATALS(RunTestHelp("--help", kTopLevelRegexes));
  NO_FATALS(RunTestHelp("not_a_mode", kTopLevelRegexes,
                        Status::InvalidArgument("unknown command 'not_a_mode'")));
}

TEST_F(ToolTest, TestModeHelp) {
  {
    const vector<string> kFsModeRegexes = {
        "format.*new Kudu filesystem",
        "print_uuid.*UUID of a Kudu filesystem"
    };
    NO_FATALS(RunTestHelp("fs", kFsModeRegexes));
    NO_FATALS(RunTestHelp("fs not_a_mode", kFsModeRegexes,
                          Status::InvalidArgument("unknown command 'not_a_mode'")));
  }
  {
    const vector<string> kTabletModeRegexes = {
        "cmeta.*consensus metadata file",
        "copy.*Copy a replica"
    };
    NO_FATALS(RunTestHelp("tablet", kTabletModeRegexes));
  }
  {
    const vector<string> kCmetaModeRegexes = {
        "print_replica_uuids.*Print all replica UUIDs",
        "rewrite_raft_config.*Rewrite a replica"
    };
    NO_FATALS(RunTestHelp("tablet cmeta", kCmetaModeRegexes));
  }
  {
    const vector<string> kClusterModeRegexes = {
        "ksck.*Check the health of a Kudu cluster",
    };
    NO_FATALS(RunTestHelp("cluster", kClusterModeRegexes));
  }
  {
    const vector<string> kPbcModeRegexes = {
        "dump.*Dump a PBC",
    };
    NO_FATALS(RunTestHelp("pbc", kPbcModeRegexes));
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

TEST_F(ToolTest, TestFsFormat) {
  const string kTestDir = GetTestPath("test");
  NO_FATALS(RunTestActionNoOut(Substitute("fs format --fs_wal_dir=$0", kTestDir)));
  FsManager fs(env_.get(), kTestDir);
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
  NO_FATALS(RunTestActionNoOut(Substitute(
      "fs format --fs_wal_dir=$0 --uuid=$1", kTestDir, original_uuid)));
  FsManager fs(env_.get(), kTestDir);
  ASSERT_OK(fs.Open());

  string canonicalized_uuid;
  ASSERT_OK(generator.Canonicalize(fs.uuid(), &canonicalized_uuid));
  ASSERT_EQ(fs.uuid(), canonicalized_uuid);
  ASSERT_EQ(fs.uuid(), original_uuid);
}

TEST_F(ToolTest, TestFsPrintUuid) {
  const string kTestDir = GetTestPath("test");
  string uuid;
  {
    FsManager fs(env_.get(), kTestDir);
    ASSERT_OK(fs.CreateInitialFileSystemLayout());
    ASSERT_OK(fs.Open());
    uuid = fs.uuid();
  }
  vector<string> stdout;
  NO_FATALS(RunTestAction(Substitute(
      "fs print_uuid --fs_wal_dir=$0", kTestDir), &stdout));
  SCOPED_TRACE(stdout);
  ASSERT_EQ(1, stdout.size());
  ASSERT_EQ(uuid, stdout[0]);
}

TEST_F(ToolTest, TestPbcDump) {
  const string kTestDir = GetTestPath("test");
  string uuid;
  string instance_path;
  {
    ObjectIdGenerator generator;
    FsManager fs(env_.get(), kTestDir);
    ASSERT_OK(fs.CreateInitialFileSystemLayout(generator.Next()));
    ASSERT_OK(fs.Open());
    uuid = fs.uuid();
    instance_path = fs.GetInstanceMetadataPath(kTestDir);
  }
  vector<string> stdout;
  {
    NO_FATALS(RunTestAction(Substitute(
        "pbc dump $0", instance_path), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_EQ(4, stdout.size());
    ASSERT_EQ("Message 0", stdout[0]);
    ASSERT_EQ("-------", stdout[1]);
    ASSERT_EQ(Substitute("uuid: \"$0\"", uuid), stdout[2]);
    ASSERT_STR_MATCHES(stdout[3], "^format_stamp: \"Formatted at .*\"$");
  }
  {
    NO_FATALS(RunTestAction(Substitute(
        "pbc dump $0/instance --oneline", kTestDir), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_EQ(1, stdout.size());
    ASSERT_STR_MATCHES(
        stdout[0], Substitute(
            "^0\tuuid: \"$0\" format_stamp: \"Formatted at .*\"$$", uuid));
  }
}

TEST_F(ToolTest, TestFsDumpCFile) {
  const int kNumEntries = 8192;
  const string kTestDir = GetTestPath("test");
  FsManager fs(env_.get(), kTestDir);
  ASSERT_OK(fs.CreateInitialFileSystemLayout());
  ASSERT_OK(fs.Open());

  gscoped_ptr<WritableBlock> block;
  ASSERT_OK(fs.CreateNewBlock(&block));
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

  vector<string> stdout;
  {
    NO_FATALS(RunTestAction(Substitute(
        "fs dump_cfile --fs_wal_dir=$0 $1 --noprint_meta --noprint_rows",
        kTestDir, block_id.ToString()), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_TRUE(stdout.empty());
  }
  {
    NO_FATALS(RunTestAction(Substitute(
        "fs dump_cfile --fs_wal_dir=$0 $1 --noprint_rows",
        kTestDir, block_id.ToString()), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_GE(stdout.size(), 4);
    ASSERT_EQ(stdout[0], "Header:");
    ASSERT_EQ(stdout[3], "Footer:");
  }
  {
    NO_FATALS(RunTestAction(Substitute(
        "fs dump_cfile --fs_wal_dir=$0 $1 --noprint_meta",
        kTestDir, block_id.ToString()), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_EQ(kNumEntries, stdout.size());
  }
  {
    NO_FATALS(RunTestAction(Substitute(
        "fs dump_cfile --fs_wal_dir=$0 $1",
        kTestDir, block_id.ToString()), &stdout));
    SCOPED_TRACE(stdout);
    ASSERT_GT(stdout.size(), kNumEntries);
    ASSERT_EQ(stdout[0], "Header:");
    ASSERT_EQ(stdout[3], "Footer:");
  }
}

} // namespace tools
} // namespace kudu
