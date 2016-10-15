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
#include <unistd.h>

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flags.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

DECLARE_string(umask);

namespace kudu {

class FsManagerTestBase : public KuduTest {
 public:
  void SetUp() OVERRIDE {
    KuduTest::SetUp();

    // Initialize File-System Layout
    ReinitFsManager();
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());
  }

  void ReinitFsManager() {
    ReinitFsManager(GetTestPath("fs_root"), { GetTestPath("fs_root")} );
  }

  void ReinitFsManager(const string& wal_path, const vector<string>& data_paths) {
    FsManagerOpts opts;
    opts.wal_path = wal_path;
    opts.data_paths = data_paths;
    fs_manager_.reset(new FsManager(env_, opts));
  }

  void TestReadWriteDataFile(const Slice& data) {
    uint8_t buffer[64];
    DCHECK_LT(data.size(), sizeof(buffer));

    // Test Write
    unique_ptr<fs::WritableBlock> writer;
    ASSERT_OK(fs_manager()->CreateNewBlock({}, &writer));
    ASSERT_OK(writer->Append(data));
    ASSERT_OK(writer->Close());

    // Test Read
    Slice result(buffer, data.size());
    unique_ptr<fs::ReadableBlock> reader;
    ASSERT_OK(fs_manager()->OpenBlock(writer->id(), &reader));
    ASSERT_OK(reader->Read(0, &result));
    ASSERT_EQ(data.size(), result.size());
    ASSERT_EQ(0, result.compare(data));
  }

  FsManager *fs_manager() const { return fs_manager_.get(); }

 private:
  gscoped_ptr<FsManager> fs_manager_;
};

TEST_F(FsManagerTestBase, TestBaseOperations) {
  fs_manager()->DumpFileSystemTree(std::cout);

  TestReadWriteDataFile(Slice("test0"));
  TestReadWriteDataFile(Slice("test1"));

  fs_manager()->DumpFileSystemTree(std::cout);
}

TEST_F(FsManagerTestBase, TestIllegalPaths) {
  vector<string> illegal = { "", "asdf", "/foo\n\t" };
  for (const string& path : illegal) {
    ReinitFsManager(path, { path });
    ASSERT_TRUE(fs_manager()->CreateInitialFileSystemLayout().IsIOError());
  }
}

TEST_F(FsManagerTestBase, TestMultiplePaths) {
  string wal_path = GetTestPath("a");
  vector<string> data_paths = { GetTestPath("a"), GetTestPath("b"), GetTestPath("c") };
  ReinitFsManager(wal_path, data_paths);
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
  ASSERT_OK(fs_manager()->Open());
}

TEST_F(FsManagerTestBase, TestMatchingPathsWithMismatchedSlashes) {
  string wal_path = GetTestPath("foo");
  vector<string> data_paths = { wal_path + "/" };
  ReinitFsManager(wal_path, data_paths);
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
}

TEST_F(FsManagerTestBase, TestDuplicatePaths) {
  string path = GetTestPath("foo");
  ReinitFsManager(path, { path, path, path });
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
  ASSERT_EQ(vector<string>({ JoinPathSegments(path, fs_manager()->kDataDirName) }),
            fs_manager()->GetDataRootDirs());
}

TEST_F(FsManagerTestBase, TestListTablets) {
  vector<string> tablet_ids;
  ASSERT_OK(fs_manager()->ListTabletIds(&tablet_ids));
  ASSERT_EQ(0, tablet_ids.size());

  string path = fs_manager()->GetTabletMetadataDir();
  unique_ptr<WritableFile> writer;
  ASSERT_OK(env_->NewWritableFile(
      JoinPathSegments(path, "foo.kudutmp"), &writer));
  ASSERT_OK(env_->NewWritableFile(
      JoinPathSegments(path, "foo.kudutmp.abc123"), &writer));
  ASSERT_OK(env_->NewWritableFile(
      JoinPathSegments(path, ".hidden"), &writer));
  ASSERT_OK(env_->NewWritableFile(
      JoinPathSegments(path, "a_tablet_sort_of"), &writer));

  ASSERT_OK(fs_manager()->ListTabletIds(&tablet_ids));
  ASSERT_EQ(1, tablet_ids.size()) << tablet_ids;
}

TEST_F(FsManagerTestBase, TestCannotUseNonEmptyFsRoot) {
  string path = GetTestPath("new_fs_root");
  ASSERT_OK(env_->CreateDir(path));
  {
    unique_ptr<WritableFile> writer;
    ASSERT_OK(env_->NewWritableFile(
        JoinPathSegments(path, "some_file"), &writer));
  }

  // Try to create the FS layout. It should fail.
  ReinitFsManager(path, { path });
  ASSERT_TRUE(fs_manager()->CreateInitialFileSystemLayout().IsAlreadyPresent());
}

TEST_F(FsManagerTestBase, TestEmptyWALPath) {
  ReinitFsManager("", {});
  Status s = fs_manager()->CreateInitialFileSystemLayout();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STR_CONTAINS(s.ToString(), "directory (fs_wal_dir) not provided");
}

TEST_F(FsManagerTestBase, TestOnlyWALPath) {
  string path = GetTestPath("new_fs_root");
  ASSERT_OK(env_->CreateDir(path));

  ReinitFsManager(path, {});
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
  ASSERT_TRUE(HasPrefixString(fs_manager()->GetWalsRootDir(), path));
  ASSERT_TRUE(HasPrefixString(fs_manager()->GetConsensusMetadataDir(), path));
  ASSERT_TRUE(HasPrefixString(fs_manager()->GetTabletMetadataDir(), path));
  vector<string> data_dirs = fs_manager()->GetDataRootDirs();
  ASSERT_EQ(1, data_dirs.size());
  ASSERT_TRUE(HasPrefixString(data_dirs[0], path));
}

TEST_F(FsManagerTestBase, TestFormatWithSpecificUUID) {
  string path = GetTestPath("new_fs_root");
  ReinitFsManager(path, {});

  // Use an invalid uuid at first.
  string uuid = "not_a_valid_uuid";
  Status s = fs_manager()->CreateInitialFileSystemLayout(uuid);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), Substitute("invalid uuid $0", uuid));

  // Now use a valid one.
  ObjectIdGenerator oid_generator;
  uuid = oid_generator.Next();
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout(uuid));
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(uuid, fs_manager()->uuid());
}

Status CountTmpFiles(Env* env, const string& path, const vector<string>& children,
                     unordered_set<string>* checked_dirs, size_t* count) {
  vector<string> sub_objects;
  for (const string& name : children) {
    if (name == "." || name == "..") continue;

    string sub_path;
    RETURN_NOT_OK(env->Canonicalize(JoinPathSegments(path, name), &sub_path));
    bool is_directory;
    RETURN_NOT_OK(env->IsDirectory(sub_path, &is_directory));
    if (is_directory) {
      if (!ContainsKey(*checked_dirs, sub_path)) {
        checked_dirs->insert(sub_path);
        RETURN_NOT_OK(env->GetChildren(sub_path, &sub_objects));
        RETURN_NOT_OK(CountTmpFiles(env, sub_path, sub_objects, checked_dirs, count));
      }
    } else if (name.find(kTmpInfix) != string::npos) {
      (*count)++;
    }
  }
  return Status::OK();
}

Status CountTmpFiles(Env* env, const vector<string>& roots, size_t* count) {
  unordered_set<string> checked_dirs;
  for (const string& root : roots) {
    vector<string> children;
    RETURN_NOT_OK(env->GetChildren(root, &children));
    RETURN_NOT_OK(CountTmpFiles(env, root, children, &checked_dirs, count));
  }
  return Status::OK();
}

TEST_F(FsManagerTestBase, TestTmpFilesCleanup) {
  string wal_path = GetTestPath("wals");
  vector<string> data_paths = { GetTestPath("data1"), GetTestPath("data2"), GetTestPath("data3") };
  ReinitFsManager(wal_path, data_paths);
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());

  // Create a few tmp files here
  shared_ptr<WritableFile> tmp_writer;

  string tmp_path = JoinPathSegments(fs_manager()->GetWalsRootDir(), "wal.kudutmp.file");
  ASSERT_OK(env_util::OpenFileForWrite(fs_manager()->env(), tmp_path, &tmp_writer));

  tmp_path = JoinPathSegments(fs_manager()->GetDataRootDirs()[0], "data1.kudutmp.file");
  ASSERT_OK(env_util::OpenFileForWrite(fs_manager()->env(), tmp_path, &tmp_writer));

  tmp_path = JoinPathSegments(fs_manager()->GetConsensusMetadataDir(), "12345.kudutmp.asdfg");
  ASSERT_OK(env_util::OpenFileForWrite(fs_manager()->env(), tmp_path, &tmp_writer));

  tmp_path = JoinPathSegments(fs_manager()->GetTabletMetadataDir(), "12345.kudutmp.asdfg");
  ASSERT_OK(env_util::OpenFileForWrite(fs_manager()->env(), tmp_path, &tmp_writer));

  // Not a misprint here: checking for just ".kudutmp" as well
  tmp_path = JoinPathSegments(fs_manager()->GetDataRootDirs()[1], "data2.kudutmp");
  ASSERT_OK(env_util::OpenFileForWrite(fs_manager()->env(), tmp_path, &tmp_writer));

  // Try with nested directory
  string nested_dir_path = JoinPathSegments(fs_manager()->GetDataRootDirs()[2], "data4");
  ASSERT_OK(env_util::CreateDirIfMissing(fs_manager()->env(), nested_dir_path));
  tmp_path = JoinPathSegments(nested_dir_path, "data4.kudutmp.file");
  ASSERT_OK(env_util::OpenFileForWrite(fs_manager()->env(), tmp_path, &tmp_writer));

  // Add a loop using symlink
  string data3_link = JoinPathSegments(nested_dir_path, "data3-link");
  int symlink_error = symlink(fs_manager()->GetDataRootDirs()[2].c_str(), data3_link.c_str());
  ASSERT_EQ(0, symlink_error);

  vector<string> lookup_dirs = fs_manager()->GetDataRootDirs();
  lookup_dirs.emplace_back(fs_manager()->GetWalsRootDir());
  lookup_dirs.emplace_back(fs_manager()->GetConsensusMetadataDir());
  lookup_dirs.emplace_back(fs_manager()->GetTabletMetadataDir());

  size_t n_tmp_files = 0;
  ASSERT_OK(CountTmpFiles(fs_manager()->env(), lookup_dirs, &n_tmp_files));
  ASSERT_EQ(6, n_tmp_files);

  // Opening fs_manager should remove tmp files
  ASSERT_OK(fs_manager()->Open());

  n_tmp_files = 0;
  ASSERT_OK(CountTmpFiles(fs_manager()->env(), lookup_dirs, &n_tmp_files));
  ASSERT_EQ(0, n_tmp_files);
}

namespace {

string FilePermsAsString(const string& path) {
  struct stat s;
  CHECK_ERR(stat(path.c_str(), &s));
  return StringPrintf("%03o", s.st_mode & ACCESSPERMS);
}

} // anonymous namespace

TEST_F(FsManagerTestBase, TestUmask) {
  // With the default umask, we should create files with permissions 600
  // and directories with permissions 700.
  ASSERT_EQ(077, g_parsed_umask) << "unexpected default value";
  string root = GetTestPath("fs_root");
  EXPECT_EQ("700", FilePermsAsString(root));
  EXPECT_EQ("700", FilePermsAsString(fs_manager()->GetConsensusMetadataDir()));
  EXPECT_EQ("600", FilePermsAsString(fs_manager()->GetInstanceMetadataPath(root)));

  // With umask 007, we should create files with permissions 660
  // and directories with 770.
  FLAGS_umask = "007";
  HandleCommonFlags();
  ASSERT_EQ(007, g_parsed_umask);
  root = GetTestPath("new_root");
  ReinitFsManager({ root }, { root });
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
  EXPECT_EQ("770", FilePermsAsString(root));
  EXPECT_EQ("770", FilePermsAsString(fs_manager()->GetConsensusMetadataDir()));
  EXPECT_EQ("660", FilePermsAsString(fs_manager()->GetInstanceMetadataPath(root)));

  // If we change the umask back to being restrictive and re-open the filesystem,
  // the permissions on the root dir should be fixed accordingly.
  FLAGS_umask = "077";
  HandleCommonFlags();
  ASSERT_EQ(077, g_parsed_umask);
  ReinitFsManager({ root }, { root });
  ASSERT_OK(fs_manager()->Open());
  EXPECT_EQ("700", FilePermsAsString(root));
}

} // namespace kudu
