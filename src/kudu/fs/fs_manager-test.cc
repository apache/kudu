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
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/fs_report.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flags.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/path_util.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::fs::ConsistencyCheckBehavior;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

DECLARE_bool(crash_on_eio);
DECLARE_double(env_inject_eio);
DECLARE_string(block_manager);
DECLARE_string(env_inject_eio_globs);
DECLARE_string(env_inject_lock_failure_globs);
DECLARE_string(umask);

namespace kudu {

class FsManagerTestBase : public KuduTest {
 public:
  FsManagerTestBase()
     : fs_root_(GetTestPath("fs_root")) {
  }

  void SetUp() OVERRIDE {
    KuduTest::SetUp();

    // Initialize File-System Layout
    ReinitFsManager();
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());
  }

  void ReinitFsManager() {
    ReinitFsManagerWithPaths(fs_root_, { fs_root_ });
  }

  void ReinitFsManagerWithPaths(string wal_path, vector<string> data_paths) {
    FsManagerOpts opts;
    opts.wal_root = std::move(wal_path);
    opts.data_roots = std::move(data_paths);
    ReinitFsManagerWithOpts(std::move(opts));
  }

  void ReinitFsManagerWithOpts(FsManagerOpts opts) {
    fs_manager_.reset(new FsManager(env_, std::move(opts)));
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
    ASSERT_OK(reader->Read(0, result));
    ASSERT_EQ(0, result.compare(data));
  }

  FsManager *fs_manager() const { return fs_manager_.get(); }

 protected:
  const string fs_root_;

 private:
  unique_ptr<FsManager> fs_manager_;
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
    ReinitFsManagerWithPaths(path, { path });
    ASSERT_TRUE(fs_manager()->CreateInitialFileSystemLayout().IsIOError());
  }
}

TEST_F(FsManagerTestBase, TestMultiplePaths) {
  string wal_path = GetTestPath("a");
  vector<string> data_paths = { GetTestPath("a"), GetTestPath("b"), GetTestPath("c") };
  ReinitFsManagerWithPaths(wal_path, data_paths);
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
  ASSERT_OK(fs_manager()->Open());
}

TEST_F(FsManagerTestBase, TestMatchingPathsWithMismatchedSlashes) {
  string wal_path = GetTestPath("foo");
  vector<string> data_paths = { wal_path + "/" };
  ReinitFsManagerWithPaths(wal_path, data_paths);
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
}

TEST_F(FsManagerTestBase, TestDuplicatePaths) {
  string path = GetTestPath("foo");
  ReinitFsManagerWithPaths(path, { path, path, path });
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
      JoinPathSegments(path, "foo.bak"), &writer));
  ASSERT_OK(env_->NewWritableFile(
      JoinPathSegments(path, "foo.bak.abc123"), &writer));
  ASSERT_OK(env_->NewWritableFile(
      JoinPathSegments(path, ".hidden"), &writer));
  // An uncanonicalized id.
  ASSERT_OK(env_->NewWritableFile(
      JoinPathSegments(path, "6ba7b810-9dad-11d1-80b4-00c04fd430c8"), &writer));
  // 1 valid tablet id.
  ASSERT_OK(env_->NewWritableFile(
      JoinPathSegments(path, "922ff7ed14c14dbca4ee16331dfda42a"), &writer));

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
  ReinitFsManagerWithPaths(path, { path });
  ASSERT_TRUE(fs_manager()->CreateInitialFileSystemLayout().IsAlreadyPresent());
}

TEST_F(FsManagerTestBase, TestEmptyWALPath) {
  ReinitFsManagerWithPaths("", {});
  Status s = fs_manager()->CreateInitialFileSystemLayout();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STR_CONTAINS(s.ToString(), "directory (fs_wal_dir) not provided");
}

TEST_F(FsManagerTestBase, TestOnlyWALPath) {
  string path = GetTestPath("new_fs_root");
  ASSERT_OK(env_->CreateDir(path));

  ReinitFsManagerWithPaths(path, {});
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
  ReinitFsManagerWithPaths(path, {});

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

TEST_F(FsManagerTestBase, TestMetadataDirInWALRoot) {
  // By default, the FsManager should put metadata in the wal root.
  FsManagerOpts opts;
  opts.wal_root = GetTestPath("wal");
  opts.data_roots = { GetTestPath("data") };
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
  ASSERT_OK(fs_manager()->Open());
  ASSERT_STR_CONTAINS(fs_manager()->GetTabletMetadataDir(),
                      JoinPathSegments("wal", FsManager::kTabletMetadataDirName));

  // Reinitializing the FS layout with any other configured metadata root
  // should fail, as a non-empty metadata root will be used verbatim.
  opts.metadata_root = GetTestPath("asdf");
  ReinitFsManagerWithOpts(opts);
  Status s = fs_manager()->Open();
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  // The above comment also applies to the default value before Kudu 1.6: the
  // first configured data directory. Let's check that too.
  opts.metadata_root = opts.data_roots[0];
  ReinitFsManagerWithOpts(opts);
  s = fs_manager()->Open();
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();

  // We should be able to verify that the metadata is in the WAL root.
  opts.metadata_root = opts.wal_root;
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
}

TEST_F(FsManagerTestBase, TestMetadataDirInDataRoot) {
  FsManagerOpts opts;
  opts.wal_root = GetTestPath("wal");
  opts.data_roots = { GetTestPath("data1") };

  // Creating a brand new FS layout configured with metadata in the first data
  // directory emulates the default behavior in Kudu 1.6 and below.
  opts.metadata_root = opts.data_roots[0];
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
  ASSERT_OK(fs_manager()->Open());
  const string& meta_root_suffix = JoinPathSegments("data1", FsManager::kTabletMetadataDirName);
  ASSERT_STR_CONTAINS(fs_manager()->GetTabletMetadataDir(), meta_root_suffix);

  // Opening the FsManager with an empty fs_metadata_dir flag should account
  // for the old default and use the first data directory for metadata.
  opts.metadata_root.clear();
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_STR_CONTAINS(fs_manager()->GetTabletMetadataDir(), meta_root_suffix);

  // Now let's test adding data directories with metadata in the data root.
  // Adding data directories is not supported by the file block manager.
  if (FLAGS_block_manager == "file") {
    LOG(INFO) << "Skipping the rest of test, file block manager not supported";
    return;
  }

  // Adding a data dir to the front of the FS root list (i.e. such that the
  // metadata root is no longer at the front) will prevent Kudu from starting.
  opts.data_roots = { GetTestPath("data2"), GetTestPath("data1") };
  opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
  ReinitFsManagerWithOpts(opts);
  Status s = fs_manager()->Open();
  ASSERT_STR_CONTAINS(s.ToString(), "could not verify required directory");
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_FALSE(env_->FileExists(opts.data_roots[0]));
  ASSERT_TRUE(env_->FileExists(opts.data_roots[1]));

  // Now allow the reordering by specifying the expected metadata root.
  opts.metadata_root = opts.data_roots[1];
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_STR_CONTAINS(fs_manager()->GetTabletMetadataDir(), meta_root_suffix);
}

TEST_F(FsManagerTestBase, TestIsolatedMetadataDir) {
  FsManagerOpts opts;
  opts.wal_root = GetTestPath("wal");
  opts.data_roots = { GetTestPath("data") };

  // Creating a brand new FS layout configured to a directory outside the WAL
  // or data directories is supported.
  opts.metadata_root = GetTestPath("asdf");
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
  ASSERT_OK(fs_manager()->Open());
  ASSERT_STR_CONTAINS(fs_manager()->GetTabletMetadataDir(),
                      JoinPathSegments("asdf", FsManager::kTabletMetadataDirName));
  ASSERT_NE(DirName(fs_manager()->GetTabletMetadataDir()),
            DirName(fs_manager()->GetWalsRootDir()));
  ASSERT_NE(DirName(fs_manager()->GetTabletMetadataDir()),
            DirName(fs_manager()->GetDataRootDirs()[0]));

  // If the user henceforth forgets to specify the metadata root, the FsManager
  // will fail to open.
  opts.metadata_root.clear();
  ReinitFsManagerWithOpts(opts);
  Status s = fs_manager()->Open();
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

Status CountTmpFiles(Env* env, const string& path, const vector<string>& children,
                     unordered_set<string>* checked_dirs, int* count) {
  int n = 0;
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
        int subdir_count = 0;
        RETURN_NOT_OK(CountTmpFiles(env, sub_path, sub_objects, checked_dirs, &subdir_count));
        n += subdir_count;
      }
    } else if (name.find(kTmpInfix) != string::npos) {
      n++;
    }
  }
  *count = n;
  return Status::OK();
}

Status CountTmpFiles(Env* env, const vector<string>& roots, int* count) {
  unordered_set<string> checked_dirs;
  int n = 0;
  for (const string& root : roots) {
    vector<string> children;
    RETURN_NOT_OK(env->GetChildren(root, &children));
    int dir_count;
    RETURN_NOT_OK(CountTmpFiles(env, root, children, &checked_dirs, &dir_count));
    n += dir_count;
  }
  *count = n;
  return Status::OK();
}

TEST_F(FsManagerTestBase, TestCreateWithFailedDirs) {
  string wal_path = GetTestPath("wals");
  // Create some top-level paths to place roots in.
  vector<string> data_paths = { GetTestPath("data1"), GetTestPath("data2"), GetTestPath("data3") };
  for (const string& path : data_paths) {
    env_->CreateDir(path);
  }
  // Initialize the FS layout with roots in subdirectories of data_paths. When
  // we canonicalize paths, we canonicalize the dirname of each path (e.g.
  // data1) to ensure it exists. With this, we can inject failures in
  // canonicalization by failing the dirname.
  vector<string> data_roots = JoinPathSegmentsV(data_paths, "root");

  FLAGS_crash_on_eio = false;
  FLAGS_env_inject_eio = 1.0;

  // Fail a directory, avoiding the metadata directory.
  FLAGS_env_inject_eio_globs = data_paths[1];
  ReinitFsManagerWithPaths(wal_path, data_roots);
  Status s = fs_manager()->CreateInitialFileSystemLayout();
  ASSERT_STR_MATCHES(s.ToString(), "cannot create FS layout; at least one directory "
                                   "failed to canonicalize");
}

TEST_F(FsManagerTestBase, TestOpenWithNoBlockManagerInstances) {
  // Open a healthy FS layout, sharing the WAL directory with a data directory.
  const string wal_path = GetTestPath("wals");
  FsManagerOpts opts;
  opts.wal_root = wal_path;
  ReinitFsManagerWithOpts(std::move(opts));
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
  ASSERT_OK(fs_manager()->Open());

  // Now try moving the data directory out of WAL directory.
  // Even if we're not enforcing consistency, we must be able to find an
  // existing block manager instance to open the FsManager successfully.
  for (auto check_behavior : { ConsistencyCheckBehavior::IGNORE_INCONSISTENCY,
                               ConsistencyCheckBehavior::UPDATE_ON_DISK }) {
    FsManagerOpts new_opts;
    new_opts.wal_root = wal_path;
    new_opts.data_roots = { GetTestPath("data") };
    new_opts.consistency_check = check_behavior;
    ReinitFsManagerWithOpts(new_opts);
    Status s = fs_manager()->Open();
    ASSERT_STR_CONTAINS(s.ToString(), "no healthy data directories found");
    ASSERT_TRUE(s.IsNotFound());

    // Once we supply the WAL directory as a data directory, we can open successfully.
    new_opts.data_roots.emplace_back(wal_path);
    ReinitFsManagerWithOpts(std::move(new_opts));
    ASSERT_OK(fs_manager()->Open());
  }
}

// Test the behavior when we fail to open a data directory for some reason (its
// mountpoint failed, it's missing, etc). Kudu should allow this and open up
// with failed data directories listed.
TEST_F(FsManagerTestBase, TestOpenWithUnhealthyDataDir) {
  // Successfully create a multi-directory FS layout.
  const string new_root = GetTestPath("new_root");
  FsManagerOpts opts;
  opts.wal_root = fs_root_;
  opts.data_roots = { fs_root_, new_root };
  opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  string new_root_uuid;
  ASSERT_TRUE(fs_manager()->dd_manager()->FindUuidByRoot(new_root, &new_root_uuid));

  // Fail the new directory. Kudu should have no problem starting up with this
  // and should list one as failed.
  FLAGS_env_inject_eio_globs = JoinPathSegments(new_root, "**");
  FLAGS_env_inject_eio = 1.0;
  opts.consistency_check = ConsistencyCheckBehavior::ENFORCE_CONSISTENCY;
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(1, fs_manager()->dd_manager()->GetFailedDataDirs().size());

  // Now remove the new directory on disk. Similarly, Kudu should have no
  // problem starting up and it should list one failed data directory.
  FLAGS_env_inject_eio = 0;
  ASSERT_OK(env_->DeleteRecursively(new_root));
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(1, fs_manager()->dd_manager()->GetFailedDataDirs().size());

  // Now let's simulate the operator replacing the drive. The update tool will
  // be run and the new directory, even at the same mountpoint, will be
  // assigned a new UUID.
  //
  // At this point, our remaining healthy instance file should know about two
  // data directories. Kudu should detect one missing and create a new one.
  // Let's update and ensure we get a new UUID.
  opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(0, fs_manager()->dd_manager()->GetFailedDataDirs().size());
  string new_root_uuid_post_update;
  ASSERT_TRUE(fs_manager()->dd_manager()->FindUuidByRoot(new_root, &new_root_uuid_post_update));
  ASSERT_NE(new_root_uuid, new_root_uuid_post_update);

  // Now let's try failing all the directories. Kudu should yield an error,
  // complaining it couldn't find any healthy data directories.
  FLAGS_env_inject_eio_globs = JoinStrings(JoinPathSegmentsV(opts.data_roots, "**"), ",");
  FLAGS_env_inject_eio = 1.0;
  opts.consistency_check = ConsistencyCheckBehavior::ENFORCE_CONSISTENCY;
  ReinitFsManagerWithOpts(opts);
  Status s = fs_manager()->Open();
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not find a healthy instance file");

  // Upon returning from FsManager::Open() with a NotFound error, Kudu will
  // attempt to create a new FS layout. With bad mountpoints, this should fail.
  s = fs_manager()->CreateInitialFileSystemLayout();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "cannot create FS layout");

  // The above behavior should be seen if the data directories are missing...
  FLAGS_env_inject_eio = 0;
  for (const auto& root : opts.data_roots) {
    ASSERT_OK(env_->DeleteRecursively(root));
  }
  ReinitFsManagerWithOpts(opts);
  s = fs_manager()->Open();
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not find a healthy instance file");

  // ...except we should be able to successfully create a new FS layout.
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
  ASSERT_EQ(0, fs_manager()->dd_manager()->GetFailedDataDirs().size());
}

// When we canonicalize a directory, we actually canonicalize the directory's
// parent directory; as such, canonicalization can fail if the parent directory
// can't be read (e.g. due to a disk error or because it's flat out missing).
// In such cases, we should still be able to open the FS layout.
TEST_F(FsManagerTestBase, TestOpenWithCanonicalizationFailure) {
  // Create some parent directories and subdirectories.
  const string dir1 = GetTestPath("test1");
  const string dir2 = GetTestPath("test2");
  ASSERT_OK(env_->CreateDir(dir1));
  ASSERT_OK(env_->CreateDir(dir2));
  const string subdir1 = GetTestPath("test1/subdir");
  const string subdir2 = GetTestPath("test2/subdir");
  FsManagerOpts opts;
  opts.wal_root = subdir1;
  opts.data_roots = { subdir1, subdir2 };
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());

  // Fail the canonicalization by injecting errors to a parent directory.
  ReinitFsManagerWithOpts(opts);
  FLAGS_env_inject_eio_globs = JoinPathSegments(dir2, "**");
  FLAGS_env_inject_eio = 1.0;
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(1, fs_manager()->dd_manager()->GetFailedDataDirs().size());
  FLAGS_env_inject_eio = 0;

  // Now fail the canonicalization by deleting a parent directory. This
  // simulates the mountpoint disappearing.
  ASSERT_OK(env_->DeleteRecursively(dir2));
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(1, fs_manager()->dd_manager()->GetFailedDataDirs().size());

  // In both of the above failures, the appropriate steps would be to run the
  // update tool after ensuring the bad mountpoint is replaced with a healthy
  // one. Until that happens, we won't be able to update the data dirs.
  opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
  ReinitFsManagerWithOpts(opts);
  Status s = fs_manager()->Open();
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "could not add new data directories");

  // Let's try that again, but with the appropriate mountpoint/directory.
  ASSERT_OK(env_->CreateDir(dir2));
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(0, fs_manager()->dd_manager()->GetFailedDataDirs().size());
}

TEST_F(FsManagerTestBase, TestTmpFilesCleanup) {
  string wal_path = GetTestPath("wals");
  vector<string> data_paths = { GetTestPath("data1"), GetTestPath("data2"), GetTestPath("data3") };
  ReinitFsManagerWithPaths(wal_path, data_paths);
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

  int n_tmp_files = 0;
  ASSERT_OK(CountTmpFiles(fs_manager()->env(), lookup_dirs, &n_tmp_files));
  ASSERT_EQ(6, n_tmp_files);

  // The FsManager should not delete any tmp files if it fails to acquire
  // a lock on the data dir.
  string bm_instance = JoinPathSegments(fs_manager()->GetDataRootDirs()[1],
                                        "block_manager_instance");
  {
    gflags::FlagSaver saver;
    FLAGS_env_inject_lock_failure_globs = bm_instance;
    ReinitFsManagerWithPaths(wal_path, data_paths);
    Status s = fs_manager()->Open();
    ASSERT_STR_MATCHES(s.ToString(), "Could not lock.*");
    ASSERT_OK(CountTmpFiles(fs_manager()->env(), lookup_dirs, &n_tmp_files));
    ASSERT_EQ(6, n_tmp_files);
  }

  // Now start up without the injected lock failure, and ensure that tmp files are deleted.
  ReinitFsManagerWithPaths(wal_path, data_paths);
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
  ReinitFsManagerWithPaths(root, { root });
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
  EXPECT_EQ("770", FilePermsAsString(root));
  EXPECT_EQ("770", FilePermsAsString(fs_manager()->GetConsensusMetadataDir()));
  EXPECT_EQ("660", FilePermsAsString(fs_manager()->GetInstanceMetadataPath(root)));

  // If we change the umask back to being restrictive and re-open the filesystem,
  // the permissions on the root dir should be fixed accordingly.
  FLAGS_umask = "077";
  HandleCommonFlags();
  ASSERT_EQ(077, g_parsed_umask);
  ReinitFsManagerWithPaths(root, { root });
  ASSERT_OK(fs_manager()->Open());
  EXPECT_EQ("700", FilePermsAsString(root));
}

TEST_F(FsManagerTestBase, TestOpenFailsWhenMissingImportantDir) {
  const string kWalRoot = fs_manager()->GetWalsRootDir();

  ASSERT_OK(env_->DeleteDir(kWalRoot));
  ReinitFsManager();
  Status s = fs_manager()->Open();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "could not verify required directory");

  unique_ptr<WritableFile> f;
  ASSERT_OK(env_->NewWritableFile(kWalRoot, &f));
  s = fs_manager()->Open();
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_STR_CONTAINS(s.ToString(), "exists but is not a directory");
}


TEST_F(FsManagerTestBase, TestAddRemoveDataDirs) {
  if (FLAGS_block_manager == "file") {
    LOG(INFO) << "Skipping test, file block manager not supported";
    return;
  }

  // Try to open with a new data dir in the list to be opened; this should fail.
  const string new_path1 = GetTestPath("new_path1");
  FsManagerOpts opts;
  opts.wal_root = fs_root_;
  opts.data_roots = { fs_root_, new_path1 };
  ReinitFsManagerWithOpts(opts);
  Status s = fs_manager()->Open();
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "2 data directories provided, but expected 1");

  // This time allow new data directories to be created.
  opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(2, fs_manager()->dd_manager()->GetDataDirs().size());

  // Try to open with a data dir removed; this should fail.
  opts.data_roots = { fs_root_ };
  opts.consistency_check = ConsistencyCheckBehavior::ENFORCE_CONSISTENCY;
  ReinitFsManagerWithOpts(opts);
  s = fs_manager()->Open();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STR_CONTAINS(s.ToString(), "could not verify integrity of files");

  // This time allow data directories to be removed.
  opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(1, fs_manager()->dd_manager()->GetDataDirs().size());

  // We should be able to add new directories anywhere in the list.
  const string new_path2 = GetTestPath("new_path2");
  opts.data_roots = { new_path2, fs_root_ };
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(2, fs_manager()->dd_manager()->GetDataDirs().size());
  ASSERT_EQ(0, fs_manager()->dd_manager()->GetFailedDataDirs().size());

  // Open the FS layout with an existing, failed data dir; this should be fine,
  // but should report a single failed directory.
  FLAGS_crash_on_eio = false;
  FLAGS_env_inject_eio = 1.0;
  FLAGS_env_inject_eio_globs = JoinPathSegments(new_path2, "**");
  opts.consistency_check = ConsistencyCheckBehavior::ENFORCE_CONSISTENCY;
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(1, fs_manager()->dd_manager()->GetFailedDataDirs().size());

  // Now try to add a new data dir with an existing, failed data dir; this
  // should fail.
  const string new_path3 = GetTestPath("new_path3");
  opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
  opts.data_roots = { fs_root_, new_path2, new_path3 };
  ReinitFsManagerWithOpts(opts);
  s = fs_manager()->Open();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_STR_CONTAINS(s.ToString(), "found failed data directory");
}

TEST_F(FsManagerTestBase, TestReAddRemovedDataDir) {
  if (FLAGS_block_manager == "file") {
    LOG(INFO) << "Skipping test, file block manager not supported";
    return;
  }

  // Add a new data directory, remove it, and add it back.
  const string new_path1 = GetTestPath("new_path1");
  for (const auto& data_roots : vector<vector<string>>({{ fs_root_, new_path1 },
                                                        { fs_root_ },
                                                        { fs_root_, new_path1 }})) {
    FsManagerOpts opts;
    opts.wal_root = fs_root_;
    opts.data_roots = data_roots;
    opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
    ReinitFsManagerWithOpts(std::move(opts));
    ASSERT_OK(fs_manager()->Open());
    ASSERT_EQ(data_roots.size(), fs_manager()->dd_manager()->GetDataDirs().size());
  }
}

TEST_F(FsManagerTestBase, TestCannotRemoveDataDirServingAsMetadataDir) {
  if (FLAGS_block_manager == "file") {
    LOG(INFO) << "Skipping test, file block manager not supported";
    return;
  }

  // Create a new fs layout with a metadata root explicitly set to the first
  // data root.
  ASSERT_OK(env_->DeleteRecursively(fs_root_));
  ASSERT_OK(env_->CreateDir(fs_root_));

  FsManagerOpts opts;
  opts.data_roots = { JoinPathSegments(fs_root_, "data1"),
                      JoinPathSegments(fs_root_, "data2") };
  opts.metadata_root = opts.data_roots[0];
  opts.wal_root = JoinPathSegments(fs_root_, "wal");
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
  ASSERT_OK(fs_manager()->Open());

  // Stop specifying the metadata root. The FsManager will automatically look
  // for and find it in the first data root.
  opts.metadata_root.clear();
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());

  // Now try to remove the first data root. This should fail because in the
  // absence of a defined metadata root, the FsManager will try looking for it
  // in the wal root (not found), and the first data dir (not found).
  opts.data_roots = { opts.data_roots[1] };
  opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
  ReinitFsManagerWithOpts(opts);
  Status s = fs_manager()->Open();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "could not verify required directory");
}

TEST_F(FsManagerTestBase, TestAddRemoveSpeculative) {
  if (FLAGS_block_manager == "file") {
    LOG(INFO) << "Skipping test, file block manager not supported";
    return;
  }

  // Add a second data directory.
  const string new_path1 = GetTestPath("new_path1");
  FsManagerOpts opts;
  opts.wal_root = fs_root_;
  opts.data_roots = { fs_root_, new_path1 };
  opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(2, fs_manager()->dd_manager()->GetDataDirs().size());

  // Create a 'speculative' FsManager with the second data directory removed.
  opts.data_roots = { fs_root_ };
  opts.consistency_check = ConsistencyCheckBehavior::IGNORE_INCONSISTENCY;
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(1, fs_manager()->dd_manager()->GetDataDirs().size());

  // Do the same thing, but with a new data directory added.
  const string new_path2 = GetTestPath("new_path2");
  opts.data_roots = { fs_root_, new_path1, new_path2 };
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->Open());
  ASSERT_EQ(3, fs_manager()->dd_manager()->GetDataDirs().size());
  ASSERT_EQ(1, fs_manager()->dd_manager()->GetFailedDataDirs().size());

  // Neither of those attempts should have changed the on-disk state. Verify
  // this by retrying all three combinations with consistency checking
  // re-enabled. Only the two-directory case should pass.
  opts.consistency_check = ConsistencyCheckBehavior::ENFORCE_CONSISTENCY;
  for (const auto& data_roots : vector<vector<string>>({{ fs_root_ },
                                                        { fs_root_, new_path1 },
                                                        { fs_root_, new_path1, new_path2 }})) {
    opts.data_roots = data_roots;
    ReinitFsManagerWithOpts(opts);
    Status s = fs_manager()->Open();
    if (data_roots.size() == 1) {
      // The first data directory's path set refers to a data directory that
      // wasn't in data_roots.
      ASSERT_TRUE(s.IsIOError()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "could not verify integrity of files");
    } else if (data_roots.size() == 2) {
      ASSERT_OK(s);
      ASSERT_EQ(2, fs_manager()->dd_manager()->GetDataDirs().size());
    } else {
      // The third data directory has no instance file.
      ASSERT_TRUE(s.IsIOError()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "3 data directories provided, but expected 2");
    }
  }
}

TEST_F(FsManagerTestBase, TestAddRemoveDataDirsFuzz) {
  const int kNumAttempts = AllowSlowTests() ? 1000 : 100;

  if (FLAGS_block_manager == "file") {
    LOG(INFO) << "Skipping test, file block manager not supported";
    return;
  }

  Random rng_(SeedRandom());

  FsManagerOpts fs_opts;
  fs_opts.wal_root = fs_root_;
  fs_opts.data_roots = { fs_root_ };
  for (int i = 0; i < kNumAttempts; i++) {
    // Randomly create a directory to add, or choose an existing one to remove.
    //
    // Note: we skip removing the last data directory because the FsManager
    // treats that as a signal to use the wal root as the sole data root.
    vector<string> old_data_roots = fs_opts.data_roots;
    bool action_was_add;
    string fs_root;
    if (rng_.Uniform(2) == 0 || fs_opts.data_roots.size() == 1) {
      action_was_add = true;
      fs_root = GetTestPath(Substitute("new_data_$0", i));
      fs_opts.data_roots.emplace_back(fs_root);
    } else {
      action_was_add = false;
      DCHECK_GT(fs_opts.data_roots.size(), 1);
      int removed_idx = rng_.Uniform(fs_opts.data_roots.size());
      fs_root = fs_opts.data_roots[removed_idx];
      auto iter = fs_opts.data_roots.begin();
      std::advance(iter, removed_idx);
      fs_opts.data_roots.erase(iter);
    }

    // Try to add or remove it with failure injection enabled.
    LOG(INFO) << Substitute("$0ing $1", action_was_add ? "Add" : "Remov", fs_root);
    bool update_succeeded;
    {
      gflags::FlagSaver saver;
      FLAGS_crash_on_eio = false;

      // This value isn't arbitrary: most attempts fail and only some succeed.
      FLAGS_env_inject_eio = 0.01;

      fs_opts.consistency_check = ConsistencyCheckBehavior::UPDATE_ON_DISK;
      ReinitFsManagerWithOpts(fs_opts);
      update_succeeded = fs_manager()->Open().ok();
    }

    // Reopen regardless, to ensure that failures didn't corrupt anything.
    fs_opts.consistency_check = ConsistencyCheckBehavior::ENFORCE_CONSISTENCY;
    ReinitFsManagerWithOpts(fs_opts);
    Status open_status = fs_manager()->Open();
    if (update_succeeded) {
      ASSERT_OK(open_status);
    }

    // The rollback logic built into the update operation isn't robust enough
    // to handle every possible sequence of injected failures. Let's see if we
    // need to apply a "workaround" in order to fix the filesystem.

    if (!open_status.ok()) {
      // Perhaps a new fs root and data directory were created, but there was
      // an injected failure later on, which led to a rollback and the removal
      // of the new fs instance file.
      //
      // Fix this as a user might (by copying the original fs instance file
      // into the new fs root) then retry.
      string source_instance = fs_manager()->GetInstanceMetadataPath(fs_root_);
      bool is_dir;
      Status s = env_->IsDirectory(fs_root, &is_dir);
      if (s.ok()) {
        ASSERT_TRUE(is_dir);
        string new_instance = fs_manager()->GetInstanceMetadataPath(fs_root);
        if (!env_->FileExists(new_instance)) {
          WritableFileOptions wr_opts;
          wr_opts.mode = Env::CREATE_NON_EXISTING;
          ASSERT_OK(env_util::CopyFile(env_, source_instance, new_instance, wr_opts));
          ReinitFsManagerWithOpts(fs_opts);
          open_status = fs_manager()->Open();
        }
      }
    }
    if (!open_status.ok()) {
      // Still failing? Unfortunately, there's not enough information to know
      // whether the injected failure occurred during the update or just
      // afterwards, when the DataDirManager reloaded the data directory
      // instance files. If the former, the failure should resolve itself if we
      // restore the old data roots.
      fs_opts.data_roots = old_data_roots;
      ReinitFsManagerWithOpts(fs_opts);
      open_status = fs_manager()->Open();
    }
    if (!open_status.ok()) {
      // We're still failing? Okay, there's only one legitimate case left, and
      // that's if an error was injected during the update of existing data
      // directory instance files AND during the restoration phase of cleanup.
      //
      // Fix this as a user might (by completing the restoration phase
      // manually), then retry.
      ASSERT_TRUE(open_status.IsIOError());
      bool repaired = false;
      for (const auto& root : fs_opts.data_roots) {
        string data_dir = JoinPathSegments(root, fs::kDataDirName);
        string instance = JoinPathSegments(data_dir,
                                           fs::kInstanceMetadataFileName);
        ASSERT_TRUE(env_->FileExists(instance));
        string copy = instance + kTmpInfix;
        if (env_->FileExists(copy)) {
          ASSERT_OK(env_->RenameFile(copy, instance));
          repaired = true;
        }
      }
      if (repaired) {
        ReinitFsManagerWithOpts(fs_opts);
        open_status = fs_manager()->Open();
      }
    }

    // We've exhausted all of our manual repair options; if this still fails,
    // something else is wrong.
    ASSERT_OK(open_status);
  }
}

TEST_F(FsManagerTestBase, TestAncillaryDirsReported) {
  FsManagerOpts opts;
  opts.wal_root = GetTestPath("wal");
  opts.data_roots = { GetTestPath("data") };
  opts.metadata_root = GetTestPath("metadata");
  ReinitFsManagerWithOpts(opts);
  ASSERT_OK(fs_manager()->CreateInitialFileSystemLayout());
  fs::FsReport report;
  ASSERT_OK(fs_manager()->Open(&report));
  string report_str = report.ToString();
  ASSERT_STR_CONTAINS(report_str, "wal directory: " + opts.wal_root);
  ASSERT_STR_CONTAINS(report_str, "metadata directory: " + opts.metadata_root);
}

} // namespace kudu
