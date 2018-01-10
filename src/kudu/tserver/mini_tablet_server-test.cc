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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/fs/fs_manager.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/env.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace tserver {

using std::string;
using std::unique_ptr;

class MiniTabletServerTest : public KuduTest {};

TEST_F(MiniTabletServerTest, TestMultiDirServer) {
  // Specifying the number of data directories will create subdirectories under the test root.
  unique_ptr<MiniTabletServer> mini_server;
  FsManager* fs_manager;

  int kNumDataDirs = 3;
  mini_server.reset(new MiniTabletServer(GetTestPath("TServer"),
      HostPort("127.0.0.1", 0), kNumDataDirs));
  ASSERT_OK(mini_server->Start());
  fs_manager = mini_server->server()->fs_manager();
  ASSERT_STR_CONTAINS(DirName(fs_manager->GetWalsRootDir()), "wal");
  ASSERT_EQ(kNumDataDirs, fs_manager->GetDataRootDirs().size());
}

// Test opening the FS layout with metadata in the WAL root, moving it to the
// first data root, and back.
//
// Users may want to move the metadata directory from the current default (the
// WAL root) to the old default (the first data root), or may be using the old
// default and may want to abide by the current default. Both cases are tested.
TEST_F(MiniTabletServerTest, TestFsLayoutEndToEnd) {
  // Use multiple data dirs, otherwise the mini tserver will colocated the WALs
  // and data dir.
  const int kNumDataDirs = 2;
  unique_ptr<MiniTabletServer> mini_server;
  FsManager* fs_manager;
  const auto& reset_mini_tserver = [&] {
    mini_server.reset(new MiniTabletServer(GetTestPath("TServer"),
        HostPort("127.0.0.1", 0), kNumDataDirs));
    ASSERT_OK(mini_server->Start());
    fs_manager = mini_server->server()->fs_manager();
  };
  reset_mini_tserver();

  // By default, the metadata directory will be placed in the WAL root.
  const string& tmeta_dir_in_wal_root = fs_manager->GetTabletMetadataDir();
  const string& cmeta_dir_in_wal_root = fs_manager->GetConsensusMetadataDir();
  ASSERT_STR_CONTAINS(DirName(tmeta_dir_in_wal_root), "wal");
  ASSERT_STR_CONTAINS(DirName(cmeta_dir_in_wal_root), "wal");

  // Now move the metadata directories into the first directory, emulating the
  // behavior in Kudu 1.6 and below.
  const string data_root = DirName(fs_manager->GetDataRootDirs()[0]);
  const string tmeta_dir_in_data_root = JoinPathSegments(
      data_root, FsManager::kTabletMetadataDirName);
  const string cmeta_dir_in_data_root = JoinPathSegments(
      data_root, FsManager::kConsensusMetadataDirName);
  mini_server->Shutdown();
  ASSERT_OK(env_->RenameFile(tmeta_dir_in_wal_root, tmeta_dir_in_data_root));
  ASSERT_OK(env_->RenameFile(cmeta_dir_in_wal_root, cmeta_dir_in_data_root));

  // Upon restarting the server, since metadata directories already exist, Kudu
  // will take them as is.
  reset_mini_tserver();
  ASSERT_EQ(tmeta_dir_in_data_root, fs_manager->GetTabletMetadataDir());
  ASSERT_EQ(cmeta_dir_in_data_root, fs_manager->GetConsensusMetadataDir());
  ASSERT_FALSE(env_->FileExists(tmeta_dir_in_wal_root));
  ASSERT_FALSE(env_->FileExists(cmeta_dir_in_wal_root));

  // Let's move back to the newer default and place metadata in the WAL root.
  mini_server->Shutdown();
  ASSERT_OK(env_->RenameFile(tmeta_dir_in_data_root, tmeta_dir_in_wal_root));
  ASSERT_OK(env_->RenameFile(cmeta_dir_in_data_root, cmeta_dir_in_wal_root));
  reset_mini_tserver();
  ASSERT_EQ(tmeta_dir_in_wal_root, fs_manager->GetTabletMetadataDir());
  ASSERT_EQ(cmeta_dir_in_wal_root, fs_manager->GetConsensusMetadataDir());
  ASSERT_FALSE(env_->FileExists(tmeta_dir_in_data_root));
  ASSERT_FALSE(env_->FileExists(cmeta_dir_in_data_root));
}

}  // namespace tserver
}  // namespace kudu
