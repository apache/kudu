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

#include "kudu/integration-tests/mini_cluster_fs_inspector.h"

#include <algorithm>
#include <set>

#include <glog/logging.h>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

namespace kudu {
namespace itest {

using cluster::MiniCluster;
using std::set;
using std::string;
using std::vector;
using consensus::ConsensusMetadataPB;
using env_util::ListFilesInDir;
using strings::Substitute;
using tablet::TabletDataState;
using tablet::TabletSuperBlockPB;

MiniClusterFsInspector::MiniClusterFsInspector(MiniCluster* cluster)
    : cluster_(cluster),
      env_(cluster->env()) {}

int MiniClusterFsInspector::CountFilesInDir(const string& path,
                                            StringPiece pattern) {
  vector<string> entries;
  Status s = ListFilesInDir(env_, path, &entries);
  if (!s.ok()) return 0;
  return std::count_if(entries.begin(), entries.end(), [&](const string& s) {
      return pattern.empty() || MatchPattern(s, pattern);
    });
}

string MiniClusterFsInspector::WalDirForTS(int ts_idx) const {
  return JoinPathSegments(cluster_->WalRootForTS(ts_idx), FsManager::kWalDirName);
}

int MiniClusterFsInspector::CountWALFilesOnTS(int ts_idx) {
  string ts_wal_dir = WalDirForTS(ts_idx);
  vector<string> tablets;
  CHECK_OK(ListFilesInDir(env_, ts_wal_dir, &tablets));
  int total_segments = 0;
  for (const string& tablet : tablets) {
    string tablet_wal_dir = JoinPathSegments(ts_wal_dir, tablet);
    total_segments += CountFilesInDir(tablet_wal_dir);
  }
  return total_segments;
}

vector<string> MiniClusterFsInspector::ListTablets() {
  set<string> tablets;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    auto ts_tablets = ListTabletsOnTS(i);
    tablets.insert(ts_tablets.begin(), ts_tablets.end());
  }
  return vector<string>(tablets.begin(), tablets.end());
}

vector<string> MiniClusterFsInspector::ListTabletsOnTS(int ts_idx) {
  string meta_dir = JoinPathSegments(cluster_->WalRootForTS(ts_idx),
                                     FsManager::kTabletMetadataDirName);
  vector<string> tablets;
  CHECK_OK(ListFilesInDir(env_, meta_dir, &tablets));
  return tablets;
}

vector<string> MiniClusterFsInspector::ListTabletsWithDataOnTS(int ts_idx) {
  vector<string> tablets;
  CHECK_OK(ListFilesInDir(env_, WalDirForTS(ts_idx), &tablets));
  return tablets;
}

int MiniClusterFsInspector::CountFilesInWALDirForTS(
    int ts_idx,
    const string& tablet_id,
    StringPiece pattern) {
  string tablet_wal_dir = JoinPathSegments(WalDirForTS(ts_idx), tablet_id);
  if (!env_->FileExists(tablet_wal_dir)) {
    return 0;
  }
  return CountFilesInDir(tablet_wal_dir, pattern);
}

bool MiniClusterFsInspector::DoesConsensusMetaExistForTabletOnTS(int ts_idx,
                                                                 const string& tablet_id) {
  ConsensusMetadataPB cmeta_pb;
  Status s = ReadConsensusMetadataOnTS(ts_idx, tablet_id, &cmeta_pb);
  return s.ok();
}

int MiniClusterFsInspector::CountReplicasInMetadataDirs() {
  // Rather than using FsManager's functionality for listing blocks, we just manually
  // list the contents of the metadata directory. This is because we're using an
  // external minicluster, and initializing a new FsManager to point at the running
  // tablet servers isn't easy.
  int count = 0;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    count += CountFilesInDir(JoinPathSegments(cluster_->WalRootForTS(i),
                                              FsManager::kTabletMetadataDirName));
  }
  return count;
}

Status MiniClusterFsInspector::CheckNoDataOnTS(int ts_idx) {
  const string& wal_root = cluster_->WalRootForTS(ts_idx);
  if (CountFilesInDir(JoinPathSegments(wal_root, FsManager::kTabletMetadataDirName)) > 0) {
    return Status::IllegalState("tablet metadata blocks still exist", wal_root);
  }
  if (CountWALFilesOnTS(ts_idx) > 0) {
    return Status::IllegalState("wals still exist", wal_root);
  }
  if (CountFilesInDir(JoinPathSegments(wal_root, FsManager::kConsensusMetadataDirName)) > 0) {
    return Status::IllegalState("consensus metadata still exists", wal_root);
  }
  return Status::OK();
}

Status MiniClusterFsInspector::CheckNoData() {
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    RETURN_NOT_OK(CheckNoDataOnTS(i));
  }
  return Status::OK();
}

string MiniClusterFsInspector::GetTabletSuperBlockPathOnTS(int ts_idx,
                                                           const string& tablet_id) const {
  string meta_dir = JoinPathSegments(cluster_->WalRootForTS(ts_idx),
                                     FsManager::kTabletMetadataDirName);
  return JoinPathSegments(meta_dir, tablet_id);
}

Status MiniClusterFsInspector::ReadTabletSuperBlockOnTS(int ts_idx,
                                                        const string& tablet_id,
                                                        TabletSuperBlockPB* sb) {
  const auto& sb_path = GetTabletSuperBlockPathOnTS(ts_idx, tablet_id);
  return pb_util::ReadPBContainerFromPath(env_, sb_path, sb);
}

int64_t MiniClusterFsInspector::GetTabletSuperBlockMTimeOrDie(int ts_idx,
                                                              const string& tablet_id) {
  int64_t timestamp;
  CHECK_OK(env_->GetFileModifiedTime(
      GetTabletSuperBlockPathOnTS(ts_idx, tablet_id), &timestamp));
  return timestamp;
}

string MiniClusterFsInspector::GetConsensusMetadataPathOnTS(int ts_idx,
                                                            const string& tablet_id) const {
  string wal_root = cluster_->WalRootForTS(ts_idx);
  string cmeta_dir = JoinPathSegments(wal_root, FsManager::kConsensusMetadataDirName);
  return JoinPathSegments(cmeta_dir, tablet_id);
}

Status MiniClusterFsInspector::ReadConsensusMetadataOnTS(int ts_idx,
                                                         const string& tablet_id,
                                                         ConsensusMetadataPB* cmeta_pb) {
  auto cmeta_path = GetConsensusMetadataPathOnTS(ts_idx, tablet_id);
  if (!env_->FileExists(cmeta_path)) {
    return Status::NotFound("Consensus metadata file not found", cmeta_path);
  }
  return pb_util::ReadPBContainerFromPath(env_, cmeta_path, cmeta_pb);
}

Status MiniClusterFsInspector::WriteConsensusMetadataOnTS(
    int ts_idx,
    const string& tablet_id,
    const ConsensusMetadataPB& cmeta_pb) {
  auto cmeta_path = GetConsensusMetadataPathOnTS(ts_idx, tablet_id);
  return pb_util::WritePBContainerToPath(env_, cmeta_path, cmeta_pb,
                                         pb_util::OVERWRITE, pb_util::NO_SYNC);
}


Status MiniClusterFsInspector::CheckTabletDataStateOnTS(
    int ts_idx,
    const string& tablet_id,
    const vector<TabletDataState>& allowed_states) {
  TabletSuperBlockPB sb;
  RETURN_NOT_OK(ReadTabletSuperBlockOnTS(ts_idx, tablet_id, &sb));
  if (std::find(allowed_states.begin(), allowed_states.end(), sb.tablet_data_state()) !=
      allowed_states.end()) {
    return Status::OK();
  }

  vector<string> state_names;
  for (auto state : allowed_states) {
    state_names.push_back(TabletDataState_Name(state));
  }
  string expected_str = JoinStrings(state_names, ",");
  if (state_names.size() > 1) {
    expected_str = "one of: " + expected_str;
  }

  return Status::IllegalState(Substitute("State $0 unexpected, expected $1",
                                         TabletDataState_Name(sb.tablet_data_state()),
                                         expected_str));
}

Status MiniClusterFsInspector::WaitForNoData(const MonoDelta& timeout) {
  MonoTime deadline = MonoTime::Now() + timeout;
  Status s;
  while (true) {
    s = CheckNoData();
    if (s.ok()) return Status::OK();
    if (deadline < MonoTime::Now()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  return Status::TimedOut("Timed out waiting for no data", s.ToString());
}

Status MiniClusterFsInspector::WaitForNoDataOnTS(int ts_idx, const MonoDelta& timeout) {
  MonoTime deadline = MonoTime::Now() + timeout;
  Status s;
  while (true) {
    s = CheckNoDataOnTS(ts_idx);
    if (s.ok()) return Status::OK();
    if (deadline < MonoTime::Now()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  return Status::TimedOut("Timed out waiting for no data", s.ToString());
}

Status MiniClusterFsInspector::WaitForMinFilesInTabletWalDirOnTS(int ts_idx,
                                                                 const string& tablet_id,
                                                                 int count,
                                                                 const MonoDelta& timeout) {
  int seen = 0;
  MonoTime deadline = MonoTime::Now() + timeout;
  while (true) {
    seen = CountFilesInWALDirForTS(ts_idx, tablet_id);
    if (seen >= count) return Status::OK();
    if (deadline < MonoTime::Now()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  return Status::TimedOut(Substitute("Timed out waiting for number of WAL segments on tablet $0 "
                                     "on TS $1 to be $2. Found $3",
                                     tablet_id, ts_idx, count, seen));
}

Status MiniClusterFsInspector::WaitForReplicaCount(int expected, const MonoDelta& timeout) {
  const MonoTime deadline = MonoTime::Now() + timeout;
  int found;
  while (true) {
    found = CountReplicasInMetadataDirs();
    if (found == expected) {
      return Status::OK();
    }
    if (MonoTime::Now() > deadline) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  return Status::TimedOut(
      Substitute("Timed out waiting for a total replica count of $0. "
                 "Found $1 replicas", expected, found));
}

Status MiniClusterFsInspector::WaitForTabletDataStateOnTS(
    int ts_idx,
    const string& tablet_id,
    const vector<TabletDataState>& expected_states,
    const MonoDelta& timeout) {
  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + timeout;
  Status s;
  while (true) {
    s = CheckTabletDataStateOnTS(ts_idx, tablet_id, expected_states);
    if (s.ok()) return Status::OK();
    if (MonoTime::Now() > deadline) break;
    SleepFor(MonoDelta::FromMilliseconds(5));
  }
  return Status::TimedOut(Substitute("Timed out after $0 waiting for correct tablet state: $1",
                                     (MonoTime::Now() - start).ToString(),
                                     s.ToString()));
}

Status MiniClusterFsInspector::WaitForFilePatternInTabletWalDirOnTs(
    int ts_idx, const string& tablet_id,
    const vector<string>& substrings_required,
    const vector<string>& substrings_disallowed,
    const MonoDelta& timeout) {
  Status s;
  MonoTime deadline = MonoTime::Now() + timeout;

  string tablet_wal_dir = JoinPathSegments(WalDirForTS(ts_idx), tablet_id);

  string error_msg;
  vector<string> entries;
  while (true) {
    Status s = ListFilesInDir(env_, tablet_wal_dir, &entries);
    std::sort(entries.begin(), entries.end());

    error_msg = "";
    bool any_missing_required = false;
    for (const string& required_filter : substrings_required) {
      bool filter_matched = false;
      for (const string& entry : entries) {
        if (entry.find(required_filter) != string::npos) {
          filter_matched = true;
          break;
        }
      }
      if (!filter_matched) {
        any_missing_required = true;
        error_msg += "missing from substrings_required: " + required_filter + "; ";
        break;
      }
    }

    bool any_present_disallowed = false;
    for (const string& entry : entries) {
      if (any_present_disallowed) break;
      for (const string& disallowed_filter : substrings_disallowed) {
        if (entry.find(disallowed_filter) != string::npos) {
          any_present_disallowed = true;
          error_msg += Substitute("present from substrings_disallowed: $0 ($1)",
                                  entry, disallowed_filter);
          break;
        }
      }
    }

    if (!any_missing_required && !any_present_disallowed) {
      return Status::OK();
    }
    if (MonoTime::Now() > deadline) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  return Status::TimedOut(Substitute("Timed out waiting for file pattern on "
                                     "tablet $0 on TS $1 in directory $2",
                                     tablet_id, ts_idx, tablet_wal_dir),
                          error_msg + "entries: " + JoinStrings(entries, ", "));
}

} // namespace itest
} // namespace kudu

