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

#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <set>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

namespace kudu {
namespace itest {

using std::set;
using std::string;
using std::vector;

using consensus::ConsensusMetadataPB;
using strings::Substitute;
using tablet::TabletDataState;
using tablet::TabletSuperBlockPB;

ExternalMiniClusterFsInspector::ExternalMiniClusterFsInspector(ExternalMiniCluster* cluster)
    : env_(Env::Default()),
      cluster_(CHECK_NOTNULL(cluster)) {
}

ExternalMiniClusterFsInspector::~ExternalMiniClusterFsInspector() {}

Status ExternalMiniClusterFsInspector::ListFilesInDir(const string& path,
                                                      vector<string>* entries) {
  RETURN_NOT_OK(env_->GetChildren(path, entries));
  auto iter = entries->begin();
  while (iter != entries->end()) {
    if (*iter == "." || *iter == ".." || iter->find(".tmp.") != string::npos) {
      iter = entries->erase(iter);
      continue;
    }
    ++iter;
  }
  return Status::OK();
}

int ExternalMiniClusterFsInspector::CountFilesInDir(const string& path,
                                                    StringPiece pattern) {
  vector<string> entries;
  Status s = ListFilesInDir(path, &entries);
  if (!s.ok()) return 0;
  return std::count_if(entries.begin(), entries.end(), [&](const string& s) {
      return pattern.empty() || MatchPattern(s, pattern);
    });
}

int ExternalMiniClusterFsInspector::CountWALFilesOnTS(int index) {
  string data_dir = cluster_->tablet_server(index)->data_dir();
  string ts_wal_dir = JoinPathSegments(data_dir, FsManager::kWalDirName);
  vector<string> tablets;
  CHECK_OK(ListFilesInDir(ts_wal_dir, &tablets));
  int total_segments = 0;
  for (const string& tablet : tablets) {
    string tablet_wal_dir = JoinPathSegments(ts_wal_dir, tablet);
    total_segments += CountFilesInDir(tablet_wal_dir);
  }
  return total_segments;
}

vector<string> ExternalMiniClusterFsInspector::ListTablets() {
  set<string> tablets;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    auto ts_tablets = ListTabletsOnTS(i);
    tablets.insert(ts_tablets.begin(), ts_tablets.end());
  }
  return vector<string>(tablets.begin(), tablets.end());
}

vector<string> ExternalMiniClusterFsInspector::ListTabletsOnTS(int index) {
  string data_dir = cluster_->tablet_server(index)->data_dir();
  string meta_dir = JoinPathSegments(data_dir, FsManager::kTabletMetadataDirName);
  vector<string> tablets;
  CHECK_OK(ListFilesInDir(meta_dir, &tablets));
  return tablets;
}

vector<string> ExternalMiniClusterFsInspector::ListTabletsWithDataOnTS(int index) {
  string data_dir = cluster_->tablet_server(index)->data_dir();
  string wal_dir = JoinPathSegments(data_dir, FsManager::kWalDirName);
  vector<string> tablets;
  CHECK_OK(ListFilesInDir(wal_dir, &tablets));
  return tablets;
}

int ExternalMiniClusterFsInspector::CountFilesInWALDirForTS(
    int index,
    const string& tablet_id,
    StringPiece pattern) {
  string data_dir = cluster_->tablet_server(index)->data_dir();
  string wal_dir = JoinPathSegments(data_dir, FsManager::kWalDirName);
  string tablet_wal_dir = JoinPathSegments(wal_dir, tablet_id);
  if (!env_->FileExists(tablet_wal_dir)) {
    return 0;
  }
  return CountFilesInDir(tablet_wal_dir, pattern);
}

bool ExternalMiniClusterFsInspector::DoesConsensusMetaExistForTabletOnTS(int index,
                                                                         const string& tablet_id) {
  ConsensusMetadataPB cmeta_pb;
  Status s = ReadConsensusMetadataOnTS(index, tablet_id, &cmeta_pb);
  return s.ok();
}

int ExternalMiniClusterFsInspector::CountReplicasInMetadataDirs() {
  // Rather than using FsManager's functionality for listing blocks, we just manually
  // list the contents of the metadata directory. This is because we're using an
  // external minicluster, and initializing a new FsManager to point at the running
  // tablet servers isn't easy.
  int count = 0;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    string data_dir = cluster_->tablet_server(i)->data_dir();
    count += CountFilesInDir(JoinPathSegments(data_dir, FsManager::kTabletMetadataDirName));
  }
  return count;
}

Status ExternalMiniClusterFsInspector::CheckNoDataOnTS(int index) {
  string data_dir = cluster_->tablet_server(index)->data_dir();
  if (CountFilesInDir(JoinPathSegments(data_dir, FsManager::kTabletMetadataDirName)) > 0) {
    return Status::IllegalState("tablet metadata blocks still exist", data_dir);
  }
  if (CountWALFilesOnTS(index) > 0) {
    return Status::IllegalState("wals still exist", data_dir);
  }
  if (CountFilesInDir(JoinPathSegments(data_dir, FsManager::kConsensusMetadataDirName)) > 0) {
    return Status::IllegalState("consensus metadata still exists", data_dir);
  }
  return Status::OK();;
}

Status ExternalMiniClusterFsInspector::CheckNoData() {
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    RETURN_NOT_OK(CheckNoDataOnTS(i));
  }
  return Status::OK();;
}

string ExternalMiniClusterFsInspector::GetTabletSuperBlockPathOnTS(int ts_index,
                                                                   const string& tablet_id) const {
  string data_dir = cluster_->tablet_server(ts_index)->data_dir();
  string meta_dir = JoinPathSegments(data_dir, FsManager::kTabletMetadataDirName);
  return JoinPathSegments(meta_dir, tablet_id);
}

Status ExternalMiniClusterFsInspector::ReadTabletSuperBlockOnTS(int index,
                                                                const string& tablet_id,
                                                                TabletSuperBlockPB* sb) {
  const auto& sb_path = GetTabletSuperBlockPathOnTS(index, tablet_id);
  return pb_util::ReadPBContainerFromPath(env_, sb_path, sb);
}

int64_t ExternalMiniClusterFsInspector::GetTabletSuperBlockMTimeOrDie(
    int ts_index, const std::string& tablet_id) {
  const auto& sb_path = GetTabletSuperBlockPathOnTS(ts_index, tablet_id);
  struct stat s;
  CHECK_ERR(stat(sb_path.c_str(), &s)) << "failed to stat: " << sb_path;
  return s.st_mtim.tv_sec * 1e6 + s.st_mtim.tv_nsec / 1000;
}

string ExternalMiniClusterFsInspector::GetConsensusMetadataPathOnTS(int index,
                                                                    const string& tablet_id) const {
  string data_dir = cluster_->tablet_server(index)->data_dir();
  string cmeta_dir = JoinPathSegments(data_dir, FsManager::kConsensusMetadataDirName);
  return JoinPathSegments(cmeta_dir, tablet_id);
}

Status ExternalMiniClusterFsInspector::ReadConsensusMetadataOnTS(int index,
                                                                 const string& tablet_id,
                                                                 ConsensusMetadataPB* cmeta_pb) {
  auto cmeta_path = GetConsensusMetadataPathOnTS(index, tablet_id);
  if (!env_->FileExists(cmeta_path)) {
    return Status::NotFound("Consensus metadata file not found", cmeta_path);
  }
  return pb_util::ReadPBContainerFromPath(env_, cmeta_path, cmeta_pb);
}

Status ExternalMiniClusterFsInspector::WriteConsensusMetadataOnTS(
    int index,
    const string& tablet_id,
    const ConsensusMetadataPB& cmeta_pb) {
  auto cmeta_path = GetConsensusMetadataPathOnTS(index, tablet_id);
  return pb_util::WritePBContainerToPath(env_, cmeta_path, cmeta_pb,
                                         pb_util::OVERWRITE, pb_util::NO_SYNC);
}


Status ExternalMiniClusterFsInspector::CheckTabletDataStateOnTS(
    int index,
    const string& tablet_id,
    const vector<TabletDataState>& allowed_states) {

  TabletSuperBlockPB sb;
  RETURN_NOT_OK(ReadTabletSuperBlockOnTS(index, tablet_id, &sb));
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

Status ExternalMiniClusterFsInspector::WaitForNoData(const MonoDelta& timeout) {
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

Status ExternalMiniClusterFsInspector::WaitForNoDataOnTS(int index, const MonoDelta& timeout) {
  MonoTime deadline = MonoTime::Now() + timeout;
  Status s;
  while (true) {
    s = CheckNoDataOnTS(index);
    if (s.ok()) return Status::OK();
    if (deadline < MonoTime::Now()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  return Status::TimedOut("Timed out waiting for no data", s.ToString());
}

Status ExternalMiniClusterFsInspector::WaitForMinFilesInTabletWalDirOnTS(int index,
                                                                         const string& tablet_id,
                                                                         int count,
                                                                         const MonoDelta& timeout) {
  int seen = 0;
  MonoTime deadline = MonoTime::Now() + timeout;
  while (true) {
    seen = CountFilesInWALDirForTS(index, tablet_id);
    if (seen >= count) return Status::OK();
    if (deadline < MonoTime::Now()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  return Status::TimedOut(Substitute("Timed out waiting for number of WAL segments on tablet $0 "
                                     "on TS $1 to be $2. Found $3",
                                     tablet_id, index, count, seen));
}

Status ExternalMiniClusterFsInspector::WaitForReplicaCount(int expected, const MonoDelta& timeout) {
  Status s;
  MonoTime deadline = MonoTime::Now() + timeout;
  int found;
  while (true) {
    found = CountReplicasInMetadataDirs();
    if (found == expected) return Status::OK();
    if (CountReplicasInMetadataDirs() == expected) return Status::OK();
    if (MonoTime::Now() > deadline) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  return Status::TimedOut(Substitute("Timed out waiting for a total replica count of $0. "
                                     "Found $2 replicas",
                                     expected, found));
}

Status ExternalMiniClusterFsInspector::WaitForTabletDataStateOnTS(
    int index,
    const string& tablet_id,
    const vector<TabletDataState>& expected_states,
    const MonoDelta& timeout) {
  MonoTime start = MonoTime::Now();
  MonoTime deadline = start + timeout;
  Status s;
  while (true) {
    s = CheckTabletDataStateOnTS(index, tablet_id, expected_states);
    if (s.ok()) return Status::OK();
    if (MonoTime::Now() > deadline) break;
    SleepFor(MonoDelta::FromMilliseconds(5));
  }
  return Status::TimedOut(Substitute("Timed out after $0 waiting for correct tablet state: $1",
                                     (MonoTime::Now() - start).ToString(),
                                     s.ToString()));
}

Status ExternalMiniClusterFsInspector::WaitForFilePatternInTabletWalDirOnTs(
    int ts_index, const string& tablet_id,
    const vector<string>& substrings_required,
    const vector<string>& substrings_disallowed,
    const MonoDelta& timeout) {
  Status s;
  MonoTime deadline = MonoTime::Now() + timeout;

  string data_dir = cluster_->tablet_server(ts_index)->data_dir();
  string ts_wal_dir = JoinPathSegments(data_dir, FsManager::kWalDirName);
  string tablet_wal_dir = JoinPathSegments(ts_wal_dir, tablet_id);

  string error_msg;
  vector<string> entries;
  while (true) {
    Status s = ListFilesInDir(tablet_wal_dir, &entries);
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
          error_msg += "present from substrings_disallowed: " + entry +
                       " (" + disallowed_filter + "); ";
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
                                     tablet_id, ts_index, tablet_wal_dir),
                          error_msg + "entries: " + JoinStrings(entries, ", "));
}

} // namespace itest
} // namespace kudu

