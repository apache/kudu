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
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/util/monotime.h"

namespace kudu {

class Env;
class Status;

namespace cluster {
class MiniCluster;
} // namespace cluster

namespace consensus {
class ConsensusMetadataPB;
} // namespace consensus

namespace itest {

// Utility class that digs around in a tablet server's FS layout and provides
// methods useful for integration testing. This class must outlive the Env and
// MiniCluster objects that are passed into it.
class MiniClusterFsInspector {
 public:
  explicit MiniClusterFsInspector(cluster::MiniCluster* cluster);

  ~MiniClusterFsInspector() {}

  // Returns the WALs FS subdirectory created for TS 'ts_idx'.
  std::string WalDirForTS(int ts_idx) const;

  // If provided, files are filtered by the glob-style pattern 'pattern'.
  int CountFilesInDir(const std::string& path, StringPiece pattern = StringPiece());

  // List all of the tablets with tablet metadata in the cluster.
  std::vector<std::string> ListTablets();

  // List all of the tablets with tablet metadata on the given tablet server
  // 'ts_idx'.  This may include tablets that are tombstoned and not running.
  std::vector<std::string> ListTabletsOnTS(int ts_idx);

  // List the tablet IDs on the given tablet which actually have data (as
  // evidenced by their having a WAL). This excludes those that are tombstoned.
  std::vector<std::string> ListTabletsWithDataOnTS(int ts_idx);

  // Returns the number of files in the WAL directory for 'tablet_id' on TS
  // 'ts_idx'. If provided, files are filtered by the glob string 'pattern'.
  int CountFilesInWALDirForTS(int ts_idx,
                              const std::string& tablet_id,
                              StringPiece pattern = StringPiece());

  bool DoesConsensusMetaExistForTabletOnTS(int ts_idx, const std::string& tablet_id);

  int CountReplicasInMetadataDirs();
  Status CheckNoDataOnTS(int ts_idx);
  Status CheckNoData();

  Status ReadTabletSuperBlockOnTS(int ts_idx, const std::string& tablet_id,
                                  tablet::TabletSuperBlockPB* sb);

  // Get the modification time (in micros) of the tablet superblock for the
  // given tablet server ts_idx and tablet ID.
  int64_t GetTabletSuperBlockMTimeOrDie(int ts_idx, const std::string& tablet_id);

  Status ReadConsensusMetadataOnTS(int ts_idx, const std::string& tablet_id,
                                   consensus::ConsensusMetadataPB* cmeta_pb);
  Status WriteConsensusMetadataOnTS(int ts_idx,
                                    const std::string& tablet_id,
                                    const consensus::ConsensusMetadataPB& cmeta_pb);

  Status CheckTabletDataStateOnTS(int ts_idx,
                                  const std::string& tablet_id,
                                  const std::vector<tablet::TabletDataState>& allowed_states);

  Status WaitForNoData(const MonoDelta& timeout = MonoDelta::FromSeconds(30));
  Status WaitForNoDataOnTS(int ts_idx, const MonoDelta& timeout = MonoDelta::FromSeconds(30));
  Status WaitForMinFilesInTabletWalDirOnTS(int ts_idx,
                                           const std::string& tablet_id,
                                           int count,
                                           const MonoDelta& timeout = MonoDelta::FromSeconds(60));
  Status WaitForReplicaCount(int expected, const MonoDelta& timeout = MonoDelta::FromSeconds(30));
  Status WaitForTabletDataStateOnTS(int ts_idx,
                                    const std::string& tablet_id,
                                    const std::vector<tablet::TabletDataState>& expected_states,
                                    const MonoDelta& timeout = MonoDelta::FromSeconds(30));

  // Loop and check for certain filenames in the WAL directory of the specified
  // tablet. This function returns OK if we reach a state where:
  // * For each string in 'substrings_required', we find *at least one file*
  //   whose name contains that string, and:
  // * For each string in 'substrings_disallowed', we find *no files* whose name
  //   contains that string, even if the file also matches a string in the
  //   'substrings_required'.
  Status WaitForFilePatternInTabletWalDirOnTs(
      int ts_idx,
      const std::string& tablet_id,
      const std::vector<std::string>& substrings_required,
      const std::vector<std::string>& substrings_disallowed,
      const MonoDelta& timeout = MonoDelta::FromSeconds(30));

 private:
  const cluster::MiniCluster* const cluster_;
  Env* const env_;

  // Return the number of files in WAL directories on the given tablet server.
  // This includes log ts_idx files (not just segments).
  int CountWALFilesOnTS(int ts_idx);

  std::string GetConsensusMetadataPathOnTS(int ts_idx,
                                           const std::string& tablet_id) const;

  std::string GetTabletSuperBlockPathOnTS(int ts_idx,
                                          const std::string& tablet_id) const;

};

} // namespace itest
} // namespace kudu
