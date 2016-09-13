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

#include <map>
#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {

class ExternalMiniCluster;
class FsManager;

// Verifies correctness of the logs in an external mini-cluster.
class LogVerifier {
 public:
  explicit LogVerifier(ExternalMiniCluster* cluster);
  ~LogVerifier();

  // Verify that, for every tablet in the cluster, the logs of each of that tablet's replicas
  // have matching committed operations. In other words, if any replica has a log entry
  // 'COMMIT term.index', then verifies that no other replica has a COMMIT entry for the
  // same index with a different term.
  //
  // This is the most basic correctness condition of Raft: all replicas should commit the
  // same operations.
  //
  // NOTE: if the cluster is not shut down, it is possible for this method to fail spuriously
  // trying to read a WAL that is currently being written. In this case, it's advisable to
  // loop and retry on failure.
  Status VerifyCommittedOpIdsMatch();

 private:
  // Scan the WALs for tablet 'tablet_id' on the given 'fs'. Sets entries
  // in '*index_to_term' for each COMMIT entry found in the WALs.
  Status ScanForCommittedOpIds(FsManager* fs, const std::string& tablet_id,
                               std::map<int64_t, int64_t>* index_to_term);

  ExternalMiniCluster* const cluster_;

  DISALLOW_COPY_AND_ASSIGN(LogVerifier);
};

} // namespace kudu
