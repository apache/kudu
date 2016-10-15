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
#include <unordered_map>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/mutex.h"

namespace kudu {
class FsManager;
class Status;

namespace consensus {
class ConsensusMetadata;
class RaftConfigPB;

// API and implementation for a consensus metadata "manager" that controls
// access to consensus metadata across a server instance. This abstracts
// the handling of consensus metadata persistence.
//
// A single manager instance can be plumbed throughout the various classes that
// deal with reading, creating, and modifying consensus metadata so that we
// don't have to pass individual consensus metadata instances around. It also
// provides flexibility to change the underlying implementation of
// ConsensusMetadata in the future.
//
// This class is ONLY thread-safe across different tablets. Concurrent access
// to Create(), Load(), or Delete() for the same tablet id is thread-hostile
// and must be externally synchronized. Failure to do so may result in a crash.
class ConsensusMetadataManager : public RefCountedThreadSafe<ConsensusMetadataManager> {
 public:
  explicit ConsensusMetadataManager(FsManager* fs_manager);

  // Create a ConsensusMetadata instance keyed by 'tablet_id'.
  // Returns an error if a ConsensusMetadata instance with that key already exists.
  Status Create(const std::string& tablet_id,
                const RaftConfigPB& config,
                int64_t current_term,
                scoped_refptr<ConsensusMetadata>* cmeta_out = nullptr);

  // Load the ConsensusMetadata instance keyed by 'tablet_id'.
  // Returns an error if it cannot be found.
  Status Load(const std::string& tablet_id,
              scoped_refptr<ConsensusMetadata>* cmeta_out);

  // Permanently delete the ConsensusMetadata instance keyed by 'tablet_id'.
  // Returns Status::NotFound if the instance cannot be found.
  // Returns another error if the cmeta instance exists but cannot be deleted
  // for some reason, perhaps due to a permissions or I/O-related issue.
  Status Delete(const std::string& tablet_id);

 private:
  friend class RefCountedThreadSafe<ConsensusMetadataManager>;

  FsManager* const fs_manager_;

  // Lock protecting the map below.
  Mutex lock_;

  // Cache for ConsensusMetadata objects (tablet_id => cmeta).
  std::unordered_map<std::string, scoped_refptr<ConsensusMetadata>> cmeta_cache_;

  DISALLOW_COPY_AND_ASSIGN(ConsensusMetadataManager);
};

} // namespace consensus
} // namespace kudu
