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
#include "kudu/consensus/consensus_meta_manager.h"

#include <mutex>

#include "kudu/consensus/consensus_meta.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/map-util.h"

namespace kudu {
namespace consensus {

using std::lock_guard;
using std::string;

ConsensusMetadataManager::ConsensusMetadataManager(FsManager* fs_manager)
    : fs_manager_(DCHECK_NOTNULL(fs_manager)) {
}

Status ConsensusMetadataManager::Create(const string& tablet_id,
                                        const RaftConfigPB& config,
                                        int64_t current_term,
                                        scoped_refptr<ConsensusMetadata>* cmeta_out) {
  scoped_refptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK(ConsensusMetadata::Create(fs_manager_, tablet_id, fs_manager_->uuid(),
                                          config, current_term, &cmeta));
  lock_guard<Mutex> l(lock_);
  InsertOrDie(&cmeta_cache_, tablet_id, cmeta);
  if (cmeta_out) *cmeta_out = std::move(cmeta);
  return Status::OK();
}

Status ConsensusMetadataManager::Load(const string& tablet_id,
                                      scoped_refptr<ConsensusMetadata>* cmeta_out) {
  DCHECK(cmeta_out);

  {
    lock_guard<Mutex> l(lock_);

    // Try to get the cmeta instance from cache first.
    scoped_refptr<ConsensusMetadata>* cached_cmeta = FindOrNull(cmeta_cache_, tablet_id);
    if (cached_cmeta) {
      *cmeta_out = *cached_cmeta;
      return Status::OK();
    }
  }

  // If it's not yet cached, drop the lock before we load it.
  scoped_refptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK(ConsensusMetadata::Load(fs_manager_, tablet_id, fs_manager_->uuid(), &cmeta));

  // Cache and return the loaded ConsensusMetadata.
  {
    lock_guard<Mutex> l(lock_);
    // Due to our thread-safety contract, no other caller may have interleaved
    // with us for this tablet id, so we use InsertOrDie().
    InsertOrDie(&cmeta_cache_, tablet_id, cmeta);
  }

  *cmeta_out = std::move(cmeta);
  return Status::OK();
}

Status ConsensusMetadataManager::Delete(const string& tablet_id) {
  {
    lock_guard<Mutex> l(lock_);
    cmeta_cache_.erase(tablet_id); // OK to delete an uncached cmeta; ignore the return value.
  }
  return ConsensusMetadata::DeleteOnDiskData(fs_manager_, tablet_id);
}

} // namespace consensus
} // namespace kudu
