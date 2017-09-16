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
#include <utility>

#include <glog/logging.h>

#include "kudu/consensus/consensus_meta.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {

using std::lock_guard;
using std::string;
using strings::Substitute;

ConsensusMetadataManager::ConsensusMetadataManager(FsManager* fs_manager)
    : fs_manager_(DCHECK_NOTNULL(fs_manager)) {
}

Status ConsensusMetadataManager::Create(const string& tablet_id,
                                        const RaftConfigPB& config,
                                        int64_t initial_term,
                                        ConsensusMetadataCreateMode create_mode,
                                        scoped_refptr<ConsensusMetadata>* cmeta_out) {
  scoped_refptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK_PREPEND(ConsensusMetadata::Create(fs_manager_, tablet_id, fs_manager_->uuid(),
                                                  config, initial_term, create_mode,
                                                  &cmeta),
                        Substitute("Unable to create consensus metadata for tablet $0", tablet_id));

  lock_guard<Mutex> l(lock_);
  if (!InsertIfNotPresent(&cmeta_cache_, tablet_id, cmeta)) {
    return Status::AlreadyPresent(Substitute("ConsensusMetadata instance for $0 already exists",
                                             tablet_id));
  }
  if (cmeta_out) *cmeta_out = std::move(cmeta);
  return Status::OK();
}

Status ConsensusMetadataManager::Load(const string& tablet_id,
                                      scoped_refptr<ConsensusMetadata>* cmeta_out) {
  {
    lock_guard<Mutex> l(lock_);

    // Try to get the cmeta instance from cache first.
    scoped_refptr<ConsensusMetadata>* cached_cmeta = FindOrNull(cmeta_cache_, tablet_id);
    if (cached_cmeta) {
      if (cmeta_out) *cmeta_out = *cached_cmeta;
      return Status::OK();
    }
  }

  // If it's not yet cached, drop the lock before we load it.
  scoped_refptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK_PREPEND(ConsensusMetadata::Load(fs_manager_, tablet_id, fs_manager_->uuid(),
                                                &cmeta),
                        Substitute("Unable to load consensus metadata for tablet $0", tablet_id));

  // Cache and return the loaded ConsensusMetadata.
  {
    lock_guard<Mutex> l(lock_);
    // Due to our thread-safety contract, no other caller may have interleaved
    // with us for this tablet id, so we use InsertOrDie().
    InsertOrDie(&cmeta_cache_, tablet_id, cmeta);
  }

  if (cmeta_out) *cmeta_out = std::move(cmeta);
  return Status::OK();
}

Status ConsensusMetadataManager::LoadOrCreate(const string& tablet_id,
                                              const RaftConfigPB& config,
                                              int64_t initial_term,
                                              ConsensusMetadataCreateMode create_mode,
                                              scoped_refptr<ConsensusMetadata>* cmeta_out) {
  Status s = Load(tablet_id, cmeta_out);
  if (s.IsNotFound()) {
    return Create(tablet_id, config, initial_term, create_mode, cmeta_out);
  }
  return s;
}

Status ConsensusMetadataManager::Delete(const string& tablet_id) {
  {
    lock_guard<Mutex> l(lock_);
    cmeta_cache_.erase(tablet_id); // OK to delete an uncached cmeta; ignore the return value.
  }
  RETURN_NOT_OK_PREPEND(ConsensusMetadata::DeleteOnDiskData(fs_manager_, tablet_id),
                        Substitute("Unable to delete consensus metadata for tablet $0", tablet_id));
  return Status::OK();
}

} // namespace consensus
} // namespace kudu
