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
#include "kudu/consensus/consensus_meta.h"

#include <memory>

#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"

DEFINE_double(fault_crash_before_cmeta_flush, 0.0,
              "Fraction of the time when the server will crash just before flushing "
              "consensus metadata. (For testing only!)");
TAG_FLAG(fault_crash_before_cmeta_flush, unsafe);

namespace kudu {
namespace consensus {

using std::lock_guard;
using std::string;
using strings::Substitute;

int64_t ConsensusMetadata::current_term() const {
  lock_guard<Mutex> l(lock_);
  return current_term_unlocked();
}

int64_t ConsensusMetadata::current_term_unlocked() const {
  lock_.AssertAcquired();
  DCHECK(pb_.has_current_term());
  return pb_.current_term();
}

void ConsensusMetadata::set_current_term(int64_t term) {
  lock_guard<Mutex> l(lock_);
  set_current_term_unlocked(term);
}

void ConsensusMetadata::set_current_term_unlocked(int64_t term) {
  lock_.AssertAcquired();
  DCHECK_GE(term, kMinimumTerm);
  pb_.set_current_term(term);
}

bool ConsensusMetadata::has_voted_for() const {
  lock_guard<Mutex> l(lock_);
  return pb_.has_voted_for();
}

string ConsensusMetadata::voted_for() const {
  lock_guard<Mutex> l(lock_);
  DCHECK(pb_.has_voted_for());
  return pb_.voted_for();
}

void ConsensusMetadata::clear_voted_for() {
  lock_guard<Mutex> l(lock_);
  clear_voted_for_unlocked();
}

void ConsensusMetadata::clear_voted_for_unlocked() {
  lock_.AssertAcquired();
  pb_.clear_voted_for();
}

void ConsensusMetadata::set_voted_for(const string& uuid) {
  lock_guard<Mutex> l(lock_);
  DCHECK(!uuid.empty());
  pb_.set_voted_for(uuid);
}

bool ConsensusMetadata::IsVoterInConfig(const string& uuid,
                                        RaftConfigState type) {
  lock_guard<Mutex> l(lock_);
  return IsRaftConfigVoter(uuid, config_unlocked(type));
}

bool ConsensusMetadata::IsMemberInConfig(const string& uuid,
                                         RaftConfigState type) {
  lock_guard<Mutex> l(lock_);
  return IsRaftConfigMember(uuid, config_unlocked(type));
}

int ConsensusMetadata::CountVotersInConfig(RaftConfigState type) {
  lock_guard<Mutex> l(lock_);
  return CountVoters(config_unlocked(type));
}

int64_t ConsensusMetadata::GetConfigOpIdIndex(RaftConfigState type) {
  lock_guard<Mutex> l(lock_);
  return config_unlocked(type).opid_index();
}

RaftConfigPB ConsensusMetadata::CommittedConfig() const {
  lock_guard<Mutex> l(lock_);
  return committed_config_unlocked();
}

const RaftConfigPB& ConsensusMetadata::config_unlocked(RaftConfigState type) const {
  switch (type) {
    case ACTIVE_CONFIG: return active_config_unlocked();
    case COMMITTED_CONFIG: return committed_config_unlocked();
    case PENDING_CONFIG: return pending_config_unlocked();
  }
}

const RaftConfigPB& ConsensusMetadata::committed_config_unlocked() const {
  lock_.AssertAcquired();
  DCHECK(pb_.has_committed_config());
  return pb_.committed_config();
}

void ConsensusMetadata::set_committed_config(const RaftConfigPB& config) {
  lock_guard<Mutex> l(lock_);
  set_committed_config_unlocked(config);
}

void ConsensusMetadata::set_committed_config_unlocked(const RaftConfigPB& config) {
  lock_.AssertAcquired();
  *pb_.mutable_committed_config() = config;
  if (!has_pending_config_) {
    UpdateActiveRoleUnlocked();
  }
}

bool ConsensusMetadata::has_pending_config() const {
  lock_guard<Mutex> l(lock_);
  return has_pending_config_unlocked();
}

bool ConsensusMetadata::has_pending_config_unlocked() const {
  lock_.AssertAcquired();
  return has_pending_config_;
}

RaftConfigPB ConsensusMetadata::PendingConfig() const {
  lock_guard<Mutex> l(lock_);
  return pending_config_unlocked();
}

const RaftConfigPB& ConsensusMetadata::pending_config_unlocked() const {
  lock_.AssertAcquired();
  CHECK(has_pending_config_) << LogPrefix() << "There is no pending config";
  return pending_config_;
}

void ConsensusMetadata::clear_pending_config() {
  lock_guard<Mutex> l(lock_);
  clear_pending_config_unlocked();
}

void ConsensusMetadata::clear_pending_config_unlocked() {
  lock_.AssertAcquired();
  has_pending_config_ = false;
  pending_config_.Clear();
  UpdateActiveRoleUnlocked();
}

void ConsensusMetadata::set_pending_config(const RaftConfigPB& config) {
  lock_guard<Mutex> l(lock_);
  has_pending_config_ = true;
  pending_config_ = config;
  UpdateActiveRoleUnlocked();
}

RaftConfigPB ConsensusMetadata::ActiveConfig() const {
  lock_guard<Mutex> l(lock_);
  return active_config_unlocked();
}

const RaftConfigPB& ConsensusMetadata::active_config_unlocked() const {
  lock_.AssertAcquired();
  if (has_pending_config_) {
    return pending_config_unlocked();
  }
  return committed_config_unlocked();
}

string ConsensusMetadata::leader_uuid() const {
  lock_guard<Mutex> l(lock_);
  return leader_uuid_;
}

void ConsensusMetadata::set_leader_uuid(const string& uuid) {
  lock_guard<Mutex> l(lock_);
  set_leader_uuid_unlocked(uuid);
}

void ConsensusMetadata::set_leader_uuid_unlocked(const string& uuid) {
  lock_.AssertAcquired();
  leader_uuid_ = uuid;
  UpdateActiveRoleUnlocked();
}

RaftPeerPB::Role ConsensusMetadata::active_role() const {
  lock_guard<Mutex> l(lock_);
  return active_role_;
}

ConsensusStatePB ConsensusMetadata::ToConsensusStatePB() const {
  lock_guard<Mutex> l(lock_);
  return ToConsensusStatePBUnlocked();
}

ConsensusStatePB ConsensusMetadata::ToConsensusStatePBUnlocked() const {
  lock_.AssertAcquired();
  ConsensusStatePB cstate;
  cstate.set_current_term(pb_.current_term());
  cstate.set_leader_uuid(leader_uuid_);
  *cstate.mutable_committed_config() = committed_config_unlocked();
  if (has_pending_config_unlocked()) {
    *cstate.mutable_pending_config() = pending_config_unlocked();
  }
  return cstate;
}

void ConsensusMetadata::MergeCommittedConsensusStatePB(const ConsensusStatePB& cstate) {
  lock_guard<Mutex> l(lock_);
  if (cstate.current_term() > current_term_unlocked()) {
    set_current_term_unlocked(cstate.current_term());
    clear_voted_for_unlocked();
  }

  set_leader_uuid_unlocked("");
  set_committed_config_unlocked(cstate.committed_config());
  clear_pending_config_unlocked();
}

Status ConsensusMetadata::Flush(FlushMode mode) {
  lock_guard<Mutex> l(lock_);
  MAYBE_FAULT(FLAGS_fault_crash_before_cmeta_flush);
  SCOPED_LOG_SLOW_EXECUTION_PREFIX(WARNING, 500, LogPrefix(), "flushing consensus metadata");

  flush_count_for_tests_++;
  // Sanity test to ensure we never write out a bad configuration.
  RETURN_NOT_OK_PREPEND(VerifyRaftConfig(pb_.committed_config(), COMMITTED_CONFIG),
                        "Invalid config in ConsensusMetadata, cannot flush to disk");

  // Create directories if needed.
  string dir = fs_manager_->GetConsensusMetadataDir();
  bool created_dir = false;
  RETURN_NOT_OK_PREPEND(fs_manager_->CreateDirIfMissing(dir, &created_dir),
                        "Unable to create consensus metadata root dir");
  // fsync() parent dir if we had to create the dir.
  if (PREDICT_FALSE(created_dir)) {
    string parent_dir = DirName(dir);
    RETURN_NOT_OK_PREPEND(Env::Default()->SyncDir(parent_dir),
                          "Unable to fsync consensus parent dir " + parent_dir);
  }

  string meta_file_path = fs_manager_->GetConsensusMetadataPath(tablet_id_);
  RETURN_NOT_OK_PREPEND(pb_util::WritePBContainerToPath(
      fs_manager_->env(), meta_file_path, pb_,
      mode == OVERWRITE ? pb_util::OVERWRITE : pb_util::NO_OVERWRITE,
      // We use FLAGS_log_force_fsync_all here because the consensus metadata is
      // essentially an extension of the primary durability mechanism of the
      // consensus subsystem: the WAL. Using the same flag ensures that the WAL
      // and the consensus metadata get the same durability guarantees.
      FLAGS_log_force_fsync_all ? pb_util::SYNC : pb_util::NO_SYNC),
          Substitute("Unable to write consensus meta file for tablet $0 to path $1",
                     tablet_id_, meta_file_path));
  return Status::OK();
}

ConsensusMetadata::ConsensusMetadata(FsManager* fs_manager,
                                     std::string tablet_id,
                                     std::string peer_uuid)
    : fs_manager_(CHECK_NOTNULL(fs_manager)),
      tablet_id_(std::move(tablet_id)),
      peer_uuid_(std::move(peer_uuid)),
      has_pending_config_(false),
      flush_count_for_tests_(0) {
}

Status ConsensusMetadata::Create(FsManager* fs_manager,
                                 const string& tablet_id,
                                 const std::string& peer_uuid,
                                 const RaftConfigPB& config,
                                 int64_t current_term,
                                 scoped_refptr<ConsensusMetadata>* cmeta_out) {
  scoped_refptr<ConsensusMetadata> cmeta(new ConsensusMetadata(fs_manager, tablet_id, peer_uuid));
  cmeta->set_committed_config(config);
  cmeta->set_current_term(current_term);
  RETURN_NOT_OK(cmeta->Flush(NO_OVERWRITE)); // Create() should not clobber.
  cmeta_out->swap(cmeta);
  return Status::OK();
}

Status ConsensusMetadata::Load(FsManager* fs_manager,
                               const std::string& tablet_id,
                               const std::string& peer_uuid,
                               scoped_refptr<ConsensusMetadata>* cmeta_out) {
  scoped_refptr<ConsensusMetadata> cmeta(new ConsensusMetadata(fs_manager, tablet_id, peer_uuid));
  RETURN_NOT_OK(pb_util::ReadPBContainerFromPath(fs_manager->env(),
                                                 fs_manager->GetConsensusMetadataPath(tablet_id),
                                                 &cmeta->pb_));
  cmeta->UpdateActiveRole(); // Needs to happen here as we sidestep the accessor APIs.
  cmeta_out->swap(cmeta);
  return Status::OK();
}

Status ConsensusMetadata::DeleteOnDiskData(FsManager* fs_manager, const string& tablet_id) {
  string cmeta_path = fs_manager->GetConsensusMetadataPath(tablet_id);
  RETURN_NOT_OK_PREPEND(fs_manager->env()->DeleteFile(cmeta_path),
                        Substitute("Unable to delete consensus metadata file for tablet $0",
                                   tablet_id));
  return Status::OK();
}

std::string ConsensusMetadata::LogPrefix() const {
  // No need to lock to read const members.
  return Substitute("T $0 P $1: ", tablet_id_, peer_uuid_);
}

void ConsensusMetadata::UpdateActiveRole() {
  lock_guard<Mutex> l(lock_);
  UpdateActiveRoleUnlocked();
}

void ConsensusMetadata::UpdateActiveRoleUnlocked() {
  lock_.AssertAcquired();
  ConsensusStatePB cstate = ToConsensusStatePBUnlocked();
  active_role_ = GetConsensusRole(peer_uuid_, cstate);
  VLOG_WITH_PREFIX(1) << "Updating active role to " << RaftPeerPB::Role_Name(active_role_)
                      << ". Consensus state: " << SecureShortDebugString(cstate);
}

} // namespace consensus
} // namespace kudu
