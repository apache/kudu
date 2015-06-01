// Copyright (c) 2014 Cloudera, Inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/consensus/consensus_meta.h"

#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"

namespace kudu {
namespace consensus {

using std::string;
using strings::Substitute;

Status ConsensusMetadata::Create(FsManager* fs_manager,
                                 const string& tablet_id,
                                 const std::string& peer_uuid,
                                 QuorumPB& quorum,
                                 int64_t current_term,
                                 gscoped_ptr<ConsensusMetadata>* cmeta_out) {
  gscoped_ptr<ConsensusMetadata> cmeta(new ConsensusMetadata(fs_manager, tablet_id, peer_uuid));
  cmeta->set_committed_quorum(quorum);
  cmeta->set_current_term(current_term);
  RETURN_NOT_OK(cmeta->Flush());
  cmeta_out->swap(cmeta);
  return Status::OK();
}

Status ConsensusMetadata::Load(FsManager* fs_manager,
                               const std::string& tablet_id,
                               const std::string& peer_uuid,
                               gscoped_ptr<ConsensusMetadata>* cmeta_out) {
  gscoped_ptr<ConsensusMetadata> cmeta(new ConsensusMetadata(fs_manager, tablet_id, peer_uuid));
  RETURN_NOT_OK(pb_util::ReadPBContainerFromPath(fs_manager->env(),
                                        fs_manager->GetConsensusMetadataPath(tablet_id),
                                        &cmeta->pb_));
  cmeta->UpdateActiveRole(); // Needs to happen here as we sidestep the accessor APIs.
  cmeta_out->swap(cmeta);
  return Status::OK();
}

const int64_t ConsensusMetadata::current_term() const {
  DCHECK(pb_.has_current_term());
  return pb_.current_term();
}

void ConsensusMetadata::set_current_term(int64_t term) {
  DCHECK_GE(term, kMinimumTerm);
  pb_.set_current_term(term);
}

bool ConsensusMetadata::has_voted_for() const {
  return pb_.has_voted_for();
}

const string& ConsensusMetadata::voted_for() const {
  DCHECK(pb_.has_voted_for());
  return pb_.voted_for();
}

void ConsensusMetadata::clear_voted_for() {
  pb_.clear_voted_for();
}

void ConsensusMetadata::set_voted_for(const string& uuid) {
  DCHECK(!uuid.empty());
  pb_.set_voted_for(uuid);
}

const QuorumPB& ConsensusMetadata::committed_quorum() const {
  DCHECK(pb_.has_committed_quorum());
  return pb_.committed_quorum();
}

void ConsensusMetadata::set_committed_quorum(const QuorumPB& quorum) {
  *pb_.mutable_committed_quorum() = quorum;
  if (!has_pending_quorum_) {
    UpdateActiveRole();
  }
}

bool ConsensusMetadata::has_pending_quorum() const {
  return has_pending_quorum_;
}

const QuorumPB& ConsensusMetadata::pending_quorum() const {
  DCHECK(has_pending_quorum_);
  return pending_quorum_;
}

void ConsensusMetadata::clear_pending_quorum() {
  has_pending_quorum_ = false;
  pending_quorum_.Clear();
  UpdateActiveRole();
}

void ConsensusMetadata::set_pending_quorum(const QuorumPB& quorum) {
  has_pending_quorum_ = true;
  pending_quorum_ = quorum;
  UpdateActiveRole();
}

const QuorumPB& ConsensusMetadata::active_quorum() const {
  if (has_pending_quorum_) {
    return pending_quorum();
  }
  return committed_quorum();
}

const string& ConsensusMetadata::leader_uuid() const {
  return leader_uuid_;
}

void ConsensusMetadata::set_leader_uuid(const string& uuid) {
  leader_uuid_ = uuid;
  UpdateActiveRole();
}

QuorumPeerPB::Role ConsensusMetadata::active_role() const {
  return active_role_;
}

ConsensusStatePB ConsensusMetadata::ToConsensusStatePB(ConfigType type) const {
  ConsensusStatePB cstate;
  cstate.set_current_term(pb_.current_term());
  if (type == ACTIVE) {
    *cstate.mutable_quorum() = active_quorum();
    cstate.set_leader_uuid(leader_uuid_);
  } else {
    *cstate.mutable_quorum() = committed_quorum();
    // It's possible, though unlikely, that a new node from a pending quorum
    // could be elected leader. Do not indicate a leader in this case.
    if (PREDICT_TRUE(IsQuorumVoter(leader_uuid_, cstate.quorum()))) {
      cstate.set_leader_uuid(leader_uuid_);
    }
  }
  return cstate;
}

Status ConsensusMetadata::Flush() {
  // Sanity test to ensure we never write out a bad quorum.
  RETURN_NOT_OK_PREPEND(VerifyQuorum(pb_.committed_quorum(), COMMITTED_QUORUM),
                        "Invalid quorum in ConsensusMetadata, cannot flush to disk");

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
      pb_util::OVERWRITE,
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
                                     const std::string& tablet_id,
                                     const std::string& peer_uuid)
    : fs_manager_(CHECK_NOTNULL(fs_manager)),
      tablet_id_(tablet_id),
      peer_uuid_(peer_uuid),
      has_pending_quorum_(false) {
}

std::string ConsensusMetadata::LogPrefix() const {
  return Substitute("T $0 P $1: ", tablet_id_, peer_uuid_);
}

void ConsensusMetadata::UpdateActiveRole() {
  ConsensusStatePB cstate = ToConsensusStatePB(ACTIVE);
  active_role_ = GetConsensusRole(peer_uuid_, cstate);
  VLOG_WITH_PREFIX(1) << "Updating active role to " << QuorumPeerPB::Role_Name(active_role_)
                      << ". Consensus state: " << cstate.ShortDebugString();
}

} // namespace consensus
} // namespace kudu
