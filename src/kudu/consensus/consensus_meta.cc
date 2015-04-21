// Copyright (c) 2014 Cloudera, Inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/consensus/consensus_meta.h"

#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/pb_util.h"

namespace kudu {
namespace consensus {

using std::string;
using strings::Substitute;

Status ConsensusMetadata::Create(FsManager* fs_manager,
                                 const string& tablet_id,
                                 QuorumPB& quorum,
                                 uint64_t current_term,
                                 gscoped_ptr<ConsensusMetadata>* cmeta_out) {
  gscoped_ptr<ConsensusMetadata> cmeta(new ConsensusMetadata(fs_manager, tablet_id));
  cmeta->mutable_pb()->mutable_committed_quorum()->CopyFrom(quorum);
  cmeta->mutable_pb()->set_current_term(current_term);
  RETURN_NOT_OK(cmeta->Flush());
  cmeta_out->swap(cmeta);
  return Status::OK();
}

Status ConsensusMetadata::Load(FsManager* fs_manager,
                               const std::string& tablet_id,
                               gscoped_ptr<ConsensusMetadata>* cmeta_out) {
  gscoped_ptr<ConsensusMetadata> cmeta(new ConsensusMetadata(fs_manager, tablet_id));
  RETURN_NOT_OK(pb_util::ReadPBContainerFromPath(fs_manager->env(),
                                        fs_manager->GetConsensusMetadataPath(tablet_id),
                                        cmeta->mutable_pb()));
  cmeta_out->swap(cmeta);
  return Status::OK();
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
      // We use FLAGS_log_force_fsync_all here because the consensus metadata is
      // essentially an extension of the primary durability mechanism of the
      // consensus subsystem: the WAL. Using the same flag ensures that the WAL
      // and the consensus metadata get the same durability guarantees.
      FLAGS_log_force_fsync_all ? pb_util::SYNC : pb_util::NO_SYNC),
          Substitute("Unable to write consensus meta file for tablet $0 to path $1",
                     tablet_id_, meta_file_path));
  return Status::OK();
}

ConsensusMetadata::ConsensusMetadata(FsManager* fs_manager, const std::string& tablet_id)
  : fs_manager_(CHECK_NOTNULL(fs_manager)),
    tablet_id_(tablet_id) {
}

} // namespace consensus
} // namespace kudu
