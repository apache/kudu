// Copyright (c) 2014 Cloudera, Inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CONSENSUS_CONSENSUS_META_H_
#define KUDU_CONSENSUS_CONSENSUS_META_H_

#include <stdint.h>
#include <string>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/server/metadata.pb.h"
#include "kudu/util/status.h"

namespace kudu {

class FsManager;

namespace consensus {

// Provides methods to read and persist consensus-related metadata.
//
// This corresponds to Raft Figure 2's "Persistent state on all servers".
//
// This class is not thread-safe and requires external synchronization.
class ConsensusMetadata {
 public:
  // Create a ConsensusMetadata object with provided initial state.
  // Encoded PB is flushed to disk before returning.
  static Status Create(FsManager* fs_manager,
                       const std::string& tablet_id,
                       metadata::QuorumPB& quorum,
                       uint64_t current_term,
                       gscoped_ptr<ConsensusMetadata>* cmeta);

  // Load a ConsensusMetadata object from disk.
  static Status Load(FsManager* fs_manager,
                     const std::string& tablet_id,
                     gscoped_ptr<ConsensusMetadata>* cmeta);

  // Accessors to the underlying in-memory protobuf object.
  ConsensusMetadataPB* mutable_pb() {
    return &pb_;
  }
  const ConsensusMetadataPB& pb() const {
    return pb_;
  }

  // Persist current state of the protobuf to disk.
  Status Flush();

 private:
  ConsensusMetadata(FsManager* fs_manager, const std::string& tablet_id);

  FsManager* fs_manager_;
  std::string tablet_id_;
  ConsensusMetadataPB pb_;

  DISALLOW_COPY_AND_ASSIGN(ConsensusMetadata);
};

} // namespace consensus
} // namespace kudu

#endif // KUDU_CONSENSUS_CONSENSUS_META_H_
