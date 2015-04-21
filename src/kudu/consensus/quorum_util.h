// Copyright (c) 2014, Cloudera Inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_CONSENSUS_QUORUM_UTIL_H_
#define KUDU_CONSENSUS_QUORUM_UTIL_H_

#include <string>

#include "kudu/consensus/metadata.pb.h"

namespace kudu {
class Status;

namespace consensus {

enum QuorumPBType {
  UNCOMMITTED_QUORUM,
  COMMITTED_QUORUM,
};

bool IsQuorumMember(const std::string& uuid, const QuorumPB& quorum);
bool IsQuorumVoter(const std::string& uuid, const QuorumPB& quorum);
bool IsQuorumLeader(const std::string& uuid, const QuorumPB& quorum);

QuorumPeerPB::Role GetRoleInQuorum(const std::string& permanent_uuid,
                                   const QuorumPB& quorum);

// Verifies that the provided quorum is well formed.
// If type == COMMITTED_QUORUM, we enforce that opid_index is set.
// If type == UNCOMMITTED_QUORUM, we enforce that opid_index is NOT set.
Status VerifyQuorum(const QuorumPB& quorum, QuorumPBType type);

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_QUORUM_UTIL_H_ */
