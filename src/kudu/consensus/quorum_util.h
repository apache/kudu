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

// Returns true if the passed role is LEADER, or FOLLOWER.
bool IsVotingRole(const QuorumPeerPB::Role role);

// Copies 'old_quorum' to 'new_quorum' but gives the peer with 'peer_uuid'
// the role in 'role'. Additionally, demotes any LEADER to a FOLLOWER role.
// Returns Status::IllegalState() if the specified peer cannot be found or if
// the specified peer appears in the quorum more than once.
Status GivePeerRoleInQuorum(const std::string& peer_uuid,
                            QuorumPeerPB::Role role,
                            const QuorumPB& old_quorum,
                            QuorumPB* new_quorum);

// Makes all voting peers (anyone with a LEADER or FOLLOWER role)
// a follower in 'new_quorum'.
void SetAllQuorumVotersToFollower(const QuorumPB& old_quorum,
                                  QuorumPB* new_quorum);

// Helper to return the role of a peer within a quorum, or NON_PARTICIPANT is the peer does
// not participate in the quorum.
QuorumPeerPB::Role GetRoleInQuorum(const std::string& permanent_uuid,
                                   const QuorumPB& quorum);

// Verifies that the provided quorum is well formed.
// If type == COMMITTED_QUORUM, we enforce that opid_index is set.
// If type == UNCOMMITTED_QUORUM, we enforce that opid_index is NOT set.
Status VerifyQuorum(const QuorumPB& quorum, QuorumPBType type);

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_QUORUM_UTIL_H_ */
