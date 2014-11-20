// Copyright (c) 2014, Cloudera Inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_CONSENSUS_QUORUM_UTIL_H_
#define KUDU_CONSENSUS_QUORUM_UTIL_H_

#include <string>

#include "kudu/server/metadata.pb.h"

namespace kudu {
class Status;

namespace consensus {

// Returns true if the passed role is LEADER, CANDIDATE, or FOLLOWER.
bool IsVotingRole(const metadata::QuorumPeerPB::Role role);

// Copies 'old_quorum' to 'new_quorum' but gives the peer with 'peer_uuid'
// the role in 'role'. Additionally, demotes all of the other peers to FOLLOWER
// if they currently have a LEADER or CANDIDATE role.
// Returns Status::IllegalState() if the specified peer cannot be found or if
// the specified peer appears in the quorum more than once.
Status GivePeerRoleInQuorum(const std::string& peer_uuid,
                            metadata::QuorumPeerPB::Role role,
                            const metadata::QuorumPB& old_quorum,
                            metadata::QuorumPB* new_quorum);

// Makes all voting peers (anyone with a LEADER, CANDIDATE, or FOLLOWER role)
// a follower in 'new_quorum'.
void SetAllQuorumVotersToFollower(const metadata::QuorumPB& old_quorum,
                                  metadata::QuorumPB* new_quorum);

// Helper to return the role of a peer within a quorum, or NON_PARTICIPANT is the peer does
// not participate in the quorum.
metadata::QuorumPeerPB::Role GetRoleInQuorum(const std::string& permanent_uuid,
                                             const metadata::QuorumPB& quorum);

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_QUORUM_UTIL_H_ */
