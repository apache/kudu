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

// Modifies 'quorum' remove the peer with the specified 'uuid'.
// Returns false if the server with 'uuid' is not found in the quorum.
// Returns true on success.
bool RemoveFromQuorum(QuorumPB* quorum, const std::string& uuid);

// Counts the number of voters in the quorum.
int CountVoters(const QuorumPB& quorum);

// Calculates size of a quorum majority based on # of voters.
int MajoritySize(int num_voters);

// Determines the role that the peer with uuid 'uuid' plays in the cluster.
// If the peer uuid is not a voter in the quorum, this function will return
// NON_PARTICIPANT, regardless of whether it is listed as leader in cstate.
QuorumPeerPB::Role GetConsensusRole(const std::string& uuid,
                                    const ConsensusStatePB& cstate);

// Verifies that the provided quorum is well formed.
// If type == COMMITTED_QUORUM, we enforce that opid_index is set.
// If type == UNCOMMITTED_QUORUM, we enforce that opid_index is NOT set.
Status VerifyQuorum(const QuorumPB& quorum, QuorumPBType type);

// Superset of checks performed by VerifyQuorum. Also ensures that the
// leader is a quorum voter, if it is set, and that a valid term is set.
Status VerifyConsensusState(const ConsensusStatePB& cstate, QuorumPBType type);

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_QUORUM_UTIL_H_ */
