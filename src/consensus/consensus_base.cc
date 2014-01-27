// Copyright (c) 2013, Cloudera, inc.

#include "consensus/consensus_base.h"

#include <boost/foreach.hpp>
#include "consensus/log.h"

namespace kudu {
namespace consensus {

using metadata::QuorumPB;
using metadata::QuorumPeerPB;

uint8_t ConsensusBase::n_majority() const {
  return (quorum_config_.peers_size() / 2) + 1;
}

uint8_t ConsensusBase::num_participants() const {
  return quorum_config_.peers_size();
}

bool ConsensusBase::is_leader() const {
  return peer_.role() == QuorumPeerPB::LEADER;
}

QuorumPeerPB::Role ConsensusBase::role() const {
  return peer_.role();
}

QuorumPB ConsensusBase::CurrentQuorum() const {
  return quorum_config_;
}

QuorumPeerPB ConsensusBase::CurrentLeader() const {
  return leader_;
}

}  // namespace consensus
}  // namespace kudu

