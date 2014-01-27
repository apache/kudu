// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_CONSENSUS_BASE_H_
#define KUDU_CONSENSUS_CONSENSUS_BASE_H_

#include "consensus/consensus.h"

#include "server/metadata.pb.h"

namespace kudu {

namespace log {
class Log;
}

namespace consensus {

// A base class for consensus implementations.
// This class mostly manages configuration.
//
// Note: the methods implemented by this class are thread safe.
class ConsensusBase : public Consensus {
 public:

  explicit ConsensusBase(const ConsensusOptions& options)
      : options_(options),
        log_(NULL) {
  }

  uint8_t n_majority() const;

  uint8_t num_participants() const;

  bool is_leader() const;

  metadata::QuorumPeerPB::Role role() const;

  metadata::QuorumPB CurrentQuorum() const;

  metadata::QuorumPeerPB CurrentLeader() const;

  virtual ~ConsensusBase() {}

 protected:
  ConsensusBase(const ConsensusOptions& options,
                const metadata::QuorumPeerPB& peer,
                log::Log* log)
      : options_(options),
        log_(log),
        peer_(peer) {
  }

  const ConsensusOptions options_;
  log::Log* log_;
  metadata::QuorumPeerPB peer_;
  metadata::QuorumPeerPB leader_;
  metadata::QuorumPB quorum_config_;

 private:
  DISALLOW_COPY_AND_ASSIGN(ConsensusBase);
};

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_CONSENSUS_BASE_H_ */
