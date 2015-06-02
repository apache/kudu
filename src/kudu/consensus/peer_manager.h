// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CONSENSUS_PEER_MANAGER_H
#define KUDU_CONSENSUS_PEER_MANAGER_H

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

#include <string>
#include <tr1/unordered_map>

namespace kudu {

class ThreadPool;

namespace log {
class Log;
} // namespace log

namespace consensus {

class Peer;
class PeerMessageQueue;
class PeerProxyFactory;
class RaftConfigPB;

// Manages the set of local and remote peers that pull data from the
// queue into the local log/remote machines.
// Methods are virtual to ease mocking.
class PeerManager {
 public:
  // All of the raw pointer arguments are not owned by the PeerManager
  // and must live at least as long as the PeerManager.
  //
  // 'request_thread_pool' is the pool used to construct requests to send
  // to the peers.
  PeerManager(const std::string tablet_id,
              const std::string local_uuid,
              PeerProxyFactory* peer_proxy_factory,
              PeerMessageQueue* queue,
              ThreadPool* request_thread_pool,
              const scoped_refptr<log::Log>& log);

  virtual ~PeerManager();

  // Updates 'peers_' according to the new configuration config.
  virtual Status UpdateRaftConfig(const RaftConfigPB& config);

  // Signals all peers of the current configuration that there is a new request pending.
  virtual void SignalRequest(bool force_if_queue_empty = false);

  // Closes all peers.
  virtual void Close();

 private:
  std::string GetLogPrefix() const;

  typedef std::tr1::unordered_map<std::string, Peer*> PeersMap;
  const std::string tablet_id_;
  const std::string local_uuid_;
  PeerProxyFactory* peer_proxy_factory_;
  PeerMessageQueue* queue_;
  ThreadPool* thread_pool_;
  scoped_refptr<log::Log> log_;
  PeersMap peers_;
  mutable simple_spinlock lock_;

  DISALLOW_COPY_AND_ASSIGN(PeerManager);
};



} // namespace consensus
} // namespace kudu
#endif /* KUDU_CONSENSUS_PEER_MANAGER_H */
