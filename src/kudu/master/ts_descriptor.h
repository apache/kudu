// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_TS_DESCRIPTOR_H
#define KUDU_MASTER_TS_DESCRIPTOR_H

#include <string>
#include <tr1/memory>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

class NodeInstancePB;

namespace rpc {
class Messenger;
}

namespace tserver {
class TabletServerServiceProxy;
}

namespace master {

class TSRegistrationPB;

// Master-side view of a single tablet server.
//
// Tracks the last heartbeat, status, instance identifier, etc.
// This class is thread-safe.
class TSDescriptor {
 public:
  static Status RegisterNew(const NodeInstancePB& instance,
                            const TSRegistrationPB& registration,
                            gscoped_ptr<TSDescriptor>* desc);

  virtual ~TSDescriptor();

  // Set the last-heartbeat time to now.
  void UpdateHeartbeatTime();

  // Return the amount of time since the last heartbeat received
  // from this TS.
  MonoDelta TimeSinceHeartbeat() const;

  // Register this tablet server.
  Status Register(const NodeInstancePB& instance,
                  const TSRegistrationPB& registration);

  const std::string &permanent_uuid() const { return permanent_uuid_; }
  int64_t latest_seqno() const;

  bool has_tablet_report() const;
  void set_has_tablet_report(bool has_report);

  // Copy the current registration info into the given PB object.
  // A safe copy is returned because the internal Registration object
  // may be mutated at any point if the tablet server re-registers.
  void GetRegistration(TSRegistrationPB* reg) const;

  void GetNodeInstancePB(NodeInstancePB* instance_pb) const;

  // Return an RPC proxy to the Tablet Server.
  Status GetProxy(const std::tr1::shared_ptr<rpc::Messenger>& messenger,
                  std::tr1::shared_ptr<tserver::TabletServerServiceProxy>* proxy);

 private:
  explicit TSDescriptor(const std::string& perm_id);

  mutable simple_spinlock lock_;

  const std::string permanent_uuid_;
  int64_t latest_seqno_;

  // The last time a heartbeat was received for this node.
  MonoTime last_heartbeat_;

  // Set to true once this instance has reported all of its tablets.
  bool has_tablet_report_;

  gscoped_ptr<TSRegistrationPB> registration_;

  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> proxy_;

  DISALLOW_COPY_AND_ASSIGN(TSDescriptor);
};

} // namespace master
} // namespace kudu
#endif /* KUDU_MASTER_TS_DESCRIPTOR_H */
