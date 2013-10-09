// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_M_TABLET_MANAGER_H
#define KUDU_MASTER_M_TABLET_MANAGER_H

#include <string>
#include <tr1/unordered_map>
#include <vector>

#include "gutil/macros.h"
#include "util/locks.h"
#include "util/status.h"

namespace kudu {

namespace rpc {
class RpcContext;
} // namespace rpc

namespace master {

class ReportedTabletPB;
class TabletReportPB;
class TabletInfo;
class TSDescriptor;

// The component of the master which tracks the state and location
// of tablets in the cluster.
//
// This is the master-side counterpart of TSTabletManager, which tracks
// the state of each tablet on a given tablet-server.
//
// This is soft-state for now (TODO: when we have some non-soft state
// about tablets, will it go here or a separate class? TBD.)
//
// Thread-safe.
class MTabletManager {
 public:
  MTabletManager();
  virtual ~MTabletManager();

  // Look up the locations of the given tablet. The locations
  // vector is overwritten (not appended to).
  // If the tablet is not found, clears the result vector.
  void GetTabletLocations(const std::string& tablet_id,
                          std::vector<TSDescriptor*>* locations);

  // Handle a tablet report from the given tablet server.
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status ProcessTabletReport(TSDescriptor* ts_desc,
                             const TabletReportPB& report,
                             rpc::RpcContext* rpc);

 private:

  // Handle one of the tablets in a tablet reported.
  // Requires that the lock is already held.
  Status HandleReportedTablet(TSDescriptor* ts_desc,
                              const ReportedTabletPB& report,
                              rpc::RpcContext* rpc);

  void ClearAllReplicasOnTS(TSDescriptor* ts_desc);


  typedef rw_spinlock LockType;
  LockType lock_;

  // TODO: this is a little wasteful of RAM, since the TabletInfo object
  // also has a copy of the tablet ID string. But STL doesn't make it
  // easy to make a "gettable set".
  typedef std::tr1::unordered_map<std::string, TabletInfo*> TabletInfoMap;
  TabletInfoMap tablet_map_;

  DISALLOW_COPY_AND_ASSIGN(MTabletManager);
};

// The information about a single tablet which exists in the cluster,
// including its state and locations.
//
// Requires external synchronization.
class TabletInfo {
 public:
  explicit TabletInfo(const std::string& tablet_id);
  ~TabletInfo();

  // Add a replica reported on the given server
  void AddReplica(TSDescriptor* ts_desc);

  // Remove any replicas which were on this server.
  void ClearReplicasOnTS(const TSDescriptor* ts_desc);

  const std::string& tablet_id() const { return tablet_id_; }
  const std::vector<TSDescriptor*> locations() const { return locations_; }

 private:
  const std::string tablet_id_;

  // The locations where this tablet has been reported.
  // TODO: this probably will turn into a struct which also includes
  // some state information at some point.
  std::vector<TSDescriptor*> locations_;

  DISALLOW_COPY_AND_ASSIGN(TabletInfo);
};

} // namespace master
} // namespace kudu
#endif /* KUDU_MASTER_M_TABLET_MANAGER_H */
