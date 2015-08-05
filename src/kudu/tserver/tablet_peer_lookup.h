// Copyright (c) 2014 Cloudera Inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TSERVER_TABLET_PEER_LOOKUP_H_
#define KUDU_TSERVER_TABLET_PEER_LOOKUP_H_

#include <string>
#include <tr1/memory>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"

namespace kudu {

class HostPort;
class NodeInstancePB;

namespace consensus {
class StartRemoteBootstrapRequestPB;
} // namespace consensus

namespace tablet {
class TabletPeer;
} // namespace tablet

namespace tserver {

// Pure virtual interface that provides an abstraction for something that
// contains and manages TabletPeers. This interface is implemented on both
// tablet servers and master servers.
// TODO: Rename this interface.
class TabletPeerLookupIf {
 public:
  virtual Status GetTabletPeer(const std::string& tablet_id,
                               scoped_refptr<tablet::TabletPeer>* tablet_peer) const = 0;

  virtual const NodeInstancePB& NodeInstance() const = 0;

  virtual Status StartRemoteBootstrap(const consensus::StartRemoteBootstrapRequestPB& req) = 0;
};

} // namespace tserver
} // namespace kudu

#endif // KUDU_TSERVER_TABLET_PEER_LOOKUP_H_
