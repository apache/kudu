// Copyright (c) 2014 Cloudera Inc.
#ifndef KUDU_TSERVER_TABLET_PEER_LOOKUP_H_
#define KUDU_TSERVER_TABLET_PEER_LOOKUP_H_

#include <string>
#include <tr1/memory>

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"

namespace kudu {

namespace tablet {
class TabletPeer;
} // namespace tablet

namespace tserver {

// Pure virtual interface that provides a uniform accessor for TabletPeer
// instances. This can be used to provide remote_bootstrap services on a TabletServer
// or a Master server, provided they implement the interface.
class TabletPeerLookupIf {
 public:
  virtual Status GetTabletPeer(const std::string& tablet_id,
                               scoped_refptr<tablet::TabletPeer>* tablet_peer) const = 0;
};

} // namespace tserver
} // namespace kudu

#endif // KUDU_TSERVER_TABLET_PEER_LOOKUP_H_
