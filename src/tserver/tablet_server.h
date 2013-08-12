// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_TABLET_SERVER_H
#define KUDU_TSERVER_TABLET_SERVER_H

#include <string>
#include <tr1/memory>
#include <vector>

#include <gtest/gtest.h>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "tserver/tserver.pb.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

namespace kudu {

class RpcServer;
struct RpcServerOptions;

namespace rpc {
class Messenger;
class ServicePool;
}

namespace tablet {
class Tablet;
}

namespace tserver {

class TabletServer {
 public:
  static const uint16_t kDefaultPort = 7150;

  explicit TabletServer(const RpcServerOptions& opts);
  ~TabletServer();

  Status Init();
  Status Start();

  string ToString() const;

  // Return the addresses that this server has successfully
  // bound to. Requires that the server has been Start()ed.
  void GetBoundAddresses(std::vector<Sockaddr>* addresses);

  // Register the given tablet to be managed by this tablet server.
  void RegisterTablet(const std::tr1::shared_ptr<tablet::Tablet>& tablet);

  // Lookup the given tablet by its ID.
  // Returns true if the tablet is found successfully.
  bool LookupTablet(const string& tablet_id,
                    std::tr1::shared_ptr<tablet::Tablet>* tablet) const;

 private:
  friend class TabletServerTest;

  RpcServer *rpc_server() const { return rpc_server_.get(); }

  bool initted_;

  gscoped_ptr<RpcServer> rpc_server_;

  // The singular hosted tablet.
  // TODO: This will be replaced with some kind of map of tablet ID to
  // tablet in the future.
  std::tr1::shared_ptr<tablet::Tablet> tablet_;

  DISALLOW_COPY_AND_ASSIGN(TabletServer);
};

} // namespace tserver
} // namespace kudu
#endif
