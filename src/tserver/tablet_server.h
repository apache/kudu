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

class ScannerManager;

class TabletServer {
 public:
  // TODO: move this out of this header, since clients want to use this
  // constant as well.
  static const uint16_t kDefaultPort = 7150;

  explicit TabletServer(const RpcServerOptions& opts);
  ~TabletServer();

  Status Init();
  Status Start();

  string ToString() const;

  // Register the given tablet to be managed by this tablet server.
  void RegisterTablet(const std::tr1::shared_ptr<tablet::Tablet>& tablet);

  // Lookup the given tablet by its ID.
  // Returns true if the tablet is found successfully.
  bool LookupTablet(const string& tablet_id,
                    std::tr1::shared_ptr<tablet::Tablet>* tablet) const;

  const RpcServer *rpc_server() const { return rpc_server_.get(); }
  ScannerManager* scanner_manager() { return scanner_manager_.get(); }

 private:
  friend class TabletServerTest;

  bool initted_;

  gscoped_ptr<RpcServer> rpc_server_;

  // The singular hosted tablet.
  // TODO: This will be replaced with some kind of map of tablet ID to
  // tablet in the future.
  std::tr1::shared_ptr<tablet::Tablet> tablet_;

  // Manager for open scanners from clients.
  // This is always non-NULL. It is scoped only to minimize header
  // dependencies.
  gscoped_ptr<ScannerManager> scanner_manager_;

  DISALLOW_COPY_AND_ASSIGN(TabletServer);
};

} // namespace tserver
} // namespace kudu
#endif
