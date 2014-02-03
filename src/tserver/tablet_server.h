// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_TABLET_SERVER_H
#define KUDU_TSERVER_TABLET_SERVER_H

#include <string>
#include <vector>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "server/metadata.pb.h"
#include "server/webserver_options.h"
#include "server/server_base.h"
#include "tserver/tablet_server_options.h"
#include "tserver/tserver.pb.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

namespace kudu {

class FsManager;
class Webserver;

namespace rpc {
class Messenger;
class ServicePool;
}

namespace tserver {

class Heartbeater;
class MutatorManager;
class ScannerManager;
class TabletServerPathHandlers;
class TSTabletManager;

class TabletServer : public server::ServerBase {
 public:
  // TODO: move this out of this header, since clients want to use this
  // constant as well.
  static const uint16_t kDefaultPort = 7050;
  static const uint16_t kDefaultWebPort = 8050;

  explicit TabletServer(const TabletServerOptions& opts);
  ~TabletServer();

  // Initializes the tablet server, including the bootstrapping of all
  // existing tablets.
  // Some initialization tasks are asynchronous, such as the bootstrapping
  // of tablets. Caller can block, waiting for the initialization to fully
  // complete by calling WaitInited().
  Status Init();

  // Waits for the tablet server to complete the initialization.
  Status WaitInited();

  Status Start();
  void Shutdown();

  string ToString() const;

  TSTabletManager* tablet_manager() { return tablet_manager_.get(); }

  ScannerManager* scanner_manager() { return scanner_manager_.get(); }
  const ScannerManager* scanner_manager() const { return scanner_manager_.get(); }

 private:
  friend class TabletServerTest;

  Status ValidateMasterAddressResolution() const;

  bool initted_;

  // The options passed at construction time.
  const TabletServerOptions opts_;

  // Manager for tablets which are available on this server.
  gscoped_ptr<TSTabletManager> tablet_manager_;

  // Manager for open scanners from clients.
  // This is always non-NULL. It is scoped only to minimize header
  // dependencies.
  gscoped_ptr<ScannerManager> scanner_manager_;

  // Thread responsible for heartbeating to the master.
  gscoped_ptr<Heartbeater> heartbeater_;

  // Webserver path handlers
  gscoped_ptr<TabletServerPathHandlers> path_handlers_;

  DISALLOW_COPY_AND_ASSIGN(TabletServer);
};

} // namespace tserver
} // namespace kudu
#endif
