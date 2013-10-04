// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_TABLET_SERVER_H
#define KUDU_TSERVER_TABLET_SERVER_H

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "server/metadata.pb.h"
#include "server/webserver_options.h"
#include "server/server_base.h"
#include "tserver/tablet_server_options.h"
#include "tserver/tserver.pb.h"
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

class TSTabletManager;
class MutatorManager;
class ScannerManager;

class TabletServer : public server::ServerBase {
 public:
  // TODO: move this out of this header, since clients want to use this
  // constant as well.
  static const uint16_t kDefaultPort = 7050;
  static const uint16_t kDefaultWebPort = 8050;

  explicit TabletServer(const TabletServerOptions& opts);
  ~TabletServer();

  Status Init();
  Status Start();
  Status Shutdown();

  string ToString() const;

  TSTabletManager* tablet_manager() { return tablet_manager_.get(); }

  ScannerManager* scanner_manager() { return scanner_manager_.get(); }
  const ScannerManager* scanner_manager() const { return scanner_manager_.get(); }

  FsManager* fs_manager() { return fs_manager_.get(); }

 private:
  friend class TabletServerTest;

  bool initted_;
  TabletServerOptions opts_;

  gscoped_ptr<FsManager> fs_manager_;

  metadata::TabletServerPB tablet_server_pb_;

  // Manager for tablets which are available on this server.
  gscoped_ptr<TSTabletManager> tablet_manager_;

  // Manager for open scanners from clients.
  // This is always non-NULL. It is scoped only to minimize header
  // dependencies.
  gscoped_ptr<ScannerManager> scanner_manager_;

  DISALLOW_COPY_AND_ASSIGN(TabletServer);
};

} // namespace tserver
} // namespace kudu
#endif
