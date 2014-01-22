// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_MINI_TABLET_SERVER_H
#define KUDU_TSERVER_MINI_TABLET_SERVER_H

#include "common/schema.h"
#include "gutil/macros.h"
#include "tserver/tablet_server_options.h"
#include "util/env.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

#include <string>

namespace kudu {

class FsManager;

namespace metadata {
class QuorumPB;
}

namespace tserver {

class TabletServer;

// An in-process tablet server meant for use in test cases.
class MiniTabletServer {
 public:
  MiniTabletServer(Env* env, const std::string& fs_root);
  ~MiniTabletServer();

  // Return the options which will be used to start the tablet server.
  // If you wish to make changes to these options, they need to be made
  // before calling Start(), or else they will have no effect.
  TabletServerOptions* options() { return &opts_; }

  // Start a tablet server running on the loopback interface and
  // an ephemeral port. To determine the address that the server
  // bound to, call MiniTabletServer::bound_addr().
  // The TS will be initialized asynchronously and then started.
  Status Start();

  // Waits for the tablet server to be fully initialized, including
  // having all tablets bootstrapped.
  Status WaitStarted();

  Status Shutdown();

  // Add a new tablet to the test server, use the default quorum.
  //
  // Requires that the server has already been started with Start().
  Status AddTestTablet(const std::string& table_id,
                       const std::string& tablet_id,
                       const Schema& schema);

  // Add a new tablet to the test server and specify the quorum
  // for the tablet.
  Status AddTestTablet(const std::string& table_id,
                       const std::string& tablet_id,
                       const Schema& schema,
                       const metadata::QuorumPB& quorum);

  const Sockaddr bound_rpc_addr() const;
  const Sockaddr bound_http_addr() const;

  FsManager* fs_manager();

  const TabletServer* server() const { return server_.get(); }
  TabletServer* server() { return server_.get(); }

 private:
  bool started_;
  Env* const env_;
  const std::string fs_root_;

  TabletServerOptions opts_;

  gscoped_ptr<FsManager> fs_manager_;
  gscoped_ptr<TabletServer> server_;
};

} // namespace tserver
} // namespace kudu
#endif
