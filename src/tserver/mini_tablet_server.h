// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_MINI_TABLET_SERVER_H
#define KUDU_TSERVER_MINI_TABLET_SERVER_H

#include "common/schema.h"
#include "gutil/macros.h"
#include "util/env.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

#include <string>

namespace kudu {

class FsManager;

namespace tserver {

class TabletServer;

// An in-process tablet server meant for use in test cases.
class MiniTabletServer {
 public:
  MiniTabletServer(Env* env, const std::string& fs_root);
  ~MiniTabletServer();

  // Start a tablet server running on the loopback interface and
  // an ephemeral port. To determine the address that the server
  // bound to, call MiniTabletServer::bound_addr()
  Status Start();

  // Add a new tablet to the test server.
  //
  // Requires that the server has already been started with Start().
  Status AddTestTablet(const std::string& tablet_id,
                       const Schema& schema);

  const Sockaddr& bound_addr() const;

  const TabletServer* server() const { return server_.get(); }

 private:
  bool started_;
  Env* const env_;
  const std::string fs_root_;

  gscoped_ptr<FsManager> fs_manager_;
  gscoped_ptr<TabletServer> server_;

  Sockaddr bound_addr_;
};

} // namespace tserver
} // namespace kudu
#endif
