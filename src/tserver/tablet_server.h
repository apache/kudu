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

namespace rpc {
class Messenger;
class ServicePool;
}

namespace tserver {

// Server-specific options. Typically, when the tablet server is launched,
// these options are parsed from gflags by TabletServerMain.
//
// See the flags at the top of tablet_server_main for descriptions
// of each option.
struct TabletServerOptions {
  TabletServerOptions();

  string rpc_bind_addresses;
  uint32_t num_rpc_reactors;
  uint32_t num_acceptors_per_address;
  uint32_t num_service_threads;
};

class TabletServer {
 public:
  static const uint16_t kDefaultPort = 7150;

  explicit TabletServer(const TabletServerOptions& opts);
  ~TabletServer();

  Status Init();
  Status Start();

  string ToString() const;

  // Return the addresses that this server has successfully
  // bound to. Requires that the server has been Start()ed.
  void GetBoundAddresses(std::vector<Sockaddr>* addresses);

 private:
  friend class TabletServerTest;

  Status StartRpcServer();

  const TabletServerOptions options_;
  bool initted_;

  // Parsed addresses to bind RPC to. Set by Init()
  std::vector<Sockaddr> rpc_bind_addresses_;

  // RPC messenger.
  std::tr1::shared_ptr<rpc::Messenger> rpc_messenger_;
  gscoped_ptr<rpc::ServicePool> rpc_service_pool_;

  DISALLOW_COPY_AND_ASSIGN(TabletServer);
};

} // namespace tserver
} // namespace kudu
#endif
