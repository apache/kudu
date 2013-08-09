// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_TABLET_SERVER_H
#define KUDU_MASTER_TABLET_SERVER_H

#include <string>
#include <tr1/memory>
#include <vector>

#include <gtest/gtest.h>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "master/master.pb.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

namespace kudu {

namespace rpc {
class Messenger;
class ServicePool;
}

namespace master {

// Server-specific options. Typically, when the master server is launched,
// these options are parsed from gflags by MasterServerMain.
//
// See the flags at the top of master_server_main for descriptions
// of each option.
struct MasterServerOptions {
  MasterServerOptions();

  string rpc_bind_addresses;
  uint32_t num_rpc_reactors;
  uint32_t num_acceptors_per_address;
  uint32_t num_service_threads;
};

class MasterServer {
 public:
  static const uint16_t kDefaultPort = 7150;

  explicit MasterServer(const MasterServerOptions& opts);
  ~MasterServer();

  Status Init();
  Status Start();

  string ToString() const;

  // Return the addresses that this server has successfully
  // bound to. Requires that the server has been Start()ed.
  void GetBoundAddresses(std::vector<Sockaddr>* addresses);

 private:
  friend class MasterServerTest;

  Status StartRpcServer();

  const MasterServerOptions options_;
  bool initted_;

  // Parsed addresses to bind RPC to. Set by Init()
  std::vector<Sockaddr> rpc_bind_addresses_;

  // RPC messenger.
  std::tr1::shared_ptr<rpc::Messenger> rpc_messenger_;
  gscoped_ptr<rpc::ServicePool> rpc_service_pool_;

  DISALLOW_COPY_AND_ASSIGN(MasterServer);
};

} // namespace master
} // namespace kudu
#endif
