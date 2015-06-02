// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_MASTER_MINI_MASTER_H
#define KUDU_MASTER_MINI_MASTER_H

#include <string>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/env.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

namespace kudu {

class HostPort;

namespace master {

class Master;
struct MasterOptions;

// An in-process Master meant for use in test cases.
//
// TODO: Store the distributed cluster configuration in the object, to avoid
// having multiple Start methods.
class MiniMaster {
 public:
  MiniMaster(Env* env, const std::string& fs_root, uint16_t rpc_port);
  ~MiniMaster();

  // Start a master running on the loopback interface and
  // an ephemeral port. To determine the address that the server
  // bound to, call MiniMaster::bound_addr()
  Status Start();

  Status StartDistributedMaster(const std::vector<uint16_t>& peer_ports);

  Status WaitForCatalogManagerInit();

  void Shutdown();

  // Restart the master on the same ports as it was previously bound.
  // Requires that the master is currently started.
  Status Restart();

  const Sockaddr bound_rpc_addr() const;
  const Sockaddr bound_http_addr() const;

  const Master* master() const { return master_.get(); }
  Master* master() { return master_.get(); }

  // Return UUID of this mini master.
  std::string permanent_uuid() const;

  std::string bound_rpc_addr_str() const;

 private:
  Status StartDistributedMasterOnPorts(uint16_t rpc_port, uint16_t web_port,
                                       const std::vector<uint16_t>& peer_ports);

  Status StartOnPorts(uint16_t rpc_port, uint16_t web_port);

  Status StartOnPorts(uint16_t rpc_port, uint16_t web_port,
                      MasterOptions* options);

  bool running_;

  ATTRIBUTE_MEMBER_UNUSED Env* const env_;
  const std::string fs_root_;
  const uint16_t rpc_port_;

  gscoped_ptr<Master> master_;
};

} // namespace master
} // namespace kudu

#endif /* KUDU_MASTER_MINI_MASTER_H */
