// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_MINI_MASTER_H
#define KUDU_MASTER_MINI_MASTER_H

#include <string>

#include "gutil/macros.h"
#include "gutil/port.h"
#include "util/env.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

namespace kudu {
namespace master {

class Master;

// An in-process Master meant for use in test cases.
class MiniMaster {
 public:
  MiniMaster(Env* env, const std::string& fs_root);
  ~MiniMaster();

  // Start a master running on the loopback interface and
  // an ephemeral port. To determine the address that the server
  // bound to, call MiniMaster::bound_addr()
  Status Start();
  void Shutdown();

  // Restart the master on the same ports as it was previously bound.
  // Requires that the master is currently started.
  Status Restart();

  const Sockaddr bound_rpc_addr() const;
  const Sockaddr bound_http_addr() const;

  const Master* master() const { return master_.get(); }
  Master* master() { return master_.get(); }

 private:
  Status StartOnPorts(uint16_t rpc_port, uint16_t web_port);

  bool started_;

  ATTRIBUTE_MEMBER_UNUSED Env* const env_;
  const std::string fs_root_;

  gscoped_ptr<Master> master_;
};

} // namespace master
} // namespace kudu

#endif /* KUDU_MASTER_MINI_MASTER_H */
