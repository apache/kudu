// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_MINI_MASTER_H
#define KUDU_MASTER_MINI_MASTER_H

#include <string>

#include "gutil/macros.h"
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

  const Sockaddr bound_rpc_addr() const;
  const Sockaddr bound_http_addr() const;

  const Master* server() const { return server_.get(); }
  Master* server() { return server_.get(); }

 private:
  bool started_;
  Env* const env_;
  const std::string fs_root_;

  gscoped_ptr<Master> server_;
};

} // namespace master
} // namespace kudu

#endif /* KUDU_MASTER_MINI_MASTER_H */
