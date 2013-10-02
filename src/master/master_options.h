// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_MASTER_OPTIONS_H
#define KUDU_MASTER_MASTER_OPTIONS_H

#include <string>

#include "server/webserver_options.h"
#include "server/rpc_server.h"

namespace kudu {
class Env;

namespace master {

// Options for constructing the master.
// These are filled in by gflags by default -- see the .cc file for
// the list of options and corresponding flags.
struct MasterOptions {
  MasterOptions();

  RpcServerOptions rpc_opts;
  WebserverOptions webserver_opts;

  Env* env;
  std::string base_dir;
};

} // namespace master
} // namespace kudu
#endif /* KUDU_MASTER_MASTER_OPTIONS_H */
