// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_SERVER_SERVER_BASE_OPTIONS_H
#define KUDU_SERVER_SERVER_BASE_OPTIONS_H

#include <string>

#include "server/webserver_options.h"
#include "server/rpc_server.h"

namespace kudu {

class Env;

namespace server {

// Options common to both types of servers.
// The subclass constructor should fill these in with defaults from
// server-specific flags.
struct ServerBaseOptions {
  Env* env;

  std::string base_dir;
  RpcServerOptions rpc_opts;
  WebserverOptions webserver_opts;

  std::string dump_info_path;
};

} // namespace server
} // namespace kudu
#endif /* KUDU_SERVER_SERVER_BASE_OPTIONS_H */
