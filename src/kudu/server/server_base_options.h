// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_SERVER_SERVER_BASE_OPTIONS_H
#define KUDU_SERVER_SERVER_BASE_OPTIONS_H

#include <string>
#include <vector>

#include "kudu/fs/fs_manager.h"
#include "kudu/server/webserver_options.h"
#include "kudu/server/rpc_server.h"

namespace kudu {

class Env;

namespace server {

// Options common to both types of servers.
// The subclass constructor should fill these in with defaults from
// server-specific flags.
struct ServerBaseOptions {
  Env* env;

  FsManagerOpts fs_opts;
  RpcServerOptions rpc_opts;
  WebserverOptions webserver_opts;

  std::string dump_info_path;
  std::string dump_info_format;

  int32_t metrics_log_interval_ms;

 protected:
  ServerBaseOptions();
};

} // namespace server
} // namespace kudu
#endif /* KUDU_SERVER_SERVER_BASE_OPTIONS_H */
