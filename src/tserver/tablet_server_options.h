// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_TABLET_SERVER_OPTIONS_H
#define KUDU_TSERVER_TABLET_SERVER_OPTIONS_H

#include <string>

#include "server/webserver_options.h"
#include "server/rpc_server.h"
#include "util/net/net_util.h"

namespace kudu {
class Env;

namespace tserver {

// Options for constructing a tablet server.
// These are filled in by gflags by default -- see the .cc file for
// the list of options and corresponding flags.
//
// This allows tests to easily start miniclusters with different
// tablet servers having different options.
struct TabletServerOptions {
  TabletServerOptions();

  RpcServerOptions rpc_opts;
  WebserverOptions webserver_opts;

  Env* env;
  std::string base_dir;
  HostPort master_hostport;
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_TABLET_SERVER_OPTIONS_H */
