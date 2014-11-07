// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TSERVER_TABLET_SERVER_OPTIONS_H
#define KUDU_TSERVER_TABLET_SERVER_OPTIONS_H

#include <vector>

#include "kudu/server/server_base_options.h"
#include "kudu/util/net/net_util.h"

namespace kudu {
namespace tserver {

// Options for constructing a tablet server.
// These are filled in by gflags by default -- see the .cc file for
// the list of options and corresponding flags.
//
// This allows tests to easily start miniclusters with different
// tablet servers having different options.
struct TabletServerOptions : public kudu::server::ServerBaseOptions {
  TabletServerOptions();

  std::vector<HostPort> master_addresses;
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_TABLET_SERVER_OPTIONS_H */
