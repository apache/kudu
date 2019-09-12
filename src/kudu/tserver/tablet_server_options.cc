// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/tserver/tablet_server_options.h"

#include <ostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/macros.h"
#ifdef FB_DO_NOT_REMOVE
#include "kudu/master/master.h"
#endif
#include "kudu/server/rpc_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/status.h"

DEFINE_string(tserver_addresses, "",
              "Comma-separated list of the RPC addresses belonging to all "
              "instances in this cluster. "
              "NOTE: if not specified, configures a non-replicated Master.");
TAG_FLAG(tserver_addresses, stable);

namespace kudu {
namespace tserver {

TabletServerOptions::TabletServerOptions() {
  rpc_opts.default_port = TabletServer::kDefaultPort;

  if (!FLAGS_tserver_addresses.empty()) {
    Status s = HostPort::ParseStrings(FLAGS_tserver_addresses, TabletServer::kDefaultPort,
                                      &tserver_addresses);
    if (!s.ok()) {
      LOG(FATAL) << "Couldn't parse the tserver_addresses flag('" << FLAGS_tserver_addresses << "'): "
                 << s.ToString();
    }
    if (tserver_addresses.size() < 2) {
      LOG(FATAL) << "At least 2 tservers are required for a distributed config, but "
          "tserver_addresses flag ('" << FLAGS_tserver_addresses << "') only specifies "
                 << tserver_addresses.size() << " tservers.";
    }

    // TODO(wdberkeley): Un-actionable warning. Link to docs, once they exist.
    if (tserver_addresses.size() == 2) {
      LOG(WARNING) << "Only 2 tservers are specified by tserver_addresses_flag ('" <<
          FLAGS_tserver_addresses << "'), but minimum of 3 are required to tolerate failures"
          " of any one tserver. It is recommended to use at least 3 tservers.";
    }
  }
}

bool TabletServerOptions::IsDistributed() const {
  return !tserver_addresses.empty();
}

} // namespace tserver
} // namespace kudu
