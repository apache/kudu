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
#include <boost/algorithm/string.hpp>

// TODO - iRitwik ( please refine these mechanisms to a standard way of
// passing all the properties of an instance )
DEFINE_string(tserver_addresses, "",
              "Comma-separated list of the RPC addresses belonging to all "
              "instances in this cluster. "
              "NOTE: if not specified, configures a non-replicated Master.");
TAG_FLAG(tserver_addresses, stable);

DEFINE_string(tserver_regions, "",
              "Comma-separated list of regions which is parallel to tserver_addresses.");
TAG_FLAG(tserver_regions, stable);

DEFINE_string(tserver_bbd, "",
              "Comma-separated list of bool strings to specify Backed by "
              "Database(non-witness). Runs parallel to tserver_addresses.");
TAG_FLAG(tserver_bbd, stable);

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

#ifdef FB_DO_NOT_REMOVE
// to simplify in FB, we allow rings with single instances.
    if (tserver_addresses.size() < 2) {
      LOG(FATAL) << "At least 2 tservers are required for a distributed config, but "
          "tserver_addresses flag ('" << FLAGS_tserver_addresses << "') only specifies "
                 << tserver_addresses.size() << " tservers.";
    }
#endif

    // TODO(wdberkeley): Un-actionable warning. Link to docs, once they exist.
    if (tserver_addresses.size() <= 2) {
      LOG(WARNING) << "Only 2 tservers are specified by tserver_addresses_flag ('" <<
          FLAGS_tserver_addresses << "'), but minimum of 3 are required to tolerate failures"
          " of any one tserver. It is recommended to use at least 3 tservers.";
    }
  }

  if (!FLAGS_tserver_regions.empty()) {
    boost::split(tserver_regions, FLAGS_tserver_regions, boost::is_any_of(","));
    if (tserver_regions.size() != tserver_addresses.size()) {
      LOG(FATAL) << "The number of tserver regions has to be same as tservers: "
          << FLAGS_tserver_regions << " " << FLAGS_tserver_addresses;
    }
  }
  if (!FLAGS_tserver_bbd.empty()) {
    std::vector<std::string> bbds;
    boost::split(bbds, FLAGS_tserver_bbd, boost::is_any_of(","));
    if (bbds.size() != tserver_addresses.size()) {
      LOG(FATAL) << "The number of tserver bbd tags has to be same as tservers: "
          << FLAGS_tserver_bbd << " " << FLAGS_tserver_addresses;
    }
    for (auto tsbbd: bbds) {
      if (tsbbd == "true") {
        tserver_bbd.push_back(true);
      } else if (tsbbd == "false") {
        tserver_bbd.push_back(false);
      } else {
        LOG(FATAL) << "tserver bbd tags has to be bool true|false : "
            << FLAGS_tserver_bbd;
      }
    }
  }
}

bool TabletServerOptions::IsDistributed() const {
  return !tserver_addresses.empty();
}

} // namespace tserver
} // namespace kudu
