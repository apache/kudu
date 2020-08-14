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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/master/master.h"
#include "kudu/server/rpc_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

DEFINE_string(tserver_master_addrs, "127.0.0.1:7051",
              "Comma separated addresses of the masters which the "
              "tablet server should connect to. The masters do not "
              "read this flag -- configure the masters separately "
              "using 'rpc_bind_addresses'.");
TAG_FLAG(tserver_master_addrs, stable);

DEFINE_uint32(tablet_apply_pool_overload_threshold_ms, 0,
              "The threshold for the queue time of the 'apply' thread pool "
              "to enter and exit overloaded state. Once the queue stalls and "
              "its queue times become longer than the specified threshold, it "
              "enters the overloaded state. Tablet server rejects incoming "
              "write requests with some probability when its apply queue is "
              "overloaded. The longer the apply queue stays overloaded, the "
              "greater the probability of the rejection. In addition, the more "
              "row operations a write request has, the greater the probablity "
              "of the rejection. The apply queue exits the overloaded state "
              "when queue times drop below the specified threshold. Set this "
              "flag to 0 to disable the behavior described above.");
TAG_FLAG(tablet_apply_pool_overload_threshold_ms, advanced);

namespace kudu {
namespace tserver {

TabletServerOptions::TabletServerOptions() {
  rpc_opts.default_port = TabletServer::kDefaultPort;
  if (FLAGS_tablet_apply_pool_overload_threshold_ms > 0) {
    apply_queue_overload_threshold = MonoDelta::FromMilliseconds(
        FLAGS_tablet_apply_pool_overload_threshold_ms);
  }
  Status s = HostPort::ParseStrings(FLAGS_tserver_master_addrs,
                                    master::Master::kDefaultPort,
                                    &master_addresses);
  if (!s.ok()) {
    LOG(FATAL) << "Couldn't parse tablet_server_master_addrs flag: " << s.ToString();
  }
}

} // namespace tserver
} // namespace kudu
