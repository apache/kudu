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

#include "kudu/master/master_options.h"

#include <ostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/master/master.h"
#include "kudu/server/rpc_server.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/status.h"

DEFINE_string(master_addresses, "",
              "Comma-separated list of the RPC addresses belonging to all "
              "Masters in this cluster. "
              "NOTE: if not specified or a single address is specified, "
              "configures a non-replicated Master.");
TAG_FLAG(master_addresses, stable);

namespace kudu {
namespace master {

MasterOptions::MasterOptions()
    : block_cache_metrics_policy_(Cache::ExistingMetricsPolicy::kKeep) {
  rpc_opts.default_port = Master::kDefaultPort;

  if (!FLAGS_master_addresses.empty()) {
    Status s = HostPort::ParseStrings(FLAGS_master_addresses, Master::kDefaultPort,
                                      &master_addresses_);
    if (!s.ok()) {
      LOG(FATAL) << "Couldn't parse the master_addresses flag('" << FLAGS_master_addresses << "'): "
                 << s.ToString();
    }
    // TODO(wdberkeley): Un-actionable warning. Link to docs, once they exist.
    if (master_addresses_.size() == 2) {
      LOG(WARNING) << "Only 2 masters are specified by master_addresses_flag ('" <<
          FLAGS_master_addresses << "'), but minimum of 3 are required to tolerate failures"
          " of any one master. It is recommended to use at least 3 masters.";
    }
  }
}

Status MasterOptions::GetTheOnlyMasterAddress(HostPort* hp) const {
  if (IsDistributed()) {
    return Status::IllegalState("Not a single master configuration");
  }
  if (master_addresses_.empty()) {
    return Status::NotFound("Master address not specified");
  }
  *hp = master_addresses_.front();
  return Status::OK();
}

} // namespace master
} // namespace kudu
