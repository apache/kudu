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
#pragma once

#include <utility>
#include <vector>

#include "kudu/kserver/kserver_options.h"
#include "kudu/util/cache.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace kudu {

namespace master {

// Options for constructing the master.
// These are filled in by gflags by default -- see the .cc file for
// the list of options and corresponding flags.
struct MasterOptions : public kserver::KuduServerOptions {
  MasterOptions();

  // Fetch master addresses from the user supplied gflags which may be empty
  // for single master configuration.
  // NOTE: Only to be used during master init time as masters can be added/removed dynamically.
  // Use Master::GetMasterHostPorts() instead after initializing the master at runtime.
  const std::vector<HostPort>& master_addresses() const {
    return master_addresses_;
  }

  // Only to be used only during init time as masters can be added/removed dynamically.
  bool IsDistributed() const {
    return master_addresses_.size() > 1;
  }

  // For a single master configuration output the only master address in 'hp', if available.
  // Otherwise NotFound error or IllegalState for distributed master config.
  Status GetTheOnlyMasterAddress(HostPort* hp) const;

  // Allows setting/overwriting list of masters. Only to be used by tests.
  void SetMasterAddressesForTests(std::vector<HostPort> addresses) {
    master_addresses_ = std::move(addresses);
  }

  void set_block_cache_metrics_policy(Cache::ExistingMetricsPolicy b) {
    block_cache_metrics_policy_ = b;
  }

  Cache::ExistingMetricsPolicy block_cache_metrics_policy() const {
    return block_cache_metrics_policy_;
  }

 private:
  // The list of deduplicated masters, as specified by --master_addresses.
  std::vector<HostPort> master_addresses_;
  Cache::ExistingMetricsPolicy block_cache_metrics_policy_;
};

} // namespace master
} // namespace kudu
