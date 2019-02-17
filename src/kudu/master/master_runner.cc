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

#include "kudu/master/master_runner.h"

#include <iostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/util/flags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/version_info.h"

using kudu::master::Master;
using std::string;

DECLARE_bool(evict_failed_followers);

namespace kudu {
namespace master {

void SetMasterFlagDefaults() {
  // Reset some default values before parsing gflags.
  CHECK_NE("", google::SetCommandLineOptionWithMode("rpc_bind_addresses",
                                                    strings::Substitute(
                                                        "0.0.0.0:$0",
                                                        Master::kDefaultPort).c_str(),
                                                    google::FlagSettingMode::SET_FLAGS_DEFAULT));
  CHECK_NE("", google::SetCommandLineOptionWithMode("webserver_port",
                                                    std::to_string(
                                                        Master::kDefaultWebPort).c_str(),
                                                    google::FlagSettingMode::SET_FLAGS_DEFAULT));

  // Setting the default value of the 'force_block_cache_capacity' flag to
  // 'false' makes the corresponding group validator enforce proper settings
  // for the memory limit and the cfile cache capacity.
  CHECK_NE("", SetCommandLineOptionWithMode("force_block_cache_capacity",
                                            "false",
                                            gflags::SET_FLAGS_DEFAULT));

  // A multi-node Master leader should not evict failed Master followers
  // because there is no-one to assign replacement servers in order to maintain
  // the desired replication factor. (It's not turtles all the way down!)
  CHECK_NE("", SetCommandLineOptionWithMode("evict_failed_followers",
                                            "false",
                                            gflags::SET_FLAGS_DEFAULT));
  // SET_FLAGS_DEFAULT won't reset the flag value if it has previously been
  // set, instead it will only change the default. Because we want to ensure
  // evict_failed_followers is always false, we explicitly set the flag.
  FLAGS_evict_failed_followers = false;
}

Status RunMasterServer() {
  string nondefault_flags = GetNonDefaultFlags();
  LOG(INFO) << "Master server non-default flags:\n"
            << nondefault_flags << '\n'
            << "Master server version:\n"
            << VersionInfo::GetAllVersionInfo();

  Master server({});
  RETURN_NOT_OK(server.Init());
  RETURN_NOT_OK(server.Start());

  while (true) {
    SleepFor(MonoDelta::FromSeconds(60));
  }
}

} // namespace master
} // namespace kudu
