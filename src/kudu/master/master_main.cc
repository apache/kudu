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

#include <iostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/flags.h"
#include "kudu/util/init.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/version_info.h"

using kudu::master::Master;

DECLARE_bool(evict_failed_followers);
DECLARE_int32(webserver_port);
DECLARE_string(rpc_bind_addresses);

DECLARE_bool(hive_metastore_sasl_enabled);
DECLARE_string(keytab_file);

namespace kudu {
namespace master {

namespace {
// Validates that if the HMS is configured with SASL enabled, the server has a
// keytab available. This is located in master.cc because the HMS module (where
// -hive_metastore_sasl_enabled is defined) doesn't link to the server module
// (where --keytab_file is defined), and vice-versa. The master module is the
// first module which links to both.
// Note: this check only needs to be run on a server. E.g. tools that run with
// the HMS don't need to pass in a keytab.
bool ValidateHiveMetastoreSaslEnabled() {
  if (FLAGS_hive_metastore_sasl_enabled &&
      FLAGS_keytab_file.empty()) {
    LOG(ERROR) << "When the Hive Metastore has SASL enabled "
                  "(--hive_metastore_sasl_enabled), Kudu must be configured with "
                  "a keytab (--keytab_file).";
    return false;
  }
  return true;
}
GROUP_FLAG_VALIDATOR(hive_metastore_sasl_enabled, ValidateHiveMetastoreSaslEnabled);
} // anonymous namespace

static int MasterMain(int argc, char** argv) {
  RETURN_MAIN_NOT_OK(InitKudu(), "InitKudu() failed", 1);

  // Reset some default values before parsing gflags.
  FLAGS_rpc_bind_addresses = strings::Substitute("0.0.0.0:$0",
                                                 Master::kDefaultPort);
  FLAGS_webserver_port = Master::kDefaultWebPort;

  // A multi-node Master leader should not evict failed Master followers
  // because there is no-one to assign replacement servers in order to maintain
  // the desired replication factor. (It's not turtles all the way down!)
  FLAGS_evict_failed_followers = false;

  // Setting the default value of the 'force_block_cache_capacity' flag to
  // 'false' makes the corresponding group validator enforce proper settings
  // for the memory limit and the cfile cache capacity.
  CHECK_NE("", SetCommandLineOptionWithMode("force_block_cache_capacity",
        "false", gflags::SET_FLAGS_DEFAULT));

  GFlagsMap default_flags = GetFlagsMap();

  ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 1) {
    std::cerr << "usage: " << argv[0] << std::endl;
    return 2;
  }
  std::string nondefault_flags = GetNonDefaultFlags(default_flags);
  InitGoogleLoggingSafe(argv[0]);

  LOG(INFO) << "Master server non-default flags:\n"
            << nondefault_flags << '\n'
            << "Master server version:\n"
            << VersionInfo::GetAllVersionInfo();

  Master server({});
  RETURN_MAIN_NOT_OK(server.Init(), "Init() failed", 3);
  RETURN_MAIN_NOT_OK(server.Start(), "Start() failed", 4);

  while (true) {
    SleepFor(MonoDelta::FromSeconds(60));
  }

  return 0;
}

} // namespace master
} // namespace kudu

int main(int argc, char** argv) {
  return kudu::master::MasterMain(argc, argv);
}
