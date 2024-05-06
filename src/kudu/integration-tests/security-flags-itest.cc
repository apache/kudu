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

#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/flags.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_string(rpc_authentication);

using gflags::SetCommandLineOption;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;

namespace kudu {
namespace security {

class SecurityFlagsTest : public KuduTest {
};

TEST_F(SecurityFlagsTest, CheckRpcAuthnFlagsGroupValidator) {
  // Do not rely on default values for the flags in question but explicitly
  // set them to the required values instead to verify the functionality
  // of the corresponding group flag validator.
  ASSERT_NE("", SetCommandLineOption("unlock_experimental_flags", "true"));

  ASSERT_NE("", SetCommandLineOption("rpc_authentication", "required"));
  // This check has two purposes. The first purpose is that it verifies that the
  // flag is set up correctly. The second purpose is that linker can omit whole
  // library files when no function or variable is used in them. This can happen
  // even if the variable's constructor has some side effects. This happenned
  // with the command line arguments in release build in some cases. As a
  // solution, FLAGS_rpc_authentication is used and as a consequence, all the
  // global variable constructors are called.
  ASSERT_EQ("required", FLAGS_rpc_authentication);

  ASSERT_NE("", SetCommandLineOption("keytab_file", ""));
  ASSERT_NE("", SetCommandLineOption("rpc_certificate_file", ""));
  ASSERT_DEATH({ ValidateFlags(); },
               ".*RPC authentication \\(--rpc_authentication\\) may not be "
               "required unless Kerberos \\(--keytab_file) or external PKI "
               "\\(--rpc_certificate_file et al\\) are configured"
               ".*Detected inconsistency in command-line flags; exiting");
}

TEST_F(SecurityFlagsTest, RpcAuthnFlagsMasterStartup) {
  ExternalMiniClusterOptions opts;

  // Explicitly disable Kerberos authentication.
  opts.enable_kerberos = false;

  // Trying to start only master: it should detect an inconsistency in its
  // command-line flags.
  opts.num_masters = 1;
  opts.num_tablet_servers = 0;

  // Explicitly require RPC authentication for the master.
  opts.extra_master_flags = { "--rpc_authentication=required" };

  ExternalMiniCluster cluster(std::move(opts));
  const auto s = cluster.Start();

  // Master should not be running.
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  const auto& errmsg = s.ToString();
  // Master should detect an inconsistency in the command-line flags.
  ASSERT_STR_CONTAINS(errmsg, "Unable to start Master");
  ASSERT_STR_CONTAINS(errmsg, "process exited with non-zero status 1");
}

TEST_F(SecurityFlagsTest, RpcAuthnFlagsTabletServerStartup) {
  ExternalMiniClusterOptions opts;

  // Explicitly disable Kerberos authentication.
  opts.enable_kerberos = false;

  // The cluster has one master and just one tablet server. To simplify this
  // test scenario which relies on the ExternalMiniCluster's functionality,
  // allow the single master to start, but the tablet server should not be able
  // to start since it should detect an inconsistency in the command-line flags.
  opts.num_masters = 1;
  opts.num_tablet_servers = 1;

  // Explicitly require RPC authentication for the tablet server.
  // The master doesn't have this extra flag, so it should be able
  // to start up just fine.
  opts.extra_tserver_flags = { "--rpc_authentication=required" };

  cluster::ExternalMiniCluster cluster(std::move(opts));
  const auto s = cluster.Start();

  // Master should be able to start up and be running.
  ASSERT_TRUE(cluster.master(0)->IsProcessAlive());
  // However, the tablet server shouldn't be running.
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  const auto& errmsg = s.ToString();
  ASSERT_STR_CONTAINS(errmsg, "failed to start tablet server");
  ASSERT_STR_CONTAINS(errmsg, "process exited with non-zero status 1");
}

} // namespace security
} // namespace kudu
