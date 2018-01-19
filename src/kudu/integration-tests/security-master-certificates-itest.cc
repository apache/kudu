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

#include <memory>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/security/tls_context.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(enable_leader_failure_detection);

using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::consensus::RaftConsensus;

using std::unique_ptr;


namespace kudu {

class SecurityMasterCertsTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    // This test requires manual system catalog leader election.
    FLAGS_enable_leader_failure_detection = false;

    InternalMiniClusterOptions opts;
    opts.master_rpc_ports = { 11010, 11011, 11012, 11013, 11014, };
    opts.num_masters = opts.master_rpc_ports.size();
    opts.num_tablet_servers = 0;
    cluster_.reset(new InternalMiniCluster(env_, opts));
    ASSERT_OK(cluster_->Start());
  }

  void TearDown() override {
    cluster_->Shutdown();
  }

 protected:
  unique_ptr<InternalMiniCluster> cluster_;
};

// This scenario verifies that follower masters get CA-signed certificates
// even if they haven't run in the leader role yet. In this particular scenario,
// only one of the masters has ever become a leader and the rest have always
// been followers. This is a test to cover regressions of KUDU-2265, if any.
TEST_F(SecurityMasterCertsTest, FollowerCertificates) {
  for (auto i = 0; i < cluster_->num_masters(); ++i) {
    const auto& tls = cluster_->mini_master(i)->master()->tls_context();
    // Initially, all master servers have self-signed certs,
    // but none has CA-signed cert.
    ASSERT_FALSE(tls.has_signed_cert());
    ASSERT_TRUE(tls.has_cert());
  }

  auto* consensus = cluster_->mini_master(0)->master()->catalog_manager()->
      sys_catalog()->tablet_replica()->consensus();
  ASSERT_OK(consensus->StartElection(
      RaftConsensus::ELECT_EVEN_IF_LEADER_IS_ALIVE,
      RaftConsensus::EXTERNAL_REQUEST));

  // After some time, all master servers should have CA-signed certs.
  ASSERT_EVENTUALLY([&] {
    for (auto i = 0; i < cluster_->num_masters(); ++i) {
      const auto& tls = cluster_->mini_master(i)->master()->tls_context();
      ASSERT_TRUE(tls.has_signed_cert());
    }
  });
}

} // namespace kudu
