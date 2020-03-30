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

#include <algorithm>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/replica_management.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/master_cert_authority.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/security/ca/cert_management.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/tls_context.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/user.h"

DECLARE_bool(raft_prepare_replacement_before_eviction);

using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::consensus::ReplicaManagementInfoPB;
using kudu::security::Cert;
using kudu::security::CertSignRequest;
using kudu::security::DataFormat;
using kudu::security::PrivateKey;
using kudu::security::ca::CertRequestGenerator;
using std::back_inserter;
using std::copy;
using std::string;
using std::vector;
using strings::Substitute;


namespace kudu {
namespace master {

class MasterCertAuthorityTest : public KuduTest {
 public:
  MasterCertAuthorityTest() {
    opts_.num_masters = 3;
  }

  void SetUp() override {
    KuduTest::SetUp();
    cluster_.reset(new InternalMiniCluster(env_, opts_));
    ASSERT_OK(cluster_->Start());

    rpc::MessengerBuilder bld("Client");
    ASSERT_OK(bld.Build(&messenger_));
  }

  Status RestartCluster() {
    cluster_->Shutdown();
    return cluster_->Start();
  }

  // Check the leader is found on the cluster.
  Status WaitForLeader() {
    return cluster_->GetLeaderMasterIndex(nullptr);
  }

  void GetCurrentCertAuthInfo(string* ca_pkey_str, string* ca_cert_str) {
    int leader_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    MiniMaster* leader = cluster_->mini_master(leader_idx);
    Master* master = leader->master();

    auto ca_pkey = master->cert_authority()->ca_private_key_.get();
    ASSERT_NE(nullptr, ca_pkey);
    ASSERT_OK(ca_pkey->ToString(ca_pkey_str, DataFormat::PEM));

    auto ca_cert = master->cert_authority()->ca_cert_.get();
    ASSERT_NE(nullptr, ca_cert);
    ASSERT_OK(ca_cert->ToString(ca_cert_str, DataFormat::PEM));
  }

  void SendRegistrationHBs() {
    TSToMasterCommonPB common;
    common.mutable_ts_instance()->set_permanent_uuid(kFakeTsUUID);
    common.mutable_ts_instance()->set_instance_seqno(1);
    ServerRegistrationPB fake_reg;
    HostPortPB* pb = fake_reg.add_rpc_addresses();
    pb->set_host("localhost");
    pb->set_port(1000);
    pb = fake_reg.add_http_addresses();
    pb->set_host("localhost");
    pb->set_port(2000);

    // Information on the replica management scheme.
    ReplicaManagementInfoPB rmi;
    rmi.set_replacement_scheme(FLAGS_raft_prepare_replacement_before_eviction
        ? ReplicaManagementInfoPB::PREPARE_REPLACEMENT_BEFORE_EVICTION
        : ReplicaManagementInfoPB::EVICT_FIRST);

    for (int i = 0; i < cluster_->num_masters(); ++i) {
      TSHeartbeatRequestPB req;
      TSHeartbeatResponsePB resp;
      rpc::RpcController rpc;

      req.mutable_common()->CopyFrom(common);
      req.mutable_registration()->CopyFrom(fake_reg);
      req.mutable_replica_management_info()->CopyFrom(rmi);

      MiniMaster* m = cluster_->mini_master(i);
      if (!m->is_started()) {
        continue;
      }
      MasterServiceProxy proxy(messenger_, m->bound_rpc_addr(), m->bound_rpc_addr().host());

      // All masters (including followers) should accept the heartbeat.
      ASSERT_OK(proxy.TSHeartbeat(req, &resp, &rpc));
      SCOPED_TRACE(pb_util::SecureDebugString(resp));
      ASSERT_FALSE(resp.has_error());
    }
  }

  Status SendCertSignRequestHBs(const string& csr_str,
                                bool* has_signed_certificate,
                                string* signed_certificate) {
    TSToMasterCommonPB common;
    common.mutable_ts_instance()->set_permanent_uuid(kFakeTsUUID);
    common.mutable_ts_instance()->set_instance_seqno(1);

    string ts_cert_str;
    bool has_leader_master_response = false;
    for (int i = 0; i < cluster_->num_masters(); ++i) {
      TSHeartbeatRequestPB req;
      TSHeartbeatResponsePB resp;
      rpc::RpcController rpc;

      req.mutable_common()->CopyFrom(common);
      req.set_csr_der(csr_str);

      MiniMaster* m = cluster_->mini_master(i);
      if (!m->is_started()) {
        continue;
      }
      MasterServiceProxy proxy(messenger_, m->bound_rpc_addr(), m->bound_rpc_addr().host());

      // All masters (including followers) should accept the heartbeat.
      RETURN_NOT_OK(proxy.TSHeartbeat(req, &resp, &rpc));
      SCOPED_TRACE(pb_util::SecureDebugString(resp));
      if (resp.has_error()) {
        return Status::RuntimeError("RPC error", resp.error().ShortDebugString());
      }

      // Only the leader sends back the signed server certificate.
      if (resp.leader_master()) {
        has_leader_master_response = true;
        ts_cert_str = resp.signed_cert_der();
      } else {
        if (resp.has_signed_cert_der()) {
          return Status::RuntimeError("unexpected cert returned from non-leader");
        }
      }
    }
    if (has_leader_master_response) {
      *has_signed_certificate = !ts_cert_str.empty();
      *signed_certificate = ts_cert_str;
    }
    return Status::OK();
  }

 protected:
  static const char kFakeTsUUID[];

  InternalMiniClusterOptions opts_;
  std::unique_ptr<InternalMiniCluster> cluster_;
  std::shared_ptr<rpc::Messenger> messenger_;
};
const char MasterCertAuthorityTest::kFakeTsUUID[] = "fake-ts-uuid";

// If no certificate authority information is present, a new one is generated
// upon start of the cluster. So, check that the CA information is available
// upon start of the cluster and it stays the same after the cluster restarts.
TEST_F(MasterCertAuthorityTest, CertAuthorityPersistsUponRestart) {
  string ref_ca_pkey_str;
  string ref_ca_cert_str;
  NO_FATALS(GetCurrentCertAuthInfo(&ref_ca_pkey_str, &ref_ca_cert_str));

  ASSERT_OK(RestartCluster());

  string ca_pkey_str;
  string ca_cert_str;
  NO_FATALS(GetCurrentCertAuthInfo(&ca_pkey_str, &ca_cert_str));

  EXPECT_EQ(ref_ca_pkey_str, ca_pkey_str);
  EXPECT_EQ(ref_ca_cert_str, ca_cert_str);
}

// Check that current master leader posesses the CA private key and certificate
// and if the leader role switches to some other master server, the new leader
// provides the same information as the former one.
TEST_F(MasterCertAuthorityTest, CertAuthorityOnLeaderRoleSwitch) {
  string ref_pkey_str;
  string ref_cert_str;
  NO_FATALS(GetCurrentCertAuthInfo(&ref_pkey_str, &ref_cert_str));

  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  MiniMaster* leader_master = cluster_->mini_master(leader_idx);
  leader_master->Shutdown();

  string new_leader_pkey_str;
  string new_leader_cert_str;
  NO_FATALS(GetCurrentCertAuthInfo(&new_leader_pkey_str, &new_leader_cert_str));

  EXPECT_EQ(ref_pkey_str, new_leader_pkey_str);
  EXPECT_EQ(ref_cert_str, new_leader_cert_str);
}


void GenerateCSR(const CertRequestGenerator::Config& gen_config,
                 string* csr_str) {
  PrivateKey key;
  ASSERT_OK(security::GeneratePrivateKey(512, &key));
  CertRequestGenerator gen(gen_config);
  ASSERT_OK(gen.Init());
  CertSignRequest csr;
  ASSERT_OK(gen.GenerateRequest(key, &csr));
  ASSERT_OK(csr.ToString(csr_str, DataFormat::DER));
}

TEST_F(MasterCertAuthorityTest, RefuseToSignInvalidCSR) {
  NO_FATALS(SendRegistrationHBs());
  string csr_str;
  {
    CertRequestGenerator::Config gen_config;
    gen_config.hostname = "ts.foo.com";
    gen_config.user_id = "joe-impostor";
    NO_FATALS(GenerateCSR(gen_config, &csr_str));
  }
  ASSERT_OK(WaitForLeader());
  {
    string ts_cert_str;
    bool has_ts_cert = false;
    Status s = SendCertSignRequestHBs(csr_str, &has_ts_cert, &ts_cert_str);
    ASSERT_STR_MATCHES(s.ToString(),
                       "Remote error: Not authorized: invalid CSR: CSR did not "
                       "contain expected username. "
                       "\\(CSR: 'joe-impostor' RPC: '.*'\\)");
  }
}

// Test that every master accepts heartbeats, but only the leader master
// responds with signed certificate if a heartbeat contains the CSR field.
TEST_F(MasterCertAuthorityTest, MasterLeaderSignsCSR) {
  NO_FATALS(SendRegistrationHBs());

  string csr_str;
  {
    CertRequestGenerator::Config gen_config;
    // The hostname is longer than permitted 255 characters and breaks other
    // restrictions on valid DNS hostnames, but it should not be an issue for
    // both the requestor and the signer.
    gen_config.hostname =
        "toooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo."
        "looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
        "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
        "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
        "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
        "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
        "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
        "ng.hostname.io";
    string test_user;
    ASSERT_OK(GetLoggedInUser(&test_user));
    gen_config.user_id = test_user;
    NO_FATALS(GenerateCSR(gen_config, &csr_str));
  }

  // Make sure a tablet server receives signed certificate from
  // the leader master.
  ASSERT_OK(WaitForLeader());
  {
    string ts_cert_str;
    bool has_ts_cert = false;
    NO_FATALS(SendCertSignRequestHBs(csr_str, &has_ts_cert, &ts_cert_str));
    ASSERT_TRUE(has_ts_cert);

    // Try to load the certificate to check that the data is not corrupted.
    Cert ts_cert;
    ASSERT_OK(ts_cert.FromString(ts_cert_str, DataFormat::DER));
  }

  // Shutdown the leader master and check the new leader signs
  // certificate request sent in a tablet server hearbeat.
  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  MiniMaster* leader_master = cluster_->mini_master(leader_idx);
  leader_master->Shutdown();

  // Re-register with the new leader.
  ASSERT_OK(WaitForLeader());
  NO_FATALS(SendRegistrationHBs());
  {
    string ts_cert_str;
    bool has_ts_cert = false;
    NO_FATALS(SendCertSignRequestHBs(csr_str, &has_ts_cert, &ts_cert_str));
    ASSERT_TRUE(has_ts_cert);
    // Try to load the certificate to check that the data is not corrupted.
    Cert ts_cert;
    ASSERT_OK(ts_cert.FromString(ts_cert_str, DataFormat::DER));
  }
}

} // namespace master

namespace client {

class ConnectToClusterBaseTest : public KuduTest {
 public:
  ConnectToClusterBaseTest(int run_time_seconds,
                           int latency_ms,
                           int num_masters)
      : run_time_seconds_(run_time_seconds),
        latency_ms_(latency_ms) {
    cluster_opts_.num_masters = num_masters;
  }

  void SetUp() override {
    KuduTest::SetUp();
    cluster_.reset(new ExternalMiniCluster(cluster_opts_));
  }

  void ConnectToCluster() {
    const MonoDelta timeout(MonoDelta::FromSeconds(run_time_seconds_));
    KuduClientBuilder builder;
    builder.default_admin_operation_timeout(timeout);
    builder.default_rpc_timeout(timeout);
    client::sp::shared_ptr<KuduClient> client;
    ASSERT_OK(cluster_->CreateClient(&builder, &client));
    ASSERT_EQ(1, client->data_->messenger_->tls_context().trusted_cert_count_for_tests());
    ASSERT_NE(boost::none, client->data_->messenger_->authn_token());
  }

  void Run() {
    const MonoTime t_stop = MonoTime::Now() + MonoDelta::FromSeconds(run_time_seconds_);
    CountDownLatch stop_latch(1);
    std::thread clear_latency_thread([&]{
      // Allow the test client to connect to the cluster (avoid timing out).
      const MonoTime clear_latency = t_stop -
          MonoDelta::FromMilliseconds(1000L * run_time_seconds_ / 4);
      stop_latch.WaitUntil(clear_latency);
      for (auto i = 0; i < cluster_->num_masters(); ++i) {
        CHECK_OK(cluster_->SetFlag(cluster_->master(i),
            "catalog_manager_inject_latency_load_ca_info_ms", "0"));
      }
    });
    SCOPED_CLEANUP({
      clear_latency_thread.join();
    });
    while (MonoTime::Now() < t_stop) {
      NO_FATALS(ConnectToCluster());
    }
    stop_latch.CountDown();
    NO_FATALS(cluster_->AssertNoCrashes());
  }

 protected:
  const int run_time_seconds_;
  const int latency_ms_;
  ExternalMiniClusterOptions cluster_opts_;
  std::shared_ptr<ExternalMiniCluster> cluster_;
};

// Test for KUDU-1927 in single-master configuration: verify that Kudu client
// successfully connects to Kudu cluster and always have CA certificate and
// authn token once connected. The test injects random latency into the process
// of loading the CA record from the system table. There is a high chance that
// during start-up the master server responds with ServiceUnavailable to
// ConnectToCluster RPC sent by the client. The client should retry in that
// case, connecting to the cluster eventually. Once successfully connected,
// the client must have Kudu IPKI CA certificate and authn token.
class SingleMasterConnectToClusterTest : public ConnectToClusterBaseTest {
 public:
  SingleMasterConnectToClusterTest()
      : ConnectToClusterBaseTest(5, 2500, 1) {
    // Add master-only flags.
    cluster_opts_.extra_master_flags.push_back(Substitute(
        "--catalog_manager_inject_latency_load_ca_info_ms=$0", latency_ms_));
  }
};

// Test for KUDU-1927 in multi-master configuration: verify that Kudu client
// successfully connects to Kudu cluster and always has CA certificate and
// authn token once connected. The test injects random latency into the process
// of loading the CA record from the system table. In addition, it uses short
// timeouts for leader failure detection. Due to many re-election events,
// sometimes elected master servers respond with ServiceUnavailable to
// ConnectToCluster RPC sent by the client. The client should retry in that
// case, connecting to the cluster eventually. Once successfully connected,
// the client must have Kudu IPKI CA certificate and authn token.
class MultiMasterConnectToClusterTest : public ConnectToClusterBaseTest {
 public:
  MultiMasterConnectToClusterTest()
      : ConnectToClusterBaseTest(120, 2000, 3) {
    constexpr int kHbIntervalMs = 100;
    // Add master-only flags.
    const vector<string> master_flags = {
      Substitute("--catalog_manager_inject_latency_load_ca_info_ms=$0", latency_ms_),
      "--raft_enable_pre_election=false",
      Substitute("--leader_failure_exp_backoff_max_delta_ms=$0", kHbIntervalMs * 3),
      "--leader_failure_max_missed_heartbeat_periods=1.0",
      Substitute("--raft_heartbeat_interval_ms=$0", kHbIntervalMs),
    };
    copy(master_flags.begin(), master_flags.end(),
         back_inserter(cluster_opts_.extra_master_flags));
  }
};

TEST_F(SingleMasterConnectToClusterTest, ConnectToCluster) {
  ASSERT_OK(cluster_->Start());
  Run();
}

TEST_F(MultiMasterConnectToClusterTest, ConnectToCluster) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }
  ASSERT_OK(cluster_->Start());
  Run();
}

} // namespace client
} // namespace kudu
