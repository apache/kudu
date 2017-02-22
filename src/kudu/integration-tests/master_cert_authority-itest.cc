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
#include <string>

#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/master_cert_authority.h"
#include "kudu/master/mini_master.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/security/ca/cert_management.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/test_util.h"
#include "kudu/util/user.h"

using std::string;
using std::shared_ptr;

namespace kudu {

using security::ca::CertRequestGenerator;
using security::Cert;
using security::CertSignRequest;
using security::DataFormat;
using security::PrivateKey;

namespace master {

class MasterCertAuthorityTest : public KuduTest {
 public:
  MasterCertAuthorityTest() {
    // Hard-coded ports for the masters. This is safe, as this unit test
    // runs under a resource lock (see CMakeLists.txt in this directory).
    // TODO(aserbin): we should have a generic method to obtain n free ports.
    opts_.master_rpc_ports = { 11010, 11011, 11012 };

    opts_.num_masters = num_masters_ = opts_.master_rpc_ports.size();
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    cluster_.reset(new MiniCluster(env_, opts_));
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

    for (int i = 0; i < cluster_->num_masters(); ++i) {
      TSHeartbeatRequestPB req;
      TSHeartbeatResponsePB resp;
      rpc::RpcController rpc;

      req.mutable_common()->CopyFrom(common);
      req.mutable_registration()->CopyFrom(fake_reg);

      MiniMaster* m = cluster_->mini_master(i);
      if (!m->is_running()) {
        continue;
      }
      MasterServiceProxy proxy(messenger_, m->bound_rpc_addr());

      // All masters (including followers) should accept the heartbeat.
      ASSERT_OK(proxy.TSHeartbeat(req, &resp, &rpc));
      SCOPED_TRACE(SecureDebugString(resp));
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
      if (!m->is_running()) {
        continue;
      }
      MasterServiceProxy proxy(messenger_, m->bound_rpc_addr());

      // All masters (including followers) should accept the heartbeat.
      RETURN_NOT_OK(proxy.TSHeartbeat(req, &resp, &rpc));
      SCOPED_TRACE(SecureDebugString(resp));
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

  int num_masters_;
  MiniClusterOptions opts_;
  gscoped_ptr<MiniCluster> cluster_;

  shared_ptr<rpc::Messenger> messenger_;
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
    gen_config.cn = "ts.foo.com";
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
    gen_config.cn = "ts.foo.com";
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
} // namespace kudu
