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

#include "kudu/security/tls_handshake.h"

#include <functional>
#include <string>

#include <gtest/gtest.h>

#include "kudu/security/security-test-util.h"
#include "kudu/security/tls_context.h"
#include "kudu/util/test_util.h"

using std::string;

namespace kudu {
namespace security {

class TestTlsHandshake : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    key_path_ = GetTestPath("key.pem");
    cert_path_ = GetTestPath("cert.pem");
    ASSERT_OK(CreateSSLServerCert(cert_path_));
    ASSERT_OK(CreateSSLPrivateKey(key_path_));

    ASSERT_OK(client_tls_.Init());
    ASSERT_OK(server_tls_.Init());
  }

 protected:
  // Run a handshake using 'client_tls_' and 'server_tls_'. The client and server
  // verification modes are set to 'client_verify' and 'server_verify' respectively.
  Status RunHandshake(TlsVerificationMode client_verify,
                      TlsVerificationMode server_verify) {
    TlsHandshake client, server;
    client_tls_.InitiateHandshake(TlsHandshakeType::CLIENT, &client);
    server_tls_.InitiateHandshake(TlsHandshakeType::SERVER, &server);

    client.set_verification_mode(client_verify);
    server.set_verification_mode(server_verify);

    bool client_done = false, server_done = false;
    string to_client;
    string to_server;
    while (!client_done || !server_done) {
      if (!client_done) {
        Status s = client.Continue(to_client, &to_server);
        VLOG(1) << "client->server: " << to_server.size() << " bytes";
        if (s.ok()) {
          client_done = true;
        } else if (!s.IsIncomplete()) {
          CHECK(s.IsRuntimeError());
          return s.CloneAndPrepend("client error");
        }
      }
      if (!server_done) {
        CHECK(!client_done);
        Status s = server.Continue(to_server, &to_client);
        VLOG(1) << "server->client: " << to_client.size() << " bytes";
        if (s.ok()) {
          server_done = true;
        } else if (!s.IsIncomplete()) {
          CHECK(s.IsRuntimeError());
          return s.CloneAndPrepend("server error");
        }
      }
    }
    return Status::OK();
  }

  TlsContext client_tls_;
  TlsContext server_tls_;

  string cert_path_;
  string key_path_;
};

TEST_F(TestTlsHandshake, TestSuccessfulHandshake) {
  // Both client and server have certs and CA.
  ASSERT_OK(client_tls_.LoadCertificateAuthority(cert_path_));
  ASSERT_OK(client_tls_.LoadCertificateAndKey(cert_path_, key_path_));
  ASSERT_OK(server_tls_.LoadCertificateAuthority(cert_path_));
  ASSERT_OK(server_tls_.LoadCertificateAndKey(cert_path_, key_path_));

  TlsHandshake server;
  TlsHandshake client;
  client_tls_.InitiateHandshake(TlsHandshakeType::SERVER, &server);
  server_tls_.InitiateHandshake(TlsHandshakeType::CLIENT, &client);

  string buf1;
  string buf2;

  // Client sends Hello
  ASSERT_TRUE(client.Continue(buf1, &buf2).IsIncomplete());
  ASSERT_GT(buf2.size(), 0);

  // Server receives client Hello, and sends server Hello
  ASSERT_TRUE(server.Continue(buf2, &buf1).IsIncomplete());
  ASSERT_GT(buf1.size(), 0);

  // Client receives server Hello and sends client Finished
  ASSERT_TRUE(client.Continue(buf1, &buf2).IsIncomplete());
  ASSERT_GT(buf2.size(), 0);

  // Server receives client Finished and sends server Finished
  ASSERT_OK(server.Continue(buf2, &buf1));
  ASSERT_GT(buf1.size(), 0);

  // Client receives server Finished
  ASSERT_OK(client.Continue(buf1, &buf2));
  ASSERT_EQ(buf2.size(), 0);
}

// Client has no cert.
// Server has self-signed cert.
TEST_F(TestTlsHandshake, Test_ClientNone_ServerSelfSigned) {
  ASSERT_OK(server_tls_.LoadCertificateAndKey(cert_path_, key_path_));

  // If the client wants to verify the server, it should fail because
  // the server cert is self-signed.
  Status s = RunHandshake(TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
                          TlsVerificationMode::VERIFY_NONE);
  ASSERT_STR_MATCHES(s.ToString(), "client error:.*certificate verify failed");

  // If the client doesn't care, it should succeed against the self-signed
  // server.
  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_NONE,
                         TlsVerificationMode::VERIFY_NONE));

  // If the client loads the cert as trusted, then it should succeed
  // with verification enabled.
  ASSERT_OK(client_tls_.LoadCertificateAuthority(cert_path_));
  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
                         TlsVerificationMode::VERIFY_NONE));

  // If the server requires authentication of the client, the handshake should fail.
  s = RunHandshake(TlsVerificationMode::VERIFY_NONE,
                   TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST);
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_MATCHES(s.ToString(), "server error:.*peer did not return a certificate");
}

// TODO(PKI): this test has both the client and server using the same cert,
// which isn't very realistic. We should have this generate self-signed certs
// on the fly.
TEST_F(TestTlsHandshake, Test_ClientSelfSigned_ServerSelfSigned) {
  ASSERT_OK(client_tls_.LoadCertificateAndKey(cert_path_, key_path_));
  ASSERT_OK(client_tls_.LoadCertificateAuthority(cert_path_));
  ASSERT_OK(server_tls_.LoadCertificateAndKey(cert_path_, key_path_));
  ASSERT_OK(server_tls_.LoadCertificateAuthority(cert_path_));

  // This scenario should succeed in all cases.
  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
                         TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST));

  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
                         TlsVerificationMode::VERIFY_NONE));

  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_NONE,
                         TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST));

  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_NONE,
                         TlsVerificationMode::VERIFY_NONE));
}

// TODO(PKI): add test coverage for mismatched common-name in the cert.

} // namespace security
} // namespace kudu
