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

#include <openssl/crypto.h>
#include <openssl/ssl.h>

#include <atomic>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/security/ca/cert_management.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/security-test-util.h"
#include "kudu/security/security_flags.h"
#include "kudu/security/tls_context.h"
#include "kudu/util/monotime.h"
#include "kudu/util/openssl_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::security::ca::CertSigner;
using std::string;
using std::vector;

DECLARE_int32(ipki_server_key_size);

namespace kudu {
namespace security {

struct Case {
  PkiConfig client_pki;
  TlsVerificationMode client_verification;
  PkiConfig server_pki;
  TlsVerificationMode server_verification;
  Status expected_status;
};

// Beautifies CLI test output.
std::ostream& operator<<(std::ostream& o, Case c) {
  auto verification_mode_name = [] (const TlsVerificationMode& verification_mode) {
    switch (verification_mode) {
      case TlsVerificationMode::VERIFY_NONE: return "NONE";
      case TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST: return "REMOTE_CERT_AND_HOST";
    }
    return "unreachable";
  };

  o << "{client-pki: " << c.client_pki << ", "
    << "client-verification: " << verification_mode_name(c.client_verification) << ", "
    << "server-pki: " << c.server_pki << ", "
    << "server-verification: " << verification_mode_name(c.server_verification) << ", "
    << "expected-status: " << c.expected_status.ToString() << "}";

  return o;
}

class TestTlsHandshakeBase : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    ASSERT_OK(client_tls_.Init());
    ASSERT_OK(server_tls_.Init());
  }

 protected:
  // Run a handshake using 'client_tls_' and 'server_tls_'. The client and server
  // verification modes are set to 'client_verify' and 'server_verify' respectively.
  Status RunHandshake(TlsVerificationMode client_verify,
                      TlsVerificationMode server_verify) {
    TlsHandshake client(TlsHandshakeType::CLIENT);
    RETURN_NOT_OK(client_tls_.InitiateHandshake(&client));
    TlsHandshake server(TlsHandshakeType::SERVER);
    RETURN_NOT_OK(server_tls_.InitiateHandshake(&server));

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

  // Read data through the specified SSL handle using OpenSSL API.
  static void ReadAndCompare(SSL* ssl, const string& expected_data) {
    ASSERT_GT(expected_data.size(), 0);
    string result;
    string buf;
    buf.resize(expected_data.size());
    int yet_to_read = expected_data.size();
    while (yet_to_read > 0) {
      auto bytes_read = SSL_read(ssl, buf.data(), yet_to_read);
      if (bytes_read <= 0) {
        auto error_code = SSL_get_error(ssl, bytes_read);
        EXPECT_EQ(SSL_ERROR_WANT_READ, error_code);
        if (error_code != SSL_ERROR_WANT_READ) {
          FAIL() << "OpenSSL error: " << GetSSLErrorDescription(error_code);
        }
        continue;
      }
      yet_to_read -= bytes_read;
      result += buf.substr(0, bytes_read);
    }
    ASSERT_EQ(expected_data, result);
  }

  // Write data through the specified SSL handle using OpenSSL API.
  static void Write(SSL* ssl, const string& data) {
    // TODO(aserbin): handle SSL_ERROR_WANT_WRITE if transferring more data
    auto bytes_written = SSL_write(ssl, data.data(), data.size());
    EXPECT_EQ(data.size(), bytes_written);
    if (bytes_written != data.size()) {
      auto error_code = SSL_get_error(ssl, bytes_written);
      FAIL() << "OpenSSL error: " << GetSSLErrorDescription(error_code);
    }
  }

  // Transfer data between the source and the destination SSL handles.
  //
  // Since the TlsHandshake class uses memory BIO for running the TLS handshake,
  // data between the client and the server TlsHandshake objects is transferred
  // using the BIO read/write API on the encrypted side of the TLS engine.
  // This diagram illustrates the approach used by the data transfer method
  // below:
  //                         +-----+
  // +--> BIO_write(rbio) -->|     |--> SSL_read(ssl)  --> application data IN
  // |                       | SSL |
  // +---- BIO_read(wbio) <--|     |<-- SSL_write(ssl) <-- application data OUT
  //                         +-----+
  static void Transfer(SSL* src, SSL* dst) {
    BIO* wbio = SSL_get_wbio(src);
    int pending_wr = BIO_ctrl_pending(wbio);

    string data;
    data.resize(pending_wr);
    auto bytes_read = BIO_read(wbio, &data[0], data.size());
    ASSERT_EQ(data.size(), bytes_read);
    ASSERT_EQ(0, BIO_ctrl_pending(wbio));

    BIO* rbio = SSL_get_rbio(dst);
    auto bytes_written = BIO_write(rbio, &data[0], data.size());
    ASSERT_EQ(data.size(), bytes_written);
  }

  TlsContext client_tls_;
  TlsContext server_tls_;

  string cert_path_;
  string key_path_;
};

class TestTlsHandshake : public TestTlsHandshakeBase,
                   public ::testing::WithParamInterface<Case> {};

class TestTlsHandshakeConcurrent : public TestTlsHandshakeBase,
                   public ::testing::WithParamInterface<int> {};

// Test concurrently running handshakes while changing the certificates on the TLS
// context. We parameterize across different numbers of threads, because surprisingly,
// fewer threads seems to trigger issues more easily in some cases.
INSTANTIATE_TEST_SUITE_P(NumThreads, TestTlsHandshakeConcurrent, ::testing::Values(1, 2, 4, 8));
TEST_P(TestTlsHandshakeConcurrent, TestConcurrentAdoptCert) {
  const int kNumThreads = GetParam();

  ASSERT_OK(server_tls_.GenerateSelfSignedCertAndKey());
  std::atomic<bool> done(false);
  vector<std::thread> handshake_threads;
  for (int i = 0; i < kNumThreads; i++) {
    handshake_threads.emplace_back([&]() {
        while (!done) {
          RunHandshake(TlsVerificationMode::VERIFY_NONE, TlsVerificationMode::VERIFY_NONE);
        }
      });
  }
  auto c = MakeScopedCleanup([&](){
      done = true;
      for (std::thread& t : handshake_threads) {
        t.join();
      }
    });

  SleepFor(MonoDelta::FromMilliseconds(10));
  {
    PrivateKey ca_key;
    Cert ca_cert;
    ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));
    Cert cert;
    ASSERT_OK(CertSigner(&ca_cert, &ca_key).Sign(*server_tls_.GetCsrIfNecessary(), &cert));
    ASSERT_OK(server_tls_.AddTrustedCertificate(ca_cert));
    ASSERT_OK(server_tls_.AdoptSignedCert(cert));
  }
  SleepFor(MonoDelta::FromMilliseconds(10));
}

TEST_F(TestTlsHandshake, HandshakeSequenceNoTLSv1dot3) {
  static const vector<string> kTlsExcludedProtocols = { "TLSv1.3" };

  PrivateKey ca_key;
  Cert ca_cert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));

  TlsContext client_tls(SecurityDefaults::kDefaultTlsCiphers,
                        SecurityDefaults::kDefaultTlsCipherSuites,
                        SecurityDefaults::kDefaultTlsMinVersion,
                        kTlsExcludedProtocols);
  ASSERT_OK(client_tls.Init());

  TlsContext server_tls(SecurityDefaults::kDefaultTlsCiphers,
                        SecurityDefaults::kDefaultTlsCipherSuites,
                        SecurityDefaults::kDefaultTlsMinVersion,
                        kTlsExcludedProtocols);
  ASSERT_OK(server_tls.Init());

  // Both client and server have certs and CA.
  ASSERT_OK(ConfigureTlsContext(PkiConfig::SIGNED, ca_cert, ca_key, &client_tls));
  ASSERT_OK(ConfigureTlsContext(PkiConfig::SIGNED, ca_cert, ca_key, &server_tls));

  TlsHandshake server(TlsHandshakeType::SERVER);
  ASSERT_OK(client_tls.InitiateHandshake(&server));
  TlsHandshake client(TlsHandshakeType::CLIENT);
  ASSERT_OK(server_tls.InitiateHandshake(&client));

  string buf1;
  string buf2;

  // Client sends Hello
  auto s = client.Continue(buf1, &buf2);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
  ASSERT_GT(buf2.size(), 0);

  // Server receives client Hello, and sends server Hello
  s = server.Continue(buf2, &buf1);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
  ASSERT_GT(buf1.size(), 0);

  // Client receives server Hello and sends client Finished
  s = client.Continue(buf1, &buf2);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
  ASSERT_GT(buf2.size(), 0);

  // Server receives client Finished and sends server Finished
  ASSERT_OK(server.Continue(buf2, &buf1));
  ASSERT_GT(buf1.size(), 0);

  // Client receives server Finished
  ASSERT_OK(client.Continue(buf1, &buf2));
  ASSERT_EQ(buf2.size(), 0);

  auto* client_ssl = client.ssl();
  auto* server_ssl = server.ssl();

  // Make sure it's possible to exchange data between the client and the server
  // once the TLS handshake is complete.
  NO_FATALS(Write(client_ssl, "hello"));
  NO_FATALS(Transfer(client_ssl, server_ssl));
  NO_FATALS(ReadAndCompare(server_ssl, "hello"));

  NO_FATALS(Write(client_ssl, "bye"));
  NO_FATALS(Transfer(client_ssl, server_ssl));
  NO_FATALS(ReadAndCompare(server_ssl, "bye"));
}

#if OPENSSL_VERSION_NUMBER >= 0x10101000L
// This scenario is specific to TLSv1.3 handshake negotiation, so it's enabled
// only if the OpenSSL library supports TLSv1.3.
TEST_F(TestTlsHandshake, HandshakeSequenceTLSv1dot3) {
  // NOTE: since --rpc_tls_min_protocol=TLSv1.3 flag isn't added, this scenario
  //       also verifies that the client and the server sides behave as expected
  //       by choosing TLSv1.3 in accordance to the cipher suite preference list
  //       specified by the --rpc_tls_ciphersuites and --rpc_tls_ciphers flags.
  PrivateKey ca_key;
  Cert ca_cert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));

  // Server has certificate signed by CA, client has self-signed certificate.
  ASSERT_OK(ConfigureTlsContext(PkiConfig::SELF_SIGNED, ca_cert, ca_key, &client_tls_));
  ASSERT_OK(ConfigureTlsContext(PkiConfig::SIGNED, ca_cert, ca_key, &server_tls_));

  TlsHandshake server(TlsHandshakeType::SERVER);
  ASSERT_OK(client_tls_.InitiateHandshake(&server));
  // The client has a self-signed certificate, so the server has nothing to
  // verify during the connection negotiation.
  server.set_verification_mode(security::TlsVerificationMode::VERIFY_NONE);

  TlsHandshake client(TlsHandshakeType::CLIENT);
  ASSERT_OK(server_tls_.InitiateHandshake(&client));
  // That's the first connection from the client to the server/cluster, and the
  // client hasn't yet seen a certificate of this Kudu cluster, hence it cannot
  // verify the certificate of the server regardless the fact that the server's
  // certificate is valid and signed by the cluster's CA private key.
  client.set_verification_mode(security::TlsVerificationMode::VERIFY_NONE);

  auto* client_ssl = client.ssl();
  auto* server_ssl = server.ssl();

  string buf1;
  string buf2;

  // Client sends "Hello" (supported ciphersuites, keyshares, etc.).
  auto s = client.Continue(buf1, &buf2);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
  ASSERT_GT(buf2.size(), 0);

  // Server receives client's "Hello", chooses cipher, calculates keyshares,
  // encrypts certificate, Finish messages and sends all this back to client.
  s = server.Continue(buf2, &buf1);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
  ASSERT_GT(buf1.size(), 0);

  // Client receives server's Hello and Finish messages.
  ASSERT_OK(client.Continue(buf1, &buf2));
  ASSERT_GT(buf2.size(), 0);

  // This isn't a part of the TLSv1.3 handshake you'd see in the docs, but it's
  // here due to the nature of the step-by-step connection negotiation protocol
  // used by Kudu RPC. In the wild (i.e. if using the direct connection over the
  // network, not the intermediate SASL framework), 'buf2' data would get to the
  // server side with encrypted data from the very first request sent by the
  // client to the server.
  ASSERT_OK(server.Continue(buf2, &buf1));
  ASSERT_GT(buf1.size(), 0);

  // An extra sanity check: since the initial phase of the handshake is done,
  // make sure it's indeed TLSv1.3 protocol, as expected.
  ASSERT_EQ(TLS1_3_VERSION, SSL_version(client_ssl));
  ASSERT_EQ(TLS1_3_VERSION, SSL_version(server_ssl));

  // The code block below passes the data produced by the server's final TLS
  // handshake message to the client side. It's not an application data: it
  // should be passed by the encrypted/raw side of the communication channel,
  // not via the SSL_{read,write}() API. In case of a regular over-the-network
  // TLS negotiation, this data would get to the client side over the wire along
  // with the encrypted data of the very first response sent by the server to
  // the client. However, since Kudu RPC connection negotiation works in a
  // step-by-step manner and runs on top of the SASL framework, this scenario
  // emulates that by moving the data to the read BIO of the client-side SSL
  // object. Once the data is in the buffer of the appropriate BIO, it will be
  // pushed through and processed by the TLS engine with next chunk of encrypted
  // application data received by the client.
  {
    BIO* rbio = SSL_get_rbio(client_ssl);
    auto bytes_written = BIO_write(rbio, buf1.data(), buf1.size());
    DCHECK_EQ(buf1.size(), bytes_written);
    DCHECK_EQ(buf1.size(), BIO_ctrl_pending(rbio));
  }

  // The TLS handshake is now complete: both sides can send and receive data
  // using OpenSSL's API. In other words, it's possible to use SSL_{read,write}
  // to successfully send application data from the client to the server and
  // back.
  {
    // The sequence of messages in this sub-scenario matches those produced by
    // the TestRpc.TestCall/TCP_SSL scenario in src/kudu/rpc/rpc-test.cc.
    const string client_msg_0 = "0123456789012345";
    NO_FATALS(Write(client_ssl, client_msg_0));
    const string client_msg_1 = "01234567890123456789012";
    NO_FATALS(Write(client_ssl, client_msg_1));

    NO_FATALS(Transfer(client_ssl, server_ssl));
    NO_FATALS(ReadAndCompare(server_ssl, "0123"));
    NO_FATALS(ReadAndCompare(server_ssl, "45678901234501234567890123456789012"));

    const string server_msg_0 = "0123456789012345";
    NO_FATALS(Write(server_ssl, server_msg_0));
    const string server_msg_1 = "012";
    NO_FATALS(Write(server_ssl, server_msg_1));

    NO_FATALS(Transfer(server_ssl, client_ssl));
    NO_FATALS(ReadAndCompare(client_ssl, "0123"));
    NO_FATALS(ReadAndCompare(client_ssl, "456789012345012"));
  }
}
#endif // #if OPENSSL_VERSION_NUMBER >= 0x10101000L ...

// Tests that the TlsContext can transition from self signed cert to signed
// cert, and that it rejects invalid certs along the way. We are testing this
// here instead of in a dedicated TlsContext test because it requires completing
// handshakes to fully validate.
TEST_F(TestTlsHandshake, TestTlsContextCertTransition) {
  ASSERT_FALSE(server_tls_.has_cert());
  ASSERT_FALSE(server_tls_.has_signed_cert());
  ASSERT_EQ(boost::none, server_tls_.GetCsrIfNecessary());

  ASSERT_OK(server_tls_.GenerateSelfSignedCertAndKey());
  ASSERT_TRUE(server_tls_.has_cert());
  ASSERT_FALSE(server_tls_.has_signed_cert());
  ASSERT_NE(boost::none, server_tls_.GetCsrIfNecessary());
  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_NONE, TlsVerificationMode::VERIFY_NONE));
  ASSERT_STR_MATCHES(RunHandshake(TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
                                  TlsVerificationMode::VERIFY_NONE).ToString(),
                     "client error:.*certificate verify failed");

  PrivateKey ca_key;
  Cert ca_cert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));

  Cert cert;
  ASSERT_OK(CertSigner(&ca_cert, &ca_key).Sign(*server_tls_.GetCsrIfNecessary(), &cert));

  // Try to adopt the cert without first trusting the CA.
  ASSERT_STR_MATCHES(server_tls_.AdoptSignedCert(cert).ToString(),
                     "could not verify certificate chain");

  // Check that we can still do (unverified) handshakes.
  ASSERT_TRUE(server_tls_.has_cert());
  ASSERT_FALSE(server_tls_.has_signed_cert());
  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_NONE, TlsVerificationMode::VERIFY_NONE));

  // Trust the root cert.
  ASSERT_OK(server_tls_.AddTrustedCertificate(ca_cert));

  // Generate a bogus cert and attempt to adopt it.
  Cert bogus_cert;
  {
    TlsContext bogus_tls;
    ASSERT_OK(bogus_tls.Init());
    ASSERT_OK(bogus_tls.GenerateSelfSignedCertAndKey());
    ASSERT_OK(CertSigner(&ca_cert, &ca_key).Sign(*bogus_tls.GetCsrIfNecessary(), &bogus_cert));
  }
  ASSERT_STR_MATCHES(server_tls_.AdoptSignedCert(bogus_cert).ToString(),
                     "certificate public key does not match the CSR public key");

  // Check that we can still do (unverified) handshakes.
  ASSERT_TRUE(server_tls_.has_cert());
  ASSERT_FALSE(server_tls_.has_signed_cert());
  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_NONE, TlsVerificationMode::VERIFY_NONE));

  // Adopt the legitimate signed cert.
  ASSERT_OK(server_tls_.AdoptSignedCert(cert));

  // Check that we can do verified handshakes.
  ASSERT_TRUE(server_tls_.has_cert());
  ASSERT_TRUE(server_tls_.has_signed_cert());
  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_NONE, TlsVerificationMode::VERIFY_NONE));
  ASSERT_OK(client_tls_.AddTrustedCertificate(ca_cert));
  ASSERT_OK(RunHandshake(TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
                         TlsVerificationMode::VERIFY_NONE));
}

TEST_P(TestTlsHandshake, TestHandshake) {
  Case test_case = GetParam();

  PrivateKey ca_key;
  Cert ca_cert;
  ASSERT_OK(GenerateSelfSignedCAForTests(&ca_key, &ca_cert));

  ASSERT_OK(ConfigureTlsContext(test_case.client_pki, ca_cert, ca_key, &client_tls_));
  ASSERT_OK(ConfigureTlsContext(test_case.server_pki, ca_cert, ca_key, &server_tls_));

  Status s = RunHandshake(test_case.client_verification, test_case.server_verification);

  EXPECT_EQ(test_case.expected_status.CodeAsString(), s.CodeAsString());
  ASSERT_STR_MATCHES(s.ToString(), test_case.expected_status.message().ToString());
}

INSTANTIATE_TEST_SUITE_P(CertCombinations,
                         TestTlsHandshake,
                         ::testing::Values(

        // We don't test any cases where the server has no cert or the client
        // has a self-signed cert, since we don't expect those to occur in
        // practice.

        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::RuntimeError("client error:.*certificate verify failed") },
        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("server error:.*peer did not return a certificate") },
        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("client error:.*certificate verify failed") },

        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::RuntimeError("client error:.*certificate verify failed") },
        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("server error:.*peer did not return a certificate") },
        Case { PkiConfig::NONE, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("client error:.*certificate verify failed") },

        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::RuntimeError("client error:.*certificate verify failed") },
        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("server error:.*peer did not return a certificate") },
        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("client error:.*certificate verify failed") },

        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("server error:.*peer did not return a certificate") },
        Case { PkiConfig::TRUSTED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("server error:.*peer did not return a certificate") },

        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::RuntimeError("client error:.*certificate verify failed") },
        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               // OpenSSL 1.0.0 returns "no certificate returned" for this case,
               // which appears to be a bug.
               Status::RuntimeError("server error:.*(certificate verify failed|"
                                                    "no certificate returned)") },
        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SELF_SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::RuntimeError("client error:.*certificate verify failed") },

        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               Status::OK() },
        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_NONE,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::OK() },
        Case { PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               PkiConfig::SIGNED, TlsVerificationMode::VERIFY_REMOTE_CERT_AND_HOST,
               Status::OK() }
));

} // namespace security
} // namespace kudu
