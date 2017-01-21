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

#include <string>

#include <gtest/gtest.h>

#include "kudu/security/security-test-util.h"
#include "kudu/security/tls_context.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace security {

class TestTlsHandshake : public KuduTest {};

TEST_F(TestTlsHandshake, TestHandshake) {
  string cert_path = GetTestPath("cert.pem");
  string key_path = GetTestPath("key.pem");
  ASSERT_OK(CreateSSLServerCert(cert_path));
  ASSERT_OK(CreateSSLPrivateKey(key_path));

  TlsContext tls_context;
  ASSERT_OK(tls_context.Init());
  ASSERT_OK(tls_context.LoadCertificate(cert_path));
  ASSERT_OK(tls_context.LoadPrivateKey(key_path));
  ASSERT_OK(tls_context.LoadCertificateAuthority(cert_path));

  TlsHandshake server;
  TlsHandshake client;

  tls_context.InitiateHandshake(TlsHandshakeType::SERVER, &server);
  tls_context.InitiateHandshake(TlsHandshakeType::CLIENT, &client);

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

} // namespace security
} // namespace kudu
