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

#include <ostream>
#include <string>

#include "kudu/util/env.h"
#include "kudu/util/status.h"

namespace kudu {
namespace security {

class Cert;
class PrivateKey;
class PublicKey;
class TlsContext;

// TODO(todd): consolidate these certs with those in
// security/test/test_certs.h once we support configuring a password
// for the RPC cert.
inline static Status CreateSSLServerCert(const std::string& file_path) {
  static const char* test_server_cert = R"(
-----BEGIN CERTIFICATE-----
MIIEejCCA2KgAwIBAgIJAKMdvDR5PL82MA0GCSqGSIb3DQEBBQUAMIGEMQswCQYD
VQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTEWMBQGA1UEBxMNU2FuIEZyYW5j
aXNjbzERMA8GA1UEChMIQ2xvdWRlcmExEjAQBgNVBAMTCWxvY2FsaG9zdDEhMB8G
CSqGSIb3DQEJARYSaGVucnlAY2xvdWRlcmEuY29tMB4XDTEzMDkyMjAwMjUxOFoX
DTQxMDIwNzAwMjUxOFowgYQxCzAJBgNVBAYTAlVTMRMwEQYDVQQIEwpDYWxpZm9y
bmlhMRYwFAYDVQQHEw1TYW4gRnJhbmNpc2NvMREwDwYDVQQKEwhDbG91ZGVyYTES
MBAGA1UEAxMJbG9jYWxob3N0MSEwHwYJKoZIhvcNAQkBFhJoZW5yeUBjbG91ZGVy
YS5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCoUj3pMQ2ELkrz
zq+koixljVFBAEEqwUWSjA+GEKwfFb/UPRjeO/wrKndp2r83jc6KRt66rvAIl8cr
b54yTOsJ/ZcARrjTwG3IG8Tely/54ZQyH0ImdJyEbCSoI04zX3ovjlppz3g5xanj
WmpAh6pzPgBOTfisCLMPD70xQ8F//QWZdNatoly54STkTWoJv/Oll/UpXcBY8JOR
+ytX82eGgG4F8YoQqmbjrrN5JAmqLRiBAkr3WUena6ekqJBalJRzex/Wh8a9XEV7
9HFVVngBhezsOJgf81hzBzzhULKfxuXl8uaUj3Z9cZg39CDsyz+ULYbsPm8VoMUI
VCf7MUVTAgMBAAGjgewwgekwHQYDVR0OBBYEFK94kea7jIKQawAIb+0DqsA1rf6n
MIG5BgNVHSMEgbEwga6AFK94kea7jIKQawAIb+0DqsA1rf6noYGKpIGHMIGEMQsw
CQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTEWMBQGA1UEBxMNU2FuIEZy
YW5jaXNjbzERMA8GA1UEChMIQ2xvdWRlcmExEjAQBgNVBAMTCWxvY2FsaG9zdDEh
MB8GCSqGSIb3DQEJARYSaGVucnlAY2xvdWRlcmEuY29tggkAox28NHk8vzYwDAYD
VR0TBAUwAwEB/zANBgkqhkiG9w0BAQUFAAOCAQEAEtkPPncCnN2IFVJvz04K+VsX
b6w3qwPynQKc67+++JkNb3TYKrh/0UVM1NrEOu3TGplqOrKgAlITuaWNqNOSBu1R
WJtrz85YkonED5awjjuALVEY82+c7pOXkuv5G5421RINfRn2hNzgw8VFb5CEvxHH
jER80Vx6UGKr/S649qTQ8AzVzTwWS86VsGI2azAD7D67G/IDGf+0P7FsXonKY+vl
vKzkfaO1+qEOLtDHV9mwlsxl3Re/MNym4ExWHi9txynCNiRZHqWoZUS+KyYqIR2q
seCrQwgi1Fer9Ekd5XNjWjigC3VC3SjMqWaxeKbZ2/AuABJMz5xSiRkgwphXEQ==
-----END CERTIFICATE-----
  )";
  RETURN_NOT_OK(WriteStringToFile(Env::Default(), test_server_cert, file_path));
  return Status::OK();
}

// Writes the test SSL private key into a temporary file.
inline static Status CreateSSLPrivateKey(const std::string& file_path) {
  static const char* test_private_key = R"(
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAqFI96TENhC5K886vpKIsZY1RQQBBKsFFkowPhhCsHxW/1D0Y
3jv8Kyp3adq/N43Oikbeuq7wCJfHK2+eMkzrCf2XAEa408BtyBvE3pcv+eGUMh9C
JnSchGwkqCNOM196L45aac94OcWp41pqQIeqcz4ATk34rAizDw+9MUPBf/0FmXTW
raJcueEk5E1qCb/zpZf1KV3AWPCTkfsrV/NnhoBuBfGKEKpm466zeSQJqi0YgQJK
91lHp2unpKiQWpSUc3sf1ofGvVxFe/RxVVZ4AYXs7DiYH/NYcwc84VCyn8bl5fLm
lI92fXGYN/Qg7Ms/lC2G7D5vFaDFCFQn+zFFUwIDAQABAoIBABNTpiIxbLDhs998
uvQ3XsumR08kXVcwa/GgvWOSZIEJOUaAYWubDaBTNvTjlhMl6DI+YvKihZMnAkp9
fXefF1nFUWJJvI0ryi8w6RD54RtbCG4c4raRqysVU7wumZsSenAdc0o09UQE6zXc
uth/+1VSKCzVjRkLwquXDg0rD3vHfQHWQvQlzwUh3OACA3LfLezVqzrEB02YVRxm
xwg5veeMg6Aod8vsvsajry9eE0hKeFGonRANerL9lwZxzD2ZjU2fSEJYY3xxKVgi
o+QVTKaAt9pivOs10YVZDcIDH0xmDpxAkaLb5ZAbnjwhf7WGYgEm8VruAHkJxyXX
yPf3rpkCgYEA0dp/Xv5KBIkD6JJao8hnhtP5x9U7o/pTzRxaO3WUflvTI6DtC0nk
cTOwFVs4WljT76T937q2x4stuX0woHzvIaZ6wwZ2vv81ikDY6pE8eLWsH/xFAmkx
HBfkSijFgJV6EpTqUnFD7QKU89tzWrh/kxaMO1WgFaBhxPPs3K1LDTUCgYEAzVW5
3yjfVHNgjWTeAbnbToGvUihOidvIvS5cVo5q0Dhfabz0tiXFxAoQUGErUGPC8Nu2
n/HxTI3b0PbCCwjBsDNqX2kzxTSe5aTGIrBUWbped1bxme8jggXuWYbg8vvLpsYf
wAJPxnGIxW/v/aftHUhbTIuVfZX2+UnilrwiwWcCgYEAg8paz4eXaH277KVtMwq6
qZwac/tgNz0Qv/GcYVcYaLq2QNvhwoMnakhxvxfIrkS25PuTTJxwCaVIlAMhNMkB
TPrGghBfJtgUAb1z/Ow1NAG0FWpS1I7HfsMqZcBxOK2nOmA3QItNg11pujQJn+Ha
jL9OVj0SCkLs48nk6ToTtjkCgYEAh8YCtNwq6IWuN3CWGCAUMpIwIqxCWof48Zch
OZ7MZEiSVrG6QmMxpRJefTfzUyHUOj2eQZ7SxqMa0c8IuhEdOeyVjudaczD7TLAq
z68252oDovfbo8Tr/sL7OzmjryfuHqXtQqKEq5xRKvR8hYavlGhO7otx2uv5thcz
/CYE+UsCgYAsgewfzbcBqJwsAUs98DK99EU8VqKuaYlU5wNvAVb27O6sVeysSokM
G1TGIXJPphA3dSAe4Pf/j4ff/eGaS20FAFhs4BPpw0fAeCHpmD0BjIba0lxBS/gY
dc+JVPKL8Fe4a8fmsI6ndcZQ9qpOdZM5WOD0ldKRc+SsrYKkTmOOJQ==
-----END RSA PRIVATE KEY-----
  )";
  RETURN_NOT_OK(WriteStringToFile(Env::Default(), test_private_key, file_path));
  return Status::OK();
}

Status GenerateSelfSignedCAForTests(PrivateKey* ca_key, Cert* ca_cert);

// Describes the options for configuring a TlsContext.
enum class PkiConfig {
  // The TLS context has no TLS cert and no trusted certs.
  NONE,
  // The TLS context has a self-signed TLS cert and no trusted certs.
  SELF_SIGNED,
  // The TLS context has no TLS cert and a trusted cert.
  TRUSTED,
  // The TLS context has a signed TLS cert and trusts the corresonding signing cert.
  SIGNED,
};

// PkiConfig pretty-printer.
std::ostream& operator<<(std::ostream& o, PkiConfig c);

Status ConfigureTlsContext(PkiConfig config,
                           const Cert& ca_cert,
                           const PrivateKey& ca_key,
                           TlsContext* tls_context);

} // namespace security
} // namespace kudu
