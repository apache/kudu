# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# See the comment in krb5_realm_override.cc for details on this library's usage.
# The top-level CMakeLists sets a ${KRB5_REALM_OVERRIDE} variable which should
# be linked first into all Kudu binaries.

##############################
# krb5_realm_override
##############################

add_library(krb5_realm_override STATIC krb5_realm_override.cc)
target_link_libraries(krb5_realm_override glog)
if(NOT APPLE)
  target_link_libraries(krb5_realm_override dl)
endif()

##############################
# token_proto
##############################

PROTOBUF_GENERATE_CPP(
  TOKEN_PROTO_SRCS TOKEN_PROTO_HDRS TOKEN_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES token.proto)
set(TOKEN_PROTO_LIBS protobuf pb_util_proto)
ADD_EXPORTABLE_LIBRARY(token_proto
  SRCS ${TOKEN_PROTO_SRCS}
  DEPS ${TOKEN_PROTO_LIBS}
  NONLINK_DEPS ${TOKEN_PROTO_TGTS})


##############################
# security
##############################

# Fall back to using the ported functionality if we're using an older version of OpenSSL.
if (${OPENSSL_VERSION} VERSION_LESS "1.0.2")
  set(PORTED_X509_CHECK_HOST_CC "x509_check_host.cc")
endif()

set(SECURITY_SRCS
  ca/cert_management.cc
  cert.cc
  crypto.cc
  kerberos_util.cc
  init.cc
  openssl_util.cc
  ${PORTED_X509_CHECK_HOST_CC}
  security_flags.cc
  simple_acl.cc
  tls_context.cc
  tls_handshake.cc
  tls_socket.cc
  token_verifier.cc
  token_signer.cc
  token_signing_key.cc
  )

set(SECURITY_LIBS
  gutil
  kudu_util
  token_proto

  krb5
  openssl_crypto
  openssl_ssl)

ADD_EXPORTABLE_LIBRARY(security
  SRCS ${SECURITY_SRCS}
  DEPS ${SECURITY_LIBS})


##############################
# mini_kdc
##############################

set(MINI_KDC_SRCS test/mini_kdc.cc)

add_library(mini_kdc ${MINI_KDC_SRCS})
target_link_libraries(mini_kdc
  gutil
  kudu_test_util
  kudu_util)

##############################
# security_test_util
##############################

if (NOT NO_TESTS)
  set(SECURITY_TEST_SRCS
    security-test-util.cc
    test/test_certs.cc
    test/test_pass.cc)

  add_library(security_test_util ${SECURITY_TEST_SRCS})
  target_link_libraries(security_test_util
    gutil
    kudu_test_util
    kudu_util
    security)

  # Tests
  set(KUDU_TEST_LINK_LIBS
    mini_kdc
    security
    security_test_util
    ${KUDU_MIN_TEST_LIBS})

  ADD_KUDU_TEST(ca/cert_management-test)
  ADD_KUDU_TEST(cert-test)
  ADD_KUDU_TEST(crypto-test)
  ADD_KUDU_TEST(test/mini_kdc-test)
  ADD_KUDU_TEST(tls_handshake-test)
  ADD_KUDU_TEST(tls_socket-test)
  ADD_KUDU_TEST(token-test)
endif()
