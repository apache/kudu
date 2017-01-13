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
add_library(krb5_realm_override STATIC krb5_realm_override.cc)
target_link_libraries(krb5_realm_override glog)
if(NOT APPLE)
  target_link_libraries(krb5_realm_override dl)
endif()

# Fall back to using the ported functionality if we're using an older version of OpenSSL.
if (${OPENSSL_VERSION} VERSION_LESS "1.0.2")
  set(PORTED_X509_CHECK_HOST_CC "x509_check_host.cc")
endif()

set(SECURITY_SRCS
  init.cc
  openssl_util.cc
  ${PORTED_X509_CHECK_HOST_CC}
  ssl_factory.cc
  ssl_socket.cc)

set(SECURITY_LIBS
  gutil
  krb5
  kudu_util
  openssl_crypto
  openssl_ssl)

ADD_EXPORTABLE_LIBRARY(security
  SRCS ${SECURITY_SRCS}
  DEPS ${SECURITY_LIBS})

if (NOT NO_TESTS)
  set(SECURITY_TEST_SRCS
    test/mini_kdc.cc)

  add_library(security-test ${SECURITY_TEST_SRCS})
  target_link_libraries(security-test
    gutil
    kudu_test_util
    kudu_util)

  # Tests
  set(KUDU_TEST_LINK_LIBS
    security
    security-test
    ${KUDU_MIN_TEST_LIBS})
endif()

ADD_KUDU_TEST(test/mini_kdc-test)
