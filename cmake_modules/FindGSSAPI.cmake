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
#
# - Find GSSAPI SASL (gssapi.h, gssapi_krb5.so)
#
# This module defines
#  GSSAPI_INCLUDE_DIR, directory containing headers
#  GSSAPI_SHARED_LIB, path to GSSAPI's shared library
#  GSSAPI_FOUND, whether GSSAPI has been found
#
# N.B: we do _not_ include GSSAPI in thirdparty. In practice, GSSAPI (like
# Cyrus-SASL) is so commonly used and generally non-ABI-breaking that we should
# be OK to depend on the host installation.

find_path(GSSAPI_INCLUDE_DIR gssapi/gssapi.h)
find_library(GSSAPI_SHARED_LIB gssapi_krb5)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GSSAPI_VARS
  GSSAPI_SHARED_LIB GSSAPI_INCLUDE_DIR)
