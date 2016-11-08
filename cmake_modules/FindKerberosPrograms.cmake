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

# - Find Kerberos Binaries
# This module ensures that the Kerberos binaries depended on by tests are
# present on the system.

include(FindPackageHandleStandardArgs)
set(bins kadmin.local kdb5_util kdestroy kinit klist krb5kdc)

foreach(bin ${bins})
  find_program(${bin} ${bin} PATHS
               # Linux install location.
               /usr/sbin
               # Homebrew install location.
               /usr/local/opt/krb5/sbin
               # Macports install location.
               /opt/local/sbin
               # SLES
               /usr/lib/mit/sbin)
endforeach(bin)

find_package_handle_standard_args(Kerberos REQUIRED_VARS ${bins}
  FAIL_MESSAGE "Kerberos binaries not found: security tests will fail")
