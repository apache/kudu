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
# This is an awk script to process output from the include-what-you-use (IWYU)
# tool. As of now, IWYU is of alpha quality and it gives many incorrect
# recommendations -- obviously invalid or leading to compilation breakage.
# Most of those can be silenced using appropriate IWYU pragmas, but it's not
# the case for the auto-generated files.
#
# Also, it's possible to address invalid recommendation using mappings:
#   https://github.com/include-what-you-use/include-what-you-use/blob/master/docs/IWYUMappings.md
#
# We are using mappings for the boost library (comes with IWYU) and a few
# custom mappings for gflags, glog, gtest and other libraries to address some
# IWYU quirks (hopefully, those should be resolved as IWYU gets better).
# The kudu.imp mappings file is used to provide Kudu-specific mappings.
#
# To run the IWYU tool on every C++ source file in the project,
# use the recipe below.
#
#  1. Run the CMake with -DCMAKE_CXX_INCLUDE_WHAT_YOU_USE=<iwyu_cmd_line>
#
#     The path to the IWYU binary should be absolute. The path to the binary
#     and the command-line options should be separated by semicolon
#     (that's for feeding it into CMake list variables).
#
#     Below is the set of commands to run from the build directory to configure
#     the build accordingly (line breaks are for better readability):
#
#     CC=../../thirdparty/clang-toolchain/bin/clang
#     CXX=../../thirdparty/clang-toolchain/bin/clang++
#     IWYU="`pwd`/../../thirdparty/clang-toolchain/bin/include-what-you-use;\
#       -Xiwyu;--max_line_length=256;\
#       -Xiwyu;--mapping_file=`pwd`/../../build-support/iwyu/mappings/boost-all.imp;\
#       -Xiwyu;--mapping_file=`pwd`/../../build-support/iwyu/mappings/boost-all-private.imp;\
#       -Xiwyu;--mapping_file=`pwd`/../../build-support/iwyu/mappings/boost-extra.imp;\
#       -Xiwyu;--mapping_file=`pwd`/../../build-support/iwyu/mappings/gtest.imp;\
#       -Xiwyu;--mapping_file=`pwd`/../../build-support/iwyu/mappings/glog.imp;\
#       -Xiwyu;--mapping_file=`pwd`/../../build-support/iwyu/mappings/gflags.imp;\
#       -Xiwyu;--mapping_file=`pwd`/../../build-support/iwyu/mappings/kudu.imp;\
#       -Xiwyu;--mapping_file=`pwd`/../../build-support/iwyu/mappings/libstdcpp.imp\
#       -Xiwyu;--mapping_file=`pwd`/../../build-support/iwyu/mappings/system-linux.imp"
#
#     ../../build-support/enable_devtoolset.sh \
#       env CC=$CC CXX=$CXX \
#       ../../thirdparty/installed/common/bin/cmake \
#       -DCMAKE_CXX_INCLUDE_WHAT_YOU_USE=\"$IWYU\" \
#       ../..
#
#     NOTE:
#       Since the Kudu code has some 'ifdef NDEBUG' directives, it's possible
#       that IWYU would produce different results if run against release, not
#       debug build. As of now, the tool is used in debug builds only.
#
#  2. Run make, separating the output from the IWYU tool into a separate file
#     (it's possible to use piping the output from the tool to the script
#      but having a file is good for future reference, if necessary):
#
#     make -j$(nproc) 2>/tmp/iwyu.log
#
#  3. Process the output from the IWYU tool using the script:
#
#     awk -f ../../build-support/iwyu/iwyu-filter.awk /tmp/iwyu.log
#

BEGIN {
  # This is the list of the files for which the suggestions from IWYU are
  # ignored. Eventually, this list should become empty as soon as all the valid
  # suggestions are addressed and invalid ones are taken care either by proper
  # IWYU pragmas or adding special mappings (e.g. like boost mappings).
  muted["kudu/cfile/cfile_reader.h"]
  muted["kudu/cfile/cfile_writer.h"]
  muted["kudu/client/client-internal.h"]
  muted["kudu/client/client-test.cc"]
  muted["kudu/common/encoded_key-test.cc"]
  muted["kudu/common/schema.h"]
  muted["kudu/consensus/consensus_meta-test.cc"]
  muted["kudu/consensus/raft_consensus_quorum-test.cc"]
  muted["kudu/experiments/rwlock-perf.cc"]
  muted["kudu/gutil/atomicops-internals-x86.cc"]
  muted["kudu/integration-tests/token_signer-itest.cc"]
  muted["kudu/rpc/reactor.cc"]
  muted["kudu/rpc/reactor.h"]
  muted["kudu/rpc/rpc_sidecar.h"]
  muted["kudu/security/ca/cert_management.cc"]
  muted["kudu/security/ca/cert_management.h"]
  muted["kudu/security/cert-test.cc"]
  muted["kudu/security/cert.cc"]
  muted["kudu/security/cert.h"]
  muted["kudu/security/openssl_util.cc"]
  muted["kudu/security/openssl_util.h"]
  muted["kudu/security/tls_context.cc"]
  muted["kudu/security/tls_handshake.cc"]
  muted["kudu/security/tls_socket.h"]
  muted["kudu/security/x509_check_host.cc"]
  muted["kudu/server/default-path-handlers.cc"]
  muted["kudu/server/webserver.cc"]
  muted["kudu/tablet/tablet.cc"]
  muted["kudu/tablet/tablet_history_gc-test.cc"]
  muted["kudu/tablet/transactions/transaction_tracker-test.cc"]
  muted["kudu/tools/tool_action_local_replica.cc"]
  muted["kudu/tserver/tablet_copy_service-test.cc"]
  muted["kudu/util/bit-util-test.cc"]
  muted["kudu/util/env_util-test.cc"]
  muted["kudu/util/file_cache-stress-test.cc"]
  muted["kudu/util/group_varint-test.cc"]
  muted["kudu/util/minidump.cc"]
  muted["kudu/util/mt-metrics-test.cc"]
  muted["kudu/util/process_memory.cc"]
  muted["kudu/util/rle-test.cc"]
}

# mute all suggestions for the auto-generated files
/.*\.(pb|proxy|service)\.(cc|h) should (add|remove) these lines:/, /^$/ {
  next
}

# mute suggestions for the explicitly specified files
/.* should (add|remove) these lines:/ {
  do_print = 1
  for (path in muted) {
    if (index($0, path)) {
      do_print = 0
      break
    }
  }
}
/^$/ {
  if (do_print) print
  do_print = 0
}
{ if (do_print) print }
