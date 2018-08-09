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

#######################################
# util_compression_proto
#######################################

PROTOBUF_GENERATE_CPP(
  UTIL_COMPRESSION_PROTO_SRCS UTIL_COMPRESSION_PROTO_HDRS UTIL_COMPRESSION_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES compression/compression.proto)
ADD_EXPORTABLE_LIBRARY(util_compression_proto
  SRCS ${UTIL_COMPRESSION_PROTO_SRCS}
  DEPS protobuf
  NONLINK_DEPS ${UTIL_COMPRESSION_PROTO_TGTS})

#######################################
# histogram_proto
#######################################

PROTOBUF_GENERATE_CPP(
  HISTOGRAM_PROTO_SRCS HISTOGRAM_PROTO_HDRS HISTOGRAM_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES histogram.proto)
ADD_EXPORTABLE_LIBRARY(histogram_proto
  SRCS ${HISTOGRAM_PROTO_SRCS}
  DEPS protobuf
  NONLINK_DEPS ${HISTOGRAM_PROTO_TGTS})

#######################################
# maintenance_manager_proto
#######################################

PROTOBUF_GENERATE_CPP(
    MAINTENANCE_MANAGER_PROTO_SRCS MAINTENANCE_MANAGER_PROTO_HDRS MAINTENANCE_MANAGER_PROTO_TGTS
    SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
    BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
    PROTO_FILES maintenance_manager.proto)
ADD_EXPORTABLE_LIBRARY(maintenance_manager_proto
    SRCS ${MAINTENANCE_MANAGER_PROTO_SRCS}
    DEPS protobuf
    NONLINK_DEPS ${MAINTENANCE_MANAGER_PROTO_TGTS})

#######################################
# pb_util_proto
#######################################

PROTOBUF_GENERATE_CPP(
  PB_UTIL_PROTO_SRCS PB_UTIL_PROTO_HDRS PB_UTIL_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES pb_util.proto)
ADD_EXPORTABLE_LIBRARY(pb_util_proto
  SRCS ${PB_UTIL_PROTO_SRCS}
  DEPS protobuf
  NONLINK_DEPS ${PB_UTIL_PROTO_TGTS})

#######################################
# version_info_proto
#######################################

PROTOBUF_GENERATE_CPP(
  VERSION_INFO_PROTO_SRCS VERSION_INFO_PROTO_HDRS VERSION_INFO_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES version_info.proto)
ADD_EXPORTABLE_LIBRARY(version_info_proto
  SRCS ${VERSION_INFO_PROTO_SRCS}
  DEPS protobuf
  NONLINK_DEPS ${VERSION_INFO_PROTO_TGTS})

############################################################
# Version stamp
############################################################

# Unlike CMAKE_CURRENT_BINARY_DIR, CMAKE_BINARY_DIR is always the root of
# the build directory.
set(VERSION_STAMP_FILE ${CMAKE_BINARY_DIR}/src/kudu/generated/version_defines.h)

list(APPEND GEN_VERSION_INFO_COMMAND "${BUILD_SUPPORT_DIR}/gen_version_info.py")
list(APPEND GEN_VERSION_INFO_COMMAND "--version=${KUDU_VERSION_NUMBER}")
list(APPEND GEN_VERSION_INFO_COMMAND "--build-type=${CMAKE_BUILD_TYPE}")
if(KUDU_GIT_HASH)
  message(STATUS "Provided git hash: ${KUDU_GIT_HASH}")
  list(APPEND GEN_VERSION_INFO_COMMAND "--git-hash=${KUDU_GIT_HASH}")
endif()
list(APPEND GEN_VERSION_INFO_COMMAND "${VERSION_STAMP_FILE}")
add_custom_target(gen_version_info
    COMMAND ${GEN_VERSION_INFO_COMMAND}
    BYPRODUCTS "${VERSION_STAMP_FILE}")

#######################################
# kudu_util
#######################################

if (APPLE)
  set(SEMAPHORE_CC "semaphore_macosx.cc")
else ()
  set(SEMAPHORE_CC "semaphore.cc")
endif()

set(UTIL_SRCS
  async_logger.cc
  atomic.cc
  bitmap.cc
  bloom_filter.cc
  bitmap.cc
  cache.cc
  cache_metrics.cc
  coding.cc
  condition_variable.cc
  cow_object.cc
  crc.cc
  debug-util.cc
  decimal_util.cc
  debug/trace_event_impl.cc
  debug/trace_event_impl_constants.cc
  debug/trace_event_synthetic_delay.cc
  debug/unwind_safeness.cc
  easy_json.cc
  env.cc env_posix.cc env_util.cc
  errno.cc
  faststring.cc
  fault_injection.cc
  file_cache.cc
  flags.cc
  flag_tags.cc
  flag_validators.cc
  group_varint.cc
  pstack_watcher.cc
  hdr_histogram.cc
  hexdump.cc
  init.cc
  jsonreader.cc
  jsonwriter.cc
  kernel_stack_watchdog.cc
  locks.cc
  logging.cc
  maintenance_manager.cc
  malloc.cc
  memcmpable_varint.cc
  memory/arena.cc
  memory/memory.cc
  memory/overwrite.cc
  mem_tracker.cc
  metrics.cc
  minidump.cc
  monotime.cc
  mutex.cc
  net/dns_resolver.cc
  net/net_util.cc
  net/sockaddr.cc
  net/socket.cc
  oid_generator.cc
  once.cc
  os-util.cc
  path_util.cc
  pb_util.cc
  pb_util-internal.cc
  process_memory.cc
  random_util.cc
  rolling_log.cc
  rw_mutex.cc
  rwc_lock.cc
  ${SEMAPHORE_CC}
  signal.cc
  slice.cc
  spinlock_profiling.cc
  status.cc
  status_callback.cc
  string_case.cc
  striped64.cc
  subprocess.cc
  test_graph.cc
  test_util_prod.cc
  thread.cc
  threadlocal.cc
  threadpool.cc
  thread_restrictions.cc
  throttler.cc
  trace.cc
  trace_metrics.cc
  user.cc
  url-coding.cc
  version_info.cc
  version_util.cc
  website_util.cc
  zlib.cc
)

# overwrite.cc contains a single function which would be a hot spot in
# debug builds. It's separated into a separate file so it can be
# optimized regardless of the default optimization options.
set_source_files_properties(memory/overwrite.cc PROPERTIES COMPILE_FLAGS "-O3")

if(HAVE_LIB_VMEM)
  set(UTIL_SRCS
    ${UTIL_SRCS}
    nvm_cache.cc)
endif()

set(UTIL_LIBS
  crcutil
  gflags
  glog
  gutil
  histogram_proto
  libev
  maintenance_manager_proto
  pb_util_proto
  protobuf
  version_info_proto
  zlib)

if(NOT APPLE)
  set(UTIL_LIBS
    ${UTIL_LIBS}
    breakpad_client
    dl
    rt)
endif()

if(HAVE_LIB_VMEM)
  set(UTIL_LIBS
    ${UTIL_LIBS}
    vmem)
endif()

# We use MallocExtension, but not in the exported version of the library.
set(EXPORTED_UTIL_LIBS ${UTIL_LIBS})
if(${KUDU_TCMALLOC_AVAILABLE})
  list(APPEND UTIL_LIBS tcmalloc)
endif()

ADD_EXPORTABLE_LIBRARY(kudu_util
  SRCS ${UTIL_SRCS}
  DEPS ${UTIL_LIBS}
  NONLINK_DEPS gen_version_info
  EXPORTED_DEPS ${EXPORTED_UTIL_LIBS})

#######################################
# kudu_util_compression
#######################################
set(UTIL_COMPRESSION_SRCS
  compression/compression_codec.cc)
set(UTIL_COMPRESSION_LIBS
  kudu_util
  util_compression_proto

  glog
  gutil
  lz4
  snappy
  zlib)
ADD_EXPORTABLE_LIBRARY(kudu_util_compression
  SRCS ${UTIL_COMPRESSION_SRCS}
  DEPS ${UTIL_COMPRESSION_LIBS})

# See the comment in sanitizer_options.cc for details on this library's usage.
# The top-level CMakeLists sets a ${SANITIZER_OPTIONS_OVERRIDE} variable which
# should be linked first into all Kudu binaries.

#######################################
# sanitizer_options
#######################################
add_library(sanitizer_options STATIC sanitizer_options.cc)
target_link_libraries(sanitizer_options gutil)
if ("${KUDU_USE_ASAN}" OR "${KUDU_USE_TSAN}" OR "${KUDU_USE_UBSAN}")
  # By default the sanitizers use addr2line utility to symbolize reports.
  # llvm-symbolizer is faster, consumes less memory and produces much better reports.
  # We set KUDU_EXTERNAL_SYMBOLIZER_PATH which is used in sanitizer_options.cc
  # to set the default external_symbolizer_path.
  SET(KUDU_LLVM_SYMBOLIZER_PATH ${THIRDPARTY_INSTALL_UNINSTRUMENTED_DIR}/bin/llvm-symbolizer)
  if (EXISTS ${KUDU_LLVM_SYMBOLIZER_PATH})
    target_compile_definitions(sanitizer_options PRIVATE KUDU_EXTERNAL_SYMBOLIZER_PATH=${KUDU_LLVM_SYMBOLIZER_PATH})
  else()
    message(SEND_ERROR
      "Could not find llvm-symbolizer required for sanitizer builds at ${KUDU_LLVM_SYMBOLIZER_PATH}")
  endif()
endif()

#######################################
# kudu_test_util
#######################################

# Used by mini-cluster, so must be built even when NO_TESTS=0.
add_library(kudu_test_util
  test_util.cc)
target_link_libraries(kudu_test_util
  gflags
  glog
  gmock
  kudu_util)

if(HAVE_LIB_VMEM)
  target_link_libraries(kudu_test_util
    vmem)
endif()

#######################################
# kudu_curl_util
#######################################
if(NOT NO_TESTS)
  add_library(kudu_curl_util
    curl_util.cc)
  target_link_libraries(kudu_curl_util
    security
    ${CURL_LIBRARIES}
    glog
    gutil)
endif()

#######################################
# kudu_test_main
#######################################
if(NOT NO_TESTS)
  add_library(kudu_test_main
    test_main.cc)
  target_link_libraries(kudu_test_main
    ${KRB5_REALM_OVERRIDE}
    gflags
    glog
    gmock
    kudu_util
    kudu_test_util)

  if(NOT APPLE)
    target_link_libraries(kudu_test_main
      dl
      rt)
  endif()
endif()

#######################################
# protoc-gen-insertions
#######################################

add_executable(protoc-gen-insertions protoc-gen-insertions.cc)
target_link_libraries(protoc-gen-insertions gutil protobuf protoc ${KUDU_BASE_LIBS})

#######################################
# Unit tests
#######################################

SET_KUDU_TEST_LINK_LIBS(kudu_util gutil)
ADD_KUDU_TEST(async_util-test)
ADD_KUDU_TEST(atomic-test)
ADD_KUDU_TEST(bit-util-test)
ADD_KUDU_TEST(bitmap-test)
ADD_KUDU_TEST(blocking_queue-test)
ADD_KUDU_TEST(bloom_filter-test)
ADD_KUDU_TEST(cache-bench RUN_SERIAL true)
ADD_KUDU_TEST(cache-test)
ADD_KUDU_TEST(callback_bind-test)
ADD_KUDU_TEST(countdown_latch-test)
ADD_KUDU_TEST(crc-test RUN_SERIAL true) # has a benchmark
ADD_KUDU_TEST(debug-util-test)
ADD_KUDU_TEST(decimal_util-test)
ADD_KUDU_TEST(easy_json-test)
ADD_KUDU_TEST(env-test LABELS no_tsan)
ADD_KUDU_TEST(env_util-test)
ADD_KUDU_TEST(errno-test)
ADD_KUDU_TEST(faststring-test)
ADD_KUDU_TEST(file_cache-test)
ADD_KUDU_TEST(file_cache-stress-test RUN_SERIAL true)
ADD_KUDU_TEST(flag_tags-test)
ADD_KUDU_TEST(flag_validators-test)
ADD_KUDU_TEST(flags-test)
ADD_KUDU_TEST(group_varint-test)
ADD_KUDU_TEST(hash_util-test)
ADD_KUDU_TEST(hdr_histogram-test)
ADD_KUDU_TEST(int128-test)
ADD_KUDU_TEST(inline_slice-test)
ADD_KUDU_TEST(interval_tree-test)
ADD_KUDU_TEST(jsonreader-test)
ADD_KUDU_TEST(knapsack_solver-test)
ADD_KUDU_TEST(logging-test)
ADD_KUDU_TEST(maintenance_manager-test)
ADD_KUDU_TEST(map-util-test)
ADD_KUDU_TEST(mem_tracker-test)
ADD_KUDU_TEST(memcmpable_varint-test LABELS no_tsan)
ADD_KUDU_TEST(memory/arena-test)
ADD_KUDU_TEST(metrics-test)
ADD_KUDU_TEST(monotime-test)
ADD_KUDU_TEST(mt-hdr_histogram-test RUN_SERIAL true)
ADD_KUDU_TEST(mt-metrics-test RUN_SERIAL true)
ADD_KUDU_TEST(mt-threadlocal-test RUN_SERIAL true)
ADD_KUDU_TEST(net/dns_resolver-test)
ADD_KUDU_TEST(net/net_util-test)
ADD_KUDU_TEST(net/socket-test)
ADD_KUDU_TEST(object_pool-test)
ADD_KUDU_TEST(oid_generator-test)
ADD_KUDU_TEST(once-test)
ADD_KUDU_TEST(os-util-test)
ADD_KUDU_TEST(path_util-test)
ADD_KUDU_TEST(process_memory-test RUN_SERIAL true)
ADD_KUDU_TEST(random-test)
ADD_KUDU_TEST(random_util-test)
ADD_KUDU_TEST(rle-test)
ADD_KUDU_TEST(rolling_log-test)
ADD_KUDU_TEST(rw_mutex-test RUN_SERIAL true)
ADD_KUDU_TEST(rw_semaphore-test)
ADD_KUDU_TEST(rwc_lock-test RUN_SERIAL true)
ADD_KUDU_TEST(safe_math-test)
ADD_KUDU_TEST(scoped_cleanup-test)
ADD_KUDU_TEST(slice-test)
ADD_KUDU_TEST(sorted_disjoint_interval_list-test)
ADD_KUDU_TEST(spinlock_profiling-test)
ADD_KUDU_TEST(stack_watchdog-test PROCESSORS 2)
ADD_KUDU_TEST(status-test)
ADD_KUDU_TEST(string_case-test)
ADD_KUDU_TEST(striped64-test RUN_SERIAL true)
ADD_KUDU_TEST(subprocess-test)
ADD_KUDU_TEST(thread-test)
ADD_KUDU_TEST(threadpool-test)
ADD_KUDU_TEST(throttler-test)
ADD_KUDU_TEST(trace-test PROCESSORS 4)
ADD_KUDU_TEST(url-coding-test)
ADD_KUDU_TEST(user-test)
ADD_KUDU_TEST(version_util-test)

if (NOT APPLE)
  ADD_KUDU_TEST(minidump-test)
  ADD_KUDU_TEST(pstack_watcher-test)
endif()

#######################################
# jsonwriter_test_proto
#######################################

PROTOBUF_GENERATE_CPP(
  JSONWRITER_TEST_PROTO_SRCS JSONWRITER_TEST_PROTO_HDRS JSONWRITER_TEST_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES jsonwriter_test.proto)
add_library(jsonwriter_test_proto ${JSONWRITER_TEST_PROTO_SRCS} ${JSONWRITER_TEST_PROTO_HDRS})
target_link_libraries(jsonwriter_test_proto
  pb_util_proto
  protobuf)

#######################################
# jsonwriter-test
#######################################

ADD_KUDU_TEST(jsonwriter-test)
if(NOT NO_TESTS)
  target_link_libraries(jsonwriter-test
    jsonwriter_test_proto)
endif()

#######################################
# pb_util_test_proto
#######################################

PROTOBUF_GENERATE_CPP(
  PROTO_CONTAINER_TEST_PROTO_SRCS PROTO_CONTAINER_TEST_PROTO_HDRS PROTO_CONTAINER_TEST_PROTO_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES
    proto_container_test.proto
    proto_container_test2.proto
    proto_container_test3.proto
    pb_util_test.proto)
add_library(pb_util_test_proto
  ${PROTO_CONTAINER_TEST_PROTO_SRCS}
  ${PROTO_CONTAINER_TEST_PROTO_HDRS})
target_link_libraries(pb_util_test_proto
  pb_util_proto
  protobuf)

#######################################
# pb_util-test
#######################################

ADD_KUDU_TEST(pb_util-test)
if(NOT NO_TESTS)
  target_link_libraries(pb_util-test
    pb_util_test_proto)
endif()

#######################################
# util/compression tests
#######################################
ADD_KUDU_TEST(compression/compression-test)
if(NOT NO_TESTS)
  target_link_libraries(compression-test
    cfile
    kudu_util_compression)
endif()
