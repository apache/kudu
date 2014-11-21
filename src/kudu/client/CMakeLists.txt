# Copyright (c) 2013, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

set(CLIENT_SRCS
  batcher.cc
  client.cc
  client_builder-internal.cc
  client-internal.cc
  encoded_key.cc
  error_collector.cc
  error-internal.cc
  meta_cache.cc
  row_result.cc
  scan_predicate.cc
  scanner-internal.cc
  session-internal.cc
  schema.cc
  table-internal.cc
  table_alterer-internal.cc
  table_creator-internal.cc
  write_op.cc
)

if (KUDU_EXPORTED_CLIENT)
  # We're building a shared library and providing all transitive
  # dependencies on the link line. As such, the dependencies should
  # not be visible to consumers of the 'client' target.
  set(CLIENT_LINKAGE "SHARED")
  set(CLIENT_LIBS ${KUDU_BASE_LIBS})
  set(CLIENT_LIBS_VISIBILITY "LINK_PRIVATE")
endif()

add_library(kudu_client ${CLIENT_LINKAGE} ${CLIENT_SRCS})
set(CLIENT_LIBS
  kudu_common
  master_proto
  master_rpc
  tserver_proto
  tserver_service_proto
  kudu_util
  gutil
  krpc
  ${CLIENT_LIBS})

if (KUDU_EXPORTED_CLIENT)
  # Hide symbols using a linker version script.
  set_target_properties(kudu_client
    PROPERTIES LINK_FLAGS -Wl,--version-script=symbols.map)
endif()
target_link_libraries(kudu_client ${CLIENT_LIBS_VISIBILITY} ${CLIENT_LIBS})

# Generate kudu_export.h.
generate_export_header(kudu_client
  BASE_NAME kudu
  EXPORT_FILE_NAME ${CMAKE_SOURCE_DIR}/src/kudu/gutil/kudu_export.h)

# "make install" invocations to generate a directory tree containing the
# exported client library and all of its headers.
if (KUDU_EXPORTED_CLIENT)
  # For CMAKE_INSTALL_<dir> variables.
  include(GNUInstallDirs)

  # Shared library.
  install(TARGETS kudu_client
    EXPORT kudu_client_export_set
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

  # Headers: gutil
  install(FILES
    ../gutil/arm_instruction_set_select.h
    ../gutil/atomic_refcount.h
    ../gutil/atomicops.h
    ../gutil/atomicops-internals-x86.h
    ../gutil/basictypes.h
    ../gutil/bind.h
    ../gutil/bind_helpers.h
    ../gutil/bind_internal.h
    ../gutil/callback.h
    ../gutil/callback_forward.h
    ../gutil/callback_internal.h
    ../gutil/casts.h
    ../gutil/dynamic_annotations.h
    ../gutil/gscoped_ptr.h
    ../gutil/gtest.h
    ../gutil/int128.h
    ../gutil/integral_types.h
    ../gutil/kudu_export.h
    ../gutil/logging.h
    ../gutil/logging-inl.h
    ../gutil/macros.h
    ../gutil/move.h
    ../gutil/port.h
    ../gutil/raw_scoped_refptr_mismatch_checker.h
    ../gutil/ref_counted.h
    ../gutil/template_util.h
    ../gutil/tuple.h
    ../gutil/type_traits.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kudu/gutil)

  # Headers: gutil/hash
  install(FILES
    ../gutil/hash/builtin_type_hash.h
    ../gutil/hash/city.h
    ../gutil/hash/hash.h
    ../gutil/hash/hash128to64.h
    ../gutil/hash/jenkins.h
    ../gutil/hash/jenkins_lookup2.h
    ../gutil/hash/legacy_hash.h
    ../gutil/hash/string_hash.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kudu/gutil/hash)

  # Headers: gutil/strings
  install(FILES
    ../gutil/strings/fastmem.h
    ../gutil/strings/stringpiece.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kudu/gutil/strings)

  # Headers: gutil/threading
  install(FILES
    ../gutil/threading/thread_collision_warner.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kudu/gutil/threading)

  # Headers: client
  install(FILES
    client.h
    encoded_key.h
    row_result.h
    scan_predicate.h
    schema.h
    write_op.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kudu/client)

  # Headers: common
  install(FILES
    ../common/partial_row.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kudu/common)

  # Headers: util
  install(FILES
    ../util/faststring.h
    ../util/monotime.h
    ../util/slice.h
    ../util/status.h
    ../util/status_callback.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kudu/util)

  # Headers: gtest (thirdparty)
  install(FILES
    ${GTEST_INCLUDE_DIR}/gtest/gtest_prod.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/gtest)

  # Client sample code.
  #
  # Can't use CMAKE_INSTALL_DOCDIR because we don't ever call project().
  install(FILES
    samples/CMakeLists.txt
    samples/sample.cc
    DESTINATION ${CMAKE_INSTALL_DATADIR}/doc/kuduClient/samples)

  # Exported cmake file for just the library's targets.
  #
  # Should not be included directly by users.
  install(EXPORT kudu_client_export_set
    FILE kuduClientTargets.cmake
    DESTINATION ${CMAKE_INSTALL_DATADIR}/kuduClient/cmake)

  # Exported cmake file for the library.
  #
  # This is the main entry point and should be included directly.
  include(CMakePackageConfigHelpers)
  configure_package_config_file(clientConfig.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/clientConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_DATADIR}/kuduClient/cmake
    PATH_VARS CMAKE_INSTALL_INCLUDEDIR)
  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/clientConfig.cmake
    DESTINATION ${CMAKE_INSTALL_DATADIR}/kuduClient/cmake
    RENAME kuduClientConfig.cmake)
endif()

# Test utility library

# This code is useful for other tests which use the client, but isn't
# part of the client itself (ie we don't want to ship it to customers,
# and therefore don't need to worry about export strictness)
add_library(kudu_client_test_util
  client-test-util.cc)
target_link_libraries(kudu_client_test_util kudu_client)

# Tests

if (KUDU_EXPORTED_CLIENT)
  # Test that there are no forbidden symbols in the exported client library.
  ADD_KUDU_TEST(client_symbol-test)

  # Test that the client sample code works properly when built out-of-tree.
  ADD_KUDU_TEST(client_samples-test RUN_SERIAL true)
else()
  # Can't be used with the exported client; it references hidden symbols.
  set(KUDU_TEST_LINK_LIBS kudu_client integration-tests ${KUDU_MIN_TEST_LIBS})
  ADD_KUDU_TEST(client-test)
endif()
