# Copyright (c) 2013, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

set(CLIENT_SRCS
  batcher.cc
  client.cc
  client_builder-internal.cc
  client-internal.cc
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
  tablet_server-internal.cc
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
    PROPERTIES LINK_FLAGS -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map)
endif()
target_link_libraries(kudu_client ${CLIENT_LIBS_VISIBILITY} ${CLIENT_LIBS})

# Generate kudu_export.h.
generate_export_header(kudu_client
  BASE_NAME kudu
  EXPORT_FILE_NAME ${CMAKE_SOURCE_DIR}/src/kudu/util/kudu_export.h)

# "make install" invocations to generate a directory tree containing the
# exported client library and all of its headers.
if (KUDU_EXPORTED_CLIENT)
  # For CMAKE_INSTALL_<dir> variables.
  include(GNUInstallDirs)

  # Shared library.
  install(TARGETS kudu_client
    EXPORT kudu_client_export_set
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

  # Headers: client
  install(FILES
    callbacks.h
    client.h
    row_result.h
    scan_predicate.h
    schema.h
    stubs.h
    write_op.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kudu/client)

  # Headers: common
  install(FILES
    ../common/partial_row.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kudu/common)

  # Headers: util
  install(FILES
    ../util/kudu_export.h
    ../util/monotime.h
    ../util/slice.h
    ../util/status.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kudu/util)

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
  ADD_KUDU_TEST(client_symbol-test.sh)

  # Test that the client sample code works properly when built out-of-tree.
  ADD_KUDU_TEST(client_samples-test.sh RUN_SERIAL true)
else()
  # Can't be used with the exported client; it references hidden symbols.
  set(KUDU_TEST_LINK_LIBS kudu_client integration-tests ${KUDU_MIN_TEST_LIBS})
  ADD_KUDU_TEST(client-test)
endif()
