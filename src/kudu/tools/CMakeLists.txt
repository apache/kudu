# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

set(LINK_LIBS
  kudu_client
  log
  consensus
  tserver
  kudu_common
  kudu_fs
  kudu_util
  gutil
  cfile
  tablet
  ${KUDU_BASE_LIBS}
)

add_library(kudu_tools_util
  data_gen_util.cc)
target_link_libraries(kudu_tools_util
  ${LINK_LIBS})

add_executable(create-demo-table create-demo-table.cc)
target_link_libraries(create-demo-table
  ${LINK_LIBS})

add_executable(insert-generated-rows insert-generated-rows.cc)
target_link_libraries(insert-generated-rows
  kudu_tools_util
  ${LINK_LIBS})

add_executable(kudu-admin kudu-admin.cc)
target_link_libraries(kudu-admin
  ${LINK_LIBS})

add_executable(kudu-ts-cli ts-cli.cc)
target_link_libraries(kudu-ts-cli
  ${LINK_LIBS})

add_library(fs_tool fs_tool.cc)
target_link_libraries(fs_tool
  gutil
  kudu_common
  server_common
  consensus
  tablet)

add_executable(kudu-fs_list fs_list-tool.cc)
target_link_libraries(kudu-fs_list
  ${LINK_LIBS}
  fs_tool)

add_executable(kudu-fs_dump fs_dump-tool.cc)
target_link_libraries(kudu-fs_dump
  ${LINK_LIBS}
  fs_tool)

add_library(ksck
    ksck.cc
    ksck_remote.cc
)
target_link_libraries(ksck
  master_proto
  server_base_proto
  tserver_proto
  tserver_service_proto
  ${KUDU_BASE_LIBS}
)

add_executable(kudu-ksck ksck_main.cc)
target_link_libraries(kudu-ksck
  ksck
)

add_executable(kudu-pbc-dump pbc-dump.cc)
target_link_libraries(kudu-pbc-dump
  ${LINK_LIBS}
)

set(KUDU_TEST_LINK_LIBS
  ksck
  kudu_tools_util
  integration-tests
  ${KUDU_MIN_TEST_LIBS})
ADD_KUDU_TEST(ksck-test)
ADD_KUDU_TEST(ksck_remote-test)
ADD_KUDU_TEST(kudu-admin-test)
ADD_KUDU_TEST_DEPENDENCIES(kudu-admin-test
  kudu-admin)

