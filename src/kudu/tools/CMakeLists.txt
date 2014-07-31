# Copyright (c) 2014, Cloudera, inc.

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

add_executable(create-demo-table create-demo-table.cc)
target_link_libraries(create-demo-table
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
