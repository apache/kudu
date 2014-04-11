# Copyright (c) 2014, Cloudera, inc.

add_executable(create-demo-table create-demo-table.cc)
target_link_libraries(create-demo-table ${KUDU_LINK_LIBS})

add_executable(kudu-ts-cli ts-cli.cc)
target_link_libraries(kudu-ts-cli ${KUDU_LINK_LIBS})

set(KUDU_LINK_LIBS
  log
  consensus
  tserver
  kudu_common
  kudu_util
  gutil
  cfile
  tablet
  ${KUDU_LINK_LIBS}
)

add_library(fs_tool fs_tool.cc)
target_link_libraries(fs_tool ${KUDU_LINK_LIBS})

add_executable(kudu-fs_list fs_list-tool.cc)
target_link_libraries(kudu-fs_list ${KUDU_LINK_LIBS})
target_link_libraries(kudu-fs_list fs_tool)