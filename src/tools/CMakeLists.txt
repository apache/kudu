# Copyright (c) 2014, Cloudera, inc.

add_executable(create-demo-table create-demo-table.cc)
target_link_libraries(create-demo-table ${KUDU_LINK_LIBS})