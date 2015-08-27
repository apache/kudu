# Copyright (c) 2015, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#
# Finds all Kudu client cmake installation files and replaces all references
# to kudu_client_exported with kudu_client, thus renaming the targets.

set(CMAKE_FILES_DIR "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/share/kuduClient/cmake")
if(NOT EXISTS ${CMAKE_FILES_DIR})
  message(FATAL_ERROR "Cannot find cmake installation directory ${CMAKE_FILES_DIR}")
endif()
file(GLOB CMAKE_FILES "${CMAKE_FILES_DIR}/*.cmake")
foreach(CMAKE_FILE ${CMAKE_FILES})
  message(STATUS "Munging kudu client targets in ${CMAKE_FILE}")
  execute_process(COMMAND sed -i s/kudu_client_exported/kudu_client/g ${CMAKE_FILE})
endforeach()
