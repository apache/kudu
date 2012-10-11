# - Find GLOG (logging.h, libglog.a, libglog.so, and libglog.so.0)
# This module defines
#  GLOG_INCLUDE_DIR, directory containing headers
#  GLOG_LIBS, directory containing glog libraries
#  GLOG_STATIC_LIB, path to libglog.a (
#  GLOG_FOUND, whether glog has been found

set(GLOG_SEARCH_HEADER_PATHS
  ${CMAKE_SOURCE_DIR}/thirdparty/glog-0.3.1/src
)

set(GLOG_SEARCH_LIB_PATH
  ${CMAKE_SOURCE_DIR}/thirdparty/glog-0.3.1/.libs
)

find_path(GLOG_INCLUDE_DIR glog/logging.h PATHS
  ${GLOG_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(GLOG_LIB_PATH NAMES glog PATHS ${GLOG_SEARCH_LIB_PATH})

if (GLOG_LIB_PATH)
  set(GLOG_FOUND TRUE)
  set(GLOG_LIBS ${GLOG_SEARCH_LIB_PATH})
  set(GLOG_STATIC_LIB ${GLOG_SEARCH_LIB_PATH}/libglog.a)
else ()
  set(GLOG_FOUND FALSE)
endif ()

if (GLOG_FOUND)
  if (NOT GLOG_FIND_QUIETLY)
    message(STATUS "GLog Found in ${GLOG_SEARCH_LIB_PATH}")
  endif ()
else ()
  message(STATUS "GLog includes and libraries NOT found. "
    "Looked for headers in ${GLOG_SEARCH_HEADER_PATH}, "
    "and for libs in ${GLOG_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  GLOG_INCLUDE_DIR
  GLOG_LIBS
  GLOG_STATIC_LIB
)
