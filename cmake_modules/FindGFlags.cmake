# - Find GFLAGS (gflags.h, libgflags.a, libgflags.so, and libgflags.so.0)
# This module defines
#  GFLAGS_INCLUDE_DIR, directory containing headers
#  GFLAGS_LIBS, directory containing gflag libraries
#  GFLAGS_STATIC_LIB, path to libgflags.a
#  GFLAGS_FOUND, whether gflags has been found

set(GFLAGS_SEARCH_HEADER_PATHS  
  ${CMAKE_SOURCE_DIR}/thirdparty/gflags-1.5/src
)

set(GFLAGS_SEARCH_LIB_PATH
  ${CMAKE_SOURCE_DIR}/thirdparty/gflags-1.5/.libs
)

find_path(GFLAGS_INCLUDE_DIR gflags/gflags.h PATHS
  ${GFLAGS_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(GFLAGS_LIB_PATH NAMES gflags PATHS ${GFLAGS_SEARCH_LIB_PATH})

if (GFLAGS_LIB_PATH)
  set(GFLAGS_FOUND TRUE)
  set(GFLAGS_LIBS ${GFLAGS_SEARCH_LIB_PATH})
  set(GFLAGS_STATIC_LIB ${GFLAGS_SEARCH_LIB_PATH}/libgflags.a)
else ()
  set(GFLAGS_FOUND FALSE)
endif ()

if (GFLAGS_FOUND)
  if (NOT GFLAGS_FIND_QUIETLY)
    message(STATUS "GFlags Found in ${GFLAGS_SEARCH_LIB_PATH}")
  endif ()
else ()
  message(STATUS "GFlags includes and libraries NOT found. "
    "Looked for headers in ${GFLAGS_SEARCH_HEADER_PATH}, "
    "and for libs in ${GFLAGS_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  GFLAGS_INCLUDE_DIR
  GFLAGS_LIBS
  GFLAGS_STATIC_LIB
)
