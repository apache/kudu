# - Find SNAPPY (snappy.h, libsnappy.a, libsnappy.so, and libsnappy.so.1)
# This module defines
#  SNAPPY_INCLUDE_DIR, directory containing headers
#  SNAPPY_LIBS, directory containing snappy libraries
#  SNAPPY_STATIC_LIB, path to libsnappy.a
#  SNAPPY_FOUND, whether snappy has been found

set(SNAPPY_SEARCH_HEADER_PATHS
  ${THIRDPARTY_PREFIX}/include
)

set(SNAPPY_SEARCH_LIB_PATH
  ${THIRDPARTY_PREFIX}/lib
)

find_path(SNAPPY_INCLUDE_DIR snappy.h PATHS
  ${SNAPPY_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(SNAPPY_LIB_PATH NAMES snappy PATHS ${SNAPPY_SEARCH_LIB_PATH})

if (SNAPPY_LIB_PATH)
  set(SNAPPY_FOUND TRUE)
  set(SNAPPY_LIBS ${SNAPPY_SEARCH_LIB_PATH})
  set(SNAPPY_STATIC_LIB ${SNAPPY_SEARCH_LIB_PATH}/libsnappy.a)
else ()
  set(SNAPPY_FOUND FALSE)
endif ()

if (SNAPPY_FOUND)
  if (NOT SNAPPY_FIND_QUIETLY)
    message(STATUS "snappy Found in ${SNAPPY_SEARCH_LIB_PATH}")
  endif ()
else ()
  message(STATUS "snappy includes and libraries NOT found. "
    "Looked for headers in ${SNAPPY_SEARCH_HEADER_PATH}, "
    "and for libs in ${SNAPPY_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  SNAPPY_INCLUDE_DIR
  SNAPPY_LIBS
  SNAPPY_STATIC_LIB
)
