# - Find ZLIB (zlib.h, libz.a, libz.so, and libz.so.1)
# This module defines
#  ZLIB_INCLUDE_DIR, directory containing headers
#  ZLIB_LIBS, directory containing zlib libraries
#  ZLIB_STATIC_LIB, path to libz.a
#  ZLIB_FOUND, whether zlib has been found

set(ZLIB_SEARCH_HEADER_PATHS
  ${THIRDPARTY_PREFIX}/include
)

set(ZLIB_SEARCH_LIB_PATH
  ${THIRDPARTY_PREFIX}/lib
)

find_path(ZLIB_INCLUDE_DIR zlib.h PATHS
  ${ZLIB_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(ZLIB_LIB_PATH NAMES z PATHS ${ZLIB_SEARCH_LIB_PATH})

if (ZLIB_LIB_PATH)
  set(ZLIB_FOUND TRUE)
  set(ZLIB_LIBS ${ZLIB_SEARCH_LIB_PATH})
  set(ZLIB_STATIC_LIB ${ZLIB_SEARCH_LIB_PATH}/libz.a)
else ()
  set(ZLIB_FOUND FALSE)
endif ()

if (ZLIB_FOUND)
  if (NOT ZLIB_FIND_QUIETLY)
    message(STATUS "zlib Found in ${ZLIB_SEARCH_LIB_PATH}")
  endif ()
else ()
  message(STATUS "zlib includes and libraries NOT found. "
    "Looked for headers in ${ZLIB_SEARCH_HEADER_PATH}, "
    "and for libs in ${ZLIB_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  ZLIB_INCLUDE_DIR
  ZLIB_LIBS
  ZLIB_STATIC_LIB
)
