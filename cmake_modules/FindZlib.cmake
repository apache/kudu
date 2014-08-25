# - Find ZLIB (zlib.h, libz.a, libz.so, and libz.so.1)
# This module defines
#  ZLIB_INCLUDE_DIR, directory containing headers
#  ZLIB_LIBS, directory containing zlib libraries
#  ZLIB_STATIC_LIB, path to libz.a
#  ZLIB_SHARED_LIB, path to libz's shared library
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

find_library(ZLIB_LIB_PATH NAMES z PATHS ${ZLIB_SEARCH_LIB_PATH} NO_DEFAULT_PATH)

if (ZLIB_INCLUDE_DIR AND ZLIB_LIB_PATH)
  set(ZLIB_FOUND TRUE)
  set(ZLIB_LIB_NAME libz)
  set(ZLIB_LIBS ${ZLIB_SEARCH_LIB_PATH})
  set(ZLIB_STATIC_LIB ${ZLIB_SEARCH_LIB_PATH}/${ZLIB_LIB_NAME}.a)
  set(ZLIB_SHARED_LIB ${ZLIB_LIBS}/${ZLIB_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
else ()
  set(ZLIB_FOUND FALSE)
endif ()

if (ZLIB_FOUND)
  if (NOT Zlib_FIND_QUIETLY)
    message(STATUS "Found the Zlib library: ${ZLIB_LIB_PATH}")
  endif ()
else ()
  if (NOT Zlib_FIND_QUIETLY)
    set(ZLIB_ERR_MSG "Could not find the Zlib library. Looked for headers")
    set(ZLIB_ERR_MSG "${ZLIB_ERR_MSG} in ${ZLIB_SEARCH_HEADER_PATHS}, and for libs")
    set(ZLIB_ERR_MSG "${ZLIB_ERR_MSG} in ${ZLIB_SEARCH_LIB_PATH}")
    if (Zlib_FIND_REQUIRED)
      message(FATAL_ERROR "${ZLIB_ERR_MSG}")
    else (Zlib_FIND_REQUIRED)
      message(STATUS "${ZLIB_ERR_MSG}")
    endif (Zlib_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  ZLIB_INCLUDE_DIR
  ZLIB_LIBS
  ZLIB_STATIC_LIB
  ZLIB_SHARED_LIB
)
