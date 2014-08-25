# - Find CRCUTIL (crcutil/include.h, libcrcutil.a)
# This module defines
#  CRCUTIL_INCLUDE_DIR, directory containing headers
#  CRCUTIL_LIBS, directory containing crcutil libraries
#  CRCUTIL_STATIC_LIB, path to libcrcutil.a
#  CRCUTIL_SHARED_LIB, path to libcrcutil's shared library
#  CRCUTIL_FOUND, whether crcutil has been found

set(CRCUTIL_SEARCH_HEADER_PATHS
  ${THIRDPARTY_PREFIX}/include
)

set(CRCUTIL_SEARCH_LIB_PATH
  ${THIRDPARTY_PREFIX}/lib
)

find_path(CRCUTIL_INCLUDE_DIR crcutil/interface.h
  PATHS ${CRCUTIL_SEARCH_HEADER_PATHS}
  NO_DEFAULT_PATH)

find_library(CRCUTIL_LIB_PATH
  NAMES crcutil
  PATHS ${CRCUTIL_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH)

if (CRCUTIL_INCLUDE_DIR AND CRCUTIL_LIB_PATH)
  set(CRCUTIL_FOUND TRUE)
  set(CRCUTIL_LIBS ${CRCUTIL_SEARCH_LIB_PATH})
  set(CRCUTIL_LIB_NAME libcrcutil)
  set(CRCUTIL_STATIC_LIB ${CRCUTIL_SEARCH_LIB_PATH}/${CRCUTIL_LIB_NAME}.a)
  set(CRCUTIL_SHARED_LIB
      ${CRCUTIL_SEARCH_LIB_PATH}/${CRCUTIL_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
else ()
  set(CRCUTIL_FOUND FALSE)
endif ()

if (CRCUTIL_FOUND)
  if (NOT Crcutil_FIND_QUIETLY)
    message(STATUS "Found the crcutil library: ${CRCUTIL_LIB_PATH}")
  endif ()
else ()
  if (NOT Crcutil_FIND_QUIETLY)
    set(CRCUTIL_ERR_MSG "Could not find the Crcutil library. Looked for headers")
    set(CRCUTIL_ERR_MSG "${CRCUTIL_ERR_MSG} in ${CRCUTIL_SEARCH_HEADER_PATHS} and for libs")
    set(CRCUTIL_ERR_MSG "${CRCUTIL_ERR_MSG} in ${CRCUTIL_SEARCH_LIB_PATH}")
    if (Crcutil_FIND_REQUIRED)
      message(FATAL_ERROR "${CRCUTIL_ERR_MSG}")
    else (Crcutil_FIND_REQUIRED)
      message(STATUS "${CRCUTIL_ERR_MSG}")
    endif (Crcutil_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  CRCUTIL_INCLUDE_DIR
  CRCUTIL_LIBS
  CRCUTIL_STATIC_LIB
  CRCUTIL_SHARED_LIB
)

