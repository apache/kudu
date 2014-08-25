# - Find SNAPPY (snappy.h, libsnappy.a, libsnappy.so, and libsnappy.so.1)
# This module defines
#  SNAPPY_INCLUDE_DIR, directory containing headers
#  SNAPPY_LIBS, directory containing snappy libraries
#  SNAPPY_STATIC_LIB, path to libsnappy.a
#  SNAPPY_SHARED_LIB, path to libsnappy's shared library
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

find_library(SNAPPY_LIB_PATH NAMES snappy PATHS ${SNAPPY_SEARCH_LIB_PATH} NO_DEFAULT_PATH)

if (SNAPPY_INCLUDE_DIR AND SNAPPY_LIB_PATH)
  set(SNAPPY_FOUND TRUE)
  set(SNAPPY_LIB_NAME libsnappy)
  set(SNAPPY_LIBS ${SNAPPY_SEARCH_LIB_PATH})
  set(SNAPPY_STATIC_LIB ${SNAPPY_SEARCH_LIB_PATH}/${SNAPPY_LIB_NAME}.a)
  set(SNAPPY_SHARED_LIB ${SNAPPY_LIBS}/${SNAPPY_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
else ()
  set(SNAPPY_FOUND FALSE)
endif ()

if (SNAPPY_FOUND)
  if (NOT Snappy_FIND_QUIETLY)
    message(STATUS "Found the Snappy library: ${SNAPPY_LIB_PATH}")
  endif ()
else ()
  if (NOT Snappy_FIND_QUIETLY)
    set(SNAPPY_ERR_MSG "Could not find the Snappy library. Looked for headers")
    set(SNAPPY_ERR_MSG "${SNAPPY_ERR_MSG} in ${SNAPPY_SEARCH_HEADER_PATHS}, and for libs")
    set(SNAPPY_ERR_MSG "${SNAPPY_ERR_MSG} in ${SNAPPY_SEARCH_LIB_PATH}")
    if (Snappy_FIND_REQUIRED)
      message(FATAL_ERROR "${SNAPPY_ERR_MSG}")
    else (Snappy_FIND_REQUIRED)
      message(STATUS "${SNAPPY_ERR_MSG}")
    endif (Snappy_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  SNAPPY_INCLUDE_DIR
  SNAPPY_LIBS
  SNAPPY_STATIC_LIB
  SNAPPY_SHARED_LIB
)
