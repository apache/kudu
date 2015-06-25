# - Find Bitshuffle (bitshuffle.h, bitshuffle.a)
# This module defines
#  BITSHUFFLE_INCLUDE_DIR, directory containing headers
#  BITSHUFFLE_LIBS, directory containing bitshuffle libraries
#  BITSHUFFLE_STATIC_LIB, path to bitshuffle.a
#  BITSHUFFLE_FOUND, whether bitshuffle has been found

set(BITSHUFFLE_SEARCH_HEADER_PATHS
  ${THIRDPARTY_PREFIX}/include
)

set(BITSHUFFLE_SEARCH_LIB_PATH
  ${THIRDPARTY_PREFIX}/lib
)

find_path(BITSHUFFLE_INCLUDE_DIR bitshuffle.h PATHS
  ${BITSHUFFLE_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(BITSHUFFLE_LIB_PATH
 NAMES bitshuffle.a
 PATHS ${BITSHUFFLE_SEARCH_LIB_PATH}
 NO_DEFAULT_PATH)

if (BITSHUFFLE_INCLUDE_DIR AND BITSHUFFLE_LIB_PATH)
  set(BITSHUFFLE_FOUND TRUE)
  set(BITSHUFFLE_LIBS ${BITSHUFFLE_SEARCH_LIB_PATH})
  set(BITSHUFFLE_STATIC_LIB ${BITSHUFFLE_SEARCH_LIB_PATH}/bitshuffle.a)
else ()
  set(BITSHUFFLE_FOUND FALSE)
endif ()

if (BITSHUFFLE_FOUND)
  if (NOT Bitshuffle_FIND_QUIETLY)
    message(STATUS "Found the Bitshuffle library: ${BITSHUFFLE_LIB_PATH}")
  endif ()
else ()
  if (NOT Bitshuffle_FIND_QUIETLY)
    set(BITSHUFFLE_ERR_MSG "Could not find the Bitshuffle library. Looked for headers")
    set(BITSHUFFLE_ERR_MSG "${BITSHUFFLE_ERR_MSG} in ${BITSHUFFLE_SEARCH_HEADER_PATHS}, and for libs")
    set(BITSHUFFLE_ERR_MSG "${BITSHUFFLE_ERR_MSG} in ${BITSHUFFLE_SEARCH_LIB_PATH}")
    if (Bitshuffle_FIND_REQUIRED)
      message(FATAL_ERROR "${BITSHUFFLE_ERR_MSG}")
    else (Bitshuffle_FIND_REQUIRED)
      message(STATUS "${BITSHUFFLE_ERR_MSG}")
    endif (Bitshuffle_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  BITSHUFFLE_INCLUDE_DIR
  BITSHUFFLE_LIBS
  BITSHUFFLE_STATIC_LIB
)
