# Copyright (c) 2013, Cloudera, inc.
# - Find Squeasel (squeasel.h, libsqueasel.a)
# This module defines
#  SQUEASEL_INCLUDE_DIR, directory containing headers
#  SQUEASEL_STATIC_LIB, path to libsqueasel.a
#  SQUEASEL_FOUND, whether squeasel has been found
#  SQUEASEL_STATIC_LIB_DEPS other libraries that are dependencies for SQUEASEL_STATIC_LIB

set(SQUEASEL_SEARCH_HEADER_PATHS
  ${THIRDPARTY_PREFIX}/include
)

set(SQUEASEL_SEARCH_LIB_PATH
  ${THIRDPARTY_PREFIX}/lib
)

find_path(SQUEASEL_INCLUDE_DIR squeasel.h
  PATHS ${SQUEASEL_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(SQUEASEL_LIB_PATH
  NAMES squeasel
  PATHS ${SQUEASEL_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH)

if (SQUEASEL_LIB_PATH AND SQUEASEL_INCLUDE_DIR)
  set(SQUEASEL_FOUND TRUE)
  set(SQUEASEL_LIBS ${SQUEASEL_SEARCH_LIB_PATH})
  set(SQUEASEL_STATIC_LIB ${SQUEASEL_SEARCH_LIB_PATH}/libsqueasel.a)
else ()
  set(SQUEASEL_FOUND FALSE)
endif ()

if (SQUEASEL_FOUND)
  if (NOT Squeasel_FIND_QUIETLY)
    message(STATUS "Found the Squeasel library: ${SQUEASEL_LIB_PATH}")
  endif ()
else ()
  if (NOT Squeasel_FIND_QUIETLY)
    set(SQUEASEL_ERR_MSG "Could not find the Squeasel library. Looked for headers")
    set(SQUEASEL_ERR_MSG "${SQUEASEL_ERR_MSG} in ${SQUEASEL_SEARCH_HEADER_PATHS}, and for libs")
    set(SQUEASEL_ERR_MSG "${SQUEASEL_ERR_MSG} in ${SQUEASEL_SEARCH_LIB_PATH}")
    if (Squeasel_FIND_REQUIRED)
      message(FATAL_ERROR "${SQUEASEL_ERR_MSG}")
    else (Squeasel_FIND_REQUIRED)
      message(STATUS "${SQUEASEL_ERR_MSG}")
    endif (Squeasel_FIND_REQUIRED)
  endif ()
endif ()

# Add OpenSSL libs to the linker path
find_package(OpenSSL REQUIRED)
if (OPENSSL_FOUND)
  set(SQUEASEL_STATIC_LIB_DEPS ${OPENSSL_LIBRARIES})
else ()
  if (NOT Squeasel_FIND_QUIETLY)
    message(FATAL_ERROR "Cannot find OpenSSL library")
  endif ()
endif ()

mark_as_advanced(
  SQUEASEL_INCLUDE_DIR
  SQUEASEL_LIBS
  SQUEASEL_STATIC_LIB
  SQUEASEL_STATIC_LIB_DEPS
)
