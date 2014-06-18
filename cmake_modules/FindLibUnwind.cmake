# Copyright (c) 2014, Cloudera, inc.
# All rights reserved.
#
# - Find libunwind (libunwind.h, libunwind.so)
#
# This module defines
#  UNWIND_INCLUDE_DIR, directory containing headers
#  UNWIND_SHARED_LIB, path to libunwind.so
#  UNWIND_STATIC_LIB, path to libunwind.a

set(UNWIND_SEARCH_HEADER_PATHS ${THIRDPARTY_PREFIX}/include)
set(UNWIND_SEARCH_LIB_PATH ${THIRDPARTY_PREFIX}/lib)

find_path(UNWIND_INCLUDE_DIR libunwind.h
  PATHS ${UNWIND_SEARCH_HEADER_PATHS}
  NO_DEFAULT_PATH)

find_library(UNWIND_SHARED_LIB
  NAMES libunwind.so
  PATHS ${UNWIND_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH)

if (UNWIND_INCLUDE_DIR AND UNWIND_SHARED_LIB)
  set(UNWIND_FOUND TRUE)
  set(UNWIND_STATIC_LIB ${UNWIND_SEARCH_LIB_PATH}/libunwind.a)
else ()
  set(UNWIND_FOUND FALSE)
endif ()

if (UNWIND_FOUND)
  if (NOT LibUnwind_FIND_QUIETLY)
    message(STATUS "Found the libunwind library: ${UNWIND_SHARED_LIB}")
  endif ()
else ()
  if (NOT LibUnwind_FIND_QUIETLY)
    set(UNWIND_ERR_MSG "Could not find the libunwind library. Looked for headers")
    set(UNWIND_ERR_MSG "${UNWIND_ERR_MSG} in ${UNWIND_SEARCH_HEADER_PATHS}, and for libs")
    set(UNWIND_ERR_MSG "${UNWIND_ERR_MSG} in ${UNWIND_SEARCH_LIB_PATH}")
    if (LibUnwind_FIND_REQUIRED)
      message(FATAL_ERROR "${UNWIND_ERR_MSG}")
    else (LibUnwind_FIND_REQUIRED)
      message(STATUS "${UNWIND_ERR_MSG}")
    endif (LibUnwind_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  UNWIND_INCLUDE_DIR
  UNWIND_SHARED_LIB
  UNWIND_STATIC_LIB
)
