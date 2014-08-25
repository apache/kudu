# - Find LIBEV (ev++.h, libev.a, and libev.so)
# This module defines
#  LIBEV_INCLUDE_DIR, directory containing headers
#  LIBEV_LIBS, directory containing libev libraries
#  LIBEV_STATIC_LIB, path to libev.a
#  LIBEV_SHARED_LIB, path to libev's shared library
#  LIBEV_FOUND, whether libev has been found

set(LIBEV_SEARCH_HEADER_PATHS  
  ${THIRDPARTY_PREFIX}/include
)

set(LIBEV_SEARCH_LIB_PATH
  ${THIRDPARTY_PREFIX}/lib
)

find_path(LIBEV_INCLUDE_DIR ev++.h PATHS
    ${LIBEV_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(LIBEV_LIB_PATH NAMES ev PATHS ${LIBEV_SEARCH_LIB_PATH} NO_DEFAULT_PATH)

if (LIBEV_INCLUDE_DIR AND LIBEV_LIB_PATH)
  set(LIBEV_FOUND TRUE)
  set(LIBEV_LIBS ${LIBEV_SEARCH_LIB_PATH})
  set(LIBEV_LIB_NAME libev)
  set(LIBEV_STATIC_LIB ${LIBEV_SEARCH_LIB_PATH}/${LIBEV_LIB_NAME}.a)
  set(LIBEV_SHARED_LIB ${LIBEV_SEARCH_LIB_PATH}/${LIBEV_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
else ()
  set(LIBEV_FOUND FALSE)
endif ()

if (LIBEV_FOUND)
  if (NOT LibEv_FIND_QUIETLY)
    message(STATUS "Found the LibEv library: ${LIBEV_LIB_PATH}")
  endif ()
else ()
  if (NOT LibEv_FIND_QUIETLY)
    set(LIBEV_ERR_MSG "Could not find the LibEv library. Looked for headers")
    set(LIBEV_ERR_MSG "${LIBEV_ERR_MSG} in ${LIBEV_SEARCH_HEADER_PATHS}, and for libs")
    set(LIBEV_ERR_MSG "${LIBEV_ERR_MSG} in ${LIBEV_SEARCH_LIB_PATH}")
    if (LibEv_FIND_REQUIRED)
      message(FATAL_ERROR "${LIBEV_ERR_MSG}")
    else (LibEv_FIND_REQUIRED)
      message(STATUS "${LIBEV_ERR_MSG}")
    endif (LibEv_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  LIBEV_INCLUDE_DIR
  LIBEV_LIBS
  LIBEV_STATIC_LIB
  LIBEV_SHARED_LIB
)
