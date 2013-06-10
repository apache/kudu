# - Find LIBEV (ev++.h, libev.a, and libev.so)
# This module defines
#  LIBEV_INCLUDE_DIR, directory containing headers
#  LIBEV_LIBS, directory containing libev libraries
#  LIBEV_STATIC_LIB, path to libev.a
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

find_library(LIBEV_LIB_PATH NAMES ev PATHS ${LIBEV_SEARCH_LIB_PATH})

if (LIBEV_LIB_PATH)
  set(LIBEV_FOUND TRUE)
  set(LIBEV_LIBS ${LIBEV_SEARCH_LIB_PATH})
  set(LIBEV_STATIC_LIB ${LIBEV_SEARCH_LIB_PATH}/libev.a)
else ()
  set(LIBEV_FOUND FALSE)
endif ()

if (LIBEV_FOUND)
  if (NOT LIBEV_FIND_QUIETLY)
    message(STATUS "Libev Found in ${LIBEV_SEARCH_LIB_PATH}")
  endif ()
else ()
  message(STATUS "Libev includes and libraries NOT found. "
    "Looked for headers in ${LIBEV_SEARCH_HEADER_PATH}, "
    "and for libs in ${LIBEV_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  LIBEV_INCLUDE_DIR
  LIBEV_LIBS
  LIBEV_STATIC_LIB
)
