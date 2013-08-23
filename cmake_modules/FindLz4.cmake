# - Find LZ4 (lz4.h, liblz4.a, liblz4.so, and liblz4.so.1)
# This module defines
#  LZ4_INCLUDE_DIR, directory containing headers
#  LZ4_LIBS, directory containing lz4 libraries
#  LZ4_STATIC_LIB, path to liblz4.a
#  LZ4_FOUND, whether lz4 has been found

set(LZ4_SEARCH_HEADER_PATHS
  ${THIRDPARTY_PREFIX}/include
)

set(LZ4_SEARCH_LIB_PATH
  ${THIRDPARTY_PREFIX}/lib
)

find_path(LZ4_INCLUDE_DIR lz4.h PATHS
  ${LZ4_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(LZ4_LIB_PATH NAMES liblz4.a PATHS ${LZ4_SEARCH_LIB_PATH} NO_DEFAULT_PATH)

if (LZ4_INCLUDE_DIR AND LZ4_LIB_PATH)
  set(LZ4_FOUND TRUE)
  set(LZ4_LIBS ${LZ4_SEARCH_LIB_PATH})
  set(LZ4_STATIC_LIB ${LZ4_SEARCH_LIB_PATH}/liblz4.a)
else ()
  set(LZ4_FOUND FALSE)
endif ()

if (LZ4_FOUND)
  if (NOT Lz4_FIND_QUIETLY)
    message(STATUS "Found the Lz4 library: ${LZ4_LIB_PATH}")
  endif ()
else ()
  if (NOT Lz4_FIND_QUIETLY)
    set(LZ4_ERR_MSG "Could not find the Lz4 library. Looked for headers")
    set(LZ4_ERR_MSG "${LZ4_ERR_MSG} in ${LZ4_SEARCH_HEADER_PATHS}, and for libs")
    set(LZ4_ERR_MSG "${LZ4_ERR_MSG} in ${LZ4_SEARCH_LIB_PATH}")
    if (Lz4_FIND_REQUIRED)
      message(FATAL_ERROR "${LZ4_ERR_MSG}")
    else (Lz4_FIND_REQUIRED)
      message(STATUS "${LZ4_ERR_MSG}")
    endif (Lz4_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  LZ4_INCLUDE_DIR
  LZ4_LIBS
  LZ4_STATIC_LIB
)
