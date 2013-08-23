# Copyright (c) 2013, Cloudera, inc.
# - Find Mongoose (mongoose.h, libmongoose.a)
# This module defines
#  MONGOOSE_INCLUDE_DIR, directory containing headers
#  MONGOOSE_STATIC_LIB, path to libmongoose.a
#  MONGOOSE_FOUND, whether mongoose has been found

set(MONGOOSE_SEARCH_HEADER_PATHS
  ${THIRDPARTY_PREFIX}/include
)

set(MONGOOSE_SEARCH_LIB_PATH
  ${THIRDPARTY_PREFIX}/lib
)

find_path(MONGOOSE_INCLUDE_DIR mongoose.h
  PATHS ${MONGOOSE_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(MONGOOSE_LIB_PATH
  NAMES mongoose
  PATHS ${MONGOOSE_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH)

if (MONGOOSE_LIB_PATH AND MONGOOSE_INCLUDE_DIR)
  set(MONGOOSE_FOUND TRUE)
  set(MONGOOSE_LIBS ${MONGOOSE_SEARCH_LIB_PATH})
  set(MONGOOSE_STATIC_LIB ${MONGOOSE_SEARCH_LIB_PATH}/libmongoose.a)
else ()
  set(MONGOOSE_FOUND FALSE)
endif ()

if (MONGOOSE_FOUND)
  if (NOT Mongoose_FIND_QUIETLY)
    message(STATUS "Found the Mongoose library: ${MONGOOSE_LIB_PATH}")
  endif ()
else ()
  if (NOT Mongoose_FIND_QUIETLY)
    set(MONGOOSE_ERR_MSG "Could not find the Mongoose library. Looked for headers")
    set(MONGOOSE_ERR_MSG "${MONGOOSE_ERR_MSG} in ${MONGOOSE_SEARCH_HEADER_PATHS}, and for libs")
    set(MONGOOSE_ERR_MSG "${MONGOOSE_ERR_MSG} in ${MONGOOSE_SEARCH_LIB_PATH}")
    if (Mongoose_FIND_REQUIRED)
      message(FATAL_ERROR "${MONGOOSE_ERR_MSG}")
    else (Mongoose_FIND_REQUIRED)
      message(STATUS "${MONGOOSE_ERR_MSG}")
    endif (Mongoose_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  MONGOOSE_INCLUDE_DIR
  MONGOOSE_LIBS
  MONGOOSE_STATIC_LIB
)
