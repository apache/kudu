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
  if (NOT MONGOOSE_FIND_QUIETLY)
    message(STATUS "mongoose Found in ${MONGOOSE_SEARCH_LIB_PATH}")
  endif ()
else ()
  message(STATUS "mongoose includes and libraries NOT found. "
    "Looked for headers in ${MONGOOSE_SEARCH_HEADER_PATHS}, "
    "and for libs in ${MONGOOSE_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  MONGOOSE_INCLUDE_DIR
  MONGOOSE_LIBS
  MONGOOSE_STATIC_LIB
)
