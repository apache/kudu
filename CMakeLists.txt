cmake_minimum_required(VERSION 2.8)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

############################################################
# Compiler flags
############################################################

set(CMAKE_CXX_FLAGS "-g -Wall")

INCLUDE_DIRECTORIES(src)

############################################################
# Dependencies
############################################################

## Boost
find_package(Boost COMPONENTS thread system REQUIRED)
include_directories( ${Boost_INCLUDE_DIR} )
set(LIBS ${LIBS} ${Boost_LIBRARIES})

## GLog
find_package(GLog REQUIRED)
include_directories(${GLOG_INCLUDE_DIR})
set(LIBS ${LIBS} ${GLOG_STATIC_LIB})

## GTest
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIR})
set(LIBS ${LIBS} ${GTEST_LIBRARIES})
add_library(gtest STATIC IMPORTED)
set_target_properties(gtest PROPERTIES IMPORTED_LOCATION "${GTEST_LIBRARY}")

## Protobuf
find_package( Protobuf REQUIRED )
include_directories(${PROTOBUF_INCLUDE_DIR})
set(LIBS ${LIBS} ${PROTOBUF_LIBRARY})

## Google PerfTools
find_package( GPerf REQUIRED )
set(LIBS ${LIBS} ${TCMALLOC_LIBRARY})

############################################################
# Linker setup
############################################################
SET(KUDU_LINK_LIBS ${LIBS})
SET(KUDU_TEST_LINK_LIBS ${KUDU_LINK_LIBS} gtest kudu_util gutil)

############################################################
# Subdirectories
############################################################

# Google util libraries borrowed from supersonic
add_subdirectory(src/gutil)
set(LIBS ${LIBS} ${GUTIL_LIBS})

add_subdirectory(src/port)
set(LIBS ${LIBS} ${KUDU_PORT_LIBS})

add_subdirectory(src/util)
set(LIBS ${LIBS} ${KUDU_UTIL_LIBS})

add_subdirectory(src/cfile)
set(LIBS ${LIBS} ${CFILE_LIBS})


