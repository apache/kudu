cmake_minimum_required(VERSION 2.8)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")
include(CMakeParseArguments)

############################################################
# Compiler flags
############################################################

# compiler flags that are common across debug/release builds
#  - msse4.2: Enable sse4.2 compiler intrinsics.
#  - Wall: Enable all warnings.
#  - Wno-sign-compare: suppress warnings for comparison between signed and unsigned
#    integers
#  -Wno-deprecated: some of the gutil code includes old things like ext/hash_set, ignore that
#  - pthread: enable multithreaded malloc
#  - -D__STDC_FORMAT_MACROS: for PRI* print format macros
set(CXX_COMMON_FLAGS "-msse4.2 -Wall -Wno-sign-compare -Wno-deprecated -pthread -D__STDC_FORMAT_MACROS")
# compiler flags for different build types (run 'cmake -DCMAKE_BUILD_TYPE=<type> .')
# For all builds:
# For CMAKE_BUILD_TYPE=Debug
#   -ggdb: Enable gdb debugging
# For CMAKE_BUILD_TYPE=FastDebug
#   Same as DEBUG, except with some optimizations on.
# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   -g: Enable symbols for profiler tools (TODO: remove for shipping)
#   -DNDEBUG: Turn off dchecks/asserts/debug only code.
#   -fno-omit-frame-pointer
#       use frame pointers to allow simple stack frame walking for backtraces.
#       This has a small perf hit but worth it for the ability to profile in production
# For profile guided optimization (PGO) builds, in addition to the flags for release builds:
#   1. Build first with CMAKE_BUILD_TYPE_PROFILE_GEN:
#     -fprofile-generate: Indicates compiler should insert profile guided optimization events
#   2. Run the benchmarks (generates *.gcda profiling data).
#   3. Build again with CMAKE_BUILD_TYPE_PROFILE_BUILD
#     -fprofile-use: Compiler will use the profile outputs for optimizations
set(CXX_FLAGS_DEBUG "-ggdb")
set(CXX_FLAGS_FASTDEBUG "-ggdb -O1 -fno-omit-frame-pointer")
set(CXX_FLAGS_RELEASE "-O3 -g -DNDEBUG -fno-omit-frame-pointer")

if (NOT "${KUDU_USE_LTO}" STREQUAL "")
  set(CXX_FLAGS_RELEASE "${CXX_FLAGS_RELEASE} flto -fno-use-linker-plugin")
endif()

set(CXX_FLAGS_PROFILE_GEN "${CXX_FLAGS_RELEASE} -fprofile-generate")
set(CXX_FLAGS_PROFILE_BUILD "${CXX_FLAGS_RELEASE} -fprofile-use")

# if no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif(NOT CMAKE_BUILD_TYPE)

string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)

# Set compile flags based on the build type.
message("Configured for ${CMAKE_BUILD_TYPE} build (set with cmake -DCMAKE_BUILD_TYPE={release,debug,...})")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS_DEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS_FASTDEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS_RELEASE})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "PROFILE_GEN")
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS_PROFILE_GEN})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "PROFILE_BUILD")
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS_PROFILE_BUILD})
else()
  message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif ()

# Add common flags
set(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")

if("${CMAKE_CXX_COMPILER}" MATCHES ".*clang.*")
  set(IS_CLANG 1)
  # Clang helpfully provides a few extensions from C++11 such as the 'override'
  # keyword on methods. This doesn't change behavior, and we selectively enable
  # it in src/gutil/port.h only on clang. So, we can safely use it, and don't want
  # to trigger warnings when we do so.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-c++11-extensions")
else()
  set(IS_CLANG 0)
endif()

# Flag to enable clang address sanitizer
# This will only build if clang is the chosen compiler
if (${KUDU_USE_ASAN})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -DADDRESS_SANITIZER")

  # Ensure static linking is on -- otherwise can run into llvm bug #56393:
  # http://www.mail-archive.com/gcc-bugs@gcc.gnu.org/msg386467.html
  set(KUDU_STATIC_LINK ON)
endif ()

# Flag to enable clang undefined behavior sanitizer
# We explicitly don't enable all of the sanitizer flags:
# - disable 'vptr' because it currently crashes somewhere in boost::intrusive::list code
# - disable 'alignment' because unaligned access is really OK on Nehalem and we do it
#   all over the place.
if (${KUDU_USE_UBSAN})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-sanitize=alignment,vptr -fno-sanitize-recover")
endif ()

if ("${KUDU_USE_UBSAN}" OR "${KUDU_USE_ASAN}")
  if (NOT "${IS_CLANG}")
    message(SEND_ERROR "Cannot use ASAN/UBSAN without clang")
  endif()
  # Don't omit frame pointers, so that our failures have better info included.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fsanitize-blacklist=${CMAKE_CURRENT_SOURCE_DIR}/build-support/sanitize-blacklist.txt")

  # Ensure static linking is on -- otherwise can run into llvm bug #56393:
  # http://www.mail-archive.com/gcc-bugs@gcc.gnu.org/msg386467.html
  set(KUDU_STATIC_LINK ON)
endif()

# Code coverage
if ("${KUDU_GENERATE_COVERAGE}")
  if("${CMAKE_CXX_COMPILER}" MATCHES ".*clang.*")
    # There appears to be some bugs in clang 3.3 which cause code coverage
    # to have link errors, not locating the llvm_gcda_* symbols.
    # This should be fixed in llvm 3.4 with http://llvm.org/viewvc/llvm-project?view=revision&revision=184666
    message(SEND_ERROR "Cannot currently generate coverage with clang")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage")
endif()

# set compile output directory
string (TOLOWER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_LOWERCASE)
set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/${CMAKE_BUILD_TYPE_LOWERCASE}/")

# Link build/latest to the current build directory, to avoid developers
# accidentally running the latest debug build when in fact they're building
# release builds.
FILE(MAKE_DIRECTORY ${BUILD_OUTPUT_ROOT_DIRECTORY})
EXECUTE_PROCESS(COMMAND ln -T -sf ${BUILD_OUTPUT_ROOT_DIRECTORY}
  ${CMAKE_CURRENT_SOURCE_DIR}/build/latest)

# where to put generated libraries
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")


include_directories(src)

############################################################
# Testing
############################################################

# Set a 15-minute timeout for tests run via 'make test'.
# This keeps our jenkins builds from hanging in the case that there's
# a deadlock or anything.
set(KUDU_TEST_TIMEOUT 900)

# Add a new test case. Additional arguments after the
# name of the test will be passed to SET_TESTS_PROPERTIES
function(ADD_KUDU_TEST REL_TEST_BINARY)
  set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${REL_TEST_BINARY}")
  get_filename_component(SRC_DIR ${TEST_PATH} PATH)
  get_filename_component(TEST_BINARY ${TEST_PATH} NAME)
  add_executable(${TEST_BINARY} "${REL_TEST_BINARY}.cc")
  add_test(${TEST_BINARY} ${CMAKE_SOURCE_DIR}/build-support/run-test.sh
    "${BUILD_OUTPUT_ROOT_DIRECTORY}/${TEST_BINARY}")
  set_tests_properties(${TEST_BINARY} PROPERTIES
    TIMEOUT ${KUDU_TEST_TIMEOUT}
    ${ARGN})
  target_link_libraries(${TEST_BINARY} ${KUDU_TEST_LINK_LIBS})
endfunction()

enable_testing()

############################################################
# Dependencies
############################################################
function(ADD_THIRDPARTY_LIB LIB_NAME)
  set(options)
  set(one_value_args SHARED_LIB STATIC_LIB)
  set(multi_value_args DEPS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if((KUDU_STATIC_LINK AND ARG_STATIC_LIB) OR (NOT ARG_SHARED_LIB))
    if(NOT ARG_STATIC_LIB)
      message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
    endif()
    add_library(${LIB_NAME} STATIC IMPORTED)
    set_target_properties(${LIB_NAME} PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    message("Added static library dependency ${LIB_NAME}: ${ARG_STATIC_LIB}")
  else()
    add_library(${LIB_NAME} SHARED IMPORTED)
    set_target_properties(${LIB_NAME} PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    message("Added shared library dependency ${LIB_NAME}: ${ARG_SHARED_LIB}")
  endif()

  if(ARG_DEPS)
    set_target_properties(${LIB_NAME} PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${ARG_DEPS}")
  endif()
endfunction()

# Look in thirdparty/installed before anywhere else for system dependencies.
set(THIRDPARTY_PREFIX ${CMAKE_SOURCE_DIR}/thirdparty/installed)
set(CMAKE_PREFIX_PATH ${THIRDPARTY_PREFIX})
set(LIBS ${LIBS} -lrt)

## Boost
### Workaround for http://stackoverflow.com/questions/9948375/cmake-find-package-succeeds-but-returns-wrong-path
set(Boost_NO_BOOST_CMAKE ON)

if ("${KUDU_STATIC_LINK}")
  set(Boost_USE_STATIC_LIBS ON)
endif()

find_package(Boost COMPONENTS thread system REQUIRED)
include_directories(SYSTEM ${Boost_INCLUDE_DIR} )
set(LIBS ${LIBS} ${Boost_LIBRARIES})

## Cyrus SASL
find_package(CyrusSASL REQUIRED)
include_directories(SYSTEM ${CYRUS_SASL_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(cyrus_sasl
  SHARED_LIB "${CYRUS_SASL_SHARED_LIB}"
  DEPS "${CYRUS_SASL_LIB_DEPS}")

## GLog
find_package(GLog REQUIRED)
include_directories(SYSTEM ${GLOG_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(glog
  STATIC_LIB "${GLOG_STATIC_LIB}"
  SHARED_LIB "${GLOG_LIBS}/libglog.so")
set(LIBS ${LIBS} glog)

## GFlags
find_package(GFlags REQUIRED)
include_directories(SYSTEM ${GFLAGS_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(gflags
  STATIC_LIB "${GFLAGS_STATIC_LIB}"
  SHARED_LIB "${GFLAGS_LIBS}/libgflags.so")
set(LIBS ${LIBS} gflags)

## GTest
find_package(GTest REQUIRED)
include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(gtest STATIC_LIB "${GTEST_LIBRARY}")

## Protobuf
set(PROTO_SRC_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(PROTO_DST_ROOT ${CMAKE_CURRENT_BINARY_DIR}/src)
find_package( Protobuf REQUIRED )
include_directories(SYSTEM ${PROTOBUF_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(protobuf
  STATIC_LIB "${PROTOBUF_STATIC_LIBRARY}"
  SHARED_LIB "${PROTOBUF_LIBRARY}")
ADD_THIRDPARTY_LIB(protoc
  STATIC_LIB "${PROTOBUF_PROTOC_STATIC_LIBRARY}"
  SHARED_LIB "${PROTOBUF_PROTOC_LIBRARY}"
  DEPS protobuf)

find_package(KRPC REQUIRED)

## Snappy
find_package(Snappy REQUIRED)
include_directories(SYSTEM ${SNAPPY_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(snappy
  STATIC_LIB "${SNAPPY_STATIC_LIB}"
  SHARED_LIB "${SNAPPY_LIBS}/libsnappy.so")

## Libev
find_package(LibEv REQUIRED)
include_directories(SYSTEM ${LIBEV_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(libev
  STATIC_LIB "${LIBEV_STATIC_LIB}"
  SHARED_LIB "${LIBEV_SHARED_LIB}")

## LZ4
find_package(Lz4 REQUIRED)
include_directories(SYSTEM ${LZ4_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(lz4 STATIC_LIB "${LZ4_STATIC_LIB}")

## ZLib
find_package(Zlib REQUIRED)
include_directories(SYSTEM ${ZLIB_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(zlib
  STATIC_LIB "${ZLIB_STATIC_LIB}"
  SHARED_LIB "${ZLIB_LIBS}/libz.so")

## Squeasel
find_package(Squeasel REQUIRED)
include_directories(SYSTEM ${SQUEASEL_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(squeasel
  STATIC_LIB "${SQUEASEL_STATIC_LIB}"
  DEPS "${SQUEASEL_STATIC_LIB_DEPS}")

## Google PerfTools
find_package( GPerf REQUIRED )
if (NOT "${KUDU_USE_ASAN}")
  set(LIBS ${LIBS} ${TCMALLOC_LIBRARY} ${PROFILER_LIBRARY})
endif()

## curl
find_package(CURL REQUIRED)

############################################################
# Linker setup
############################################################
set(KUDU_DEP_LIBS ${LIBS})
set(KUDU_LINK_LIBS client master master_proto tserver tserver_proto tablet server_common server_process
    krpc cfile kudu_common kudu_util gutil ${LIBS})
set(KUDU_MIN_TEST_LIBS kudu_test_main)
set(KUDU_TEST_LINK_LIBS ${KUDU_MIN_TEST_LIBS})

############################################################
# "make ctags" target
############################################################
if (UNIX)
  add_custom_target(ctags ctags -R --languages=c++,c --exclude=thirdparty/installed)
endif (UNIX)

############################################################
# "make etags" target
############################################################
if (UNIX)
  add_custom_target(tags etags --members --declarations
  `find ${CMAKE_CURRENT_SOURCE_DIR}/src -name
*.cc -or -name *.hh -or -name *.cpp -or -name *.h -or -name *.c -or
-name *.f`)
  add_custom_target(etags DEPENDS tags)
endif (UNIX)

############################################################
# "make cscope" target
############################################################
if (UNIX)
  add_custom_target(cscope find ${CMAKE_CURRENT_SOURCE_DIR}/src
-name *.cc -or -name *.hh -or -name *.cpp -or -name *.h -or
-name *.c -or -name *.f > cscope.files && cscope -q -b)
endif (UNIX)

############################################################
# "make lint" target
############################################################
if (UNIX)
  # Full lint
  add_custom_target(lint ${CMAKE_CURRENT_SOURCE_DIR}/build-support/lint.sh)
  # Incremental lint - only checks files changed since the last
  # merged upstream commit
  add_custom_target(ilint ${CMAKE_CURRENT_SOURCE_DIR}/build-support/lint.sh -c)
endif (UNIX)

############################################################
# Subdirectories
############################################################

# Google util libraries borrowed from supersonic
add_subdirectory(src/gutil)
add_subdirectory(src/util)
add_subdirectory(src/common)
add_subdirectory(src/cfile)
add_subdirectory(src/server)
add_subdirectory(src/tablet)
add_subdirectory(src/rpc)
add_subdirectory(src/tserver)
add_subdirectory(src/consensus)
add_subdirectory(src/master)
add_subdirectory(src/client)
add_subdirectory(src/integration-tests)
add_subdirectory(src/experiments)
add_subdirectory(src/benchmarks)
add_subdirectory(src/twitter-demo)
add_subdirectory(src/tools)
