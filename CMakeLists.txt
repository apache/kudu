# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Require cmake that can work with OpenJDK 10 and newer [1].
#
# Note: cmake in thirdparty/ will always meet this minimum.
#
# 1. https://gitlab.kitware.com/cmake/cmake/issues/17938
cmake_minimum_required(VERSION 3.11.2)

project(Kudu)

# Prevent builds from the top-level source directory. This ensures that build
# output is well isolated from the source tree.
#
# May be overridden by setting KUDU_ALLOW_IN_SOURCE_BUILD; this is only
# recommended for experts!
if("${CMAKE_CURRENT_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_BINARY_DIR}" AND
    NOT "${KUDU_ALLOW_IN_SOURCE_BUILD}")
  message(FATAL_ERROR
    "Kudu may not be built from the top-level source directory. Create a new "
    "directory and run cmake from there, passing the path to the top-level "
    "source directory as the last argument. "
    "To override this, rerun CMake with -DKUDU_ALLOW_IN_SOURCE_BUILD=1. "
    "Also, delete 'CMakeCache.txt' and 'CMakeFiles' from the top-level source "
    "directory, otherwise future builds will not work.")
endif()

# Provide a 'latest' symlink to this build directory if the "blessed"
# multi-build layout is detected:
#
# build/
# build/<first build directory>
# build/<second build directory>
# ...
if ("${CMAKE_CURRENT_BINARY_DIR}" STREQUAL
    "${CMAKE_CURRENT_SOURCE_DIR}/build/latest")
  message(FATAL_ERROR "Should not run cmake inside the build/latest symlink. "
    "First change directories into the destination of the symlink.")
endif()
string(REPLACE "${CMAKE_CURRENT_SOURCE_DIR}/build/" ""
  BLESSED_BUILD_SUBDIR "${CMAKE_CURRENT_BINARY_DIR}")
string(FIND "${BLESSED_BUILD_SUBDIR}" "/" SLASH_POS)
if (SLASH_POS EQUAL -1)
  # Create the symlink both during cmake invocation and when the default target
  # ('all') is built. The former is useful for scripts that, after running
  # cmake, only build a single target (i.e. "make lint").
  execute_process(COMMAND ln ${MORE_ARGS} -nsf ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/build/latest)
  add_custom_target(latest_symlink ALL
    ln ${MORE_ARGS} -nsf ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/build/latest
    COMMENT "Recreating ../build/latest symlink")

  # 'ALL' above doesn't actually add 'latest_symlink' as a dependency to all
  # targets. So, we override add_executable to ensure that whenever any executable
  # is built, the symlink is re-created.
  function(add_executable name)
    # Call through to the original add_executable function.
    _add_executable(${name} ${ARGN})
    add_dependencies(${name} latest_symlink)
  endfunction()
endif()

file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/version.txt" KUDU_VERSION_NUMBER)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")
include(CMakeParseArguments)

set(BUILD_SUPPORT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/build-support)
set(THIRDPARTY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty)
set(THIRDPARTY_TOOLCHAIN_DIR ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/clang-toolchain)
set(THIRDPARTY_INSTALL_DIR ${THIRDPARTY_DIR}/installed)
set(THIRDPARTY_INSTALL_COMMON_DIR ${THIRDPARTY_INSTALL_DIR}/common)
set(THIRDPARTY_INSTALL_UNINSTRUMENTED_DIR ${THIRDPARTY_INSTALL_DIR}/uninstrumented)
set(THIRDPARTY_INSTALL_TSAN_DIR ${THIRDPARTY_INSTALL_DIR}/tsan)

# Set the Java directory so our C++ tests are able to access its contents.
# This can be useful to, e.g. spawn a Java subprocess.
set(JAVA_DIR ${CMAKE_CURRENT_SOURCE_DIR}/java)

# Adds the EXTRA_GRADLE_FLAGS environment variable to a cmake list and calls `separate_arguments`.
# This solves the problem of escaped spaces in the EXTRA_GRADLE_FLAGS environment variable.
list(APPEND GRADLE_FLAGS $ENV{EXTRA_GRADLE_FLAGS})
separate_arguments(GRADLE_FLAGS)
# We always want Gradle to use the plain console and quiet flag when called from cmake.
list(APPEND GRADLE_FLAGS --quiet --console=plain)
list(REMOVE_DUPLICATES GRADLE_FLAGS)

# Initialize Gradle to ensure the wrapper is downloaded.
# This is important so that multiple calls to Gradle don't try and
# download the gradle-wrapper.jar at the same time.
set(GRADLE_WRAPPER_JAR ${JAVA_DIR}/gradle/wrapper/gradle-wrapper.jar)
add_custom_command(OUTPUT ${GRADLE_WRAPPER_JAR}
  COMMAND ./gradlew --version ${GRADLE_FLAGS}
  COMMAND ./gradlew initializeTasks ${GRADLE_FLAGS}
  WORKING_DIRECTORY "${JAVA_DIR}")
add_custom_target(init_gradle DEPENDS ${GRADLE_WRAPPER_JAR})

# Allow "make install" to not depend on all targets.
#
# Must be declared in the top-level CMakeLists.txt.
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

# Codegen-dependent executables need to be linked with -rdynamic; otherwise LLVM
# can't find dependent Kudu symbols at runtime.
#
# Setting the ENABLE_EXPORTS target property for each codegen-dependent
# executable is more precise, but that's a rather long target list, so we'll
# just do it once here for all Kudu executables.
set(CMAKE_ENABLE_EXPORTS true)

# Always generate the compilation database file (compile_commands.json) for use
# with various development tools, such as IWYU and Vim's YouCompleteMe plugin.
# See http://clang.llvm.org/docs/JSONCompilationDatabase.html
set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)

# Make sure thirdparty stuff is up-to-date.
if ("$ENV{NO_REBUILD_THIRDPARTY}" STREQUAL "")
  if (${KUDU_USE_TSAN})
    set(TP_ARGS "tsan")
  endif()
  execute_process(
    COMMAND ${THIRDPARTY_DIR}/build-if-necessary.sh ${TP_ARGS}
    RESULT_VARIABLE THIRDPARTY_SCRIPT_RESULT)
  if (NOT (${THIRDPARTY_SCRIPT_RESULT} EQUAL 0))
    message(FATAL_ERROR "Thirdparty was built unsuccessfully, terminating.")
  endif()
endif()

############################################################
# Compiler flags
############################################################

# Determine compiler version
include(CompilerInfo)


# compiler flags that are common across debug/release builds
execute_process(COMMAND uname -m OUTPUT_VARIABLE ARCH_NAME)
if("${ARCH_NAME}" MATCHES "aarch64")
  # Certain platforms such as ARM do not use signed chars by default
  # which causes issues with certain bounds checks.
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fsigned-char")
  # Turn off fp-contract on aarch64 to avoid multiply-add operation result difference.
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -ffp-contract=off")
else()
  # -msse4.2: Enable sse4.2 compiler intrinsics.
  set(CXX_COMMON_FLAGS "-msse4.2")
endif()
#  -Wall: Enable all warnings.
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall")
#  -Wno-sign-compare: suppress warnings for comparison between signed and unsigned
#     integers
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-sign-compare")
#  -Wno-comment: suppress warnings about backslash-newline in "//" comments,
#     which appear in the Kudu source code due to the use of ASCII art and
#     multi-line command line examples in comments.
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-comment")
#  -pthread: enable multithreaded malloc
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -pthread")
#  -fno-strict-aliasing
#     Assume programs do not follow strict aliasing rules.
#     GCC cannot always verify whether strict aliasing rules are indeed followed due to
#     fundamental limitations in escape analysis, which can result in subtle bad code generation.
#     This has a small perf hit but worth it to avoid hard to debug crashes.
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fno-strict-aliasing")
#  -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG: enable nanosecond precision for boost
set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG")

# We want access to the PRI* print format macros.
add_definitions(-D__STDC_FORMAT_MACROS)

# We want short macros from util/status.h.
add_definitions(-DKUDU_HEADERS_USE_SHORT_STATUS_MACROS=1)

# Slice includes many gutil dependencies that third-party users of the Kudu
# client library don't have. Our build has them, though.
add_definitions(-DKUDU_HEADERS_USE_RICH_SLICE=1)

# We don't want to use any stubs; that's exclusively for builds using our
# exported client headers).
add_definitions(-DKUDU_HEADERS_NO_STUBS=1)

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

set(CXX_FLAGS_PROFILE_GEN "${CXX_FLAGS_RELEASE} -fprofile-generate")
set(CXX_FLAGS_PROFILE_BUILD "${CXX_FLAGS_RELEASE} -fprofile-use")

# if no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif(NOT CMAKE_BUILD_TYPE)

string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)

# Alias RELEASE as RELWITHDEBINFO and MINSIZEREL. These are common CMake
# release type names and this provides compatibility with the CLion IDE.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "RELWITHDEBINFO" OR "${CMAKE_BUILD_TYPE}" STREQUAL "MINSIZEREL")
  set(CMAKE_BUILD_TYPE RELEASE)
endif ()

# Append to compile flags based on the build type. It's important not to clobber
# CMAKE_CXX_FLAGS because it may contain flags specified by the user.
message("Configured for ${CMAKE_BUILD_TYPE} build (set with cmake -DCMAKE_BUILD_TYPE={release,debug,...})")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_DEBUG}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_FASTDEBUG}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_RELEASE}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "PROFILE_GEN")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_PROFILE_GEN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "PROFILE_BUILD")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_PROFILE_BUILD}")
else()
  message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif ()

# Add common flags
set(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  # Ensure that we don't use old versions of clang. There isn't anything
  # particularly wrong with those, but no reason to support going back
  # this far.
  if ("${COMPILER_VERSION}" VERSION_LESS "6.0")
    message(FATAL_ERROR "Clang version ${COMPILER_VERSION} too old. "
      "Consider using the version of clang in ${THIRDPARTY_TOOLCHAIN_DIR}:\n"
      ""
      "  CC=${BUILD_SUPPORT_DIR}/ccache-clang/clang CXX=$CC++ cmake <args>")
  endif()

  # Using Clang with ccache causes a bunch of spurious warnings that are
  # purportedly fixed in the next version of ccache. See the following for details:
  #
  #   http://petereisentraut.blogspot.com/2011/05/ccache-and-clang.html
  #   http://petereisentraut.blogspot.com/2011/09/ccache-and-clang-part-2.html
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments")

  # Clang generates ambiguous member template warnings when calling the ev++ api.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-ambiguous-member-template")

  # Emit a warning when the method/function marked as deprecated
  # in its in-line documentation but lacks the deprecated attribute
  # ATTRIBUTE_DEPRECATED in its signature.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wdocumentation-deprecated-sync")

  # Avoid 'taking address of packed member' warnings, which pollute macOs/clang 4.0 builds.
  # This is also done in chromium. See:
  # https://bugs.chromium.org/p/chromium/issues/detail?id=619640
  # Unfortunately older versions of clang complain about not knowing the warning, so
  # this also disables the warning about unknown warnings.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unknown-warning-option -Wno-address-of-packed-member")

  # Only hardcode -fcolor-diagnostics if stderr is opened on a terminal. Otherwise
  # the color codes show up as noisy artifacts.
  #
  # This test is imperfect because 'cmake' and 'make' can be run independently
  # (with different terminal options), and we're testing during the former.
  #
  # We also provide a manual override as -DKUDU_FORCE_COLOR_DIAGNOSTICS=1 or
  # by setting an environment variable of the same name.
  execute_process(COMMAND test -t 2 RESULT_VARIABLE KUDU_IS_TTY)
  if ((NOT "${KUDU_FORCE_COLOR_DIAGNOSTICS}" STREQUAL "") OR
      (NOT "$ENV{KUDU_FORCE_COLOR_DIAGNOSTICS}" STREQUAL "") OR
      ((${KUDU_IS_TTY} EQUAL 0) AND (NOT ("$ENV{TERM}" STREQUAL "dumb"))))
    message("Running in a controlling terminal")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcolor-diagnostics")
  else()
    message("Running without a controlling terminal or in a dumb terminal")
  endif()
elseif("${COMPILER_FAMILY}" STREQUAL "gcc")
  # Disallow GCC < 4.8, since it doesn't support the C++11 standard
  # well enough for us.
  if ("${COMPILER_VERSION}" VERSION_LESS "4.8")
    message(FATAL_ERROR "GCC <4.8 not supported. Consider using "
        "thirdparty/clang-toolchain/ to build on older hosts.")
  endif()

  # GCC 4.8's tree vectorizer has a bug which causes hard-to-debug incorrect
  # code. Disable this option on release builds, where it would otherwise
  # be enabled by -O3.
  if ("${COMPILER_VERSION}" MATCHES "^4.8" AND
      "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    message("Running release build on gcc 4.8. The tree-vectorize optimization "
      "in this version of gcc is known to be buggy. Disabling it.")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-tree-vectorize")
  endif()
endif()

# Sanity check linking option.
if (NOT KUDU_LINK)
  set(KUDU_LINK "a")
elseif(NOT ("auto" MATCHES "^${KUDU_LINK}" OR
            "dynamic" MATCHES "^${KUDU_LINK}" OR
            "static" MATCHES "^${KUDU_LINK}"))
  message(FATAL_ERROR "Unknown value for KUDU_LINK, must be auto|dynamic|static")
else()
  # Remove all but the first letter.
  string(SUBSTRING "${KUDU_LINK}" 0 1 KUDU_LINK)
endif()

# If not set, any file that includes kudu_export.h (an autogenerated file) will
# use visibility("hidden") with symbols annotated with KUDU_NO_EXPORT, even when
# compiled with default visibility flags. It is overridden as needed by
# ADD_EXPORTABLE_LIBRARY() when actually compiling exported library variants.
add_definitions("-DKUDU_STATIC_DEFINE")

# Clang does not support using ASAN and TSAN simultaneously.
if ("${KUDU_USE_ASAN}" AND "${KUDU_USE_TSAN}")
  message(SEND_ERROR "Can only enable one of ASAN or TSAN at a time")
endif()

# Flag to enable clang address sanitizer (using it along with leak sanitizer).
# This will only build if clang or a recent enough gcc is the chosen compiler.
if (${KUDU_USE_ASAN})
  if(NOT (("${COMPILER_FAMILY}" STREQUAL "clang") OR
          ("${COMPILER_FAMILY}" STREQUAL "gcc" AND "${COMPILER_VERSION}" VERSION_GREATER "4.8")))
    message(SEND_ERROR "Cannot use ASAN without clang or gcc >= 4.8")
  endif()

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -DADDRESS_SANITIZER")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLEAK_SANITIZER")
endif()

if (${KUDU_USE_XRAY})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fxray-instrument")
endif()

# For any C code, use the same flags.
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# Enable the Clang undefined behavior (UB) sanitizer.
if (${KUDU_USE_UBSAN})
  if(NOT (("${COMPILER_FAMILY}" STREQUAL "clang") OR
          ("${COMPILER_FAMILY}" STREQUAL "gcc" AND "${COMPILER_VERSION}" VERSION_GREATER "4.9")))
    message(SEND_ERROR "Cannot use UBSAN without clang or gcc >= 4.9")
  endif()
  # Enable UB and unsigned integer overflow detection.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined,integer")
  # Disable 'alignment' because unaligned access is really OK on Nehalem and we do it all over the place.
  # TODO(todd): enable implicit-integer-sign-change checking after fixing issues.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-sanitize=alignment,implicit-integer-sign-change,implicit-unsigned-integer-truncation,implicit-signed-integer-truncation")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize-blacklist=${CMAKE_CURRENT_SOURCE_DIR}/build-support/ubsan-blacklist.txt")
  # Stop execution after UB or overflow is detected.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-sanitize-recover=undefined,integer")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUNDEFINED_SANITIZER")
endif ()

# Flag to enable thread sanitizer (clang or gcc 4.8)
if (${KUDU_USE_TSAN})
  if(NOT (("${COMPILER_FAMILY}" STREQUAL "clang") OR
          ("${COMPILER_FAMILY}" STREQUAL "gcc" AND "${COMPILER_VERSION}" VERSION_GREATER "4.8")))
    message(SEND_ERROR "Cannot use TSAN without clang or gcc >= 4.8")
  endif()

  add_definitions("-fsanitize=thread")

  # Enables dynamic_annotations.h to actually generate code
  add_definitions("-DDYNAMIC_ANNOTATIONS_ENABLED")

  # changes atomicops to use the tsan implementations
  add_definitions("-DTHREAD_SANITIZER")

  # Disables using the precompiled template specializations for std::string, shared_ptr, etc
  # so that the annotations in the header actually take effect.
  add_definitions("-D_GLIBCXX_EXTERN_TEMPLATE=0")

  # Compile and link against the thirdparty TSAN instrumented libstdcxx.
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath,${THIRDPARTY_INSTALL_TSAN_DIR}/lib")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -nostdinc++")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -L${THIRDPARTY_INSTALL_TSAN_DIR}/lib")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -isystem ${THIRDPARTY_INSTALL_TSAN_DIR}/include/c++/v1")

  # Strictly speaking, TSAN doesn't require dynamic linking. But it does
  # require all code to be position independent, and the easiest way to
  # guarantee that is via dynamic linking (not all 3rd party archives are
  # compiled with -fPIC e.g. boost).
  if(NOT "${ARCH_NAME}" MATCHES "aarch64")
    if("${KUDU_LINK}" STREQUAL "a")
      message("Using dynamic linking for TSAN")
      set(KUDU_LINK "d")
    elseif("${KUDU_LINK}" STREQUAL "s")
      message(SEND_ERROR "Cannot use TSAN with static linking")
    endif()
  else()
    # workaround for github.com/google/sanitizers/issues/1208
    # TSAN with dynamic linking cause all of test cases failed on aarch64,
    # we don't apply ENABLE_DIST_TEST on aarch64, so apply static linking direcly
    message("Using static linking for TSAN on aarch64")
    set(KUDU_LINK "s")
  endif()
endif()

include("KuduLinker")
APPEND_LINKER_FLAGS()

if ("${KUDU_USE_ASAN}" OR "${KUDU_USE_TSAN}" OR "${KUDU_USE_UBSAN}")
  # GCC 4.8 and 4.9 (latest as of this writing) don't allow you to specify a
  # sanitizer blacklist.
  if("${COMPILER_FAMILY}" STREQUAL "clang")
    add_definitions("-fsanitize-blacklist=${BUILD_SUPPORT_DIR}/sanitize-blacklist.txt")
  else()
    message(WARNING "GCC does not support specifying a sanitizer blacklist. Known sanitizer check failures will not be suppressed.")
  endif()
endif()

if (KUDU_USE_LTO)
  if(NOT "${CMAKE_EXE_LINKER_FLAGS}" MATCHES "-fuse-ld=lld" OR
     NOT "${COMPILER_FAMILY}" STREQUAL "clang")
    message(FATAL_ERROR "must use clang and lld for LTO build")
  endif()
  if (NOT "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    # Doesn't make much sense to do LTO on a non-release build -- it's slow
    # and the only reason you'd want to do it is high performance.
    message(FATAL_ERROR "LTO only supported for release builds")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto=thin")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--thinlto-cache-dir=${CMAKE_CURRENT_BINARY_DIR}/lto.cache")
  set(CMAKE_AR "${THIRDPARTY_TOOLCHAIN_DIR}/bin/llvm-ar")
  set(CMAKE_RANLIB "${THIRDPARTY_TOOLCHAIN_DIR}/bin/llvm-ranlib")
endif()


# Code coverage
if ("${KUDU_GENERATE_COVERAGE}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -DCOVERAGE_BUILD")

  if(NOT "${COMPILER_FAMILY}" STREQUAL "clang")
    # Use clang for coverage builds so that we can standardize on a single
    # compiler. clang also handles flushing coverage from shared libraries
    # better than gcc.
    message(SEND_ERROR "Must use clang for coverage build")
  endif()
endif()

# If we still don't know what kind of linking to perform, choose based on
# build type (developers like fast builds).
if ("${KUDU_LINK}" STREQUAL "a")
  if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR
      "${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
    message("Using dynamic linking for ${CMAKE_BUILD_TYPE} builds")
    set(KUDU_LINK "d")
  else()
    message("Using static linking for ${CMAKE_BUILD_TYPE} builds")
    set(KUDU_LINK "s")
  endif()
endif()

# Having set KUDU_LINK due to build type and/or sanitizer, it's now safe to
# act on its value.
if ("${KUDU_LINK}" STREQUAL "d")
  set(BUILD_SHARED_LIBS ON)

  # Position independent code is only necessary when producing shared objects.
  add_definitions(-fPIC)
endif()

# where to put generated archives (.a files)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib")
file(MAKE_DIRECTORY "${CMAKE_ARCHIVE_OUTPUT_DIRECTORY}")

# where to put generated libraries (.so files)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib")
file(MAKE_DIRECTORY "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_CURRENT_BINARY_DIR}/bin")
file(MAKE_DIRECTORY "${EXECUTABLE_OUTPUT_PATH}")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/src)
include_directories(src)
include_directories(SYSTEM ${THIRDPARTY_INSTALL_COMMON_DIR}/include)

############################################################
# Visibility
############################################################
# For generate_export_header() and add_compiler_export_flags().
include(GenerateExportHeader)

# Honor visibility properties for all target types. See
# "cmake --help-policy CMP0063" for details.
#
# This policy was only added to cmake in version 3.3, so until the cmake in
# thirdparty is updated, we must check if the policy exists before setting it.
if(POLICY CMP0063)
  cmake_policy(SET CMP0063 NEW)
endif()

# add_library() wrapper that adds a second variant of the library for use in the
# exported Kudu C++ client. This variant is suffixed with "_exported" and is
# compiled with special visibility flags to hide all symbols except those that
# are part of the public ABI.
#
# There are two different kinds of exported libraries: internal and leaf.
# Internal libraries are static archives while leaf libraries are shared
# objects built from internal libraries. In practice there is only one leaf
# library: the Kudu C++ client itself.
#
# Arguments:
#
# LIB_NAME is the name of the library. It must come first. Required.
#
# SRCS is the list of source files to compile into the library. Required.
#
# DEPS is the list of targets that both library variants depend on. Required.
#
# NONLINK_DEPS is the list of (non-linked) targets that both library variants
# depend on. Optional.
#
# COMPILE_FLAGS is a string containing any additional compilation flags that
# should be added to both library variants. Optional.
#
# EXPORTED_SHARED is a toggle that, if set, indicates that the exported variant
# is a "leaf" library. Otherwise it is an "internal" library. Optional.
#
# EXPORTED_OUTPUT_NAME is a string describing a different file name for the
# exported library variant. If not set, defaults to LIB_NAME. Optional.
#
# EXPORTED_OUTPUT_DIRECTORY is a string describing a different directory where
# the exported library variant should be written. If not set, defaults to the
# directory where this function was called. Optional.
#
# EXPORTED_DEPS is a list of targets that the exported library variant depends
# on. If not set, defaults to DEPS. Optional.
function(ADD_EXPORTABLE_LIBRARY LIB_NAME)
  # Parse the arguments.
  set(options EXPORTED_SHARED)
  set(one_value_args COMPILE_FLAGS EXPORTED_OUTPUT_NAME EXPORTED_OUTPUT_DIRECTORY)
  set(multi_value_args SRCS DEPS EXPORTED_DEPS NONLINK_DEPS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  # First add the regular version of the library. It uses
  # whatever linkage was defined globally.
  add_library(${LIB_NAME} ${ARG_SRCS})
  if(ARG_COMPILE_FLAGS)
    set_target_properties(${LIB_NAME}
      PROPERTIES COMPILE_FLAGS ${ARG_COMPILE_FLAGS})
  endif()
  target_link_libraries(${LIB_NAME} ${ARG_DEPS})
  if(ARG_NONLINK_DEPS)
    add_dependencies(${LIB_NAME} ${ARG_NONLINK_DEPS})
  endif()

  # Now start setting up the exported variant.
  set(EXPORTED_LIB_NAME ${LIB_NAME}_exported)
  if(ARG_EXPORTED_SHARED)
    # Leaf library.
    set(EXPORTED_LINKAGE "SHARED")
    set(EXPORTED_LINK_PRIVATE "LINK_PRIVATE")
  else()
    # Internal library.
    set(EXPORTED_LINKAGE "STATIC")
    set(EXPORTED_LINK_PRIVATE)
  endif()
  add_library(${EXPORTED_LIB_NAME} ${EXPORTED_LINKAGE} ${ARG_SRCS})

  # Compile with visibility flags:
  # - default for classes annotated with KUDU_EXPORT.
  # - hidden for classes annotated with KUDU_NO_EXPORT.
  # - hidden for everything else.
if(POLICY CMP0063)
  set_target_properties(${EXPORTED_LIB_NAME}
    PROPERTIES C_VISIBILITY_PRESET hidden)
  set_target_properties(${EXPORTED_LIB_NAME}
    PROPERTIES CXX_VISIBILITY_PRESET hidden)
  set_target_properties(${EXPORTED_LIB_NAME}
    PROPERTIES VISIBILITY_INLINES_HIDDEN 1)
else()
  add_compiler_export_flags(EXPORTED_FLAGS)
endif()

  # Exported variants are either static archives that will be linked to a shared
  # object, or shared objects. Either way, -fPIC is needed.
  if("${KUDU_LINK}" STREQUAL "s")
    set(EXPORTED_FLAGS "${EXPORTED_FLAGS} -fPIC")
  endif()

  # We need to remove some definitions previously added at directory scope.
  # There doesn't appear to be a good way to do this in cmake, so we do it via
  # the compiler with -U (e.g. "-UFOO" means "undefine the FOO definition").
  # Adding insult to injury, the COMPILE_DEFINITIONS property adds a -D prefix
  # to anything passed into it, so we're forced to handle the removal via
  # COMPILE_FLAGS, which, lucky for us, is emitted on the command line after
  # COMPILE_DEFINITIONS.

  # Exported variants need KUDU_EXPORT definitions to take effect.
  set(EXPORTED_FLAGS "${EXPORTED_FLAGS} -UKUDU_STATIC_DEFINE")

  # Exported variants may not use tcmalloc.
  set(EXPORTED_FLAGS "${EXPORTED_FLAGS} -UTCMALLOC_ENABLED")

  # Exported variants should conform to the C++03 ABI, which doesn't
  # include sized deallocation (new in C++14). This reverses the setting from
  # non-exported (default) flags.
  if(COMPILER_SUPPORTS_SIZED_DEALLOCATION)
    # Note: this is retained by the set_target_properties() call below.
    target_compile_options(${EXPORTED_LIB_NAME}
      PRIVATE $<$<COMPILE_LANGUAGE:CXX>:-fno-sized-deallocation>)
  endif()

  set_target_properties(${EXPORTED_LIB_NAME}
    PROPERTIES COMPILE_FLAGS "${ARG_COMPILE_FLAGS} ${EXPORTED_FLAGS}")

  # Handle EXPORTED_OUTPUT_NAME and EXPORTED_OUTPUT_DIRECTORY.
  if(ARG_EXPORTED_OUTPUT_NAME)
    set_target_properties(${EXPORTED_LIB_NAME}
      PROPERTIES LIBRARY_OUTPUT_NAME ${ARG_EXPORTED_OUTPUT_NAME})
  endif()
  if(ARG_EXPORTED_OUTPUT_DIRECTORY)
    set_target_properties(${EXPORTED_LIB_NAME}
      PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${ARG_EXPORTED_OUTPUT_DIRECTORY})
  endif()

  # Set up exported variant dependent targets.
  #
  # Every linked dependency is suffixed with "_exported". This is fine; the
  # exported target graph is expected to be complete, and ADD_THIRDPARTY_LIB
  # will provide an "exported variant" for each third party target.
  if(ARG_EXPORTED_DEPS)
    set(EXPORTED_DEPS ${ARG_EXPORTED_DEPS})
  else()
    set(EXPORTED_DEPS ${ARG_DEPS})
  endif()
  foreach(DEP ${EXPORTED_DEPS})
    list(APPEND EXPORTED_SUFFIXED_DEPS "${DEP}_exported")
  endforeach()
  target_link_libraries(${EXPORTED_LIB_NAME} ${EXPORTED_LINK_PRIVATE} ${EXPORTED_SUFFIXED_DEPS})
  if(ARG_NONLINK_DEPS)
    add_dependencies(${EXPORTED_LIB_NAME} ${ARG_NONLINK_DEPS})
  endif()
endfunction()

############################################################
# Testing
############################################################

# Add a new binary that is compiled just like a unit test but is not executed
# by or registered with "ctest". Useful for writing tests that use the Google
# Test infrastructure but cannot or should not be run pre-commit.
function(ADD_KUDU_TEST_NO_CTEST REL_TEST_NAME)
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)
  add_executable(${TEST_NAME} "${REL_TEST_NAME}.cc")
  target_link_libraries(${TEST_NAME} ${KUDU_TEST_LINK_LIBS})
endfunction()

# Add a new test case, with or without an executable that should be built.
#
# REL_TEST_NAME is the name of the test. It may be a single component
# (e.g. monotime-test) or contain additional components (e.g.
# net/net_util-test). Either way, the last component must be a globally
# unique name.
#
# Additional optional arguments:
#
#   TIMEOUT <secs>
#       Sets a timeout for running this test.
#
#       NOTE: this only affects the test timeout when run via 'ctest'.
#       Jenkins builds typically execute tests using dist-test, and that
#       does not respect these timeouts. If a test suite is long enough
#       to require a bumped timeout, consider enabling sharding of the
#       test (see below).
#
#   NUM_SHARDS <num shards>
#       Sets the number of shards used for running this test.
#
#       This configuration splits up the test cases within the binary
#       into several separate shards. Each shard becomes a separate
#       test case when run by ctest or when submitted for distributed testing.
#       This should be used whenever a test binary is long-running and
#       consists of many separate test cases.
#
#       NOTE: sharding is still recommended even for tests with RUN_SERIAL
#       or RESOURCE_LOCK properties. Even though the shards cannot run in
#       parallel on a single machine using ctest, they will still run in
#       parallel across separate machines using dist-test.
#
#   PROCESSORS <num processors>
#   RUN_SERIAL true
#       These built-in CMake properties should be used when a test is
#       a heavy consumer of CPU.
#
#       The PROCESSORS flag allows ctest to ensure that tests are only
#       scheduled in parallel when an appropriate number of cores are
#       available. The default is to assume that a test uses a single core
#       (i.e. equivalent to PROCESSORS 1).
#
#       RUN_SERIAL ensures that the test does not run in parallel with any
#       other test. This should be used for stress tests which start many
#       threads and attempt to monopolize the machine.
#
#       In order to determine an appropriate setting, you can run the test
#       with the KUDU_MEASURE_TEST_CPU_CONSUMPTION environment variable
#       set. See build-support/run-test.sh for more details.
#
#   DATA_FILES <file1> <file2> ...
#       Specify data files that should be copied into the build directory next
#       to test executable into the 'testdata' sub-directory, i.e. into
#       '<build_dir>/bin/testdata' with current layout of the test binaries.
#       The path to the source file should be specified from the location
#       of the corresponding CMakeLists.txt file.
#
# Any other arguments will be passed to set_tests_properties().
function(ADD_KUDU_TEST REL_TEST_NAME)
  # Parse out properties for which we have special handling.
  set(options)
  set(one_value_args TIMEOUT NUM_SHARDS)
  set(multi_value_args DATA_FILES)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(NOT ARG_TIMEOUT)
    # Default to 15 minutes.
    # NOTE: this should be kept in sync with the default value of KUDU_TEST_TIMEOUT
    # in build-support/run-test.sh
    set(ARG_TIMEOUT 900)
  endif()
  if(NOT ARG_NUM_SHARDS)
    set(ARG_NUM_SHARDS 1)
  endif()

  # Any unrecognized arguments go into ${ARG_UNPARSED_ARGUMENTS}, which we forward
  # along as properties down below.

  if(NO_TESTS)
    return()
  endif()

  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME}.cc)
    # This test has a corresponding .cc file, set it up as an executable.
    set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
    ADD_KUDU_TEST_NO_CTEST(${REL_TEST_NAME})
  elseif(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME})
    # No executable, just invoke the test (probably a script) directly.
    get_filename_component(TEST_NAME_WITH_EXT ${REL_TEST_NAME} NAME)
    set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME_WITH_EXT}")

    # Ideally this would run only when the test is built, not when cmake runs,
    # but add_test() doesn't yield a target (if it did, that target could depend
    # on an add_custom_command() that copies the test file into place).
    execute_process(COMMAND ln -sf ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME}
      ${EXECUTABLE_OUTPUT_PATH})
  else()
    message(FATAL_ERROR "Neither ${REL_TEST_NAME} nor ${REL_TEST_NAME}.cc were found in ${CMAKE_CURRENT_SOURCE_DIR}/")
  endif()

  # Copy data files into the build directory.
  set(DATA_FILES_DST_SUBDIR testdata)
  set(DST_DIR ${EXECUTABLE_OUTPUT_PATH}/${DATA_FILES_DST_SUBDIR})
  set(DATA_FILES_LIST)
  foreach(DATA_FILE ${ARG_DATA_FILES})
    get_filename_component(DATA_FILE_NAME ${DATA_FILE} NAME)
    list(APPEND DATA_FILES_LIST ${DATA_FILES_DST_SUBDIR}/${DATA_FILE_NAME})
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/${DATA_FILE}
      # Copy with read and execute permissions, since tests should not modify
      # the data files in place, but data files may be scripts used by tests.
      DIRECTORY_PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE
      FILE_PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE
      DESTINATION ${DST_DIR})
  endforeach()

  math(EXPR MAX_SHARD "${ARG_NUM_SHARDS} - 1")
  foreach(SHARD_NUM RANGE ${MAX_SHARD})
    # Only name the targets with a '.<shard>' if the test is sharded.
    if(${ARG_NUM_SHARDS} EQUAL 1)
      set(TARGET ${TEST_NAME})
    else()
      set(TARGET ${TEST_NAME}.${SHARD_NUM})
    endif()

    add_test(${TARGET}
      ${BUILD_SUPPORT_DIR}/run-test.sh ${TEST_PATH})
    if(ARG_UNPARSED_ARGUMENTS)
      set_tests_properties(${TARGET} PROPERTIES ${ARG_UNPARSED_ARGUMENTS})
    endif()
    # Set the ctest timeout to be a bit longer than the timeout we pass to
    # our test wrapper. This gives the test wrapper some opportunity to do
    # things like dump stacks, compress the log, etc.
    math(EXPR EXTENDED_TIMEOUT "${ARG_TIMEOUT} + 30")

    # Add the configured timeout to the environment for the test wrapper.
    get_test_property(${TARGET} ENVIRONMENT CUR_TEST_ENV)
    if(NOT CUR_TEST_ENV)
      set(CUR_TEST_ENV "")
    endif()
    list(APPEND CUR_TEST_ENV "KUDU_TEST_TIMEOUT=${ARG_TIMEOUT}")
    list(APPEND CUR_TEST_ENV "GTEST_TOTAL_SHARDS=${ARG_NUM_SHARDS}")
    list(APPEND CUR_TEST_ENV "GTEST_SHARD_INDEX=${SHARD_NUM}")

    # The only way we can pass information to dist-test is through the environment.
    # So, use a comma-delimited environment variable to pass the list of data files
    # that need to be uploaded.
    if(DATA_FILES_LIST)
      string(REPLACE ";" "," DATA_FILES_ENV "${DATA_FILES_LIST}")
      list(APPEND CUR_TEST_ENV "KUDU_DATA_FILES=${DATA_FILES_ENV}")
    endif()

    set_tests_properties(${TARGET} PROPERTIES
      TIMEOUT ${EXTENDED_TIMEOUT}
      ENVIRONMENT "${CUR_TEST_ENV}")
  endforeach(SHARD_NUM)
endfunction()

# A wrapper for add_dependencies() that is compatible with NO_TESTS.
function(ADD_KUDU_TEST_DEPENDENCIES REL_TEST_NAME)
  if(NO_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_dependencies(${TEST_NAME} ${ARGN})
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

  if(("${KUDU_LINK}" STREQUAL "s" AND ARG_STATIC_LIB) OR (NOT ARG_SHARED_LIB))
    if(NOT ARG_STATIC_LIB)
      message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
    endif()
    add_library(${LIB_NAME} STATIC IMPORTED)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    message("Added static library dependency ${LIB_NAME}: ${ARG_STATIC_LIB}")
  else()
    add_library(${LIB_NAME} SHARED IMPORTED)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    message("Added shared library dependency ${LIB_NAME}: ${ARG_SHARED_LIB}")
  endif()

  if(ARG_DEPS)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${ARG_DEPS}")
  endif()

  # Set up an "exported variant" for this thirdparty library (see "Visibility"
  # above). It's the same as the real target, just with an "_exported" suffix.
  # We prefer the static archive if it exists (as it's akin to an "internal"
  # library), but we'll settle for the shared object if we must.
  #
  # A shared object exported variant will force any "leaf" library that
  # transitively depends on it to also depend on it at runtime; this is
  # desirable for some libraries (e.g. cyrus_sasl).
  set(LIB_NAME_EXPORTED "${LIB_NAME}_exported")
  if(ARG_STATIC_LIB)
    add_library(${LIB_NAME_EXPORTED} STATIC IMPORTED)
    set_target_properties(${LIB_NAME_EXPORTED}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
  else()
    add_library(${LIB_NAME_EXPORTED} SHARED IMPORTED)
    set_target_properties(${LIB_NAME_EXPORTED}
      PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
  endif()
  if(ARG_DEPS)
    foreach(DEP ${ARG_DEPS})
      list(APPEND EXPORTED_DEPS "${DEP}_exported")
    endforeach()
    set_target_properties(${LIB_NAME_EXPORTED}
      PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${EXPORTED_DEPS}")
  endif()
endfunction()

if (${KUDU_USE_TSAN})
  set(THIRDPARTY_INSTALL_CURRENT_DIR ${THIRDPARTY_INSTALL_DIR}/tsan)
else()
  set(THIRDPARTY_INSTALL_CURRENT_DIR ${THIRDPARTY_INSTALL_DIR}/uninstrumented)
endif()

# Look in thirdparty prefix paths before anywhere else for system dependencies.
set(CMAKE_PREFIX_PATH
    ${THIRDPARTY_INSTALL_COMMON_DIR}
    ${THIRDPARTY_INSTALL_CURRENT_DIR}
    ${CMAKE_PREFIX_PATH})

## Cyrus SASL
find_package(CyrusSASL REQUIRED)
include_directories(SYSTEM ${CYRUS_SASL_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(cyrus_sasl
  SHARED_LIB "${CYRUS_SASL_SHARED_LIB}")

## GSSAPI
find_package(GSSAPI REQUIRED)
include_directories(SYSTEM ${GSSAPI_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(gssapi_krb5
  SHARED_LIB "${GSSAPI_SHARED_LIB}")

## GLog (depends on libunwind)
find_package(GLog REQUIRED)
include_directories(SYSTEM ${GLOG_INCLUDE_DIR})
set(GLOG_DEPS)
if (NOT APPLE)
  set(GLOG_DEPS unwind)
endif()
ADD_THIRDPARTY_LIB(glog
  STATIC_LIB "${GLOG_STATIC_LIB}"
  SHARED_LIB "${GLOG_SHARED_LIB}"
  DEPS "${GLOG_DEPS}")
list(APPEND KUDU_BASE_LIBS glog)

## libunwind
##
## Doesn't build on OSX.
if (NOT APPLE)
  find_package(LibUnwind REQUIRED)
  include_directories(SYSTEM ${UNWIND_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(unwind
    STATIC_LIB "${UNWIND_STATIC_LIB}"
    SHARED_LIB "${UNWIND_SHARED_LIB}")
  list(APPEND KUDU_BASE_LIBS unwind)
endif()

## GFlags
find_package(GFlags REQUIRED)
include_directories(SYSTEM ${GFLAGS_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(gflags
  STATIC_LIB "${GFLAGS_STATIC_LIB}"
  SHARED_LIB "${GFLAGS_SHARED_LIB}")
list(APPEND KUDU_BASE_LIBS gflags)

## GMock
find_package(GMock REQUIRED)
include_directories(SYSTEM ${GMOCK_INCLUDE_DIR} ${GTEST_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(gmock
  STATIC_LIB ${GMOCK_STATIC_LIBRARY}
  SHARED_LIB ${GMOCK_SHARED_LIBRARY})

## Protobuf
find_package(Protobuf REQUIRED)
include_directories(SYSTEM ${PROTOBUF_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(protobuf
  STATIC_LIB "${PROTOBUF_STATIC_LIBRARY}"
  SHARED_LIB "${PROTOBUF_SHARED_LIBRARY}")
ADD_THIRDPARTY_LIB(protoc
  STATIC_LIB "${PROTOBUF_PROTOC_STATIC_LIBRARY}"
  SHARED_LIB "${PROTOBUF_PROTOC_SHARED_LIBRARY}"
  DEPS protobuf)
find_package(KRPC REQUIRED)

## Thrift
find_package(Thrift REQUIRED)
include_directories(SYSTEM ${THRIFT_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(thrift
  STATIC_LIB "${THRIFT_STATIC_LIBRARY}"
  SHARED_LIB "${THRIFT_SHARED_LIBRARY}")

# The mini-HMS relies on JAVA_HOME being set in order to run the HMS, and
# JDK 1.7 or later for compiling the Kudu metastore plugin.
find_package(JavaHome REQUIRED)
find_package(Java 1.7 REQUIRED)
# Defines the add_jar() CMake command.
include(UseJava)

## Snappy
find_package(Snappy REQUIRED)
include_directories(SYSTEM ${SNAPPY_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(snappy
  STATIC_LIB "${SNAPPY_STATIC_LIB}"
  SHARED_LIB "${SNAPPY_SHARED_LIB}")

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

## Bitshuffle
find_package(Bitshuffle REQUIRED)
include_directories(SYSTEM ${BITSHUFFLE_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(bitshuffle STATIC_LIB "${BITSHUFFLE_STATIC_LIB}")

## ZLib
find_package(Zlib REQUIRED)
include_directories(SYSTEM ${ZLIB_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(zlib
  STATIC_LIB "${ZLIB_STATIC_LIB}"
  SHARED_LIB "${ZLIB_SHARED_LIB}")

## Squeasel
find_package(Squeasel REQUIRED)
include_directories(SYSTEM ${SQUEASEL_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(squeasel
  STATIC_LIB "${SQUEASEL_STATIC_LIB}")

## Mustache
find_package(Mustache REQUIRED)
include_directories(SYSTEM ${MUSTACHE_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(mustache
  STATIC_LIB "${MUSTACHE_STATIC_LIB}")

## OpenSSL
##
## Version 1.0.0 or higher is required because we are using the following
## features introduced started OpenSSL 1.0.0:
##   * The new breed of functions to work with the X509_EXTENSION stack
##   * automatic usage of &errno as a safe per-thread identifier
##
## If having multiple OpenSSL libraries installed on the system,
## use the OPENSSL_ROOT_DIR cmake flag to specify where to look for the proper
## version of the OpenSSL framework/library, e.g.
##
##   cmake -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl ...
##
## If no such OPENSSL_ROOT_DIR is specified, and we see that there is an OpenSSL
## binary in thirdparty (deposited there by thirdparty/install-openssl-el6-workaround.sh)
## then we'll use that one. See that script for more information.
set(CENTOS_6_4_OPENSSL_DIR "${THIRDPARTY_INSTALL_DIR}/openssl-el6-workaround/usr/")
if (NOT OPENSSL_ROOT_DIR AND EXISTS "${CENTOS_6_4_OPENSSL_DIR}")
  set(OPENSSL_ROOT_DIR "${CENTOS_6_4_OPENSSL_DIR}")
endif()
find_package(OpenSSL 1.0.0 REQUIRED)
include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(openssl_ssl
  SHARED_LIB "${OPENSSL_SSL_LIBRARY}")
ADD_THIRDPARTY_LIB(openssl_crypto
  SHARED_LIB "${OPENSSL_CRYPTO_LIBRARY}")

## Kerberos
find_package(Kerberos REQUIRED)
include_directories(${KERBEROS_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(krb5
  SHARED_LIB "${KERBEROS_LIBRARY}")

if (NO_CHRONY)
  add_definitions("-DNO_CHRONY")
endif()

## Google PerfTools
##
## Disabled with TSAN/ASAN.
if (NOT "${KUDU_USE_ASAN}" AND
    NOT "${KUDU_USE_TSAN}")
  find_package(GPerf REQUIRED)
  ADD_THIRDPARTY_LIB(tcmalloc
    STATIC_LIB "${TCMALLOC_STATIC_LIB}"
    SHARED_LIB "${TCMALLOC_SHARED_LIB}")
  ADD_THIRDPARTY_LIB(profiler
    STATIC_LIB "${PROFILER_STATIC_LIB}"
    SHARED_LIB "${PROFILER_SHARED_LIB}")
  list(APPEND KUDU_BASE_LIBS tcmalloc profiler)
  add_definitions("-DTCMALLOC_ENABLED")
  set(KUDU_TCMALLOC_AVAILABLE 1)
endif()

## curl
find_package(CURL REQUIRED)
include_directories(SYSTEM ${CURL_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(curl
  STATIC_LIB "${CURL_STATIC_LIB}"
  SHARED_LIB "${CURL_SHARED_LIB}"
  DEPS zlib)

## crcutil
find_package(Crcutil REQUIRED)
include_directories(SYSTEM ${CRCUTIL_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(crcutil
  STATIC_LIB "${CRCUTIL_STATIC_LIB}"
  SHARED_LIB "${CRCUTIL_SHARED_LIB}")

## breakpad
if (NOT APPLE)
  find_package(BreakpadClient REQUIRED)
  include_directories(SYSTEM ${BREAKPAD_CLIENT_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(breakpad_client
    STATIC_LIB "${BREAKPAD_CLIENT_STATIC_LIB}"
    SHARED_LIB "${BREAKPAD_CLIENT_SHARED_LIB}")
endif()

## llvm
# Note that llvm has a unique cmake setup. See kudu/codegen/CMakeLists.txt
# for details.
find_package(LLVM REQUIRED CONFIG)
if(${LLVM_PACKAGE_VERSION} VERSION_LESS 3.4)
  message(FATAL_ERROR "LLVM version (${LLVM_PACKAGE_VERSION}) must be at least 3.4")
endif()
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")

## librt
if (NOT APPLE)
  find_library(RT_LIB_PATH rt)
  if(NOT RT_LIB_PATH)
    message(FATAL_ERROR "Could not find librt on the system path")
  endif()
  ADD_THIRDPARTY_LIB(rt
    SHARED_LIB "${RT_LIB_PATH}")

  find_library(DL_LIB_PATH dl)
  if(NOT DL_LIB_PATH)
    message(FATAL_ERROR "Could not find libdl on the system path")
  endif()
  ADD_THIRDPARTY_LIB(dl
    SHARED_LIB "${DL_LIB_PATH}")
endif()

## Kerberos binaries (kinit, kadmin, etc).
if (NOT NO_TESTS)
  ## We rely on the Kerberos binaries for testing security.
  find_package(KerberosPrograms)
endif()

# The tests as well as any binaries which are run as subprocesses by tests (eg tserver,
# master, and the 'kudu' CLI tool) need to link these in. We have to set them
# here so they are accessible by all targets.
if (NOT APPLE)
  set(SANITIZER_OPTIONS_OVERRIDE -Wl,-u_sanitizer_options_link_helper sanitizer_options)
  set(KRB5_REALM_OVERRIDE -Wl,--undefined=krb5_realm_override_loaded krb5_realm_override)
else()
  set(SANITIZER_OPTIONS_OVERRIDE -Wl,-U,_sanitizer_options_link_helper sanitizer_options)
  set(KRB5_REALM_OVERRIDE -Wl,-U,krb5_realm_override_loaded krb5_realm_override)
endif()

## yaml
find_package(Yaml REQUIRED)
include_directories(SYSTEM ${YAML_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(yaml
  STATIC_LIB "${YAML_STATIC_LIB}"
  SHARED_LIB "${YAML_SHARED_LIB}")

## gumbo-parser
find_package(GumboParser REQUIRED)
include_directories(SYSTEM ${GUMBO_PARSER_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(gumbo-parser
  STATIC_LIB "${GUMBO_PARSER_STATIC_LIB}"
  SHARED_LIB "${GUMBO_PARSER_SHARED_LIB}")

## gumbo-query
find_package(GumboQuery REQUIRED)
include_directories(SYSTEM ${GUMBO_QUERY_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(gumbo-query
  STATIC_LIB "${GUMBO_QUERY_STATIC_LIB}"
  SHARED_LIB "${GUMBO_QUERY_SHARED_LIB}"
  DEPS gumbo-parser)

## Boost

# We use a custom cmake module and not cmake's FindBoost.
# see: cmake_modules/FindKuduBoost.cmake
find_package(KuduBoost REQUIRED)
include_directories(SYSTEM ${BOOST_INCLUDE_DIR})

ADD_THIRDPARTY_LIB(boost_date_time
    STATIC_LIB "${BOOST_DATE_TIME_STATIC_LIB}"
    SHARED_LIB "${BOOST_DATE_TIME_SHARED_LIB}")

############################################################
# Enable sized deallocation where supported.
# This happens down here instead of up with the rest of the
# compiler options since we only do this when tcmalloc was
# found and enabled.
############################################################

# On OS X 10.12 (El Capitan), sized-deallocation symbols are missing
# in the libc++ library for some reason.
if (APPLE)
  get_filename_component(SIZED_DEALLOCATION_TEST_TCMALLOC_LIBDIR
    "${TCMALLOC_STATIC_LIB}" DIRECTORY)
  execute_process(
    COMMAND printf
      "#include <new>\nint main(){(::operator delete)(0,256);return 0;}\n"
    COMMAND ${CMAKE_CXX_COMPILER}
      -x c++ -fsized-deallocation -O0 -fno-builtin
      -L${SIZED_DEALLOCATION_TEST_TCMALLOC_LIBDIR} -ltcmalloc -o /dev/null -
    RESULT_VARIABLE SIZED_DEALLOCATION_TEST_RESULT OUTPUT_QUIET ERROR_QUIET)
  if (${SIZED_DEALLOCATION_TEST_RESULT} EQUAL 0)
    set(COMPILER_SUPPORTS_SIZED_DEALLOCATION TRUE)
  endif()
else()
  if (KUDU_TCMALLOC_AVAILABLE AND
      (("${COMPILER_FAMILY}" STREQUAL "clang") OR
       ("${COMPILER_FAMILY}" STREQUAL "gcc" AND
        "${COMPILER_VERSION}" VERSION_GREATER "5.0")))
    set(COMPILER_SUPPORTS_SIZED_DEALLOCATION TRUE)
  endif()
endif()

if(COMPILER_SUPPORTS_SIZED_DEALLOCATION)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsized-deallocation")
  message("sized-deallocation is ENABLED")
else()
  message("sized-deallocation is DISABLED")
endif()

############################################################
# Linker setup
############################################################

set(KUDU_MIN_TEST_LIBS ${KUDU_BASE_LIBS} kudu_test_main kudu_test_util)
# Prepend SANITIZER_OPTIONS_OVERRIDE if this is a sanitizer build.
# SANITIZER_OPTIONS_OVERRIDE needs to be linked first so that it is statically
# linked to the test binaries directly. Otherwise the weakly linked default
# implementations coul be be used when running tests.
if ("${KUDU_USE_ASAN}" OR "${KUDU_USE_TSAN}" OR "${KUDU_USE_UBSAN}")
  list(INSERT KUDU_MIN_TEST_LIBS 0 ${SANITIZER_OPTIONS_OVERRIDE})
endif()
set(KUDU_TEST_LINK_LIBS ${KUDU_MIN_TEST_LIBS})

# This macro initializes KUDU_MIN_TEST_LIBS to KUDU_MIN_TEST_LIBS and
# appends the passed list of libraries to the end. This ensures that
# KUDU_MIN_TEST_LIBS is linked first.
macro(SET_KUDU_TEST_LINK_LIBS)
  set(KUDU_TEST_LINK_LIBS ${KUDU_MIN_TEST_LIBS})
  list(APPEND KUDU_TEST_LINK_LIBS ${ARGN})
endmacro()

# Use "thin archives" for our static libraries. We only use static libraries
# internal to our own build, so thin ones are just as good and much smaller.
if (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  set(CMAKE_CXX_ARCHIVE_CREATE "<CMAKE_AR> qcT <TARGET> <LINK_FLAGS> <OBJECTS>")
  set(CMAKE_C_ARCHIVE_CREATE "<CMAKE_AR> qcT <TARGET> <LINK_FLAGS> <OBJECTS>")
  set(CMAKE_CXX_ARCHIVE_APPEND "<CMAKE_AR> qT <TARGET> <LINK_FLAGS> <OBJECTS>")
  set(CMAKE_C_ARCHIVE_APPEND "<CMAKE_AR> qT <TARGET> <LINK_FLAGS> <OBJECTS>")
endif()

############################################################
# "make ctags" target
############################################################
if (UNIX)
  add_custom_target(ctags ctags --languages=c++,c -L
    `find ${CMAKE_CURRENT_SOURCE_DIR}/src
          ${CMAKE_CURRENT_BINARY_DIR}/src`)
endif (UNIX)

############################################################
# "make etags" target
#
# Requires the exuberant-ctags system package.
############################################################
if (UNIX)
  add_custom_target(etags etags --members --declarations
    `find ${CMAKE_CURRENT_SOURCE_DIR}/src
          ${CMAKE_CURRENT_BINARY_DIR}/src
          -name \\*.cc -or -name \\*.hh -or -name \\*.cpp -or
          -name \\*.h -or -name \\*.c`)
endif (UNIX)

############################################################
# "make cscope" target
############################################################
if (UNIX)
  add_custom_target(cscope
    find ${CMAKE_CURRENT_SOURCE_DIR}/src
         ${CMAKE_CURRENT_BINARY_DIR}/src
         -name \\*.cc -or -name \\*.hh -or -name \\*.cpp -or
         -name \\*.h -or -name \\*.c
    > cscope.files && cscope -q -b)
endif (UNIX)

############################################################
# "make lint" target
############################################################
if (UNIX)
  # Full lint
  add_custom_target(lint ${BUILD_SUPPORT_DIR}/lint.sh)
  # Incremental lint - only checks files changed since the last
  # merged upstream commit
  add_custom_target(ilint ${BUILD_SUPPORT_DIR}/lint.sh -c)
endif (UNIX)

############################################################
# "make pb-gen": generate all protobuf source/header files
############################################################
if (UNIX)
  add_custom_target(pb-gen)
endif (UNIX)

############################################################
# "make krpc-gen": generate all KRPC source/header files
############################################################
if (UNIX)
  add_custom_target(krpc-gen)
endif (UNIX)

############################################################
# "generated-headers" target
############################################################
if (UNIX)
  add_custom_target(generated-headers
    DEPENDS pb-gen krpc-gen hms_thrift gen_version_info)
endif (UNIX)

############################################################
# "make iwyu" and "make iwyu-fix" target
############################################################
if (UNIX)
  add_custom_target(iwyu ${BUILD_SUPPORT_DIR}/iwyu.py --from-git
    DEPENDS generated-headers)
  add_custom_target(iwyu-fix ${BUILD_SUPPORT_DIR}/iwyu.py --fix --from-git
    DEPENDS generated-headers)
endif (UNIX)

############################################################
# "make tidy" target
############################################################
if (UNIX)
  add_custom_target(tidy ${BUILD_SUPPORT_DIR}/tidy.sh)
  add_dependencies(tidy generated-headers)
endif (UNIX)

############################################################
# "make docs" target
############################################################
if (UNIX)
  add_custom_target(docs
    # The docs output HTML will end up in a docs/ subdir.
    ${CMAKE_CURRENT_SOURCE_DIR}/docs/support/scripts/make_docs.sh
      --build_root ${CMAKE_CURRENT_BINARY_DIR})
endif (UNIX)

############################################################
# "make doxygen" target
# Requires doxygen of version >= 1.8.11 to leverage the
# WARN_AS_ERROR configuration parameter (introduced in
# version 1.8.11) for spotting various issues in the Kudu
# C++ client API documentation.
############################################################
if (UNIX)
  find_package(Doxygen 1.8.11)
  if (NOT (DOXYGEN_FOUND AND DOXYGEN_DOT_FOUND))
    message(WARNING "Doxygen with Dot support (graphviz) not found: 'doxygen' target is not available")
  else ()
    if (DOXYGEN_WARN_AS_ERROR)
      set(DOXY_CLIENT_API_WARN_AS_ERROR YES)
    else()
      set(DOXY_CLIENT_API_WARN_AS_ERROR NO)
    endif()
    set(DOXY_SUBDIR ${CMAKE_CURRENT_BINARY_DIR}/docs/doxygen)
    set(DOXY_CLIENT_DESTDIR ${DOXY_SUBDIR}/tmp.client)
    set(DOXY_CLIENT_API_CFG ${DOXY_SUBDIR}/client_api.doxy)
    set(DOXY_CLIENT_API_FOOTER ${DOXY_SUBDIR}/client_api.footer)
    set(DOXY_CLIENT_API_OUTDIR ${DOXY_SUBDIR}/client_api)
    list(APPEND DOXY_CLIENT_API_EXCLUDE
      "share/doc/kuduClient/examples/example.cc")
    # NOTE: DOXY_CLIENT_API_EXCLUDE, DOXY_CLIENT_API_OUTDIR, and
    #       DOXY_CLIENT_API_FOOTER are used in client_api.doxy.in template file
    configure_file(docs/support/doxygen/client_api.doxy.in ${DOXY_CLIENT_API_CFG} @ONLY)
    configure_file(docs/support/doxygen/client_api.footer.in ${DOXY_CLIENT_API_FOOTER} @ONLY)
    add_custom_target(doxy_install_client_alt_destdir
      COMMAND ${CMAKE_COMMAND} -E remove_directory ${DOXY_CLIENT_DESTDIR}
      COMMAND DESTDIR=${DOXY_CLIENT_DESTDIR} ${CMAKE_MAKE_PROGRAM} install
      COMMENT "Installing Kudu client files into temporary destroot"
    )
    add_custom_target(doxygen
      COMMAND ${DOXYGEN_EXECUTABLE} ${DOXY_CLIENT_API_CFG}
      WORKING_DIRECTORY ${DOXY_CLIENT_DESTDIR}/${CMAKE_INSTALL_PREFIX}
      COMMENT "Generating Kudu C++ client API documentation"
    )
    add_dependencies(doxy_install_client_alt_destdir kudu_client_exported)
    add_dependencies(doxygen doxy_install_client_alt_destdir)
    # If doxygen is present, generate doxygen documentation along with 'docs'.
    add_dependencies(docs doxygen)
  endif ()
endif (UNIX)

############################################################
# "make site" target
#
# NOTE: It's supposed find_package(Doxygen) has already
# been run at this point.
############################################################
if (UNIX)
  if (NOT DOXYGEN_FOUND)
    add_custom_target(site
      ${CMAKE_CURRENT_SOURCE_DIR}/docs/support/scripts/make_site.sh --no-doxygen)
  else ()
    add_custom_target(site
      ${CMAKE_CURRENT_SOURCE_DIR}/docs/support/scripts/make_site.sh)
  endif ()
endif (UNIX)

############################################################
# Subdirectories
############################################################

add_subdirectory(src/kudu/benchmarks)
add_subdirectory(src/kudu/cfile)
add_subdirectory(src/kudu/client)
add_subdirectory(src/kudu/clock)
add_subdirectory(src/kudu/codegen)
add_subdirectory(src/kudu/common)
add_subdirectory(src/kudu/consensus)
add_subdirectory(src/kudu/experiments)
add_subdirectory(src/kudu/fs)
# Google util libraries borrowed from supersonic, tcmalloc, Chromium, etc.
add_subdirectory(src/kudu/gutil)
add_subdirectory(src/kudu/hms)
add_subdirectory(src/kudu/integration-tests)
add_subdirectory(src/kudu/kserver)
add_subdirectory(src/kudu/master)
add_subdirectory(src/kudu/mini-cluster)
add_subdirectory(src/kudu/postgres)
add_subdirectory(src/kudu/ranger)
add_subdirectory(src/kudu/rebalance)
add_subdirectory(src/kudu/rpc)
add_subdirectory(src/kudu/security)
add_subdirectory(src/kudu/server)
add_subdirectory(src/kudu/subprocess)
add_subdirectory(src/kudu/tablet)
add_subdirectory(src/kudu/thrift)
add_subdirectory(src/kudu/tools)
add_subdirectory(src/kudu/tserver)
add_subdirectory(src/kudu/util)
