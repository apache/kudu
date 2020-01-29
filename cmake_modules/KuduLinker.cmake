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

# The Gold linker must be manually enabled.
function(APPEND_LINKER_FLAGS)
  # Search candidate linkers in the order of decreasing speed and functionality.
  # In particular, lld is the best choice since it's quite fast and supports
  # ThinLTO, etc.
  #
  # On macOS, LLD from third-party ${THIRDPARTY_TOOLCHAIN_DIR}/bin/ld64.lld
  # isn't fully functional yet: it doesn't support -U option, etc.
  if (NOT APPLE)
    set(linkers "lld" "${THIRDPARTY_TOOLCHAIN_DIR}/bin/ld.lld" "gold")
  endif()
  list(APPEND linkers "default")
  foreach(candidate_linker ${linkers})
    if(candidate_linker STREQUAL "default")
      set(candidate_flags "")
    else()
      set(candidate_flags "-fuse-ld=${candidate_linker}")
    endif()
    GET_LINKER_VERSION("${candidate_flags}")
    if(NOT LINKER_FOUND)
      continue()
    endif()

    # Older versions of the gold linker are vulnerable to a bug [1] which
    # prevents weak symbols from being overridden properly. This leads to
    # omitting of Kudu's tcmalloc dependency if using dynamic linking.
    #
    # We'll skip gold in our list of candidate linkers if this bug is relevant.
    #
    # 1. https://sourceware.org/bugzilla/show_bug.cgi?id=16979.
    if ("${LINKER_FAMILY}" STREQUAL "gold")
      if("${LINKER_VERSION}" VERSION_LESS "1.12" AND
         "${KUDU_LINK}" STREQUAL "d")
        message(WARNING "Skipping gold <1.12 with dynamic linking.")
        continue()
      endif()

      # We can't use the gold linker if it's inside devtoolset because the compiler
      # won't find it when invoked directly from make/ninja (which is typically
      # done outside devtoolset). This seems to be fixed in devtoolset-6 or later
      # by having the gcc/clang binary search the devtoolset bin directory even
      # if it's not on $PATH.
      execute_process(COMMAND which ld.gold
        OUTPUT_VARIABLE GOLD_LOCATION
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)

        if ("${GOLD_LOCATION}" MATCHES "^/opt/rh/devtoolset-[3-5]/")
          message(WARNING "Cannot use gold with devtoolset < 6, skipping...")
          continue()
        endif()
    endif()

    # LLD < 10.0.0 has a bug[1] which causes the __tsan_default_options (and similar)
    # symbols to be defined as WEAK instead of DEFINED in the resulting binary. This
    # causes our suppressions to not work properly, resulting in failed tests.
    #
    # [1] https://reviews.llvm.org/D63974
    if ("${LINKER_FAMILY}" STREQUAL "lld" AND
        "${LINKER_VERSION}" VERSION_LESS "10.0.0" AND
        ("${KUDU_USE_TSAN}" OR "${KUDU_USE_ASAN}" OR "${KUDU_USE_UBSAN}"))
      message(WARNING "Cannot use lld < 10.0.0 with TSAN/ASAN/UBSAN. Skipping...")
      continue()
    endif()
    set(linker_flags "${candidate_flags}")
    break()
  endforeach()

  if(NOT DEFINED linker_flags)
    message(SEND_ERROR "Could not find a suitable linker")
  endif()
  message(STATUS "Using linker flags: ${linker_flags}")
  foreach(var CMAKE_SHARED_LINKER_FLAGS CMAKE_EXE_LINKER_FLAGS CMAKE_MODULE_LINKER_FLAGS)
    set(${var} "${${var}} ${linker_flags}" PARENT_SCOPE)
  endforeach()
endfunction()

# Interrogates the linker version via the C++ compiler to determine which linker
# we're using, along with its version.
#
# Sets the following variables:
#   LINKER_FOUND: true/false
# When LINKER_FOUND is true, additionally sets the following variables:
#   LINKER_FAMILY: lld/ld/gold on Linux, ld64 on macOS
#   LINKER_VERSION: version number of the linker
# Any additional arguments are passed verbatim into the C++ compiler invocation.
function(GET_LINKER_VERSION)
  if(ARGN)
    message(STATUS "Checking linker version with flags: ${ARGN}")
  else()
    message(STATUS "Checking linker version when not specifying any flags")
  endif()
  if (APPLE)
    set(ld_version_flag "-v,-bundle")
  else()
    set(ld_version_flag "--version")
  endif()
  execute_process(
    COMMAND ${CMAKE_CXX_COMPILER} "-Wl,${ld_version_flag}" ${ARGN}
    ERROR_VARIABLE LINKER_STDERR
    OUTPUT_VARIABLE LINKER_STDOUT
    RESULT_VARIABLE LINKER_EXITCODE)
  if (NOT LINKER_EXITCODE EQUAL 0)
    set(LINKER_FOUND FALSE)
  elseif (LINKER_STDOUT MATCHES "GNU gold")
    # We're expecting LINKER_STDOUT to look like one of these:
    #   GNU gold (version 2.24) 1.11
    #   GNU gold (GNU Binutils for Ubuntu 2.30) 1.15
    if (NOT "${LINKER_STDOUT}" MATCHES "GNU gold \\([^\\)]*\\) (([0-9]+\\.?)+)")
      message(SEND_ERROR "Could not extract GNU gold version. "
        "Linker version output: ${LINKER_STDOUT}")
    endif()
    set(LINKER_FOUND TRUE)
    set(LINKER_FAMILY "gold")
    set(LINKER_VERSION "${CMAKE_MATCH_1}")
  elseif (LINKER_STDOUT MATCHES "GNU ld")
    # We're expecting LINKER_STDOUT to look like one of these:
    #   GNU ld (GNU Binutils for Ubuntu) 2.30
    #   GNU ld version 2.20.51.0.2-5.42.el6 20100205
    #   GNU ld version 2.25.1-22.base.el7
    if (NOT "${LINKER_STDOUT}" MATCHES "GNU ld version (([0-9]+\\.?)+)" AND
        NOT "${LINKER_STDOUT}" MATCHES "GNU ld \\([^\\)]*\\) (([0-9]+\\.?)+)")
      message(SEND_ERROR "Could not extract GNU ld version. "
        "Linker version output: ${LINKER_STDOUT}")
    endif()
    set(LINKER_FOUND TRUE)
    set(LINKER_FAMILY "ld")
    set(LINKER_VERSION "${CMAKE_MATCH_1}")
  elseif (LINKER_STDOUT MATCHES "LLD")
    # Sample:
    #   LLD 9.0.0 (example.com:kudu.git a5848a4c3c8c72a1ac823182e87cd1f6c31ddc15) (compatible with GNU linkers)
    if (NOT "${LINKER_STDOUT}" MATCHES "LLD (([0-9]+\\.?)+)")
      message(SEND_ERROR "Could not extract lld version. "
        "Linker version output: ${LINKER_STDOUT}")
    endif()
    set(LINKER_FOUND TRUE)
    set(LINKER_FAMILY "lld")
    set(LINKER_VERSION "${CMAKE_MATCH_1}")
  elseif (LINKER_STDERR MATCHES "PROJECT:ld64")
    # ld64 outputs the versioning information into stderr.
    # Sample:
    #   @(#)PROGRAM:ld  PROJECT:ld64-409.12
    #   @(#)PROGRAM:ld  PROJECT:ld64-530
    if (NOT "${LINKER_STDERR}" MATCHES "PROJECT:ld64-([0-9]+(\\.[0-9]+)?)")
      message(SEND_ERROR "Could not extract ld64 version. "
        "Linker version output: ${LINKER_STDOUT}")
    endif()
    set(LINKER_FOUND TRUE)
    set(LINKER_FAMILY "ld64")
    set(LINKER_VERSION "${CMAKE_MATCH_1}")
  else()
    set(LINKER_FOUND FALSE)
  endif()

  # Propagate the results to the caller.
  set(LINKER_FOUND "${LINKER_FOUND}" PARENT_SCOPE)
  set(LINKER_FAMILY "${LINKER_FAMILY}" PARENT_SCOPE)
  set(LINKER_VERSION "${LINKER_VERSION}" PARENT_SCOPE)

  if (LINKER_FOUND)
    message(STATUS "Found linker: ${LINKER_FAMILY} version ${LINKER_VERSION}")
  else()
    message(STATUS "Linker not found")
  endif()
endfunction()
