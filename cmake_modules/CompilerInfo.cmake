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
#
# Sets COMPILER_FAMILY to 'clang' or 'gcc'
# Sets COMPILER_VERSION to the version
execute_process(COMMAND env LANG=C "${CMAKE_CXX_COMPILER}" -v
                ERROR_VARIABLE COMPILER_VERSION_FULL)
message(${COMPILER_VERSION_FULL})

# Information on Xcode, llvm-gcc, and clang versions.
#   https://trac.macports.org/wiki/XcodeVersionInfo
# Information on Xcode/LLVM and LLVM Apple/upstream version mapping:
#   https://en.wikipedia.org/wiki/Xcode#Xcode_7.0_-_12.x_(since_Free_On-Device_Development)

# gcc
if("${COMPILER_VERSION_FULL}" MATCHES ".*gcc version.*")
  set(COMPILER_FAMILY "gcc")
  string(REGEX REPLACE ".*gcc version ([0-9]+(\\.[0-9]+)*).*" "\\1"
    COMPILER_VERSION "${COMPILER_VERSION_FULL}")

# clang on Mac OS X, XCode 7
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple LLVM version [0-9]+(\\.[0-9]+)* \\(clang-700\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "3.7.0svn")
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple LLVM version [0-9]+(\\.[0-9]+)* \\(clang-703\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "3.8.0svn")

# clang on Mac OS X, XCode 8
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple LLVM version [0-9]+(\\.[0-9]+)* \\(clang-80[02]\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "3.9.0svn")

# clang on Mac OS X, XCode 9
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple LLVM version [0-9]+(\\.[0-9]+)* \\(clang-900\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "4.0.0")
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple LLVM version [0-9]+(\\.[0-9]+)* \\(clang-902\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "5.0.2")

# clang on macOS, XCode 10
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple LLVM version [0-9]+(\\.[0-9]+)* \\(clang-1000\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "6.0.1")
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple LLVM version [0-9]+(\\.[0-9]+)* \\(clang-1001\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "7.0.0")

# clang on macOS, XCode 11
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple clang version [0-9]+(\\.[0-9]+)* \\(clang-1100\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "8.0.0")
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple clang version [0-9]+(\\.[0-9]+)* \\(clang-1103\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "9.0.0")

# clang on macOS, XCode 12
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple clang version [0-9]+(\\.[0-9]+)* \\(clang-1200\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "10.0.0")
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple clang version [0-9]+(\\.[0-9]+)* \\(clang-1205\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "11.1.0")

# clang on macOS, XCode 13
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple clang version [0-9]+(\\.[0-9]+)* \\(clang-1300\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "12.0.0")
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple clang version [0-9]+(\\.[0-9]+)* \\(clang-1316\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "13.0.0")

# clang on macOS, XCode 14
elseif("${COMPILER_VERSION_FULL}" MATCHES
    "Apple clang version [0-9]+(\\.[0-9]+)* \\(clang-1400\\.")
  set(COMPILER_FAMILY "clang")
  set(COMPILER_VERSION "14.0.0")

# clang on Mac OS X with Xcode verions from 3.2.6 to 6.4 inclusive
elseif("${COMPILER_VERSION_FULL}" MATCHES "Apple .* \\(based on LLVM.*")
  set(COMPILER_FAMILY "clang")
  string(REGEX REPLACE ".* \\(based on LLVM ([0-9]+(\\.[0-9]+)*.*)\\).*" "\\1"
    COMPILER_VERSION "${COMPILER_VERSION_FULL}")

# clang on Linux and MacOS (other than Xcode-based)
elseif("${COMPILER_VERSION_FULL}" MATCHES ".*clang version.*")
  set(COMPILER_FAMILY "clang")
  string(REGEX REPLACE ".*clang version ([0-9]+(\\.[0-9]+)*).*" "\\1"
    COMPILER_VERSION "${COMPILER_VERSION_FULL}")

else()
  message(FATAL_ERROR "Unknown compiler. Version info:\n${COMPILER_VERSION_FULL}")
endif()

message("Selected compiler ${COMPILER_FAMILY} ${COMPILER_VERSION}")

# gcc (and some varieties of clang) mention the path prefix where system headers
# and libraries are located.
if("${COMPILER_VERSION_FULL}" MATCHES "Configured with: .* --prefix=([^ ]*)")
  set(COMPILER_SYSTEM_PREFIX_PATH ${CMAKE_MATCH_1})
  message("Selected compiler built with --prefix=${COMPILER_SYSTEM_PREFIX_PATH}")
endif()
