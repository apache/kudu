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

# - Find Bitshuffle (bitshuffle.h, bitshuffle.a)
# This module defines
#  BITSHUFFLE_INCLUDE_DIR, directory containing headers
#  BITSHUFFLE_STATIC_LIB, path to bitshuffle's static library
#  BITSHUFFLE_FOUND, whether bitshuffle has been found

find_path(BITSHUFFLE_INCLUDE_DIR bitshuffle.h
  # make sure we don't accidentally pick up a different version
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(BITSHUFFLE_STATIC_LIB bitshuffle.a
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(BITSHUFFLE REQUIRED_VARS
  BITSHUFFLE_STATIC_LIB BITSHUFFLE_INCLUDE_DIR)
