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

# - Find SNAPPY (snappy.h, libsnappy.a, libsnappy.so, and libsnappy.so.1)
# This module defines
#  SNAPPY_INCLUDE_DIR, directory containing headers
#  SNAPPY_SHARED_LIB, path to snappy's shared library
#  SNAPPY_STATIC_LIB, path to snappy's static library
#  SNAPPY_FOUND, whether snappy has been found

find_path(SNAPPY_INCLUDE_DIR snappy.h
  # make sure we don't accidentally pick up a different version
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(SNAPPY_STATIC_LIB libsnappy.a
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

set(__CURRENT_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
if (APPLE)
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".dylib")
else()
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
endif()
find_library(SNAPPY_SHARED_LIB snappy
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
set(CMAKE_FIND_LIBRARY_SUFFIXES ${__CURRENT_FIND_LIBRARY_SUFFIXES})
unset(__CURRENT_FIND_LIBRARY_SUFFIXES)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Snappy REQUIRED_VARS
  SNAPPY_SHARED_LIB SNAPPY_STATIC_LIB SNAPPY_INCLUDE_DIR)
