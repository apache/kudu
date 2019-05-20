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

# - Find required numa libraries
# This module defines
#  NUMA_INCLUDE_DIR, directory containing headers
#  NUMA_STATIC_LIB, path to *.a
#  NUMA_SHARED_LIB, path to *.so shared library

find_path(NUMA_INCLUDE_DIR numa.h
  NO_CMAKE_SYSTEM_PATH
NO_SYSTEM_ENVIRONMENT_PATH)
find_library(NUMA_SHARED_LIB numa
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(NUMA_STATIC_LIB libnuma.a
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(NUMA REQUIRED_VARS
  NUMA_SHARED_LIB NUMA_STATIC_LIB
  NUMA_INCLUDE_DIR
)
