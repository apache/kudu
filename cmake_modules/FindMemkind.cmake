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

# - Find required memkind libraries
# This module defines
#  MEMKIND_INCLUDE_DIR, directory containing headers
#  MEMKIND_STATIC_LIB, path to *.a
#  MEMKIND_SHARED_LIB, path to *.so shared library

find_path(MEMKIND_INCLUDE_DIR memkind.h
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(MEMKIND_SHARED_LIB memkind
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(MEMKIND_STATIC_LIB libmemkind.a
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(MEMKIND REQUIRED_VARS
  MEMKIND_SHARED_LIB MEMKIND_STATIC_LIB
  MEMKIND_INCLUDE_DIR
)
