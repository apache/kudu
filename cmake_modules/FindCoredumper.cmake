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

# - Find COREDUMPER (google/coredumper.h, libcoredumper.a)
# This module defines
#  COREDUMPER_INCLUDE_DIR, directory containing headers
#  COREDUMPER_SHARED_LIB, path to libcoredumper's shared library
#  COREDUMPER_STATIC_LIB, path to libcoredumper's static library
#  COREDUMPER_FOUND, whether coredumper has been found

find_path(COREDUMPER_INCLUDE_DIR google/coredumper.h
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(COREDUMPER_SHARED_LIB coredumper
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(COREDUMPER_STATIC_LIB libcoredumper.a
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(COREDUMPER REQUIRED_VARS
  COREDUMPER_SHARED_LIB COREDUMPER_STATIC_LIB COREDUMPER_INCLUDE_DIR)
