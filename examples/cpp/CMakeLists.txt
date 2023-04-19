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
# Client API example executables

cmake_minimum_required(VERSION 2.8)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(kuduClient REQUIRED)
include_directories(${KUDU_CLIENT_INCLUDE_DIR})

add_executable(example example.cc)
target_link_libraries(example kudu_client)

# While Kudu C++ client API is still compatible with C++98, the example that
# demonstrates non-unique keys and auto-incrementing column uses features
# not available in C++98, so -std=c++0x is added to compile the example code
# with legacy compilers.
set_source_files_properties(non_unique_primary_key.cc PROPERTIES
  COMPILE_FLAGS -std=c++0x)
add_executable(non_unique_primary_key non_unique_primary_key.cc)
target_link_libraries(non_unique_primary_key kudu_client)
