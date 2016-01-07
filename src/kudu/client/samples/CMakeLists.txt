#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Client API sample executables

cmake_minimum_required(VERSION 2.8)

find_package(kuduClient REQUIRED)
include_directories(${KUDU_CLIENT_INCLUDE_DIR})

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  # Use libstdc++ and not libc++. The latter lacks support for tr1 in OSX
  # and since 10.9 is now the default.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libstdc++")
endif()

# The Kudu client library always uses the old gcc ABI.
add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)

add_executable(sample sample.cc)
target_link_libraries(sample kudu_client)
