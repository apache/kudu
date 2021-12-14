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

# Find jwt-cpp headers.
# This module defines:
# - JWT_CPP_INCLUDE_DIR, where to find jwt-cpp header files.
# - JwtCpp_FOUND, If false, do not try to use jwt-cpp.

find_path(JWT_CPP_INCLUDE_DIR jwt-cpp/jwt.h
        DOC "Path to the jwt-cpp header file"
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

find_package_handle_standard_args(JwtCpp REQUIRED_VARS
        JWT_CPP_INCLUDE_DIR)
