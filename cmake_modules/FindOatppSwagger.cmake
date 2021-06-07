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

# - Find Oatpp-Swagger Module
# This module defines
#  OATPP_SWAGGER_INCLUDE_DIR, directory containing headers
#  OATPP_SWAGGER_STATIC_LIB, path to liboatpp-swagger.a.
#  OATPP_SWAGGER_FOUND, whether oat++ swagger has been found

find_path(OATPP_SWAGGER_INCLUDE_DIR oatpp/oatpp-swagger/oatpp-swagger/Resources.hpp
        # make sure we don't accidentally pick up a different version
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

set(OATPP_SWAGGER_INCLUDE_DIR ${OATPP_SWAGGER_INCLUDE_DIR}/oatpp/oatpp-swagger/)

find_library(OATPP_SWAGGER_STATIC_LIB oatpp/liboatpp-swagger.a
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(OatppSwagger  REQUIRED_VARS
        OATPP_SWAGGER_STATIC_LIB OATPP_SWAGGER_INCLUDE_DIR)