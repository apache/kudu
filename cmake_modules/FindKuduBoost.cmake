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
# Find Boost's libs (currently date_time) and makes sure boost includes are available.
#
# We use a custom cmake module instead of cmake's FindBoost as otherwise we'd have to:
# - Run cmake's FindBoost twice to make sure it picks up the shared and static version of the lib.
# - Have a check to make sure we're not picking up the system's boost.
#
# Less (and simpler) cmake lines are required than if we were to work around cmake's FindBoost
# behavior.
#
# This module defines
#  BOOST_DATE_TIME_SHARED_LIB, path to boost's date_time shared library
#  BOOST_DATE_TIME_STATIC_LIB, path to boost's date_time static library
#  BOOST_INCLUDE_DIR, patch to boost's headers
#  BOOST_DATE_TIME_FOUND, whether boost has been found

find_path(BOOST_INCLUDE_DIR boost/bind.hpp
    NO_CMAKE_SYSTEM_PATH
    NO_SYSTEM_ENVIRONMENT_PATH)

find_library(BOOST_DATE_TIME_SHARED_LIB boost_date_time
    NO_CMAKE_SYSTEM_PATH
    NO_SYSTEM_ENVIRONMENT_PATH)
find_library(BOOST_DATE_TIME_STATIC_LIB libboost_date_time.a
    NO_CMAKE_SYSTEM_PATH
    NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(BOOST_DATE_TIME REQUIRED_VARS
    BOOST_DATE_TIME_SHARED_LIB BOOST_DATE_TIME_STATIC_LIB BOOST_INCLUDE_DIR)