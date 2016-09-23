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

# - Find Required PMEM libraries (libvmem, libpmem, libpmemobj)
# This module defines
#  PMEM_INCLUDE_DIR, directory containing headers
#  XXX_STATIC_LIBS, path to *.a
#  XXX_SHARED_LIBS, path to *.so shared library
#  PMEM_FOUND, whether PMEM libraries have been found
#  PMEMOBJ_DEPS, dependencies required for using libpmemobj

find_path(PMEM_INCLUDE_DIR libpmem.h
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(VMEM_SHARED_LIB vmem
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(VMEM_STATIC_LIB libvmem.a
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(PMEM_SHARED_LIB pmem
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(PMEM_STATIC_LIB libpmem.a
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(PMEMOBJ_SHARED_LIB pmemobj
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(PMEMOBJ_STATIC_LIB libpmemobj.a
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PMEM REQUIRED_VARS
  VMEM_SHARED_LIB VMEM_STATIC_LIB
  PMEM_SHARED_LIB PMEM_STATIC_LIB
  PMEMOBJ_SHARED_LIB PMEMOBJ_STATIC_LIB
  PMEM_INCLUDE_DIR
)
